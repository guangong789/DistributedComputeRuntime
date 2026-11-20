package main

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	diagnosticTimeout          = 2 * time.Second
	maxDiagnosticWorkers       = 256
	maxDiagnosticResponseBytes = 256 << 10
)

// sampleDiagnosticWorkers copies value records at one registry clock sample.
// The session map, live Attempt authority and scheduling state stay private.
// The size limit bounds both time under the registry lock and projection work.
func (s *CoordinatorServer) sampleDiagnosticWorkers(ctx context.Context) (time.Time, []WorkerInfo, error) {
	if err := ctx.Err(); err != nil {
		return time.Time{}, nil, status.FromContextError(err).Err()
	}
	// Retry publication can hold this mutex during storage I/O. Diagnostic
	// callers may retry instead of waiting behind an execution-path writer.
	if !s.mu.TryLock() {
		return time.Time{}, nil, status.Error(codes.Unavailable, "worker registry snapshot temporarily unavailable")
	}
	defer s.mu.Unlock()
	if len(s.workers) > maxDiagnosticWorkers {
		return time.Time{}, nil, status.Error(codes.ResourceExhausted, "worker registry exceeds diagnostics limit")
	}
	now := s.workerRegistryNow()
	workers := make([]WorkerInfo, 0, len(s.workers))
	for _, worker := range s.workers {
		workers = append(workers, worker)
	}
	return now, workers, nil
}

func (s *CoordinatorServer) ListWorkers(ctx context.Context, req *runtimepb.ListWorkersRequest) (*runtimepb.ListWorkersResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "ListWorkers request is required")
	}
	ctx, cancel := context.WithTimeout(ctx, diagnosticTimeout)
	defer cancel()
	now, workers, err := s.sampleDiagnosticWorkers(ctx)
	if err != nil {
		return nil, err
	}
	slices.SortFunc(workers, func(a, b WorkerInfo) int { return strings.Compare(a.ID, b.ID) })
	response := &runtimepb.ListWorkersResponse{WorkerSnapshotAt: timestamppb.New(now)}
	for _, worker := range workers {
		response.Workers = append(response.Workers, projectWorkerDiagnostic(worker, now))
	}
	if err := checkDiagnosticResponse(ctx, response); err != nil {
		return nil, err
	}
	return response, nil
}

func projectWorkerDiagnostic(worker WorkerInfo, now time.Time) *runtimepb.WorkerDiagnostic {
	out := &runtimepb.WorkerDiagnostic{
		WorkerId:          worker.ID,
		LastSeenAge:       durationpb.New(max(0, now.Sub(worker.LastSeen))),
		CudaHealthManaged: worker.cudaHealth.managed,
	}
	switch worker.Status {
	case WorkerAlive:
		out.Liveness = runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_ALIVE
	case WorkerDead:
		out.Liveness = runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_DEAD
	}
	for _, taskType := range [...]string{externalsquare.TaskType, cudavector.TaskType} {
		if worker.configuredFor(taskType) {
			out.ExternalTaskTypes = append(out.ExternalTaskTypes, taskType)
		}
	}
	for _, taskType := range [...]string{"hash", "sleep", externalsquare.TaskType, cudavector.TaskType} {
		if worker.eligibleFor(taskType, now) {
			out.EligibleTaskTypes = append(out.EligibleTaskTypes, taskType)
		}
	}
	if health := worker.cudaHealth; health.managed && !health.expiresAt.IsZero() {
		if health.freshAt(now) {
			out.EffectiveCudaHealth = health.state
			out.CudaEvidenceFreshness = runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_FRESH
			out.CudaFreshFor = durationpb.New(health.expiresAt.Sub(now))
		} else {
			out.CudaEvidenceFreshness = runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_EXPIRED
		}
	}
	return out
}

func (s *CoordinatorServer) DiagnoseJob(ctx context.Context, req *runtimepb.DiagnoseJobRequest) (*runtimepb.DiagnoseJobResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "DiagnoseJob request is required")
	}
	ctx, cancel := context.WithTimeout(ctx, diagnosticTimeout)
	defer cancel()
	record, found, err := s.loadDiagnosticJob(ctx, req.JobId)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	state := wireJobState(record.State)
	if state == runtimepb.JobState_JOB_STATE_UNSPECIFIED {
		return nil, status.Error(codes.Internal, "stored job state is not supported")
	}
	now, workers, err := s.sampleDiagnosticWorkers(ctx)
	if err != nil {
		return nil, err
	}
	current, found, err := s.loadDiagnosticJob(ctx, req.JobId)
	if err != nil {
		return nil, err
	}
	response := &runtimepb.DiagnoseJobResponse{
		JobId: record.JobID, State: state, TaskType: record.TaskType, AttemptId: record.AttemptID,
		WorkerId: record.WorkerID, HasWorkerResult: record.HasWorkerResult, WorkerSnapshotAt: timestamppb.New(now),
	}
	for _, worker := range workers {
		if worker.configuredFor(record.TaskType) {
			response.ConfiguredWorkers++
			if worker.Status == WorkerAlive {
				response.AliveCompatibleWorkers++
			}
		}
		if worker.eligibleFor(record.TaskType, now) {
			response.EligibleWorkers++
		}
	}

	switch {
	case !found || current != record:
		response.Interpretation = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_JOB_CHANGED
	case record.State == JobRunning:
		response.Interpretation = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_UNKNOWN
	case record.State != JobQueued && record.State != JobRecovering:
		response.Interpretation = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NOT_APPLICABLE
	case response.ConfiguredWorkers == 0:
		response.Interpretation = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_CONFIGURED_WORKER
	case response.AliveCompatibleWorkers == 0:
		response.Interpretation = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ALIVE_WORKER
	case response.EligibleWorkers == 0:
		response.Interpretation = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ELIGIBLE_WORKER
	default:
		response.Interpretation = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_ELIGIBLE_WORKER_EXISTS
	}
	if err := checkDiagnosticResponse(ctx, response); err != nil {
		return nil, err
	}
	return response, nil
}

// Keep the existing JobStore contract for scheduling. The diagnostic reads
// require context support so a timeout doesn't leave detached database work.
func (s *CoordinatorServer) loadDiagnosticJob(ctx context.Context, id int64) (JobRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return JobRecord{}, false, status.FromContextError(err).Err()
	}
	store, ok := s.jobStore.(interface {
		loadContext(context.Context, int64) (JobRecord, bool, error)
	})
	if !ok {
		return JobRecord{}, false, status.Error(codes.FailedPrecondition, "diagnostic job reads are not configured")
	}
	record, found, err := store.loadContext(ctx, id)
	if contextErr := ctx.Err(); contextErr != nil {
		return JobRecord{}, false, status.FromContextError(contextErr).Err()
	}
	if err != nil {
		return JobRecord{}, false, status.Error(codes.Internal, "failed to read job")
	}
	return record, found, nil
}

func checkDiagnosticResponse(ctx context.Context, response proto.Message) error {
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	if proto.Size(response) > maxDiagnosticResponseBytes {
		return status.Error(codes.ResourceExhausted, "diagnostic response exceeds size limit")
	}
	return nil
}
