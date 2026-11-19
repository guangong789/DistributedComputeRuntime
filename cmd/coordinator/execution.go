package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryExhaustionError identifies only the retryable session exit and carries
// the exact last attempt it published. Its RPC status remains Unavailable.
type retryExhaustionError struct {
	attemptID int64
	cause     error
}

func (e *retryExhaustionError) Error() string              { return e.cause.Error() }
func (e *retryExhaustionError) Unwrap() error              { return e.cause }
func (e *retryExhaustionError) GRPCStatus() *status.Status { return status.Convert(e.cause) }

func jobStoreErrorCode(err error) codes.Code {
	if errors.Is(err, errQueuedPublicationConflict) {
		return codes.Aborted
	}
	if errors.Is(err, ErrJobSpecConflict) || errors.Is(err, errJobAlreadyAdmitted) {
		return codes.AlreadyExists
	}
	if errors.Is(err, errJobStoreNotConfigured) {
		return codes.FailedPrecondition
	}
	return codes.Internal
}

func (s *CoordinatorServer) dispatchJob(
	ctx context.Context,
	worker WorkerInfo,
	req *runtimepb.ExecuteJobRequest,
) (*runtimepb.ExecuteJobResponse, error) {
	if _, err := rpcauth.ParseTCPEndpoint(worker.Address); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid Worker TCP endpoint")
	}
	if s.workerCredentials == nil {
		return nil, status.Error(codes.FailedPrecondition, "Worker dispatch credentials are not configured")
	}
	transport, err := s.workerCredentials(worker)
	if err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, status.Error(codes.FailedPrecondition, "Worker dispatch credentials are missing")
	}
	conn, err := grpc.NewClient(
		worker.Address,
		grpc.WithTransportCredentials(transport),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := runtimepb.NewWorkerServiceClient(conn)

	return client.ExecuteJob(ctx, req)
}

// executeJob finalizes normal submission cancellation and retry exhaustion.
// The caller must hold claimJob ownership for spec.JobID until this returns.
// Ownership stays at the entry point; this method neither claims nor releases it.
func (s *CoordinatorServer) executeJob(
	ctx context.Context,
	spec JobSpec,
) (*runtimepb.SubmitJobResponse, error) {
	s.mu.Lock()
	initialAttempt := s.jobAttempts[spec.JobID]
	s.mu.Unlock()

	result, err := s.executeAttempts(ctx, spec, metrics.SourceSubmit, true)
	// An accepted terminal result wins if cancellation arrives during/after Save.
	if err == nil {
		return result.submitResponse(), nil
	}
	var exhausted *retryExhaustionError
	if ctx.Err() == nil && errors.As(err, &exhausted) {
		persisted, persistenceErr := s.persistSubmitFailure(ctx, spec.JobID, initialAttempt, exhausted.attemptID)
		if persisted {
			// A committed Failed wins subsequent cancellation, despite the RPC error.
			metrics.Resolve(s.metrics).JobTerminal(metrics.SourceSubmit, metrics.TerminalFailed)
			return nil, err
		}
		// Only the helper's direct context error selects cancellation here;
		// wrapped storage errors remain persistence failures even after cancel.
		if persistenceErr != nil && persistenceErr != ctx.Err() {
			log.Printf("submit failure persistence failed: job=%d attempt=%d err=%v", spec.JobID, exhausted.attemptID, persistenceErr)
			return nil, status.Errorf(jobStoreErrorCode(persistenceErr), "failed to finalize job %d attempt %d: %v (execution: %v)",
				spec.JobID, exhausted.attemptID, persistenceErr, err)
		}
	}
	contextErr := ctx.Err()
	if contextErr == nil {
		return nil, err
	}
	persisted, persistenceErr := s.persistSubmitCancellation(spec.JobID, initialAttempt)
	if persistenceErr != nil {
		// A cancelled RPC client may no longer receive the returned store error.
		log.Printf("submit cancellation persistence failed: job=%d err=%v", spec.JobID, persistenceErr)
		return nil, errors.Join(contextErr, fmt.Errorf("persist cancellation for job %d: %w", spec.JobID, persistenceErr))
	}
	if persisted {
		metrics.Resolve(s.metrics).JobTerminal(metrics.SourceSubmit, metrics.TerminalCancelled)
	}
	return nil, contextErr
}

func (s *CoordinatorServer) executeJobFromSource(
	ctx context.Context,
	spec JobSpec,
	source metrics.ExecutionSource,
) (*runtimepb.SubmitJobResponse, error) {
	result, err := s.executeAttempts(ctx, spec, source, false)
	if err != nil {
		return nil, err
	}
	return result.submitResponse(), nil
}

// submitResponse is a compatibility adapter at the synchronous entrypoints.
// The attempt engine and session loop exchange durable result data, not RPC responses.
func (r *acceptedJobResult) submitResponse() *runtimepb.SubmitJobResponse {
	return &runtimepb.SubmitJobResponse{
		JobId: r.record.JobID, AttemptId: r.record.AttemptID, Status: r.workerStatus,
		Output: r.record.Output, Error: r.record.Error,
	}
}

// executeAttempts owns the bounded retry policy, not per-attempt execution or
// entrypoint finalization. firstAdmission is true only for a new client submission;
// retry/recovery retain their existing generation publication path. Its caller
// holds the JobID claim throughout and supplies the session context.
func (s *CoordinatorServer) executeAttempts(
	ctx context.Context,
	spec JobSpec,
	source metrics.ExecutionSource,
	firstAdmission bool,
) (*acceptedJobResult, error) {
	start := attemptSessionStart{mode: continueGeneration}
	if firstAdmission {
		start.mode = createRunningJob
	}
	result := s.executeAttemptSession(ctx, spec, source, start)
	return result.terminal, result.err
}

// The queued first attempt uses the same two-attempt loop as Submit/recovery.
// This trace retains both confirmed and uncertain publication provenance for
// the internal owner, without changing the existing entrypoints' error policy.
type firstPublicationMode uint8

const (
	continueGeneration firstPublicationMode = iota
	createRunningJob
	startPersistedQueuedJob
)

type attemptSessionStart struct {
	mode   firstPublicationMode
	queued JobRecord // exact durable snapshot; only used by startPersistedQueuedJob
}

type attemptSessionResult struct {
	terminal           *acceptedJobResult
	lastPublished      *JobRecord
	pendingPublication *JobRecord
	err                error
}

func (s *CoordinatorServer) executeAttemptSession(
	ctx context.Context, spec JobSpec, source metrics.ExecutionSource, start attemptSessionStart,
) (session attemptSessionResult) {
	if err := ctx.Err(); err != nil {
		session.err = err
		return session
	}
	if err := validateJobSpec(spec); err != nil {
		session.err = err
		return session
	}

	const maxAttempts = 2
	leaseDuration := s.leaseDuration
	if leaseDuration <= 0 {
		leaseDuration = defaultAttemptLease
	}

	var lastErr error
	var lastPublishedAttempt int64

	for i := 0; i < maxAttempts; i++ {
		if err := ctx.Err(); err != nil {
			session.err = err
			return session
		}

		worker, ok := s.getNextWorker()
		if !ok {
			break
		}

		var attemptID int64
		var err error
		if i == 0 && start.mode == startPersistedQueuedJob {
			attemptID, err = s.startQueuedAttempt(start.queued, worker.ID, leaseDuration)
		} else if i == 0 && start.mode == createRunningJob {
			attemptID, err = s.admitJobAttempt(spec, worker.ID, leaseDuration)
		} else {
			attemptID, err = s.startAttemptFromSource(
				spec.JobID, worker.ID, leaseDuration,
				spec.TaskType, spec.Payload, spec.TimeoutMs, source,
			)
		}
		if err != nil {
			var publication *attemptPublicationError
			if errors.As(err, &publication) {
				candidate := publication.record
				session.pendingPublication = &candidate
			}
			session.err = status.Errorf(
				jobStoreErrorCode(err),
				"failed to start attempt for job %d: %v",
				spec.JobID,
				err,
			)
			return session
		}
		lastPublishedAttempt = attemptID
		session.lastPublished = &JobRecord{JobID: spec.JobID, State: JobRunning,
			AttemptID: attemptID, WorkerID: worker.ID, TaskType: spec.TaskType,
			Payload: spec.Payload, TimeoutMs: spec.TimeoutMs}

		outcome := s.executeAuthorizedAttempt(ctx, authorizedAttempt{
			spec: spec, worker: worker,
			attemptID: attemptID, leaseDuration: leaseDuration, source: source,
		})
		switch outcome.kind {
		case attemptAccepted:
			session.terminal = outcome.terminal
			return session
		case attemptRPCFailed, attemptLeaseExpired, attemptFenced:
			lastErr = outcome.err
		case attemptCancelled, attemptPersistenceFailed, attemptInvalidStatus:
			session.err = outcome.err
			return session
		default:
			session.err = status.Errorf(codes.Internal, "unknown outcome for job %d attempt %d", outcome.jobID, outcome.attemptID)
			return session
		}
	}

	if err := ctx.Err(); err != nil {
		session.err = err
		return session
	}
	if lastErr != nil {
		session.err = &retryExhaustionError{
			attemptID: lastPublishedAttempt,
			cause:     status.Errorf(codes.Unavailable, "job submission failed: %v", lastErr),
		}
		return session
	}
	session.err = status.Error(codes.Unavailable, "no alive workers available")
	return session
}

// persistTerminalResult returns eligibility at the final check, plus any context
// or Save error. An ineligible result with no error is fenced; one with an error
// is cancelled. Eligible results can still fail persistence.
//
// As in publishAttempt, s.mu excludes local generation/lease changes through
// Save. JobStore must not reenter the coordinator while saving. Verification
// and metrics run outside this critical section. Time and cancellation can
// still advance during Save; this is not a commit-before-deadline guarantee.
// A Save error leaves the commit outcome uncertain; it does not prove rollback.
func (s *CoordinatorServer) persistTerminalResult(ctx context.Context, record JobRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !s.isLeaseCurrentLocked(record.JobID, record.AttemptID) {
		return false, nil
	}
	if s.jobStore != nil {
		return true, s.jobStore.Save(record)
	}
	return true, nil
}
