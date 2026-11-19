package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// executeSubmissionForTest retains the old entrypoint's setup around executeJob
// for focused execution/finalization fault tests. It is NOT the SubmitJob RPC:
// public admission is covered separately, and no production path calls this.
// Keeping these tests on the internal synchronous engine preserves exact error,
// cancellation and lost-ack assertions that an admission response cannot carry.
func (s *CoordinatorServer) executeSubmissionForTest(ctx context.Context, req *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	spec := jobSpecFromRequest(req)
	if err := validateJobSpec(spec); err != nil {
		return nil, err
	}
	if s.jobStore == nil {
		return nil, status.Error(codes.FailedPrecondition, errJobStoreNotConfigured.Error())
	}
	if err := s.checkExistingSubmission(spec); err != nil {
		return nil, status.Errorf(jobStoreErrorCode(err), "submit job %d: %v", req.JobId, err)
	}
	owned := s.claimJob(req.JobId)
	if owned {
		defer s.releaseJob(req.JobId)
	}
	if err := s.checkExistingSubmission(spec); err != nil {
		return nil, status.Errorf(jobStoreErrorCode(err), "submit job %d: %v", req.JobId, err)
	}
	if !owned {
		return nil, status.Errorf(codes.Aborted, "job %d has a local session in progress; durable admission is not confirmed", req.JobId)
	}
	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	defer func() { observer.ObserveJobExecution(metrics.SourceSubmit, time.Since(start)) }()
	observer.JobSubmitted(metrics.SourceSubmit)
	return s.executeJob(ctx, spec)
}

// Register cleanup at fixture construction, before tests add blocking gates.
// Start lazily, after all hooks are configured; no second production controller
// exists. A test may also run a manually controlled recovery loop instead.
func prepareQueuedTestScheduler(t *testing.T, s *CoordinatorServer) func() {
	t.Helper()
	// These fixtures test execution/admission invariants rather than SQLite
	// connection contention. Serialize their concurrent observation and writes;
	// production settings and explicit default-configuration BUSY tests stay intact.
	switch store := s.jobStore.(type) {
	case *SQLiteJobStore:
		store.db.SetMaxOpenConns(1)
	case *workerFailureStore:
		if db, ok := store.JobStore.(*SQLiteJobStore); ok {
			db.db.SetMaxOpenConns(1)
		}
	}
	if s.lifecycle == nil {
		s.lifecycle = newCoordinatorLifecycle()
	}
	if s.recoveryReady == nil {
		s.recoveryReady = make(chan struct{}, 1)
	}
	ctx, cancel := context.WithCancel(s.lifecycle.ctx)
	var once sync.Once
	var started atomic.Bool
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		if !started.Load() {
			return
		}
		s.lifecycle.beginShutdown()
		s.lifecycle.cancelWork()
		<-done
		<-s.lifecycle.done
		s.dispatchWG.Wait()
	})
	return func() {
		once.Do(func() { started.Store(true); go func() { defer close(done); s.runRecoveryLoop(ctx) }() })
	}
}

type schedulingTestClient struct {
	runtimepb.CoordinatorServiceClient
	start   func()
	service *CoordinatorServer
}

type directSnapshotTestClient struct {
	runtimepb.CoordinatorServiceClient
	service *CoordinatorServer
}

func (c directSnapshotTestClient) GetJob(ctx context.Context, req *runtimepb.GetJobRequest, _ ...grpc.CallOption) (*runtimepb.GetJobResponse, error) {
	return c.service.GetJob(ctx, req)
}

func waitDirectTerminalForTest(t *testing.T, ctx context.Context, s *CoordinatorServer, id int64) *runtimepb.SubmitJobResponse {
	t.Helper()
	result, err := observeTerminalForTest(ctx, directSnapshotTestClient{service: s}, id)
	if err == nil {
		err = waitExecutionReleaseForTest(ctx, s, id)
	}
	if err != nil {
		t.Fatalf("observe job %d: %v", id, err)
	}
	return result
}

func queuedAdmissionRecord(r JobRecord) JobRecord {
	return JobRecord{JobID: r.JobID, State: JobQueued, TaskType: r.TaskType, Payload: r.Payload, TimeoutMs: r.TimeoutMs}
}

// submitAndObserveForTest is only a test migration helper: verify the actual
// admission acknowledgement, then independently query GetJob until terminal.
// It never resubmits, converts a Job failure into an RPC error, or retries a
// read failure. Returning the old result shape keeps execution assertions small.
func submitAndObserveForTest(ctx context.Context, client runtimepb.CoordinatorServiceClient, req *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
	ack, err := client.SubmitJob(ctx, req)
	if err != nil {
		return nil, err
	}
	if ack.JobId != req.JobId || ack.Status != "Accepted" || ack.AttemptId != 0 || ack.Output != "" || ack.Error != "" {
		return nil, fmt.Errorf("invalid admission acknowledgement: %v", ack)
	}
	if local, ok := client.(*schedulingTestClient); ok {
		local.start()
	}
	result, err := observeTerminalForTest(ctx, client, req.JobId)
	if err == nil {
		if local, ok := client.(*schedulingTestClient); ok && local.service != nil {
			err = waitExecutionReleaseForTest(ctx, local.service, req.JobId)
		}
	}
	return result, err
}

func waitExecutionReleaseForTest(ctx context.Context, s *CoordinatorServer, id int64) error {
	for {
		s.mu.Lock()
		_, active := s.activeJobs[id]
		s.mu.Unlock()
		if !active {
			return nil
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func observeTerminalForTest(ctx context.Context, client runtimepb.CoordinatorServiceClient, id int64) (*runtimepb.SubmitJobResponse, error) {
	for {
		snapshot, err := client.GetJob(ctx, &runtimepb.GetJobRequest{JobId: id})
		if err != nil {
			return nil, err
		}
		var state string
		switch snapshot.State {
		case runtimepb.JobState_JOB_STATE_SUCCEEDED:
			state = "Succeeded"
		case runtimepb.JobState_JOB_STATE_FAILED:
			state = "Failed"
		case runtimepb.JobState_JOB_STATE_CANCELLED:
			state = "Cancelled"
		}
		if state != "" {
			return &runtimepb.SubmitJobResponse{JobId: snapshot.JobId, Status: state, AttemptId: snapshot.AttemptId, Output: snapshot.Output, Error: snapshot.Error}, nil
		}
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, status.FromContextError(ctx.Err()).Err()
		case <-timer.C:
		}
	}
}
