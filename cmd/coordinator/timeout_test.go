package main

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCoordinatorTimeoutValidationBeforeSideEffects(t *testing.T) {
	f := newMTLSCoordinator(t)
	var calls atomic.Int32
	address := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	client := f.observingClient(t, testpki.Client)
	for _, ms := range []int64{0, -1, math.MinInt64, 9223372036855, 1 << 58, 1<<58 + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(ms), func(t *testing.T) {
			before := f.snapshot()
			resp, err := submitAndObserveForTest(mtlsContext(t), client, &runtimepb.SubmitJobRequest{JobId: 12101, TaskType: "hash", Payload: "hello", TimeoutMs: ms})
			if resp != nil || status.Code(err) != codes.InvalidArgument {
				t.Fatalf("timeout=%d response=%v error=%v", ms, resp, err)
			}
			f.assertUnchanged(t, before, 12101)
		})
	}
	// Validation must also precede ownership conflict, without releasing its owner.
	if !f.s.claimJob(12101) {
		t.Fatal("cannot preclaim")
	}
	before := f.snapshot()
	if _, err := submitAndObserveForTest(mtlsContext(t), client, &runtimepb.SubmitJobRequest{JobId: 12101, TimeoutMs: 0}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("ownership preceded validation")
	}
	f.assertUnchanged(t, before, 12101)
	f.s.releaseJob(12101)
	if calls.Load() != 0 {
		t.Fatal("invalid timeout dispatched")
	}
	for _, ms := range []int64{1, 5000, 9223372036854} {
		resp, err := submitAndObserveForTest(mtlsContext(t), client, &runtimepb.SubmitJobRequest{JobId: ms, TaskType: "hash", Payload: "hello", TimeoutMs: ms})
		if err != nil || resp.GetStatus() != "Succeeded" || resp.GetAttemptId() != 1 {
			t.Fatalf("valid timeout=%d response=%v error=%v", ms, resp, err)
		}
		record, ok, err := f.store.Load(ms)
		if err != nil || !ok || record.TimeoutMs != ms {
			t.Fatal("valid timeout not preserved")
		}
	}
}

func TestInvalidLegacyTimeoutPreservedAndRecoveryBacksOff(t *testing.T) {
	for _, ms := range []int64{0, -1, 9223372036855, 1 << 58, 1<<58 + 1} {
		t.Run(fmt.Sprint(ms), func(t *testing.T) {
			store := newWorkerFailureStore(t)
			running := JobRecord{JobID: 12110, State: JobRunning, AttemptID: 7, WorkerID: "old", TaskType: "hash", Payload: "hello", TimeoutMs: ms}
			recovering := running
			recovering.JobID, recovering.State = 12111, JobRecovering
			saveRecoveryRecord(t, store, running)
			saveRecoveryRecord(t, store, recovering)
			s := &CoordinatorServer{jobStore: store}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if err := s.recoverRunningJobs(); err != nil {
				t.Fatal(err)
			}
			if err := s.retryRecoveringJob(context.Background(), recovering); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid persisted timeout: %v", err)
			}
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			for i, delay := range []time.Duration{time.Second, 2 * time.Second} {
				if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
					t.Fatal(err)
				}
				e := q.entries[recovering.JobID]
				if e.generation != 7 || e.failures != uint32(i+1) || !e.nextRetry.Equal(clock.now().Add(delay)) {
					t.Fatalf("invalid spec must retain generation and bounded cooldown: %+v", e)
				}
				if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
					t.Fatal(err)
				}
				if q.entries[recovering.JobID] != e {
					t.Fatal("early scan bypassed cooldown")
				}
				clock.advance(delay)
			}
			assertRecoveryRecord(t, store, running)
			assertRecoveryRecord(t, store, recovering)
			if calls.Load() != 0 || len(store.savedRecords()) != 2 || len(s.jobAttempts) != 0 || len(s.leases) != 0 || s.nextWorker != 0 || len(s.activeJobs) != 0 {
				t.Fatal("invalid legacy timeout was overwritten, dispatched or allocated an attempt")
			}
		})
	}
}

// Simulate a task that returns successful output only after its deadline.
type heldHashTask struct {
	payload string
	release <-chan struct{}
}

func (task heldHashTask) Execute(context.Context) (string, error) {
	<-task.release
	return hashResult(task.payload), nil
}

func TestCoordinatorTaskDeadlineCancelledWithoutRetryAndResubmits(t *testing.T) {
	f := newMTLSCoordinator(t)
	const payload = "deadline test"
	var calls atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	taskContexts := make(chan context.Context, 1)
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if calls.Add(1) == 1 {
			// Keep the RPC alive while the shorter Worker task budget expires.
			timeout, err := runtime.TaskTimeoutFromMilliseconds(req.TimeoutMs)
			if err != nil {
				return nil, err
			}
			taskCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			taskContexts <- taskCtx
			result := runtime.ExecuteJob(taskCtx, runtime.Job{ID: int(req.JobId), Task: heldHashTask{payload: req.Payload, release: release}})
			return &runtimepb.ExecuteJobResponse{JobId: req.JobId, AttemptId: req.AttemptId,
				Status: result.Status.String(), Output: result.Output, Error: result.Err.Error()}, nil
		}
		return executeHashJob(ctx, req)
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	ctx := mtlsContext(t)
	done := make(chan workerFailureResult, 1)
	submissionFinished := make(chan struct{})
	go func() {
		defer close(submissionFinished)
		r, err := f.s.executeSubmissionForTest(ctx, &runtimepb.SubmitJobRequest{JobId: 12120, TaskType: "hash", Payload: payload, TimeoutMs: 250})
		done <- workerFailureResult{r, err}
	}()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-submissionFinished:
		case <-time.After(5 * time.Second):
			t.Error("submission did not finish after releasing the held task")
		}
	})
	var taskCtx context.Context
	select {
	case taskCtx = <-taskContexts:
	case <-ctx.Done():
		t.Fatal("task context not published")
	}
	select {
	case <-taskCtx.Done():
	case <-ctx.Done():
		t.Fatal("task deadline did not expire")
	}
	select {
	case <-done:
		t.Fatal("deadline falsely completed synchronous execution")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case result := <-done:
		if result.err != nil || result.resp.GetStatus() != "Cancelled" || result.resp.GetAttemptId() != 1 || result.resp.GetOutput() != "" || result.resp.GetError() != context.DeadlineExceeded.Error() {
			t.Fatalf("task timeout changed terminal mapping: %+v error=%v", result.resp, result.err)
		}
	case <-ctx.Done():
		t.Fatal("late task did not finish")
	}
	want := JobRecord{JobID: 12120, State: JobCancelled, AttemptID: 1, WorkerID: "worker-a", TaskType: "hash", Payload: payload, TimeoutMs: 250, HasWorkerResult: true, Error: context.DeadlineExceeded.Error()}
	assertRecoveryRecord(t, f.store, want)
	if calls.Load() != 1 || f.observer.count("started", "") != 1 {
		t.Fatal("task Cancelled was retried or double-counted")
	}
	assertRecoveryOwnershipReleased(t, f.s, want.JobID)
	// A changed budget is now a spec conflict and must preserve the Cancelled result.
	r, err := f.s.executeSubmissionForTest(ctx, &runtimepb.SubmitJobRequest{JobId: want.JobID, TaskType: want.TaskType, Payload: want.Payload, TimeoutMs: 5000})
	if r != nil || status.Code(err) != codes.AlreadyExists || calls.Load() != 1 {
		t.Fatalf("changed-budget resubmission: response=%v error=%v", r, err)
	}
	assertRecoveryRecord(t, f.store, want)
	assertRecoveryOwnershipReleased(t, f.s, want.JobID)
	// An equal-spec resubmission must also preserve the terminal generation.
	r, err = f.s.executeSubmissionForTest(ctx, &runtimepb.SubmitJobRequest{JobId: want.JobID, TaskType: want.TaskType, Payload: want.Payload, TimeoutMs: want.TimeoutMs})
	if r != nil || status.Code(err) != codes.AlreadyExists || calls.Load() != 1 {
		t.Fatalf("terminal duplicate: response=%v error=%v", r, err)
	}
	assertRecoveryRecord(t, f.store, want)
	assertRecoveryOwnershipReleased(t, f.s, want.JobID)
	// A different logical Job may run with a new relative execution budget.
	r, err = f.s.executeSubmissionForTest(ctx, &runtimepb.SubmitJobRequest{JobId: want.JobID + 1, TaskType: want.TaskType, Payload: want.Payload, TimeoutMs: 5000})
	if err != nil || r.GetStatus() != "Succeeded" || r.GetAttemptId() != 1 || calls.Load() != 2 || r.GetOutput() != hashResult(payload) {
		t.Fatalf("new job: response=%v error=%v", r, err)
	}
	newJob := withWorkerResult(want, r.Output, r.Error)
	newJob.JobID, newJob.State, newJob.TimeoutMs = want.JobID+1, JobSucceeded, 5000
	assertRecoveryRecord(t, f.store, newJob)
	assertRecoveryRecord(t, f.store, want)
	if f.observer.count("terminal", string(metrics.TerminalCancelled)) != 1 || f.observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 {
		t.Fatal("unexpected terminal classification")
	}
}
