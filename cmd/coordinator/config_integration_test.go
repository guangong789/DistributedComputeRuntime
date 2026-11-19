package main

import (
	"context"
	"flag"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	"github.com/guangong789/DistributedComputeRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConfiguredLeaseAllowsControlledTaskOverMTLS(t *testing.T) {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	execution := coordinatorExecutionFlags(fs)
	if err := fs.Parse([]string{"-listen", "127.0.0.1:0", "-attempt-lease", "45s"}); err != nil {
		t.Fatal(err)
	}
	f := newMTLSCoordinator(t, *execution)
	const payload = "lease test"
	release, entered := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return executeHashJob(ctx, req)
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	client := f.observingClient(t, testpki.Client)
	parent, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWaiting()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	done := make(chan workerFailureResult, 1)
	t.Cleanup(func() { unblock(); cancel() })
	const jobID int64 = 11201
	// Check plaintext refusal on this listener before the authenticated Job
	// starts the recovery scheduler, whose metrics can change the snapshot.
	plain, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	plainCtx, stopPlain := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stopPlain()
	snapshot := f.snapshot()
	if response, err := runtimepb.NewCoordinatorServiceClient(plain).SubmitJob(plainCtx, &runtimepb.SubmitJobRequest{JobId: jobID + 1, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}); err == nil || response != nil {
		t.Fatal("configured production listener accepted plaintext")
	}
	f.assertUnchanged(t, snapshot, jobID+1)
	before := time.Now()
	go func() {
		response, err := submitAndObserveForTest(parent, client, &runtimepb.SubmitJobRequest{
			JobId: jobID, TaskType: "hash", Payload: payload, TimeoutMs: 40000,
		})
		done <- workerFailureResult{resp: response, err: err}
	}()
	select {
	case <-entered:
	case <-waitCtx.Done():
		t.Fatal("configured request did not enter the Worker")
	}
	lease, ok := f.s.getLease(jobID)
	if !ok || lease.AttemptID != 1 || lease.WorkerID != "worker-a" ||
		lease.ExpiresAt.Before(before.Add(45*time.Second)) || lease.ExpiresAt.After(time.Now().Add(45*time.Second)) {
		t.Fatal("configured 45-second lease did not reach actual publication")
	}
	// A controlled Worker call outlasts this deliberately shorter budget. The
	// channels enforce ordering; no five- or forty-second wall-clock wait is needed.
	shortDeadline := time.Now().Add(50 * time.Millisecond)
	timer := time.NewTimer(time.Until(shortDeadline))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-waitCtx.Done():
		t.Fatal("controlled Worker interval did not finish")
	}
	select {
	case result := <-done:
		t.Fatalf("blocked execution returned early: %v", result.err)
	default:
	}
	if parent.Err() != nil || !f.s.isLeaseCurrent(jobID, 1) ||
		f.observer.count("terminal", "") != 0 {
		t.Fatal("task was accepted before completion or its configured budget expired")
	}
	unblock()
	var result workerFailureResult
	select {
	case result = <-done:
	case <-waitCtx.Done():
		t.Fatal("configured task did not complete")
	}
	if result.err != nil || result.resp.GetStatus() != "Succeeded" || result.resp.GetAttemptId() != 1 || calls.Load() != 1 {
		t.Fatalf("configured task response=%v error=%v", result.resp, result.err)
	}
	if result.resp.Output != hashResult(payload) {
		t.Fatal("response did not contain the expected hash")
	}
	want := JobRecord{JobID: jobID, State: JobSucceeded, AttemptID: 1, WorkerID: "worker-a",
		TaskType: "hash", Payload: payload, TimeoutMs: 40000}
	running := want
	running.State = JobRunning
	want = withWorkerResult(want, result.resp.Output, result.resp.Error)
	assertAdmittedWorkerFailureSaves(t, f.store, []JobRecord{running, want})
	assertRecoveryRecord(t, f.store, want)
	if f.observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 {
		t.Fatal("completion duplicated success metrics")
	}
	assertRecoveryOwnershipReleased(t, f.s, jobID)

}

func TestCoordinatorDefaultLeaseForDirectServices(t *testing.T) {
	s := newTestCoordinator() // Zero leaseDuration retains the existing fallback.
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	before := time.Now()
	response, err := s.executeSubmissionForTest(context.Background(), &runtimepb.SubmitJobRequest{JobId: 11202, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	if err != nil || response.GetStatus() != "Succeeded" {
		t.Fatal(err)
	}
	lease, ok := s.getLease(11202)
	if !ok || lease.ExpiresAt.Before(before.Add(5*time.Second)) || lease.ExpiresAt.After(time.Now().Add(5*time.Second)) {
		t.Fatal("direct Coordinator construction changed its five-second lease default")
	}
}
