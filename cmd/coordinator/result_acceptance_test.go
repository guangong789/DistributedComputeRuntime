package main

import (
	"context"
	"errors"
	"flag"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

// Delay the first Worker reply to exercise cancellation and expiration at dispatch.
func TestResultAcceptanceBoundaries(t *testing.T) {
	for _, scenario := range []string{
		"lease expires during dispatch",
		"parent canceled during dispatch",
		"valid before deadline",
	} {
		t.Run(scenario, func(t *testing.T) {
			const jobID int64 = 9301
			const workerID = "acceptance-worker"
			const payload = "acceptance boundary"
			output := hashResult(payload)
			store := newWorkerFailureStore(t) // Records successful writes to real SQLite.
			entered, releaseWorker := make(chan struct{}), make(chan struct{})
			observer := &recordingMetrics{}
			fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
			execution := coordinatorExecutionFlags(fs)
			leaseFlag := "5s"
			if scenario == "lease expires during dispatch" {
				leaseFlag = "250ms"
			}
			if err := fs.Parse([]string{"-attempt-lease", leaseFlag}); err != nil {
				t.Fatal(err)
			}
			if err := execution.validate(); err != nil {
				t.Fatal(err)
			}
			s := &CoordinatorServer{jobStore: store, leaseDuration: execution.AttemptLease, metrics: observer}
			observer.hook = func(event recordedMetric) {
				switch event.name {
				case "started", "finished", "terminal":
					// Reentering here also catches metrics emitted under s.mu.
					s.getLease(jobID)
				}
			}
			var workerCalls atomic.Int32
			registerTestWorker(t, s, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				if workerCalls.Add(1) == 1 {
					close(entered)
					<-releaseWorker
				}
				return &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: output,
				}, nil
			})

			parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			waitCtx, stopWaiting := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopWaiting()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
			finished := make(chan struct{})
			var result workerFailureResult
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("submission did not finish during cleanup")
				}
			})
			go func() {
				defer close(finished)
				result.resp, result.err = s.executeSubmissionForTest(parent, &runtimepb.SubmitJobRequest{
					JobId: jobID, TaskType: "hash", Payload: payload, TimeoutMs: workerFailureTimeoutMs,
				})
			}()
			select {
			case <-entered:
			case <-waitCtx.Done():
				t.Fatal("first request did not reach Worker")
			}
			lease, ok := s.getLease(jobID)
			if !ok || lease.AttemptID != 1 || !time.Now().Before(lease.ExpiresAt) {
				t.Fatalf("test requires dispatch to begin within Attempt 1's real lease: %+v", lease)
			}
			if err := parent.Err(); err != nil {
				t.Fatalf("parent expired before the controlled boundary: %v", err)
			}
			assertWorkerFailureSaves(t, store, []JobRecord{
				workerFailureRecord(jobID, 1, workerID, JobRunning, payload),
			})

			switch scenario {
			case "lease expires during dispatch":
				// Wait for the actual stored deadline; do not mutate the lease or
				// approximate expiry with a sleep measured from test startup.
				timer := time.NewTimer(time.Until(lease.ExpiresAt))
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-waitCtx.Done():
					t.Fatal("lease deadline did not arrive")
				}
				if s.isLeaseCurrent(jobID, 1) {
					t.Fatal("Attempt 1 must be expired before releasing the Worker")
				}
				if err := parent.Err(); err != nil {
					t.Fatalf("lease expiry must be isolated from parent cancellation: %v", err)
				}
			case "parent canceled during dispatch":
				cancel()
				<-parent.Done()
				if !errors.Is(parent.Err(), context.Canceled) || !s.isLeaseCurrent(jobID, 1) {
					t.Fatal("test requires parent cancellation while the lease is still valid")
				}
			}
			release()
			select {
			case <-finished:
			case <-waitCtx.Done():
				t.Fatal("submission did not finish after releasing the Worker")
			}

			record, found, err := store.Load(jobID)
			if err != nil || !found {
				t.Fatalf("load durable outcome: found=%v error=%v", found, err)
			}
			saves := store.savedRecords()
			if !s.claimJob(jobID) {
				t.Error("submission retained JobID ownership")
			} else {
				s.releaseJob(jobID)
			}
			switch scenario {
			case "lease expires during dispatch":
				// A fresh retry may succeed. No write or response may accept the
				// expired first attempt, even if a later write would hide it.
				for _, saved := range saves {
					if saved.AttemptID == 1 && saved.State == JobSucceeded {
						t.Error("accepted expired Attempt 1: SQLite saved JobSucceeded after its lease expired")
					}
				}
				if result.resp != nil && result.resp.AttemptId == 1 && result.resp.Status == "Succeeded" {
					t.Error("returned success for expired Attempt 1")
				}
				want := withWorkerResult(workerFailureRecord(jobID, 2, workerID, JobSucceeded, payload), output, "")
				if result.err != nil || result.resp == nil || result.resp.AttemptId != 2 ||
					result.resp.Status != "Succeeded" || record != want || workerCalls.Load() != 2 {
					t.Errorf("lease-expiry retry: record=%+v response=%v error=%v calls=%d",
						record, result.resp, result.err, workerCalls.Load())
				}
				assertWorkerFailureSaves(t, store, []JobRecord{
					workerFailureRecord(jobID, 1, workerID, JobRunning, payload),
					workerFailureRecord(jobID, 2, workerID, JobRunning, payload), want,
				})
				if observer.count("finished", string(metrics.AttemptLeaseExpired)) != 1 ||
					observer.count("rejected", "") != 0 {
					t.Error("lease expiry must finish one attempt as expired")
				}
			case "parent canceled during dispatch":
				for _, saved := range saves {
					if saved.State == JobSucceeded {
						t.Error("SQLite saved JobSucceeded after parent cancellation was established before the Worker returned")
					}
				}
				if !errors.Is(result.err, context.Canceled) || result.resp != nil {
					t.Errorf("canceled submission returned status=%q attempt=%d error=%v, want nil response and context.Canceled",
						result.resp.GetStatus(), result.resp.GetAttemptId(), result.err)
				}
				if workerCalls.Load() != 1 {
					t.Error("parent cancellation started an extra attempt")
				}
				assertWorkerFailureSaves(t, store, []JobRecord{
					workerFailureRecord(jobID, 1, workerID, JobRunning, payload),
					workerFailureRecord(jobID, 1, workerID, JobCancelled, payload),
				})
				if record.State != JobCancelled || observer.count("terminal", string(metrics.TerminalCancelled)) != 1 {
					t.Error("normal-submit cancellation must durably persist and report Cancelled")
				}
				if observer.count("finished", string(metrics.AttemptCancelled)) != 1 ||
					observer.count("rejected", "") != 0 {
					t.Error("parent cancellation must finish once as cancelled, without fencing/retry")
				}
			case "valid before deadline":
				if parent.Err() != nil || !s.isLeaseCurrent(jobID, 1) {
					t.Fatal("positive control exceeded its parent deadline or lease")
				}
				if result.err != nil || result.resp == nil || result.resp.JobId != jobID ||
					result.resp.AttemptId != 1 || result.resp.Status != "Succeeded" || result.resp.Output != output {
					t.Fatalf("valid result not accepted: response=%v error=%v", result.resp, result.err)
				}
				want := withWorkerResult(workerFailureRecord(jobID, 1, workerID, JobSucceeded, payload), output, "")
				if record != want || workerCalls.Load() != 1 {
					t.Errorf("valid acceptance record=%+v, want %+v with one dispatch", record, want)
				}
				assertWorkerFailureSaves(t, store, []JobRecord{
					workerFailureRecord(jobID, 1, workerID, JobRunning, payload), want,
				})
			}
			wantAttempts, wantTerminals, wantAccepted := 1, 1, 1
			if scenario == "lease expires during dispatch" {
				wantAttempts = 2
			} else if scenario == "parent canceled during dispatch" {
				wantAccepted = 0
			}
			if observer.count("submitted", "") != 1 || observer.count("started", "") != wantAttempts ||
				observer.count("finished", "") != wantAttempts ||
				observer.count("terminal", "") != wantTerminals ||
				observer.count("finished", string(metrics.AttemptAccepted)) != wantAccepted {
				t.Errorf("acceptance events doubled, missing, or emitted for invalid success: %+v", observer.snapshot())
			}
		})
	}
}

// Exercise the gate directly so invalidation occurs immediately before Save,
// rather than being intercepted by the earlier RPC-response check.
func TestPersistTerminalResultRejectsInvalidResult(t *testing.T) {
	for _, terminal := range []struct {
		name  string
		state JobState
	}{
		{"Succeeded", JobSucceeded},
		{"Failed", JobFailed},
		{"Cancelled", JobCancelled},
	} {
		for _, invalidation := range []string{"expired", "superseded", "cancelled", "cancelled and expired"} {
			t.Run(terminal.name+"/"+invalidation, func(t *testing.T) {
				inner := NewMemoryJobStore()
				saves := 0
				s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
					saves++
					return inner.Save(record)
				}}}
				const jobID int64 = 9302
				if !s.claimJob(jobID) {
					t.Fatal("cannot claim job")
				}
				defer s.releaseJob(jobID)
				attempt, err := s.startAttempt(jobID, "worker", time.Hour, "hash", "hello", 1000)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if invalidation == "superseded" {
					if _, err := s.startAttempt(jobID, "new-worker", time.Hour, "hash", "hello", 1000); err != nil {
						t.Fatal(err)
					}
				}
				if invalidation == "expired" || invalidation == "cancelled and expired" {
					s.setLease(jobID, attempt, "worker", -time.Second)
				}
				cancelled := invalidation == "cancelled" || invalidation == "cancelled and expired"
				if cancelled {
					cancel()
				}
				before, ok, err := inner.Load(jobID)
				if err != nil || !ok {
					t.Fatalf("load current record: found=%v error=%v", ok, err)
				}
				record := JobRecord{JobID: jobID, State: terminal.state, AttemptID: attempt, WorkerID: "worker",
					TaskType: "hash", Payload: "hello", TimeoutMs: 1000, HasWorkerResult: true, Output: "rejected output", Error: "rejected error"}
				beforeSaves := saves
				eligible, err := s.persistTerminalResult(ctx, record)
				if eligible || (cancelled && !errors.Is(err, context.Canceled)) ||
					(!cancelled && err != nil) {
					t.Fatalf("invalid result: eligible=%v error=%v", eligible, err)
				}
				after, _, err := inner.Load(jobID)
				if err != nil || after != before || saves != beforeSaves {
					t.Fatalf("invalid terminal changed persistence: before=%+v after=%+v saves=%d/%d error=%v",
						before, after, beforeSaves, saves, err)
				}
			})
		}
	}
}
