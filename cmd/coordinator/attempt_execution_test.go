package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAuthorizedAttemptUsesPublishedGeneration(t *testing.T) {
	for _, recovery := range []bool{false, true} {
		name := "first submission"
		if recovery {
			name = "recovery"
		}
		t.Run(name, func(t *testing.T) {
			inner := NewMemoryJobStore()
			observer := &recordingMetrics{}
			s := &CoordinatorServer{jobStore: inner, metrics: observer}
			stored := JobSpec{JobID: 9801, TaskType: "hash", Payload: "input", TimeoutMs: 1000}
			source, workerStatus := metrics.SourceSubmit, "Succeeded"
			if recovery {
				stored.TaskType, stored.Payload = "hash", `  original input  `
				source, workerStatus = metrics.SourceRecovery, "Failed"
				saveRecoveryRecord(t, inner, JobRecord{JobID: stored.JobID, State: JobRecovering,
					AttemptID: 7, WorkerID: "old", TaskType: stored.TaskType, Payload: stored.Payload, TimeoutMs: stored.TimeoutMs})
			}
			if err := validateJobSpec(stored); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			registerTestWorker(t, s, "selected", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				if req.JobId != stored.JobID || req.TaskType != stored.TaskType ||
					req.Payload != stored.Payload || req.TimeoutMs != stored.TimeoutMs {
					t.Errorf("dispatch did not use the validated execution snapshot: %+v", req)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("dispatch lost explicit session deadline")
				}
				return &runtimepb.ExecuteJobResponse{JobId: req.JobId, AttemptId: req.AttemptId, Status: workerStatus}, nil
			})
			if !s.claimJob(stored.JobID) {
				t.Fatal("cannot claim job")
			}
			defer s.releaseJob(stored.JobID)
			worker, _ := s.getNextWorker()
			const leaseDuration = time.Minute
			var id int64
			var err error
			if recovery {
				id, err = s.startAttemptFromSource(stored.JobID, worker.ID, leaseDuration,
					stored.TaskType, stored.Payload, stored.TimeoutMs, source)
			} else {
				id, err = s.admitJobAttempt(stored, worker.ID, leaseDuration)
			}
			if err != nil {
				t.Fatal(err)
			}
			leaseBefore, _ := s.getLease(stored.JobID)
			// Authorization is complete. The execution boundary must neither
			// recreate the record nor allocate a new generation, even for recovery.
			terminalSaves := 0
			s.jobStore = &recoveryPassStore{JobStore: inner,
				create: func(JobRecord) (bool, error) {
					t.Error("execution repeated Create")
					return false, errors.New("unexpected Create")
				},
				startQueued: func(JobRecord, string) (bool, error) {
					t.Error("execution published Queued")
					return false, errors.New("unexpected queued publication")
				},
				save: func(r JobRecord) error {
					if !r.HasWorkerResult {
						t.Error("execution attempted a nonterminal publication/finalization")
						return errors.New("unexpected nonterminal Save")
					}
					terminalSaves++
					return inner.Save(r)
				}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			outcome := s.executeAuthorizedAttempt(ctx, authorizedAttempt{spec: stored,
				worker: worker, attemptID: id, leaseDuration: leaseDuration, source: source})
			s.dispatchWG.Wait()
			wantID, wantState := int64(1), JobSucceeded
			if recovery {
				wantID, wantState = 8, JobFailed
			}
			want := JobRecord{JobID: stored.JobID, State: wantState, AttemptID: wantID, WorkerID: worker.ID,
				TaskType: stored.TaskType, Payload: stored.Payload, TimeoutMs: stored.TimeoutMs, HasWorkerResult: true}
			if outcome.kind != attemptAccepted || outcome.err != nil || outcome.jobID != stored.JobID ||
				outcome.attemptID != wantID || outcome.terminal == nil || outcome.terminal.record != want {
				t.Fatalf("accepted outcome=%+v, want terminal record %+v", outcome, want)
			}
			assertRecoveryRecord(t, inner, want)
			leaseAfter, _ := s.getLease(stored.JobID)
			if calls.Load() != 1 || terminalSaves != 1 || s.jobAttempts[stored.JobID] != wantID || leaseAfter != leaseBefore {
				t.Fatalf("execution republished/retried: calls=%d terminal Saves=%d generation=%d lease=%+v",
					calls.Load(), terminalSaves, s.jobAttempts[stored.JobID], leaseAfter)
			}
			if s.claimJob(stored.JobID) {
				t.Fatal("execution released its caller's session ownership")
			}
			if observer.count("started", "") != 1 || observer.count("finished", "") != 1 ||
				observer.count("terminal", "") != 1 || observer.count("execution", "") != 0 {
				t.Fatalf("attempt/session metrics changed: %+v", observer.snapshot())
			}
		})
	}
}

func TestAuthorizedAttemptLeavesPolicyToSession(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind attemptOutcomeKind
		code codes.Code
	}{
		{"rpc failure", attemptRPCFailed, codes.Unavailable},
		{"terminal write failure", attemptPersistenceFailed, codes.Internal},
		{"terminal acknowledgement lost", attemptPersistenceFailed, codes.Internal},
		{"invalid status", attemptInvalidStatus, codes.Internal},
		{"session cancelled", attemptCancelled, codes.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := NewMemoryJobStore()
			observer := &recordingMetrics{}
			s := &CoordinatorServer{jobStore: inner, metrics: observer}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stored := JobSpec{JobID: 9802, TaskType: "hash", Payload: "input", TimeoutMs: 1000}
			var calls atomic.Int32
			registerTestWorker(t, s, "selected", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				switch tc.name {
				case "rpc failure":
					return nil, status.Error(codes.Unavailable, "injected RPC failure")
				case "invalid status":
					resp := successfulResult(req)
					resp.Status = "invalid"
					return resp, nil
				case "session cancelled":
					cancel()
				}
				return successfulResult(req), nil
			})
			if !s.claimJob(stored.JobID) {
				t.Fatal("cannot claim job")
			}
			defer s.releaseJob(stored.JobID)
			worker, _ := s.getNextWorker()
			id, err := s.admitJobAttempt(stored, worker.ID, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected terminal persistence failure")
			s.jobStore = &recoveryPassStore{JobStore: inner, save: func(r JobRecord) error {
				if tc.kind == attemptPersistenceFailed {
					if tc.name == "terminal acknowledgement lost" {
						if err := inner.Save(r); err != nil {
							return err
						}
					}
					return injected
				}
				t.Error("unexpected finalization Save")
				return inner.Save(r)
			}}
			outcome := s.executeAuthorizedAttempt(ctx, authorizedAttempt{spec: stored,
				worker: worker, attemptID: id, leaseDuration: time.Minute, source: metrics.SourceSubmit})
			s.dispatchWG.Wait()
			code := status.Code(outcome.err)
			if errors.Is(outcome.err, context.Canceled) {
				code = codes.Canceled
			}
			if outcome.kind != tc.kind || outcome.terminal != nil || code != tc.code ||
				outcome.jobID != stored.JobID || outcome.attemptID != 1 {
				t.Fatalf("unsettled outcome=%+v, code=%v", outcome, code)
			}
			if tc.kind == attemptPersistenceFailed && !strings.Contains(outcome.err.Error(), injected.Error()) {
				t.Fatalf("lost persistence diagnostic: %v", outcome.err)
			}
			want := JobRecord{JobID: stored.JobID, State: JobRunning, AttemptID: 1, WorkerID: worker.ID,
				TaskType: stored.TaskType, Payload: stored.Payload, TimeoutMs: stored.TimeoutMs}
			if tc.name == "terminal acknowledgement lost" {
				want.State, want.HasWorkerResult, want.Output = JobSucceeded, true, "done"
			}
			assertRecoveryRecord(t, inner, want)
			if calls.Load() != 1 || s.jobAttempts[stored.JobID] != 1 || observer.count("terminal", "") != 0 ||
				observer.count("finished", "") != 1 {
				t.Fatalf("attempt applied session policy: calls=%d generation=%d events=%+v", calls.Load(), s.jobAttempts[stored.JobID], observer.snapshot())
			}
			if s.claimJob(stored.JobID) {
				t.Fatal("unsettled attempt released its caller's session ownership")
			}
		})
	}
}

func TestSubmitPreservesWorkerStatusSpelling(t *testing.T) {
	for _, state := range []string{"sUcCeEdEd", "FAILED", "cancelled"} {
		t.Run(state, func(t *testing.T) {
			s := newTestCoordinator()
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				return &runtimepb.ExecuteJobResponse{JobId: req.JobId, AttemptId: req.AttemptId, Status: state}, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := s.executeSubmissionForTest(ctx, &runtimepb.SubmitJobRequest{JobId: 9803, TaskType: "hash", Payload: "input", TimeoutMs: 1000})
			if err != nil || resp == nil || resp.Status != state || resp.JobId != 9803 || resp.AttemptId != 1 || resp.Output != "" || resp.Error != "" {
				t.Fatalf("response lost Worker status or empty result: response=%v err=%v", resp, err)
			}
			record, found, err := s.jobStore.Load(9803)
			wantState, _ := jobStateFromStatus(state)
			if err != nil || !found || record.State != wantState || !record.HasWorkerResult || record.Output != "" || record.Error != "" {
				t.Fatalf("empty accepted terminal result=%+v, found=%v err=%v", record, found, err)
			}
		})
	}
}
