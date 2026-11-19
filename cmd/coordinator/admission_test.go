package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func admissionRequest(r JobRecord) *runtimepb.SubmitJobRequest {
	return &runtimepb.SubmitJobRequest{JobId: r.JobID, TaskType: r.TaskType, Payload: r.Payload, TimeoutMs: r.TimeoutMs}
}

func TestSubmitExistingJobDoesNotExecute(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, state := range []JobState{JobQueued, JobRunning, JobRecovering, JobSucceeded, JobFailed, JobCancelled} {
			for _, owned := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/state=%d/owned=%v", backend, state, owned), func(t *testing.T) {
					inner := newCreationTestStore(t, backend)
					original := creationTestRecord()
					original.State = state
					if state == JobQueued {
						original = queuedAdmissionRecord(original)
					}
					if state == JobRunning || state == JobRecovering {
						original.HasWorkerResult, original.Output, original.Error = false, "", ""
					}
					if created, err := inner.Create(original); !created || err != nil {
						t.Fatalf("seed: created=%v error=%v", created, err)
					}
					var creates, saves, calls atomic.Int32
					store := &recoveryPassStore{JobStore: inner,
						create: func(r JobRecord) (bool, error) { creates.Add(1); return inner.Create(r) },
						save:   func(r JobRecord) error { saves.Add(1); return inner.Save(r) },
					}
					s := &CoordinatorServer{jobStore: store, jobAttempts: map[int64]int64{original.JobID: original.AttemptID}}
					s.setLease(original.JobID, original.AttemptID, original.WorkerID, time.Hour)
					lease, _ := s.getLease(original.JobID)
					if owned {
						if !s.claimJob(original.JobID) {
							t.Fatal("claim failed")
						}
						defer s.releaseJob(original.JobID)
					}
					registerTestWorker(t, s, "unused", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
						calls.Add(1)
						return successfulResult(r), nil
					})
					for _, change := range []string{"same", "TaskType", "Payload", "TimeoutMs"} {
						req := admissionRequest(original)
						switch change {
						case "TaskType":
							req.TaskType = "sleep"
						case "Payload":
							req.Payload = "different"
						case "TimeoutMs":
							req.TimeoutMs++
						}
						resp, err := s.SubmitJob(context.Background(), req)
						message := errJobAlreadyAdmitted.Error()
						if change != "same" {
							message = ErrJobSpecConflict.Error()
						}
						if resp != nil || status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), message) {
							t.Fatalf("%s duplicate: response=%v error=%v", change, resp, err)
						}
						assertRecoveryRecord(t, inner, original)
					}
					if creates.Load() != 0 || saves.Load() != 0 || calls.Load() != 0 {
						t.Fatalf("duplicate wrote/dispatched: creates=%d saves=%d calls=%d", creates.Load(), saves.Load(), calls.Load())
					}
					gotLease, _ := s.getLease(original.JobID)
					if gotLease != lease || s.jobAttempts[original.JobID] != original.AttemptID {
						t.Fatal("duplicate changed the existing attempt or lease")
					}
					if owned {
						if s.claimJob(original.JobID) {
							t.Fatal("duplicate released the winner/recovery owner's claim")
						}
					} else {
						assertRecoveryOwnershipReleased(t, s, original.JobID)
					}
				})
			}
		}
	}
}

func TestConcurrentSubmitCreatesOneExecution(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			inner := newCreationTestStore(t, backend)
			if db, ok := inner.(*SQLiteJobStore); ok {
				db.db.SetMaxOpenConns(1) // Exercise submission races without unrelated BUSY failures.
			}
			var creates, saves, calls atomic.Int32
			s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner,
				create: func(r JobRecord) (bool, error) { creates.Add(1); return inner.Create(r) },
				save:   func(r JobRecord) error { saves.Add(1); return inner.Save(r) },
			}}
			startScheduler := prepareQueuedTestScheduler(t, s)
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				if r.AttemptId != 1 {
					t.Errorf("first admission used attempt %d", r.AttemptId)
				}
				return successfulResult(r), nil
			})
			const contenders = 16
			start, results := make(chan struct{}), make(chan workerFailureResult, contenders)
			var ready sync.WaitGroup
			ready.Add(contenders)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			req := &runtimepb.SubmitJobRequest{JobId: 13001, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
			for i := 0; i < contenders; i++ {
				go func() {
					ready.Done()
					<-start
					r, err := s.SubmitJob(ctx, req)
					results <- workerFailureResult{r, err}
				}()
			}
			ready.Wait()
			close(start)
			accepted := 0
			for i := 0; i < contenders; i++ {
				r := <-results
				if r.err == nil {
					accepted++
					if r.resp.GetStatus() != "Accepted" || r.resp.GetAttemptId() != 0 || r.resp.Output != "" || r.resp.Error != "" {
						t.Errorf("unexpected acceptance: %v", r.resp)
					}
				} else if r.resp != nil || (status.Code(r.err) != codes.Aborted && status.Code(r.err) != codes.AlreadyExists) {
					t.Errorf("unexpected contender result: %+v", r)
				}
			}
			if accepted != 1 || creates.Load() != 1 || saves.Load() != 0 || calls.Load() != 0 {
				t.Fatalf("accepted=%d creates=%d terminal saves=%d dispatches=%d", accepted, creates.Load(), saves.Load(), calls.Load())
			}
			want := withWorkerResult(JobRecord{JobID: req.JobId, State: JobSucceeded, AttemptID: 1,
				WorkerID: "worker", TaskType: req.TaskType, Payload: req.Payload, TimeoutMs: req.TimeoutMs}, "done", "")
			assertRecoveryRecord(t, inner, queuedAdmissionRecord(want))
			startScheduler()
			waitDirectTerminalForTest(t, ctx, s, req.JobId)
			if creates.Load() != 1 || saves.Load() != 1 || calls.Load() != 1 {
				t.Fatal("duplicate admission dispatched more than one execution")
			}
			assertRecoveryRecord(t, inner, want)
			t.Logf("%d concurrent requests: one Create, one dispatch, one terminal Save", contenders)
		})
	}
}

func TestSubmitLocalClaimBeforeAndAfterAdmission(t *testing.T) {
	inner := NewMemoryJobStore()
	createEntered, createRelease := make(chan struct{}), make(chan struct{})
	workerEntered, workerRelease := make(chan struct{}), make(chan struct{})
	var createOnce, workerOnce sync.Once
	unblockCreate := func() { createOnce.Do(func() { close(createRelease) }) }
	unblockWorker := func() { workerOnce.Do(func() { close(workerRelease) }) }
	store := &recoveryPassStore{JobStore: inner, create: func(r JobRecord) (bool, error) {
		close(createEntered)
		<-createRelease
		return inner.Create(r)
	}}
	s := &CoordinatorServer{jobStore: store}
	startScheduler := prepareQueuedTestScheduler(t, s)
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		close(workerEntered)
		<-workerRelease
		return successfulResult(r), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &runtimepb.SubmitJobRequest{JobId: 13002, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
	result, finished := make(chan workerFailureResult, 1), make(chan struct{})
	t.Cleanup(func() {
		cancel()
		unblockCreate()
		unblockWorker()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("owner did not finish during cleanup")
		}
	})
	go func() { defer close(finished); r, err := s.SubmitJob(ctx, req); result <- workerFailureResult{r, err} }()
	select {
	case <-createEntered:
	case <-ctx.Done():
		t.Fatal("Create did not start")
	}
	contenderCtx, stopContender := context.WithTimeout(context.Background(), time.Second)
	r, err := s.SubmitJob(contenderCtx, req)
	stopContender()
	if r != nil || status.Code(err) != codes.Aborted {
		t.Fatalf("unconfirmed admission must be retryable contention: %v %v", r, err)
	}
	if _, found, _ := inner.Load(req.JobId); found || calls.Load() != 0 {
		t.Fatal("pre-admission contender created or dispatched")
	}
	unblockCreate()
	startScheduler()
	select {
	case <-workerEntered:
	case <-ctx.Done():
		t.Fatal("confirmed owner did not dispatch")
	}
	if r, err := s.SubmitJob(context.Background(), req); r != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatalf("admitted local job: response=%v error=%v", r, err)
	}
	if ctx.Err() != nil || calls.Load() != 1 {
		t.Fatal("loser cancellation disturbed the owner")
	}
	unblockWorker()
	select {
	case outcome := <-result:
		if outcome.err != nil || outcome.resp.GetStatus() != "Accepted" || outcome.resp.GetAttemptId() != 0 {
			t.Fatalf("owner result: %+v", outcome)
		}
	case <-ctx.Done():
		t.Fatal("owner did not finish")
	}
	waitDirectTerminalForTest(t, ctx, s, req.JobId)
}

func TestSubmitFirstCreateConflictDoesNotPublish(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprintf("differentSpec=%v", conflict), func(t *testing.T) {
			inner := NewMemoryJobStore()
			winner := withWorkerResult(JobRecord{JobID: 13003, State: JobSucceeded, AttemptID: 42,
				WorkerID: "winner", TaskType: "hash", Payload: "hello", TimeoutMs: 1000}, "winning output", "winning error")
			if conflict {
				winner.Payload = "different"
			}
			s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, create: func(JobRecord) (bool, error) {
				// A competing writer wins between the read and atomic admission.
				if created, err := inner.Create(winner); !created || err != nil {
					t.Fatalf("competing Create: created=%v error=%v", created, err)
				}
				return false, nil
			}}}
			var calls atomic.Int32
			registerTestWorker(t, s, "unused", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(r), nil
			})
			r, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 13003, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
			message := errJobAlreadyAdmitted.Error()
			if conflict {
				message = ErrJobSpecConflict.Error()
			}
			if r != nil || status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), message) || calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
				t.Fatalf("losing Create published: response=%v error=%v calls=%d", r, err, calls.Load())
			}
			assertRecoveryRecord(t, inner, winner)
			assertRecoveryOwnershipReleased(t, s, winner.JobID)
		})
	}
}

func TestSubmitUncertainCreatePreservedAndRecovered(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%v", backend, commit), func(t *testing.T) {
				var inner JobStore = NewMemoryJobStore()
				path := filepath.Join(t.TempDir(), "jobs.db")
				if backend == "sqlite" {
					db, err := NewSQLiteJobStore(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = db.Close() })
					inner = db
				}
				injected := errors.New("Create acknowledgement unavailable")
				creates := 0
				s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, create: func(r JobRecord) (bool, error) {
					creates++
					if commit {
						if created, err := inner.Create(r); !created || err != nil {
							t.Fatalf("backing Create: created=%v error=%v", created, err)
						}
					}
					return false, injected
				}}}
				var calls atomic.Int32
				worker := func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					return successfulResult(r), nil
				}
				registerTestWorker(t, s, "worker", worker)
				req := &runtimepb.SubmitJobRequest{JobId: 13004, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
				r, err := s.SubmitJob(context.Background(), req)
				if r != nil || status.Code(err) != codes.Internal || !strings.Contains(err.Error(), injected.Error()) || calls.Load() != 0 || creates != 1 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
					t.Fatalf("uncertain Create: response=%v error=%v calls=%d creates=%d", r, err, calls.Load(), creates)
				}
				assertRecoveryOwnershipReleased(t, s, req.JobId)
				queued := JobRecord{JobID: req.JobId, State: JobQueued, TaskType: req.TaskType, Payload: req.Payload, TimeoutMs: req.TimeoutMs}
				if !commit {
					if _, found, err := inner.Load(req.JobId); found || err != nil {
						t.Fatalf("failed insertion: found=%v error=%v", found, err)
					}
					return
				}
				assertRecoveryRecord(t, inner, queued)
				if r, err := s.SubmitJob(context.Background(), req); r != nil || status.Code(err) != codes.AlreadyExists || calls.Load() != 0 || creates != 1 {
					t.Fatalf("uncertain admission retried: %v %v", r, err)
				}
				if backend == "sqlite" {
					if err := inner.(*SQLiteJobStore).Close(); err != nil {
						t.Fatal(err)
					}
					db, err := NewSQLiteJobStore(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = db.Close() })
					inner = db
				}
				fresh := &CoordinatorServer{jobStore: inner}
				startScheduler := prepareQueuedTestScheduler(t, fresh)
				registerTestWorker(t, fresh, "worker", worker)
				if r, err := fresh.SubmitJob(context.Background(), req); r != nil || status.Code(err) != codes.AlreadyExists {
					t.Fatalf("restart duplicate: %v %v", r, err)
				}
				if err := fresh.recoverRunningJobs(); err != nil {
					t.Fatal(err)
				}
				assertRecoveryRecord(t, inner, queued) // Startup does not rewrite Queued.
				if r, err := fresh.SubmitJob(context.Background(), req); r != nil || status.Code(err) != codes.AlreadyExists || calls.Load() != 0 {
					t.Fatalf("client took over recovery: %v %v", r, err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				startScheduler()
				waitDirectTerminalForTest(t, ctx, fresh, req.JobId)
				terminal := withWorkerResult(queued, "done", "")
				terminal.State, terminal.AttemptID, terminal.WorkerID = JobSucceeded, 1, "worker"
				assertRecoveryRecord(t, inner, terminal)
				if r, err := fresh.SubmitJob(context.Background(), req); r != nil || status.Code(err) != codes.AlreadyExists || calls.Load() != 1 {
					t.Fatalf("terminal restarted: %v %v", r, err)
				}
				assertRecoveryRecord(t, inner, terminal)
			})
		}
	}
}

func TestSubmitTerminalDuplicateAcrossSQLiteReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var records []JobRecord
	for _, state := range []JobState{JobSucceeded, JobFailed, JobCancelled} {
		for _, hasResult := range []bool{false, true} {
			r := creationTestRecord()
			r.JobID, r.State = int64(13100+len(records)), state
			if !hasResult {
				r.HasWorkerResult, r.Output, r.Error = false, "", ""
			}
			if created, err := store.Create(r); !created || err != nil {
				t.Fatalf("seed terminal: created=%v error=%v", created, err)
			}
			records = append(records, r)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	fresh := &CoordinatorServer{jobStore: reopened}
	if err := fresh.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	// No Worker is registered. Existing terminal records still return an explicit
	// duplicate, including legacy/coordinator terminal states with no Worker result.
	for _, r := range records {
		response, err := fresh.SubmitJob(context.Background(), admissionRequest(r))
		if response != nil || status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), errJobAlreadyAdmitted.Error()) {
			t.Fatalf("reopened terminal duplicate: response=%v error=%v", response, err)
		}
		assertSQLiteJobRecord(t, reopened, r)
	}
	if len(fresh.jobAttempts) != 0 || len(fresh.leases) != 0 {
		t.Fatal("terminal duplicates published after restart")
	}
}

func TestSubmitCancellationDuringCreate(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(fmt.Sprintf("acknowledged=%v", acknowledged), func(t *testing.T) {
			inner := NewMemoryJobStore()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var creates, saves, calls atomic.Int32
			s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner,
				create: func(r JobRecord) (bool, error) {
					creates.Add(1)
					created, err := inner.Create(r)
					if !created || err != nil {
						t.Fatalf("backing Create: created=%v error=%v", created, err)
					}
					cancel()
					if !acknowledged {
						return false, errors.New("injected lost Create acknowledgement")
					}
					return true, nil
				},
				save: func(r JobRecord) error { saves.Add(1); return inner.Save(r) },
			}}
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(r), nil
			})
			req := &runtimepb.SubmitJobRequest{JobId: 13005, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
			r, err := s.SubmitJob(ctx, req)
			s.dispatchWG.Wait()
			if acknowledged && (err != nil || r.GetStatus() != "Accepted" || r.GetAttemptId() != 0) || !acknowledged && (r != nil || status.Code(err) != codes.Internal) || creates.Load() != 1 || calls.Load() != 0 {
				t.Fatalf("cancellation during Create: response=%v error=%v creates=%d dispatches=%d", r, err, creates.Load(), calls.Load())
			}
			want := JobRecord{JobID: req.JobId, State: JobQueued,
				TaskType: req.TaskType, Payload: req.Payload, TimeoutMs: req.TimeoutMs}
			if saves.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
				t.Fatal("admission published an Attempt or cancelled the logical job")
			}
			assertRecoveryRecord(t, inner, want)
			assertRecoveryOwnershipReleased(t, s, req.JobId)
			if response, err := s.SubmitJob(context.Background(), req); response != nil || status.Code(err) != codes.AlreadyExists {
				t.Fatalf("post-cancellation duplicate: response=%v error=%v", response, err)
			}
			assertRecoveryRecord(t, inner, want)
		})
	}
}
