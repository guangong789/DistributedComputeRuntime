package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func recoveryReconciliationFixture(t *testing.T) (*CoordinatorServer, JobStore, JobRecord) {
	t.Helper()
	inner := newCreationTestStore(t, "sqlite")
	inner.(*SQLiteJobStore).db.SetMaxOpenConns(1)
	r := recoveringTestRecord()
	saveRecoveryRecord(t, inner, r)
	s := &CoordinatorServer{jobStore: inner, lifecycle: newCoordinatorLifecycle(), recoveryReady: make(chan struct{}, 1)}
	t.Cleanup(func() {
		s.lifecycle.beginShutdown()
		s.lifecycle.cancelWork()
		<-s.lifecycle.done
		s.dispatchWG.Wait()
	})
	return s, inner, r
}

func TestRecoveryUncertainPublicationOnline(t *testing.T) {
	for _, tc := range []struct {
		name      string
		candidate int64
		commit    bool
	}{
		{"first uncommitted", 8, false},
		{"first acknowledgement lost", 8, true},
		{"retry uncommitted", 9, false},
		{"retry acknowledgement lost", 9, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, inner, original := recoveryReconciliationFixture(t)
			var uncertain, healthy atomic.Bool
			var publications, calls atomic.Int32
			s.jobStore = &recoveryPassStore{JobStore: inner,
				save: func(r JobRecord) error {
					if r.JobID == original.JobID && r.State == JobRunning && r.AttemptID == tc.candidate && !healthy.Load() {
						publications.Add(1)
						if tc.commit {
							if err := inner.Save(r); err != nil {
								return err
							}
						}
						uncertain.Store(true)
						return errors.New("publication acknowledgement unavailable")
					}
					return inner.Save(r)
				},
				load: func(id int64) (JobRecord, bool, error) {
					if id == original.JobID && uncertain.Load() && !healthy.Load() {
						return JobRecord{}, false, errors.New("confirmation read unavailable")
					}
					return inner.Load(id)
				}}
			wantAttempt := tc.candidate
			if tc.commit {
				wantAttempt++
			}
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				if req.JobId == original.JobID {
					calls.Add(1)
					if !healthy.Load() {
						if tc.candidate != 9 || req.AttemptId != 8 {
							t.Errorf("unconfirmed dispatch: %+v", req)
						}
						return nil, status.Error(codes.Unavailable, "first confirmed RPC failed")
					}
					if req.AttemptId != wantAttempt {
						t.Errorf("resumed attempt=%d, want %d", req.AttemptId, wantAttempt)
					}
				}
				return successfulResult(req), nil
			})
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			result, err := s.retryRecoveringJobResult(context.Background(), original)
			if err == nil || !result.reconciliationPending || result.generation != tc.candidate {
				t.Fatalf("uncertainty lost: result=%+v err=%v", result, err)
			}
			if err := q.completed(s.jobStore, original, result, err); err != nil {
				t.Fatal(err)
			}
			entry := s.queuedReconciliations[original.JobID]
			if entry == nil || entry.expected.AttemptID != tc.candidate || entry.expected.WorkerID != "worker" ||
				entry.recovering == nil || *entry.recovering != original || entry.cause == nil ||
				(tc.candidate == 8) != (entry.lastPublished == nil) {
				t.Fatalf("lost publication provenance: %+v", entry)
			}
			if tc.candidate == 9 && (entry.lastPublished.AttemptID != 8 || entry.lastPublished.WorkerID != "worker") {
				t.Fatal("lost previous confirmed generation")
			}
			assertQueuedSessionReleased(t, s, original.JobID, true)
			if len(s.leases) != 0 || calls.Load() != int32(tc.candidate-8) {
				t.Fatal("uncertain generation dispatched or retained authorization")
			}
			// An unrelated Recovering row still executes while this obligation
			// is unavailable. The SAME production loop later repairs/resumes it.
			other := original
			other.JobID++
			saveRecoveryRecord(t, inner, other)
			completed := make(chan struct{}, 1)
			s.metrics = &recordingMetrics{hook: func(e recordedMetric) {
				if e.name == "pass" && healthy.Load() {
					r, _, _ := inner.Load(original.JobID)
					if r.State == JobSucceeded && !s.hasQueuedReconciliation(original.JobID) {
						select {
						case completed <- struct{}{}:
						default:
						}
					}
				}
			}}
			stop := startManualRecovery(t, s, q, clock)
			waitRecoveryReset(t, clock, time.Second)
			other.State, other.AttemptID, other.WorkerID = JobSucceeded, 8, "worker"
			other = withWorkerResult(other, "done", "")
			assertRecoveryRecord(t, inner, other)
			healthy.Store(true)
			clock.advance(time.Second)
			waitRecoverySignal(t, completed)
			stop()
			want := original
			want.State, want.AttemptID, want.WorkerID = JobSucceeded, wantAttempt, "worker"
			want = withWorkerResult(want, "done", "")
			assertRecoveryRecord(t, inner, want)
			if publications.Load() != 1 || calls.Load() != int32(tc.candidate-7) {
				t.Fatalf("duplicate execution: publications=%d calls=%d", publications.Load(), calls.Load())
			}
			assertQueuedSessionReleased(t, s, original.JobID, false)
		})
	}
}

func TestRecoveryRepairAcknowledgementLostOnline(t *testing.T) {
	s, inner, original := recoveryReconciliationFixture(t)
	var committed, healthy atomic.Bool
	var repairs, calls atomic.Int32
	s.jobStore = &recoveryPassStore{JobStore: inner,
		recoverRunning: func(r JobRecord) (bool, error) {
			repairs.Add(1)
			ok, err := inner.TryRecoverRunning(r)
			if !ok || err != nil {
				return ok, err
			}
			committed.Store(true)
			return false, errors.New("repair acknowledgement lost")
		},
		load: func(id int64) (JobRecord, bool, error) {
			if committed.Load() && !healthy.Load() {
				return JobRecord{}, false, errors.New("repair confirmation unavailable")
			}
			return inner.Load(id)
		}}
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if !healthy.Load() {
			return nil, status.Error(codes.Unavailable, "RPC failed")
		}
		if req.AttemptId != 10 {
			t.Errorf("resumed attempt=%d, want 10", req.AttemptId)
		}
		return successfulResult(req), nil
	})
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	want := original
	want.AttemptID, want.WorkerID = 9, "worker"
	assertRecoveryRecord(t, inner, want)
	assertQueuedSessionReleased(t, s, original.JobID, true)
	s.runQueuedReconciliationPass(context.Background(), q)
	if !s.hasQueuedReconciliation(original.JobID) || repairs.Load() != 1 {
		t.Fatal("unavailable confirmation lost ownership or repeated uncertain repair")
	}
	healthy.Store(true)
	clock.advance(time.Second)
	s.runQueuedReconciliationPass(context.Background(), q)
	assertRecoveryRecord(t, inner, want)
	if s.hasQueuedReconciliation(original.JobID) || repairs.Load() != 1 {
		t.Fatal("committed repair not confirmed without another write")
	}
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	want.State, want.AttemptID = JobSucceeded, 10
	want = withWorkerResult(want, "done", "")
	assertRecoveryRecord(t, inner, want)
	if calls.Load() != 3 {
		t.Fatalf("dispatches=%d, want two confirmed failures and one resumed attempt", calls.Load())
	}
}

func TestRecoveryReconciliationFencesNewerState(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		name := "newer Running"
		if terminal {
			name = "accepted empty terminal"
		}
		t.Run(name, func(t *testing.T) {
			s, inner, original := recoveryReconciliationFixture(t)
			var uncertain, healthy atomic.Bool
			var calls, repairs atomic.Int32
			advanced := original
			advanced.State, advanced.AttemptID, advanced.WorkerID = JobRunning, 42, "new-owner"
			if terminal {
				advanced.State, advanced.HasWorkerResult = JobSucceeded, true
			}
			s.jobStore = &recoveryPassStore{JobStore: inner,
				save: func(r JobRecord) error {
					if err := inner.Save(r); err != nil {
						return err
					}
					uncertain.Store(true)
					return errors.New("publication acknowledgement lost")
				},
				load: func(id int64) (JobRecord, bool, error) {
					if uncertain.Load() && !healthy.Load() {
						return JobRecord{}, false, errors.New("read unavailable")
					}
					return inner.Load(id)
				},
				recoverRunning: func(expected JobRecord) (bool, error) {
					repairs.Add(1)
					// Advance AFTER settlement's Load, exercising the mutation-time
					// conditional predicate rather than just its initial comparison.
					if err := inner.Save(advanced); err != nil {
						return false, err
					}
					return inner.TryRecoverRunning(expected)
				}}
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if err := s.retryRecoveringJob(context.Background(), original); err == nil {
				t.Fatal("lost acknowledgement reported success")
			}
			healthy.Store(true)
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			s.runQueuedReconciliationPass(context.Background(), q)
			assertRecoveryRecord(t, inner, advanced)
			assertQueuedSessionReleased(t, s, original.JobID, true)
			clock.advance(time.Second)
			s.runQueuedReconciliationPass(context.Background(), q)
			assertRecoveryRecord(t, inner, advanced)
			assertQueuedSessionReleased(t, s, original.JobID, !terminal)
			if calls.Load() != 0 || repairs.Load() != 1 {
				t.Fatal("stale provenance dispatched or overwrote newer state")
			}
		})
	}
}

func TestRecoveryReconciliationHandoffDuringShutdown(t *testing.T) {
	s, inner, original := recoveryReconciliationFixture(t)
	var uncertain atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	faults := &recoveryPassStore{JobStore: inner,
		save: func(r JobRecord) error {
			if err := inner.Save(r); err != nil {
				return err
			}
			uncertain.Store(true)
			return errors.New("publication acknowledgement lost")
		},
		load: func(id int64) (JobRecord, bool, error) {
			if uncertain.Load() {
				close(entered)
				<-release
				return JobRecord{}, false, errors.New("confirmation unavailable")
			}
			return inner.Load(id)
		}}
	store := &shutdownStore{JobStore: faults, closeBacking: inner.(*SQLiteJobStore).Close}
	var seq int
	var name, path string
	if err := inner.(*SQLiteJobStore).db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	s.jobStore = store
	p := newCoordinatorProcess(s, 5*time.Second)
	p.closeStore = store.Close
	p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
	t.Cleanup(func() { unblock(); _ = p.shutdown(); awaitShutdownSignal(t, p.cleanupDone) })
	// Execution observation runs after ledger installation but before the
	// deferred claim release. Inspect that exact handoff boundary under mu.
	handoff := make(chan bool, 1)
	s.metrics = &recordingMetrics{hook: func(e recordedMetric) {
		if e.name == "execution" {
			s.mu.Lock()
			_, active := s.activeJobs[original.JobID]
			pending := s.queuedReconciliations[original.JobID] != nil
			s.mu.Unlock()
			handoff <- active && pending
		}
	}}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	p.background.Add(1)
	go func() { defer p.background.Done(); s.runRecoveryLoop(s.lifecycle.ctx) }()
	awaitShutdownSignal(t, entered)
	if s.claimJob(original.JobID) {
		t.Fatal("claim released before uncertainty handoff")
	}
	done := make(chan error, 1)
	go func() { done <- p.shutdown() }()
	awaitShutdownSignal(t, s.lifecycle.ctx.Done())
	if closed, _, _ := store.snapshot(); closed {
		t.Fatal("store closed during active settlement")
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !<-handoff {
		t.Fatal("ledger installation did not precede claim release")
	}
	assertQueuedSessionReleased(t, s, original.JobID, true)
	entry := s.queuedReconciliations[original.JobID]
	if attempted, err := s.reconcileQueuedJob(context.Background(), original.JobID, entry, newRecoverySchedule()); attempted || !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown admitted reconciliation: attempted=%v err=%v", attempted, err)
	}
	if calls.Load() != 0 {
		t.Fatal("unconfirmed attempt dispatched during shutdown")
	}
	assertShutdownStoreClosed(t, store)
	want := original
	want.State, want.AttemptID, want.WorkerID = JobRunning, 8, "worker"
	loadShutdownRecord(t, path, want)
}
