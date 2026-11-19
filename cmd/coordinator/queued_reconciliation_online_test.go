package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
)

func TestQueuedReconciliationOnlinePublicationOutcome(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, commit := range []bool{false, true} {
			name := "not committed"
			if commit {
				name = "acknowledgement lost"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				s, inner, queued := queuedSessionFixture(t, backend)
				if db, ok := inner.(*SQLiteJobStore); ok {
					// This tests acknowledgement provenance, not connection lock
					// contention between the newly concurrent producer/session.
					// Default-connection BUSY tests remain separate and unchanged.
					db.db.SetMaxOpenConns(1)
				}
				control := queued
				control.JobID++
				if ok, err := inner.Create(control); !ok || err != nil {
					t.Fatalf("control Create=%v, %v", ok, err)
				}
				if !s.claimJob(control.JobID) {
					t.Fatal("cannot hold unrelated JobID claim")
				}
				defer s.releaseJob(control.JobID)
				s.recoveryReady = make(chan struct{}, 1)
				var failed, healthy atomic.Bool
				var publications, calls atomic.Int32
				s.jobStore = &recoveryPassStore{JobStore: inner,
					startQueued: func(r JobRecord, w string) (bool, error) {
						if r.JobID != queued.JobID || healthy.Load() {
							return inner.TryStartQueued(r, w)
						}
						publications.Add(1)
						if commit {
							if ok, err := inner.TryStartQueued(r, w); !ok || err != nil {
								return ok, err
							}
						}
						failed.Store(true)
						return false, errors.New("publication acknowledgement unavailable")
					},
					load: func(id int64) (JobRecord, bool, error) {
						if failed.Load() && !healthy.Load() {
							return JobRecord{}, false, errors.New("confirmation read unavailable")
						}
						return inner.Load(id)
					}}
				registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					// Recovery freshly authorizes generation 2 after a committed
					// uncertain write; confirmed Queued rows now schedule normally.
					wantAttempt := int64(1)
					if req.JobId == queued.JobID && commit {
						wantAttempt = 2
					}
					if req.AttemptId != wantAttempt {
						t.Errorf("unauthorized dispatch: %+v", req)
					}
					return successfulResult(req), nil
				})
				if err := s.runQueuedJob(queued.JobID); err == nil {
					t.Fatal("uncertain publication reported success")
				}
				assertQueuedSessionReleased(t, s, queued.JobID, true)
				if calls.Load() != 0 || len(s.leases) != 0 || len(s.jobAttempts) != 0 {
					t.Fatal("unconfirmed publication authorized dispatch")
				}
				completed := make(chan struct{}, 1)
				observer := &recordingMetrics{hook: func(e recordedMetric) {
					if e.name != "pass" {
						return
					}
					first, _, _ := inner.Load(queued.JobID)
					if healthy.Load() && !s.hasQueuedReconciliation(queued.JobID) && first.State == JobSucceeded {
						select {
						case completed <- struct{}{}:
						default:
						}
					}
				}}
				s.metrics = observer
				clock := newManualRecoveryClock()
				q := testRecoverySchedule(clock)
				stop := startManualRecovery(t, s, q, clock)
				waitRecoveryReset(t, clock, time.Second)
				healthy.Store(true)
				clock.advance(time.Second) // No client RPC or Worker notification.
				waitRecoverySignal(t, completed)
				stop()
				want := queued
				want.State, want.AttemptID, want.WorkerID, want.HasWorkerResult, want.Output = JobSucceeded, 1, "worker", true, "done"
				if commit {
					want.AttemptID = 2
				}
				assertRecoveryRecord(t, inner, want)
				assertRecoveryRecord(t, inner, control)
				if publications.Load() != 1 || calls.Load() != 1 {
					t.Fatalf("publications=%d calls=%d", publications.Load(), calls.Load())
				}
				assertQueuedSessionReleased(t, s, queued.JobID, false)
			})
		}
	}
}

func TestQueuedReconciliationOnlineBackoffAndUnrelatedJob(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "memory")
	s.recoveryReady = make(chan struct{}, 1)
	expected := queued
	expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
	saveRecoveryRecord(t, inner, expected)
	entry := installQueuedReconciliation(s, expected, nil)
	var reads atomic.Int32
	s.jobStore = &recoveryPassStore{JobStore: inner, load: func(id int64) (JobRecord, bool, error) {
		if id == expected.JobID {
			reads.Add(1)
			return JobRecord{}, false, errors.New("permanently unavailable job read")
		}
		return inner.Load(id)
	}}
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, time.Second)
	// A new terminal obligation wakes the SAME producer while the first job is
	// cooling down; it need not wait for that job's storage to recover.
	other := expected
	other.JobID++
	other.State, other.HasWorkerResult = JobSucceeded, true
	if ok, err := inner.Create(other); !ok || err != nil {
		t.Fatalf("Create=%v, %v", ok, err)
	}
	installQueuedReconciliation(s, func() JobRecord { r := other; r.State, r.HasWorkerResult = JobRunning, false; return r }(), nil)
	s.notifyRecoveryReady()
	waitRecoveryReset(t, clock, time.Second)
	if s.hasQueuedReconciliation(other.JobID) || reads.Load() != 1 {
		t.Fatal("new obligation blocked behind failing job")
	}
	assertRecoveryRecord(t, inner, other)
	for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second} {
		s.mu.Lock()
		failures, due, diagnostic := entry.failures, entry.nextRetry, entry.cause
		s.mu.Unlock()
		if failures != uint32(i+1) || !due.Equal(clock.now().Add(delay)) || diagnostic == nil || reads.Load() != int32(i+1) {
			t.Fatalf("backoff=%d due=%v reads=%d", failures, due, reads.Load())
		}
		// Notifications cannot bypass the pending obligation's cooldown.
		s.notifyRecoveryReady()
		waitRecoveryReset(t, clock, delay)
		if reads.Load() != int32(i+1) {
			t.Fatal("notification bypassed reconciliation cooldown")
		}
		if i < 6 {
			clock.advance(delay)
			waitRecoveryReset(t, clock, []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}[i])
		}
	}
	stop()
	assertQueuedSessionReleased(t, s, expected.JobID, true)
	assertQueuedSessionReleased(t, s, other.JobID, false)
}

func TestQueuedReconciliationOnlineTerminalCommit(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			s, inner, queued := queuedSessionFixture(t, backend)
			s.recoveryReady = make(chan struct{}, 1)
			var committed, healthy atomic.Bool
			var calls, terminalWrites atomic.Int32
			s.jobStore = &recoveryPassStore{JobStore: inner,
				save: func(r JobRecord) error {
					if err := inner.Save(r); err != nil {
						return err
					}
					if r.HasWorkerResult {
						terminalWrites.Add(1)
						committed.Store(true)
						return errors.New("terminal acknowledgement lost")
					}
					return nil
				},
				load: func(id int64) (JobRecord, bool, error) {
					if committed.Load() && !healthy.Load() {
						return JobRecord{}, false, errors.New("confirmation read unavailable")
					}
					return inner.Load(id)
				}}
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return &runtimepb.ExecuteJobResponse{JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded"}, nil
			})
			if err := s.runQueuedJob(queued.JobID); err == nil {
				t.Fatal("lost acknowledgement was accepted")
			}
			assertQueuedSessionReleased(t, s, queued.JobID, true)
			completed := make(chan struct{}, 1)
			s.metrics = &recordingMetrics{hook: func(e recordedMetric) {
				if e.name == "pass" && healthy.Load() && !s.hasQueuedReconciliation(queued.JobID) {
					select {
					case completed <- struct{}{}:
					default:
					}
				}
			}}
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			stop := startManualRecovery(t, s, q, clock)
			waitRecoveryReset(t, clock, time.Second)
			healthy.Store(true)
			clock.advance(time.Second)
			waitRecoverySignal(t, completed)
			stop()
			want := queued
			want.State, want.AttemptID, want.WorkerID, want.HasWorkerResult = JobSucceeded, 1, "worker", true
			assertRecoveryRecord(t, inner, want)
			if calls.Load() != 1 || terminalWrites.Load() != 1 {
				t.Fatalf("terminal overwritten/re-executed: calls=%d writes=%d", calls.Load(), terminalWrites.Load())
			}
			assertQueuedSessionReleased(t, s, queued.JobID, false)
		})
	}
}

func TestQueuedReconciliationOnlineTransientRepairFailure(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "memory")
	expected := queued
	expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
	saveRecoveryRecord(t, inner, expected)
	installQueuedReconciliation(s, expected, nil)
	var repairs atomic.Int32
	s.jobStore = &recoveryPassStore{JobStore: inner, recoverRunning: func(r JobRecord) (bool, error) {
		if repairs.Add(1) == 1 {
			return false, errors.New("transient repair failure")
		}
		return inner.TryRecoverRunning(r)
	}}
	completed := make(chan struct{}, 1)
	s.metrics = &recordingMetrics{hook: func(e recordedMetric) {
		if e.name == "pass" && !s.hasQueuedReconciliation(queued.JobID) {
			select {
			case completed <- struct{}{}:
			default:
			}
		}
	}}
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, time.Second)
	assertRecoveryRecord(t, inner, expected)
	if !s.hasQueuedReconciliation(queued.JobID) {
		t.Fatal("failed repair discarded responsibility")
	}
	clock.advance(time.Second)
	waitRecoverySignal(t, completed)
	stop()
	if repairs.Load() != 2 {
		t.Fatalf("repair attempts=%d", repairs.Load())
	}
	expected.State = JobRecovering
	assertRecoveryRecord(t, inner, expected)
	assertQueuedSessionReleased(t, s, queued.JobID, false)
}

func TestQueuedReconciliationPreservesRecoveryCooldown(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "memory")
	s.recoveryReady = make(chan struct{}, 1)
	expected := queued
	expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
	recovering := expected
	recovering.State = JobRecovering
	saveRecoveryRecord(t, inner, recovering)
	installQueuedReconciliation(s, expected, nil)
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	original := retryEntry{generation: 1, specHash: recoverySpecHash(recovering), failures: 5, nextRetry: clock.now().Add(30 * time.Second)}
	q.entries[queued.JobID] = original
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, 30*time.Second)
	// A handoff notification may cause another pass, without bypassing cooldown.
	stop()
	if s.hasQueuedReconciliation(queued.JobID) || calls.Load() != 0 || q.entries[queued.JobID] != original {
		t.Fatalf("recovery cooldown changed: %+v calls=%d", q.entries[queued.JobID], calls.Load())
	}
	assertRecoveryRecord(t, inner, recovering)
	assertQueuedSessionReleased(t, s, queued.JobID, false)
}

func TestQueuedReconciliationIndependentOfRecoveryScanBackoff(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "memory")
	expected := queued
	expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
	terminal := expected
	terminal.State, terminal.HasWorkerResult = JobSucceeded, true
	saveRecoveryRecord(t, inner, terminal)
	installQueuedReconciliation(s, expected, nil)
	var scans atomic.Int32
	s.jobStore = &recoveryPassStore{JobStore: inner, list: func(JobState) ([]JobRecord, error) { scans.Add(1); return nil, errors.New("scan unavailable") }}
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	q.scan = retryEntry{failures: 5, nextRetry: clock.now().Add(30 * time.Second)}
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, 30*time.Second)
	stop()
	if scans.Load() != 0 || s.hasQueuedReconciliation(queued.JobID) || q.scan.failures != 5 {
		t.Fatal("global recovery scan cooldown blocked ledger settlement")
	}
	assertRecoveryRecord(t, inner, terminal)
}

func TestQueuedReconciliationShutdownDuringRepair(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "sqlite")
	s.recoveryReady = make(chan struct{}, 1)
	expected := queued
	expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
	saveRecoveryRecord(t, inner, expected)
	installQueuedReconciliation(s, expected, nil)
	db := inner.(*SQLiteJobStore)
	var seq int
	var name, path string
	if err := db.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	backing := &recoveryPassStore{JobStore: inner, recoverRunning: func(r JobRecord) (bool, error) { close(entered); <-release; return inner.TryRecoverRunning(r) }}
	store := &shutdownStore{JobStore: backing, closeBacking: db.Close}
	s.jobStore = store
	p := newCoordinatorProcess(s, 2*time.Second)
	p.closeStore = store.Close
	p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
	t.Cleanup(func() { _ = p.shutdown(); awaitShutdownSignal(t, p.cleanupDone) })
	t.Cleanup(unblock)
	p.startBackground()
	awaitShutdownSignal(t, entered)
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.shutdown() }()
	awaitShutdownSignal(t, s.lifecycle.ctx.Done())
	if s.claimJob(queued.JobID) {
		t.Fatal("shutdown released active reconciliation claim")
	}
	if closed, _, _ := store.snapshot(); closed {
		t.Fatal("storage closed during repair")
	}
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown skipped repair: %v", err)
	default:
	}
	unblock()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	assertShutdownStoreClosed(t, store)
	want := expected
	want.State = JobRecovering
	loadShutdownRecord(t, path, want)
	assertQueuedSessionReleased(t, s, queued.JobID, false)
}
