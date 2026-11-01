package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func queuedSchedulerFixture(t *testing.T, backend string) (*CoordinatorServer, JobStore, chan struct{}) {
	t.Helper()
	store := newCreationTestStore(t, backend)
	if db, ok := store.(*SQLiteJobStore); ok {
		// Deterministic unit scheduling checks use one connection. Separate
		// default-configuration tests exercise BUSY and uncertain publications.
		db.db.SetMaxOpenConns(1)
	}
	passes := make(chan struct{}, 64)
	s := &CoordinatorServer{jobStore: store, lifecycle: newCoordinatorLifecycle(), recoveryReady: make(chan struct{}, 1)}
	s.metrics = &recordingMetrics{hook: func(e recordedMetric) {
		if e.name == "pass" || e.name == "terminal" {
			select {
			case passes <- struct{}{}:
			default:
			}
		}
	}}
	t.Cleanup(func() {
		s.lifecycle.beginShutdown()
		s.lifecycle.cancelWork()
		<-s.lifecycle.done
		s.dispatchWG.Wait()
	})
	return s, store, passes
}

func seedScheduledQueued(t *testing.T, store JobStore, id int64) JobRecord {
	t.Helper()
	r := queuedPublicationRecord()
	r.JobID = id
	if ok, err := store.Create(r); !ok || err != nil {
		t.Fatalf("Create Queued %d=%v, %v", id, ok, err)
	}
	return r
}

func waitQueuedCondition(t *testing.T, passes <-chan struct{}, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for !condition() {
		select {
		case <-passes:
		case <-deadline.C:
			t.Fatal("queued scheduling condition did not become true")
		}
	}
}

func scheduledJobDone(s *CoordinatorServer, store JobStore, id int64) bool {
	r, ok, err := store.Load(id)
	s.mu.Lock()
	_, active := s.activeJobs[id]
	s.mu.Unlock()
	return err == nil && ok && r.State == JobSucceeded && !active
}

func TestQueuedSchedulerDurableDiscovery(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, seedBefore := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/before=%v", backend, seedBefore), func(t *testing.T) {
				s, store, passes := queuedSchedulerFixture(t, backend)
				var calls atomic.Int32
				registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					if r.AttemptId != 1 {
						t.Errorf("first generation=%d", r.AttemptId)
					}
					return successfulResult(r), nil
				})
				if seedBefore {
					seedScheduledQueued(t, store, 11000)
				}
				clock := newManualRecoveryClock()
				q := testRecoverySchedule(clock)
				stop := startManualRecovery(t, s, q, clock)
				if !seedBefore {
					waitRecoveryReset(t, clock, durableDiscoveryInterval)
					seedScheduledQueued(t, store, 11000)
					// No admission or Worker notification survives persistence.
					clock.advance(durableDiscoveryInterval)
				}
				waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, store, 11000) })
				stop()
				if calls.Load() != 1 || s.metrics.(*recordingMetrics).count("started", "") != 1 || s.metrics.(*recordingMetrics).count("terminal", "") != 1 {
					t.Fatal("discovery fabricated or duplicated execution")
				}
				if got, _, _ := store.Load(11000); !got.HasWorkerResult || got.Output != "done" {
					t.Fatalf("lost result: %+v", got)
				}
			})
		}
	}
}

func TestQueuedSchedulerWorkerAvailabilityAndWaitingBudget(t *testing.T) {
	for _, wake := range []string{"registration", "heartbeat", "lost notification"} {
		t.Run(wake, func(t *testing.T) {
			s, store, passes := queuedSchedulerFixture(t, "memory")
			r := seedScheduledQueued(t, store, 11001)
			if wake != "registration" {
				// Publish a real backend before starting; mark its registration dead.
				registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					return successfulResult(r), nil
				})
				s.mu.Lock()
				w := s.workers["worker"]
				w.Status = WorkerDead
				s.workers["worker"] = w
				s.mu.Unlock()
				if wake == "lost notification" {
					s.recoveryReady = nil
				}
			} else {
				// Transport is immutable before the producer starts.
				useTestWorkerTransport(s)
			}
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			stop := startManualRecovery(t, s, q, clock)
			waitRecoveryReset(t, clock, durableDiscoveryInterval)
			clock.advance(durableDiscoveryInterval) // Much longer than TimeoutMs.
			waitRecoveryReset(t, clock, durableDiscoveryInterval)
			assertRecoveryRecord(t, store, r)
			s.mu.Lock()
			phantom := len(s.jobAttempts) != 0 || len(s.leases) != 0 || s.queuedSessions != 0
			s.mu.Unlock()
			if phantom || s.metrics.(*recordingMetrics).count("execution", "") != 0 {
				t.Fatal("waiting consumed execution resources or timeout")
			}
			if wake != "registration" {
				if response, err := s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: "worker"}); err != nil || !response.Accepted {
					t.Fatalf("heartbeat=%v, %v", response, err)
				}
				if wake == "lost notification" {
					clock.advance(durableDiscoveryInterval)
				}
			} else {
				registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					if req.AttemptId != 1 || req.TimeoutMs != r.TimeoutMs {
						t.Errorf("waiting exhausted budget: %+v", req)
					}
					return successfulResult(req), nil
				})
			}
			waitQueuedCondition(t, passes, func() bool {
				if wake == "lost notification" {
					// Completion wakeups are intentionally lost too. The accepted
					// terminal metric acknowledges persistence; fixture shutdown joins
					// the session's remaining claim/capacity cleanup.
					got, ok, err := store.Load(r.JobID)
					return err == nil && ok && got.State == JobSucceeded
				}
				return scheduledJobDone(s, store, r.JobID)
			})
			stop()
		})
	}
}

func TestQueuedSchedulerBoundAndIndependentControl(t *testing.T) {
	s, store, passes := queuedSchedulerFixture(t, "memory")
	s.queuedSessionLimit = 2
	s.leaseDuration = time.Minute
	for id := int64(11010); id < 11015; id++ {
		seedScheduledQueued(t, store, id)
	}
	started := make(chan int64, 8)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var inFlight, maximum, calls atomic.Int32
	registerTestWorker(t, s, "worker", func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for previous := maximum.Load(); n > previous && !maximum.CompareAndSwap(previous, n); previous = maximum.Load() {
		}
		started <- r.JobId
		select {
		case <-release:
			return successfulResult(r), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	stop := startManualRecovery(t, s, q, clock)
	for i := 0; i < 2; i++ {
		select {
		case id := <-started:
			if id != 11010 && id != 11011 {
				t.Fatalf("non-deterministic selection: %d", id)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("bounded sessions did not launch")
		}
	}
	// A durable terminal obligation must retire while BOTH slots are occupied.
	expected := queuedPublicationRecord()
	expected.JobID, expected.State, expected.AttemptID, expected.WorkerID = 11020, JobRunning, 1, "worker"
	terminal := expected
	terminal.State, terminal.HasWorkerResult = JobSucceeded, true
	saveRecoveryRecord(t, store, terminal)
	installQueuedReconciliation(s, expected, nil)
	for i := 0; i < 20; i++ {
		s.notifyRecoveryReady()
	}
	waitQueuedCondition(t, passes, func() bool { return !s.hasQueuedReconciliation(expected.JobID) })
	assertRecoveryRecord(t, store, terminal)
	s.mu.Lock()
	active := s.queuedSessions
	s.mu.Unlock()
	if active != 2 || calls.Load() != 2 || maximum.Load() > 2 {
		t.Fatalf("bound/duplicate dispatch: sessions=%d calls=%d max=%d", active, calls.Load(), maximum.Load())
	}
	assertRecoveryRecord(t, store, func() JobRecord { r := queuedPublicationRecord(); r.JobID = 11012; return r }())
	unblock()
	// Completion alone wakes the producer; the clock never advances.
	waitQueuedCondition(t, passes, func() bool {
		for id := int64(11010); id < 11015; id++ {
			if !scheduledJobDone(s, store, id) {
				return false
			}
		}
		return true
	})
	stop()
	if calls.Load() != 5 || maximum.Load() > 2 {
		t.Fatalf("backlog stranded/duplicated: calls=%d max=%d", calls.Load(), maximum.Load())
	}
}

func TestQueuedSchedulerCombinedScanFailurePreservesState(t *testing.T) {
	for _, failState := range []JobState{JobQueued, JobRecovering} {
		t.Run(fmt.Sprint(failState), func(t *testing.T) {
			s, inner, passes := queuedSchedulerFixture(t, "memory")
			queued := seedScheduledQueued(t, inner, 11030)
			recovering := queued
			recovering.JobID, recovering.State, recovering.AttemptID, recovering.WorkerID = 11031, JobRecovering, 7, "old"
			saveRecoveryRecord(t, inner, recovering)
			var fail atomic.Bool
			fail.Store(true)
			s.jobStore = &recoveryPassStore{JobStore: inner, list: func(state JobState) ([]JobRecord, error) {
				if state == failState && fail.Load() {
					return nil, errors.New("transient discovery failure")
				}
				return inner.ListByState(state)
			}}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(r), nil
			})
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			cooldown := retryEntry{generation: 7, specHash: recoverySpecHash(recovering), failures: 5, nextRetry: clock.now().Add(30 * time.Second)}
			q.entries[recovering.JobID] = cooldown
			q.recheck(queued)
			queuedCooldown := q.entries[queued.JobID]
			stop := startManualRecovery(t, s, q, clock)
			waitRecoveryReset(t, clock, time.Second)
			stop()
			if q.entries[recovering.JobID] != cooldown || q.entries[queued.JobID] != queuedCooldown || calls.Load() != 0 {
				t.Fatal("partial failed scan erased scheduling state or executed work")
			}
			fail.Store(false)
			clock.advance(time.Second)
			stop = startManualRecovery(t, s, q, clock)
			waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, inner, queued.JobID) })
			stop()
			if q.entries[recovering.JobID] != cooldown || calls.Load() != 1 {
				t.Fatal("Queued discovery erased Recovering cooldown")
			}
			clock.advance(29 * time.Second)
			stop = startManualRecovery(t, s, q, clock)
			waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, inner, recovering.JobID) })
			stop()
			if got, _, _ := inner.Load(recovering.JobID); got.AttemptID != 8 || calls.Load() != 2 {
				t.Fatalf("recovery not preserved: %+v calls=%d", got, calls.Load())
			}
		})
	}
}

func TestQueuedSchedulerPendingExclusionAndRetirement(t *testing.T) {
	s, inner, passes := queuedSchedulerFixture(t, "memory")
	queued := seedScheduledQueued(t, inner, 11040)
	other := seedScheduledQueued(t, inner, 11041)
	expected := queued
	expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "worker"
	installQueuedReconciliation(s, expected, nil)
	var healthy atomic.Bool
	s.jobStore = &recoveryPassStore{JobStore: inner, load: func(id int64) (JobRecord, bool, error) {
		if id == queued.JobID && !healthy.Load() {
			return JobRecord{}, false, errors.New("ledger read unavailable")
		}
		return inner.Load(id)
	}}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(r), nil
	})
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	stop := startManualRecovery(t, s, q, clock)
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, inner, other.JobID) })
	assertRecoveryRecord(t, inner, queued)
	if !s.hasQueuedReconciliation(queued.JobID) || calls.Load() != 1 {
		t.Fatal("pending ledger authorized execution or blocked unrelated work")
	}
	healthy.Store(true)
	clock.advance(time.Second)
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, inner, queued.JobID) })
	stop()
	if s.hasQueuedReconciliation(queued.JobID) || calls.Load() != 2 {
		t.Fatal("confirmed Queued retirement did not restore eligibility")
	}
}

func TestQueuedSchedulerPendingDeadlineDoesNotSpin(t *testing.T) {
	s, inner, _ := queuedSchedulerFixture(t, "memory")
	expected := queuedPublicationRecord()
	expected.JobID, expected.State, expected.AttemptID, expected.WorkerID = 11045, JobRunning, 3, "old"
	recovering := expected
	recovering.State = JobRecovering
	saveRecoveryRecord(t, inner, recovering)
	installQueuedReconciliation(s, expected, nil)
	var reads atomic.Int32
	s.jobStore = &recoveryPassStore{JobStore: inner, load: func(int64) (JobRecord, bool, error) {
		reads.Add(1)
		return JobRecord{}, false, errors.New("permanent ledger read error")
	}}
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	overdue := retryEntry{generation: 3, specHash: recoverySpecHash(recovering), failures: 5, nextRetry: clock.now().Add(-time.Second)}
	q.entries[expected.JobID] = overdue
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, time.Second)
	s.notifyRecoveryReady()
	waitRecoveryReset(t, clock, time.Second)
	clock.advance(time.Second)
	waitRecoveryReset(t, clock, 2*time.Second)
	stop()
	if reads.Load() != 2 || q.entries[expected.JobID] != overdue {
		t.Fatal("ordinary cooldown bypassed ledger backoff or was destroyed")
	}
}

func TestQueuedSchedulerWorkerLossDefersOverdueEntry(t *testing.T) {
	s, store, _ := queuedSchedulerFixture(t, "memory")
	queued := seedScheduledQueued(t, store, 11044)
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	q.entries[queued.JobID] = retryEntry{generation: 0, specHash: recoverySpecHash(queued), nextRetry: clock.now().Add(-time.Second)}
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, time.Second)
	clock.advance(time.Second)
	waitRecoveryReset(t, clock, time.Second)
	stop()
	assertRecoveryRecord(t, store, queued)
	if s.metrics.(*recordingMetrics).count("started", "") != 0 || len(s.leases) != 0 || len(s.jobAttempts) != 0 {
		t.Fatal("deferred eligibility allocated execution resources")
	}
}

func TestQueuedSchedulerPreservesConcurrentTerminal(t *testing.T) {
	for _, boundary := range []string{"before owned reload", "at atomic publication"} {
		t.Run(boundary, func(t *testing.T) {
			s, inner, passes := queuedSchedulerFixture(t, "memory")
			queued := seedScheduledQueued(t, inner, 11046)
			terminal := queued
			terminal.State, terminal.AttemptID, terminal.WorkerID, terminal.HasWorkerResult = JobSucceeded, 4, "old", true
			var changed atomic.Bool
			backing := &recoveryPassStore{JobStore: inner}
			if boundary == "before owned reload" {
				backing.list = func(state JobState) ([]JobRecord, error) {
					records, err := inner.ListByState(state)
					if state == JobQueued && !changed.Swap(true) {
						if err := inner.Save(terminal); err != nil {
							return nil, err
						}
					}
					return records, err
				}
			} else {
				backing.startQueued = func(r JobRecord, worker string) (bool, error) {
					changed.Store(true)
					if err := inner.Save(terminal); err != nil {
						return false, err
					}
					return inner.TryStartQueued(r, worker)
				}
			}
			s.jobStore = backing
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(r), nil
			})
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			stop := startManualRecovery(t, s, q, clock)
			waitQueuedCondition(t, passes, func() bool {
				s.mu.Lock()
				defer s.mu.Unlock()
				return changed.Load() && s.queuedSessions == 0
			})
			stop()
			assertRecoveryRecord(t, inner, terminal) // Empty accepted result stays present.
			if calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 || s.metrics.(*recordingMetrics).count("started", "") != 0 {
				t.Fatal("stale discovery overwrote terminal state or authorized a Worker")
			}
		})
	}
}

func TestQueuedSchedulerRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queued.db")
	first, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	queued := seedScheduledQueued(t, first, 11050)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s := &CoordinatorServer{jobStore: store, lifecycle: newCoordinatorLifecycle(), recoveryReady: make(chan struct{}, 1)}
	if err := s.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, store, queued)
	completed := make(chan struct{}, 8)
	s.metrics = &recordingMetrics{hook: func(e recordedMetric) {
		if e.name == "pass" {
			select {
			case completed <- struct{}{}:
			default:
			}
		}
	}}
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	p := newCoordinatorProcess(s, 2*time.Second)
	t.Cleanup(func() {
		if err := p.shutdown(); err != nil {
			t.Error(err)
		}
	})
	p.startBackground()
	waitQueuedCondition(t, completed, func() bool { return scheduledJobDone(s, store, queued.JobID) })
}

func TestQueuedSchedulerErrorSettlementAndCapacityRelease(t *testing.T) {
	for _, scenario := range []string{"exhausted", "invalid status", "terminal write", "retry publication", "first publication"} {
		t.Run(scenario, func(t *testing.T) {
			s, inner, passes := queuedSchedulerFixture(t, "memory")
			s.queuedSessionLimit = 1
			queued := seedScheduledQueued(t, inner, 11060)
			other := seedScheduledQueued(t, inner, 11061)
			var failed, uncertain atomic.Bool
			s.jobStore = &recoveryPassStore{JobStore: inner,
				startQueued: func(r JobRecord, worker string) (bool, error) {
					if r.JobID == queued.JobID && scenario == "first publication" {
						_, _ = inner.TryStartQueued(r, worker)
						uncertain.Store(true)
						return false, errors.New("lost publication acknowledgement")
					}
					return inner.TryStartQueued(r, worker)
				},
				save: func(r JobRecord) error {
					if r.JobID == queued.JobID && (scenario == "terminal write" && r.HasWorkerResult || scenario == "retry publication" && r.State == JobRunning) {
						uncertain.Store(true)
						return errors.New("write unavailable")
					}
					return inner.Save(r)
				},
				load: func(id int64) (JobRecord, bool, error) {
					if id == queued.JobID && uncertain.Load() {
						return JobRecord{}, false, errors.New("confirmation read unavailable")
					}
					return inner.Load(id)
				}}
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				if r.JobId == queued.JobID {
					failed.Store(true)
					if scenario == "exhausted" || scenario == "retry publication" {
						return nil, status.Error(codes.Unavailable, "injected Worker error")
					}
					if scenario == "invalid status" {
						return &runtimepb.ExecuteJobResponse{JobId: r.JobId, AttemptId: r.AttemptId, Status: "unknown"}, nil
					}
				}
				return successfulResult(r), nil
			})
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			if scenario == "invalid status" {
				// Force repair uncertainty so the retained obligation can be
				// inspected before normal recovery executes another generation.
				s.jobStore.(*recoveryPassStore).recoverRunning = func(r JobRecord) (bool, error) {
					uncertain.Store(true)
					return false, errors.New("repair unavailable")
				}
			}
			stop := startManualRecovery(t, s, q, clock)
			waitQueuedCondition(t, passes, func() bool {
				if !scheduledJobDone(s, inner, other.JobID) {
					return false
				}
				s.mu.Lock()
				defer s.mu.Unlock()
				return s.queuedSessions == 0
			})
			stop()
			got, _, _ := inner.Load(queued.JobID)
			if scenario == "exhausted" {
				if got.State != JobFailed || got.AttemptID != 2 || got.HasWorkerResult {
					t.Fatalf("exhaustion lost two-attempt finalization: %+v", got)
				}
			} else if !s.hasQueuedReconciliation(queued.JobID) || got.State != JobRunning {
				t.Fatalf("background failure lost ownership: %+v pending=%v", got, s.hasQueuedReconciliation(queued.JobID))
			}
			s.mu.Lock()
			reserved := s.queuedSessions
			s.mu.Unlock()
			if reserved != 0 || scenario != "first publication" && !failed.Load() {
				t.Fatalf("capacity/dispatch=%d/%v", reserved, failed.Load())
			}
		})
	}
}
