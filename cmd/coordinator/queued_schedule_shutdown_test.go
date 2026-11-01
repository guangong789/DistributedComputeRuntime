package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func TestQueuedSchedulerLaunchConcurrentWithShutdown(t *testing.T) {
	// Supplement the deterministic before-launch and storage barriers below
	// with the actual reservation-vs-seal race. Both winning orders are valid.
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			s, inner, _ := queuedSchedulerFixture(t, "memory")
			queued := seedScheduledQueued(t, inner, 11099)
			store := &shutdownStore{JobStore: inner, closeBacking: func() error { return nil }}
			s.jobStore = store
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				return successfulResult(r), nil
			})
			p := newCoordinatorProcess(s, 2*time.Second)
			p.closeStore = store.Close
			p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
			t.Cleanup(func() { _ = p.shutdown() })
			start, producerDone := make(chan struct{}), make(chan struct{})
			go func() {
				defer close(producerDone)
				<-start
				s.scheduleQueuedJobs(s.lifecycle.ctx, []JobRecord{queued}, newRecoverySchedule())
			}()
			shutdown := make(chan error, 1)
			go func() { <-start; shutdown <- p.shutdown() }()
			close(start)
			awaitShutdownSignal(t, producerDone)
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
			assertShutdownStoreClosed(t, store)
			r, _, err := inner.Load(queued.JobID)
			if err != nil || r.State != JobQueued && r.State != JobRecovering && r.State != JobSucceeded {
				t.Fatalf("unsettled shutdown race: %+v err=%v", r, err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.queuedSessions != 0 || len(s.activeJobs) != 0 {
				t.Fatal("shutdown leaked session ownership")
			}
		})
	}
}

func TestQueuedSchedulerOwnershipBeforeLaunch(t *testing.T) {
	s, inner, _ := queuedSchedulerFixture(t, "memory")
	queued := seedScheduledQueued(t, inner, 11100)
	store := &shutdownStore{JobStore: inner, closeBacking: func() error { return nil }}
	s.jobStore = store
	p := newCoordinatorProcess(s, 2*time.Second)
	p.closeStore = store.Close
	p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
	ctx, release, full := s.reserveQueuedSession(s.lifecycle.ctx, queued.JobID)
	if release == nil || full {
		t.Fatal("cannot reserve session")
	}
	var once sync.Once
	finish := func() { once.Do(func() { _ = s.runClaimedQueuedJob(ctx, queued.JobID); release() }) }
	t.Cleanup(func() { finish(); _ = p.shutdown() })
	// The goroutine has deliberately not launched yet. The lifecycle must
	// already own it, including its capacity and JobID exclusion.
	s.lifecycle.mu.Lock()
	admitted := len(s.lifecycle.active)
	s.lifecycle.mu.Unlock()
	if admitted != 1 || s.claimJob(queued.JobID) {
		t.Fatalf("ownership not established before launch: admitted=%d", admitted)
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.shutdown() }()
	awaitShutdownSignal(t, s.lifecycle.ctx.Done())
	if closed, _, _ := store.snapshot(); closed {
		t.Fatal("closed storage before a reserved session launched")
	}
	if _, rejected, _ := s.reserveQueuedSession(s.lifecycle.ctx, queued.JobID+1); rejected != nil {
		t.Fatal("scheduler admitted work after shutdown seal")
	}
	go finish()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	assertShutdownStoreClosed(t, store)
	assertRecoveryRecord(t, inner, queued) // Cancelled before any publication.
	s.mu.Lock()
	reserved, claims := s.queuedSessions, len(s.activeJobs)
	s.mu.Unlock()
	if reserved != 0 || claims != 0 {
		t.Fatalf("reservation leaked: slots=%d claims=%d", reserved, claims)
	}
}

func TestQueuedSchedulerShutdownStorageBoundaries(t *testing.T) {
	for _, boundary := range []string{"first publication", "terminal Save", "recovery repair"} {
		t.Run(boundary, func(t *testing.T) {
			s, inner, _ := queuedSchedulerFixture(t, "sqlite")
			queued := seedScheduledQueued(t, inner, 11101)
			db := inner.(*SQLiteJobStore)
			var sequence int
			var name, path string
			if err := db.db.QueryRow("PRAGMA database_list").Scan(&sequence, &name, &path); err != nil {
				t.Fatal(err)
			}
			entered, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(unblock) }) }
			backing := &recoveryPassStore{JobStore: inner}
			gate := func() { close(entered); <-unblock }
			if boundary == "first publication" {
				backing.startQueued = func(r JobRecord, w string) (bool, error) { gate(); return inner.TryStartQueued(r, w) }
			}
			if boundary == "terminal Save" {
				backing.save = func(r JobRecord) error {
					if r.HasWorkerResult {
						gate()
					}
					return inner.Save(r)
				}
			}
			if boundary == "recovery repair" {
				backing.recoverRunning = func(r JobRecord) (bool, error) { gate(); return inner.TryRecoverRunning(r) }
			}
			store := &shutdownStore{JobStore: backing, closeBacking: db.Close}
			s.jobStore = store
			started := make(chan struct{})
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				close(started)
				if boundary == "recovery repair" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return successfulResult(r), nil
			})
			p := newCoordinatorProcess(s, 2*time.Second)
			p.closeStore = store.Close
			p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
			t.Cleanup(func() { _ = p.shutdown(); awaitShutdownSignal(t, p.cleanupDone) })
			t.Cleanup(release)
			p.startBackground()
			if boundary == "recovery repair" {
				awaitShutdownSignal(t, started)
			} else {
				awaitShutdownSignal(t, entered)
			}
			shutdown := make(chan error, 1)
			go func() { shutdown <- p.shutdown() }()
			awaitShutdownSignal(t, s.lifecycle.ctx.Done())
			if boundary == "recovery repair" {
				awaitShutdownSignal(t, entered)
			}
			if closed, _, _ := store.snapshot(); closed {
				t.Fatal("storage closed with session operation in flight")
			}
			select {
			case err := <-shutdown:
				t.Fatalf("shutdown skipped owned storage call: %v", err)
			default:
			}
			release()
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
			assertShutdownStoreClosed(t, store)
			want := queued
			want.State, want.AttemptID, want.WorkerID = JobRecovering, 1, "worker"
			if boundary == "terminal Save" {
				want.State, want.HasWorkerResult, want.Output = JobSucceeded, true, "done"
			}
			loadShutdownRecord(t, path, want)
			wantCalls := int32(1)
			if boundary == "first publication" {
				wantCalls = 0
			}
			if calls.Load() != wantCalls {
				t.Fatalf("dispatch=%d want %d", calls.Load(), wantCalls)
			}
			s.mu.Lock()
			reserved, claims := s.queuedSessions, len(s.activeJobs)
			s.mu.Unlock()
			if reserved != 0 || claims != 0 {
				t.Fatalf("ownership leaked: capacity=%d claims=%d", reserved, claims)
			}
		})
	}
}

func TestQueuedSchedulerLongSessionDoesNotBlockRediscovery(t *testing.T) {
	s, inner, passes := queuedSchedulerFixture(t, "memory")
	s.leaseDuration = time.Minute
	long := seedScheduledQueued(t, inner, 11110)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	registerTestWorker(t, s, "worker", func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if r.JobId == long.JobID {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return successfulResult(r), nil
	})
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	stop := startManualRecovery(t, s, q, clock)
	awaitShutdownSignal(t, started)
	select {
	case <-clock.resets:
	case <-time.After(5 * time.Second):
		t.Fatal("discovery fallback was not armed")
	}
	other := seedScheduledQueued(t, inner, 11111)
	// Discovery fallback while the first complete session is still blocked.
	clock.advance(durableDiscoveryInterval)
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, inner, other.JobID) })
	if r, _, _ := inner.Load(long.JobID); r.State != JobRunning || r.AttemptID != 1 {
		t.Fatalf("unrelated discovery waited for long session: %+v", r)
	}
	unblock()
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, inner, long.JobID) })
	stop()
}
