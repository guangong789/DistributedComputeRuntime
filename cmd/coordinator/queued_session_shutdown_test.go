package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestQueuedJobShutdownBoundaries(t *testing.T) {
	for _, boundary := range []string{"publication", "active RPC and recovery handoff", "terminal Save"} {
		t.Run(boundary, func(t *testing.T) {
			// Real mTLS/SQLite and existing Close instrumentation, with no automatic
			// background producers: this milestone initiates sessions explicitly.
			f := newMTLSCoordinator(t)
			db := f.store.JobStore.(*SQLiteJobStore)
			var seq int
			var name, path string
			if err := db.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
				t.Fatal(err)
			}
			queued := queuedPublicationRecord()
			queued.JobID = 9903
			if ok, err := db.Create(queued); !ok || err != nil {
				t.Fatalf("Create=%v, %v", ok, err)
			}
			store := &shutdownStore{JobStore: f.store, closeBacking: db.Close}
			f.s.jobStore = store
			p := newCoordinatorProcess(f.s, 2*time.Second)
			p.server, p.serveDone, p.closeStore = f.server, f.serveDone, store.Close
			p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
			t.Cleanup(func() { _ = p.shutdown(); awaitShutdownSignal(t, p.cleanupDone) })
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			if boundary == "publication" {
				store.beforeQueued = func(JobRecord) { close(entered); <-release }
			}
			if boundary == "terminal Save" {
				store.beforeSave = func(r JobRecord) {
					if r.HasWorkerResult {
						close(entered)
						<-release
					}
				}
			}
			if boundary == "active RPC and recovery handoff" {
				store.beforeSave = func(r JobRecord) {
					if r.State == JobRecovering {
						close(entered)
						<-release
					}
				}
			}
			started, returned := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				close(started)
				defer close(returned)
				if boundary == "active RPC and recovery handoff" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return successfulResult(req), nil
			})
			registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
			result := make(chan error, 1)
			go func() { result <- f.s.runQueuedJob(queued.JobID) }()
			if boundary == "active RPC and recovery handoff" {
				awaitShutdownSignal(t, started)
				// A separate Submit RPC ending cannot cancel the Coordinator session.
				client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: queued.JobID, TaskType: queued.TaskType, Payload: queued.Payload, TimeoutMs: queued.TimeoutMs}); status.Code(err) != codes.Canceled {
					t.Fatalf("cancelled client RPC=%v", err)
				}
				select {
				case <-returned:
					t.Fatal("unrelated client RPC cancelled queued Worker")
				default:
				}
				if f.s.lifecycle.ctx.Err() != nil {
					t.Fatal("client cancellation reached Coordinator lifecycle")
				}
			} else {
				awaitShutdownSignal(t, entered)
			}
			shutdown := make(chan error, 1)
			go func() { shutdown <- p.shutdown() }()
			awaitShutdownSignal(t, f.s.lifecycle.ctx.Done())
			if boundary == "active RPC and recovery handoff" {
				awaitShutdownSignal(t, returned)
				awaitShutdownSignal(t, entered)
				if f.s.claimJob(queued.JobID) {
					t.Fatal("claim released before recovery handoff")
				}
			}
			if closed, _, _ := store.snapshot(); closed {
				t.Fatal("storage closed before queued session settlement")
			}
			select {
			case err := <-shutdown:
				t.Fatalf("shutdown skipped blocked session: %v", err)
			default:
			}
			select {
			case err := <-result:
				t.Fatalf("session skipped blocked finalization: %v", err)
			default:
			}
			if err := f.s.runQueuedJob(queued.JobID); status.Code(err) != codes.Unavailable {
				t.Fatalf("shutdown admitted another session: %v", err)
			}
			unblock()
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
			err := <-result
			want := queued
			want.State, want.AttemptID, want.WorkerID = JobRecovering, 1, "worker-a"
			if boundary == "terminal Save" {
				if err != nil {
					t.Fatalf("acknowledged terminal commit lost to shutdown: %v", err)
				}
				want.State, want.HasWorkerResult, want.Output = JobSucceeded, true, "done"
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("session cancellation=%v", err)
			}
			wantCalls := int32(1)
			if boundary == "publication" {
				wantCalls = 0
			}
			if calls.Load() != wantCalls {
				t.Fatalf("dispatches=%d, want %d", calls.Load(), wantCalls)
			}
			assertShutdownStoreClosed(t, store)
			loadShutdownRecord(t, path, want)
			assertQueuedSessionReleased(t, f.s, queued.JobID, false)
		})
	}
}

func TestQueuedJobStartConcurrentWithShutdown(t *testing.T) {
	// The existing admission lock arbitrates start versus shutdown. No session
	// WaitGroup Add can race Wait because sessions use lifecycle admission itself.
	for i := 0; i < 12; i++ {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s, inner, queued := queuedSessionFixture(t, "memory")
			store := &shutdownStore{JobStore: inner, closeBacking: func() error { return nil }}
			s.jobStore = store
			p := newCoordinatorProcess(s, 2*time.Second)
			p.closeStore = store.Close
			p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				return successfulResult(req), nil
			})
			start := make(chan struct{})
			result, shutdown := make(chan error, 1), make(chan error, 1)
			go func() { <-start; result <- s.runQueuedJob(queued.JobID) }()
			go func() { <-start; shutdown <- p.shutdown() }()
			close(start)
			err := <-result
			if err != nil && !errors.Is(err, context.Canceled) && status.Code(err) != codes.Unavailable {
				t.Fatalf("start/shutdown outcome=%v", err)
			}
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
			assertShutdownStoreClosed(t, store)
			actual, _, err := inner.Load(queued.JobID)
			if err != nil || !(isUnattemptedQueued(actual) || actual.State == JobRecovering || actual.State == JobSucceeded) {
				t.Fatalf("unsettled shutdown state=%+v err=%v", actual, err)
			}
			assertQueuedSessionReleased(t, s, queued.JobID, false)
		})
	}
}

func TestQueuedJobRecoveryHandoffKeepsOwnership(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "memory")
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	s.jobStore = &recoveryPassStore{JobStore: inner, recoverRunning: func(r JobRecord) (bool, error) {
		repaired, err := inner.TryRecoverRunning(r)
		if repaired && err == nil {
			close(entered)
			<-release
		}
		return repaired, err
	}}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		resp := successfulResult(req)
		resp.Status = "invalid"
		return resp, nil
	})
	result := make(chan error, 1)
	go func() { result <- s.runQueuedJob(queued.JobID) }()
	awaitShutdownSignal(t, entered)
	current, _, err := inner.Load(queued.JobID)
	if err != nil || current.State != JobRecovering {
		t.Fatalf("handoff state=%+v err=%v", current, err)
	}
	if err := s.retryRecoveringJob(context.Background(), current); err == nil {
		t.Fatal("background recovery took the unsettled session's claim")
	}
	if _, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: queued.JobID, TaskType: queued.TaskType, Payload: queued.Payload, TimeoutMs: queued.TimeoutMs}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("Submit duplicate during handoff=%v", err)
	}
	unblock()
	if err := <-result; status.Code(err) != codes.Internal {
		t.Fatalf("invalid status classification=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate execution during handoff: calls=%d", calls.Load())
	}
	assertRecoveryRecord(t, inner, current)
	assertQueuedSessionReleased(t, s, queued.JobID, false)
}

func TestQueuedJobHandoffPreservesAdvancedGeneration(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "memory")
	advanced := queued
	advanced.State, advanced.AttemptID, advanced.WorkerID = JobRunning, 7, "independent-writer"
	s.jobStore = &recoveryPassStore{JobStore: inner, save: func(r JobRecord) error {
		if r.HasWorkerResult {
			if err := inner.Save(advanced); err != nil {
				return err
			}
			return errors.New("unconfirmed terminal Save")
		}
		t.Error("handoff overwrote a different durable generation")
		return inner.Save(r)
	}}
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(req), nil
	})
	if err := s.runQueuedJob(queued.JobID); err == nil {
		t.Fatal("advanced durable state was silently settled")
	}
	assertRecoveryRecord(t, inner, advanced)
	assertQueuedSessionReleased(t, s, queued.JobID, true)
}
