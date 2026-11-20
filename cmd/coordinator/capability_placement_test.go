package main

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	"github.com/guangong789/DistributedComputeRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func submitPlacementJob(t *testing.T, s *CoordinatorServer, id int64, task, payload string) JobRecord {
	t.Helper()
	const timeoutMS = 1000
	ack, err := s.SubmitJob(t.Context(), &runtimepb.SubmitJobRequest{
		JobId: id, TaskType: task, Payload: payload, TimeoutMs: timeoutMS,
	})
	assertAdmissionAck(t, ack, err, id)
	return JobRecord{JobID: id, State: JobQueued, TaskType: task, Payload: payload, TimeoutMs: timeoutMS}
}

func TestCapabilityQueuedPlacement(t *testing.T) {
	s, store, passes := queuedSchedulerFixture(t, "sqlite")
	cpuStarted := make(chan struct{}, 1)
	releaseCPU := make(chan struct{})
	var cpuCalls atomic.Int32
	registerTestWorker(t, s, "cpu-only", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		cpuCalls.Add(1)
		if req.TaskType != "hash" || req.AttemptId != 1 {
			t.Errorf("CPU-only Worker received unexpected request: %v", req)
			return nil, status.Error(codes.FailedPrecondition, "incompatible task")
		}
		cpuStarted <- struct{}{}
		select {
		case <-releaseCPU:
			return successfulResult(req), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})

	var waiting []JobRecord
	for i := range defaultQueuedSessionLimit + 1 {
		waiting = append(waiting, submitPlacementJob(t, s, int64(i), cudavector.TaskType, "0"+strings.Repeat(",0", 63)))
	}
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	if err := s.runScheduledRecoveryPass(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	sessions, attempts, leases := s.queuedSessions, len(s.jobAttempts), len(s.leases)
	s.mu.Unlock()
	if sessions != 0 || len(q.entries) != 0 || attempts != 0 || leases != 0 || cpuCalls.Load() != 0 {
		t.Fatalf("unmatched jobs used execution resources: sessions=%d entries=%d attempts=%d leases=%d dispatches=%d",
			sessions, len(q.entries), attempts, leases, cpuCalls.Load())
	}
	for _, record := range waiting {
		assertRecoveryRecord(t, store, record)
	}
	t.Log("unmatched CUDA jobs stayed Queued/0 with zero sessions, Attempts, leases and dispatches")

	cpu := submitPlacementJob(t, s, 100, "hash", "compatible")
	if err := s.runScheduledRecoveryPass(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	waitRecoverySignal(t, cpuStarted)
	s.mu.Lock()
	sessions, attempts, leases = s.queuedSessions, len(s.jobAttempts), len(s.leases)
	s.mu.Unlock()
	if sessions != 1 || len(q.entries) != 1 || q.entries[cpu.JobID].nextRetry.IsZero() || attempts != 1 || leases != 1 {
		t.Fatalf("compatible job shared resources with unmatched work: sessions=%d entries=%v attempts=%d leases=%d",
			sessions, q.entries, attempts, leases)
	}
	close(releaseCPU)
	waitQueuedCondition(t, passes, func() bool {
		record, _, err := store.Load(cpu.JobID)
		return err == nil && record.State == JobSucceeded
	})
	cpu.State, cpu.AttemptID, cpu.WorkerID = JobSucceeded, 1, "cpu-only"
	assertRecoveryRecord(t, store, withWorkerResult(cpu, "done", ""))
	t.Log("CPU job completed while three lower-ID CUDA jobs stayed queued")

	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, durableDiscoveryInterval)
	var cudaCalls atomic.Int32
	registerTestWorkerWithCapabilities(t, s, "cuda", []string{cudavector.TaskType},
		func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			cudaCalls.Add(1)
			if req.TaskType != cudavector.TaskType || req.AttemptId != 1 {
				t.Errorf("unexpected first CUDA dispatch: %v", req)
			}
			return successfulResult(req), nil
		})
	// Session release wakes discovery to run the third queued job.
	for _, record := range waiting {
		waitQueuedCondition(t, passes, func() bool {
			got, _, err := store.Load(record.JobID)
			return err == nil && got.State == JobSucceeded
		})
		record.State, record.AttemptID, record.WorkerID = JobSucceeded, 1, "cuda"
		assertRecoveryRecord(t, store, withWorkerResult(record, "done", ""))
	}
	stop()
	if cpuCalls.Load() != 1 || cudaCalls.Load() != int32(len(waiting)) {
		t.Fatalf("dispatch counts CPU=%d CUDA=%d", cpuCalls.Load(), cudaCalls.Load())
	}
	t.Log("late registration dispatched the original jobs on the CUDA Worker at Attempt 1")
}

func TestCapabilityRoundRobinAndSnapshots(t *testing.T) {
	s := newTestCoordinator()
	register := func(id, address string, tasks []string) {
		t.Helper()
		got, err := s.RegisterWorker(t.Context(), &runtimepb.RegisterWorkerRequest{
			WorkerId: id, Address: address, ExternalTaskTypes: tasks,
		})
		if err != nil || !got.GetAccepted() {
			t.Fatalf("registration=%v, %v", got, err)
		}
	}
	register("legacy", "localhost:50051", nil)
	tasks := []string{cudavector.TaskType, "unrecognized", cudavector.TaskType}
	register("gpu", "localhost:50052", tasks)
	tasks[0], tasks[2] = externalsquare.TaskType, externalsquare.TaskType
	if s.hasEligibleWorker(externalsquare.TaskType) || !s.hasEligibleWorker(cudavector.TaskType) ||
		!s.hasEligibleWorker("sleep") || s.nextWorker != 0 {
		t.Fatal("precheck mutated cursor or capabilities")
	}
	for _, check := range []struct{ task, id string }{
		{"hash", "legacy"}, {"sleep", "gpu"}, {cudavector.TaskType, "gpu"},
		{"hash", "legacy"}, {cudavector.TaskType, "gpu"},
	} {
		got, ok := s.getNextWorker(check.task)
		if !ok || got.ID != check.id {
			t.Fatalf("selection for %s=%+v, want %s", check.task, got, check.id)
		}
	}
	old, _ := s.getNextWorker(cudavector.TaskType)
	register("gpu", "localhost:50053", []string{externalsquare.TaskType})
	if old.Address != "localhost:50052" || !old.eligibleFor(cudavector.TaskType) ||
		s.hasEligibleWorker(cudavector.TaskType) || len(s.workerOrder) != 2 {
		t.Fatal("re-registration mutated the old snapshot or kept stale CUDA support")
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Go(func() {
		<-start
		for range 50 {
			register("gpu", "localhost:50052", []string{cudavector.TaskType})
			register("gpu", "localhost:50053", []string{externalsquare.TaskType})
		}
	})
	wg.Go(func() {
		<-start
		for range 100 {
			for _, task := range []string{cudavector.TaskType, externalsquare.TaskType} {
				got, ok := s.getNextWorker(task)
				if !ok {
					continue
				}
				wantAddr := "localhost:50052"
				if task == externalsquare.TaskType {
					wantAddr = "localhost:50053"
				}
				if got.ID != "gpu" || got.Address != wantAddr || !got.eligibleFor(task) {
					t.Errorf("torn snapshot for %s: %+v", task, got)
				}
			}
		}
	})
	close(start)
	wg.Wait()

	// Force expiry using the actual liveness scan, then revive each record.
	s.checkWorkerLiveness(0)
	if s.hasEligibleWorker("hash") || s.hasEligibleWorker(externalsquare.TaskType) {
		t.Fatal("dead Worker passed eligibility")
	}
	for _, id := range []string{"legacy", "gpu"} {
		if got, err := s.Heartbeat(t.Context(), &runtimepb.HeartbeatRequest{WorkerId: id}); err != nil || !got.GetAccepted() {
			t.Fatalf("heartbeat=%v, %v", got, err)
		}
	}
	if !s.hasEligibleWorker("hash") || !s.hasEligibleWorker(externalsquare.TaskType) || s.hasEligibleWorker(cudavector.TaskType) {
		t.Fatal("heartbeat revival invented or lost advertised support")
	}
}

func TestCapabilityRetryAndFencing(t *testing.T) {
	for _, outcome := range []string{"success", "stale reply", "RPC exhaustion"} {
		t.Run(outcome, func(t *testing.T) {
			s, store, _ := queuedSchedulerFixture(t, "memory")
			var cpuCalls, cudaCalls atomic.Int32
			registerTestWorker(t, s, "cpu-only", func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				cpuCalls.Add(1)
				return nil, status.Error(codes.FailedPrecondition, "incompatible task")
			})
			registerTestWorkerWithCapabilities(t, s, "cuda", []string{cudavector.TaskType},
				func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					call := int64(cudaCalls.Add(1))
					current, ok, err := store.Load(req.JobId)
					if err != nil || !ok || current.State != JobRunning || current.WorkerID != "cuda" ||
						current.AttemptID != call || req.AttemptId != call || !s.isLeaseCurrent(req.JobId, call) {
						t.Errorf("dispatch not backed by the correct durable Attempt and lease: req=%v row=%+v error=%v",
							req, current, err)
					}
					if call == 1 || outcome == "RPC exhaustion" {
						return nil, status.Error(codes.Unavailable, "injected RPC error")
					}
					result := successfulResult(req)
					if outcome == "stale reply" {
						result.AttemptId, result.Output = 1, "stale"
					}
					return result, nil
				})
			want := submitPlacementJob(t, s, 4, cudavector.TaskType, "0"+strings.Repeat(",0", 63))
			err := s.runQueuedJob(want.JobID)
			want.AttemptID, want.WorkerID = 2, "cuda"
			if outcome == "success" {
				if err != nil {
					t.Fatal(err)
				}
				want.State = JobSucceeded
				want = withWorkerResult(want, "done", "")
			} else {
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("runQueuedJob=%v", err)
				}
				want.State = JobFailed
			}
			assertRecoveryRecord(t, store, want)
			if cpuCalls.Load() != 0 || cudaCalls.Load() != 2 {
				t.Fatalf("retry dispatch counts CPU=%d CUDA=%d", cpuCalls.Load(), cudaCalls.Load())
			}
		})
	}
}

func TestCapabilityRecoveryAndReregistration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	first, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })
	old := &CoordinatorServer{jobStore: first}
	queued := submitPlacementJob(t, old, 8, cudavector.TaskType, "0"+strings.Repeat(",0", 63))
	running := queued
	running.JobID, running.State, running.AttemptID, running.WorkerID = 9, JobRunning, 4, "old"
	saveRecoveryRecord(t, first, running)

	var cudaCalls atomic.Int32
	registerTestWorkerWithCapabilities(t, old, "cuda", []string{cudavector.TaskType},
		func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			cudaCalls.Add(1)
			return successfulResult(req), nil
		})
	worker, ok := old.getNextWorker(cudavector.TaskType)
	if !ok {
		t.Fatal("old CUDA Worker not registered")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	store.db.SetMaxOpenConns(1)
	s, _, passes := queuedSchedulerFixture(t, "memory")
	s.jobStore = store
	if err := s.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	running.State = JobRecovering
	registerTestWorker(t, s, "cpu-only", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		t.Errorf("legacy Worker received incompatible job: %v", req)
		return nil, status.Error(codes.FailedPrecondition, "incompatible task")
	})
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	if err := s.runScheduledRecoveryPass(t.Context(), q); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, store, queued)
	assertRecoveryRecord(t, store, running)
	if len(q.entries) != 1 || q.entries[running.JobID].generation != running.AttemptID ||
		!q.entries[running.JobID].nextRetry.Equal(clock.now().Add(time.Second)) ||
		s.metrics.(*recordingMetrics).count("execution", "") != 0 {
		t.Fatal("unmatched restart records changed recovery cooldown or consumed an execution")
	}
	t.Log("after SQLite reopen, Queued stayed at generation 0 and Recovering at generation 4")
	client := &recordingCoordinatorClient{CoordinatorServiceClient: startTestCoordinator(t, s)}
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, time.Second)
	registration := &runtimepb.RegisterWorkerRequest{WorkerId: worker.ID, Address: worker.Address,
		ExternalTaskTypes: []string{cudavector.TaskType}}
	if err := workerheartbeat.NewRegistration(registration).Send(t.Context(), client); err != nil {
		t.Fatal(err)
	}
	if len(client.heartbeatAccepted) != 1 || client.heartbeatAccepted[0] || len(client.registrations) != 1 ||
		!slices.Equal(client.registrations[0].GetExternalTaskTypes(), registration.GetExternalTaskTypes()) {
		t.Fatalf("missing capability advertisement during reconnect: %v, %v", client.heartbeatAccepted, client.registrations)
	}
	waitQueuedCondition(t, passes, func() bool {
		got, _, err := store.Load(queued.JobID)
		return err == nil && got.State == JobSucceeded
	})
	assertRecoveryRecord(t, store, running)
	clock.advance(time.Second)
	for _, record := range []JobRecord{queued, running} {
		waitQueuedCondition(t, passes, func() bool {
			got, _, err := store.Load(record.JobID)
			return err == nil && got.State == JobSucceeded
		})
		record.State, record.AttemptID, record.WorkerID = JobSucceeded, record.AttemptID+1, "cuda"
		assertRecoveryRecord(t, store, withWorkerResult(record, "done", ""))
	}
	stop()
	if cudaCalls.Load() != 2 {
		t.Fatalf("CUDA dispatch count=%d", cudaCalls.Load())
	}
	t.Log("unknown-Worker heartbeat re-registered capabilities; queued and recovering work resumed at generations 1 and 5")
}

func TestCapabilityLostBeforeAttemptSelection(t *testing.T) {
	for _, tc := range []struct {
		name       string
		state      JobState
		withdrawOn int32
	}{
		{"queued", JobQueued, 1},
		{"recovering", JobRecovering, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, store, _ := queuedSchedulerFixture(t, "memory")
			record := submitPlacementJob(t, s, 1, cudavector.TaskType, "0"+strings.Repeat(",0", 63))
			if tc.state == JobRecovering {
				record.State, record.AttemptID, record.WorkerID = JobRecovering, 4, "previous"
				saveRecoveryRecord(t, store, record)
			}
			registerTestWorkerWithCapabilities(t, s, "cuda", []string{cudavector.TaskType},
				func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					t.Errorf("dispatched after capability was withdrawn: %v", req)
					return successfulResult(req), nil
				})
			endpoint := s.workers["cuda"].Address
			var loads atomic.Int32
			s.jobStore = &recoveryPassStore{JobStore: store, load: func(id int64) (JobRecord, bool, error) {
				// The owned reread is after discovery's precheck and before selection.
				if loads.Add(1) == tc.withdrawOn {
					reply, err := s.RegisterWorker(t.Context(), &runtimepb.RegisterWorkerRequest{
						WorkerId: "cuda", Address: endpoint,
					})
					if err != nil || !reply.GetAccepted() {
						t.Errorf("withdraw CUDA capability: %v, %v", reply, err)
					}
				}
				return store.Load(id)
			}}
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			if err := s.runScheduledRecoveryPass(t.Context(), q); err != nil {
				t.Fatal(err)
			}
			s.lifecycle.beginShutdown() // Join the queued session without canceling it.
			waitRecoverySignal(t, s.lifecycle.done)
			assertRecoveryRecord(t, store, record)
			if loads.Load() < tc.withdrawOn || s.hasEligibleWorker(cudavector.TaskType) {
				t.Fatal("did not withdraw capability between discovery and selection")
			}
			if len(s.leases) != 0 || len(s.jobAttempts) != 0 || s.queuedSessions != 0 {
				t.Fatal("capability loss created an Attempt, lease or leaked a session")
			}
			observer := s.metrics.(*recordingMetrics)
			if observer.count("execution", "") != 0 || observer.count("recovery_failed", "") != 0 ||
				observer.count("pass", "completed") != 1 {
				t.Fatalf("placement wait was reported as execution or failure: %+v", observer.snapshot())
			}
			entry := q.entries[record.JobID]
			if entry.generation != record.AttemptID || !entry.nextRetry.Equal(clock.now().Add(time.Second)) {
				t.Fatalf("placement wait changed recheck timing or generation: %+v", entry)
			}
			if tc.state == JobRecovering && entry.failures != 1 {
				t.Fatalf("placement wait lost recovery backoff: %+v", entry)
			}
		})
	}
}
