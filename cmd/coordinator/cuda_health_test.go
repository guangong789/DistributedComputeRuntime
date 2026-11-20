package main

import (
	"bytes"
	"context"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	"github.com/guangong789/DistributedComputeRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	testHealthTTL     = 5 * time.Second
	healthReady       = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY
	healthUnavailable = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE
)

func healthCoordinator(t *testing.T) (*mtlsCoordinator, *manualRecoveryClock) {
	t.Helper()
	f := newMTLSCoordinator(t)
	clock := newManualRecoveryClock()
	f.s.mu.Lock()
	f.s.healthTTL, f.s.workerNow = testHealthTTL, clock.now
	f.s.mu.Unlock()
	return f, clock
}

func managedCUDAClaim(address string, instance byte) *runtimepb.RegisterWorkerRequest {
	req := sessionClaim(address, instance, externalsquare.TaskType)
	req.CudaHealthManaged = true
	return req
}

func healthReport(sequence, anchor uint64, state runtimepb.CudaHealthState) *runtimepb.CudaHealthObservation {
	return &runtimepb.CudaHealthObservation{ObservationSeq: sequence, State: state, ProbeAnchor: anchor}
}

func managedHeartbeat(t *testing.T, ctx context.Context, client runtimepb.CoordinatorServiceClient, token []byte, obs *runtimepb.CudaHealthObservation) uint64 {
	t.Helper()
	reply, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: token, CudaHealth: obs})
	if err != nil || !reply.GetAccepted() || reply.GetCudaHealthProbeAnchor() == 0 {
		t.Fatalf("managed heartbeat failed: %v", err)
	}
	return reply.GetCudaHealthProbeAnchor()
}

func TestCUDAHealthObservationOrderingAndExpiry(t *testing.T) {
	f, clock := healthCoordinator(t)
	ctx := mtlsContext(t)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	req := managedCUDAClaim("127.0.0.1:50051", 1)
	token := claimTestSession(t, ctx, client, req)
	waitRecoverySignal(t, f.s.recoveryReady)

	eligible := func(want bool, wakes int) {
		t.Helper()
		before := f.snapshot()
		if f.s.hasEligibleWorker(cudavector.TaskType) != want || !f.s.hasEligibleWorker("hash") ||
			!f.s.hasEligibleWorker(externalsquare.TaskType) || before.notifications != wakes || f.snapshot().cursor != before.cursor {
			t.Fatalf("unexpected eligibility, cursor or wakeup (want CUDA=%t wakeups=%d)", want, wakes)
		}
	}
	eligible(false, 0)
	origin := clock.now()
	anchor := managedHeartbeat(t, ctx, client, token, nil)
	if anchor != 1 {
		t.Fatal("session origin anchor must reserve zero")
	}
	for _, bad := range []*runtimepb.CudaHealthObservation{
		healthReport(0, anchor, healthReady),
		healthReport(100, anchor, runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNSPECIFIED),
		healthReport(100, anchor, runtimepb.CudaHealthState(99)),
		healthReport(100, 0, healthReady),
		healthReport(100, anchor+1, healthReady),
		healthReport(100, math.MaxUint64, healthReady),
	} {
		managedHeartbeat(t, ctx, client, token, bad)
	}
	eligible(false, 0)

	clock.advance(time.Second)
	newAnchor := managedHeartbeat(t, ctx, client, token, healthReport(1, anchor, healthReady))
	eligible(true, 1)
	waitRecoverySignal(t, f.s.recoveryReady)
	before := f.snapshot()
	if replay := claimTestSession(t, ctx, client, req); !bytes.Equal(replay, token) {
		t.Fatal("registration replay changed the token")
	}
	f.assertUnchanged(t, before, 0)

	clock.advance(testHealthTTL - time.Second - time.Nanosecond)
	managedHeartbeat(t, ctx, client, token, healthReport(1, newAnchor, healthUnavailable))
	eligible(true, 0) // Duplicate sequence cannot change the state.
	clock.advance(time.Nanosecond)
	anchorAfterExpiry := managedHeartbeat(t, ctx, client, token, healthReport(1, newAnchor, healthReady))
	eligible(false, 0) // Re-anchoring a duplicate cannot keep it fresh.
	clock.advance(time.Nanosecond)
	managedHeartbeat(t, ctx, client, token, healthReport(100, anchor, healthReady))
	eligible(false, 0)

	worker := f.snapshot().workers[sessionWorkerID]
	if worker.cudaHealth.sequence != 1 || !worker.cudaHealth.expiresAt.Equal(origin.Add(testHealthTTL)) ||
		worker.Status != WorkerAlive || !worker.LastSeen.Equal(clock.now()) {
		t.Fatal("invalid evidence changed the watermark or expiry, or stopped liveness")
	}
	if _, ok := f.s.getNextWorker(cudavector.TaskType); ok {
		t.Fatal("actual selection ignored TTL expiry")
	}

	managedHeartbeat(t, ctx, client, token, healthReport(2, anchorAfterExpiry, healthReady))
	eligible(true, 1)
	waitRecoverySignal(t, f.s.recoveryReady)
	latestAnchor := managedHeartbeat(t, ctx, client, token, nil)
	managedHeartbeat(t, ctx, client, token, healthReport(3, latestAnchor, healthReady))
	eligible(true, 0) // Continuous Ready refresh needs no wakeup.
	managedHeartbeat(t, ctx, client, token, healthReport(5, latestAnchor, healthUnavailable))
	managedHeartbeat(t, ctx, client, token, healthReport(4, latestAnchor, healthReady))
	eligible(false, 0)
	managedHeartbeat(t, ctx, client, token, healthReport(6, latestAnchor, healthReady))
	eligible(true, 1)
	waitRecoverySignal(t, f.s.recoveryReady)

	clock.advance(workerLivenessTimeout + time.Nanosecond)
	f.s.checkWorkerLiveness(workerLivenessTimeout)
	if f.s.hasEligibleWorker("hash") || f.s.hasEligibleWorker(cudavector.TaskType) {
		t.Fatal("fresh CUDA evidence bypassed Worker liveness")
	}
	managedHeartbeat(t, ctx, client, token, nil)
	eligible(true, 1) // Same owner may revive while evidence is still fresh.
	waitRecoverySignal(t, f.s.recoveryReady)
	clock.advance(testHealthTTL)
	f.s.checkWorkerLiveness(workerLivenessTimeout)
	managedHeartbeat(t, ctx, client, token, nil)
	eligible(false, 1) // Preserve the ordinary CPU wakeup after TTL expiry.
}

func TestCUDAHealthRegistrationCompatibility(t *testing.T) {
	ctx := mtlsContext(t)
	for _, sessionAware := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "session-aware unmanaged"}[sessionAware], func(t *testing.T) {
			f, _ := healthCoordinator(t)
			client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
			req := sessionClaim("127.0.0.1:50051", 1, cudavector.TaskType)
			req.ExternalTaskTypes = append(req.ExternalTaskTypes, cudavector.TaskType) // B1 deduplicates.
			if !sessionAware {
				req.RegistrationKey = nil
			}
			reply, err := client.RegisterWorker(ctx, req)
			if err != nil || !reply.GetAccepted() || (len(reply.GetSessionToken()) != 0) != sessionAware ||
				!f.s.hasEligibleWorker(cudavector.TaskType) {
				t.Fatalf("unmanaged CUDA advertisement failed: %v", err)
			}
			waitRecoverySignal(t, f.s.recoveryReady)
			response, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{
				WorkerId: sessionWorkerID, SessionToken: reply.GetSessionToken(), CudaHealth: healthReport(1, 1, healthUnavailable),
			})
			if err != nil || !response.GetAccepted() || response.GetCudaHealthProbeAnchor() != 0 ||
				!f.s.hasEligibleWorker(cudavector.TaskType) || len(f.s.recoveryReady) != 0 {
				t.Fatalf("unmanaged heartbeat changed B1 placement: %v", err)
			}
		})
	}

	f, _ := healthCoordinator(t)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	req := managedCUDAClaim("127.0.0.1:50051", 1)
	req.ExternalTaskTypes = append(req.ExternalTaskTypes, cudavector.TaskType, cudavector.TaskType)
	before := f.snapshot()
	if _, err := client.RegisterWorker(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("dual advertisement must be rejected: %v", err)
	}
	req.ExternalTaskTypes = req.ExternalTaskTypes[:1]
	key := req.RegistrationKey
	req.RegistrationKey = nil
	if _, err := client.RegisterWorker(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("legacy managed claim must be rejected: %v", err)
	}
	f.assertUnchanged(t, before, 0)
	req.RegistrationKey = key
	if _, err := (&CoordinatorServer{}).RegisterWorker(ctx, req); status.Code(err) != codes.Unimplemented {
		t.Fatalf("managed protocol accepted without a configured TTL: %v", err)
	}
	claimTestSession(t, ctx, client, req)
	before = f.snapshot()
	req.CudaHealthManaged = false
	req.ExternalTaskTypes = append(req.ExternalTaskTypes, cudavector.TaskType) // Same effective capabilities.
	if _, err := client.RegisterWorker(ctx, req); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("same-session downgrade must be rejected: %v", err)
	}
	f.assertUnchanged(t, before, 0)

	// An older Coordinator sees only the ordinary advertisement of a managed Worker.
	b1Only := &CoordinatorServer{}
	reply, err := b1Only.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{
		WorkerId: req.WorkerId, Address: req.Address, RegistrationKey: req.RegistrationKey,
		ExternalTaskTypes: []string{externalsquare.TaskType},
	})
	if err != nil || !reply.GetAccepted() || b1Only.hasEligibleWorker(cudavector.TaskType) ||
		!b1Only.hasEligibleWorker(externalsquare.TaskType) {
		t.Fatal("old registration interpretation must fail closed only for CUDA")
	}
}

func TestCUDAHealthSessionReset(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(map[bool]string{false: "takeover", true: "restart"}[restart], func(t *testing.T) {
			original, clock := healthCoordinator(t)
			ctx := mtlsContext(t)
			a := runtimepb.NewCoordinatorServiceClient(original.connection(t, testpki.Worker))
			first := managedCUDAClaim("127.0.0.1:50051", 1)
			old := claimTestSession(t, ctx, a, first)
			clock.advance(time.Second)
			oldAnchor := managedHeartbeat(t, ctx, a, old, nil)
			report := healthReport(2, oldAnchor, healthReady)
			managedHeartbeat(t, ctx, a, old, report)
			if !original.s.hasEligibleWorker(cudavector.TaskType) {
				t.Fatal("setup did not publish old owner's readiness")
			}

			replacement := managedCUDAClaim("127.0.0.1:50052", 2)
			f := original
			if restart {
				original.server.Stop()
				f, _ = healthCoordinator(t)
				replacement = first
				replacement.SessionToken = old
			} else {
				clock.advance(workerLivenessTimeout + time.Nanosecond)
			}
			b := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
			if restart {
				reply, err := b.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: old, CudaHealth: report})
				if err != nil || reply.GetAccepted() || reply.GetCudaHealthProbeAnchor() != 0 {
					t.Fatalf("restarted Coordinator reported the old registration as known: %v", err)
				}
			}
			token := claimTestSession(t, ctx, b, replacement)
			w := f.snapshot().workers[sessionWorkerID]
			if bytes.Equal(old, token) || f.s.hasEligibleWorker(cudavector.TaskType) ||
				w.cudaHealth.sequence != 0 || !w.cudaHealth.expiresAt.IsZero() || w.Address != replacement.Address {
				t.Fatal("new session inherited old CUDA evidence")
			}
			before := f.snapshot()
			if _, err := b.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: old, CudaHealth: report}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("old session heartbeat: %v", err)
			}
			f.assertUnchanged(t, before, 0)
			newAnchor := managedHeartbeat(t, ctx, b, token, report) // Old anchor is in the new session's future.
			if newAnchor != 1 || f.snapshot().workers[sessionWorkerID].cudaHealth.sequence != 0 {
				t.Fatal("new session did not establish its own anchor origin")
			}
			managedHeartbeat(t, ctx, b, token, healthReport(1, newAnchor, healthReady))
			if !f.s.hasEligibleWorker(cudavector.TaskType) {
				t.Fatal("fresh evidence could not enable new session")
			}
		})
	}
}

func TestCUDAHealthConcurrentExpiryAndTakeover(t *testing.T) {
	f, clock := healthCoordinator(t)
	ctx := mtlsContext(t)
	a := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	b := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	old := claimTestSession(t, ctx, a, managedCUDAClaim("127.0.0.1:50051", 1))
	anchor := managedHeartbeat(t, ctx, a, old, nil)
	managedHeartbeat(t, ctx, a, old, healthReport(1, anchor, healthReady))
	clock.advance(testHealthTTL)
	waitRecoverySignal(t, f.s.recoveryReady)

	start := make(chan struct{})
	beats, claims := make(chan error, 1), make(chan error, 1)
	go func() {
		<-start
		_, err := a.Heartbeat(ctx, &runtimepb.HeartbeatRequest{
			WorkerId: sessionWorkerID, SessionToken: old, CudaHealth: healthReport(2, anchor, healthReady),
		})
		beats <- err
	}()
	go func() {
		<-start
		_, err := b.RegisterWorker(ctx, managedCUDAClaim("127.0.0.1:50052", 2))
		claims <- err
	}()
	close(start)
	for range 32 {
		if _, ok := f.s.getNextWorker(cudavector.TaskType); ok || f.s.hasEligibleWorker(cudavector.TaskType) {
			t.Fatal("selection saw expired or uninitialized CUDA readiness")
		}
	}
	beat, claim := status.Code(<-beats), status.Code(<-claims)
	w := f.snapshot().workers[sessionWorkerID]
	switch {
	case beat == codes.OK && claim == codes.AlreadyExists:
		if w.Address != "127.0.0.1:50051" || w.cudaHealth.sequence != 1 {
			t.Fatal("incumbent renewal changed expired evidence")
		}
	case beat == codes.FailedPrecondition && claim == codes.OK:
		if w.Address != "127.0.0.1:50052" || w.cudaHealth.sequence != 0 {
			t.Fatal("takeover did not discard old evidence")
		}
	default:
		t.Fatalf("ownership was not serialized: heartbeat=%s claim=%s", beat, claim)
	}
}

func TestCUDAHealthQueuedAndRecoveringPlacement(t *testing.T) {
	f, clock := healthCoordinator(t)
	ctx := mtlsContext(t)
	passes := make(chan struct{}, 64)
	f.observer.hook = func(e recordedMetric) {
		if e.name == "pass" || e.name == "terminal" {
			select {
			case passes <- struct{}{}:
			default:
			}
		}
	}
	var cudaDispatches atomic.Int32
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if req.TaskType == "hash" {
			return executeHashJob(ctx, req)
		}
		cudaDispatches.Add(1)
		return successfulResult(req), nil
	})
	worker := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	token := claimTestSession(t, ctx, worker, managedCUDAClaim(address, 1))
	anchor := managedHeartbeat(t, ctx, worker, token, nil)

	payload := "0" + strings.Repeat(",0", 63)
	queued := submitPlacementJob(t, f.s, 11, cudavector.TaskType, payload)
	recovering := queued
	recovering.JobID, recovering.State, recovering.AttemptID, recovering.WorkerID = 12, JobRecovering, 4, "previous"
	saveRecoveryRecord(t, f.store, recovering)
	q := testRecoverySchedule(clock)
	if err := f.s.runScheduledRecoveryPass(ctx, q); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, f.store, queued)
	assertRecoveryRecord(t, f.store, recovering)
	idle := f.snapshot()
	if len(idle.active) != 0 || len(idle.attempts) != 0 || len(idle.leases) != 0 || cudaDispatches.Load() != 0 ||
		!q.entries[recovering.JobID].nextRetry.Equal(clock.now().Add(time.Second)) {
		t.Fatal("Unknown consumed execution resources or changed recovery cooldown")
	}

	stop := startManualRecovery(t, f.s, q, clock)
	defer stop()
	waitRecoveryReset(t, clock, time.Second)
	managedHeartbeat(t, ctx, worker, token, healthReport(1, anchor, healthReady))
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(f.s, f.store, queued.JobID) })
	queued.State, queued.AttemptID, queued.WorkerID = JobSucceeded, 1, sessionWorkerID
	assertRecoveryRecord(t, f.store, withWorkerResult(queued, "done", ""))
	assertRecoveryRecord(t, f.store, recovering) // Readiness did not bypass cooldown.

	clock.advance(time.Second)
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(f.s, f.store, recovering.JobID) })
	recovering.State, recovering.AttemptID, recovering.WorkerID = JobSucceeded, 5, sessionWorkerID
	assertRecoveryRecord(t, f.store, withWorkerResult(recovering, "done", ""))

	anchor = managedHeartbeat(t, ctx, worker, token, nil)
	managedHeartbeat(t, ctx, worker, token, healthReport(2, anchor, healthUnavailable))
	waiting := submitPlacementJob(t, f.s, 21, cudavector.TaskType, payload)
	cpu := submitPlacementJob(t, f.s, 22, "hash", "CPU survives CUDA withdrawal")
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(f.s, f.store, cpu.JobID) })
	assertRecoveryRecord(t, f.store, waiting)
	snapshot := f.snapshot()
	if _, attempted := snapshot.attempts[waiting.JobID]; attempted || len(snapshot.active) != 0 || cudaDispatches.Load() != 2 {
		t.Fatal("CUDA withdrawal consumed a queued permit or dispatched an Attempt")
	}
}

func TestCUDAHealthWithdrawalPreservesAttempt(t *testing.T) {
	for _, rpcFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "accept in-lease result", true: "reselect for retry"}[rpcFailure], func(t *testing.T) {
			f, _ := healthCoordinator(t)
			ctx := mtlsContext(t)
			started := make(chan struct{}, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				if req.AttemptId != 1 {
					t.Error("retry dispatched to an unhealthy Worker")
					return nil, status.Error(codes.Internal, "unexpected retry")
				}
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					t.Error("withdrawing health cancelled the Attempt")
					return nil, ctx.Err()
				}
				if rpcFailure {
					return nil, status.Error(codes.Unavailable, "controlled execution failure")
				}
				return successfulResult(req), nil
			})
			worker := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
			token := claimTestSession(t, ctx, worker, managedCUDAClaim(address, 1))
			anchor := managedHeartbeat(t, ctx, worker, token, nil)
			managedHeartbeat(t, ctx, worker, token, healthReport(1, anchor, healthReady))
			waiting := submitPlacementJob(t, f.s, 31, cudavector.TaskType, "0"+strings.Repeat(",0", 63))
			done := make(chan error, 1)
			go func() { done <- f.s.runQueuedJob(waiting.JobID) }()
			waitRecoverySignal(t, started)
			lease, ok := f.s.getLease(waiting.JobID)
			if !ok || lease.AttemptID != 1 {
				t.Fatal("CUDA Attempt was not published")
			}
			running := waiting
			running.State, running.AttemptID, running.WorkerID = JobRunning, 1, sessionWorkerID
			assertRecoveryRecord(t, f.store, running)

			managedHeartbeat(t, ctx, worker, token, healthReport(2, anchor, healthUnavailable))
			assertRecoveryRecord(t, f.store, running)
			if current, ok := f.s.getLease(waiting.JobID); !ok || current != lease ||
				!f.s.isLeaseCurrent(waiting.JobID, 1) || f.s.hasEligibleWorker(cudavector.TaskType) {
				t.Fatal("health withdrawal changed the published lease or left CUDA eligible")
			}
			unblock()
			select {
			case err := <-done:
				if !rpcFailure && err != nil {
					t.Fatal(err)
				}
				if rpcFailure && status.Code(err) != codes.Unavailable {
					t.Fatalf("unexpected retry settlement: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("CUDA Attempt did not settle")
			}
			if rpcFailure {
				running.State = JobFailed
				assertRecoveryRecord(t, f.store, running) // No second generation was published.
			} else {
				running.State = JobSucceeded
				assertRecoveryRecord(t, f.store, withWorkerResult(running, "done", ""))
			}
		})
	}
}
