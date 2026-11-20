package main

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	"github.com/guangong789/DistributedComputeRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedComputeRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const sessionWorkerID = "worker-a"

func sessionClaim(address string, instance byte, task string) *runtimepb.RegisterWorkerRequest {
	return &runtimepb.RegisterWorkerRequest{
		WorkerId: sessionWorkerID, Address: address, ExternalTaskTypes: []string{task},
		RegistrationKey: bytes.Repeat([]byte{instance}, workerSessionSecretSize),
	}
}

func claimTestSession(t *testing.T, ctx context.Context, client runtimepb.CoordinatorServiceClient, req *runtimepb.RegisterWorkerRequest) []byte {
	t.Helper()
	reply, err := client.RegisterWorker(ctx, req)
	if err != nil || !reply.GetAccepted() || len(reply.GetSessionToken()) != workerSessionSecretSize {
		t.Fatalf("session claim failed: %v", err)
	}
	return reply.GetSessionToken()
}

// Advance LastSeen past the production timeout without running the scanner.
func expireTestSession(s *CoordinatorServer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.workers[sessionWorkerID]
	w.LastSeen = time.Now().Add(-workerLivenessTimeout - time.Second)
	s.workers[sessionWorkerID] = w
}

func TestWorkerSessionTakeoverAndRevocation(t *testing.T) {
	f := newMTLSCoordinator(t)
	ctx := mtlsContext(t)
	a := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	b := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	aClaim := sessionClaim("127.0.0.1:50051", 1, externalsquare.TaskType)
	bClaim := sessionClaim("127.0.0.1:50052", 2, cudavector.TaskType)
	legacy := &runtimepb.RegisterWorkerRequest{WorkerId: sessionWorkerID, Address: aClaim.Address, ExternalTaskTypes: aClaim.ExternalTaskTypes}

	if reply, err := a.RegisterWorker(ctx, legacy); err != nil || !reply.GetAccepted() || len(reply.GetSessionToken()) != 0 {
		t.Fatalf("legacy registration failed: %v", err)
	}
	if reply, err := a.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID}); err != nil || !reply.GetAccepted() {
		t.Fatalf("legacy heartbeat failed: %v", err)
	}
	expireTestSession(f.s)
	aToken := claimTestSession(t, ctx, a, aClaim)
	waitRecoverySignal(t, f.s.recoveryReady)
	other := runtimepb.NewCoordinatorServiceClient(f.connection(t, workerBIdentity))
	if reply, err := other.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-b", Address: "127.0.0.1:50053"}); err != nil || !reply.GetAccepted() {
		t.Fatalf("second logical Worker registration failed: %v", err)
	}
	waitRecoverySignal(t, f.s.recoveryReady)
	if w, ok := f.s.getNextWorker("hash"); !ok || w.ID != sessionWorkerID || f.snapshot().cursor != 1 {
		t.Fatal("test did not advance the round-robin cursor")
	}

	before := f.snapshot()
	if token := claimTestSession(t, ctx, a, aClaim); !bytes.Equal(aToken, token) {
		t.Fatal("retry rotated the token")
	}
	if _, err := b.RegisterWorker(ctx, bClaim); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting live claim: %v", err)
	}
	if _, err := a.RegisterWorker(ctx, legacy); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("legacy downgrade: %v", err)
	}
	if _, err := a.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("legacy heartbeat for session owner: %v", err)
	}
	f.assertUnchanged(t, before, 0)

	expireTestSession(f.s)
	if f.snapshot().workers[sessionWorkerID].Status != WorkerAlive {
		t.Fatal("takeover setup unexpectedly ran the liveness scanner")
	}
	bToken := claimTestSession(t, ctx, b, bClaim)
	after := f.snapshot()
	owner := after.workers[sessionWorkerID]
	if bytes.Equal(aToken, bToken) || owner.Address != bClaim.Address || owner.Status != WorkerAlive ||
		!owner.LastSeen.After(before.workers[sessionWorkerID].LastSeen) ||
		!slices.Equal(before.order, after.order) || before.cursor != after.cursor || after.notifications != 1 ||
		f.s.hasEligibleWorker(externalsquare.TaskType) || !f.s.hasEligibleWorker(cudavector.TaskType) {
		t.Fatal("takeover failed to update ownership, placement or discovery atomically")
	}

	if _, err := a.RegisterWorker(ctx, aClaim); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("fresh-looking former claimant while B is live: %v", err)
	}
	aClaim.SessionToken = aToken
	if _, err := a.RegisterWorker(ctx, aClaim); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("revoked claim while B is live: %v", err)
	}
	if _, err := a.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: aToken}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("revoked heartbeat while B is live: %v", err)
	}
	f.assertUnchanged(t, after, 0)

	expireTestSession(f.s)
	f.s.checkWorkerLiveness(workerLivenessTimeout)
	dead := f.snapshot()
	if dead.workers[sessionWorkerID].Status != WorkerDead {
		t.Fatal("replacement should be marked dead for the stale revival check")
	}
	if _, err := a.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: aToken}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("revoked heartbeat: %v", err)
	}
	if _, err := a.RegisterWorker(ctx, aClaim); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("revoked claim after B expired: %v", err)
	}
	f.assertUnchanged(t, dead, 0)

	if reply, err := b.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: bToken}); err != nil || !reply.GetAccepted() {
		t.Fatalf("current owner's heartbeat failed: %v", err)
	}
	revived := f.snapshot().workers[sessionWorkerID]
	if revived.Status != WorkerAlive || !revived.LastSeen.After(owner.LastSeen) || revived.Address != owner.Address || revived.externalTasks != owner.externalTasks {
		t.Fatal("valid heartbeat failed to revive the current owner's record")
	}
	if token := claimTestSession(t, ctx, b, bClaim); !bytes.Equal(bToken, token) {
		t.Fatal("heartbeat rotated the session token")
	}
}

func TestWorkerSessionConcurrentClaims(t *testing.T) {
	for _, state := range []string{"absent", "expired"} {
		t.Run(state, func(t *testing.T) {
			f := newMTLSCoordinator(t)
			ctx := mtlsContext(t)
			clients := []runtimepb.CoordinatorServiceClient{
				runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker)),
				runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker)),
			}
			claims := []*runtimepb.RegisterWorkerRequest{
				sessionClaim("127.0.0.1:50051", 1, externalsquare.TaskType),
				sessionClaim("127.0.0.1:50052", 2, cudavector.TaskType),
			}
			if state == "expired" {
				claimTestSession(t, ctx, clients[0], sessionClaim("127.0.0.1:50053", 3, ""))
				expireTestSession(f.s)
				waitRecoverySignal(t, f.s.recoveryReady)
			}

			type outcome struct {
				index int
				reply *runtimepb.RegisterWorkerResponse
				err   error
			}
			results := make(chan outcome, len(clients))
			start := make(chan struct{})
			for index, client := range clients {
				go func() {
					<-start
					reply, err := client.RegisterWorker(ctx, claims[index])
					results <- outcome{index, reply, err}
				}()
			}
			heartbeat := make(chan error, 1)
			go func() {
				<-start
				reply, err := clients[0].Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID})
				if reply.GetAccepted() {
					err = status.Error(codes.Internal, "heartbeat without token was accepted")
				}
				heartbeat <- err
			}()
			scan := make(chan struct{})
			go func() {
				<-start
				f.s.checkWorkerLiveness(workerLivenessTimeout)
				close(scan)
			}()
			close(start)

			var winners []outcome
			for range clients {
				result := <-results
				switch status.Code(result.err) {
				case codes.OK:
					if !result.reply.GetAccepted() || len(result.reply.GetSessionToken()) != workerSessionSecretSize {
						t.Fatal("success did not return a session token")
					}
					winners = append(winners, result)
				case codes.AlreadyExists:
				default:
					t.Fatalf("claim returned unexpected status: %v", result.err)
				}
			}
			if err := <-heartbeat; (state == "expired" || err != nil) && status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("heartbeat without token during claim: %v", err)
			}
			<-scan
			if len(winners) != 1 {
				t.Fatalf("accepted claims = %d, want 1", len(winners))
			}
			winner := winners[0]
			snapshot := f.snapshot()
			owner := snapshot.workers[sessionWorkerID]
			claim := claims[winner.index]
			if owner.Address != claim.Address || owner.externalTasks != externalTaskCapability(claim.ExternalTaskTypes[0]) ||
				owner.Status != WorkerAlive || !slices.Equal(snapshot.order, []string{sessionWorkerID}) || snapshot.notifications != 1 {
				t.Fatal("concurrent claim did not publish one consistent owner")
			}
			if reply, err := clients[winner.index].Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: winner.reply.GetSessionToken()}); err != nil || !reply.GetAccepted() {
				t.Fatalf("installed owner could not heartbeat: %v", err)
			}
		})
	}
}

func TestWorkerSessionCoordinatorRestart(t *testing.T) {
	ctx := mtlsContext(t)
	original := newMTLSCoordinator(t)
	restarted := newMTLSCoordinator(t)
	a := runtimepb.NewCoordinatorServiceClient(original.connection(t, testpki.Worker))
	b := runtimepb.NewCoordinatorServiceClient(restarted.connection(t, testpki.Worker))
	req := sessionClaim("127.0.0.1:50051", 1, externalsquare.TaskType)
	registration := workerheartbeat.NewRegistration(req)
	if err := registration.Register(ctx, a); err != nil {
		t.Fatal(err)
	}
	old := bytes.Clone(req.GetSessionToken())
	original.server.Stop()

	if reply, err := b.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: old}); err != nil || reply.GetAccepted() {
		t.Fatalf("empty Coordinator did not report unknown Worker: %v", err)
	}
	if err := registration.Send(ctx, b); err != nil || bytes.Equal(old, req.GetSessionToken()) {
		t.Fatalf("Worker failed to acquire a new token after restart: %v", err)
	}
	before := restarted.snapshot().workers[sessionWorkerID]
	if err := registration.Send(ctx, b); err != nil {
		t.Fatal(err)
	}
	if !restarted.snapshot().workers[sessionWorkerID].LastSeen.After(before.LastSeen) {
		t.Fatal("heartbeat did not resume after the new registration")
	}
	if _, err := b.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: old}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("restarted Coordinator accepted the old token: %v", err)
	}
}

func TestWorkerSessionTakeoverPreservesAttemptFencing(t *testing.T) {
	for _, tc := range []struct {
		name        string
		superseded  bool
		wantAttempt int64
		wantOutput  string
	}{
		{name: "in-lease completion", wantAttempt: 1, wantOutput: "incumbent"},
		{name: "superseded result", superseded: true, wantAttempt: 2, wantOutput: "replacement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMTLSCoordinator(t, coordinatorExecutionConfig{
				ListenAddress: "127.0.0.1:0", AttemptLease: 2 * time.Second, ShutdownTimeout: defaultShutdownTimeout,
			})
			ctx := mtlsContext(t)
			a := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
			b := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
			client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))

			aStarted, bStarted := make(chan int64, 1), make(chan int64, 1)
			aRelease := make(chan struct{})
			defer func() {
				select {
				case <-aRelease:
				default:
					close(aRelease)
				}
			}()
			aReturned := make(chan struct{})
			aAddress := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				aStarted <- req.AttemptId
				// The Worker can still finish after its dispatch RPC was cancelled.
				<-aRelease
				reply := successfulResult(req)
				reply.Output = "incumbent"
				defer close(aReturned)
				return reply, nil
			})
			bAddress := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				bStarted <- req.AttemptId
				reply := successfulResult(req)
				reply.Output = "replacement"
				return reply, nil
			})

			aToken := claimTestSession(t, ctx, a, sessionClaim(aAddress, 1, ""))
			// Warm the replacement's mTLS channel before starting the lease.
			if _, err := b.RegisterWorker(ctx, sessionClaim(bAddress, 2, "")); status.Code(err) != codes.AlreadyExists {
				t.Fatalf("replacement unexpectedly claimed the live record: %v", err)
			}
			const jobID = 20801
			req := &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "original spec", TimeoutMs: 3000}
			if reply, err := client.SubmitJob(ctx, req); err != nil || reply.GetStatus() != "Accepted" || reply.GetAttemptId() != 0 {
				t.Fatalf("durable admission failed: %v", err)
			}
			done := make(chan error, 1)
			go func() { done <- f.s.runQueuedJob(jobID) }()
			select {
			case attemptID := <-aStarted:
				if attemptID != 1 {
					t.Fatalf("A got Attempt %d, want 1", attemptID)
				}
			case <-ctx.Done():
				t.Fatal("A did not receive its authorized Attempt")
			}
			before, ok := f.s.getLease(jobID)
			if !ok || before.AttemptID != 1 || before.WorkerID != sessionWorkerID {
				t.Fatal("A did not hold the expected lease")
			}

			expireTestSession(f.s)
			claimTestSession(t, ctx, b, sessionClaim(bAddress, 2, ""))
			if after, ok := f.s.getLease(jobID); !ok || after != before {
				t.Fatal("session takeover changed the active Attempt lease")
			}
			if _, err := a.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: sessionWorkerID, SessionToken: aToken}); status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("A retained registration ownership: %v", err)
			}

			assertTerminal := func() {
				t.Helper()
				reply, err := client.GetJob(ctx, &runtimepb.GetJobRequest{JobId: jobID})
				if err != nil || reply.GetState() != runtimepb.JobState_JOB_STATE_SUCCEEDED || reply.GetWorkerId() != sessionWorkerID ||
					reply.GetAttemptId() != tc.wantAttempt || !reply.GetHasWorkerResult() || reply.GetOutput() != tc.wantOutput {
					t.Fatalf("unexpected durable terminal result: %v (err=%v)", reply, err)
				}
				record, ok, err := f.store.Load(jobID)
				if err != nil || !ok || record.TaskType != req.TaskType || record.Payload != req.Payload || record.TimeoutMs != req.TimeoutMs {
					t.Fatalf("execution did not preserve the original JobSpec: %v", err)
				}
			}
			if tc.superseded {
				// The normal dispatch deadline triggers retry. Keep A blocked
				// until B's newer Attempt has been durably accepted.
				select {
				case attemptID := <-bStarted:
					if attemptID != 2 {
						t.Fatalf("B got Attempt %d, want 2", attemptID)
					}
				case <-ctx.Done():
					t.Fatal("retry did not use the replacement endpoint")
				}
			} else {
				close(aRelease)
				waitRecoverySignal(t, aReturned)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("queued session did not settle")
			}
			assertTerminal()
			if tc.superseded {
				close(aRelease)
				waitRecoverySignal(t, aReturned)
				assertTerminal()
			} else if len(bStarted) != 0 {
				t.Fatal("in-lease completion triggered a retry")
			}
		})
	}
}
