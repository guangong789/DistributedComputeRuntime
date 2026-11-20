package main

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerSessionStartupConflict(t *testing.T) {
	for _, waitForClaim := range []bool{true, false} {
		name := "cancel while waiting"
		if waitForClaim {
			name = "claim after owner expires"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var canClaim atomic.Bool
			var calls, heartbeats atomic.Int32
			times := make(chan time.Time, 8)
			token := bytes.Repeat([]byte{3}, 32)
			firstHeartbeat := make(chan struct{})
			service := shutdownCoordinator{
				register: func(ctx context.Context, req *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
					calls.Add(1)
					if len(req.GetRegistrationKey()) != 32 || len(req.GetSessionToken()) != 0 {
						t.Error("startup dropped its process key or sent a token before acceptance")
					}
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 3*time.Second {
						t.Error("startup risked an unbounded registration RPC")
					}
					allowed := canClaim.Load()
					select {
					case times <- time.Now():
					default:
					}
					if !allowed {
						return nil, status.Error(codes.AlreadyExists, "test incumbent is live")
					}
					return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: token}, nil
				},
				heartbeat: func(_ context.Context, req *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
					if !canClaim.Load() || !bytes.Equal(req.GetSessionToken(), token) {
						t.Error("Worker began heartbeat before acquiring its session")
					}
					if heartbeats.Add(1) == 1 {
						close(firstHeartbeat)
					}
					return &runtimepb.HeartbeatResponse{Accepted: true}, nil
				},
			}
			address, ca, _, _, _ := startShutdownCoordinator(t, service)
			cfg := workerTestConfig(t, ca, address)
			done := make(chan error, 1)
			go func() { done <- runWorker(ctx, cfg) }()
			first := awaitWorker(t, times)
			wantCalls := int32(1)
			if waitForClaim {
				second := awaitWorker(t, times)
				if second.Sub(first) < time.Second || heartbeats.Load() != 0 {
					t.Fatal("startup retried early or began heartbeat while the incumbent was live")
				}
				canClaim.Store(true)
				awaitWorker(t, firstHeartbeat)
				wantCalls = 3
			}
			cancel()
			if err := awaitWorker(t, done); err != nil {
				t.Fatalf("Worker failed to exit cleanly: %v", err)
			}
			if calls.Load() != wantCalls || (!waitForClaim && heartbeats.Load() != 0) {
				t.Fatal("unexpected registration retries or heartbeat at shutdown")
			}
		})
	}
}

func TestWorkerSessionRevocationKeepsWorkerRunning(t *testing.T) {
	var registrations, heartbeats atomic.Int32
	revoked := make(chan struct{})
	token := bytes.Repeat([]byte{1}, 32)
	service := shutdownCoordinator{
		register: func(_ context.Context, req *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
			if registrations.Add(1) != 1 || len(req.GetRegistrationKey()) != 32 {
				t.Error("revoked Worker attempted to register again")
			}
			return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: token}, nil
		},
		heartbeat: func(_ context.Context, req *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
			if !bytes.Equal(token, req.GetSessionToken()) {
				t.Error("heartbeat lacked the accepted token")
			}
			if heartbeats.Add(1) == 1 {
				close(revoked)
			}
			return nil, status.Error(codes.FailedPrecondition, "revoked")
		},
	}
	address, ca, connections, _, _ := startShutdownCoordinator(t, service)
	cfg := workerTestConfig(t, ca, address)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runWorker(ctx, cfg) }()
	awaitWorker(t, revoked)
	// The registration and heartbeat connections close when their RPC loops end.
	// Worker shutdown must still wait for cancellation of its lifecycle.
	awaitWorker(t, connections.ended)
	awaitWorker(t, connections.ended)
	select {
	case err := <-done:
		t.Fatalf("revocation stopped the Worker: %v", err)
	default:
	}
	cancel()
	if err := awaitWorker(t, done); err != nil {
		t.Fatalf("Worker failed to exit cleanly: %v", err)
	}
	if registrations.Load() != 1 || heartbeats.Load() != 1 {
		t.Fatal("revoked Worker kept registering or heartbeating")
	}
}

func TestWorkerSessionReconnectAfterLostReply(t *testing.T) {
	old := bytes.Repeat([]byte{1}, 32)
	next := bytes.Repeat([]byte{2}, 32)
	key := bytes.Repeat([]byte{3}, 32)
	req := &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", RegistrationKey: key}
	r := workerheartbeat.NewRegistration(req)
	var registers, beats int
	client := &fakeCoordinatorClient{
		register: func(_ context.Context, got *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
			registers++
			if !bytes.Equal(got.GetRegistrationKey(), key) {
				t.Fatal("reconnect changed process key")
			}
			if registers == 1 {
				return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: old}, nil
			}
			if !bytes.Equal(got.GetSessionToken(), old) {
				t.Fatal("reconnect dropped its last accepted token")
			}
			if registers == 2 {
				return nil, status.Error(codes.Unavailable, "registration reply lost")
			}
			return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: next}, nil
		},
		heartbeat: func(_ context.Context, got *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
			beats++
			switch beats {
			case 1:
				return nil, status.Error(codes.Unavailable, "network unavailable")
			case 2:
				if !bytes.Equal(got.GetSessionToken(), old) {
					t.Fatal("lost heartbeat changed the token")
				}
				return &runtimepb.HeartbeatResponse{Accepted: false}, nil
			default:
				if !bytes.Equal(got.GetSessionToken(), next) {
					t.Fatal("Worker used the old token after reconnect")
				}
				return &runtimepb.HeartbeatResponse{Accepted: true}, nil
			}
		},
	}
	ctx := context.Background()
	if err := r.Register(ctx, client); err != nil {
		t.Fatal(err)
	}
	for i, want := range []codes.Code{codes.Unavailable, codes.Unavailable, codes.OK, codes.OK} {
		if err := r.Send(ctx, client); status.Code(err) != want {
			t.Fatalf("step %d: %v, want %v", i, err, want)
		}
	}
	if beats != 3 || registers != 3 {
		t.Fatal("lost heartbeat should preserve ownership, lost registration reply should retry registration")
	}
}

func TestWorkerSessionRequiresUpgradedCoordinator(t *testing.T) {
	client := &fakeCoordinatorClient{
		register: func(_ context.Context, _ *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
			return &runtimepb.RegisterWorkerResponse{Accepted: true}, nil
		},
	}
	r := workerheartbeat.NewRegistration(&runtimepb.RegisterWorkerRequest{RegistrationKey: make([]byte, 32)})
	if err := r.Register(context.Background(), client); status.Code(err) != codes.Unimplemented {
		t.Fatalf("Worker silently accepted registration without a session: %v", err)
	}
}
