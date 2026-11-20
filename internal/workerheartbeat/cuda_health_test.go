package workerheartbeat

import (
	"bytes"
	"context"
	"math"
	"testing"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ready       = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY
	unavailable = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE
	unspecified = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNSPECIFIED
)

type healthCoordinatorStub struct {
	token           []byte
	beat            *runtimepb.HeartbeatRequest
	anchor          uint64
	unknown         bool
	heartbeatStatus codes.Code
	registerStatus  codes.Code
}

func (c *healthCoordinatorStub) RegisterWorker(_ context.Context, _ *runtimepb.RegisterWorkerRequest, _ ...grpc.CallOption) (*runtimepb.RegisterWorkerResponse, error) {
	if c.registerStatus != codes.OK {
		return nil, status.Error(c.registerStatus, "controlled registration error")
	}
	return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: c.token}, nil
}

func (c *healthCoordinatorStub) Heartbeat(_ context.Context, req *runtimepb.HeartbeatRequest, _ ...grpc.CallOption) (*runtimepb.HeartbeatResponse, error) {
	c.beat = req
	if c.heartbeatStatus != codes.OK {
		return nil, status.Error(c.heartbeatStatus, "controlled heartbeat error")
	}
	if c.unknown {
		return &runtimepb.HeartbeatResponse{}, nil
	}
	return &runtimepb.HeartbeatResponse{Accepted: true, CudaHealthProbeAnchor: c.anchor}, nil
}

func TestCUDAHealthCheckLifecycle(t *testing.T) {
	ctx := t.Context()
	// Synthetic proof values are used only by this in-process stub.
	client := &healthCoordinatorStub{token: bytes.Repeat([]byte{1}, 32)}
	r := NewRegistration(&runtimepb.RegisterWorkerRequest{WorkerId: "worker", CudaHealthManaged: true, RegistrationKey: make([]byte, 32)})
	send := func(want codes.Code) {
		t.Helper()
		if err := r.Send(ctx, client); status.Code(err) != want {
			t.Fatalf("heartbeat: %v, want %s", err, want)
		}
	}
	begin := func() *CUDAHealthCheck {
		t.Helper()
		check := r.BeginCUDAHealthCheck()
		if check == nil {
			t.Fatal("expected a check for the current anchor")
		}
		return check
	}
	assertObservation := func(seq, anchor uint64, state runtimepb.CudaHealthState) {
		t.Helper()
		obs := client.beat.GetCudaHealth()
		if obs.GetObservationSeq() != seq || obs.GetProbeAnchor() != anchor || obs.GetState() != state {
			t.Fatal("unexpected observation was sent")
		}
	}
	if err := r.Register(ctx, client); err != nil {
		t.Fatal(err)
	}
	send(codes.OK) // Model B2-01: the Coordinator accepts the session but has no anchor.
	if r.BeginCUDAHealthCheck() != nil || client.beat.GetCudaHealth() != nil {
		t.Fatal("token alone enabled CUDA health against an older Coordinator")
	}

	client.anchor = 10
	send(codes.OK)
	first := begin()
	if r.BeginCUDAHealthCheck() != nil {
		t.Fatal("started a second check concurrently")
	}
	client.anchor = 20
	send(codes.OK)
	if client.beat.GetCudaHealth() != nil {
		t.Fatal("heartbeat generated a result before the check completed")
	}
	first.Finish(ready)
	first.Finish(unavailable) // Finishing a check twice must not generate a new observation.
	client.heartbeatStatus = codes.Unavailable
	send(codes.Unavailable)
	assertObservation(1, 10, ready)
	client.heartbeatStatus, client.anchor = codes.OK, 30
	send(codes.OK)
	assertObservation(1, 10, ready) // Lost reply resends the original observation.

	begin().Finish(unspecified)
	send(codes.OK)
	assertObservation(1, 10, ready) // Busy or cancelled checks report no evidence.
	begin().Finish(unavailable)
	send(codes.OK)
	assertObservation(2, 30, unavailable)
	stale := begin()

	client.unknown, client.registerStatus = true, codes.Unavailable
	send(codes.Unavailable)
	if r.BeginCUDAHealthCheck() != nil {
		t.Fatal("unknown registration retained its anchor while waiting to re-register")
	}
	stale.Finish(ready)
	client.registerStatus, client.token = codes.OK, bytes.Repeat([]byte{2}, 32)
	send(codes.OK)
	if r.BeginCUDAHealthCheck() != nil {
		t.Fatal("new token inherited the previous session's anchor")
	}
	client.unknown, client.anchor = false, 1
	send(codes.OK)
	if client.beat.GetCudaHealth() != nil || !bytes.Equal(client.beat.GetSessionToken(), client.token) {
		t.Fatal("reconnect sent stale health or used the wrong token")
	}
	begin().Finish(unavailable)
	send(codes.OK)
	assertObservation(1, 1, unavailable)

	revoked := begin()
	client.heartbeatStatus = codes.FailedPrecondition
	send(codes.FailedPrecondition)
	revoked.Finish(ready)
	if r.BeginCUDAHealthCheck() != nil || r.cuda.observation() != nil {
		t.Fatal("revoked registration retained an anchor or observation")
	}
}

func TestCUDAHealthSequenceExhaustion(t *testing.T) {
	r := &Registration{}
	r.cuda.anchor = 1
	r.cuda.latest = &runtimepb.CudaHealthObservation{ObservationSeq: math.MaxUint64 - 1}
	check := r.BeginCUDAHealthCheck()
	if check == nil {
		t.Fatal("last sequence should still be available")
	}
	check.Finish(ready)
	if r.cuda.observation().GetObservationSeq() != math.MaxUint64 || r.BeginCUDAHealthCheck() != nil {
		t.Fatal("observation sequence wrapped or allowed another check")
	}
}
