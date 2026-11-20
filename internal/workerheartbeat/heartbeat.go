package workerheartbeat

import (
	"bytes"
	"context"
	"fmt"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CoordinatorClient is the coordinator RPC subset needed by heartbeat recovery.
type CoordinatorClient interface {
	Heartbeat(context.Context, *runtimepb.HeartbeatRequest, ...grpc.CallOption) (*runtimepb.HeartbeatResponse, error)
	RegisterWorker(context.Context, *runtimepb.RegisterWorkerRequest, ...grpc.CallOption) (*runtimepb.RegisterWorkerResponse, error)
}

// Registration owns its request and token. Startup and heartbeat use it
// sequentially, including when a re-registration reply is lost.
type Registration struct {
	request *runtimepb.RegisterWorkerRequest
	pending bool
}

func NewRegistration(request *runtimepb.RegisterWorkerRequest) *Registration {
	return &Registration{request: request}
}

func (r *Registration) Register(ctx context.Context, client CoordinatorClient) error {
	response, err := client.RegisterWorker(ctx, r.request)
	if err != nil {
		return err
	}
	if !response.GetAccepted() {
		return fmt.Errorf("registration rejected: %s", response.GetError())
	}
	if size := len(r.request.GetRegistrationKey()); size != 0 {
		if len(response.GetSessionToken()) != size {
			return status.Error(codes.Unimplemented, "Coordinator did not provide a Worker registration session")
		}
		r.request.SessionToken = bytes.Clone(response.GetSessionToken())
	}
	r.pending = false
	return nil
}

// Send sends one heartbeat, or retries a pending registration after the
// Coordinator reported an unknown Worker. A revoked session gets an RPC error.
func (r *Registration) Send(ctx context.Context, client CoordinatorClient) error {
	if !r.pending {
		resp, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{
			WorkerId: r.request.WorkerId, SessionToken: r.request.GetSessionToken(),
		})
		if err != nil {
			return err
		}
		if resp.GetAccepted() {
			return nil
		}
		r.pending = true
	}
	return r.Register(ctx, client)
}
