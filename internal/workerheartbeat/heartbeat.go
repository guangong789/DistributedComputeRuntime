package workerheartbeat

import (
	"context"
	"fmt"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
)

// CoordinatorClient is the coordinator RPC subset needed by heartbeat recovery.
type CoordinatorClient interface {
	Heartbeat(context.Context, *runtimepb.HeartbeatRequest, ...grpc.CallOption) (*runtimepb.HeartbeatResponse, error)
	RegisterWorker(context.Context, *runtimepb.RegisterWorkerRequest, ...grpc.CallOption) (*runtimepb.RegisterWorkerResponse, error)
}

// Send sends one heartbeat and re-registers when the coordinator has lost the worker.
func Send(
	ctx context.Context,
	client CoordinatorClient,
	registration *runtimepb.RegisterWorkerRequest,
) error {
	resp, err := client.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: registration.WorkerId})
	if err != nil {
		return err
	}
	if resp.Accepted {
		return nil
	}

	response, err := client.RegisterWorker(ctx, registration)
	if err != nil {
		return err
	}
	if !response.Accepted {
		return fmt.Errorf("registration rejected: %s", response.Error)
	}
	return nil
}
