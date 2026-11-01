package main

import (
	"context"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type observationClient struct {
	runtimepb.CoordinatorServiceClient
	get         func(context.Context, *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error)
	submissions int
}

func (c *observationClient) GetJob(ctx context.Context, request *runtimepb.GetJobRequest, _ ...grpc.CallOption) (*runtimepb.GetJobResponse, error) {
	return c.get(ctx, request)
}
func (c *observationClient) SubmitJob(context.Context, *runtimepb.SubmitJobRequest, ...grpc.CallOption) (*runtimepb.SubmitJobResponse, error) {
	c.submissions++
	return nil, status.Error(codes.Internal, "observation attempted submission")
}

func TestClientGetJobSingleObservation(t *testing.T) {
	for _, state := range []runtimepb.JobState{runtimepb.JobState_JOB_STATE_QUEUED, runtimepb.JobState_JOB_STATE_RUNNING, runtimepb.JobState_JOB_STATE_RECOVERING,
		runtimepb.JobState_JOB_STATE_SUCCEEDED, runtimepb.JobState_JOB_STATE_FAILED, runtimepb.JobState_JOB_STATE_CANCELLED} {
		t.Run(state.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			want := &runtimepb.GetJobResponse{JobId: -1, State: state, HasWorkerResult: state == runtimepb.JobState_JOB_STATE_SUCCEEDED}
			calls := 0
			client := &observationClient{get: func(gotCtx context.Context, request *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error) {
				calls++
				if calls > 1 {
					return nil, status.Error(codes.Internal, "implicit polling")
				}
				if gotCtx != ctx || request.JobId != want.JobId {
					t.Fatal("observation changed caller context or JobID")
				}
				return want, nil
			}}
			got, err := getJob(ctx, client, want.JobId)
			if err != nil || got != want || calls != 1 || client.submissions != 0 {
				t.Fatalf("GetJob=%v, %v calls=%d submissions=%d", got, err, calls, client.submissions)
			}
		})
	}
}

func TestClientGetJobPreservesRPCErrors(t *testing.T) {
	for _, code := range []codes.Code{codes.NotFound, codes.Internal, codes.Unavailable, codes.PermissionDenied, codes.Canceled, codes.DeadlineExceeded} {
		t.Run(code.String(), func(t *testing.T) {
			want := status.Error(code, "original RPC failure")
			calls := 0
			client := &observationClient{get: func(context.Context, *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error) {
				calls++
				return nil, want
			}}
			got, err := getJob(context.Background(), client, 42)
			if got != nil || err != want || status.Code(err) != code || calls != 1 || client.submissions != 0 {
				t.Fatalf("error changed or retried: %v, %v", got, err)
			}
		})
	}
}
