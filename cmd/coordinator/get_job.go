package main

import (
	"context"
	"log"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetJob projects a single durable read. RPC admission/authentication belongs to
// the existing transport interceptors; no execution ownership or live state is
// consulted. All int64 JobIDs remain valid, as in SubmitJob and JobStore.
func (s *CoordinatorServer) GetJob(ctx context.Context, req *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "GetJob request is required")
	}
	if s.jobStore == nil {
		return nil, status.Error(codes.FailedPrecondition, "job store is not configured")
	}
	record, found, err := s.jobStore.Load(req.JobId)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, status.FromContextError(contextErr).Err()
	}
	if err != nil {
		log.Printf("GetJob read failed: job=%d err=%v", req.JobId, err)
		return nil, status.Error(codes.Internal, "failed to read job")
	}
	if !found {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	var state runtimepb.JobState
	switch record.State {
	case JobQueued:
		state = runtimepb.JobState_JOB_STATE_QUEUED
	case JobRunning:
		state = runtimepb.JobState_JOB_STATE_RUNNING
	case JobRecovering:
		state = runtimepb.JobState_JOB_STATE_RECOVERING
	case JobSucceeded:
		state = runtimepb.JobState_JOB_STATE_SUCCEEDED
	case JobFailed:
		state = runtimepb.JobState_JOB_STATE_FAILED
	case JobCancelled:
		state = runtimepb.JobState_JOB_STATE_CANCELLED
	default:
		return nil, status.Error(codes.Internal, "stored job state is not supported")
	}
	return &runtimepb.GetJobResponse{
		JobId: record.JobID, State: state, AttemptId: record.AttemptID,
		WorkerId: record.WorkerID, HasWorkerResult: record.HasWorkerResult,
		Output: record.Output, Error: record.Error,
	}, nil
}
