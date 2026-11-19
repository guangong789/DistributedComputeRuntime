package main

import (
	"context"
	"fmt"
	"io"

	"github.com/guangong789/DistributedComputeRuntime/internal/jobobservation"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
)

// getJob makes one observation using the caller's connection and context. It
// neither submits nor polls, and returns the snapshot and RPC error unchanged.
func getJob(ctx context.Context, client runtimepb.CoordinatorServiceClient, jobID int64) (*runtimepb.GetJobResponse, error) {
	return client.GetJob(ctx, &runtimepb.GetJobRequest{JobId: jobID})
}

// runClient admits once (or explicitly reconnects by ID), then observes using
// the same connection. Admission and observation have independent time budgets.
func runClient(ctx context.Context, client runtimepb.CoordinatorServiceClient, c clientExecutionConfig, out io.Writer) error {
	if err := c.validate(); err != nil {
		return err
	}
	id := int64(3001)
	if c.ObserveJobID != nil {
		id = *c.ObserveJobID
	} else {
		ack, err := submitDemo(ctx, client, c)
		if err != nil {
			return fmt.Errorf("admission for job %d not confirmed; use -observe-job=%d to query its durable outcome: %w", id, id, err)
		}
		if err := jobobservation.ValidateAdmission(ack, id); err != nil {
			return err
		}
	}
	observeCtx, cancel := context.WithTimeout(ctx, c.ObserveTimeout)
	defer cancel()
	snapshot, err := jobobservation.Wait(observeCtx, id, c.PollInterval, func(ctx context.Context, id int64) (*runtimepb.GetJobResponse, error) {
		rpcCtx, stop := context.WithTimeout(ctx, c.RPCTimeout)
		defer stop()
		return getJob(rpcCtx, client, id)
	})
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "job=%d status=%s attempt=%d worker=%q has_worker_result=%t output=%q error=%q\n", snapshot.JobId, jobobservation.StateName(snapshot.State), snapshot.AttemptId, snapshot.WorkerId, snapshot.HasWorkerResult, snapshot.Output, snapshot.Error); err != nil {
		return err
	}
	return jobobservation.TerminalError(snapshot)
}
