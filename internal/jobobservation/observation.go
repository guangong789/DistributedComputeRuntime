// Package jobobservation contains the Client and Demo's durable observation
// rules. It never submits a Job or owns its execution lifecycle.
package jobobservation

import (
	"context"
	"fmt"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/status"
)

const DefaultPollInterval = 250 * time.Millisecond

func ValidateAdmission(ack *runtimepb.SubmitJobResponse, id int64) error {
	if ack == nil || ack.JobId != id || ack.Status != "Accepted" || ack.AttemptId != 0 || ack.Output != "" || ack.Error != "" {
		return fmt.Errorf("invalid admission acknowledgement for job %d", id)
	}
	return nil
}

// Wait polls at the bounded interval, including before the first read. The
// caller supplies an observation deadline and its existing authenticated fetch.
// Read errors stop observation; neither errors nor cancellation resubmit or
// mutate the admitted Job. A terminal snapshot is returned even for failed Jobs.
func Wait(ctx context.Context, id int64, interval time.Duration, fetch func(context.Context, int64) (*runtimepb.GetJobResponse, error)) (*runtimepb.GetJobResponse, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("poll interval must be positive")
	}
	for {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, observationEnded(ctx, id)
		case <-timer.C:
		}
		if ctx.Err() != nil {
			return nil, observationEnded(ctx, id)
		}
		snapshot, err := fetch(ctx, id)
		if ctx.Err() != nil {
			return nil, observationEnded(ctx, id)
		}
		if err != nil {
			return nil, fmt.Errorf("GetJob observation for job %d: %w", id, err)
		}
		if snapshot == nil || snapshot.JobId != id {
			return nil, fmt.Errorf("invalid GetJob snapshot identity for job %d", id)
		}
		switch snapshot.State {
		case runtimepb.JobState_JOB_STATE_QUEUED, runtimepb.JobState_JOB_STATE_RUNNING, runtimepb.JobState_JOB_STATE_RECOVERING:
		case runtimepb.JobState_JOB_STATE_SUCCEEDED, runtimepb.JobState_JOB_STATE_FAILED, runtimepb.JobState_JOB_STATE_CANCELLED:
			return snapshot, nil
		default:
			return nil, fmt.Errorf("unknown GetJob state %d for job %d", snapshot.State, id)
		}
	}
}

func observationEnded(ctx context.Context, id int64) error {
	return fmt.Errorf("observation ended for job %d; stopping observation does not cancel the Job: %w", id, status.FromContextError(ctx.Err()).Err())
}

func StateName(state runtimepb.JobState) string {
	switch state {
	case runtimepb.JobState_JOB_STATE_QUEUED:
		return "Queued"
	case runtimepb.JobState_JOB_STATE_RUNNING:
		return "Running"
	case runtimepb.JobState_JOB_STATE_RECOVERING:
		return "Recovering"
	case runtimepb.JobState_JOB_STATE_SUCCEEDED:
		return "Succeeded"
	case runtimepb.JobState_JOB_STATE_FAILED:
		return "Failed"
	case runtimepb.JobState_JOB_STATE_CANCELLED:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

type TerminalJobError struct {
	JobID           int64
	State           runtimepb.JobState
	HasWorkerResult bool
	Message         string
}

func (e *TerminalJobError) Error() string {
	return fmt.Sprintf("job %d ended %s (has_worker_result=%t, error=%q)", e.JobID, StateName(e.State), e.HasWorkerResult, e.Message)
}

// TerminalError describes a Job outcome, never an RPC status. A missing Worker
// result does not change the persisted state or imply that strings were accepted.
func TerminalError(snapshot *runtimepb.GetJobResponse) error {
	if snapshot == nil {
		return fmt.Errorf("missing terminal snapshot")
	}
	switch snapshot.State {
	case runtimepb.JobState_JOB_STATE_SUCCEEDED:
		return nil
	case runtimepb.JobState_JOB_STATE_FAILED, runtimepb.JobState_JOB_STATE_CANCELLED:
		return &TerminalJobError{snapshot.JobId, snapshot.State, snapshot.HasWorkerResult, snapshot.Error}
	default:
		return fmt.Errorf("job %d is not terminal", snapshot.JobId)
	}
}
