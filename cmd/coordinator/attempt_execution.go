package main

import (
	"context"
	"fmt"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// authorizedAttempt is an execution snapshot, not an authorization operation.
// Its caller holds the JobID claim, has confirmed publication of this generation
// (except for legacy nil-store internal callers), and has installed its lease.
type authorizedAttempt struct {
	spec          JobSpec
	worker        WorkerInfo
	attemptID     int64
	leaseDuration time.Duration
	source        metrics.ExecutionSource
}

type attemptOutcomeKind uint8

const (
	attemptAccepted attemptOutcomeKind = iota + 1
	attemptRPCFailed
	attemptLeaseExpired
	attemptCancelled
	attemptFenced
	attemptPersistenceFailed
	attemptInvalidStatus
)

// acceptedJobResult exists only after terminal persistence is acknowledged (or
// accepted in legacy nil-store mode). Preserve the Worker's original status
// spelling for the synchronous RPC adapter as well as the durable result record.
type acceptedJobResult struct {
	record       JobRecord
	workerStatus string
}

// Every outcome carries the generation it executed. Only attemptAccepted has a
// terminal result. attemptPersistenceFailed may have an unknown durable outcome;
// it does not establish rollback and must not be treated as an RPC retry.
type attemptOutcome struct {
	kind      attemptOutcomeKind
	jobID     int64
	attemptID int64
	terminal  *acceptedJobResult
	err       error
}

type dispatchResult struct {
	resp *runtimepb.ExecuteJobResponse
	err  error
}

// executeAuthorizedAttempt owns one dispatch child context and lease timer. It
// neither publishes a generation nor claims/releases the job, selects another
// Worker, retries, or finalizes a failed/cancelled session. The caller retains
// session ownership through its return and supplies the session context.
// Dispatch goroutines retain the existing dispatchWG/onDispatchDone protocol;
// terminal persistence finish synchronously within this call.
func (s *CoordinatorServer) executeAuthorizedAttempt(ctx context.Context, attempt authorizedAttempt) (outcome attemptOutcome) {
	spec, worker := attempt.spec, attempt.worker
	attemptID, source := attempt.attemptID, attempt.source
	outcome.jobID, outcome.attemptID = spec.JobID, attemptID
	observer := metrics.Resolve(s.metrics)
	attemptStart := time.Now()
	// Durable lifecycle events are omitted in the legacy nil-store mode.
	finishAttempt := func(result metrics.AttemptOutcome) {
		if s.jobStore != nil {
			observer.AttemptFinished(source, result, time.Since(attemptStart))
		}
	}
	attemptCtx, cancelAttempt := context.WithCancel(ctx)
	resultCh := make(chan dispatchResult, 1)

	s.dispatchWG.Add(1)
	go func() {
		defer s.dispatchWG.Done()
		resp, err := s.dispatchJob(attemptCtx, worker, &runtimepb.ExecuteJobRequest{
			JobId:     spec.JobID,
			TaskType:  spec.TaskType,
			Payload:   spec.Payload,
			TimeoutMs: spec.TimeoutMs,
			AttemptId: attemptID,
		})
		resultCh <- dispatchResult{resp: resp, err: err}
		if s.onDispatchDone != nil {
			s.onDispatchDone(spec.JobID, attemptID)
		}
	}()

	leaseTimer := time.NewTimer(attempt.leaseDuration)
	select {
	case result := <-resultCh:
		leaseTimer.Stop()
		cancelAttempt()

		if err := ctx.Err(); err != nil {
			finishAttempt(metrics.AttemptCancelled)
			outcome.kind, outcome.err = attemptCancelled, err
			return outcome
		}
		if result.err != nil {
			finishAttempt(metrics.AttemptRPCError)
			fmt.Printf("attempt failed: job=%d attempt=%d worker=%s err=%v\n",
				spec.JobID, attemptID, worker.ID, result.err)
			outcome.kind, outcome.err = attemptRPCFailed, result.err
			return outcome
		}

		resp := result.resp
		if resp == nil || resp.JobId != spec.JobID || resp.AttemptId != attemptID ||
			!s.isLeaseCurrent(spec.JobID, attemptID) {
			observer.AttemptRejected(source, metrics.RejectFencing)
			finishAttempt(metrics.AttemptFenced)
			outcome.kind, outcome.err = attemptFenced, fmt.Errorf(
				"stale, expired, or mismatched result for job %d attempt %d", spec.JobID, attemptID)
			return outcome
		}

		finalState, err := jobStateFromStatus(resp.Status)
		if err != nil {
			finishAttempt(metrics.AttemptInvalidStatus)
			outcome.kind, outcome.err = attemptInvalidStatus, status.Errorf(codes.Internal,
				"invalid worker status for job %d attempt %d: %v", spec.JobID, attemptID, err)
			return outcome
		}

		record := JobRecord{
			JobID: spec.JobID, State: finalState, AttemptID: attemptID, WorkerID: worker.ID,
			TaskType: spec.TaskType, Payload: spec.Payload, TimeoutMs: spec.TimeoutMs,
			HasWorkerResult: true, Output: resp.Output, Error: resp.Error,
		}
		// Keep the final context/lease check and Save in their established critical
		// section. The result may already have outlived this generation or lease.
		eligible, err := s.persistTerminalResult(ctx, record)
		if !eligible {
			if err != nil {
				finishAttempt(metrics.AttemptCancelled)
				outcome.kind, outcome.err = attemptCancelled, err
				return outcome
			}
			observer.AttemptRejected(source, metrics.RejectFencing)
			finishAttempt(metrics.AttemptFenced)
			outcome.kind, outcome.err = attemptFenced, fmt.Errorf(
				"stale, expired, or mismatched result for job %d attempt %d", spec.JobID, attemptID)
			return outcome
		}
		if err != nil {
			finishAttempt(metrics.AttemptPersistenceError)
			outcome.kind, outcome.err = attemptPersistenceFailed, status.Errorf(jobStoreErrorCode(err),
				"failed to persist final state for job %d attempt %d: %v", spec.JobID, attemptID, err)
			return outcome
		}

		terminalState, metricOutcome := metrics.TerminalSucceeded, metrics.AttemptAccepted
		switch finalState {
		case JobFailed:
			terminalState, metricOutcome = metrics.TerminalFailed, metrics.AttemptTaskFailed
		case JobCancelled:
			terminalState, metricOutcome = metrics.TerminalCancelled, metrics.AttemptTaskCancelled
		}
		if s.jobStore != nil {
			observer.JobTerminal(source, terminalState)
		}
		finishAttempt(metricOutcome)
		outcome.kind, outcome.terminal = attemptAccepted, &acceptedJobResult{record: record, workerStatus: resp.Status}
		return outcome

	case <-leaseTimer.C:
		cancelAttempt()
		finishAttempt(metrics.AttemptLeaseExpired)
		outcome.kind, outcome.err = attemptLeaseExpired, fmt.Errorf(
			"lease expired for job %d attempt %d", spec.JobID, attemptID)
		fmt.Printf("lease expired: job=%d attempt=%d worker=%s\n", spec.JobID, attemptID, worker.ID)
		return outcome

	case <-ctx.Done():
		leaseTimer.Stop()
		cancelAttempt()
		finishAttempt(metrics.AttemptCancelled)
		outcome.kind, outcome.err = attemptCancelled, ctx.Err()
		return outcome
	}
}
