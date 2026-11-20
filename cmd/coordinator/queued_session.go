package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errQueuedPublicationConflict = errors.New("queued publication precondition changed")

// queuedReconciliation is a Coordinator-owned handoff, installed before the
// execution claim is released, for either queued or ordinary recovery sessions.
// It blocks ordinary claims until the recovery loop reconciles it online.
// After process loss, durable Running rows use startup recovery; the ledger
// itself is process-local. Provenance is immutable;
// diagnostic/backoff fields are protected by s.mu.
type queuedReconciliation struct {
	expected      JobRecord // exact attempted/confirmed Running generation and Worker
	lastPublished *JobRecord
	// An ordinary recovery session may fail its first publication without
	// changing this exact durable Recovering snapshot. It authorizes no RPC.
	recovering *JobRecord
	cause      error
	failures   uint32
	nextRetry  time.Time
}

// runQueuedJob is synchronous, internal, and requires an initialized lifecycle.
// It derives exclusively from the Coordinator context, never an earlier RPC.
// Lifecycle admission tracks the entire session (including settlement), so
// shutdown seals new starts and joins it before dispatchWG and storage closure.
func (s *CoordinatorServer) runQueuedJob(jobID int64) error {
	if s.lifecycle == nil {
		return status.Error(codes.FailedPrecondition, "Coordinator lifecycle is not configured")
	}
	ctx, leave, ok := s.lifecycle.tryEnter(s.lifecycle.ctx)
	if !ok {
		return status.Error(codes.Unavailable, "coordinator is shutting down")
	}
	defer leave()
	if !s.claimJob(jobID) {
		return status.Errorf(codes.Aborted, "job %d is active or awaiting queued reconciliation", jobID)
	}
	return s.runClaimedQueuedJob(ctx, jobID)
}

// The caller transfers a JobID claim and an already admitted lifecycle context.
// Direct internal calls and the bounded producer use exactly one admission.
// The claim lasts through finalization or ledger installation; lifecycle
// ownership lasts until this method and the caller's cleanup return.
func (s *CoordinatorServer) runClaimedQueuedJob(ctx context.Context, jobID int64) error {
	handoff := false
	pending := false
	defer func() {
		s.releaseJob(jobID)
		if handoff || pending {
			s.notifyRecoveryReady()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.jobStore == nil {
		return errJobStoreNotConfigured
	}

	// Re-read under ownership; a caller supplies only an ID, never a stale spec.
	queued, found, err := s.jobStore.Load(jobID)
	if err != nil {
		return fmt.Errorf("load queued job %d: %w", jobID, err)
	}
	if !found {
		return status.Errorf(codes.NotFound, "queued job %d does not exist", jobID)
	}
	if !isUnattemptedQueued(queued) {
		return status.Errorf(codes.FailedPrecondition, "job %d is not unattempted Queued", jobID)
	}
	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	session := s.executeAttemptSession(ctx, jobSpecFromRecord(queued), metrics.SourceSubmit,
		attemptSessionStart{mode: startPersistedQueuedJob, queued: queued})
	if session.err == errNoEligibleWorker {
		return session.err
	}
	defer func() { observer.ObserveJobExecution(metrics.SourceSubmit, time.Since(start)) }()
	if session.err == nil {
		return nil // The shared engine confirmed terminal persistence.
	}

	// No publication was attempted/confirmed: no Worker is authorized and the
	// unchanged Queued row remains discoverable, including when capacity is absent.
	expected := session.pendingPublication
	if expected == nil {
		expected = session.lastPublished
	}
	if expected == nil {
		return session.err
	}

	// Exhaustion retains normal execution's Failed finalization. Shutdown and
	// uncertain publication instead hand unfinished work to recovery; neither is
	// a client-requested logical cancellation. Never finalize an uncertain candidate.
	var exhausted *retryExhaustionError
	if ctx.Err() == nil && session.pendingPublication == nil && errors.As(session.err, &exhausted) {
		persisted, finalErr := s.persistSubmitFailure(ctx, jobID, 0, exhausted.attemptID)
		if persisted {
			observer.JobTerminal(metrics.SourceSubmit, metrics.TerminalFailed)
			return session.err
		}
		if finalErr != nil {
			session.err = errors.Join(session.err, fmt.Errorf("finalize queued job %d: %w", jobID, finalErr))
		}
	}

	handoff, pending, err = s.settleAttemptSession(session, nil)
	return err
}

// settleAttemptSession transfers unresolved publication responsibility to the
// existing ledger while its caller still holds the JobID claim. Both queued and
// ordinary recovery sessions release the claim and signal pending work afterward.
func (s *CoordinatorServer) settleAttemptSession(session attemptSessionResult, recovering *JobRecord) (handoff, pending bool, err error) {
	expected := session.pendingPublication
	if expected == nil {
		expected = session.lastPublished
	}
	if expected == nil {
		return false, false, session.err
	}
	// One bounded reconciliation pass runs while the claim is still held, even
	// after context cancellation. Storage methods themselves are not cancellable.
	handoff, err = s.settleAttempt(*expected, session.lastPublished, recovering)
	if err != nil {
		session.err = errors.Join(session.err, fmt.Errorf("reconcile job %d attempt %d: %w", expected.JobID, expected.AttemptID, err))
		s.mu.Lock()
		if s.queuedReconciliations == nil {
			s.queuedReconciliations = make(map[int64]*queuedReconciliation)
		}
		s.queuedReconciliations[expected.JobID] = &queuedReconciliation{
			expected: *expected, lastPublished: session.lastPublished, recovering: recovering, cause: session.err,
		}
		pending = true
		s.mu.Unlock()
	}
	// The attempt engine has returned; late RPC completion cannot write results.
	// Remove this session's authorization, including an earlier confirmed lease
	// when a subsequent generation's publication acknowledgement was lost.
	s.mu.Lock()
	if lease, ok := s.leases[expected.JobID]; ok && lease.AttemptID <= expected.AttemptID {
		delete(s.leases, expected.JobID)
	}
	s.mu.Unlock()
	return handoff, pending, session.err
}

// settleAttempt requires claimJob ownership through the whole operation.
// Both execution and online reconciliation use this settlement path. The
// conditional repair rechecks the entire Running record at mutation time.
func (s *CoordinatorServer) settleAttempt(expected JobRecord, lastPublished, recovering *JobRecord) (bool, error) {
	current, found, err := s.jobStore.Load(expected.JobID)
	if err != nil {
		metrics.Resolve(s.metrics).RecoveryRepairFailed(metrics.RepairLoad)
		return false, err
	}
	if !found {
		return false, errors.New("durable job is unexpectedly missing; no removal protocol exists")
	}
	if jobSpecFromRecord(current) != jobSpecFromRecord(expected) {
		return false, ErrJobSpecConflict
	}
	if current.State == JobSucceeded || current.State == JobFailed || current.State == JobCancelled {
		return false, validateReconciledTerminal(current) // Includes a lost terminal acknowledgement.
	}
	if current.State == JobRecovering {
		if recovering != nil && current == *recovering {
			return true, nil // First recovery publication did not commit.
		}
		running := current
		running.State = JobRunning
		if running == expected || lastPublished != nil && running == *lastPublished {
			return true, nil // Compatible durable handoff to ordinary recovery.
		}
		return false, errors.New("Recovering generation or Worker conflicts with pending provenance")
	}
	if expected.AttemptID == 1 && lastPublished == nil && isUnattemptedQueued(current) {
		return false, nil // The uncertain first publication did not leave Running.
	}
	// A failed retry write may have left the preceding confirmed generation.
	// Both identities belong to this session; no other generation may be repaired.
	if current != expected && (lastPublished == nil || current != *lastPublished) {
		return false, errors.New("durable generation or Worker changed; reconciliation required")
	}
	repaired, err := s.jobStore.TryRecoverRunning(current)
	if err != nil {
		// A repair acknowledgement can also be lost. Confirm by reading once;
		// never replace a terminal result or blindly repeat the repair write.
		actual, ok, readErr := s.jobStore.Load(expected.JobID)
		if readErr != nil {
			metrics.Resolve(s.metrics).RecoveryRepairFailed(metrics.RepairSave)
			return false, errors.Join(err, readErr)
		}
		if ok && jobSpecFromRecord(actual) == jobSpecFromRecord(expected) {
			if actual.State == JobSucceeded || actual.State == JobFailed || actual.State == JobCancelled {
				return false, validateReconciledTerminal(actual)
			}
			current.State = JobRecovering
			if actual == current {
				metrics.Resolve(s.metrics).RecoveryTransition(metrics.RecoveryRepair)
				return true, nil
			}
		}
		metrics.Resolve(s.metrics).RecoveryRepairFailed(metrics.RepairSave)
		return false, err
	}
	if !repaired {
		return false, errors.New("Running record changed before conditional recovery repair")
	}
	metrics.Resolve(s.metrics).RecoveryTransition(metrics.RecoveryRepair)
	return true, nil
}

func validateReconciledTerminal(record JobRecord) error {
	if record.AttemptID < 0 || record.HasWorkerResult && (record.AttemptID == 0 || record.WorkerID == "") ||
		!record.HasWorkerResult && (record.Output != "" || record.Error != "") {
		return errors.New("terminal record has inconsistent generation or result presence")
	}
	return nil
}
