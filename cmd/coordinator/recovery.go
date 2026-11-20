package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func recoveryFailureReason(err error) metrics.RecoveryFailureReason {
	switch {
	case errors.Is(err, context.Canceled):
		return metrics.FailureCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return metrics.FailureDeadline
	case status.Code(err) == codes.Unavailable:
		return metrics.FailureUnavailable
	case status.Code(err) == codes.Internal:
		return metrics.FailureInternal
	default:
		return metrics.FailureOther
	}
}

// runRecoveryPass processes one snapshot sequentially. Per-job errors are logged
// and leave persistence to retryRecoveringJob; only listing failure or cancellation ends
// the pass early. A nil return means the scan completed, not that every job ran.
func (s *CoordinatorServer) runRecoveryPass(ctx context.Context) error {
	return s.runScheduledRecoveryPass(ctx, nil)
}

// A standalone pass has no retained cooldown; the production loop supplies its
// private schedule. Both entry points use the same execution and metrics path.
func (s *CoordinatorServer) runScheduledRecoveryPass(ctx context.Context, schedule *recoverySchedule) error {
	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	outcome := metrics.PassCompleted
	defer func() { observer.RecoveryPass(outcome, time.Since(start)) }()
	if err := ctx.Err(); err != nil {
		outcome = metrics.PassCancelled
		return err
	}
	if s.jobStore == nil {
		outcome = metrics.PassSkipped
		return nil
	}
	records, err := s.jobStore.ListByState(JobRecovering)
	var queued []JobRecord
	if err == nil && schedule != nil {
		queued, err = s.jobStore.ListByState(JobQueued)
		if err != nil {
			err = fmt.Errorf("queued discovery: %w", err)
		}
	}
	if err != nil {
		outcome = metrics.PassStoreError
		if schedule != nil {
			schedule.scan.failures = incrementRecoveryFailures(schedule.scan.failures)
			schedule.scan.nextRetry = schedule.now().Add(recoveryBackoff(schedule.scan.failures, schedule.sample))
		}
		return err
	}
	if schedule != nil {
		schedule.scan = retryEntry{}
		// Reconcile only after BOTH scans succeed. A partial view must never
		// erase either state's cooldown. Each candidate is reloaded under claim.
		combined := append(append([]JobRecord(nil), records...), queued...)
		schedule.reconcile(combined)
		s.scheduleQueuedJobs(ctx, queued, schedule)
	}
	seen := make(map[int64]bool, len(records))
	for _, snapshot := range records {
		if seen[snapshot.JobID] {
			continue
		}
		seen[snapshot.JobID] = true
		if err := ctx.Err(); err != nil {
			outcome = metrics.PassCancelled
			return err
		}
		// Reconciliation owns this job until its ledger entry retires. Do not
		// turn that ownership contention into an ordinary recovery retry entry.
		if s.hasQueuedReconciliation(snapshot.JobID) {
			continue
		}
		if schedule != nil && !schedule.eligible(snapshot) {
			continue
		}
		current, ok, err := s.jobStore.Load(snapshot.JobID)
		if err != nil {
			outcome = metrics.PassCompletedWithErrors
			log.Printf("recovery load failed: job=%d err=%v", snapshot.JobID, err)
			if schedule != nil {
				schedule.recheck(snapshot)
			}
			continue
		}
		if !ok || current.State != JobRecovering || current.AttemptID != snapshot.AttemptID {
			if schedule != nil {
				delete(schedule.entries, snapshot.JobID)
				if ok && current.State == JobRecovering {
					schedule.recheck(current)
				}
			}
			continue
		}
		if err := ctx.Err(); err != nil {
			outcome = metrics.PassCancelled
			return err
		}
		if !s.hasEligibleWorker(current.TaskType) {
			if schedule != nil {
				s.mu.Lock()
				_, active := s.activeJobs[current.JobID]
				s.mu.Unlock()
				if active {
					schedule.recheck(current)
				} else {
					schedule.backOff(current.JobID, current.AttemptID, recoverySpecHash(current))
				}
			}
			continue
		}
		result, err := s.retryRecoveringJobResult(ctx, current)
		if schedule != nil && ctx.Err() == nil {
			if loadErr := schedule.completed(s.jobStore, current, result, err); loadErr != nil {
				outcome = metrics.PassCompletedWithErrors
				log.Printf("recovery reconciliation failed: job=%d err=%v", current.JobID, loadErr)
			}
		}
		if err != nil && err != errNoEligibleWorker {
			if ctx.Err() != nil {
				outcome = metrics.PassCancelled
				// Preserve a repair failure joined with the cancellation error.
				return err
			}
			outcome = metrics.PassCompletedWithErrors
			log.Printf("recovery execution failed: job=%d err=%v", current.JobID, err)
		}
	}
	if ctx.Err() != nil {
		outcome = metrics.PassCancelled
	}
	return ctx.Err()
}
