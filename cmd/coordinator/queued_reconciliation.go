package main

import (
	"context"
	"errors"
	"log"
	"time"
)

var errReconciliationLifecycleMissing = errors.New("queued reconciliation requires Coordinator lifecycle")

type queuedReconciliationSnapshot struct {
	jobID     int64
	entry     *queuedReconciliation
	nextRetry time.Time
}

// Snapshots are transient discovery views, not a second obligation queue.
func (s *CoordinatorServer) pendingQueuedReconciliations() []queuedReconciliationSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]queuedReconciliationSnapshot, 0, len(s.queuedReconciliations))
	for id, entry := range s.queuedReconciliations {
		entries = append(entries, queuedReconciliationSnapshot{id, entry, entry.nextRetry})
	}
	return entries
}

// claimQueuedReconciliation keeps the ledger entry installed while acquiring
// ordinary JobID exclusion in the same critical section. It does not weaken
// claimJob: execution/recovery still reject any pending obligation.
func (s *CoordinatorServer) claimQueuedReconciliation(id int64, entry *queuedReconciliation, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry == nil || s.queuedReconciliations[id] != entry || now.Before(entry.nextRetry) {
		return false
	}
	if _, active := s.activeJobs[id]; active {
		return false
	}
	if s.activeJobs == nil {
		s.activeJobs = make(map[int64]struct{})
	}
	s.activeJobs[id] = struct{}{}
	return true
}

// finishQueuedReconciliation runs while its caller still owns the JobID claim.
// Pointer identity makes a stale completion harmless to a newer obligation.
// The execution claim is released separately, after retirement/backoff update.
func (s *CoordinatorServer) finishQueuedReconciliation(id int64, entry *queuedReconciliation, err error, q *recoverySchedule) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queuedReconciliations[id] != entry {
		return false
	}
	if err == nil {
		delete(s.queuedReconciliations, id)
		return true
	}
	entry.cause = err
	entry.failures = incrementRecoveryFailures(entry.failures)
	entry.nextRetry = q.now().Add(recoveryBackoff(entry.failures, q.sample))
	return false
}

// reconcileQueuedJob is called only by a Coordinator-owned caller. The existing
// lifecycle gate tracks Load/conditional repair/retirement through shutdown.
// It never publishes a generation, lease, or Worker RPC.
func (s *CoordinatorServer) reconcileQueuedJob(ctx context.Context, id int64, entry *queuedReconciliation, q *recoverySchedule) (attempted bool, err error) {
	if s.lifecycle == nil {
		return false, errReconciliationLifecycleMissing
	}
	ctx, leave, ok := s.lifecycle.tryEnter(ctx)
	if !ok {
		return false, context.Canceled
	}
	defer leave()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !s.claimQueuedReconciliation(id, entry, q.now()) {
		return false, nil
	}
	retired := false
	defer func() {
		s.releaseJob(id)
		if retired {
			s.notifyRecoveryReady()
		}
	}()
	if entry.expected.JobID != id || validateRunningRepair(entry.expected) != nil ||
		entry.lastPublished != nil && (validateRunningRepair(*entry.lastPublished) != nil || jobSpecFromRecord(*entry.lastPublished) != jobSpecFromRecord(entry.expected)) ||
		entry.recovering != nil && (entry.recovering.State != JobRecovering || entry.recovering.AttemptID < 0 ||
			entry.recovering.HasWorkerResult || entry.recovering.Output != "" || entry.recovering.Error != "" ||
			jobSpecFromRecord(*entry.recovering) != jobSpecFromRecord(entry.expected)) {
		err = errors.New("pending obligation has inconsistent publication identity")
	} else {
		_, err = s.settleAttempt(entry.expected, entry.lastPublished, entry.recovering)
	}
	retired = s.finishQueuedReconciliation(id, entry, err, q)
	return true, err
}

// The existing recovery producer is the sole online consumer. Per-obligation
// delays live on the ledger and use recovery's existing jitter/cap; ordinary
// recovery entries/cooldowns are untouched by reconciliation attempts.
func (s *CoordinatorServer) runQueuedReconciliationPass(ctx context.Context, q *recoverySchedule) {
	for _, snapshot := range s.pendingQueuedReconciliations() {
		if ctx.Err() != nil {
			return
		}
		if q.now().Before(snapshot.nextRetry) {
			continue
		}
		attempted, err := s.reconcileQueuedJob(ctx, snapshot.jobID, snapshot.entry, q)
		if ctx.Err() != nil {
			return
		}
		if !attempted {
			// Ownership contention is not a failed repair. Defer it briefly so
			// an active owner or configuration error cannot cause a tight loop.
			s.mu.Lock()
			entry := snapshot.entry
			if s.queuedReconciliations[snapshot.jobID] == entry && !q.now().Before(entry.nextRetry) {
				entry.nextRetry = q.now().Add(recoveryBackoff(1, q.sample))
				if err != nil {
					entry.cause = err
				}
			}
			s.mu.Unlock()
		}
		if err != nil {
			log.Printf("queued reconciliation failed: job=%d err=%v", snapshot.jobID, err)
		}
	}
}

func (s *CoordinatorServer) queuedReconciliationDeadline(now time.Time) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	var next time.Time
	for _, entry := range s.queuedReconciliations {
		due := entry.nextRetry
		if due.IsZero() {
			due = now
		}
		if next.IsZero() || due.Before(next) {
			next = due
		}
	}
	return next
}

func (s *CoordinatorServer) hasQueuedReconciliation(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queuedReconciliations[id] != nil
}
