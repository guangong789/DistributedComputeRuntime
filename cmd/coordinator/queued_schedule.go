package main

import (
	"context"
	"log"
	"sort"
)

// Two complete queued sessions allow an unrelated job to progress alongside a
// slow one without flooding the round-robin Worker model.
// This is a Coordinator resource bound, not a claim of Worker free capacity.
// queuedSessionLimit follows leaseDuration's configure-before-start convention.
const defaultQueuedSessionLimit = 2

// reserveQueuedSession establishes lifecycle ownership, capacity and JobID
// exclusion BEFORE launch. No new WaitGroup/producer or second admission is
// needed: shutdown seals the existing gate and joins these complete sessions.
func (s *CoordinatorServer) reserveQueuedSession(ctx context.Context, id int64) (sessionCtx context.Context, release func(), full bool) {
	if s.lifecycle == nil || ctx.Err() != nil {
		return nil, nil, false
	}
	ownedCtx, leave, ok := s.lifecycle.tryEnter(ctx)
	if !ok {
		return nil, nil, false
	}
	s.mu.Lock()
	limit := s.queuedSessionLimit
	if limit <= 0 {
		limit = defaultQueuedSessionLimit
	}
	_, active := s.activeJobs[id]
	full = s.queuedSessions >= limit
	if full || active || s.queuedReconciliations[id] != nil || ownedCtx.Err() != nil {
		s.mu.Unlock()
		leave()
		return nil, nil, full
	}
	if s.activeJobs == nil {
		s.activeJobs = make(map[int64]struct{})
	}
	s.activeJobs[id] = struct{}{}
	s.queuedSessions++
	saturated := s.queuedSessions == limit
	s.mu.Unlock()
	if saturated {
		log.Printf("queued session capacity saturated: limit=%d", limit)
	}
	return ownedCtx, func() {
		s.mu.Lock()
		s.queuedSessions--
		s.mu.Unlock()
		// Keep lifecycle ownership through all cleanup, then wake the same
		// producer so capacity release cannot strand the remaining backlog.
		s.notifyRecoveryReady()
		leave()
	}, false
}

// Only the existing discovery producer accesses q. Goroutines never mutate its
// maps or clocks: a bounded recheck is installed before launch, and completion
// wakes discovery to reload authoritative durable state. Sorted IDs provide a
// simple deterministic order; no strict fairness across continuously arriving
// lower IDs is promised.
func (s *CoordinatorServer) scheduleQueuedJobs(ctx context.Context, records []JobRecord, q *recoverySchedule) {
	sort.Slice(records, func(i, j int) bool { return records[i].JobID < records[j].JobID })
	seen := make(map[int64]bool, len(records))
	for _, record := range records {
		if ctx.Err() != nil {
			return
		}
		id := record.JobID
		if s.hasQueuedReconciliation(id) {
			delete(q.entries, id) // The ledger owns Queued eligibility until retirement.
			continue
		}
		if seen[id] || !q.eligible(record) {
			continue
		}
		seen[id] = true
		if !s.hasEligibleWorker(record.TaskType) {
			if _, retrying := q.entries[id]; retrying {
				q.recheck(record) // Defer an overdue entry after Worker loss.
			}
			continue
		}
		sessionCtx, release, full := s.reserveQueuedSession(ctx, id)
		if release == nil {
			// Only an existing overdue entry needs deferring; a fresh backlog
			// waits for capacity release/Worker wakeups or periodic discovery.
			if _, retrying := q.entries[id]; retrying {
				q.recheck(record)
			}
			if !full && s.lifecycle == nil {
				log.Printf("queued scheduling unavailable: job=%d lifecycle missing", id)
			}
			continue
		}
		q.recheck(record)
		log.Printf("queued session started: job=%d", id)
		go func() {
			defer release()
			err := s.runClaimedQueuedJob(sessionCtx, id)
			if err == errNoEligibleWorker {
				return // Availability changed after the session precheck.
			}
			if err != nil {
				log.Printf("queued session completed with durable scheduling error: job=%d pending_reconciliation=%v err=%v", id, s.hasQueuedReconciliation(id), err)
			} else {
				log.Printf("queued session completed: job=%d", id)
			}
		}()
	}
}
