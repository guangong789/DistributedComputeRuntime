package main

import (
	"fmt"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"google.golang.org/grpc/status"
)

// attemptPublicationError retains the exact write candidate even when its
// acknowledgement is lost. It authorizes neither a lease nor a dispatch.
type attemptPublicationError struct {
	record JobRecord
	cause  error
}

func (e *attemptPublicationError) Error() string              { return e.cause.Error() }
func (e *attemptPublicationError) Unwrap() error              { return e.cause }
func (e *attemptPublicationError) GRPCStatus() *status.Status { return status.Convert(e.cause) }

// startQueuedAttempt requires claimJob ownership and the original stored record.
// A conflict is distinct from an uncertain write. No automatic publication retry.
func (s *CoordinatorServer) startQueuedAttempt(expected JobRecord, workerID string, duration time.Duration) (int64, error) {
	if s.jobStore == nil {
		return 0, errJobStoreNotConfigured
	}
	candidate := expected
	candidate.State, candidate.AttemptID, candidate.WorkerID = JobRunning, 1, workerID
	published, err := s.jobStore.TryStartQueued(expected, workerID)
	if err != nil {
		return 0, &attemptPublicationError{record: candidate, cause: err}
	}
	if !published {
		return 0, errQueuedPublicationConflict
	}
	s.mu.Lock()
	if s.jobAttempts == nil {
		s.jobAttempts = make(map[int64]int64)
	}
	s.jobAttempts[expected.JobID] = 1
	s.setLeaseLocked(expected.JobID, 1, workerID, duration)
	s.mu.Unlock()
	metrics.Resolve(s.metrics).AttemptStarted(metrics.SourceSubmit)
	return 1, nil
}

// admitJobAttempt requires the submit session's JobID claim. Create is the
// admission linearization point, before in-memory publication and dispatch.
// Do not hold s.mu during Create: another client must be able to observe local
// pre-admission contention without waiting for the owner's storage operation.
func (s *CoordinatorServer) admitJobAttempt(spec JobSpec, workerID string, duration time.Duration) (int64, error) {
	if s.jobStore == nil {
		return 0, errJobStoreNotConfigured
	}
	record := JobRecord{JobID: spec.JobID, State: JobRunning, AttemptID: 1,
		WorkerID: workerID, TaskType: spec.TaskType, Payload: spec.Payload, TimeoutMs: spec.TimeoutMs}
	created, err := s.jobStore.Create(record)
	if err != nil {
		// No reconciliation write or automatic retry: Create may have committed.
		return 0, err
	}
	if !created {
		if err := s.checkExistingSubmission(spec); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("Create rejected job %d but its durable record could not be confirmed", spec.JobID)
	}
	s.mu.Lock()
	if s.jobAttempts == nil {
		s.jobAttempts = make(map[int64]int64)
	}
	s.jobAttempts[spec.JobID] = 1
	s.setLeaseLocked(spec.JobID, 1, workerID, duration)
	s.mu.Unlock()
	metrics.Resolve(s.metrics).AttemptStarted(metrics.SourceSubmit)
	return 1, nil
}

type JobLease struct {
	JobID     int64
	AttemptID int64
	WorkerID  string
	ExpiresAt time.Time
}

// startAttempt retains the submit-source entry point used by existing callers.
func (s *CoordinatorServer) startAttempt(
	jobID int64,
	workerID string,
	duration time.Duration,
	taskType string,
	payload string,
	timeoutMs int64,
) (int64, error) {
	return s.startAttemptFromSource(jobID, workerID, duration, taskType, payload, timeoutMs, metrics.SourceSubmit)
}

func (s *CoordinatorServer) startAttemptFromSource(
	jobID int64,
	workerID string,
	duration time.Duration,
	taskType, payload string,
	timeoutMs int64,
	source metrics.ExecutionSource,
) (int64, error) {
	attempt, err := s.publishAttempt(jobID, workerID, duration, taskType, payload, timeoutMs)
	if err == nil && s.jobStore != nil {
		// The callback runs after durable/in-memory publication and outside s.mu.
		metrics.Resolve(s.metrics).AttemptStarted(source)
	}
	return attempt, err
}

// publishAttempt persists the next ID before publishing it and its lease under s.mu.
func (s *CoordinatorServer) publishAttempt(
	jobID int64,
	workerID string,
	duration time.Duration,
	taskType, payload string,
	timeoutMs int64,
) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.jobAttempts == nil {
		s.jobAttempts = make(map[int64]int64)
	}

	var persistedAttempt int64

	if s.jobStore != nil {
		record, ok, err := s.jobStore.Load(jobID)
		if err != nil {
			return 0, err
		}

		if ok {
			persistedAttempt = record.AttemptID
		}
	}

	baseAttempt := s.jobAttempts[jobID]

	if persistedAttempt > baseAttempt {
		baseAttempt = persistedAttempt
	}

	next := baseAttempt + 1

	// A fresh record deliberately clears any previous generation's result.
	record := JobRecord{
		JobID:     jobID,
		State:     JobRunning,
		AttemptID: next,
		WorkerID:  workerID,
		TaskType:  taskType,
		Payload:   payload,
		TimeoutMs: timeoutMs,
	}

	if s.jobStore != nil {
		if err := s.jobStore.Save(record); err != nil {
			return 0, &attemptPublicationError{record: record, cause: err}
		}
	}

	s.jobAttempts[jobID] = next
	s.setLeaseLocked(
		jobID,
		next,
		workerID,
		duration,
	)

	return next, nil
}

func (s *CoordinatorServer) setLease(
	jobID int64,
	attemptID int64,
	workerID string,
	duration time.Duration,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.setLeaseLocked(jobID, attemptID, workerID, duration)
}

// setLeaseLocked requires s.mu to be held.
func (s *CoordinatorServer) setLeaseLocked(jobID, attemptID int64, workerID string, duration time.Duration) {
	if s.leases == nil {
		s.leases = make(map[int64]JobLease)
	}
	s.leases[jobID] = JobLease{
		JobID:     jobID,
		AttemptID: attemptID,
		WorkerID:  workerID,
		ExpiresAt: time.Now().Add(duration),
	}
}

func (s *CoordinatorServer) getLease(
	jobID int64,
) (JobLease, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.leases[jobID]
	return lease, ok
}

func (s *CoordinatorServer) leaseExpired(
	jobID int64,
	now time.Time,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	lease, ok := s.leases[jobID]
	if !ok {
		return false
	}

	return !now.Before(lease.ExpiresAt)
}

func (s *CoordinatorServer) isLeaseCurrent(
	jobID int64,
	attemptID int64,
) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isLeaseCurrentLocked(jobID, attemptID)
}

// isLeaseCurrentLocked requires s.mu to be held.
func (s *CoordinatorServer) isLeaseCurrentLocked(jobID, attemptID int64) bool {
	lease, ok := s.leases[jobID]
	if !ok {
		return false
	}

	if lease.AttemptID != attemptID {
		return false
	}

	return time.Now().Before(lease.ExpiresAt)
}

func (s *CoordinatorServer) claimJob(jobID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// A failed queued-session settlement hands ownership to the Coordinator's
	// reconciliation ledger. Ordinary execution/recovery cannot bypass it.
	if _, pending := s.queuedReconciliations[jobID]; pending {
		return false
	}

	if s.activeJobs == nil {
		s.activeJobs = make(map[int64]struct{})
	}

	if _, exists := s.activeJobs[jobID]; exists {
		return false
	}

	s.activeJobs[jobID] = struct{}{}
	return true
}

func (s *CoordinatorServer) releaseJob(jobID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.activeJobs, jobID)
}
