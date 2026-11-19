package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
)

type JobState int

const (
	JobQueued JobState = iota
	JobRunning
	JobSucceeded
	JobFailed
	JobCancelled
	JobRecovering // Keep existing states' persisted SQLite values unchanged.
)

type JobRecord struct {
	JobID     int64
	State     JobState
	AttemptID int64
	WorkerID  string

	TaskType  string
	Payload   string
	TimeoutMs int64

	// HasWorkerResult distinguishes an accepted (possibly empty) Worker result
	// from legacy rows and coordinator-generated terminal states without one.
	HasWorkerResult bool
	Output          string
	Error           string
}

var ErrJobSpecConflict = errors.New("job specification conflicts with stored specification")
var ErrInvalidQueuedPublication = errors.New("invalid queued publication")
var ErrInvalidRunningRepair = errors.New("invalid running repair")

var errJobAlreadyAdmitted = errors.New("job is already durably admitted")
var errJobStoreNotConfigured = errors.New("job store is not configured")

type JobStore interface {
	Load(jobID int64) (JobRecord, bool, error)
	// Create atomically inserts the entire record only if JobID is absent.
	// An existing JobID is unchanged and returns (false, nil), without comparing
	// specifications. Errors return false but do not prove the insert rolled back.
	Create(record JobRecord) (created bool, err error)
	// TryStartQueued atomically changes an existing, unattempted Queued record to
	// Running(1) with workerID. expected must have State=JobQueued, AttemptID=0,
	// empty WorkerID/Output/Error and HasWorkerResult=false; workerID must be nonempty.
	// Invalid inputs return ErrInvalidQueuedPublication (possibly wrapped).
	// The durable record must match expected exactly, including its original spec.
	// No spec normalization or validation is performed here, including for legacy rows.
	// (true, nil) alone authorizes a subsequent lease and dispatch; AttemptID 0 never
	// does. (false, nil) means a missing record or precondition mismatch, with no write.
	// Errors return false and may conceal a committed Running(1): callers must not
	// assume rollback or blindly retry. This does not make other Save writers conditional.
	TryStartQueued(expected JobRecord, workerID string) (published bool, err error)
	// TryRecoverRunning atomically changes only State to Recovering if the entire
	// existing record equals expected: Running, positive AttemptID, assigned Worker,
	// exact stored spec, and no result/residual fields. It never inserts a row.
	// Invalid expected records return ErrInvalidRunningRepair. (false, nil) is a
	// confirmed conflict; errors return false but do not establish rollback.
	TryRecoverRunning(expected JobRecord) (repaired bool, err error)
	// Save inserts absent JobIDs. For existing IDs, it atomically replaces mutable
	// fields only if TaskType, Payload and TimeoutMs match the stored values exactly.
	// ErrJobSpecConflict leaves the entire existing record unchanged.
	// Other errors do not establish whether a write committed.
	Save(record JobRecord) error
	ListByState(state JobState) ([]JobRecord, error)
}

type MemoryJobStores struct {
	mu   sync.Mutex
	jobs map[int64]JobRecord
}

func NewMemoryJobStore() *MemoryJobStores {
	return &MemoryJobStores{
		jobs: make(map[int64]JobRecord),
	}
}

func (s *MemoryJobStores) Create(record JobRecord) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[record.JobID]; exists {
		return false, nil
	}
	s.jobs[record.JobID] = record
	return true, nil
}

// isUnattemptedQueued defines eligibility for first-attempt publication only.
// Create and Save intentionally continue accepting unrelated legacy records.
func isUnattemptedQueued(record JobRecord) bool {
	return record.State == JobQueued && record.AttemptID == 0 && record.WorkerID == "" &&
		!record.HasWorkerResult && record.Output == "" && record.Error == ""
}

func validateQueuedPublication(expected JobRecord, workerID string) error {
	if !isUnattemptedQueued(expected) {
		return fmt.Errorf("%w: expected record must be unattempted Queued without results", ErrInvalidQueuedPublication)
	}
	if workerID == "" {
		return fmt.Errorf("%w: worker ID is empty", ErrInvalidQueuedPublication)
	}
	return nil
}

func (s *MemoryJobStores) TryStartQueued(expected JobRecord, workerID string) (bool, error) {
	if err := validateQueuedPublication(expected, workerID); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current, exists := s.jobs[expected.JobID]
	if !exists || current != expected {
		return false, nil
	}
	current.State, current.AttemptID, current.WorkerID = JobRunning, 1, workerID
	s.jobs[expected.JobID] = current
	return true, nil
}

func validateRunningRepair(expected JobRecord) error {
	if expected.State != JobRunning || expected.AttemptID <= 0 || expected.WorkerID == "" ||
		expected.HasWorkerResult || expected.Output != "" || expected.Error != "" {
		return ErrInvalidRunningRepair
	}
	return nil
}

func (s *MemoryJobStores) TryRecoverRunning(expected JobRecord) (bool, error) {
	if err := validateRunningRepair(expected); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, exists := s.jobs[expected.JobID]; !exists || current != expected {
		return false, nil
	}
	expected.State = JobRecovering
	s.jobs[expected.JobID] = expected
	return true, nil
}

func (s *MemoryJobStores) Save(record JobRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if current, exists := s.jobs[record.JobID]; exists &&
		(current.TaskType != record.TaskType || current.Payload != record.Payload || current.TimeoutMs != record.TimeoutMs) {
		return ErrJobSpecConflict
	}
	s.jobs[record.JobID] = record
	return nil
}

func (s *MemoryJobStores) Load(
	jobID int64,
) (JobRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	record, ok := s.jobs[jobID]
	return record, ok, nil
}

func jobStateFromStatus(status string) (JobState, error) {
	switch strings.ToLower(status) {
	case "succeeded":
		return JobSucceeded, nil
	case "failed":
		return JobFailed, nil
	case "cancelled":
		return JobCancelled, nil
	default:
		return 0, fmt.Errorf("unknown job status %q", status)
	}
}

func (s *MemoryJobStores) ListByState(state JobState) ([]JobRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var records []JobRecord

	for _, record := range s.jobs {
		if record.State == state {
			records = append(records, record)
		}
	}

	return records, nil
}

// persistSubmitCancellation runs while the submit session still owns JobID.
// initialAttempt is the memory generation before this session executed; only
// an attempt published afterward belongs to its cancellation finalization.
// Reload/check/Save uses the same local mutex ordering as publishAttempt and
// persistTerminalResult. Metrics are emitted by the caller after unlocking.
func (s *CoordinatorServer) persistSubmitCancellation(jobID, initialAttempt int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, owned := s.activeJobs[jobID]
	attempt := s.jobAttempts[jobID]
	if s.jobStore == nil || !owned || attempt <= initialAttempt {
		return false, nil
	}
	current, ok, err := s.jobStore.Load(jobID)
	if err != nil {
		return false, fmt.Errorf("load current job: %w", err)
	}
	if !ok || current.JobID != jobID || current.State != JobRunning || current.AttemptID != attempt {
		return false, nil
	}
	current.State = JobCancelled
	current.HasWorkerResult, current.Output, current.Error = false, "", ""
	if err := s.jobStore.Save(current); err != nil {
		return false, fmt.Errorf("save cancelled job: %w", err)
	}
	return true, nil
}

// persistSubmitFailure closes only this submit session's published Running
// generation. The caller retains ownership through Save, as with cancellation.
// An expired lease must not prevent failure finalization. Cancellation is
// checked again after Load; once Save succeeds the terminal commit wins.
func (s *CoordinatorServer) persistSubmitFailure(ctx context.Context, jobID, initialAttempt, expectedAttempt int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return false, err
	}
	_, owned := s.activeJobs[jobID]
	if s.jobStore == nil || !owned || expectedAttempt <= initialAttempt || expectedAttempt <= 0 || s.jobAttempts[jobID] != expectedAttempt {
		return false, nil
	}
	current, ok, err := s.jobStore.Load(jobID)
	if err != nil {
		return false, fmt.Errorf("load current job: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if !ok || current.JobID != jobID || current.State != JobRunning || current.AttemptID != expectedAttempt {
		return false, nil
	}
	current.State = JobFailed
	current.HasWorkerResult, current.Output, current.Error = false, "", ""
	if err := s.jobStore.Save(current); err != nil {
		return false, fmt.Errorf("save failed job: %w", err)
	}
	return true, nil
}

func (s *CoordinatorServer) recoverRunningJobs() error {
	observer := metrics.Resolve(s.metrics)
	if s.jobStore == nil {
		return nil
	}

	records, err := s.jobStore.ListByState(JobRunning)
	if err != nil {
		return err
	}

	for _, record := range records {
		if err := validateJobSpec(jobSpecFromRecord(record)); err != nil {
			// Preserve invalid legacy rows for operator correction.
			fmt.Printf("startup recovery rejected job %d: invalid job specification\n", record.JobID)
			continue
		}
		record.State = JobRecovering
		record.HasWorkerResult, record.Output, record.Error = false, "", ""

		if err := s.jobStore.Save(record); err != nil {
			return err
		}

		observer.RecoveryTransition(metrics.RecoveryStartup)
		fmt.Printf(
			"recovering job: id=%d attempt=%d worker=%s\n",
			record.JobID,
			record.AttemptID,
			record.WorkerID,
		)
	}

	return nil
}

func (s *CoordinatorServer) retryRecoveringJob(
	ctx context.Context,
	record JobRecord,
) error {
	_, err := s.retryRecoveringJobResult(ctx, record)
	return err
}

// recoveryExecution records provenance before releasing ownership. A nil error
// alone does not imply execution: a stale snapshot is deliberately skipped.
type recoveryExecution struct {
	executed              bool
	terminal              bool
	reconciliationPending bool // Existing ledger owns settlement before claim release.
	generation            int64
	specHash              [32]byte
}

func (s *CoordinatorServer) retryRecoveringJobResult(ctx context.Context, record JobRecord) (result recoveryExecution, err error) {
	result.generation = record.AttemptID
	result.specHash = recoverySpecHash(record)
	if record.State != JobRecovering {
		return result, fmt.Errorf(
			"job %d is not recovering",
			record.JobID,
		)
	}

	if !s.claimJob(record.JobID) {
		return result, fmt.Errorf(
			"job %d is already active",
			record.JobID,
		)
	}
	defer func() {
		s.releaseJob(record.JobID)
		if result.reconciliationPending {
			s.notifyRecoveryReady()
		}
	}()

	// A submission may have completed between the recovery pass's reload and
	// this claim. Revalidate under ownership before executing the snapshot.
	if s.jobStore != nil {
		current, ok, err := s.jobStore.Load(record.JobID)
		if err != nil {
			return result, err
		}
		if !ok || current.State != JobRecovering || current.AttemptID != record.AttemptID {
			return result, nil
		}
		record = current
	}

	result.specHash = recoverySpecHash(record)
	spec := jobSpecFromRecord(record)

	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	defer func() { observer.ObserveJobExecution(metrics.SourceRecovery, time.Since(start)) }()
	result.executed = true
	// Keep the shared engine's confirmed/attempted identities; the legacy
	// response adapter discards precisely the provenance needed on write errors.
	session := s.executeAttemptSession(ctx, spec, metrics.SourceRecovery, attemptSessionStart{mode: continueGeneration})
	result.terminal = session.err == nil
	if result.terminal {
		result.generation = session.terminal.record.AttemptID
		return result, nil
	}
	observer.RecoveryExecutionFailed(recoveryFailureReason(session.err))
	if session.lastPublished != nil {
		result.generation = session.lastPublished.AttemptID
	}
	if session.pendingPublication != nil {
		result.generation = session.pendingPublication.AttemptID
	}
	_, result.reconciliationPending, err = s.settleAttemptSession(session, &record)
	return result, err
}
