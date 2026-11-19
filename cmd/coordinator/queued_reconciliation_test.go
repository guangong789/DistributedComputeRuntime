package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func installQueuedReconciliation(s *CoordinatorServer, expected JobRecord, previous *JobRecord) *queuedReconciliation {
	entry := &queuedReconciliation{expected: expected, lastPublished: previous, cause: errors.New("unresolved publication")}
	s.mu.Lock()
	if s.queuedReconciliations == nil {
		s.queuedReconciliations = make(map[int64]*queuedReconciliation)
	}
	s.queuedReconciliations[expected.JobID] = entry
	s.mu.Unlock()
	return entry
}

func TestQueuedReconciliationDurableStates(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, scenario := range []string{"running", "retry candidate", "retry previous", "recovering", "recovering previous", "queued",
			"succeeded empty", "failed", "cancelled before publication", "newer terminal", "exact payload bytes", "missing", "different generation",
			"different worker", "different spec", "recovering conflict", "inconsistent queued", "inconsistent result", "inconsistent terminal", "unknown state"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				s, inner, seed := queuedSessionFixture(t, backend)
				expected := seed
				expected.JobID++
				expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
				var previous *JobRecord
				if strings.HasPrefix(scenario, "retry ") || scenario == "recovering previous" {
					old := expected
					previous = &old
					expected.AttemptID, expected.WorkerID = 2, "retry-worker"
				}
				if scenario == "exact payload bytes" {
					expected.TaskType, expected.Payload = "hash", `  original input  `
				}
				current := expected
				unresolved := false
				switch scenario {
				case "retry previous":
					current = *previous
				case "recovering":
					current.State = JobRecovering
				case "recovering previous":
					current = *previous
					current.State = JobRecovering
				case "queued":
					current.State, current.AttemptID, current.WorkerID = JobQueued, 0, ""
				case "succeeded empty":
					current.State, current.HasWorkerResult = JobSucceeded, true
				case "failed":
					current.State, current.HasWorkerResult, current.Error = JobFailed, true, "task failed"
				case "cancelled before publication":
					current.State, current.AttemptID, current.WorkerID = JobCancelled, 0, ""
				case "newer terminal":
					current.State, current.AttemptID, current.WorkerID, current.HasWorkerResult = JobSucceeded, 9, "other", true
				case "missing":
					unresolved = true
				case "different generation":
					current.AttemptID++
					unresolved = true
				case "different worker":
					current.WorkerID = "other"
					unresolved = true
				case "different spec":
					current.Payload += " "
					unresolved = true
				case "recovering conflict":
					current.State, current.AttemptID = JobRecovering, 9
					unresolved = true
				case "inconsistent queued":
					current.State = JobQueued
					unresolved = true
				case "inconsistent result":
					current.Output = "residual"
					unresolved = true
				case "inconsistent terminal":
					current.State, current.Output = JobSucceeded, "absent result with output"
					unresolved = true
				case "unknown state":
					current.State = JobState(99)
					unresolved = true
				}
				if scenario != "missing" {
					if ok, err := inner.Create(current); !ok || err != nil {
						t.Fatalf("Create=%v, %v", ok, err)
					}
				}
				entry := installQueuedReconciliation(s, expected, previous)
				var calls atomic.Int32
				registerTestWorker(t, s, "selected", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					return successfulResult(req), nil
				})
				clock := newManualRecoveryClock()
				q := testRecoverySchedule(clock)
				attempted, err := s.reconcileQueuedJob(s.lifecycle.ctx, expected.JobID, entry, q)
				if !attempted || (err != nil) != unresolved {
					t.Fatalf("attempted=%v error=%v unresolved=%v", attempted, err, unresolved)
				}
				want := current
				if !unresolved && current.State == JobRunning {
					want.State = JobRecovering
				}
				if scenario != "missing" {
					assertRecoveryRecord(t, inner, want)
				} else if _, exists, err := inner.Load(expected.JobID); exists || err != nil {
					t.Fatalf("missing inserted: %v, %v", exists, err)
				}
				if calls.Load() != 0 || len(s.leases) != 0 || len(s.jobAttempts) != 0 {
					t.Fatal("reconciliation authorized execution")
				}
				if unresolved && (entry.failures != 1 || !entry.nextRetry.Equal(clock.now().Add(time.Second)) || entry.cause == nil) {
					t.Fatalf("lost retry obligation: %+v", entry)
				}
				assertQueuedSessionReleased(t, s, expected.JobID, unresolved)
			})
		}
	}
}

func TestQueuedReconciliationAtomicRepairRace(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, change := range []string{"terminal", "generation"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				s, inner, queued := queuedSessionFixture(t, backend)
				expected := queued
				expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
				saveRecoveryRecord(t, inner, expected)
				entry := installQueuedReconciliation(s, expected, nil)
				changed := expected
				if change == "terminal" {
					changed.State, changed.HasWorkerResult = JobSucceeded, true
				} else {
					changed.AttemptID, changed.WorkerID = 7, "other"
				}
				repairs := 0
				s.jobStore = &recoveryPassStore{JobStore: inner, recoverRunning: func(r JobRecord) (bool, error) {
					repairs++
					// This commits AFTER the reconciler's Load, BEFORE its mutation.
					saveRecoveryRecord(t, inner, changed)
					return inner.TryRecoverRunning(r)
				}}
				clock := newManualRecoveryClock()
				q := testRecoverySchedule(clock)
				if _, err := s.reconcileQueuedJob(s.lifecycle.ctx, queued.JobID, entry, q); err == nil {
					t.Fatal("stale repair was accepted")
				}
				assertRecoveryRecord(t, inner, changed)
				assertQueuedSessionReleased(t, s, queued.JobID, true)
				clock.advance(time.Second)
				_, err := s.reconcileQueuedJob(s.lifecycle.ctx, queued.JobID, entry, q)
				if (err == nil) != (change == "terminal") || repairs != 1 {
					t.Fatalf("second pass: error=%v repairs=%d", err, repairs)
				}
				assertRecoveryRecord(t, inner, changed)
				assertQueuedSessionReleased(t, s, queued.JobID, change != "terminal")
			})
		}
	}
}

func TestQueuedReconciliationClaimsAndReplacement(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "memory")
	expected := queued
	expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
	saveRecoveryRecord(t, inner, expected)
	old := installQueuedReconciliation(s, expected, nil)
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	s.jobStore = &recoveryPassStore{JobStore: inner, load: func(id int64) (JobRecord, bool, error) { close(entered); <-release; return inner.Load(id) }}
	result := make(chan error, 1)
	go func() { _, err := s.reconcileQueuedJob(s.lifecycle.ctx, queued.JobID, old, q); result <- err }()
	awaitShutdownSignal(t, entered)
	if s.claimJob(queued.JobID) {
		t.Fatal("ordinary claim bypassed reconciler")
	}
	if attempted, err := s.reconcileQueuedJob(s.lifecycle.ctx, queued.JobID, old, q); attempted || err != nil {
		t.Fatalf("second reconciler acquired: %v, %v", attempted, err)
	}
	if err := s.runQueuedJob(queued.JobID); status.Code(err) != codes.Aborted {
		t.Fatalf("queued claim=%v", err)
	}
	recovering := expected
	recovering.State = JobRecovering
	if err := s.retryRecoveringJob(context.Background(), recovering); err == nil {
		t.Fatal("ordinary recovery bypassed reconciler")
	}
	other := queued.JobID + 1
	if !s.claimJob(other) {
		t.Fatal("unrelated JobID blocked")
	}
	s.releaseJob(other)
	// Replacement is synchronized; the old owner still excludes every contender.
	replacement := installQueuedReconciliation(s, expected, nil)
	if s.claimQueuedReconciliation(queued.JobID, replacement, clock.now()) {
		t.Fatal("replacement acquired before previous owner released")
	}
	unblock()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if s.queuedReconciliations[queued.JobID] != replacement {
		t.Fatal("stale completion retired newer responsibility")
	}
	if s.finishQueuedReconciliation(queued.JobID, old, nil, q) {
		t.Fatal("stale retirement accepted")
	}
	if s.finishQueuedReconciliation(queued.JobID, old, errors.New("stale failure"), q) {
		t.Fatal("stale error completion accepted")
	}
	if replacement.failures != 0 || replacement.cause == nil {
		t.Fatal("stale completion changed replacement metadata")
	}
	s.jobStore = inner
	if attempted, err := s.reconcileQueuedJob(s.lifecycle.ctx, queued.JobID, replacement, q); !attempted || err != nil {
		t.Fatalf("replacement=%v, %v", attempted, err)
	}
	assertQueuedSessionReleased(t, s, queued.JobID, false)
}

func TestQueuedReconciliationRepairAcknowledgementLoss(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			s, inner, queued := queuedSessionFixture(t, backend)
			expected := queued
			expected.State, expected.AttemptID, expected.WorkerID = JobRunning, 1, "selected"
			saveRecoveryRecord(t, inner, expected)
			entry := installQueuedReconciliation(s, expected, nil)
			failed, repairs := false, 0
			injected := errors.New("repair acknowledgement lost")
			s.jobStore = &recoveryPassStore{JobStore: inner,
				recoverRunning: func(r JobRecord) (bool, error) {
					repairs++
					if ok, err := inner.TryRecoverRunning(r); !ok || err != nil {
						return ok, err
					}
					failed = true
					return false, injected
				},
				load: func(id int64) (JobRecord, bool, error) {
					if failed {
						return JobRecord{}, false, errors.New("confirmation read unavailable")
					}
					return inner.Load(id)
				}}
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			if _, err := s.reconcileQueuedJob(s.lifecycle.ctx, queued.JobID, entry, q); err == nil {
				t.Fatal("unconfirmed repair reported settlement")
			}
			want := expected
			want.State = JobRecovering
			assertRecoveryRecord(t, inner, want)
			assertQueuedSessionReleased(t, s, queued.JobID, true)
			s.jobStore = inner
			clock.advance(time.Second)
			if attempted, err := s.reconcileQueuedJob(s.lifecycle.ctx, queued.JobID, entry, q); !attempted || err != nil {
				t.Fatalf("confirmation=%v, %v", attempted, err)
			}
			if repairs != 1 {
				t.Fatalf("repair repeated %d times", repairs)
			}
			assertRecoveryRecord(t, inner, want)
			assertQueuedSessionReleased(t, s, queued.JobID, false)
		})
	}
}
