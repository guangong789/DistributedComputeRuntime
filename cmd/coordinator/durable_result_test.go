package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func withWorkerResult(record JobRecord, output, resultError string) JobRecord {
	record.HasWorkerResult, record.Output, record.Error = true, output, resultError
	return record
}

// Background recovery has no SubmitJob response. Validate the retained proof
// independently before using its randomized encoding in full-record assertions.
func storedProofOutput(t *testing.T, store JobStore, jobID int64, fixture integrationZKFixture) string {
	t.Helper()
	r, ok, err := store.Load(jobID)
	if err != nil || !ok || !r.HasWorkerResult || r.Error != "" {
		t.Fatalf("missing durable proof: record=%+v found=%v error=%v", r, ok, err)
	}
	proof, err := base64.StdEncoding.DecodeString(r.Output)
	public, payloadErr := zk.ParsePreimagePublicPayload(fixture.payload)
	if err != nil || payloadErr != nil || len(proof) == 0 || fixture.verifier.Verify(proof, public.Digest) != nil {
		t.Fatal("durable proof did not verify")
	}
	return r.Output
}

func TestSQLiteTerminalResultMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = legacy.Exec(`CREATE TABLE jobs (
		job_id INTEGER PRIMARY KEY, state INTEGER NOT NULL, attempt_id INTEGER NOT NULL,
		worker_id TEXT NOT NULL, task_type TEXT NOT NULL DEFAULT '',
		payload TEXT NOT NULL DEFAULT '', timeout_ms INTEGER NOT NULL DEFAULT 0);
		INSERT INTO jobs VALUES (1, 2, 7, 'old', 'hash', 'hello', 1000),
		(2, 3, 8, 'old', 'hash', '', 1000), (3, 4, 9, 'old', 'sleep', '1s', 1000);`)
	closeErr := legacy.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("create previous schema: %v; close: %v", err, closeErr)
	}
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := 0; i < 2; i++ {
		if err := store.initSchema(); err != nil {
			t.Fatalf("repeated initialization: %v", err)
		}
	}
	for _, r := range []JobRecord{
		{JobID: 1, State: JobSucceeded, AttemptID: 7, WorkerID: "old", TaskType: "hash", Payload: "hello", TimeoutMs: 1000},
		{JobID: 2, State: JobFailed, AttemptID: 8, WorkerID: "old", TaskType: "hash", TimeoutMs: 1000},
		{JobID: 3, State: JobCancelled, AttemptID: 9, WorkerID: "old", TaskType: "sleep", Payload: "1s", TimeoutMs: 1000},
	} {
		assertSQLiteJobRecord(t, store, r)
		assertSQLiteStateRecord(t, store, r)
		// An accepted empty result remains distinguishable from legacy absence.
		accepted := withWorkerResult(r, "", "")
		if err := store.Save(accepted); err != nil {
			t.Fatal(err)
		}
		assertSQLiteJobRecord(t, store, accepted)
		assertSQLiteStateRecord(t, store, accepted)
	}
}

func TestNonWorkerWritesClearTerminalResults(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, operation := range []string{"new attempt", "cancel", "retry exhaustion", "startup recovery"} {
			t.Run(backend+"/"+operation, func(t *testing.T) {
				var store JobStore = NewMemoryJobStore()
				if backend == "sqlite" {
					db, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
					if err != nil {
						t.Fatal(err)
					}
					defer db.Close()
					store = db
				}
				// Deliberately seed stale fields to test every clearing transition.
				record := withWorkerResult(JobRecord{JobID: 1, State: JobRunning, AttemptID: 7,
					WorkerID: "old", TaskType: "hash", Payload: "hello", TimeoutMs: 1000}, "old output", "old error")
				if operation == "new attempt" {
					record.State = JobSucceeded
				}
				saveRecoveryRecord(t, store, record)
				s := &CoordinatorServer{jobStore: store, jobAttempts: map[int64]int64{1: 7}}
				if !s.claimJob(1) {
					t.Fatal("claim failed")
				}
				defer s.releaseJob(1)
				want := record
				want.HasWorkerResult, want.Output, want.Error = false, "", ""
				switch operation {
				case "new attempt":
					attempt, err := s.startAttempt(1, "new", time.Hour, record.TaskType, record.Payload, record.TimeoutMs)
					if err != nil || attempt != 8 {
						t.Fatalf("attempt=%d error=%v", attempt, err)
					}
					want.State, want.AttemptID, want.WorkerID = JobRunning, 8, "new"
				case "cancel":
					ok, err := s.persistSubmitCancellation(1, 0)
					if !ok || err != nil {
						t.Fatalf("cancel: persisted=%v error=%v", ok, err)
					}
					want.State = JobCancelled
				case "retry exhaustion":
					ok, err := s.persistSubmitFailure(context.Background(), 1, 0, 7)
					if !ok || err != nil {
						t.Fatalf("failure: persisted=%v error=%v", ok, err)
					}
					want.State = JobFailed
				case "startup recovery":
					if err := s.recoverRunningJobs(); err != nil {
						t.Fatal(err)
					}
					want.State = JobRecovering
				}
				assertRecoveryRecord(t, store, want)
				assertListedRecords(t, store, want.State, want)
			})
		}
	}
}

func TestSQLiteTerminalStateAndResultCommitTogether(t *testing.T) {
	store, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s := &CoordinatorServer{jobStore: store}
	attempt, err := s.startAttempt(1, "worker", time.Hour, "hash", "hello", 1000)
	if err != nil {
		t.Fatal(err)
	}
	running, _, err := store.Load(1)
	if err != nil {
		t.Fatal(err)
	}
	terminal := withWorkerResult(running, strings.Repeat("proof", 16384), "error details")
	terminal.State = JobSucceeded
	if _, err := store.db.Exec(`CREATE TRIGGER reject_terminal AFTER UPDATE ON jobs
		WHEN NEW.has_worker_result = 1 BEGIN SELECT RAISE(ABORT, 'injected failure'); END;`); err != nil {
		t.Fatal(err)
	}
	eligible, err := s.persistTerminalResult(context.Background(), terminal)
	if !eligible || err == nil {
		t.Fatalf("attempt %d: eligible=%v error=%v", attempt, eligible, err)
	}
	assertSQLiteJobRecord(t, store, running)
	assertListedRecords(t, store, JobSucceeded)
	if _, err := store.db.Exec(`DROP TRIGGER reject_terminal`); err != nil {
		t.Fatal(err)
	}
	if eligible, err := s.persistTerminalResult(context.Background(), terminal); !eligible || err != nil {
		t.Fatalf("eligible=%v error=%v", eligible, err)
	}
	assertSQLiteJobRecord(t, store, terminal)
	assertSQLiteStateRecord(t, store, terminal)
}

func TestTerminalCommitWithLostStoreAcknowledgement(t *testing.T) {
	for _, scenario := range []string{"submit", "submit cancellation", "recovery"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			db, err := NewSQLiteJobStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ackErr := errors.New("terminal commit acknowledgement lost")
			saves := 0
			store := &recoveryPassStore{JobStore: db, save: func(r JobRecord) error {
				if err := db.Save(r); err != nil {
					return err
				}
				if r.HasWorkerResult {
					saves++
					if scenario == "submit cancellation" {
						cancel()
					}
					return ackErr
				}
				return nil
			}}
			s := &CoordinatorServer{jobStore: store}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			want := JobRecord{JobID: 1, State: JobSucceeded, AttemptID: 1, WorkerID: "worker", TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
			if scenario == "recovery" {
				record := want
				record.State, record.AttemptID = JobRecovering, 7
				saveRecoveryRecord(t, db, record)
				err = s.retryRecoveringJob(ctx, record)
				want.AttemptID = 8
			} else {
				var resp *runtimepb.SubmitJobResponse
				resp, err = s.executeSubmissionForTest(ctx, &runtimepb.SubmitJobRequest{JobId: 1, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
				if resp != nil {
					t.Fatalf("unacknowledged persistence returned acceptance: %v", resp)
				}
			}
			if scenario == "submit cancellation" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancellation error=%v", err)
				}
			} else if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), ackErr.Error()) {
				t.Fatalf("persistence error=%v", err)
			}
			if calls.Load() != 1 || saves != 1 {
				t.Fatalf("unacknowledged commit retried: calls=%d terminal saves=%d", calls.Load(), saves)
			}
			assertRecoveryOwnershipReleased(t, s, 1)
			// A client retry cannot replace a terminal result after a lost store
			// acknowledgement, including when the original context was cancelled.
			duplicate, duplicateErr := s.executeSubmissionForTest(context.Background(), &runtimepb.SubmitJobRequest{
				JobId: 1, TaskType: "hash", Payload: "hello", TimeoutMs: 1000,
			})
			if duplicate != nil || status.Code(duplicateErr) != codes.AlreadyExists || calls.Load() != 1 || saves != 1 {
				t.Fatalf("lost-ack terminal duplicate: response=%v error=%v calls=%d saves=%d", duplicate, duplicateErr, calls.Load(), saves)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewSQLiteJobStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			assertSQLiteJobRecord(t, reopened, withWorkerResult(want, "done", ""))
		})
	}
}
