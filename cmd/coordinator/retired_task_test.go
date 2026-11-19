package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubmitRetiredTaskRejectsBeforeAdmission(t *testing.T) {
	store := &recoveryPassStore{
		load: func(int64) (JobRecord, bool, error) {
			t.Fatal("retired task reached a job store read")
			return JobRecord{}, false, nil
		},
		create: func(JobRecord) (bool, error) {
			t.Fatal("retired task reached admission")
			return false, nil
		},
	}
	s := &CoordinatorServer{jobStore: store}
	const marker = "PRIVATE_LEGACY_PAYLOAD"
	req := &runtimepb.SubmitJobRequest{JobId: 14006, TaskType: "zk_preimage_prove", Payload: marker, TimeoutMs: 1000}
	ack, err := s.SubmitJob(t.Context(), req)
	if ack != nil || status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "retired") || strings.Contains(err.Error(), marker) {
		t.Fatalf("retired admission response = %v, %v", ack, err)
	}
	if len(s.activeJobs) != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatal("retired task acquired runtime state")
	}
}

func TestRetiredTaskStartupPreservesSQLite(t *testing.T) {
	files := testpki.New(t).Issue(t, testpki.Coordinator, nil)
	security := coordinatorSecurityConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, Identity: testpki.Coordinator}
	cfg := coordinatorExecutionConfig{ListenAddress: "127.0.0.1:0", AttemptLease: time.Second, ShutdownTimeout: time.Second}
	for _, tc := range []struct {
		name  string
		state JobState
	}{{"queued", JobQueued}, {"running", JobRunning}, {"recovering", JobRecovering}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			store, err := NewSQLiteJobStore(path)
			if err != nil {
				t.Fatal(err)
			}
			retired := JobRecord{JobID: 42, State: tc.state, TaskType: retiredTaskType, Payload: "PRIVATE_LEGACY_PAYLOAD", TimeoutMs: 1000}
			if tc.state != JobQueued {
				retired.AttemptID, retired.WorkerID = 7, "old"
			}
			ordinary := JobRecord{JobID: 43, State: JobRunning, AttemptID: 4, WorkerID: "old", TaskType: "hash", Payload: "recover", TimeoutMs: 1000}
			terminal := withWorkerResult(JobRecord{JobID: 44, State: JobFailed, AttemptID: 9, WorkerID: "old", TaskType: retiredTaskType, Payload: "stored spec", TimeoutMs: 1000}, "", "stored error")
			records := []JobRecord{retired, ordinary, terminal}
			for _, record := range records {
				saveRecoveryRecord(t, store, record)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err = runCoordinator(ctx, cfg, security, path, "127.0.0.1:0")
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("job %d has unfinished retired task type", retired.JobID)) ||
				!strings.Contains(err.Error(), "different database") || strings.Contains(err.Error(), retired.Payload) {
				t.Fatalf("startup refusal = %v", err)
			}
			reopened, err := NewSQLiteJobStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, record := range records {
				assertSQLiteJobRecord(t, reopened, record)
			}
		})
	}
	t.Run("schema before terminal results", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "jobs.db")
		db, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		_, err = db.Exec(`CREATE TABLE jobs (
			job_id INTEGER PRIMARY KEY, state INTEGER NOT NULL, attempt_id INTEGER NOT NULL,
			worker_id TEXT NOT NULL, task_type TEXT NOT NULL, payload TEXT NOT NULL, timeout_ms INTEGER NOT NULL);
			INSERT INTO jobs VALUES (42, ?, 7, 'old', ?, 'retired payload', 1000),
			(43, ?, 4, 'old', 'hash', 'ordinary payload', 1000);`, JobRunning, retiredTaskType, JobRunning)
		if closeErr := db.Close(); err != nil || closeErr != nil {
			t.Fatalf("seed legacy schema: %v; close: %v", err, closeErr)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		err = runCoordinator(ctx, cfg, security, path, "127.0.0.1:0")
		if err == nil || !strings.Contains(err.Error(), "job 42 has unfinished retired task type") {
			t.Fatalf("startup refusal = %v", err)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("refused startup changed the legacy database")
		}
	})
}
