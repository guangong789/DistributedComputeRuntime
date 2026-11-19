package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func assertGetJobSnapshot(t *testing.T, got *runtimepb.GetJobResponse, record JobRecord, state runtimepb.JobState) {
	t.Helper()
	want := &runtimepb.GetJobResponse{JobId: record.JobID, State: state, AttemptId: record.AttemptID,
		WorkerId: record.WorkerID, HasWorkerResult: record.HasWorkerResult, Output: record.Output, Error: record.Error}
	if !proto.Equal(got, want) {
		t.Fatalf("GetJob=%v, want %v", got, want)
	}
}

func TestGetJobPersistedSnapshots(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			inner := newCreationTestStore(t, backend)
			for index, tc := range []struct {
				name            string
				state           JobState
				wire            runtimepb.JobState
				attempt         int64
				worker          string
				present         bool
				output, message string
			}{
				{"queued", JobQueued, runtimepb.JobState_JOB_STATE_QUEUED, 0, "", false, "", ""},
				{"running", JobRunning, runtimepb.JobState_JOB_STATE_RUNNING, 1, "w1", false, "", ""},
				{"recovering", JobRecovering, runtimepb.JobState_JOB_STATE_RECOVERING, 7, "w7", false, "", ""},
				{"succeeded", JobSucceeded, runtimepb.JobState_JOB_STATE_SUCCEEDED, 2, "w2", true, "accepted", ""},
				{"empty succeeded", JobSucceeded, runtimepb.JobState_JOB_STATE_SUCCEEDED, 2, "w2", true, "", ""},
				{"Worker failed", JobFailed, runtimepb.JobState_JOB_STATE_FAILED, 3, "w3", true, "", "worker reported failure"},
				{"Worker cancelled", JobCancelled, runtimepb.JobState_JOB_STATE_CANCELLED, 4, "w4", true, "", "worker cancelled"},
				{"empty Worker failed", JobFailed, runtimepb.JobState_JOB_STATE_FAILED, 3, "w3", true, "", ""},
				{"empty Worker cancelled", JobCancelled, runtimepb.JobState_JOB_STATE_CANCELLED, 4, "w4", true, "", ""},
				{"Coordinator failed", JobFailed, runtimepb.JobState_JOB_STATE_FAILED, 2, "w2", false, "", ""},
				{"Coordinator cancelled before execution", JobCancelled, runtimepb.JobState_JOB_STATE_CANCELLED, 0, "", false, "", ""},
				{"legacy succeeded", JobSucceeded, runtimepb.JobState_JOB_STATE_SUCCEEDED, 5, "old", false, "", ""},
				{"legacy failed", JobFailed, runtimepb.JobState_JOB_STATE_FAILED, 5, "old", false, "", ""},
				{"legacy cancelled", JobCancelled, runtimepb.JobState_JOB_STATE_CANCELLED, 5, "old", false, "", ""},
				{"legacy strings do not imply presence", JobFailed, runtimepb.JobState_JOB_STATE_FAILED, 5, "old", false, "legacy output", "legacy error"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					r := JobRecord{JobID: int64(12000 + index), State: tc.state, AttemptID: tc.attempt, WorkerID: tc.worker,
						TaskType: "hash", Payload: "PRIVATE_MARKER", TimeoutMs: 1500,
						HasWorkerResult: tc.present, Output: tc.output, Error: tc.message}
					if ok, err := inner.Create(r); !ok || err != nil {
						t.Fatalf("Create=%v, %v", ok, err)
					}
					loads := 0
					s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, load: func(id int64) (JobRecord, bool, error) { loads++; return inner.Load(id) }}}
					got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: r.JobID})
					if err != nil {
						t.Fatal(err)
					}
					assertGetJobSnapshot(t, got, r, tc.wire)
					if loads != 1 || strings.Contains(got.String(), "PRIVATE_MARKER") {
						t.Fatal("extra read or leaked JobSpec")
					}
					assertRecoveryRecord(t, inner, r) // Original stored bytes remain unchanged.
				})
			}
		})
	}
}

func TestGetJobErrorsAndJobIDContract(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request *runtimepb.GetJobRequest
		store   JobStore
		code    codes.Code
	}{
		{"missing request", nil, NewMemoryJobStore(), codes.InvalidArgument},
		{"store unconfigured", &runtimepb.GetJobRequest{JobId: 1}, nil, codes.FailedPrecondition},
		{"not found", &runtimepb.GetJobRequest{JobId: 1}, NewMemoryJobStore(), codes.NotFound},
		{"read failure", &runtimepb.GetJobRequest{JobId: 1}, &recoveryPassStore{load: func(int64) (JobRecord, bool, error) {
			return JobRecord{}, false, errors.New("SQLITE_BUSY: private /database/path; secret SQL")
		}}, codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &CoordinatorServer{jobStore: tc.store}
			got, err := s.GetJob(context.Background(), tc.request)
			if got != nil || status.Code(err) != tc.code {
				t.Fatalf("response=%v error=%v", got, err)
			}
			if strings.Contains(err.Error(), "SQLITE") || strings.Contains(err.Error(), "private") {
				t.Fatal("storage internals leaked to client")
			}
		})
	}
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newCreationTestStore(t, backend)
			s := &CoordinatorServer{jobStore: store}
			for _, id := range []int64{0, -1, math.MinInt64, math.MaxInt64} {
				r := JobRecord{JobID: id, State: JobQueued}
				if ok, err := store.Create(r); !ok || err != nil {
					t.Fatalf("Create=%v, %v", ok, err)
				}
				got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: id})
				if err != nil {
					t.Fatalf("existing int64 identity %d rejected: %v", id, err)
				}
				assertGetJobSnapshot(t, got, r, runtimepb.JobState_JOB_STATE_QUEUED)
			}
			bad := JobRecord{JobID: 42, State: JobState(99)}
			saveRecoveryRecord(t, store, bad)
			if got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: bad.JobID}); got != nil || status.Code(err) != codes.Internal {
				t.Fatalf("unknown persisted state=%v, %v", got, err)
			}
			assertRecoveryRecord(t, store, bad)
		})
	}
}

func TestGetJobCancellation(t *testing.T) {
	for _, duringRead := range []bool{false, true} {
		t.Run(fmt.Sprint(duringRead), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			loads := 0
			s := &CoordinatorServer{jobStore: &recoveryPassStore{load: func(id int64) (JobRecord, bool, error) {
				loads++
				cancel()
				return JobRecord{JobID: id, State: JobQueued}, true, nil
			}}}
			if !duringRead {
				cancel()
			}
			got, err := s.GetJob(ctx, &runtimepb.GetJobRequest{JobId: 1})
			wantLoads := 0
			if duringRead {
				wantLoads = 1
			}
			if got != nil || status.Code(err) != codes.Canceled || loads != wantLoads {
				t.Fatalf("GetJob=%v, %v loads=%d", got, err, loads)
			}
		})
	}
}

func TestGetJobHasNoExecutionOrMutationSideEffects(t *testing.T) {
	inner := NewMemoryJobStore()
	r := JobRecord{JobID: 12100, State: JobRunning, AttemptID: 1, WorkerID: "persisted", TaskType: "hash", Payload: "private", TimeoutMs: 1000}
	saveRecoveryRecord(t, inner, r)
	entry := &queuedReconciliation{expected: r, cause: errors.New("pending"), failures: 3, nextRetry: time.Now().Add(time.Minute)}
	beforeEntry := *entry
	observer := &recordingMetrics{}
	loads := 0
	// An embedded nil store makes any unintended Store method fail immediately.
	readOnly := &recoveryPassStore{load: func(id int64) (JobRecord, bool, error) { loads++; return inner.Load(id) }}
	s := &CoordinatorServer{jobStore: readOnly, metrics: observer, recoveryReady: make(chan struct{}, 1),
		activeJobs: map[int64]struct{}{r.JobID: {}}, queuedReconciliations: map[int64]*queuedReconciliation{r.JobID: entry},
		jobAttempts: map[int64]int64{r.JobID: 999}, leases: map[int64]JobLease{r.JobID: {AttemptID: 999, WorkerID: "live"}},
		workers: map[string]WorkerInfo{"live": {ID: "live", Status: WorkerAlive}}, workerOrder: []string{"live"}}
	// Live state is deliberately contradictory. GetJob must only use Load and
	// must remain callable while execution ownership and reconciliation coexist.
	got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: r.JobID})
	if err != nil {
		t.Fatal(err)
	}
	assertGetJobSnapshot(t, got, r, runtimepb.JobState_JOB_STATE_RUNNING)
	if loads != 1 || s.nextWorker != 0 || len(s.activeJobs) != 1 || s.jobAttempts[r.JobID] != 999 || s.leases[r.JobID].WorkerID != "live" ||
		len(s.recoveryReady) != 0 || s.queuedReconciliations[r.JobID] != entry || !reflect.DeepEqual(*entry, beforeEntry) || len(observer.snapshot()) != 0 {
		t.Fatal("GetJob changed execution, ledger, scheduling or metrics")
	}
	assertRecoveryRecord(t, inner, r)
}

func TestGetJobSnapshotAcrossConcurrentWrite(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			inner := newCreationTestStore(t, backend)
			old := JobRecord{JobID: 12101, State: JobRunning, AttemptID: 1, WorkerID: "old", TaskType: "hash", Payload: "same", TimeoutMs: 1000}
			saveRecoveryRecord(t, inner, old)
			loaded, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var reads atomic.Int32
			s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, load: func(id int64) (JobRecord, bool, error) {
				r, ok, err := inner.Load(id)
				if reads.Add(1) == 1 {
					close(loaded)
					<-release
				}
				return r, ok, err
			}}}
			type outcome struct {
				response *runtimepb.GetJobResponse
				err      error
			}
			done := make(chan outcome, 1)
			go func() {
				response, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: old.JobID})
				done <- outcome{response, err}
			}()
			waitRecoverySignal(t, loaded)
			newer := old
			newer.State, newer.AttemptID, newer.WorkerID, newer.HasWorkerResult, newer.Output = JobSucceeded, 2, "new", true, "new result"
			saveRecoveryRecord(t, inner, newer)
			unblock()
			result := <-done
			if result.err != nil {
				t.Fatal(result.err)
			}
			assertGetJobSnapshot(t, result.response, old, runtimepb.JobState_JOB_STATE_RUNNING)
			if reads.Load() != 1 {
				t.Fatal("request mixed multiple reads")
			}
			next, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: old.JobID})
			if err != nil {
				t.Fatal(err)
			}
			assertGetJobSnapshot(t, next, newer, runtimepb.JobState_JOB_STATE_SUCCEEDED)
		})
	}
}

func TestGetJobSQLiteReopenAndLegacyMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	r := JobRecord{JobID: 12102, State: JobSucceeded, AttemptID: 2, WorkerID: "worker", TaskType: "hash", Payload: "private", TimeoutMs: 1000, HasWorkerResult: true}
	saveRecoveryRecord(t, store, r)
	// A pre-result-fields schema must migrate without inventing Worker presence.
	if _, err := store.db.Exec(`ALTER TABLE jobs RENAME TO current_jobs;
		CREATE TABLE jobs (job_id INTEGER PRIMARY KEY, state INTEGER NOT NULL, attempt_id INTEGER NOT NULL,
		worker_id TEXT NOT NULL, task_type TEXT NOT NULL, payload TEXT NOT NULL, timeout_ms INTEGER NOT NULL);
		INSERT INTO jobs SELECT job_id, state, attempt_id, worker_id, task_type, payload, timeout_ms FROM current_jobs;
		DROP TABLE current_jobs`); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	s := &CoordinatorServer{jobStore: reopened}
	r.HasWorkerResult = false
	got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: r.JobID})
	if err != nil {
		t.Fatal(err)
	}
	assertGetJobSnapshot(t, got, r, runtimepb.JobState_JOB_STATE_SUCCEEDED)
	// Persist a legitimate empty accepted result, then instantiate a fresh store
	// and Coordinator. Presence must survive both boundaries independently.
	r.HasWorkerResult = true
	if err := reopened.Save(r); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
	final, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer final.Close()
	got, err = (&CoordinatorServer{jobStore: final}).GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: r.JobID})
	if err != nil {
		t.Fatal(err)
	}
	assertGetJobSnapshot(t, got, r, runtimepb.JobState_JOB_STATE_SUCCEEDED)
	if err := final.Close(); err != nil {
		t.Fatal(err)
	}
	if response, err := (&CoordinatorServer{jobStore: final}).GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: r.JobID}); response != nil || status.Code(err) != codes.Internal || status.Convert(err).Message() != "failed to read job" {
		t.Fatalf("closed store=%v, %v", response, err)
	}
}
