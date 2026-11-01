package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func queuedPublicationRecord() JobRecord {
	return JobRecord{JobID: 800, State: JobQueued, TaskType: "hash", Payload: "input", TimeoutMs: 1000}
}

func TestJobStoreQueuedPublication(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			for i, spec := range []struct{ task, payload string }{
				{"hash", "input"}, {"sleep", "1ms"},
				{"zk_preimage_prove", ` { "digest":"+001", "witness_ref":"ref" } `},
				{"", ""}, // Legacy/default specs are not newly validated by storage.
			} {
				t.Run(fmt.Sprint(i), func(t *testing.T) {
					store := newCreationTestStore(t, backend)
					queued := queuedPublicationRecord()
					queued.JobID += int64(i)
					queued.TaskType, queued.Payload = spec.task, spec.payload
					if created, err := store.Create(queued); !created || err != nil {
						t.Fatalf("Create=%v, %v", created, err)
					}
					published, err := store.TryStartQueued(queued, "selected-worker")
					if !published || err != nil {
						t.Fatalf("publication=%v, %v", published, err)
					}
					want := queued
					want.State, want.AttemptID, want.WorkerID = JobRunning, 1, "selected-worker"
					assertRecoveryRecord(t, store, want)
					assertListedRecords(t, store, JobRunning, want)
					assertListedRecords(t, store, JobQueued)
					if published, err := store.TryStartQueued(queued, "second-worker"); published || err != nil {
						t.Fatalf("repeated publication=%v, %v", published, err)
					}
					assertRecoveryRecord(t, store, want)
					if queued.State != JobQueued || queued.AttemptID != 0 || queued.WorkerID != "" {
						t.Fatal("publication mutated caller's expected record")
					}
				})
			}
		})
	}
}

func TestJobStoreQueuedPublicationConflicts(t *testing.T) {
	changes := []struct {
		name string
		edit func(*JobRecord)
	}{
		{"running", func(r *JobRecord) { r.State, r.AttemptID, r.WorkerID = JobRunning, 1, "winner" }},
		{"cancelled", func(r *JobRecord) { r.State = JobCancelled }},
		{"succeeded_empty_result", func(r *JobRecord) { r.State, r.HasWorkerResult = JobSucceeded, true }},
		{"failed_result", func(r *JobRecord) { r.State, r.HasWorkerResult, r.Error = JobFailed, true, "failed" }},
		{"recovering", func(r *JobRecord) { r.State = JobRecovering }},
		{"unknown_state", func(r *JobRecord) { r.State = JobState(99) }},
		{"queued_attempt", func(r *JobRecord) { r.AttemptID = 1 }},
		{"queued_negative_attempt", func(r *JobRecord) { r.AttemptID = -1 }},
		{"queued_worker", func(r *JobRecord) { r.WorkerID = "assigned" }},
		{"queued_empty_result", func(r *JobRecord) { r.HasWorkerResult = true }},
		{"queued_output", func(r *JobRecord) { r.Output = "residual" }},
		{"queued_error", func(r *JobRecord) { r.Error = "residual" }},
		{"task_case", func(r *JobRecord) { r.TaskType = "HASH" }},
		{"payload_case", func(r *JobRecord) { r.Payload = "INPUT" }},
		{"payload_space", func(r *JobRecord) { r.Payload += " " }},
		{"payload_nul", func(r *JobRecord) { r.Payload += "\x00suffix" }},
		{"timeout", func(r *JobRecord) { r.TimeoutMs++ }},
	}
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newCreationTestStore(t, backend)
			for i, change := range changes {
				t.Run(change.name, func(t *testing.T) {
					expected := queuedPublicationRecord()
					expected.JobID += int64(i)
					stored := expected
					change.edit(&stored)
					if created, err := store.Create(stored); !created || err != nil {
						t.Fatalf("Create=%v, %v", created, err)
					}
					if published, err := store.TryStartQueued(expected, "worker"); published || err != nil {
						t.Fatalf("conflicting publication=%v, %v", published, err)
					}
					assertRecoveryRecord(t, store, stored)
				})
			}
			expected := queuedPublicationRecord()
			expected.JobID = 999
			if published, err := store.TryStartQueued(expected, "worker"); published || err != nil {
				t.Fatalf("missing record publication=%v, %v", published, err)
			}
			if _, found, err := store.Load(expected.JobID); found || err != nil {
				t.Fatalf("publication inserted missing record: found=%v err=%v", found, err)
			}
		})
	}
}

func TestJobStoreQueuedPublicationInvalidInput(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newCreationTestStore(t, backend)
			queued := queuedPublicationRecord()
			saveRecoveryRecord(t, store, queued)
			for _, field := range []string{"state", "attempt", "worker", "presence", "output", "error", "selected_worker"} {
				t.Run(field, func(t *testing.T) {
					expected, worker := queued, "selected"
					switch field {
					case "state":
						expected.State = JobRunning
					case "attempt":
						expected.AttemptID = 1
					case "worker":
						expected.WorkerID = "assigned"
					case "presence":
						expected.HasWorkerResult = true
					case "output":
						expected.Output = "residual"
					case "error":
						expected.Error = "residual"
					case "selected_worker":
						worker = ""
					}
					if published, err := store.TryStartQueued(expected, worker); published || !errors.Is(err, ErrInvalidQueuedPublication) {
						t.Fatalf("invalid publication=%v, %v", published, err)
					}
					assertRecoveryRecord(t, store, queued)
				})
			}
		})
	}
}

func TestJobStoreQueuedPublicationLegacySpec(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newCreationTestStore(t, backend)
			queued := queuedPublicationRecord()
			queued.TaskType = "zk_preimage_prove"
			queued.Payload = ` { "digest":"+001", "witness_ref":"ref" } `
			// Use the existing validator to establish semantic equivalence, while
			// requiring the original durable bytes at the publication boundary.
			canonical, err := validateJobSpec(JobSpec{JobID: queued.JobID, TaskType: queued.TaskType, Payload: queued.Payload, TimeoutMs: queued.TimeoutMs})
			if err != nil || canonical.Payload == queued.Payload {
				t.Fatalf("noncanonical fixture: canonical=%+v err=%v", canonical, err)
			}
			saveRecoveryRecord(t, store, queued)
			stale := queued
			stale.Payload = canonical.Payload
			if published, err := store.TryStartQueued(stale, "worker"); published || err != nil {
				t.Fatalf("normalized spec publication=%v, %v", published, err)
			}
			assertRecoveryRecord(t, store, queued)
			if published, err := store.TryStartQueued(queued, "worker"); !published || err != nil {
				t.Fatalf("original spec publication=%v, %v", published, err)
			}
			queued.State, queued.AttemptID, queued.WorkerID = JobRunning, 1, "worker"
			assertRecoveryRecord(t, store, queued)
		})
	}
}

// Only contention fixtures wait for SQLite's write lock so that losing callers
// reach the predicate and return confirmed conflicts. Production connection
// settings are unchanged; TestSQLiteQueuedPublicationBusy covers default BUSY.
func queuedContentionStores(t *testing.T, backend string) []JobStore {
	t.Helper()
	if backend == "memory" {
		return []JobStore{NewMemoryJobStore()}
	}
	count := 1
	if backend == "sqlite separate stores" {
		count = 4
	}
	path := filepath.Join(t.TempDir(), "jobs.db") + "?_pragma=busy_timeout(5000)"
	var stores []JobStore
	for i := 0; i < count; i++ {
		store, err := NewSQLiteJobStore(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		stores = append(stores, store)
	}
	return stores
}

func TestJobStoreQueuedPublicationCompetingPublishers(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "sqlite separate stores"} {
		t.Run(backend, func(t *testing.T) {
			stores := queuedContentionStores(t, backend)
			queued := queuedPublicationRecord()
			saveRecoveryRecord(t, stores[0], queued)
			const contenders = 16
			type outcome struct {
				worker    string
				published bool
				err       error
			}
			start, results := make(chan struct{}), make(chan outcome, contenders)
			var ready sync.WaitGroup
			ready.Add(contenders)
			for i := 0; i < contenders; i++ {
				go func(store JobStore, worker string) {
					ready.Done()
					<-start
					published, err := store.TryStartQueued(queued, worker)
					results <- outcome{worker, published, err}
				}(stores[i%len(stores)], fmt.Sprintf("worker-%d", i))
			}
			ready.Wait()
			close(start)
			wins, winner := 0, ""
			for i := 0; i < contenders; i++ {
				result := <-results
				if result.err != nil {
					t.Errorf("publication error: %v", result.err)
				}
				if result.published {
					wins++
					winner = result.worker
				}
			}
			if wins != 1 {
				t.Fatalf("confirmed publishers=%d, want one", wins)
			}
			queued.State, queued.AttemptID, queued.WorkerID = JobRunning, 1, winner
			for _, store := range stores {
				assertRecoveryRecord(t, store, queued)
			}
			t.Logf("confirmed publishers=1, conflicts=%d, winner=%s", contenders-1, winner)
		})
	}
}

func TestSQLiteQueuedPublicationReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	queued := queuedPublicationRecord()
	queued.TaskType, queued.Payload = "zk_preimage_prove", ` { "digest":"+001", "witness_ref":"ref" } `
	if created, err := store.Create(queued); !created || err != nil {
		t.Fatalf("Create=%v, %v", created, err)
	}
	if published, err := store.TryStartQueued(queued, "durable-worker"); !published || err != nil {
		t.Fatalf("publication=%v, %v", published, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	want := queued
	want.State, want.AttemptID, want.WorkerID = JobRunning, 1, "durable-worker"
	assertRecoveryRecord(t, reopened, want)
	if published, err := reopened.TryStartQueued(queued, "other"); published || err != nil {
		t.Fatalf("reopened publication=%v, %v", published, err)
	}
	if created, err := reopened.Create(queued); created || err != nil {
		t.Fatalf("reopened Create=%v, %v", created, err)
	}
	assertRecoveryRecord(t, reopened, want)
}

func TestJobStoreQueuedPublicationUncertainAcknowledgement(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%v", backend, commit), func(t *testing.T) {
				inner := newCreationTestStore(t, backend)
				queued := queuedPublicationRecord()
				saveRecoveryRecord(t, inner, queued)
				injected, calls := errors.New("publication acknowledgement lost"), 0
				store := &recoveryPassStore{JobStore: inner, startQueued: func(expected JobRecord, worker string) (bool, error) {
					calls++
					if commit {
						if published, err := inner.TryStartQueued(expected, worker); !published || err != nil {
							t.Fatalf("underlying publication=%v, %v", published, err)
						}
						want := expected
						want.State, want.AttemptID, want.WorkerID = JobRunning, 1, worker
						assertRecoveryRecord(t, inner, want)
					}
					return false, injected
				}}
				if published, err := store.TryStartQueued(queued, "worker"); published || !errors.Is(err, injected) || calls != 1 {
					t.Fatalf("uncertain publication=%v, %v, calls=%d", published, err, calls)
				}
				// The same returned error permits both durable outcomes; reconcile
				// by reading, not by assuming the write rolled back.
				want := queued
				if commit {
					want.State, want.AttemptID, want.WorkerID = JobRunning, 1, "worker"
					if published, err := inner.TryStartQueued(queued, "other"); published || err != nil {
						t.Fatalf("repeat after lost acknowledgement=%v, %v", published, err)
					}
				}
				assertRecoveryRecord(t, store, want)
			})
		}
	}
}

func TestSQLiteQueuedPublicationErrors(t *testing.T) {
	store := newCreationTestStore(t, "sqlite").(*SQLiteJobStore)
	queued := queuedPublicationRecord()
	saveRecoveryRecord(t, store, queued)
	if _, err := store.db.Exec(`CREATE TRIGGER reject_publication AFTER UPDATE ON jobs
		BEGIN SELECT RAISE(ABORT, 'injected publication rollback'); END;`); err != nil {
		t.Fatal(err)
	}
	if published, err := store.TryStartQueued(queued, "worker"); published || err == nil || !strings.Contains(err.Error(), "injected publication rollback") {
		t.Fatalf("aborted publication=%v, %v", published, err)
	}
	// This injected ABORT establishes rollback; arbitrary errors do not.
	assertRecoveryRecord(t, store, queued)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if published, err := store.TryStartQueued(queued, "worker"); published || err == nil {
		t.Fatalf("closed publication=%v, %v", published, err)
	}
}

func TestSQLiteQueuedPublicationBusy(t *testing.T) {
	store := newCreationTestStore(t, "sqlite").(*SQLiteJobStore)
	queued := queuedPublicationRecord()
	saveRecoveryRecord(t, store, queued)
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE jobs SET worker_id = 'uncommitted' WHERE job_id = ?`, queued.JobID); err != nil {
		t.Fatal(err)
	}
	if published, err := store.TryStartQueued(queued, "worker"); published || !sqliteCreationBusy(err) {
		t.Fatalf("locked publication=%v, %v", published, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, store, queued)
}

func TestJobStoreQueuedPublicationRacesWithSave(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite separate stores"} {
		for _, order := range []string{"cancel_first", "publish_first", "simultaneous"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				stores := queuedContentionStores(t, backend)
				queued := queuedPublicationRecord()
				saveRecoveryRecord(t, stores[0], queued)
				cancelled := queued
				cancelled.State = JobCancelled
				start, saved, publishedDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
				type publication struct {
					ok  bool
					err error
				}
				result := make(chan publication, 1)
				saveResult := make(chan error, 1)
				go func() {
					<-start
					if order == "cancel_first" {
						<-saved
					}
					ok, err := stores[0].TryStartQueued(queued, "worker")
					result <- publication{ok, err}
					close(publishedDone)
				}()
				go func() {
					<-start
					if order == "publish_first" {
						<-publishedDone
					}
					saveResult <- stores[len(stores)-1].Save(cancelled)
					close(saved)
				}()
				close(start)
				outcome, saveErr := <-result, <-saveResult
				if outcome.err != nil || saveErr != nil {
					t.Fatalf("publication error=%v, Save error=%v", outcome.err, saveErr)
				}
				if order == "cancel_first" && outcome.ok || order == "publish_first" && !outcome.ok {
					t.Fatalf("order=%s publication=%v", order, outcome.ok)
				}
				// Save is still unconditional on state: when it commits after a
				// successful publication it overwrites Running(1) with Cancelled(0).
				// This is a blocker for reusing Save for future live Queued cancellation,
				// not a guarantee that all writers now obey the publication predicate.
				for _, store := range stores {
					assertRecoveryRecord(t, store, cancelled)
				}
				t.Logf("order=%s publication=%v; final state Cancelled(0)", order, outcome.ok)
			})
		}
	}
}
