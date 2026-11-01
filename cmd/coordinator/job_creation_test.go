package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

func newCreationTestStore(t *testing.T, backend string) JobStore {
	t.Helper()
	if backend == "memory" {
		return NewMemoryJobStore()
	}
	store, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func creationTestRecord() JobRecord {
	return JobRecord{JobID: 700, State: JobFailed, AttemptID: 7, WorkerID: "original-worker",
		TaskType: "hash", Payload: "original input", TimeoutMs: 1234,
		HasWorkerResult: true, Output: "retained output", Error: "retained error"}
}

func TestJobStoreCreatePreservesCompleteRecord(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, state := range []JobState{JobRunning, JobSucceeded, JobFailed, JobCancelled} {
			t.Run(fmt.Sprintf("%s/state=%d", backend, state), func(t *testing.T) {
				store := newCreationTestStore(t, backend)
				original := creationTestRecord()
				original.State = state
				if state == JobRunning {
					original.HasWorkerResult, original.Output, original.Error = false, "", ""
				}
				created, err := store.Create(original)
				if !created || err != nil {
					t.Fatalf("first Create: created=%v error=%v", created, err)
				}
				assertRecoveryRecord(t, store, original)
				assertListedRecords(t, store, state, original)
				// Change every non-key field, including terminal result presence.
				different := JobRecord{JobID: original.JobID, State: JobRecovering, AttemptID: 99,
					WorkerID: "other", TaskType: "sleep", Payload: "1h", TimeoutMs: 9999}
				for _, duplicate := range []JobRecord{original, different} {
					created, err := store.Create(duplicate)
					if created || err != nil {
						t.Fatalf("duplicate Create: created=%v error=%v", created, err)
					}
					assertRecoveryRecord(t, store, original)
					assertListedRecords(t, store, state, original)
				}
				other := different
				other.JobID++
				if created, err := store.Create(other); !created || err != nil {
					t.Fatalf("independent JobID: created=%v error=%v", created, err)
				}
				assertRecoveryRecord(t, store, other)
				assertRecoveryRecord(t, store, original)
				// Create is still a no-op on any duplicate, while Save now rejects
				// a different spec and permits only matching-spec mutable updates.
				if err := store.Save(different); !errors.Is(err, ErrJobSpecConflict) {
					t.Fatalf("conflicting Save: %v", err)
				}
				assertRecoveryRecord(t, store, original)
				updated := different
				updated.TaskType, updated.Payload, updated.TimeoutMs = original.TaskType, original.Payload, original.TimeoutMs
				if err := store.Save(updated); err != nil {
					t.Fatal(err)
				}
				assertRecoveryRecord(t, store, updated)
				if created, err := store.Create(original); created || err != nil {
					t.Fatalf("duplicate after Save: created=%v error=%v", created, err)
				}
				assertRecoveryRecord(t, store, updated)
			})
		}
	}
}

func sqliteCreationBusy(err error) bool {
	var sqliteErr *sqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	code := sqliteErr.Code() & 0xff // Include extended BUSY/LOCKED codes.
	return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
}

func TestJobStoreConcurrentCreate(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "sqlite separate stores"} {
		t.Run(backend, func(t *testing.T) {
			const contenders = 16
			var stores []JobStore
			path := filepath.Join(t.TempDir(), "jobs.db")
			if backend == "memory" {
				stores = []JobStore{NewMemoryJobStore()}
			} else {
				count := 1
				if backend == "sqlite separate stores" {
					count = 4
				}
				// Initialize sequentially: concurrent migration is not the contract.
				for i := 0; i < count; i++ {
					store, err := NewSQLiteJobStore(path)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = store.Close() })
					// One connection per instance. The separate-store case still
					// exercises SQLite arbitration across independent connections.
					store.db.SetMaxOpenConns(1)
					stores = append(stores, store)
				}
			}
			type outcome struct {
				record  JobRecord
				created bool
				err     error
			}
			start := make(chan struct{})
			results := make(chan outcome, contenders)
			var ready sync.WaitGroup
			ready.Add(contenders)
			for i := 0; i < contenders; i++ {
				record := creationTestRecord()
				record.AttemptID = int64(i + 1)
				record.WorkerID, record.Payload = fmt.Sprintf("worker-%d", i), fmt.Sprintf("input-%d", i)
				record.Output, record.Error = fmt.Sprintf("output-%d", i), fmt.Sprintf("error-%d", i)
				go func(store JobStore, r JobRecord) {
					ready.Done()
					<-start
					created, err := store.Create(r)
					results <- outcome{r, created, err}
				}(stores[i%len(stores)], record)
			}
			ready.Wait()
			close(start)
			var winner JobRecord
			wins, busy := 0, 0
			for i := 0; i < contenders; i++ {
				result := <-results
				if result.err != nil {
					if result.created || backend != "sqlite separate stores" || !sqliteCreationBusy(result.err) {
						t.Errorf("unexpected Create error: created=%v error=%v", result.created, result.err)
					}
					busy++
					continue // No automatic retries of failed or uncertain operations.
				}
				if result.created {
					wins++
					winner = result.record
				}
			}
			if wins != 1 {
				t.Fatalf("successful creators=%d, want exactly one (busy=%d)", wins, busy)
			}
			for _, store := range stores {
				assertRecoveryRecord(t, store, winner)
				assertListedRecords(t, store, winner.State, winner)
			}
			t.Logf("creators=%d duplicates=%d contention errors=%d", wins, contenders-wins-busy, busy)
			if backend != "memory" {
				for _, store := range stores {
					if err := store.(*SQLiteJobStore).Close(); err != nil {
						t.Fatal(err)
					}
				}
				reopened, err := NewSQLiteJobStore(path)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				assertSQLiteJobRecord(t, reopened, winner)
			}
		})
	}
}

func TestSQLiteCreateInsertionErrors(t *testing.T) {
	store := newCreationTestStore(t, "sqlite").(*SQLiteJobStore)
	if _, err := store.db.Exec(`CREATE TRIGGER reject_create AFTER INSERT ON jobs
		BEGIN SELECT RAISE(ABORT, 'injected insertion rollback'); END;`); err != nil {
		t.Fatal(err)
	}
	record := creationTestRecord()
	created, err := store.Create(record)
	if created || err == nil || !strings.Contains(err.Error(), "injected insertion rollback") {
		t.Fatalf("aborted Create: created=%v error=%v", created, err)
	}
	// The injected ABORT establishes rollback in this test; arbitrary errors do not.
	if _, found, err := store.Load(record.JobID); found || err != nil {
		t.Fatalf("aborted insertion: found=%v error=%v", found, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if created, err := store.Create(record); created || err == nil {
		t.Fatalf("closed Create: created=%v error=%v", created, err)
	}
}

func TestSQLiteCreateReportsBusy(t *testing.T) {
	store := newCreationTestStore(t, "sqlite").(*SQLiteJobStore)
	// Hold a real write lock on another connection before starting Create.
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`INSERT INTO jobs (job_id, state, attempt_id, worker_id) VALUES (1, 1, 1, 'locker')`); err != nil {
		t.Fatal(err)
	}
	record := creationTestRecord()
	created, err := store.Create(record)
	if created || !sqliteCreationBusy(err) {
		t.Fatalf("locked Create: created=%v error=%v", created, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.Load(record.JobID); found || err != nil {
		t.Fatalf("locked insertion: found=%v error=%v", found, err)
	}
}

func TestJobStoreCreateUncertainAcknowledgement(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/commit=%v", backend, commit), func(t *testing.T) {
				inner := newCreationTestStore(t, backend)
				injected := errors.New("creation acknowledgement unavailable")
				calls := 0
				store := &recoveryPassStore{JobStore: inner, create: func(r JobRecord) (bool, error) {
					calls++
					if commit {
						if created, err := inner.Create(r); !created || err != nil {
							t.Fatalf("backing Create: created=%v error=%v", created, err)
						}
					}
					return false, injected
				}}
				record := creationTestRecord()
				created, err := store.Create(record)
				if created || !errors.Is(err, injected) || calls != 1 {
					t.Fatalf("created=%v error=%v calls=%d", created, err, calls)
				}
				// The identical returned error permits either durable outcome.
				got, found, err := inner.Load(record.JobID)
				if err != nil || found != commit || (found && got != record) {
					t.Fatalf("reconciliation read: record=%+v found=%v error=%v", got, found, err)
				}
			})
		}
	}
}
