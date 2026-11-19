package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestJobStoreSaveImmutableSpec(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, legacyDefaults := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/defaults=%v", backend, legacyDefaults), func(t *testing.T) {
				store := newCreationTestStore(t, backend)
				original := creationTestRecord()
				if legacyDefaults {
					original.TaskType, original.Payload, original.TimeoutMs = "", "", 0
				}
				// Save must still insert an absent JobID, including its full result.
				if err := store.Save(original); err != nil {
					t.Fatal(err)
				}
				for _, change := range []struct {
					name  string
					apply func(*JobRecord)
				}{
					{"TaskType", func(r *JobRecord) { r.TaskType += "different" }},
					{"Payload", func(r *JobRecord) { r.Payload += "different" }},
					{"TimeoutMs", func(r *JobRecord) { r.TimeoutMs++ }},
					{"multiple", func(r *JobRecord) { r.TaskType, r.Payload, r.TimeoutMs = "sleep", "1h", 9999 }},
				} {
					t.Run(change.name, func(t *testing.T) {
						incoming := original
						incoming.State, incoming.AttemptID, incoming.WorkerID = JobRunning, 99, "other"
						incoming.HasWorkerResult, incoming.Output, incoming.Error = false, "", ""
						change.apply(&incoming)
						if err := store.Save(incoming); !errors.Is(err, ErrJobSpecConflict) {
							t.Fatalf("Save error=%v, want specification conflict", err)
						}
						assertRecoveryRecord(t, store, original)
						assertListedRecords(t, store, original.State, original)
					})
				}
				// Equal-spec updates may replace every mutable field, including
				// clearing a terminal result for another execution generation.
				updated := original
				updated.State, updated.AttemptID, updated.WorkerID = JobRunning, 8, "new-worker"
				updated.HasWorkerResult, updated.Output, updated.Error = false, "", ""
				if err := store.Save(updated); err != nil {
					t.Fatal(err)
				}
				assertRecoveryRecord(t, store, updated)
				updated.State = JobSucceeded
				updated = withWorkerResult(updated, "new output", "new error")
				for i := 0; i < 2; i++ { // An identical no-op Save must also succeed.
					if err := store.Save(updated); err != nil {
						t.Fatal(err)
					}
					assertRecoveryRecord(t, store, updated)
					assertListedRecords(t, store, updated.State, updated)
				}
			})
		}
	}
}

func TestJobStoreSaveDoesNotNormalizeSpec(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newCreationTestStore(t, backend)
			original := creationTestRecord()
			original.TaskType = "hash"
			original.Payload = `  original input  `
			if err := store.Save(original); err != nil {
				t.Fatal(err)
			}
			incoming := original
			incoming.Payload = `original input`
			if err := store.Save(incoming); !errors.Is(err, ErrJobSpecConflict) {
				t.Fatalf("normalization changed stored spec: %v", err)
			}
			incoming = original
			incoming.TaskType = "HASH"
			if err := store.Save(incoming); !errors.Is(err, ErrJobSpecConflict) {
				t.Fatalf("case folding changed stored spec: %v", err)
			}
			assertRecoveryRecord(t, store, original)
		})
	}
}

func TestJobStoreConcurrentSaveImmutableSpec(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite separate stores"} {
		for _, established := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/established=%v", backend, established), func(t *testing.T) {
				var stores []JobStore
				if backend == "memory" {
					stores = []JobStore{NewMemoryJobStore()}
				} else {
					path := filepath.Join(t.TempDir(), "jobs.db")
					for i := 0; i < 4; i++ {
						store, err := NewSQLiteJobStore(path)
						if err != nil {
							t.Fatal(err)
						}
						store.db.SetMaxOpenConns(1)
						t.Cleanup(func() { _ = store.Close() })
						stores = append(stores, store)
					}
				}
				winner := creationTestRecord()
				if established {
					if err := stores[0].Save(winner); err != nil {
						t.Fatal(err)
					}
				}
				type outcome struct {
					record JobRecord
					err    error
				}
				const contenders = 16
				results, start := make(chan outcome, contenders), make(chan struct{})
				var ready sync.WaitGroup
				ready.Add(contenders)
				for i := 0; i < contenders; i++ {
					incoming := creationTestRecord()
					incoming.Payload = fmt.Sprintf("competing input %d", i)
					incoming.AttemptID, incoming.WorkerID = int64(i+1), fmt.Sprintf("worker-%d", i)
					incoming.Output, incoming.Error = fmt.Sprintf("output-%d", i), fmt.Sprintf("error-%d", i)
					go func(store JobStore, r JobRecord) {
						ready.Done()
						<-start
						results <- outcome{r, store.Save(r)}
					}(stores[i%len(stores)], incoming)
				}
				ready.Wait()
				close(start)
				successes, conflicts, busy := 0, 0, 0
				for i := 0; i < contenders; i++ {
					r := <-results
					switch {
					case r.err == nil:
						successes++
						winner = r.record
					case errors.Is(r.err, ErrJobSpecConflict):
						conflicts++
					case backend != "memory" && sqliteCreationBusy(r.err):
						busy++ // Contention is an I/O outcome, not a spec conflict. No retry.
					default:
						t.Errorf("unexpected Save error: %v", r.err)
					}
				}
				wantSuccesses := 1
				if established {
					wantSuccesses = 0
				}
				if successes != wantSuccesses {
					t.Fatalf("successful writes=%d, want %d", successes, wantSuccesses)
				}
				for _, store := range stores {
					assertRecoveryRecord(t, store, winner)
					assertListedRecords(t, store, winner.State, winner)
				}
				t.Logf("successful writes=%d conflicts=%d contention errors=%d", successes, conflicts, busy)
			})
		}
	}
}

func TestSQLiteSaveIOErrorsAreNotSpecConflicts(t *testing.T) {
	store := newCreationTestStore(t, "sqlite").(*SQLiteJobStore)
	original := creationTestRecord()
	if err := store.Save(original); err != nil {
		t.Fatal(err)
	}
	tx, err := store.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE jobs SET worker_id = 'locked' WHERE job_id = ?`, original.JobID); err != nil {
		t.Fatal(err)
	}
	incoming := original
	incoming.Payload = "conflicting input"
	if err := store.Save(incoming); !sqliteCreationBusy(err) || errors.Is(err, ErrJobSpecConflict) {
		t.Fatalf("locked Save must report contention, not conflict: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, store, original)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(incoming); err == nil || errors.Is(err, ErrJobSpecConflict) {
		t.Fatalf("closed Save must report storage failure, not conflict: %v", err)
	}
}
