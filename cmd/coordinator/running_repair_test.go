package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func runningRepairRecord() JobRecord {
	return JobRecord{JobID: 9950, State: JobRunning, AttemptID: 2, WorkerID: "selected",
		TaskType: "hash", Payload: `  original input  `, TimeoutMs: 1000}
}

func TestJobStoreRunningRepair(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			store := newCreationTestStore(t, backend)
			expected := runningRepairRecord()
			if repaired, err := store.TryRecoverRunning(expected); repaired || err != nil {
				t.Fatalf("missing repair=%v, %v", repaired, err)
			}
			if _, exists, err := store.Load(expected.JobID); exists || err != nil {
				t.Fatalf("repair inserted row: %v, %v", exists, err)
			}
			for i, change := range []struct {
				name string
				edit func(*JobRecord)
			}{
				{"same", func(*JobRecord) {}},
				{"terminal", func(r *JobRecord) { r.State, r.HasWorkerResult = JobSucceeded, true }},
				{"cancelled", func(r *JobRecord) { r.State = JobCancelled }},
				{"recovering", func(r *JobRecord) { r.State = JobRecovering }},
				{"generation", func(r *JobRecord) { r.AttemptID++ }},
				{"worker", func(r *JobRecord) { r.WorkerID = "other" }},
				{"task", func(r *JobRecord) { r.TaskType = "HASH" }},
				{"payload", func(r *JobRecord) { r.Payload += " " }},
				{"timeout", func(r *JobRecord) { r.TimeoutMs++ }},
				{"presence", func(r *JobRecord) { r.HasWorkerResult = true }},
				{"output", func(r *JobRecord) { r.Output = "residual" }},
				{"error", func(r *JobRecord) { r.Error = "residual" }},
			} {
				t.Run(change.name, func(t *testing.T) {
					want, candidate := expected, expected
					want.JobID += int64(i)
					candidate.JobID = want.JobID
					change.edit(&want)
					if ok, err := store.Create(want); !ok || err != nil {
						t.Fatalf("Create=%v, %v", ok, err)
					}
					repaired, err := store.TryRecoverRunning(candidate)
					if err != nil || repaired != (change.name == "same") {
						t.Fatalf("repair=%v, %v", repaired, err)
					}
					if repaired {
						want.State = JobRecovering
					}
					assertRecoveryRecord(t, store, want)
					if again, err := store.TryRecoverRunning(candidate); again || err != nil {
						t.Fatalf("repeated repair=%v, %v", again, err)
					}
					assertRecoveryRecord(t, store, want)
				})
			}
			for _, field := range []string{"state", "generation", "worker", "presence", "output", "error"} {
				r := expected
				switch field {
				case "state":
					r.State = JobQueued
				case "generation":
					r.AttemptID = 0
				case "worker":
					r.WorkerID = ""
				case "presence":
					r.HasWorkerResult = true
				case "output":
					r.Output = "residual"
				case "error":
					r.Error = "residual"
				}
				if repaired, err := store.TryRecoverRunning(r); repaired || !errors.Is(err, ErrInvalidRunningRepair) {
					t.Fatalf("invalid %s: repair=%v, %v", field, repaired, err)
				}
			}
		})
	}
}

func TestJobStoreRunningRepairCompetingWriters(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite", "sqlite separate stores"} {
		t.Run(backend, func(t *testing.T) {
			stores := queuedContentionStores(t, backend)
			expected := runningRepairRecord()
			saveRecoveryRecord(t, stores[0], expected)
			start := make(chan struct{})
			type result struct {
				repaired bool
				err      error
			}
			results := make(chan result, 8)
			var ready sync.WaitGroup
			ready.Add(8)
			for i := 0; i < 8; i++ {
				go func(s JobStore) {
					ready.Done()
					<-start
					ok, err := s.TryRecoverRunning(expected)
					results <- result{ok, err}
				}(stores[i%len(stores)])
			}
			ready.Wait()
			close(start)
			wins := 0
			for i := 0; i < 8; i++ {
				r := <-results
				if r.err != nil {
					t.Errorf("repair: %v", r.err)
				}
				if r.repaired {
					wins++
				}
			}
			if wins != 1 {
				t.Fatalf("repair winners=%d", wins)
			}
			expected.State = JobRecovering
			for _, s := range stores {
				assertRecoveryRecord(t, s, expected)
			}
		})
	}
}

func TestSQLiteRunningRepairErrorsAndReopen(t *testing.T) {
	for _, scenario := range []string{"rollback", "busy", "reopen"} {
		t.Run(scenario, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			store, err := NewSQLiteJobStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			expected := runningRepairRecord()
			saveRecoveryRecord(t, store, expected)
			switch scenario {
			case "rollback":
				if _, err := store.db.Exec(`CREATE TRIGGER reject_repair AFTER UPDATE ON jobs BEGIN SELECT RAISE(ABORT, 'repair rollback'); END;`); err != nil {
					t.Fatal(err)
				}
				if ok, err := store.TryRecoverRunning(expected); ok || err == nil {
					t.Fatalf("aborted repair=%v, %v", ok, err)
				}
				assertRecoveryRecord(t, store, expected)
			case "busy":
				tx, err := store.db.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec(`UPDATE jobs SET worker_id = 'uncommitted' WHERE job_id = ?`, expected.JobID); err != nil {
					t.Fatal(err)
				}
				if ok, err := store.TryRecoverRunning(expected); ok || !sqliteCreationBusy(err) {
					t.Fatalf("locked repair=%v, %v", ok, err)
				}
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				assertRecoveryRecord(t, store, expected)
			case "reopen":
				if ok, err := store.TryRecoverRunning(expected); !ok || err != nil {
					t.Fatalf("repair=%v, %v", ok, err)
				}
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
				reopened, err := NewSQLiteJobStore(path)
				if err != nil {
					t.Fatal(err)
				}
				defer reopened.Close()
				expected.State = JobRecovering
				assertRecoveryRecord(t, reopened, expected)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if ok, err := store.TryRecoverRunning(runningRepairRecord()); ok || err == nil {
				t.Fatal(fmt.Sprintf("closed repair=%v, %v", ok, err))
			}
		})
	}
}
