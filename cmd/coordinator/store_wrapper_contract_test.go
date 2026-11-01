package main

import (
	"errors"
	"reflect"
	"testing"
)

func TestStoreWrappersForwardAllMethods(t *testing.T) {
	for _, kind := range []string{"recovery", "shutdown", "worker failure"} {
		t.Run(kind, func(t *testing.T) {
			base := NewMemoryJobStore()
			var store JobStore
			var shutdown *shutdownStore
			var worker *workerFailureStore
			switch kind {
			case "recovery":
				store = &recoveryPassStore{JobStore: base}
			case "shutdown":
				shutdown = &shutdownStore{JobStore: base}
				store = shutdown
			case "worker failure":
				worker = &workerFailureStore{JobStore: base}
				store = worker
			}
			field, ok := reflect.TypeOf(store).Elem().FieldByName("JobStore")
			if !ok || field.Anonymous {
				t.Fatal("underlying JobStore must be a named field, not an embedded interface")
			}
			queued := JobRecord{JobID: 1, State: JobQueued, TaskType: "hash", Payload: "input", TimeoutMs: 1000}
			if created, err := store.Create(queued); !created || err != nil {
				t.Fatalf("Create: %v, %v", created, err)
			}
			if got, found, err := store.Load(queued.JobID); err != nil || !found || got != queued {
				t.Fatalf("Load: %+v, %v, %v", got, found, err)
			}
			if got, err := store.ListByState(JobQueued); err != nil || !reflect.DeepEqual(got, []JobRecord{queued}) {
				t.Fatalf("ListByState: %+v, %v", got, err)
			}
			wrongSpec := queued
			wrongSpec.Payload = "different"
			if published, err := store.TryStartQueued(wrongSpec, "worker"); published || err != nil {
				t.Fatalf("conflicting publication: %v, %v", published, err)
			}
			if published, err := store.TryStartQueued(queued, "worker"); !published || err != nil {
				t.Fatalf("publication: %v, %v", published, err)
			}
			running := queued
			running.State, running.AttemptID, running.WorkerID = JobRunning, 1, "worker"
			stale := running
			stale.AttemptID++
			if repaired, err := store.TryRecoverRunning(stale); repaired || err != nil {
				t.Fatalf("conflicting repair: %v, %v", repaired, err)
			}
			if repaired, err := store.TryRecoverRunning(running); !repaired || err != nil {
				t.Fatalf("repair: %v, %v", repaired, err)
			}
			recovering := running
			recovering.State = JobRecovering
			terminal := recovering
			terminal.State, terminal.HasWorkerResult, terminal.Output = JobSucceeded, true, "result"
			if err := store.Save(terminal); err != nil {
				t.Fatalf("Save: %v", err)
			}
			if got, found, err := store.Load(queued.JobID); err != nil || !found || got != terminal {
				t.Fatalf("terminal Load: %+v, %v, %v", got, found, err)
			}
			if got, err := store.ListByState(JobSucceeded); err != nil || !reflect.DeepEqual(got, []JobRecord{terminal}) {
				t.Fatalf("terminal ListByState: %+v, %v", got, err)
			}
			if worker != nil && !reflect.DeepEqual(worker.savedRecords(), []JobRecord{queued, running, recovering, terminal}) {
				t.Fatalf("worker mutation history: %+v", worker.savedRecords())
			}
			if shutdown != nil {
				_, afterClose, events := shutdown.snapshot()
				want := []string{"create", "load", "list", "start queued", "start queued", "recover running", "recover running", "save", "load", "list"}
				if afterClose != 0 || !reflect.DeepEqual(events, want) {
					t.Fatalf("shutdown operation history: %+v, after close=%d", events, afterClose)
				}
			}
		})
	}
}

func TestStoreWrappersPreserveFaultCommitBoundary(t *testing.T) {
	for _, kind := range []string{"recovery", "shutdown", "worker failure"} {
		for _, operation := range []string{"create", "save", "start queued", "recover running"} {
			for _, commit := range []bool{false, true} {
				name := kind + "/" + operation + "/before commit"
				if commit {
					name = kind + "/" + operation + "/lost acknowledgement"
				}
				t.Run(name, func(t *testing.T) {
					base := NewMemoryJobStore()
					queued := JobRecord{JobID: 1, State: JobQueued, TaskType: "hash", Payload: "input", TimeoutMs: 1000}
					running := queued
					running.State, running.AttemptID, running.WorkerID = JobRunning, 1, "worker"
					before, after := queued, queued
					switch operation {
					case "save":
						after.State = JobFailed
					case "start queued":
						after = running
					case "recover running":
						before, after = running, running
						after.State = JobRecovering
					}
					if operation != "create" {
						if created, err := base.Create(before); !created || err != nil {
							t.Fatalf("seed: %v, %v", created, err)
						}
					}
					injected := errors.New("injected storage error")
					hits := 0
					fault := &recoveryPassStore{JobStore: base}
					apply := func() error {
						hits++
						if !commit {
							return injected
						}
						var ok bool
						var err error
						switch operation {
						case "create":
							ok, err = base.Create(queued)
						case "save":
							err = base.Save(after)
							ok = err == nil
						case "start queued":
							ok, err = base.TryStartQueued(queued, "worker")
						case "recover running":
							ok, err = base.TryRecoverRunning(running)
						}
						if !ok || err != nil {
							t.Fatalf("backing %s: %v, %v", operation, ok, err)
						}
						return injected
					}
					switch operation {
					case "create":
						fault.create = func(JobRecord) (bool, error) { return false, apply() }
					case "save":
						fault.save = func(JobRecord) error { return apply() }
					case "start queued":
						fault.startQueued = func(JobRecord, string) (bool, error) { return false, apply() }
					case "recover running":
						fault.recoverRunning = func(JobRecord) (bool, error) { return false, apply() }
					}
					var store JobStore = fault
					var worker *workerFailureStore
					if kind == "shutdown" {
						store = &shutdownStore{JobStore: fault}
					} else if kind == "worker failure" {
						worker = &workerFailureStore{JobStore: fault}
						store = worker
					}
					var ok bool
					var err error
					switch operation {
					case "create":
						ok, err = store.Create(queued)
					case "save":
						err = store.Save(after)
					case "start queued":
						ok, err = store.TryStartQueued(queued, "worker")
					case "recover running":
						ok, err = store.TryRecoverRunning(running)
					}
					if ok || !errors.Is(err, injected) || hits != 1 {
						t.Fatalf("%s returned %v, %v with %d hooks", operation, ok, err, hits)
					}
					got, found, readErr := base.Load(queued.JobID)
					want, wantFound := before, operation != "create"
					if commit {
						want, wantFound = after, true
					}
					if readErr != nil || found != wantFound || found && got != want {
						t.Fatalf("durable result: %+v, %v, %v; want %+v, %v", got, found, readErr, want, wantFound)
					}
					if worker != nil && len(worker.savedRecords()) != 0 {
						t.Fatal("unconfirmed write was recorded as successful")
					}
				})
			}
		}
	}
}

func TestStoreWrapperHooksUseActualOperations(t *testing.T) {
	base := NewMemoryJobStore()
	injected := errors.New("injected read error")
	fault := &recoveryPassStore{JobStore: base,
		load: func(int64) (JobRecord, bool, error) { return JobRecord{}, false, injected },
		list: func(JobState) ([]JobRecord, error) { return nil, injected },
	}
	if _, _, err := fault.Load(1); !errors.Is(err, injected) {
		t.Fatalf("Load hook: %v", err)
	}
	if _, err := fault.ListByState(JobQueued); !errors.Is(err, injected) {
		t.Fatalf("ListByState hook: %v", err)
	}
	queued := JobRecord{JobID: 1, State: JobQueued, TaskType: "hash", Payload: "input", TimeoutMs: 1000}
	running := queued
	running.State, running.AttemptID, running.WorkerID = JobRunning, 1, "worker"
	recovering := running
	recovering.State = JobRecovering
	var hooks []JobRecord
	assertBefore := func(want JobRecord, wantFound bool) {
		t.Helper()
		got, found, err := base.Load(queued.JobID)
		if err != nil || found != wantFound || found && got != want {
			t.Fatalf("hook ran after write: %+v, %v, %v", got, found, err)
		}
	}
	store := &shutdownStore{JobStore: base,
		beforeCreate: func(r JobRecord) { assertBefore(JobRecord{}, false); hooks = append(hooks, r) },
		beforeQueued: func(r JobRecord) { assertBefore(queued, true); hooks = append(hooks, r) },
		beforeSave: func(r JobRecord) {
			if len(hooks) == 2 {
				assertBefore(running, true)
			} else {
				assertBefore(recovering, true)
			}
			hooks = append(hooks, r)
		},
	}
	if ok, err := store.Create(queued); !ok || err != nil {
		t.Fatalf("Create: %v, %v", ok, err)
	}
	if ok, err := store.TryStartQueued(queued, "worker"); !ok || err != nil {
		t.Fatalf("TryStartQueued: %v, %v", ok, err)
	}
	if ok, err := store.TryRecoverRunning(running); !ok || err != nil {
		t.Fatalf("TryRecoverRunning: %v, %v", ok, err)
	}
	if err := store.Save(recovering); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// TryRecoverRunning invokes beforeSave with the projected Recovering record.
	if want := []JobRecord{queued, queued, recovering, recovering}; !reflect.DeepEqual(hooks, want) {
		t.Fatalf("hook records: %+v, want %+v", hooks, want)
	}
	_, _, events := store.snapshot()
	if want := []string{"create", "start queued", "recover running", "save"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("actual operations: %v, want %v", events, want)
	}
}
