package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func queuedSessionFixture(t *testing.T, backend string) (*CoordinatorServer, JobStore, JobRecord) {
	t.Helper()
	store := newCreationTestStore(t, backend)
	queued := queuedPublicationRecord()
	queued.JobID = 9901
	if created, err := store.Create(queued); !created || err != nil {
		t.Fatalf("Create Queued=%v, %v", created, err)
	}
	s := &CoordinatorServer{jobStore: store, lifecycle: newCoordinatorLifecycle()}
	t.Cleanup(func() {
		s.lifecycle.beginShutdown()
		s.lifecycle.cancelWork()
		<-s.lifecycle.done
		s.dispatchWG.Wait()
	})
	return s, store, queued
}

func assertQueuedSessionReleased(t *testing.T, s *CoordinatorServer, jobID int64, pending bool) {
	t.Helper()
	s.mu.Lock()
	_, active := s.activeJobs[jobID]
	_, unresolved := s.queuedReconciliations[jobID]
	s.mu.Unlock()
	s.lifecycle.mu.Lock()
	tracked := len(s.lifecycle.active)
	s.lifecycle.mu.Unlock()
	if active || unresolved != pending || tracked != 0 {
		t.Fatalf("ownership leaked: active=%v unresolved=%v tracked=%d", active, unresolved, tracked)
	}
	claimed := s.claimJob(jobID)
	if claimed {
		s.releaseJob(jobID)
	}
	if claimed == pending {
		t.Fatalf("claim=%v with reconciliation pending=%v", claimed, pending)
	}
}

func TestQueuedJobExecution(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, task := range []string{"hash", "sleep", "failed hash"} {
			t.Run(backend+"/"+task, func(t *testing.T) {
				s, inner, original := queuedSessionFixture(t, backend)
				// Give each fixture its final immutable spec before creating it.
				queued := original
				queued.JobID++
				workerStatus, wantState := "Succeeded", JobSucceeded
				if task == "sleep" {
					queued.TaskType, queued.Payload, workerStatus, wantState = "sleep", "1ms", "Cancelled", JobCancelled
				}
				if task == "failed hash" {
					queued.TaskType, queued.Payload = "hash", `  original input  `
					workerStatus, wantState = "Failed", JobFailed
				}
				if created, err := inner.Create(queued); !created || err != nil {
					t.Fatalf("Create=%v, %v", created, err)
				}
				if err := validateJobSpec(jobSpecFromRecord(queued)); err != nil {
					t.Fatal(err)
				}
				observer := &recordingMetrics{}
				s.metrics = observer
				publications := 0
				s.jobStore = &recoveryPassStore{JobStore: inner, startQueued: func(expected JobRecord, worker string) (bool, error) {
					publications++
					if expected != queued {
						t.Errorf("publication changed stored bytes: %+v", expected)
					}
					published, err := inner.TryStartQueued(expected, worker)
					s.mu.Lock()
					defer s.mu.Unlock()
					if len(s.leases) != 0 || len(s.jobAttempts) != 0 {
						t.Error("memory/lease published before durable acknowledgement")
					}
					return published, err
				}, create: func(JobRecord) (bool, error) {
					t.Error("queued execution repeated Create")
					return false, errors.New("unexpected Create")
				}}
				var calls atomic.Int32
				registerTestWorker(t, s, "selected", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					if req.AttemptId != 1 || req.JobId != queued.JobID || req.TaskType != queued.TaskType || req.Payload != queued.Payload || req.TimeoutMs != queued.TimeoutMs {
						t.Errorf("incorrect dispatch: %+v", req)
					}
					want := queued
					want.State, want.AttemptID, want.WorkerID = JobRunning, 1, "selected"
					assertRecoveryRecord(t, inner, want)
					lease, ok := s.getLease(queued.JobID)
					if !ok || lease.AttemptID != 1 || lease.WorkerID != "selected" {
						t.Errorf("dispatch before matching lease: %+v", lease)
					}
					return &runtimepb.ExecuteJobResponse{JobId: req.JobId, AttemptId: req.AttemptId, Status: workerStatus}, nil
				})
				if err := s.runQueuedJob(queued.JobID); err != nil {
					t.Fatal(err)
				}
				want := queued
				want.State, want.AttemptID, want.WorkerID, want.HasWorkerResult = wantState, 1, "selected", true
				assertRecoveryRecord(t, inner, want) // Empty output/error still means an accepted Worker result.
				assertRecoveryRecord(t, inner, original)
				if publications != 1 || calls.Load() != 1 || observer.count("started", "") != 1 || observer.count("terminal", "") != 1 {
					t.Fatalf("publications=%d calls=%d metrics=%+v", publications, calls.Load(), observer.snapshot())
				}
				assertQueuedSessionReleased(t, s, queued.JobID, false)
			})
		}
	}
}

func TestQueuedJobNoWorkerAndEligibility(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, scenario := range []string{"no worker", "missing", "running", "cancelled", "malformed", "invalid timeout", "retired task"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				s, inner, queued := queuedSessionFixture(t, backend)
				id, wantCode := queued.JobID, codes.FailedPrecondition
				switch scenario {
				case "no worker":
					wantCode = codes.Unavailable
				case "missing":
					id++
					wantCode = codes.NotFound
				case "running":
					queued.State, queued.AttemptID, queued.WorkerID = JobRunning, 4, "old"
				case "cancelled":
					queued.State = JobCancelled
				case "malformed":
					queued.Output = "residual"
				case "invalid timeout", "retired task":
					queued.JobID++
					id = queued.JobID
					if scenario == "invalid timeout" {
						queued.TimeoutMs = 0
						wantCode = codes.InvalidArgument
					} else {
						queued.TaskType = retiredTaskType
					}
				}
				if scenario != "missing" {
					saveRecoveryRecord(t, inner, queued)
				}
				s.jobStore = &recoveryPassStore{JobStore: inner, startQueued: func(JobRecord, string) (bool, error) {
					t.Error("ineligible publication")
					return false, errors.New("unexpected publication")
				}}
				if err := s.runQueuedJob(id); status.Code(err) != wantCode {
					t.Fatalf("error=%v, want %v", err, wantCode)
				}
				assertRecoveryRecord(t, inner, queued)
				if len(s.jobAttempts) != 0 || len(s.leases) != 0 {
					t.Fatal("ineligible execution authorized memory/lease")
				}
				assertQueuedSessionReleased(t, s, id, false)
			})
		}
	}
}

func TestQueuedJobEntryGuards(t *testing.T) {
	for _, scenario := range []string{"no lifecycle", "no store", "initial read failure", "cancelled owner", "draining owner"} {
		t.Run(scenario, func(t *testing.T) {
			s, inner, queued := queuedSessionFixture(t, "memory")
			lifecycle := s.lifecycle
			injected := errors.New("initial queued read unavailable")
			switch scenario {
			case "no lifecycle":
				s.lifecycle = nil
			case "no store":
				s.jobStore = nil
			case "initial read failure":
				s.jobStore = &recoveryPassStore{JobStore: inner, load: func(int64) (JobRecord, bool, error) { return JobRecord{}, false, injected }}
			case "cancelled owner":
				lifecycle.cancelWork()
			case "draining owner":
				lifecycle.beginShutdown()
			}
			err := s.runQueuedJob(queued.JobID)
			s.lifecycle = lifecycle
			switch scenario {
			case "no lifecycle":
				if status.Code(err) != codes.FailedPrecondition {
					t.Fatalf("lifecycle guard=%v", err)
				}
			case "no store":
				if !errors.Is(err, errJobStoreNotConfigured) {
					t.Fatalf("store guard=%v", err)
				}
			case "initial read failure":
				if !errors.Is(err, injected) {
					t.Fatalf("read failure=%v", err)
				}
			case "cancelled owner":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("owner cancellation=%v", err)
				}
			case "draining owner":
				if status.Code(err) != codes.Unavailable {
					t.Fatalf("admission guard=%v", err)
				}
			}
			assertRecoveryRecord(t, inner, queued)
			assertQueuedSessionReleased(t, s, queued.JobID, false)
		})
	}
}

func TestQueuedJobCompetingSessions(t *testing.T) {
	s, inner, queued := queuedSessionFixture(t, "sqlite")
	entered, release := make(chan struct{}), make(chan struct{})
	var once atomic.Bool
	unblock := func() {
		if once.CompareAndSwap(false, true) {
			close(release)
		}
	}
	t.Cleanup(unblock)
	var calls, publications atomic.Int32
	s.jobStore = &recoveryPassStore{JobStore: inner, startQueued: func(r JobRecord, w string) (bool, error) {
		publications.Add(1)
		close(entered)
		<-release
		return inner.TryStartQueued(r, w)
	}}
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	first := make(chan error, 1)
	go func() { first <- s.runQueuedJob(queued.JobID) }()
	awaitShutdownSignal(t, entered)
	if err := s.runQueuedJob(queued.JobID); status.Code(err) != codes.Aborted {
		t.Fatalf("competing session=%v", err)
	}
	s.mu.Lock()
	_, owned := s.activeJobs[queued.JobID]
	s.mu.Unlock()
	if !owned {
		t.Fatal("losing session released winner's ownership")
	}
	unblock()
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || publications.Load() != 1 {
		t.Fatalf("calls=%d publications=%d", calls.Load(), publications.Load())
	}
	assertQueuedSessionReleased(t, s, queued.JobID, false)
}

func TestQueuedJobRetryBudget(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, scenario := range []string{"retry succeeds", "exhausted", "no second worker", "higher memory generation", "fenced first"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				s, inner, queued := queuedSessionFixture(t, backend)
				var calls atomic.Int32
				registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					n := calls.Add(1)
					if n == 1 {
						if scenario == "higher memory generation" {
							s.mu.Lock()
							s.jobAttempts[queued.JobID] = 7
							s.mu.Unlock()
						}
						if scenario == "no second worker" {
							s.mu.Lock()
							w := s.workers["worker"]
							w.Status = WorkerDead
							s.workers["worker"] = w
							s.mu.Unlock()
						}
						if scenario == "fenced first" {
							r := successfulResult(req)
							r.AttemptId = 0
							return r, nil
						}
					}
					if n == 1 || scenario == "exhausted" {
						return nil, status.Error(codes.Unavailable, "injected RPC failure")
					}
					return successfulResult(req), nil
				})
				err := s.runQueuedJob(queued.JobID)
				wantState, wantID, wantCalls := JobSucceeded, int64(2), int32(2)
				if scenario == "higher memory generation" {
					wantID = 8
				}
				if scenario == "exhausted" || scenario == "no second worker" {
					wantState = JobFailed
					if scenario == "no second worker" {
						wantID, wantCalls = 1, 1
					}
					if status.Code(err) != codes.Unavailable {
						t.Fatalf("exhaustion error=%v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				want := queued
				want.State, want.AttemptID, want.WorkerID = wantState, wantID, "worker"
				if wantState == JobSucceeded {
					want.HasWorkerResult, want.Output = true, "done"
				}
				assertRecoveryRecord(t, inner, want)
				if calls.Load() != wantCalls {
					t.Fatalf("Worker calls=%d, want %d", calls.Load(), wantCalls)
				}
				assertQueuedSessionReleased(t, s, queued.JobID, false)
			})
		}
	}
}

func TestQueuedJobPublicationConflict(t *testing.T) {
	for _, state := range []JobState{JobCancelled, JobSucceeded, JobRecovering, JobRunning} {
		t.Run(fmt.Sprint(state), func(t *testing.T) {
			s, inner, queued := queuedSessionFixture(t, "sqlite")
			changed := queued
			changed.State = state
			if state == JobRunning {
				changed.AttemptID, changed.WorkerID = 9, "other"
			}
			if state == JobSucceeded {
				changed.HasWorkerResult, changed.Output = true, "retained"
			}
			s.jobStore = &recoveryPassStore{JobStore: inner, startQueued: func(expected JobRecord, worker string) (bool, error) {
				saveRecoveryRecord(t, inner, changed) // A competing durable state change wins before the predicate.
				return inner.TryStartQueued(expected, worker)
			}}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if err := s.runQueuedJob(queued.JobID); status.Code(err) != codes.Aborted {
				t.Fatalf("conflict error=%v", err)
			}
			assertRecoveryRecord(t, inner, changed)
			if calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
				t.Fatal("conflict authorized execution")
			}
			assertQueuedSessionReleased(t, s, queued.JobID, false)
		})
	}
}

func TestQueuedJobPersistenceFailures(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		for _, tc := range []struct {
			name, stage                  string
			commit, readFail, repairFail bool
			wantState                    JobState
			wantID                       int64
			wantCalls                    int32
		}{
			{"first write fails", "first", false, false, false, JobQueued, 0, 0},
			{"first ack lost", "first", true, false, false, JobRecovering, 1, 0},
			{"first unknown read", "first", true, true, false, JobRunning, 1, 0},
			{"first unknown repair", "first", true, false, true, JobRunning, 1, 0},
			{"repair ack lost", "repair ack", true, false, false, JobRecovering, 1, 0},
			{"retry write fails", "retry", false, false, false, JobRecovering, 1, 1},
			{"retry ack lost", "retry", true, false, false, JobRecovering, 2, 1},
			{"retry unknown read", "retry", true, true, false, JobRunning, 2, 1},
			{"terminal write fails", "terminal", false, false, false, JobRecovering, 1, 1},
			{"terminal ack lost", "terminal", true, false, false, JobSucceeded, 1, 1},
			{"terminal unknown read", "terminal", true, true, false, JobSucceeded, 1, 1},
			{"exhaustion ack lost", "exhaustion", true, false, false, JobFailed, 2, 2},
		} {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				s, inner, queued := queuedSessionFixture(t, backend)
				injected, readErr := errors.New("injected acknowledgement failure"), errors.New("reconciliation read unavailable")
				failed, publications := false, 0
				s.jobStore = &recoveryPassStore{JobStore: inner,
					startQueued: func(r JobRecord, w string) (bool, error) {
						publications++
						if tc.stage == "first" || tc.stage == "repair ack" {
							if tc.commit {
								if ok, err := inner.TryStartQueued(r, w); !ok || err != nil {
									t.Fatalf("backing publication=%v, %v", ok, err)
								}
							}
							failed = true
							return false, injected
						}
						return inner.TryStartQueued(r, w)
					},
					load: func(id int64) (JobRecord, bool, error) {
						if failed && tc.readFail {
							return JobRecord{}, false, readErr
						}
						return inner.Load(id)
					},
					recoverRunning: func(r JobRecord) (bool, error) {
						if tc.repairFail {
							return false, injected
						}
						repaired, err := inner.TryRecoverRunning(r)
						if repaired && err == nil && tc.stage == "repair ack" {
							return false, injected
						}
						return repaired, err
					},
					save: func(r JobRecord) error {
						fail := tc.stage == "retry" && r.State == JobRunning || tc.stage == "terminal" && r.HasWorkerResult ||
							tc.stage == "exhaustion" && r.State == JobFailed
						if fail {
							if tc.commit {
								if err := inner.Save(r); err != nil {
									return err
								}
							}
							failed = true
							return injected
						}
						return inner.Save(r)
					}}
				var calls atomic.Int32
				registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					if tc.stage == "retry" || tc.stage == "exhaustion" {
						return nil, status.Error(codes.Unavailable, "retryable RPC error")
					}
					return successfulResult(req), nil
				})
				err := s.runQueuedJob(queued.JobID)
				if err == nil || !strings.Contains(err.Error(), injected.Error()) {
					t.Fatalf("storage failure lost: %v", err)
				}
				want := queued
				want.State, want.AttemptID = tc.wantState, tc.wantID
				if tc.wantID > 0 {
					want.WorkerID = "worker"
				}
				if tc.wantState == JobSucceeded {
					want.HasWorkerResult, want.Output = true, "done"
				}
				assertRecoveryRecord(t, inner, want)
				if calls.Load() != tc.wantCalls || publications != 1 || len(s.leases) != 0 {
					t.Fatalf("calls=%d publications=%d leases=%+v", calls.Load(), publications, s.leases)
				}
				pending := tc.readFail || tc.repairFail
				assertQueuedSessionReleased(t, s, queued.JobID, pending)
				if pending {
					entry := s.queuedReconciliations[queued.JobID]
					wantCandidate := int64(1)
					if tc.stage == "retry" {
						wantCandidate = 2
						if entry.lastPublished == nil || entry.lastPublished.AttemptID != 1 {
							t.Fatal("lost preceding confirmed generation")
						}
					}
					if entry.expected.JobID != queued.JobID || entry.expected.AttemptID != wantCandidate || entry.expected.WorkerID != "worker" || jobSpecFromRecord(entry.expected) != jobSpecFromRecord(queued) || entry.cause == nil {
						t.Fatalf("lost reconciliation provenance: %+v", entry)
					}
					if err := s.runQueuedJob(queued.JobID); status.Code(err) != codes.Aborted {
						t.Fatalf("unresolved job executed again: %v", err)
					}
				}
			})
		}
	}
}
