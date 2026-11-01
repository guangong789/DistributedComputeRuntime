package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func assertAdmissionAck(t *testing.T, response *runtimepb.SubmitJobResponse, err error, id int64) {
	t.Helper()
	if err != nil || response == nil || response.JobId != id || response.Status != "Accepted" || response.AttemptId != 0 || response.Output != "" || response.Error != "" {
		t.Fatalf("admission=%v error=%v", response, err)
	}
}

func TestAsyncAdmissionWithoutWorker(t *testing.T) {
	for _, backend := range []string{"memory", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			s, inner, _ := queuedSchedulerFixture(t, backend)
			for i, id := range []int64{0, -1, math.MinInt64, math.MaxInt64} {
				req := &runtimepb.SubmitJobRequest{JobId: id, TaskType: "hash", Payload: "raw input", TimeoutMs: 10}
				if i == 1 {
					req.TaskType, req.Payload = "sleep", "invalid duration still fails at the Worker"
				}
				if i == 2 {
					req.TaskType, req.Payload = "zk_preimage_prove", ` { "digest":"+001", "witness_ref":"ref" } `
				}
				if i == 3 {
					req.TaskType = "unsupported-workload"
				} // Preserve existing execution-time validation.
				response, err := s.SubmitJob(context.Background(), req)
				assertAdmissionAck(t, response, err, id)
				spec, err := validateJobSpec(jobSpecFromRequest(req))
				if err != nil {
					t.Fatal(err)
				}
				want := JobRecord{JobID: id, State: JobQueued, TaskType: spec.TaskType, Payload: spec.Payload, TimeoutMs: spec.TimeoutMs}
				assertRecoveryRecord(t, inner, want)
				if got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: id}); err != nil || got.State != runtimepb.JobState_JOB_STATE_QUEUED || got.AttemptId != 0 || got.HasWorkerResult {
					t.Fatalf("queued observation=%v, %v", got, err)
				}
			}
			observer := s.metrics.(*recordingMetrics)
			if observer.count("submitted", "") != 4 || observer.count("admission", string(metrics.AdmissionAccepted)) != 4 || observer.count("execution", "") != 0 || observer.count("started", "") != 0 || observer.count("terminal", "") != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
				t.Fatal("admission was counted or published as execution")
			}
		})
	}
}

func TestAsyncAdmissionWaitingAndLaterRegistration(t *testing.T) {
	s, inner, passes := queuedSchedulerFixture(t, "memory")
	useTestWorkerTransport(s)
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	stop := startManualRecovery(t, s, q, clock)
	waitRecoveryReset(t, clock, durableDiscoveryInterval)
	ctx, cancel := context.WithCancel(context.Background())
	req := &runtimepb.SubmitJobRequest{JobId: 14001, TaskType: "hash", Payload: "later", TimeoutMs: 10}
	ack, err := s.SubmitJob(ctx, req)
	assertAdmissionAck(t, ack, err, req.JobId)
	cancel()
	waitRecoveryReset(t, clock, durableDiscoveryInterval)
	clock.advance(2 * durableDiscoveryInterval)
	waitRecoveryReset(t, clock, durableDiscoveryInterval)
	queued := JobRecord{JobID: req.JobId, State: JobQueued, TaskType: req.TaskType, Payload: req.Payload, TimeoutMs: req.TimeoutMs}
	assertRecoveryRecord(t, inner, queued)
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if r.AttemptId != 1 || r.TimeoutMs != 10 {
			t.Errorf("waiting consumed Attempt budget: %v", r)
		}
		return successfulResult(r), nil
	})
	waitQueuedCondition(t, passes, func() bool { return scheduledJobDone(s, inner, req.JobId) })
	stop()
	if calls.Load() != 1 || ack.AttemptId != 0 || ack.Status != "Accepted" {
		t.Fatal("admission response raced with execution state")
	}
	if response, err := s.SubmitJob(context.Background(), req); response != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatalf("terminal duplicate=%v, %v", response, err)
	}
}

func TestAsyncAdmissionSurvivesLostNotificationAndCreateAck(t *testing.T) {
	for _, uncertain := range []bool{false, true} {
		t.Run(fmt.Sprint(uncertain), func(t *testing.T) {
			s, inner, passes := queuedSchedulerFixture(t, "memory")
			// Disable all readiness hints before the producer starts. The same
			// loop must discover the record using its periodic durable fallback.
			s.recoveryReady = nil
			registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				return successfulResult(r), nil
			})
			var creates atomic.Int32
			s.jobStore = &recoveryPassStore{JobStore: inner, create: func(r JobRecord) (bool, error) {
				creates.Add(1)
				ok, err := inner.Create(r)
				if uncertain && ok && err == nil {
					return false, errors.New("lost admission acknowledgement")
				}
				return ok, err
			}}
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			stop := startManualRecovery(t, s, q, clock)
			waitRecoveryReset(t, clock, durableDiscoveryInterval)
			req := &runtimepb.SubmitJobRequest{JobId: 14002, TaskType: "hash", Payload: "persisted", TimeoutMs: 1000}
			ack, err := s.SubmitJob(context.Background(), req)
			if uncertain {
				if ack != nil || status.Code(err) != codes.Internal {
					t.Fatalf("uncertain Create falsely accepted: %v, %v", ack, err)
				}
			} else {
				assertAdmissionAck(t, ack, err, req.JobId)
			}
			got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: req.JobId})
			if err != nil || got.State != runtimepb.JobState_JOB_STATE_QUEUED || got.AttemptId != 0 {
				t.Fatalf("lost ack identity=%v, %v", got, err)
			}
			clock.advance(durableDiscoveryInterval)
			// Notifications stay disabled after completion too. Wait on the
			// terminal event; producer shutdown then joins the complete session.
			waitQueuedCondition(t, passes, func() bool { r, ok, err := inner.Load(req.JobId); return err == nil && ok && r.State == JobSucceeded })
			stop()
			if creates.Load() != 1 {
				t.Fatal("scheduler repeated logical admission")
			}
			if response, err := s.SubmitJob(context.Background(), req); response != nil || status.Code(err) != codes.AlreadyExists {
				t.Fatalf("lost ack duplicate=%v, %v", response, err)
			}
		})
	}
}

func TestAsyncAdmissionClientLifetimeDoesNotOwnExecution(t *testing.T) {
	for _, mode := range []string{"disconnect", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			f := newMTLSCoordinator(t)
			f.s.leaseDuration = time.Minute
			started, release := make(chan context.Context, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			var calls atomic.Int32
			address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				started <- ctx
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return &runtimepb.ExecuteJobResponse{JobId: r.JobId, AttemptId: r.AttemptId, Status: "Succeeded"}, nil
			})
			registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
			f.startScheduler()
			connection := f.connection(t, testpki.Client)
			client := runtimepb.NewCoordinatorServiceClient(connection)
			observer := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req := &runtimepb.SubmitJobRequest{JobId: 14003, TaskType: "hash", Payload: "independent", TimeoutMs: 10000}
			ack, err := client.SubmitJob(ctx, req)
			assertAdmissionAck(t, ack, err, req.JobId)
			var workerCtx context.Context
			select {
			case workerCtx = <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("background execution did not start")
			}
			if mode == "disconnect" {
				cancel()
				_ = connection.Close()
			} else {
				<-ctx.Done()
				if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
					t.Fatal("submission deadline did not expire")
				}
			}
			if workerCtx.Err() != nil {
				t.Fatal("client lifetime cancelled Coordinator-owned Worker RPC")
			}
			got, err := observer.GetJob(mtlsContext(t), &runtimepb.GetJobRequest{JobId: req.JobId})
			if err != nil || got.State != runtimepb.JobState_JOB_STATE_RUNNING {
				t.Fatalf("client cancelled admitted job: %v, %v", got, err)
			}
			if duplicate, err := observer.SubmitJob(mtlsContext(t), req); duplicate != nil || status.Code(err) != codes.AlreadyExists {
				t.Fatalf("running duplicate=%v, %v", duplicate, err)
			}
			unblock()
			if _, err := observeTerminalForTest(mtlsContext(t), observer, req.JobId); err != nil {
				t.Fatal(err)
			}
			got, err = observer.GetJob(mtlsContext(t), &runtimepb.GetJobRequest{JobId: req.JobId})
			if err != nil || got.State != runtimepb.JobState_JOB_STATE_SUCCEEDED || !got.HasWorkerResult || got.Output != "" || got.Error != "" || calls.Load() != 1 {
				t.Fatalf("durable empty result=%v, %v calls=%d", got, err, calls.Load())
			}
		})
	}
}

func TestAsyncAdmissionShutdownJoinsCreate(t *testing.T) {
	for _, lostAck := range []bool{false, true} {
		t.Run(fmt.Sprint(lostAck), func(t *testing.T) {
			f := newMTLSCoordinator(t)
			db := f.store.JobStore.(*SQLiteJobStore)
			var seq int
			var name, path string
			if err := db.db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			backing := &recoveryPassStore{JobStore: f.store, create: func(r JobRecord) (bool, error) {
				close(entered)
				<-release
				ok, err := f.store.Create(r)
				if lostAck && ok && err == nil {
					return false, errors.New("lost Create acknowledgement")
				}
				return ok, err
			}}
			store := &shutdownStore{JobStore: backing, closeBacking: db.Close}
			f.s.jobStore = store
			p := newCoordinatorProcess(f.s, 2*time.Second)
			p.server, p.serveDone, p.closeStore = f.server, f.serveDone, store.Close
			p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
			t.Cleanup(func() { _ = p.shutdown(); awaitShutdownSignal(t, p.cleanupDone) })
			t.Cleanup(unblock)
			client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
			returned := make(chan error, 1)
			go func() {
				_, err := client.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 14004, TaskType: "hash", Payload: "durable", TimeoutMs: 1000})
				returned <- err
			}()
			awaitShutdownSignal(t, entered)
			shutdown := make(chan error, 1)
			go func() { shutdown <- p.shutdown() }()
			awaitShutdownSignal(t, f.s.lifecycle.ctx.Done())
			if closed, _, _ := store.snapshot(); closed {
				t.Fatal("store closed during admission Create")
			}
			select {
			case err := <-shutdown:
				t.Fatalf("shutdown skipped Create: %v", err)
			default:
			}
			unblock()
			if err := <-shutdown; err != nil {
				t.Fatal(err)
			}
			<-returned
			assertShutdownStoreClosed(t, store)
			loadShutdownRecord(t, path, JobRecord{JobID: 14004, State: JobQueued, TaskType: "hash", Payload: "durable", TimeoutMs: 1000})
		})
	}
}

func TestAsyncAdmissionValidationPrecedesPersistence(t *testing.T) {
	for _, req := range []*runtimepb.SubmitJobRequest{nil, {JobId: 1, TaskType: "hash", TimeoutMs: 0}, {JobId: 2, TaskType: "zk_preimage_prove", Payload: `{"secret":"private"}`, TimeoutMs: 1000}} {
		s := &CoordinatorServer{jobStore: &recoveryPassStore{create: func(JobRecord) (bool, error) { t.Fatal("invalid request persisted"); return false, nil }}}
		if ack, err := s.SubmitJob(context.Background(), req); ack != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatalf("invalid admission=%v, %v", ack, err)
		}
	}
	s := &CoordinatorServer{zkDisabled: true, jobStore: NewMemoryJobStore()}
	if ack, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{TaskType: "zk_preimage_prove", TimeoutMs: 1000}); ack != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("disabled admission=%v, %v", ack, err)
	}
	// Existing validation ordering remains: an invalid request does not become
	// a duplicate merely because the same durable JobID is already present.
	s = &CoordinatorServer{jobStore: NewMemoryJobStore()}
	saveRecoveryRecord(t, s.jobStore, JobRecord{JobID: 1, State: JobQueued})
	if _, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 1, TimeoutMs: 0}); status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), "already") {
		t.Fatalf("validation ordering changed: %v", err)
	}
}

func TestAsyncAdmissionMetricsSeparateCreateAndExecution(t *testing.T) {
	s, inner, _ := queuedSchedulerFixture(t, "memory")
	start := prepareQueuedTestScheduler(t, s)
	observer := s.metrics.(*recordingMetrics)
	observer.hook = func(e recordedMetric) {
		if e.name == "submitted" {
			r, ok, err := inner.Load(14005)
			if err != nil || !ok || !isUnattemptedQueued(r) {
				t.Error("admission counted before confirmed Queued Create")
			}
		}
	}
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	req := &runtimepb.SubmitJobRequest{JobId: 14005, TaskType: "hash", Payload: "metrics", TimeoutMs: 1000}
	ack, err := s.SubmitJob(context.Background(), req)
	assertAdmissionAck(t, ack, err, req.JobId)
	if observer.count("admission", string(metrics.AdmissionAccepted)) != 1 || observer.count("submitted", "") != 1 || observer.count("execution", "") != 0 || observer.count("started", "") != 0 || observer.count("terminal", "") != 0 {
		t.Fatal("Create was recorded as execution")
	}
	if _, err := s.SubmitJob(context.Background(), req); status.Code(err) != codes.AlreadyExists {
		t.Fatal(err)
	}
	if observer.count("submitted", "") != 1 || observer.count("admission", "") != 1 {
		t.Fatal("duplicate precheck counted another durable admission")
	}
	start()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitDirectTerminalForTest(t, ctx, s, req.JobId)
	if err := waitExecutionReleaseForTest(ctx, s, req.JobId); err != nil {
		t.Fatal(err)
	}
	if observer.count("admission", "") != 1 || observer.count("submitted", "") != 1 || observer.count("execution", "") != 1 || observer.count("started", "") != 1 || observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 {
		t.Fatalf("execution/admission duplicated: %+v", observer.snapshot())
	}
}

func TestAsyncAdmissionQueuedZKBlocksDisabledRestart(t *testing.T) {
	store := NewMemoryJobStore()
	s := &CoordinatorServer{jobStore: store}
	req := &runtimepb.SubmitJobRequest{JobId: 14006, TaskType: "zk_preimage_prove", Payload: `{"witness_ref":"ref","digest":"1"}`, TimeoutMs: 1000}
	ack, err := s.SubmitJob(context.Background(), req)
	assertAdmissionAck(t, ack, err, req.JobId)
	fresh := &CoordinatorServer{jobStore: store, zkDisabled: true}
	if err := fresh.checkDisabledZKRecovery(); err == nil {
		t.Fatal("disabled restart accepted an unfinished admitted ZK job")
	}
	if r, ok, err := store.Load(req.JobId); err != nil || !ok || !isUnattemptedQueued(r) {
		t.Fatalf("startup guard changed queued admission: %+v, %v", r, err)
	}
}
