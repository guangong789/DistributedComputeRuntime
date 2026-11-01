package main

import (
	"context"
	"crypto/tls"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestGetJobMTLSAuthorizedSnapshot(t *testing.T) {
	f := newMTLSCoordinator(t)
	r := JobRecord{JobID: 12200, State: JobSucceeded, AttemptID: 2, WorkerID: "old", TaskType: "hash", Payload: "PRIVATE_MARKER", TimeoutMs: 1000, HasWorkerResult: true}
	saveRecoveryRecord(t, f.store, r)
	before := f.snapshot()
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	got, err := client.GetJob(mtlsContext(t), &runtimepb.GetJobRequest{JobId: r.JobID})
	if err != nil {
		t.Fatal(err)
	}
	assertGetJobSnapshot(t, got, r, runtimepb.JobState_JOB_STATE_SUCCEEDED)
	if !reflect.DeepEqual(before, f.snapshot()) {
		t.Fatal("authorized observation changed Coordinator execution or scheduling")
	}
	assertRecoveryRecord(t, f.store, r)
	if response, err := client.GetJob(mtlsContext(t), &runtimepb.GetJobRequest{JobId: r.JobID + 1}); response != nil || status.Code(err) != codes.NotFound {
		t.Fatalf("RPC NotFound=%v, %v", response, err)
	}
}

func TestGetJobMTLSRejectsNonClientsBeforeLoad(t *testing.T) {
	f := newMTLSCoordinator(t)
	var reads atomic.Int32
	f.s.jobStore = &recoveryPassStore{load: func(int64) (JobRecord, bool, error) { reads.Add(1); return JobRecord{}, false, nil }}
	for _, identity := range []string{testpki.Worker, testpki.Coordinator, "spiffe://distributedzk.local/client/unknown"} {
		t.Run(identity, func(t *testing.T) {
			// The raw trusted TLS connection admits the recognized Coordinator
			// role, which method authorization must still reject. Unknown clients
			// fail earlier in the TLS identity allowlist.
			files := f.ca.Issue(t, identity, nil)
			conn := f.connect(t, credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{files.Pair}, RootCAs: f.ca.Roots(), ServerName: "127.0.0.1"}))
			got, err := runtimepb.NewCoordinatorServiceClient(conn).GetJob(mtlsContext(t), &runtimepb.GetJobRequest{JobId: 12200})
			if got != nil || err == nil {
				t.Fatalf("unauthorized read=%v, %v", got, err)
			}
			if identity != "spiffe://distributedzk.local/client/unknown" && status.Code(err) != codes.PermissionDenied {
				t.Fatalf("role error=%v", err)
			}
			if reads.Load() != 0 {
				t.Fatal("rejected identity entered durable Load")
			}
		})
	}
}

func TestGetJobIndependentOfAdmission(t *testing.T) {
	f := newMTLSCoordinator(t)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &runtimepb.ExecuteJobResponse{JobId: r.JobId, AttemptId: r.AttemptId, Status: "Succeeded"}, nil
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	f.startScheduler()
	submitClient := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	observer := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	finished := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go func() {
		response, err := submitClient.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 12201, TaskType: "hash", Payload: "private", TimeoutMs: 1000})
		if err == nil && (response.GetStatus() != "Accepted" || response.AttemptId != 0 || response.Output != "" || response.Error != "") {
			err = status.Error(codes.Internal, "Submit response changed")
		}
		finished <- err
	}()
	waitRecoverySignal(t, started)
	got, err := observer.GetJob(mtlsContext(t), &runtimepb.GetJobRequest{JobId: 12201})
	if err != nil || got.State != runtimepb.JobState_JOB_STATE_RUNNING || got.AttemptId != 1 || got.WorkerId != "worker-a" || got.HasWorkerResult {
		t.Fatalf("in-flight snapshot=%v, %v", got, err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("admission waited for execution: %v", err)
	}
	cancel() // Original submission lifetime cannot control the queued session.
	unblock()
	if _, err := observeTerminalForTest(mtlsContext(t), observer, 12201); err != nil {
		t.Fatal(err)
	}
	got, err = observer.GetJob(mtlsContext(t), &runtimepb.GetJobRequest{JobId: 12201})
	if err != nil || got.State != runtimepb.JobState_JOB_STATE_SUCCEEDED || !got.HasWorkerResult || got.Output != "" || got.Error != "" || got.AttemptId != 1 {
		t.Fatalf("independent accepted result=%v, %v", got, err)
	}
}

func TestGetJobMatchesSubmitJobIDContract(t *testing.T) {
	s := newTestCoordinator()
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	for _, id := range []int64{0, -1, math.MinInt64, math.MaxInt64} {
		if _, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: id, TaskType: "hash", Payload: "id", TimeoutMs: 1000}); err != nil {
			t.Fatalf("SubmitJob ID %d: %v", id, err)
		}
		got, err := s.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: id})
		if err != nil || got.JobId != id || got.State != runtimepb.JobState_JOB_STATE_QUEUED || got.AttemptId != 0 || got.HasWorkerResult {
			t.Fatalf("GetJob disagrees with SubmitJob ID %d: %v, %v", id, got, err)
		}
	}
}

func TestGetJobShutdownJoinsDurableRead(t *testing.T) {
	f := newMTLSCoordinator(t)
	loaded, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	backing := &recoveryPassStore{JobStore: f.store, load: func(id int64) (JobRecord, bool, error) { close(loaded); <-release; return f.store.Load(id) }}
	store := &shutdownStore{JobStore: backing, closeBacking: f.store.JobStore.(*SQLiteJobStore).Close}
	f.s.jobStore = store
	p := newCoordinatorProcess(f.s, 2*time.Second)
	p.server, p.serveDone, p.closeStore = f.server, f.serveDone, store.Close
	p.shutdownMetrics = func(context.Context) error { store.access("metrics"); return nil }
	t.Cleanup(func() { _ = p.shutdown(); awaitShutdownSignal(t, p.cleanupDone) })
	t.Cleanup(unblock)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	returned := make(chan error, 1)
	go func() {
		_, err := client.GetJob(context.Background(), &runtimepb.GetJobRequest{JobId: 12202})
		returned <- err
	}()
	awaitShutdownSignal(t, loaded)
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.shutdown() }()
	awaitShutdownSignal(t, f.s.lifecycle.ctx.Done())
	if closed, _, _ := store.snapshot(); closed {
		t.Fatal("store closed with GetJob Load in flight")
	}
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown skipped GetJob read: %v", err)
	default:
	}
	unblock()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	if err := <-returned; err == nil {
		t.Fatal("shutdown read unexpectedly returned OK")
	}
	assertShutdownStoreClosed(t, store)
}
