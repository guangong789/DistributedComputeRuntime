package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	"github.com/guangong789/DistributedComputeRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	diagAlive   = runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_ALIVE
	diagDead    = runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_DEAD
	diagFresh   = runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_FRESH
	diagExpired = runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_EXPIRED

	diagNoConfigured  = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_CONFIGURED_WORKER
	diagNoAlive       = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ALIVE_WORKER
	diagNoEligible    = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ELIGIBLE_WORKER
	diagEligible      = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_ELIGIBLE_WORKER_EXISTS
	diagUnknown       = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_UNKNOWN
	diagNotApplicable = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NOT_APPLICABLE
	diagChanged       = runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_JOB_CHANGED
)

func TestDiagnosticsWorkerListing(t *testing.T) {
	clock := newManualRecoveryClock()
	var samples int
	s := &CoordinatorServer{workerNow: func() time.Time { samples++; return clock.now() }, healthTTL: testHealthTTL}
	ctx := t.Context()
	list := func() []*runtimepb.WorkerDiagnostic {
		t.Helper()
		before := samples
		got, err := s.ListWorkers(ctx, &runtimepb.ListWorkersRequest{})
		if err != nil || !got.GetWorkerSnapshotAt().AsTime().Equal(clock.now()) || samples != before+1 {
			t.Fatalf("bad single-time worker snapshot: %v", err)
		}
		return got.GetWorkers()
	}
	if got := list(); len(got) != 0 {
		t.Fatal("empty registry returned a Worker")
	}
	legacy, err := s.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "legacy", Address: "127.0.0.1:50051",
		ExternalTaskTypes: []string{externalsquare.TaskType, cudavector.TaskType, "ignored"}})
	if err != nil || !legacy.GetAccepted() {
		t.Fatalf("legacy registration: %v", err)
	}
	for _, id := range []string{"unknown", "unavailable", "ready"} {
		req := managedCUDAClaim("127.0.0.1:50052", 1)
		req.WorkerId = id
		got, err := s.RegisterWorker(ctx, req)
		if err != nil || !got.GetAccepted() {
			t.Fatalf("managed registration: %v", err)
		}
		if id != "unknown" {
			state := healthReady
			if id == "unavailable" {
				state = healthUnavailable
			}
			// Anchor 1 is the registration origin with this stationary clock.
			reply, err := s.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: id,
				SessionToken: got.GetSessionToken(), CudaHealth: healthReport(1, 1, state)})
			if err != nil || !reply.GetAccepted() {
				t.Fatalf("managed observation: %v", err)
			}
		}
	}
	clock.advance(time.Second)
	got := list()
	all := []string{"hash", "sleep", externalsquare.TaskType, cudavector.TaskType}
	for i, want := range []struct {
		id       string
		managed  bool
		health   runtimepb.CudaHealthState
		fresh    runtimepb.CudaEvidenceFreshness
		eligible []string
	}{
		{"legacy", false, 0, 0, all},
		{"ready", true, healthReady, diagFresh, all},
		{"unavailable", true, healthUnavailable, diagFresh, all[:3]},
		{"unknown", true, 0, 0, all[:3]},
	} {
		if len(got) != 4 {
			t.Fatalf("workers = %d, want 4", len(got))
		}
		worker := got[i]
		if worker.WorkerId != want.id || worker.Liveness != diagAlive || worker.LastSeenAge.AsDuration() != time.Second ||
			worker.CudaHealthManaged != want.managed || worker.EffectiveCudaHealth != want.health || worker.CudaEvidenceFreshness != want.fresh ||
			!slices.Equal(worker.ExternalTaskTypes, all[2:]) || !slices.Equal(worker.EligibleTaskTypes, want.eligible) {
			t.Fatalf("worker projection: %v", worker)
		}
		if want.fresh == diagFresh && worker.CudaFreshFor.AsDuration() != testHealthTTL-time.Second ||
			want.fresh == 0 && worker.CudaFreshFor != nil {
			t.Fatalf("worker freshness: %v", worker)
		}
	}

	// A sampled age past the timeout does not perform the monitor's mutation.
	clock.advance(workerLivenessTimeout)
	atTimeout := list()[1]
	if atTimeout.Liveness != diagAlive || atTimeout.LastSeenAge.AsDuration() <= workerLivenessTimeout ||
		!slices.Contains(atTimeout.EligibleTaskTypes, cudavector.TaskType) {
		t.Fatal("diagnostics changed recorded liveness before its monitor ran")
	}
	clock.advance(testHealthTTL - time.Second - workerLivenessTimeout)
	expired := list()[1]
	if expired.Liveness != diagAlive || expired.EffectiveCudaHealth != 0 || expired.CudaEvidenceFreshness != diagExpired ||
		expired.CudaFreshFor != nil || slices.Contains(expired.EligibleTaskTypes, cudavector.TaskType) || !expired.CudaHealthManaged {
		t.Fatalf("expired Ready appeared fresh or unmanaged: %v", expired)
	}
	s.checkWorkerLiveness(workerLivenessTimeout)
	for _, worker := range list() {
		if worker.Liveness != diagDead || len(worker.EligibleTaskTypes) != 0 {
			t.Fatalf("dead Worker remains eligible: %v", worker)
		}
	}
}

func TestDiagnosticsJobPlacement(t *testing.T) {
	clock := newManualRecoveryClock()
	s := &CoordinatorServer{jobStore: NewMemoryJobStore(), workerNow: clock.now,
		workers:     map[string]WorkerInfo{"cpu": {ID: "cpu", Status: WorkerAlive}},
		workerOrder: []string{"cpu", "gpu"}, nextWorker: 1, queuedSessions: 1, queuedSessionLimit: 1}
	record := JobRecord{JobID: -9, State: JobQueued, TaskType: cudavector.TaskType, Payload: "private payload", TimeoutMs: 1000}
	saveRecoveryRecord(t, s.jobStore, record)
	req := &runtimepb.DiagnoseJobRequest{JobId: record.JobID}
	gpu := WorkerInfo{ID: "gpu", Status: WorkerDead, externalTasks: cudaVectorCapability, cudaHealth: cudaHealthEvidence{managed: true}}
	for _, tc := range []struct {
		name       string
		worker     *WorkerInfo
		wantCounts [3]uint32
		want       runtimepb.PlacementInterpretation
	}{
		{"missing capability", nil, [3]uint32{0, 0, 0}, diagNoConfigured},
		{"dead", &gpu, [3]uint32{1, 0, 0}, diagNoAlive},
		{"managed unknown", &WorkerInfo{ID: "gpu", Status: WorkerAlive, externalTasks: cudaVectorCapability, cudaHealth: gpu.cudaHealth}, [3]uint32{1, 1, 0}, diagNoEligible},
		{"unmanaged", &WorkerInfo{ID: "gpu", Status: WorkerAlive, externalTasks: cudaVectorCapability}, [3]uint32{1, 1, 1}, diagEligible},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.worker != nil {
				s.workers["gpu"] = *tc.worker
			}
			got, err := s.DiagnoseJob(t.Context(), req)
			if err != nil || got.GetInterpretation() != tc.want ||
				[3]uint32{got.GetConfiguredWorkers(), got.GetAliveCompatibleWorkers(), got.GetEligibleWorkers()} != tc.wantCounts ||
				s.hasEligibleWorker(record.TaskType) != (tc.wantCounts[2] > 0) {
				t.Fatalf("job diagnostic: %v, %v", got, err)
			}
			if s.nextWorker != 1 || s.queuedSessions != 1 {
				t.Fatal("diagnostics moved the cursor or changed session capacity")
			}
		})
	}

	for _, tc := range []struct {
		state JobState
		want  runtimepb.PlacementInterpretation
	}{
		{JobRunning, diagUnknown}, {JobRecovering, diagEligible}, {JobSucceeded, diagNotApplicable},
	} {
		record.State, record.AttemptID, record.WorkerID = tc.state, 2, "gpu"
		record.HasWorkerResult = tc.state == JobSucceeded
		saveRecoveryRecord(t, s.jobStore, record)
		// An older lease must not make the persisted generation look authorized.
		s.leases = map[int64]JobLease{record.JobID: {AttemptID: 1, WorkerID: "cpu", ExpiresAt: time.Now().Add(time.Minute)}}
		got, err := s.DiagnoseJob(t.Context(), req)
		if err != nil || got.GetInterpretation() != tc.want || got.GetState() != wireJobState(tc.state) ||
			got.GetAttemptId() != 2 || got.GetWorkerId() != "gpu" || got.GetHasWorkerResult() != record.HasWorkerResult {
			t.Fatalf("durable job diagnostic: %v, %v", got, err)
		}
	}
	if _, err := s.DiagnoseJob(t.Context(), &runtimepb.DiagnoseJobRequest{JobId: 42}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing job status: %v", err)
	}
}

type diagnosticReadStore struct {
	JobStore
	load func(context.Context, int64) (JobRecord, bool, error)
}

func (s diagnosticReadStore) loadContext(ctx context.Context, id int64) (JobRecord, bool, error) {
	return s.load(ctx, id)
}

func TestDiagnosticsDurableRevalidationDoesNotHoldRegistryLock(t *testing.T) {
	for _, changed := range []bool{false, true} {
		t.Run(fmt.Sprintf("job_changed=%t", changed), func(t *testing.T) {
			backing := NewMemoryJobStore()
			record := JobRecord{JobID: 91, State: JobQueued, TaskType: "hash"}
			saveRecoveryRecord(t, backing, record)
			recheck, release := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			reads := 0
			s := &CoordinatorServer{jobStore: diagnosticReadStore{JobStore: backing, load: func(ctx context.Context, id int64) (JobRecord, bool, error) {
				reads++
				if reads == 2 {
					close(recheck)
					select {
					case <-release:
					case <-ctx.Done():
						return JobRecord{}, false, ctx.Err()
					}
				}
				return backing.loadContext(ctx, id)
			}}}
			type response struct {
				got *runtimepb.DiagnoseJobResponse
				err error
			}
			done := make(chan response, 1)
			go func() {
				got, err := s.DiagnoseJob(ctx, &runtimepb.DiagnoseJobRequest{JobId: record.JobID})
				done <- response{got, err}
			}()
			waitRecoverySignal(t, recheck)
			if _, err := s.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "new", Address: "127.0.0.1:50051"}); err != nil {
				t.Fatal(err)
			}
			want := diagNoConfigured
			if changed {
				record.State, record.AttemptID, record.WorkerID = JobRunning, 1, "new"
				saveRecoveryRecord(t, backing, record)
				want = diagChanged
			}
			close(release)
			select {
			case result := <-done:
				if result.err != nil || result.got.GetAttemptId() != 0 || result.got.GetConfiguredWorkers() != 0 ||
					result.got.GetInterpretation() != want {
					t.Fatalf("diagnosis mixed durable or registry snapshots: %v, %v", result.got, result.err)
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

func TestDiagnosticsConcurrentRegistryUpdates(t *testing.T) {
	s := &CoordinatorServer{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, registration := range []bool{true, false} {
		wg.Go(func() {
			<-start
			ctx := t.Context()
			for i := range 16 {
				id := fmt.Sprintf("worker-%02d", i)
				if registration {
					_, err := s.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: id, Address: "127.0.0.1:50051"})
					if err != nil {
						t.Error(err)
					}
				} else {
					_, err := s.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: id})
					if err != nil {
						t.Error(err)
					}
				}
			}
		})
	}
	close(start)
	defer wg.Wait()
	assertSnapshot := func(got *runtimepb.ListWorkersResponse) {
		t.Helper()
		var previous string
		for _, worker := range got.Workers {
			if worker.WorkerId <= previous || worker.Liveness != diagAlive || worker.LastSeenAge.AsDuration() < 0 ||
				!slices.Equal(worker.EligibleTaskTypes, []string{"hash", "sleep"}) {
				t.Fatalf("incoherent registry projection: %v", worker)
			}
			previous = worker.WorkerId
		}
	}
	for range 16 {
		got, err := s.ListWorkers(t.Context(), &runtimepb.ListWorkersRequest{})
		if status.Code(err) == codes.Unavailable {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		assertSnapshot(got)
	}
	wg.Wait()
	got, err := s.ListWorkers(t.Context(), &runtimepb.ListWorkersRequest{})
	if err != nil || len(got.GetWorkers()) != 16 {
		t.Fatalf("final worker registry snapshot: %v", err)
	}
	assertSnapshot(got)
}

func TestDiagnosticsRegistryContentionDoesNotWait(t *testing.T) {
	store := NewMemoryJobStore()
	saveRecoveryRecord(t, store, JobRecord{JobID: 1, State: JobQueued, TaskType: "hash"})
	s := &CoordinatorServer{jobStore: store}
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan [2]error, 1)
	go func() {
		_, listErr := s.ListWorkers(ctx, &runtimepb.ListWorkersRequest{})
		_, jobErr := s.DiagnoseJob(ctx, &runtimepb.DiagnoseJobRequest{JobId: 1})
		done <- [2]error{listErr, jobErr}
	}()
	select {
	case errs := <-done:
		for _, err := range errs {
			if status.Code(err) != codes.Unavailable {
				t.Fatalf("contended registry did not return Unavailable: %v", err)
			}
		}
	case <-ctx.Done():
		t.Fatal("diagnostic query waited for the Coordinator mutex")
	}
}

func TestDiagnosticsLimitsAndCancellation(t *testing.T) {
	s := &CoordinatorServer{workers: make(map[string]WorkerInfo)}
	for i := range maxDiagnosticWorkers + 1 {
		id := fmt.Sprint(i)
		s.workers[id] = WorkerInfo{ID: id}
	}
	if _, err := s.ListWorkers(t.Context(), &runtimepb.ListWorkersRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("worker limit: %v", err)
	}
	delete(s.workers, fmt.Sprint(maxDiagnosticWorkers))
	got, err := s.ListWorkers(t.Context(), &runtimepb.ListWorkersRequest{})
	if err != nil || len(got.GetWorkers()) != maxDiagnosticWorkers {
		t.Fatalf("registry bound rejected an in-range registry: %v", err)
	}
	id := strings.Repeat("w", maxDiagnosticResponseBytes)
	s.workers = map[string]WorkerInfo{id: {ID: id}}
	if _, err := s.ListWorkers(t.Context(), &runtimepb.ListWorkersRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("byte limit: %v", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.ListWorkers(canceled, &runtimepb.ListWorkersRequest{}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled worker listing: %v", err)
	}
	if _, err := s.DiagnoseJob(canceled, &runtimepb.DiagnoseJobRequest{}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled diagnosis: %v", err)
	}

	synctest.Test(t, func(t *testing.T) {
		s := &CoordinatorServer{jobStore: diagnosticReadStore{load: func(ctx context.Context, _ int64) (JobRecord, bool, error) {
			<-ctx.Done()
			return JobRecord{}, false, ctx.Err()
		}}}
		start := time.Now()
		_, err := s.DiagnoseJob(t.Context(), &runtimepb.DiagnoseJobRequest{})
		if status.Code(err) != codes.DeadlineExceeded || time.Since(start) != diagnosticTimeout {
			t.Fatalf("server-side diagnostic timeout: %v", err)
		}
	})
}

func TestDiagnosticsMTLSAuthorizationAndPurity(t *testing.T) {
	const diagnosticClient = "spiffe://distributedzk.local/client/diagnostics"
	f := newMTLSCoordinatorWithSecurity(t, func(cfg *coordinatorSecurityConfig) {
		cfg.Clients = append(cfg.Clients, diagnosticClient)
		cfg.DiagnosticClients = []string{diagnosticClient}
	})
	ctx := mtlsContext(t)
	clock := newManualRecoveryClock()
	f.s.mu.Lock()
	f.s.workerNow, f.s.healthTTL = clock.now, testHealthTTL
	f.s.queuedSessions, f.s.queuedSessionLimit = 1, 1
	f.s.mu.Unlock()
	sqlite := f.store.JobStore.(*SQLiteJobStore)
	var reads atomic.Int32
	f.s.jobStore = diagnosticReadStore{JobStore: f.store, load: func(ctx context.Context, id int64) (JobRecord, bool, error) {
		reads.Add(1)
		return sqlite.loadContext(ctx, id)
	}}
	worker := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	key := managedCUDAClaim("127.0.0.1:50051", 2)
	token := claimTestSession(t, ctx, worker, key)
	anchor := managedHeartbeat(t, ctx, worker, token, nil)
	managedHeartbeat(t, ctx, worker, token, healthReport(1, anchor, healthReady))
	registerMTLSExecutionWorker(t, f, workerBIdentity, "worker-b", "127.0.0.1:50052")
	if _, ok := f.s.getNextWorker("hash"); !ok {
		t.Fatal("fixture did not advance cursor before diagnostics")
	}
	record := JobRecord{JobID: 101, State: JobSucceeded, AttemptID: 2, WorkerID: sessionWorkerID,
		TaskType: cudavector.TaskType, Payload: "diagnostics-sensitive-payload", TimeoutMs: 1000,
		HasWorkerResult: true, Output: "diagnostics-sensitive-output", Error: "diagnostics-sensitive-error"}
	saveRecoveryRecord(t, sqlite, record)
	before := f.snapshot()
	f.s.mu.Lock()
	sessions := maps.Clone(f.s.workerSessions)
	f.s.mu.Unlock()

	ordinary := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	operator := runtimepb.NewCoordinatorServiceClient(f.connection(t, diagnosticClient))
	coordCert := f.ca.Issue(t, testpki.Coordinator, nil)
	coord := runtimepb.NewCoordinatorServiceClient(f.connect(t, credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{coordCert.Pair}, RootCAs: f.ca.Roots(), ServerName: "127.0.0.1",
	})))
	unknown := runtimepb.NewCoordinatorServiceClient(f.connection(t, "spiffe://distributedzk.local/client/unlisted"))
	for _, tc := range []struct {
		name   string
		client runtimepb.CoordinatorServiceClient
		want   codes.Code
	}{
		{"ordinary Client", ordinary, codes.PermissionDenied}, {"Worker", worker, codes.PermissionDenied},
		{"Coordinator", coord, codes.PermissionDenied}, {"unlisted Client", unknown, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.client.ListWorkers(ctx, &runtimepb.ListWorkersRequest{}); status.Code(err) != tc.want {
				t.Fatalf("ListWorkers authorization: %v", err)
			}
			if _, err := tc.client.DiagnoseJob(ctx, &runtimepb.DiagnoseJobRequest{JobId: record.JobID}); status.Code(err) != tc.want {
				t.Fatalf("DiagnoseJob authorization: %v", err)
			}
		})
	}
	if reads.Load() != 0 {
		t.Fatal("unauthorized diagnostic reached the durable store")
	}
	for _, client := range []runtimepb.CoordinatorServiceClient{ordinary, operator} {
		if got, err := client.GetJob(ctx, &runtimepb.GetJobRequest{JobId: record.JobID}); err != nil || got.GetOutput() != record.Output {
			t.Fatalf("ordinary GetJob authorization changed: %v", err)
		}
	}
	list, err := operator.ListWorkers(ctx, &runtimepb.ListWorkersRequest{})
	if err != nil || len(list.GetWorkers()) != 2 || list.Workers[0].GetCudaEvidenceFreshness() != diagFresh {
		t.Fatalf("authorized list: %v", err)
	}
	job, err := operator.DiagnoseJob(ctx, &runtimepb.DiagnoseJobRequest{JobId: record.JobID})
	if err != nil || job.GetInterpretation() != diagNotApplicable || !job.GetHasWorkerResult() || job.GetEligibleWorkers() != 1 ||
		job.GetAttemptId() != record.AttemptID || job.GetWorkerId() != sessionWorkerID || reads.Load() != 2 {
		t.Fatalf("authorized diagnostic failed to project durable or current state: %v", err)
	}
	for _, data := range []string{protojson.Format(list), protojson.Format(job)} {
		for _, forbidden := range []string{"session", "registration", "anchor", "sequence", key.Address, record.Payload, record.Output, record.Error} {
			if strings.Contains(data, forbidden) {
				t.Fatal("diagnostics exposed a private field or value")
			}
		}
	}
	if !reflect.DeepEqual(before, f.snapshot()) {
		t.Fatal("diagnostics changed registry, cursor, health, Attempts, writes, or notifications")
	}
	f.s.mu.Lock()
	unchanged := reflect.DeepEqual(sessions, f.s.workerSessions) && f.s.queuedSessions == 1
	f.s.mu.Unlock()
	if !unchanged {
		t.Fatal("diagnostics touched session ownership or reserved queued capacity")
	}
	assertRecoveryRecord(t, sqlite, record)
}

func TestDiagnosticsSecurityFlagRejectsNonClientSubset(t *testing.T) {
	for _, principal := range []string{testpki.Worker, "spiffe://distributedzk.local/client/not-allowed"} {
		flags := flag.NewFlagSet("diagnostic", flag.ContinueOnError)
		cfg := coordinatorSecurityFlags(flags)
		if err := flags.Parse([]string{"-allow-client", testpki.Client, "-allow-worker", testpki.Worker, "-allow-diagnostics-client", principal}); err != nil {
			t.Fatal(err)
		}
		if _, err := cfg.load(); err == nil || !strings.Contains(err.Error(), "diagnostic") {
			t.Fatalf("invalid diagnostic allowlist was not rejected: %v", err)
		}
	}
}

func TestDiagnosticsStoreErrorIsSanitized(t *testing.T) {
	s := &CoordinatorServer{jobStore: diagnosticReadStore{load: func(context.Context, int64) (JobRecord, bool, error) {
		return JobRecord{}, false, errors.New("private database state")
	}}}
	if _, err := s.DiagnoseJob(t.Context(), &runtimepb.DiagnoseJobRequest{}); status.Code(err) != codes.Internal ||
		status.Convert(err).Message() != "failed to read job" {
		t.Fatalf("diagnostic store error was exposed: %v", err)
	}
}

func TestDiagnosticsSQLiteReadHonorsDeadline(t *testing.T) {
	store, err := NewSQLiteJobStore(t.TempDir() + "/jobs.db")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	// A reserved sole connection makes database/sql wait before starting SQL.
	// No lock race or driver-dependent lock duration is needed.
	store.db.SetMaxOpenConns(1)
	conn, err := store.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	s := &CoordinatorServer{jobStore: store}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := s.DiagnoseJob(ctx, &runtimepb.DiagnoseJobRequest{}); status.Code(err) != codes.DeadlineExceeded ||
		store.db.Stats().WaitCount != 1 {
		t.Fatalf("SQLite read did not stop waiting for its connection: %v", err)
	}
}
