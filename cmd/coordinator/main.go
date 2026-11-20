package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedComputeRuntime/internal/metrics/prometheus"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type CoordinatorServer struct {
	runtimepb.UnimplementedCoordinatorServiceServer

	// mu protects the worker registry, round-robin cursor, attempts, leases, and active jobs.
	mu             sync.Mutex
	workers        map[string]WorkerInfo
	workerSessions map[string]workerRegistrationSession // guarded by mu; never used for dispatch
	workerOrder    []string
	nextWorker     int

	// Configure before serving. A nil clock uses time.Now. Managed CUDA
	// registration requires a positive healthTTL.
	workerNow func() time.Time
	healthTTL time.Duration

	jobAttempts map[int64]int64
	leases      map[int64]JobLease

	activeJobs map[int64]struct{}
	// Protected by mu. Explicit responsibility for unsettled internal queued
	// sessions, consumed by the existing recovery loop. Pointer identity guards
	// retirement against replacement; ordinary claims remain excluded.
	queuedReconciliations map[int64]*queuedReconciliation
	// Session capacity is separate from per-Job exclusion. Configure before
	// startup; non-positive limits use defaultQueuedSessionLimit.
	queuedSessionLimit int
	queuedSessions     int // protected by mu, includes reserved/not-yet-launched work

	// Set before serving requests; non-positive values use the 5-second default.
	leaseDuration time.Duration
	// Optional completion observer, called after dispatch publishes its result.
	// Configure before serving; callbacks may run concurrently.
	onDispatchDone func(jobID, attemptID int64)

	jobStore  JobStore
	lifecycle *coordinatorLifecycle
	// Joined after admitted RPCs/internal sessions and recovery producers exit.
	dispatchWG sync.WaitGroup
	// Configure before serving. Worker readiness notifications coalesce.
	recoveryReady chan struct{}

	// Configure before serving; implementations must support concurrent calls.
	metrics metrics.Metrics
	// Configure before serving. Nil fails closed; isolated tests may explicitly
	// inject their own transport. Production binds TLS to each selected Worker.
	workerCredentials func(WorkerInfo) (credentials.TransportCredentials, error)
}

func (s *CoordinatorServer) SubmitJob(
	ctx context.Context,
	req *runtimepb.SubmitJobRequest,
) (*runtimepb.SubmitJobResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "SubmitJob request is required")
	}
	spec := jobSpecFromRequest(req)
	if err := validateJobSpec(spec); err != nil {
		return nil, err
	}
	if s.jobStore == nil {
		return nil, status.Error(codes.FailedPrecondition, errJobStoreNotConfigured.Error())
	}
	// Admitted duplicates need no execution claim, including during recovery or
	// a blocked terminal write. Recheck after claiming to close the read/claim race.
	if err := s.checkExistingSubmission(spec); err != nil {
		return nil, status.Errorf(jobStoreErrorCode(err), "submit job %d: %v", req.JobId, err)
	}
	owned := s.claimJob(req.JobId)
	admitted := false
	if owned {
		defer func() {
			s.releaseJob(req.JobId)
			if admitted {
				s.notifyRecoveryReady()
			}
		}()
	}
	if err := s.checkExistingSubmission(spec); err != nil {
		return nil, status.Errorf(jobStoreErrorCode(err), "submit job %d: %v", req.JobId, err)
	}
	if !owned {
		return nil, status.Errorf(codes.Aborted, "job %d has a local session in progress; durable admission is not confirmed", req.JobId)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Create's confirmed success is durable admission. No Worker, generation,
	// lease or execution context is allocated by this request.
	queued := JobRecord{JobID: spec.JobID, State: JobQueued, TaskType: spec.TaskType,
		Payload: spec.Payload, TimeoutMs: spec.TimeoutMs}
	start := time.Now()
	created, err := s.jobStore.Create(queued)
	observer := metrics.Resolve(s.metrics)
	if err != nil {
		observer.ObserveJobAdmission(metrics.AdmissionStoreError, time.Since(start))
		// An error can hide a committed Queued row. Never delete, finalize,
		// publish an Attempt or report Accepted based on an uncertain write.
		return nil, status.Errorf(jobStoreErrorCode(err), "admit job %d: %v", spec.JobID, err)
	}
	if !created {
		observer.ObserveJobAdmission(metrics.AdmissionConflict, time.Since(start))
		if err := s.checkExistingSubmission(spec); err != nil {
			return nil, status.Errorf(jobStoreErrorCode(err), "submit job %d: %v", spec.JobID, err)
		}
		return nil, status.Errorf(codes.Internal, "Create rejected job %d but durable admission could not be confirmed", spec.JobID)
	}
	admitted = true
	observer.ObserveJobAdmission(metrics.AdmissionAccepted, time.Since(start))
	observer.JobSubmitted(metrics.SourceSubmit)
	// Confirmed admission wins cancellation during Create. The transport may
	// still lose this response; GetJob observes the durable identity afterward.
	return &runtimepb.SubmitJobResponse{JobId: spec.JobID, Status: "Accepted"}, nil
}

func main() {
	dbPath := flag.String(
		"db",
		"runtime.db",
		"path to coordinator SQLite database",
	)

	metricsAddress := flag.String("metrics-listen", "127.0.0.1:9090", "metrics HTTP listen address (required; loopback by default)")
	execution := coordinatorExecutionFlags(flag.CommandLine)
	security := coordinatorSecurityFlags(flag.CommandLine)
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runCoordinator(ctx, *execution, *security, *dbPath, *metricsAddress); err != nil {
		// Incomplete shutdown must not run deferred resource closure. Unfinished
		// Running records retain the existing crash-recovery semantics on restart.
		log.Print(err)
		os.Exit(1)
	}
}

// runCoordinator owns startup and runtime resources. All exits, including startup
// and Serve failures, use the same shutdown protocol; it never calls os.Exit.
func runCoordinator(ctx context.Context, execution coordinatorExecutionConfig, security coordinatorSecurityConfig, dbPath, metricsAddress string) (runErr error) {
	if err := execution.validate(); err != nil {
		return fmt.Errorf("coordinator execution configuration: %w", err)
	}
	if ctx.Err() != nil {
		return nil
	}
	transport, err := security.load()
	if err != nil {
		return fmt.Errorf("coordinator mTLS configuration: %w", err)
	}
	coordinator := &CoordinatorServer{
		workers: make(map[string]WorkerInfo), jobAttempts: make(map[int64]int64),
		leases: make(map[int64]JobLease), activeJobs: make(map[int64]struct{}),
		leaseDuration: execution.AttemptLease, recoveryReady: make(chan struct{}, 1),
		workerCredentials: transport.workerCredentials,
	}
	process := newCoordinatorProcess(coordinator, execution.ShutdownTimeout)
	defer func() { runErr = errors.Join(runErr, process.shutdown()) }()
	// Startup is an owned producer too: a signal can select the shutdown
	// deadline even if a non-cancelable startup store operation is still blocked.
	startupDone := make(chan struct{})
	process.startupDone = startupDone
	initialized := make(chan error, 1)
	go func() {
		defer close(startupDone)
		initialized <- initializeCoordinator(process, execution, transport, dbPath, metricsAddress)
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-initialized:
		if err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-process.serveResult:
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return errors.New("coordinator gRPC server stopped unexpectedly")
		}
		return fmt.Errorf("coordinator gRPC server: %w", err)
	}
}

func initializeCoordinator(process *coordinatorProcess, execution coordinatorExecutionConfig, transport *coordinatorTransport, dbPath, metricsAddress string) error {
	coordinator := process.service
	ctx := coordinator.lifecycle.ctx
	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := openSQLiteJobStore(dbPath)
	if err != nil {
		return fmt.Errorf("open job store: %w", err)
	}
	process.closeStore = store.Close
	coordinator.jobStore = store
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.checkRetiredJobs(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := store.initSchema(); err != nil {
		return fmt.Errorf("initialize job store: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	registry := prom.NewRegistry()
	observer, err := prommetrics.New(registry)
	if err != nil {
		return fmt.Errorf("initialize metrics: %w", err)
	}
	coordinator.metrics = observer
	metricsServer, err := prommetrics.StartHTTP(metricsAddress, registry)
	if err != nil {
		return fmt.Errorf("start metrics HTTP server: %w", err)
	}
	process.shutdownMetrics = metricsServer.Shutdown
	fmt.Printf("coordinator metrics listening on http://%s/metrics\n", metricsServer.Addr())
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := coordinator.recoverRunningJobs(); err != nil {
		return fmt.Errorf("recover running jobs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := execution.listen()
	if err != nil {
		return fmt.Errorf("coordinator listen: %w", err)
	}
	process.listener = listener
	if err := ctx.Err(); err != nil {
		return err
	}
	process.start(transport.newServer(coordinator), listener)
	fmt.Printf("coordinator mTLS listening on %s\n", listener.Addr())
	return nil
}
