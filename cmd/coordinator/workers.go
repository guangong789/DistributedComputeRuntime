package main

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"fmt"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	"github.com/guangong789/DistributedComputeRuntime/internal/rpcauth"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type WorkerStatus int

const (
	WorkerAlive WorkerStatus = iota
	WorkerDead
)

const workerLivenessTimeout = 3 * time.Second
const workerSessionSecretSize = 32

type workerRegistrationSession struct {
	key, token [workerSessionSecretSize]byte
}

type WorkerInfo struct {
	ID            string
	Address       string
	LastSeen      time.Time
	Status        WorkerStatus
	externalTasks externalTaskSet
}

// A value set keeps endpoint and capability snapshots self-contained.
type externalTaskSet uint8

const (
	externalSquareCapability externalTaskSet = 1 << iota
	cudaVectorCapability
)

func externalTaskCapability(taskType string) externalTaskSet {
	switch taskType {
	case externalsquare.TaskType:
		return externalSquareCapability
	case cudavector.TaskType:
		return cudaVectorCapability
	default:
		return 0
	}
}

func (w WorkerInfo) eligibleFor(taskType string) bool {
	required := externalTaskCapability(taskType)
	// Built-ins require no advertisement. Other task names still use the
	// Worker's existing execution-time validation; unknown ads grant nothing.
	return w.Status == WorkerAlive && (required == 0 || w.externalTasks&required != 0)
}

func (s *CoordinatorServer) RegisterWorker(
	ctx context.Context,
	req *runtimepb.RegisterWorkerRequest,
) (*runtimepb.RegisterWorkerResponse, error) {

	if req.WorkerId == "" || req.Address == "" {
		return &runtimepb.RegisterWorkerResponse{
			Accepted: false,
			Error:    "worker_id and address are required",
		}, nil
	}
	if _, err := rpcauth.ParseTCPEndpoint(req.Address); err != nil {
		return &runtimepb.RegisterWorkerResponse{Accepted: false, Error: "invalid Worker TCP endpoint"}, nil
	}

	key, token := req.GetRegistrationKey(), req.GetSessionToken()
	if (len(key) != 0 && len(key) != workerSessionSecretSize) ||
		(len(token) != 0 && (len(key) == 0 || len(token) != workerSessionSecretSize)) {
		return nil, status.Error(codes.InvalidArgument, "invalid Worker registration proof")
	}

	var externalTasks externalTaskSet
	for _, taskType := range req.GetExternalTaskTypes() {
		externalTasks |= externalTaskCapability(taskType)
	}

	s.mu.Lock()

	if s.workers == nil {
		s.workers = make(map[string]WorkerInfo)
	}

	previous, exists := s.workers[req.WorkerId]
	session, retry, err := s.claimWorkerRegistrationLocked(previous, exists, req, externalTasks)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if retry {
		s.mu.Unlock()
		return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: session}, nil
	}

	s.workers[req.WorkerId] = WorkerInfo{
		ID:            req.WorkerId,
		Address:       req.Address,
		LastSeen:      time.Now(),
		Status:        WorkerAlive,
		externalTasks: externalTasks,
	}

	if !exists {
		s.workerOrder = append(s.workerOrder, req.WorkerId)
	}

	s.mu.Unlock()

	observer := metrics.Resolve(s.metrics)
	if !exists {
		observer.WorkerStateChanged(metrics.WorkerUnregistered, metrics.WorkerAlive, metrics.WorkerRegistration)
	} else if previous.Status == WorkerDead {
		observer.WorkerStateChanged(metrics.WorkerDead, metrics.WorkerAlive, metrics.WorkerRegistration)
	}

	s.notifyRecoveryReady()

	fmt.Printf(
		"registered worker: id=%s address=%s\n",
		req.WorkerId,
		req.Address,
	)

	return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: session}, nil
}

// The caller publishes the endpoint and capabilities under the same lock.
func (s *CoordinatorServer) claimWorkerRegistrationLocked(previous WorkerInfo, exists bool, req *runtimepb.RegisterWorkerRequest, tasks externalTaskSet) (token []byte, retry bool, err error) {
	key, priorToken := req.GetRegistrationKey(), req.GetSessionToken()
	current, owned := s.workerSessions[req.WorkerId]
	if len(key) == 0 {
		if owned {
			return nil, false, status.Error(codes.FailedPrecondition, "Worker registration requires a session")
		}
		return nil, false, nil
	}
	if owned && subtle.ConstantTimeCompare(current.key[:], key) == 1 {
		// A reply can be lost after publication, including during reconnect.
		// Repeating a claim only returns its token, without renewing liveness.
		if previous.Address != req.Address || previous.externalTasks != tasks {
			return nil, false, status.Error(codes.InvalidArgument, "Worker registration changed within a session")
		}
		return current.token[:], true, nil
	}
	if exists && len(priorToken) != 0 {
		return nil, false, status.Error(codes.FailedPrecondition, "Worker registration session revoked")
	}
	if exists && time.Since(previous.LastSeen) <= workerLivenessTimeout {
		return nil, false, status.Error(codes.AlreadyExists, "Worker registration is live")
	}
	var next workerRegistrationSession
	copy(next.key[:], key)
	if _, err := rand.Read(next.token[:]); err != nil {
		return nil, false, status.Error(codes.Internal, "cannot create Worker registration session")
	}
	if s.workerSessions == nil {
		s.workerSessions = make(map[string]workerRegistrationSession)
	}
	s.workerSessions[req.WorkerId] = next
	return next.token[:], false, nil
}

// hasEligibleWorker is a non-reserving precheck and leaves the cursor alone.
func (s *CoordinatorServer) hasEligibleWorker(taskType string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range s.workerOrder {
		if worker, ok := s.workers[id]; ok && worker.eligibleFor(taskType) {
			return true
		}
	}
	return false
}

func (s *CoordinatorServer) getNextWorker(taskType string) (WorkerInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.workerOrder) == 0 {
		return WorkerInfo{}, false
	}

	for i := 0; i < len(s.workerOrder); i++ {
		workerID := s.workerOrder[s.nextWorker]

		s.nextWorker = (s.nextWorker + 1) % len(s.workerOrder)

		worker, ok := s.workers[workerID]
		if !ok {
			continue
		}

		if !worker.eligibleFor(taskType) {
			continue
		}

		return worker, true
	}

	return WorkerInfo{}, false
}

func (s *CoordinatorServer) Heartbeat(
	ctx context.Context,
	req *runtimepb.HeartbeatRequest,
) (*runtimepb.HeartbeatResponse, error) {
	s.mu.Lock()

	worker, ok := s.workers[req.WorkerId]
	if !ok {
		s.mu.Unlock()
		return &runtimepb.HeartbeatResponse{
			Accepted: false,
		}, nil
	}

	session, owned := s.workerSessions[req.WorkerId]
	if (owned && subtle.ConstantTimeCompare(session.token[:], req.GetSessionToken()) != 1) ||
		(!owned && len(req.GetSessionToken()) != 0) {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "Worker registration session revoked")
	}
	previousState := worker.Status
	worker.LastSeen = time.Now()
	worker.Status = WorkerAlive
	s.workers[req.WorkerId] = worker
	s.mu.Unlock()
	if previousState == WorkerDead {
		metrics.Resolve(s.metrics).WorkerStateChanged(metrics.WorkerDead, metrics.WorkerAlive, metrics.WorkerHeartbeat)
		s.notifyRecoveryReady()
	}

	return &runtimepb.HeartbeatResponse{
		Accepted: true,
	}, nil
}

// notifyRecoveryReady runs after registry publication and outside s.mu.
// A pending notification coalesces another wake-up; nil disables notification.
// Registration and heartbeat never wait for a recovery pass to finish.
func (s *CoordinatorServer) notifyRecoveryReady() {
	select {
	case s.recoveryReady <- struct{}{}:
	default:
	}
}

func (s *CoordinatorServer) checkWorkerLiveness(
	timeout time.Duration,
) {
	now := time.Now()

	s.mu.Lock()
	transitions := 0

	for id, worker := range s.workers {
		if now.Sub(worker.LastSeen) > timeout {
			if worker.Status != WorkerDead {
				transitions++
				fmt.Printf(
					"worker timed out: id=%s last_seen=%v\n",
					worker.ID,
					worker.LastSeen,
				)
			}

			worker.Status = WorkerDead
			s.workers[id] = worker
		}
	}
	s.mu.Unlock()
	observer := metrics.Resolve(s.metrics)
	for i := 0; i < transitions; i++ {
		observer.WorkerStateChanged(metrics.WorkerAlive, metrics.WorkerDead, metrics.WorkerTimeout)
	}
}

func (s *CoordinatorServer) runLivenessMonitor(
	ctx context.Context,
	checkInterval time.Duration,
	workerTimeout time.Duration,
) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if ctx.Err() != nil {
				return
			}
			s.checkWorkerLiveness(workerTimeout)
		}
	}
}
