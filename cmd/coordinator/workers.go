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
	cudaHealth    cudaHealthEvidence
}

type cudaHealthEvidence struct {
	managed   bool
	origin    time.Time
	sequence  uint64
	state     runtimepb.CudaHealthState
	expiresAt time.Time
}

// Anchors encode elapsed nanoseconds plus one to reserve zero. Since Sub
// returns a signed duration, the offset fits in uint64 even at saturation.
func (h cudaHealthEvidence) anchor(now time.Time) uint64 {
	elapsed := now.Sub(h.origin)
	if elapsed < 0 {
		return 0
	}
	return uint64(elapsed) + 1
}

func (h *cudaHealthEvidence) observe(observation *runtimepb.CudaHealthObservation, now time.Time, currentAnchor uint64, ttl time.Duration) {
	sequence := observation.GetObservationSeq()
	if sequence <= h.sequence {
		return
	}
	state := observation.GetState()
	anchor := observation.GetProbeAnchor()
	if (state != runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY &&
		state != runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE) ||
		anchor == 0 || anchor > currentAnchor {
		return
	}
	// The range check bounds the offset to a signed duration. Add the TTL
	// separately to avoid overflowing when summing two durations.
	expiresAt := h.origin.Add(time.Duration(anchor - 1)).Add(ttl)
	if !now.Before(expiresAt) {
		return
	}
	h.sequence, h.state, h.expiresAt = sequence, state, expiresAt
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

func (w WorkerInfo) configuredFor(taskType string) bool {
	required := externalTaskCapability(taskType)
	// Built-ins require no advertisement. Other task names still use the
	// Worker's existing execution-time validation; unknown ads grant nothing.
	return required == 0 || w.externalTasks&required != 0
}

func (h cudaHealthEvidence) freshAt(now time.Time) bool {
	return now.Before(h.expiresAt)
}

func (w WorkerInfo) eligibleFor(taskType string, now time.Time) bool {
	if w.Status != WorkerAlive || !w.configuredFor(taskType) {
		return false
	}
	if taskType != cudavector.TaskType || !w.cudaHealth.managed {
		return true
	}
	return w.cudaHealth.state == runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY &&
		w.cudaHealth.freshAt(now)
}

// workerRegistryNow is sampled under mu for both heartbeat and placement.
func (s *CoordinatorServer) workerRegistryNow() time.Time {
	if s.workerNow != nil {
		return s.workerNow()
	}
	return time.Now()
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

	managed := req.GetCudaHealthManaged()
	if managed {
		if len(key) == 0 || externalTasks&cudaVectorCapability != 0 {
			return nil, status.Error(codes.InvalidArgument, "managed CUDA health requires a session and no ordinary CUDA advertisement")
		}
		externalTasks |= cudaVectorCapability
	}

	s.mu.Lock()
	if managed && s.healthTTL <= 0 {
		s.mu.Unlock()
		return nil, status.Error(codes.Unimplemented, "managed CUDA health is not configured")
	}
	now := s.workerRegistryNow()

	if s.workers == nil {
		s.workers = make(map[string]WorkerInfo)
	}

	previous, exists := s.workers[req.WorkerId]
	session, retry, err := s.claimWorkerRegistrationLocked(previous, exists, req, externalTasks, now)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if retry {
		s.mu.Unlock()
		return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: session}, nil
	}

	worker := WorkerInfo{
		ID:            req.WorkerId,
		Address:       req.Address,
		LastSeen:      now,
		Status:        WorkerAlive,
		externalTasks: externalTasks,
	}
	if managed {
		worker.cudaHealth = cudaHealthEvidence{managed: true, origin: now}
	}
	s.workers[req.WorkerId] = worker

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
func (s *CoordinatorServer) claimWorkerRegistrationLocked(previous WorkerInfo, exists bool, req *runtimepb.RegisterWorkerRequest, tasks externalTaskSet, now time.Time) (token []byte, retry bool, err error) {
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
		if previous.Address != req.Address || previous.externalTasks != tasks ||
			previous.cudaHealth.managed != req.GetCudaHealthManaged() {
			return nil, false, status.Error(codes.InvalidArgument, "Worker registration changed within a session")
		}
		return current.token[:], true, nil
	}
	if exists && len(priorToken) != 0 {
		return nil, false, status.Error(codes.FailedPrecondition, "Worker registration session revoked")
	}
	if exists && now.Sub(previous.LastSeen) <= workerLivenessTimeout {
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
	now := s.workerRegistryNow()
	for _, id := range s.workerOrder {
		if worker, ok := s.workers[id]; ok && worker.eligibleFor(taskType, now) {
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

	now := s.workerRegistryNow()
	for i := 0; i < len(s.workerOrder); i++ {
		workerID := s.workerOrder[s.nextWorker]

		s.nextWorker = (s.nextWorker + 1) % len(s.workerOrder)

		worker, ok := s.workers[workerID]
		if !ok {
			continue
		}

		if !worker.eligibleFor(taskType, now) {
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
	now := s.workerRegistryNow()
	previousState := worker.Status
	managed := worker.cudaHealth.managed
	wasCUDAEligible := managed && worker.eligibleFor(cudavector.TaskType, now)
	var anchor uint64
	if managed {
		anchor = worker.cudaHealth.anchor(now)
		worker.cudaHealth.observe(req.GetCudaHealth(), now, anchor, s.healthTTL)
	}
	worker.LastSeen = now
	worker.Status = WorkerAlive
	s.workers[req.WorkerId] = worker
	s.mu.Unlock()
	if previousState == WorkerDead {
		metrics.Resolve(s.metrics).WorkerStateChanged(metrics.WorkerDead, metrics.WorkerAlive, metrics.WorkerHeartbeat)
	}
	if previousState == WorkerDead || (managed && !wasCUDAEligible && worker.eligibleFor(cudavector.TaskType, now)) {
		s.notifyRecoveryReady()
	}

	return &runtimepb.HeartbeatResponse{Accepted: true, CudaHealthProbeAnchor: anchor}, nil
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
	s.mu.Lock()
	now := s.workerRegistryNow()
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
