package workerheartbeat

import (
	"math"
	"sync"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
)

// cudaHealth protects the observation handed off from a check to the
// heartbeat sender. The sender alone manages registration RPCs and tokens.
type cudaHealth struct {
	mu     sync.Mutex
	anchor uint64
	check  *CUDAHealthCheck
	latest *runtimepb.CudaHealthObservation
}

func (h *cudaHealth) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.anchor, h.check, h.latest = 0, nil, nil
}

func (h *cudaHealth) setAnchor(anchor uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.anchor = anchor
	if anchor == 0 {
		h.check = nil
	}
}

func (h *cudaHealth) observation() *runtimepb.CudaHealthObservation {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.anchor == 0 {
		return nil
	}
	return h.latest // Immutable once published.
}

// CUDAHealthCheck binds one in-flight check to the anchor available at start.
type CUDAHealthCheck struct {
	health *cudaHealth
	anchor uint64
}

// BeginCUDAHealthCheck returns nil until a supported managed session supplies
// an anchor, while a check is in flight, or once the sequence is exhausted.
func (r *Registration) BeginCUDAHealthCheck() *CUDAHealthCheck {
	h := &r.cuda
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.anchor == 0 || h.check != nil || h.latest.GetObservationSeq() == math.MaxUint64 {
		return nil
	}
	h.check = &CUDAHealthCheck{health: h, anchor: h.anchor}
	return h.check
}

// Finish publishes a completed result once. Use UNSPECIFIED to release a
// check with no evidence (for example, capacity contention or cancellation).
// It returns true only when the current registration accepted the observation.
func (c *CUDAHealthCheck) Finish(state runtimepb.CudaHealthState) bool {
	h := c.health
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.check != c {
		return false
	}
	h.check = nil
	if state != runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY &&
		state != runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE {
		return false
	}
	h.latest = &runtimepb.CudaHealthObservation{
		ObservationSeq: h.latest.GetObservationSeq() + 1,
		State:          state,
		ProbeAnchor:    c.anchor,
	}
	return true
}
