package externalcompute

import (
	"sync"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
)

// Slot is the single external-compute permit shared by a Worker's programs.
// mu keeps the permit, owner, and admission observations in the same transition.
type Slot struct {
	mu       sync.Mutex
	held     chan struct{}
	state    SlotSnapshot
	observer metrics.WorkerCompute
}

// SlotSnapshot is one coherent local sample. Owner, AcquiredAt and HoldAge
// describe the holder only when Occupied is true. LastHold covers the most
// recent completed hold, including the runner's wait and verification.
type SlotSnapshot struct {
	Occupied   bool
	Owner      metrics.ComputeOwner
	AcquiredAt time.Time
	HoldAge    time.Duration
	LastHold   time.Duration
	denials    [2][2]uint64
}

func ownerIndex(owner metrics.ComputeOwner) int {
	if owner == metrics.ComputeProbe {
		return 1
	}
	return 0
}

// Denials returns cumulative failures for the requester and holder at the
// admission decision. Counts reset when a Worker creates a new slot.
func (s SlotSnapshot) Denials(requester, holder metrics.ComputeOwner) uint64 {
	return s.denials[ownerIndex(requester)][ownerIndex(holder)]
}

func NewSlot() *Slot {
	return NewObservedSlot(metrics.NoopMetrics{})
}

func NewObservedSlot(observer metrics.WorkerCompute) *Slot {
	return &Slot{held: make(chan struct{}, 1), observer: observer}
}

func (s *Slot) Snapshot() SlotSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := s.state
	if snapshot.Occupied {
		snapshot.HoldAge = time.Since(snapshot.AcquiredAt)
	}
	return snapshot
}

func (s *Slot) tryAcquire(owner metrics.ComputeOwner) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case s.held <- struct{}{}:
		s.state.Occupied = true
		s.state.Owner = owner
		s.state.AcquiredAt = time.Now()
		return true
	default:
		if s.state.Occupied {
			s.state.denials[ownerIndex(owner)][ownerIndex(s.state.Owner)]++
		}
		return false
	}
}

func (s *Slot) release() {
	s.mu.Lock()
	<-s.held
	owner := s.state.Owner
	held := time.Since(s.state.AcquiredAt)
	s.state.LastHold = held
	s.state.Occupied = false
	s.state.Owner = metrics.ComputeJob
	s.state.AcquiredAt = time.Time{}
	s.mu.Unlock()
	if s.observer != nil {
		s.observer.ObserveSlotHold(owner, held)
	}
}
