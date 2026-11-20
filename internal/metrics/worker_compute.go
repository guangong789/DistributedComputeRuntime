package metrics

import "time"

// ComputeOwner identifies the caller of a shared external-compute operation.
type ComputeOwner bool

const (
	ComputeJob   ComputeOwner = false
	ComputeProbe ComputeOwner = true
)

func (owner ComputeOwner) String() string {
	if owner == ComputeProbe {
		return "probe"
	}
	return "job"
}

type ProbeOutcome string

const (
	ProbeReady         ProbeOutcome = "ready"
	ProbeUnavailable   ProbeOutcome = "unavailable"
	ProbeNoObservation ProbeOutcome = "no_observation"
)

// WorkerCompute observes completed operations. Callbacks must be concurrent,
// fast and non-blocking. Occupancy and denial counts belong to the slot.
type WorkerCompute interface {
	ObserveSlotHold(ComputeOwner, time.Duration)
	CUDAProbeFinished(ProbeOutcome)
}

func (NoopMetrics) ObserveSlotHold(ComputeOwner, time.Duration) {}
func (NoopMetrics) CUDAProbeFinished(ProbeOutcome)              {}
