package prometheus

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/externalcompute"
	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
)

var computeOwners = [...]metrics.ComputeOwner{metrics.ComputeJob, metrics.ComputeProbe}
var probeOutcomes = [...]metrics.ProbeOutcome{metrics.ProbeReady, metrics.ProbeUnavailable, metrics.ProbeNoObservation}

type workerCompute struct {
	slot      *externalcompute.Slot
	occupied  *prom.Desc
	holdAge   *prom.Desc
	denials   *prom.Desc
	lastProbe *prom.Desc
	last      atomic.Uint32
	hold      *prom.HistogramVec
	probes    *prom.CounterVec
}

// NewWorkerSlot creates the Worker-local slot and registers its observations
// with the supplied Worker registry before the slot can serve work.
func NewWorkerSlot(registerer prom.Registerer) (*externalcompute.Slot, error) {
	desc := func(name, help string, labels ...string) *prom.Desc {
		return prom.NewDesc("dzkr_"+name, help, labels, nil)
	}
	m := &workerCompute{
		occupied:  desc("compute_slot_occupied", "Whether this owner category currently holds the Worker external-compute slot.", "owner"),
		holdAge:   desc("compute_slot_hold_age_seconds", "Elapsed time in the current slot hold, or zero for a category without the slot.", "owner"),
		denials:   desc("compute_slot_denials_total", "Worker slot admission denials by requester and holder at the decision.", "requester", "holder"),
		lastProbe: desc("cuda_probe_last_outcome", "One for the last local probe outcome, zero for other outcomes or if none has completed.", "outcome"),
		hold: prom.NewHistogramVec(prom.HistogramOpts{
			Namespace: "dzkr", Name: "compute_slot_hold_duration_seconds",
			Help:    "Completed Worker slot holds including child wait, cleanup and result verification.",
			Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"owner"}),
		probes: prom.NewCounterVec(prom.CounterOpts{
			Namespace: "dzkr", Name: "cuda_probe_outcomes_total", Help: "Completed local CUDA probe calls by outcome.",
		}, []string{"outcome"}),
	}
	m.slot = externalcompute.NewObservedSlot(m)
	for _, owner := range computeOwners {
		m.hold.WithLabelValues(owner.String())
	}
	for _, outcome := range probeOutcomes {
		m.probes.WithLabelValues(string(outcome))
	}
	if err := registerer.Register(m); err != nil {
		return nil, fmt.Errorf("register Worker compute metrics: %w", err)
	}
	return m.slot, nil
}

func (m *workerCompute) ObserveSlotHold(owner metrics.ComputeOwner, duration time.Duration) {
	m.hold.WithLabelValues(owner.String()).Observe(duration.Seconds())
}

func (m *workerCompute) CUDAProbeFinished(outcome metrics.ProbeOutcome) {
	for i, candidate := range probeOutcomes {
		if candidate == outcome {
			m.probes.WithLabelValues(string(outcome)).Inc()
			m.last.Store(uint32(i + 1))
			return
		}
	}
}

func (m *workerCompute) Describe(ch chan<- *prom.Desc) {
	ch <- m.occupied
	ch <- m.holdAge
	ch <- m.denials
	ch <- m.lastProbe
	m.hold.Describe(ch)
	m.probes.Describe(ch)
}

func (m *workerCompute) Collect(ch chan<- prom.Metric) {
	snapshot := m.slot.Snapshot()
	for _, owner := range computeOwners {
		label := owner.String()
		var occupied, age float64
		if snapshot.Occupied && snapshot.Owner == owner {
			occupied, age = 1, snapshot.HoldAge.Seconds()
		}
		ch <- prom.MustNewConstMetric(m.occupied, prom.GaugeValue, occupied, label)
		ch <- prom.MustNewConstMetric(m.holdAge, prom.GaugeValue, age, label)
		for _, holder := range computeOwners {
			ch <- prom.MustNewConstMetric(m.denials, prom.CounterValue, float64(snapshot.Denials(owner, holder)), label, holder.String())
		}
	}
	last := m.last.Load()
	for i, outcome := range probeOutcomes {
		var value float64
		if last == uint32(i+1) {
			value = 1
		}
		ch <- prom.MustNewConstMetric(m.lastProbe, prom.GaugeValue, value, string(outcome))
	}
	m.hold.Collect(ch)
	m.probes.Collect(ch)
}
