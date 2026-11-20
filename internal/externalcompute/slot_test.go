package externalcompute

import (
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
)

type slotHoldRecorder struct {
	metrics.NoopMetrics
	holds  []time.Duration
	owners []metrics.ComputeOwner
}

func (r *slotHoldRecorder) ObserveSlotHold(owner metrics.ComputeOwner, duration time.Duration) {
	r.holds = append(r.holds, duration)
	r.owners = append(r.owners, owner)
}

func TestSlotSnapshotAndDenialAttribution(t *testing.T) {
	recorder := &slotHoldRecorder{}
	slot := NewObservedSlot(recorder)
	if idle := slot.Snapshot(); idle.Occupied || !idle.AcquiredAt.IsZero() || idle.HoldAge != 0 || idle.LastHold != 0 {
		t.Fatalf("initial snapshot: %+v", idle)
	}
	for i, owner := range []metrics.ComputeOwner{metrics.ComputeProbe, metrics.ComputeJob} {
		requester := !owner
		if !slot.tryAcquire(owner) {
			t.Fatalf("idle slot denied %s", owner)
		}
		held := slot.Snapshot()
		if !held.Occupied || held.Owner != owner || held.AcquiredAt.IsZero() || held.HoldAge < 0 {
			t.Fatalf("active snapshot: %+v", held)
		}
		if slot.tryAcquire(requester) {
			t.Fatal("second contender acquired the slot")
		}
		slot.release()
		idle := slot.Snapshot()
		if idle.Occupied || !idle.AcquiredAt.IsZero() || idle.HoldAge != 0 || idle.Denials(requester, owner) != 1 {
			t.Fatalf("released snapshot: %+v", idle)
		}
		if len(recorder.holds) != i+1 || recorder.owners[i] != owner || recorder.holds[i] != idle.LastHold || idle.LastHold < held.HoldAge {
			t.Fatalf("invalid hold accounting for %s: %+v", owner, recorder.holds)
		}
	}
	final := slot.Snapshot()
	if final.Denials(metrics.ComputeJob, metrics.ComputeProbe) != 1 || final.Denials(metrics.ComputeProbe, metrics.ComputeJob) != 1 ||
		final.Denials(metrics.ComputeJob, metrics.ComputeJob) != 0 || final.Denials(metrics.ComputeProbe, metrics.ComputeProbe) != 0 {
		t.Fatalf("holder attribution changed after release and reacquisition: %+v", final)
	}
}

func TestSlotConcurrentAdmissionAndSnapshot(t *testing.T) {
	const perOwner = 16
	slot := NewSlot()
	start := make(chan struct{})
	type admission struct {
		owner    metrics.ComputeOwner
		acquired bool
	}
	attempts := make(chan admission, 2*perOwner)
	for _, owner := range []metrics.ComputeOwner{metrics.ComputeJob, metrics.ComputeProbe} {
		for range perOwner {
			go func() {
				<-start
				attempts <- admission{owner, slot.tryAcquire(owner)}
			}()
		}
	}
	sampled := make(chan bool, 1)
	go func() {
		<-start
		for range 4 * perOwner {
			snapshot := slot.Snapshot()
			if snapshot.Occupied == snapshot.AcquiredAt.IsZero() || snapshot.HoldAge < 0 {
				sampled <- false
				return
			}
		}
		sampled <- true
	}()
	close(start)
	var winner metrics.ComputeOwner
	acquired := 0
	for range 2 * perOwner {
		if attempt := <-attempts; attempt.acquired {
			winner = attempt.owner
			acquired++
		}
	}
	if acquired != 1 || !slot.Snapshot().Occupied {
		t.Fatalf("successful contenders: %d", acquired)
	}
	slot.release()
	if !<-sampled {
		t.Fatal("incoherent concurrent snapshot")
	}
	final := slot.Snapshot()
	if final.Occupied || final.Denials(winner, winner) != perOwner-1 || final.Denials(!winner, winner) != perOwner ||
		final.Denials(winner, !winner) != 0 || final.Denials(!winner, !winner) != 0 {
		t.Fatalf("unexpected final slot: %+v", final)
	}
}
