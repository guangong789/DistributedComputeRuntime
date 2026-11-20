package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudahealth"
	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalcompute"
	"github.com/guangong789/DistributedComputeRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedComputeRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
)

// This opt-in covers the real CUDA program and placement contract. Normal Go
// profiles use deterministic substitutes and never access a GPU.
func TestRealCUDAHealthProbePlacement(t *testing.T) {
	path := os.Getenv("DCR_TEST_CUDA_PROBE_EXECUTABLE")
	if path == "" {
		t.Skip("set DCR_TEST_CUDA_PROBE_EXECUTABLE to the built project CUDA vector executable")
	}
	program, err := externalcompute.NewProgram(cudavector.TaskType, path, externalcompute.NewSlot())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	f, clock := healthCoordinator(t)
	var inputs, outputs [64]string
	for i := range inputs {
		inputs[i] = strconv.Itoa(i)
		outputs[i] = strconv.Itoa(i*i + 3*i + 11)
	}
	input, expected := strings.Join(inputs[:], ","), strings.Join(outputs[:], ",")
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if req.TaskType == "hash" {
			return executeHashJob(ctx, req)
		}
		task, err := cudavector.NewTask(program, req.Payload)
		if err != nil {
			return nil, err
		}
		output, err := task.Execute(ctx)
		if err != nil {
			return nil, err
		}
		resp := successfulResult(req)
		resp.Output = output
		return resp, nil
	})
	workerClient := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Worker))
	claim := managedCUDAClaim(address, 1)
	claim.ExternalTaskTypes = nil
	reg := workerheartbeat.NewRegistration(claim)
	if err := reg.Register(ctx, workerClient); err != nil {
		t.Fatal(err)
	}
	origin := clock.now()
	if f.s.hasEligibleWorker(cudavector.TaskType) {
		t.Fatal("managed CUDA eligible before any probe")
	}
	client := f.observingClient(t, testpki.Client)
	ack, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 31, TaskType: cudavector.TaskType, Payload: input, TimeoutMs: 3000})
	if err != nil || ack.GetStatus() != "Accepted" || ack.GetAttemptId() != 0 {
		t.Fatalf("CUDA admission: %v, %v", ack, err)
	}
	const hashInput = "cpu during unknown CUDA health"
	cpu, err := submitAndObserveForTest(ctx, client, &runtimepb.SubmitJobRequest{JobId: 32, TaskType: "hash", Payload: hashInput, TimeoutMs: 1000})
	if err != nil || cpu.GetStatus() != "Succeeded" || cpu.GetOutput() != fmt.Sprintf("%x", sha256.Sum256([]byte(hashInput))) {
		t.Fatalf("CPU execution while CUDA health was unknown: %v, %v", cpu, err)
	}
	queued, err := client.GetJob(ctx, &runtimepb.GetJobRequest{JobId: 31})
	if err != nil || queued.GetState() != runtimepb.JobState_JOB_STATE_QUEUED || queued.GetAttemptId() != 0 {
		t.Fatalf("CUDA work did not wait for evidence: %v, %v", queued, err)
	}
	clock.advance(time.Second)
	if err := reg.Send(ctx, workerClient); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	result, err := cudahealth.Probe(ctx, reg, program)
	if err != nil || result.State != healthReady || result.Output != expected {
		t.Fatalf("real CUDA probe did not verify its expected output: state=%s, err=%v", result.State, err)
	}
	t.Logf("real CUDA probe verified 64 outputs, elapsed=%s output_sha256=%x", time.Since(start), sha256.Sum256([]byte(result.Output+"\n")))
	if f.s.hasEligibleWorker(cudavector.TaskType) {
		t.Fatal("probe bypassed the authorized heartbeat boundary")
	}
	clock.advance(time.Second) // Delayed delivery must use the first heartbeat's anchor.
	if err := reg.Send(ctx, workerClient); err != nil {
		t.Fatal(err)
	}
	w := f.snapshot().workers[sessionWorkerID]
	if !f.s.hasEligibleWorker(cudavector.TaskType) || w.cudaHealth.sequence != 1 ||
		w.cudaHealth.state != healthReady || !w.cudaHealth.expiresAt.Equal(origin.Add(time.Second+testHealthTTL)) {
		t.Fatal("Coordinator did not accept session-bound Ready at the original anchor")
	}
	t.Logf("Coordinator accepted Ready sequence=%d anchor_offset=%s, CUDA eligible", w.cudaHealth.sequence, time.Second)
	got, err := observeTerminalForTest(ctx, client, 31)
	if err != nil || got.GetStatus() != "Succeeded" || got.GetAttemptId() != 1 || got.GetOutput() != expected {
		t.Fatalf("queued CUDA job did not execute on the released slot: %v, %v", got, err)
	}
	t.Log("CPU hash succeeded while managed CUDA was Unknown; queued CUDA Attempt 1 completed on the same program and slot after the probe")
}
