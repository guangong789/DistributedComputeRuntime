package main

import (
	"context"
	"errors"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerBuildOrdinaryTasks(t *testing.T) {
	for _, tc := range []struct {
		name, kind, payload, output, buildError, executeError string
		cancel                                                bool
	}{
		{name: "hash", kind: "hash", payload: "hello", output: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		{name: "empty hash fails at execution", kind: "hash", executeError: "hash input cannot be empty"},
		{name: "sleep", kind: "sleep", payload: "0s", output: "slept for 0s"},
		{name: "negative sleep", kind: "sleep", payload: "-1s", output: "slept for -1s"},
		{name: "cancel sleep", kind: "sleep", payload: "1h", cancel: true, executeError: context.Canceled.Error()},
		{name: "invalid sleep", kind: "sleep", payload: "invalid", buildError: `time: invalid duration "invalid"`},
		{name: "unknown", kind: "unknown", buildError: "unknown task type: unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Ordinary tasks must work without any ZK dependencies.
			task, err := (&WorkerService{}).buildTask(tc.kind, tc.payload)
			if tc.buildError != "" {
				if task != nil || errorString(err) != tc.buildError {
					t.Fatalf("task=%#v error=%v, want %q", task, err, tc.buildError)
				}
				return
			}
			if err != nil || task == nil {
				t.Fatalf("build task: task=%#v error=%v", task, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if tc.cancel {
				cancel()
			}
			output, err := task.Execute(ctx)
			if output != tc.output || errorString(err) != tc.executeError {
				t.Fatalf("output=%q error=%v, want output=%q error=%q", output, err, tc.output, tc.executeError)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation identity lost: %v", err)
			}
		})
	}
}

func TestWorkerBuildZKTaskInjectsDependencies(t *testing.T) {
	limiter := newTestProvingLimiter(t)
	if !limiter.TryAcquire() {
		t.Fatal("cannot occupy proving slot")
	}
	defer limiter.Release()
	observer := &workerProvingMetrics{durations: make(chan time.Duration, 1)}
	worker := &WorkerService{preimageProver: &zk.PreimageProver{}, witnessStore: runtime.NewMemoryWitnessStore(), provingLimiter: limiter, metrics: observer}
	const payload = `{"witness_ref":"missing","digest":"123"}`
	task, err := worker.buildTask("zk_preimage_prove", payload)
	if err != nil {
		t.Fatal(err)
	}
	preimage, ok := task.(runtime.ZKPreimageTask)
	if !ok || preimage.Payload != payload || preimage.Prover != worker.preimageProver ||
		preimage.WitnessStore != worker.witnessStore || preimage.Limiter != limiter || preimage.Metrics != observer {
		t.Fatalf("incomplete ZK dependency injection: %#v", task)
	}
	// Construction does not acquire or release capacity. Missing witnesses still
	// fail before admission, even when proving capacity is full.
	output, err := task.Execute(context.Background())
	if output != "" || errorString(err) != "witness not found" || len(observer.durations) != 0 {
		t.Fatalf("output=%q error=%v proving samples=%d", output, err, len(observer.durations))
	}
	if limiter.TryAcquire() {
		// Keep the replacement slot for the deferred release on this failure path.
		t.Fatal("construction or validation released another task's slot")
	}
}

func TestWorkerConstructionErrorPrecedence(t *testing.T) {
	prover := &zk.PreimageProver{}
	for _, tc := range []struct {
		name    string
		worker  WorkerService
		code    codes.Code
		message string
	}{
		{name: "prover before store and limiter", message: "preimage prover is not configured"},
		{name: "store before limiter", worker: WorkerService{preimageProver: prover}, message: "witness store is not configured"},
		{name: "limiter before payload validation", worker: WorkerService{preimageProver: prover, witnessStore: runtime.NewMemoryWitnessStore()}, code: codes.FailedPrecondition, message: "proving limiter is not configured"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := tc.worker.ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{
				JobId: 501, AttemptId: 9, TaskType: "zk_preimage_prove", Payload: "{", TimeoutMs: 1000,
			})
			if tc.code != codes.OK {
				if resp != nil || status.Code(err) != tc.code || status.Convert(err).Message() != tc.message {
					t.Fatalf("response=%v error=%v", resp, err)
				}
				return
			}
			if err != nil || resp == nil || resp.JobId != 501 || resp.AttemptId != 9 ||
				resp.Status != "failed" || resp.Output != "" || resp.Error != tc.message {
				t.Fatalf("response=%v error=%v", resp, err)
			}
		})
	}
}
