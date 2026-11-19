package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/cudavector"
	"github.com/guangong789/DistributedZKRuntime/internal/externalcompute"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const cudaVectorFixtureName = "cuda-vector-fixture"
const cudaPrivateMarker = "private-test-cuda-stderr-marker"

func cudaZeroOutput() string {
	values := make([]string, 64)
	for i, expected := 0, 11; i < len(values); i, expected = i+1, expected+3 {
		values[i] = strconv.Itoa(expected)
	}
	return strings.Join(values, ",")
}

func cudaVectorFixture() int {
	if env := os.Environ(); len(env) != 1 || env[0] != "LC_ALL=C" {
		return 90
	}
	input, err := io.ReadAll(io.LimitReader(os.Stdin, 385))
	first, remainder, found := strings.Cut(string(input), ",")
	if err != nil || !found || remainder != "0"+strings.Repeat(",0", 62)+"\n" {
		return 91
	}
	switch first {
	case "0":
		fmt.Fprintln(os.Stdout, cudaZeroOutput())
	case "1":
		return 7
	case "2":
		fmt.Fprintln(os.Stdout, "malformed")
	case "3":
		// Correct shape, incorrect first value (20 would be correct).
		fmt.Fprintln(os.Stdout, "19"+cudaZeroOutput()[2:])
	case "4":
		fmt.Fprint(os.Stdout, strings.Repeat("x", 1025))
	case "5":
		fmt.Fprintln(os.Stdout, "36"+cudaZeroOutput()[2:])
		fmt.Fprint(os.Stderr, strings.Repeat("x", 1025))
	case "6":
		return holdComputeFixture()
	case "7":
		fmt.Fprintln(os.Stderr, "cudaGetDeviceCount=cudaErrorNoDevice(100)")
		return 7
	case "8":
		fmt.Fprintf(os.Stderr, "device=0 name=%s cc=8.6 uuid=12345678-1234-1234-1234-123456789abc\n", cudaPrivateMarker)
		fmt.Fprint(os.Stderr, "cudaMalloc=cudaSuccess(0)\nkernel_launch=cudaErrorInvalidConfiguration(9)\ncudaDeviceReset=cudaSuccess(0)\n")
		return 7
	case "9":
		fmt.Fprintf(os.Stderr, "kernel_launch=cudaErrorInvalidConfiguration(%s)\n", cudaPrivateMarker)
		return 7
	case "10":
		fmt.Fprintf(os.Stderr, "%s=cudaErrorUnknown(999)\n", cudaPrivateMarker)
		return 7
	case "11":
		fmt.Fprintln(os.Stderr, "cudaGetDeviceCount=cudaErrorNoDevice(100)")
		fmt.Fprintln(os.Stderr, strings.Repeat(cudaPrivateMarker, 64))
		fmt.Fprintln(os.Stdout, cudaPrivateMarker)
		return 7
	default:
		return 94
	}
	return 0
}

func cudaVectorRequest(first string, timeoutMS int64) *runtimepb.ExecuteJobRequest {
	return &runtimepb.ExecuteJobRequest{JobId: 907, AttemptId: 8, TaskType: cudavector.TaskType,
		Payload: first + strings.Repeat(",0", 63), TimeoutMs: timeoutMS}
}

func TestWorkerCUDAVectorResults(t *testing.T) {
	path := computeFixturePath(t, t.TempDir(), cudaVectorFixtureName)
	program, err := externalcompute.NewProgram(cudavector.TaskType, path, externalcompute.NewSlot())
	if err != nil {
		t.Fatal(err)
	}
	// The tests in this package run serially. Capture the existing Worker stdout
	// with a file so the mTLS handler can write normally before it returns.
	logFile, err := os.CreateTemp(t.TempDir(), "completion.log")
	if err != nil {
		t.Fatal(err)
	}
	stdout := os.Stdout
	os.Stdout = logFile
	t.Cleanup(func() {
		os.Stdout = stdout
		_ = logFile.Close()
	})
	worker := &WorkerService{cudaVector: program, zkDisabled: true}
	_, client := shutdownWorker(t, worker, 2*time.Second)
	exitError := "cuda_vector_transform: exit status 7"
	for _, tc := range []struct {
		first, output, message, stage string
	}{
		{first: "0", output: cudaZeroOutput()},
		{first: "1", message: exitError, stage: "unknown"},
		{first: "2", message: "cuda_vector_transform child returned an invalid or incorrect result"},
		{first: "3", message: "cuda_vector_transform child returned an invalid or incorrect result"},
		{first: "4", message: "cuda_vector_transform child output exceeded its byte limit"},
		{first: "5", message: "cuda_vector_transform child output exceeded its byte limit"},
		{first: "7", message: exitError, stage: "device"},
		{first: "8", message: exitError, stage: "launch_or_sync"},
		{first: "9", message: exitError, stage: "unknown"},
		{first: "10", message: exitError, stage: "unknown"},
		{first: "11", message: exitError, stage: "unknown"},
	} {
		info, err := logFile.Stat()
		if err != nil {
			t.Fatal(err)
		}
		logOffset := info.Size()
		t.Run(tc.first, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			got, err := client.ExecuteJob(ctx, cudaVectorRequest(tc.first, 4000))
			if err != nil || got.GetJobId() != 907 || got.GetAttemptId() != 8 || got.GetOutput() != tc.output || got.GetError() != tc.message {
				t.Fatalf("ExecuteJob returned %v, %v", got, err)
			}
			if tc.message == "" && got.Status != "Succeeded" || tc.message != "" && got.Status != "Failed" {
				t.Fatalf("unexpected status: %s", got.Status)
			}
			logText, err := io.ReadAll(io.NewSectionReader(logFile, logOffset, 4096))
			if err != nil {
				t.Fatal(err)
			}
			wantLog := "executing job: id=907 attempt=8 type=cuda_vector_transform"
			if tc.stage != "" {
				wantLog += " cuda_failure_stage=" + tc.stage
			}
			if string(logText) != wantLog+"\n" {
				t.Fatalf("completion log = %q, want %q", logText, wantLog)
			}
		})
	}
	got, err := worker.ExecuteJob(t.Context(), cudaVectorRequest("00", 4000))
	if err != nil || got.GetStatus() != "failed" || !strings.Contains(got.GetError(), "64 canonical comma-separated") {
		t.Fatalf("invalid payload execution returned %v, %v", got, err)
	}
	got, err = (&WorkerService{}).ExecuteJob(t.Context(), cudaVectorRequest("0", 4000))
	if got != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unconfigured execution returned %v, %v", got, err)
	}
}
