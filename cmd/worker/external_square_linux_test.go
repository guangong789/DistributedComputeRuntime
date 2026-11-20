package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalcompute"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The production command passes no arguments. Only this symlink to the test
// binary selects fixture mode, leaving the R2-01 subprocess mode unchanged.
const externalSquareFixtureName = "external-square-fixture"

func TestMain(m *testing.M) {
	if len(os.Args) == 1 {
		switch filepath.Base(os.Args[0]) {
		case externalSquareFixtureName:
			os.Exit(externalSquareFixture())
		case cudaVectorFixtureName:
			os.Exit(cudaVectorFixture())
		}
	}
	os.Exit(m.Run())
}

func externalSquareFixture() int {
	if env := os.Environ(); len(env) != 1 || env[0] != "LC_ALL=C" {
		return 90
	}
	input, err := io.ReadAll(io.LimitReader(os.Stdin, 7))
	if err != nil {
		return 91
	}
	switch string(input) {
	case "0\n":
		fmt.Fprintln(os.Stdout, "0")
	case "1\n":
		fmt.Fprintln(os.Stderr, "private diagnostic marker")
		return 7
	case "2\n":
		fmt.Fprintln(os.Stdout, "malformed")
	case "3\n":
		fmt.Fprintln(os.Stdout, "8")
	case "4\n":
		fmt.Fprint(os.Stdout, strings.Repeat("x", 4096))
	case "5\n":
		fmt.Fprintln(os.Stdout, "25")
		fmt.Fprint(os.Stderr, strings.Repeat("x", 4096))
	case "6\n":
		return holdComputeFixture()
	case "7\n":
		if os.WriteFile("ready", []byte(strconv.Itoa(os.Getpid())), 0600) != nil {
			return 92
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			_, err := os.Stat("release")
			if err == nil {
				fmt.Fprintln(os.Stdout, "49")
				return 0
			}
			if !errors.Is(err, os.ErrNotExist) {
				return 95
			}
			time.Sleep(5 * time.Millisecond)
		}
		return 96
	default:
		return 94
	}
	return 0
}

func holdComputeFixture() int {
	terms := make(chan os.Signal, 1)
	signal.Notify(terms, syscall.SIGTERM)
	pid := []byte(strconv.Itoa(os.Getpid()))
	if os.WriteFile("ready", pid, 0600) != nil {
		return 92
	}
	<-terms
	if os.WriteFile("term", pid, 0600) != nil {
		return 93
	}
	// A second TERM would also be ignored. The parent must kill and reap.
	for range terms {
	}
	return 0
}

func computeFixturePath(t *testing.T, dir, name string) string {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.Symlink(exe, path); err != nil {
		t.Fatal(err)
	}
	return path
}

func externalSquareWorker(t *testing.T, slot *externalcompute.Slot) (*WorkerService, string) {
	t.Helper()
	dir := t.TempDir()
	program, err := externalsquare.NewProgram(computeFixturePath(t, dir, externalSquareFixtureName), slot)
	if err != nil {
		t.Fatal(err)
	}
	return &WorkerService{externalSquare: program}, dir
}

func externalSquareRequest(payload string, timeoutMS int64) *runtimepb.ExecuteJobRequest {
	return &runtimepb.ExecuteJobRequest{JobId: 906, AttemptId: 7, TaskType: externalsquare.TaskType,
		Payload: payload, TimeoutMs: timeoutMS}
}

func TestWorkerExternalSquareResults(t *testing.T) {
	worker, _ := externalSquareWorker(t, externalcompute.NewSlot())
	_, client := shutdownWorker(t, worker, 2*time.Second)
	for _, tc := range []struct {
		payload, output, message string
	}{
		{payload: "0", output: "0"},
		{payload: "1", message: "external_square: exit status 7"},
		{payload: "2", message: "external_square child returned an invalid or incorrect result"},
		{payload: "3", message: "external_square child returned an invalid or incorrect result"},
		{payload: "4", message: "external_square child output exceeded its byte limit"},
		{payload: "5", message: "external_square child output exceeded its byte limit"},
		{payload: "00001", message: "external_square payload must be a canonical decimal integer from 0 to 65535"},
		{payload: "123456", message: "external_square payload must be a canonical decimal integer from 0 to 65535"},
	} {
		t.Run(tc.payload, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			got, err := client.ExecuteJob(ctx, externalSquareRequest(tc.payload, 4000))
			if err != nil || got.GetJobId() != 906 || got.GetAttemptId() != 7 || got.GetOutput() != tc.output ||
				got.GetError() != tc.message {
				t.Fatalf("ExecuteJob returned %v, %v", got, err)
			}
			if tc.message == "" && got.Status != "Succeeded" || tc.message != "" && !strings.EqualFold(got.Status, "Failed") {
				t.Fatalf("unexpected status: %s", got.Status)
			}
		})
	}

	got, err := (&WorkerService{}).ExecuteJob(t.Context(), externalSquareRequest("0", 4000))
	if got != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("unconfigured execution returned %v, %v", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err = worker.ExecuteJob(ctx, externalSquareRequest("6", 4000))
	if err != nil || got.GetStatus() != "Cancelled" || got.GetError() != context.Canceled.Error() {
		t.Fatalf("pre-canceled execution returned %v, %v", got, err)
	}
}

func awaitExternalSquareMarker(t *testing.T, dir, name string) int {
	t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if pid, err := strconv.Atoi(string(raw)); err == nil && pid > 0 {
			return pid
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("child did not publish %s marker", name)
	return 0
}

func TestWorkerExternalSquareSurvivesThreadExit(t *testing.T) {
	worker, dir := externalSquareWorker(t, externalcompute.NewSlot())
	ctx, cancel := context.WithTimeout(t.Context(), 6*time.Second)
	defer cancel()
	type response struct {
		reply *runtimepb.ExecuteJobResponse
		err   error
	}
	done := make(chan response, 1)
	go func() {
		reply, err := worker.ExecuteJob(ctx, externalSquareRequest("7", 6000))
		done <- response{reply, err}
	}()
	pid := awaitExternalSquareMarker(t, dir, "ready")
	original, state, err := readComputeChild(pid)
	if err != nil || state == "Z" {
		t.Fatalf("compute child did not start: state=%s err=%v", state, err)
	}

	// Keep both goroutines locked until their TIDs are known so at least one
	// uses a non-leader thread. Returning without unlocking retires that thread.
	tids := make(chan int, 2)
	retire := make(chan struct{})
	for range cap(tids) {
		go func() {
			runtime.LockOSThread()
			tid := syscall.Gettid()
			tids <- tid
			<-retire
			if tid == os.Getpid() {
				runtime.UnlockOSThread()
			}
		}()
	}
	threads := []int{<-tids, <-tids}
	close(retire)
	for _, tid := range threads {
		if tid == os.Getpid() {
			continue
		}
		path := fmt.Sprintf("/proc/self/task/%d", tid)
		for {
			_, err := os.Stat(path)
			if errors.Is(err, os.ErrNotExist) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-ctx.Done():
				t.Fatalf("unrelated thread %d did not retire: %v", tid, ctx.Err())
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	current, state, err := readComputeChild(pid)
	if err != nil || current != original || state == "Z" {
		t.Fatalf("compute child did not survive unrelated thread exit: state=%s err=%v", state, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	got := awaitWorker(t, done)
	if got.err != nil || got.reply.GetStatus() != "Succeeded" || got.reply.GetOutput() != "49" {
		t.Fatalf("ExecuteJob returned %v, %v", got.reply, got.err)
	}
}

func TestWorkerExternalSquareLifecycle(t *testing.T) {
	for _, mode := range []string{"timeout", "shutdown", "cuda_timeout", "cuda_shutdown"} {
		t.Run(mode, func(t *testing.T) {
			slot := externalcompute.NewSlot()
			worker, squareDir := externalSquareWorker(t, slot)
			cudaDir := t.TempDir()
			var err error
			worker.cudaVector, err = externalcompute.NewProgram(cudavector.TaskType,
				computeFixturePath(t, cudaDir, cudaVectorFixtureName), slot)
			if err != nil {
				t.Fatal(err)
			}
			shutdown := strings.HasSuffix(mode, "shutdown")
			dir := squareDir
			hold, probe := externalSquareRequest("6", 1000), externalSquareRequest("0", 4000)
			cross := cudaVectorRequest("0", 4000)
			probeOutput, crossOutput := "0", cudaZeroOutput()
			if strings.HasPrefix(mode, "cuda_") {
				dir = cudaDir
				hold, probe, cross = cudaVectorRequest("6", 1000), cross, probe
				probeOutput, crossOutput = crossOutput, probeOutput
			}
			if shutdown {
				hold.TimeoutMs = 6000
			}
			type response struct {
				reply *runtimepb.ExecuteJobResponse
				err   error
			}
			local := make(chan response, 1)
			service := shutdownExecutionService{execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				reply, err := worker.ExecuteJob(ctx, req)
				if req.TaskType == hold.TaskType && req.Payload == hold.Payload {
					local <- response{reply, err}
				}
				return reply, err
			}}
			p, client := shutdownWorker(t, service, 3*time.Second)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			remote := make(chan response, 1)
			start := time.Now()
			go func() {
				reply, err := client.ExecuteJob(ctx, hold)
				remote <- response{reply, err}
			}()
			pid := awaitExternalSquareMarker(t, dir, "ready")
			identity, _, err := readComputeChild(pid)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("+%s child ready pid=%d start_ticks=%s", time.Since(start), pid, identity.startTicks)
			stopped := make(chan error, 1)
			wantCancel := context.DeadlineExceeded
			if shutdown {
				wantCancel = context.Canceled
				go func() { stopped <- p.shutdown() }()
				awaitWorker(t, p.lifecycle.ctx.Done())
				if _, leave, ok := p.lifecycle.tryEnter(ctx); ok {
					leave()
					t.Fatal("shutdown did not seal admission")
				}
			}
			if got := awaitExternalSquareMarker(t, dir, "term"); got != pid {
				t.Fatalf("TERM marker pid=%d, want %d", got, pid)
			}
			current, state, err := readComputeChild(pid)
			if err != nil || current != identity || state == "Z" {
				t.Fatalf("child did not remain alive after TERM: %v %s %v", current, state, err)
			}
			t.Logf("+%s SIGTERM observed; same owned child still alive", time.Since(start))
			// Probe the actual Worker slot even after lifecycle admission is sealed.
			got, err := worker.ExecuteJob(ctx, probe)
			if got != nil || status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("cancellation released the slot before reap: %v, %v", got, err)
			}
			got, err = worker.ExecuteJob(ctx, cross)
			if got != nil || status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("other workload bypassed the slot: %v, %v", got, err)
			}
			select {
			case <-local:
				t.Fatal("handler completed during TERM grace")
			case <-p.shutdownDone:
				t.Fatal("shutdown completed before child reaping")
			default:
			}
			t.Logf("+%s capacity rejected with ResourceExhausted", time.Since(start))

			result := awaitWorker(t, local)
			if result.err != nil || result.reply.GetStatus() != "Cancelled" || result.reply.GetOutput() != "" ||
				!strings.Contains(result.reply.GetError(), "signal: killed") || !strings.HasSuffix(result.reply.GetError(), wantCancel.Error()) {
				t.Fatalf("local cancellation returned %v, %v", result.reply, result.err)
			}
			if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("child remains after handler result: pid=%d error=%v", pid, err)
			}
			t.Logf("+%s pid=%d reaped; %s", time.Since(start), pid, result.reply.Error)
			rpc := awaitWorker(t, remote)
			if shutdown {
				if rpc.err == nil && rpc.reply.GetStatus() != "Cancelled" {
					t.Fatalf("unexpected shutdown reply: %v", rpc.reply)
				}
				if err := awaitWorker(t, stopped); err != nil {
					t.Fatal(err)
				}
				awaitWorker(t, p.lifecycle.done)
				t.Logf("+%s shutdown joined production handler", time.Since(start))
			} else if rpc.err != nil || rpc.reply.GetStatus() != "Cancelled" || rpc.reply.GetError() != result.reply.GetError() {
				t.Fatalf("timeout RPC returned %v, %v", rpc.reply, rpc.err)
			}
			got, err = worker.ExecuteJob(ctx, probe)
			if err != nil || got.GetStatus() != "Succeeded" || got.GetOutput() != probeOutput {
				t.Fatalf("original workload could not reuse its slot: %v, %v", got, err)
			}
			got, err = worker.ExecuteJob(ctx, cross)
			if err != nil || got.GetStatus() != "Succeeded" || got.GetOutput() != crossOutput {
				t.Fatalf("slot was not reusable after reap: %v, %v", got, err)
			}
		})
	}
}
