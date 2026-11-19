package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/externalsquare"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The production command passes no arguments. Only this symlink to the test
// binary selects fixture mode, leaving the R2-01 subprocess mode unchanged.
const externalSquareFixtureName = "external-square-fixture"

func TestMain(m *testing.M) {
	if len(os.Args) == 1 && filepath.Base(os.Args[0]) == externalSquareFixtureName {
		os.Exit(externalSquareFixture())
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
	default:
		return 94
	}
	return 0
}

func externalSquareWorker(t *testing.T) (*WorkerService, string) {
	t.Helper()
	dir := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, externalSquareFixtureName)
	if err := os.Symlink(exe, path); err != nil {
		t.Fatal(err)
	}
	program, err := externalsquare.NewProgram(path)
	if err != nil {
		t.Fatal(err)
	}
	return &WorkerService{externalSquare: program, zkDisabled: true}, dir
}

func externalSquareRequest(payload string, timeoutMS int64) *runtimepb.ExecuteJobRequest {
	return &runtimepb.ExecuteJobRequest{JobId: 906, AttemptId: 7, TaskType: externalsquare.TaskType,
		Payload: payload, TimeoutMs: timeoutMS}
}

func TestWorkerExternalSquareResults(t *testing.T) {
	worker, _ := externalSquareWorker(t)
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

func TestWorkerExternalSquareLifecycle(t *testing.T) {
	for _, mode := range []string{"timeout", "shutdown"} {
		t.Run(mode, func(t *testing.T) {
			worker, dir := externalSquareWorker(t)
			type response struct {
				reply *runtimepb.ExecuteJobResponse
				err   error
			}
			local := make(chan response, 1)
			service := shutdownExecutionService{execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				reply, err := worker.ExecuteJob(ctx, req)
				if req.Payload == "6" {
					local <- response{reply, err}
				}
				return reply, err
			}}
			p, client := shutdownWorker(t, service, 3*time.Second)
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			timeoutMS := int64(1000)
			if mode == "shutdown" {
				timeoutMS = 6000
			}
			remote := make(chan response, 1)
			start := time.Now()
			go func() {
				reply, err := client.ExecuteJob(ctx, externalSquareRequest("6", timeoutMS))
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
			if mode == "shutdown" {
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
			got, err := worker.ExecuteJob(ctx, externalSquareRequest("0", 4000))
			if got != nil || status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("cancellation released the slot before reap: %v, %v", got, err)
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
			if mode == "shutdown" {
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
			got, err = worker.ExecuteJob(ctx, externalSquareRequest("0", 4000))
			if err != nil || got.GetStatus() != "Succeeded" || got.GetOutput() != "0" {
				t.Fatalf("slot was not reusable after reap: %v, %v", got, err)
			}
		})
	}
}
