package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"golang.org/x/sys/unix"
)

const (
	computeChildMode   = "DZKR_TEST_COMPUTE_CHILD"
	computeChildReady  = "ready\n"
	computeChildOutput = "cpu-result:42\n"
	computeChildGrace  = 150 * time.Millisecond
	computeChildBound  = 2 * time.Second
)

func TestComputeSubprocessHelper(t *testing.T) {
	mode := os.Getenv(computeChildMode)
	switch mode {
	case "":
		return
	case "normal", "timeout":
	case "shutdown":
		signal.Ignore(syscall.SIGTERM)
	default:
		os.Exit(2)
	}

	if _, err := io.WriteString(os.Stdout, computeChildReady); err != nil {
		os.Exit(2)
	}
	// The parent owns the pipe. EOF is the harmless emergency exit path.
	var command [1]byte
	if _, err := io.ReadFull(os.Stdin, command[:]); err != nil || mode != "normal" || command[0] != 'g' {
		os.Exit(2)
	}
	if _, err := io.WriteString(os.Stdout, computeChildOutput); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

type computeChildIdentity struct {
	pid        int
	startTicks string
}

func readComputeChild(pid int) (computeChildIdentity, string, error) {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return computeChildIdentity{}, "", err
	}
	end := strings.LastIndex(string(stat), ") ")
	if end < 0 {
		return computeChildIdentity{}, "", errors.New("malformed process stat")
	}
	fields := strings.Fields(string(stat[end+2:]))
	if len(fields) <= 19 || fields[1] != fmt.Sprint(os.Getpid()) {
		return computeChildIdentity{}, "", errors.New("process is not an owned child")
	}
	return computeChildIdentity{pid, fields[19]}, fields[0], nil
}

// A readable pidfd establishes exit before the sole Cmd.Wait reaps the child.
func computeChildExited(pidfd int, within time.Duration) (bool, error) {
	deadline := time.Now().Add(within)
	for {
		left := time.Until(deadline)
		if left <= 0 {
			return false, nil
		}
		fds := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
		n, err := unix.Poll(fds, int((left+time.Millisecond-1)/time.Millisecond))
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil || n == 0 {
			return false, err
		}
		if fds[0].Revents&unix.POLLIN == 0 {
			return false, fmt.Errorf("unexpected pidfd events: %#x", fds[0].Revents)
		}
		return true, nil
	}
}

type computeSubprocessTask struct {
	test             *testing.T
	trace            func(string, ...any)
	executable, mode string
	slot             chan struct{}
	ready            chan computeChildIdentity
	allowIOCleanup   <-chan struct{}
	reaped           chan syscall.WaitStatus
}

func (task *computeSubprocessTask) requestSignal(child computeChildIdentity, pidfd int, sig syscall.Signal) error {
	current, state, err := readComputeChild(child.pid)
	if err != nil || current != child || state == "Z" {
		return fmt.Errorf("cannot verify live child %d for %s: identity=%+v state=%s error=%v", child.pid, sig, current, state, err)
	}
	if err := unix.PidfdSendSignal(pidfd, sig, nil, 0); err != nil {
		return fmt.Errorf("signal child %d with %s: %w", child.pid, sig, err)
	}
	task.trace("signal requested pid=%d signal=%d (%s) state_before=%s", child.pid, sig, sig, state)
	return nil
}

func (task *computeSubprocessTask) supervise(ctx context.Context, child computeChildIdentity, pidfd int, input, output *os.File) error {
	if err := output.SetReadDeadline(time.Now().Add(computeChildBound)); err != nil {
		return err
	}
	var marker [len(computeChildReady)]byte
	if _, err := io.ReadFull(output, marker[:]); err != nil || string(marker[:]) != computeChildReady {
		return fmt.Errorf("child readiness: marker=%q error=%v", marker, err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("child did not become ready before cancellation: %w", err)
	}
	current, state, err := readComputeChild(child.pid)
	if err != nil || current != child || state == "Z" {
		return fmt.Errorf("ready child identity changed: identity=%+v state=%s error=%v", current, state, err)
	}
	task.trace("child ready pid=%d start_ticks=%s", child.pid, child.startTicks)
	task.ready <- child

	if task.mode == "normal" {
		if err := input.SetWriteDeadline(time.Now().Add(computeChildBound)); err != nil {
			return err
		}
		_, err := io.WriteString(input, "g")
		return err
	}
	<-ctx.Done() // The injected Worker handler always sets an explicit deadline.
	task.trace("task context canceled: %v", ctx.Err())
	if err := task.requestSignal(child, pidfd, syscall.SIGTERM); err != nil {
		return err
	}
	exited, err := computeChildExited(pidfd, computeChildGrace)
	if err != nil {
		return err
	}
	if !exited {
		task.trace("TERM grace elapsed (%s); checking child identity before escalation", computeChildGrace)
		if err := task.requestSignal(child, pidfd, syscall.SIGKILL); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (task *computeSubprocessTask) Execute(ctx context.Context) (string, error) {
	select {
	case task.slot <- struct{}{}:
	default:
		return "", errors.New("test compute slot already owned")
	}
	defer func() {
		<-task.slot
		task.trace("slot released")
	}()
	inputReader, input, err := os.Pipe()
	if err != nil {
		return "", err
	}
	defer input.Close()
	defer inputReader.Close()
	output, outputWriter, err := os.Pipe()
	if err != nil {
		return "", err
	}
	defer output.Close()
	defer outputWriter.Close()

	pidfd := -1
	cmd := exec.Command(task.executable, "-test.run=^TestComputeSubprocessHelper$")
	cmd.Env = []string{computeChildMode + "=" + task.mode, "GORACE=atexit_sleep_ms=0"}
	cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inputReader, outputWriter, outputWriter
	if err := cmd.Start(); err != nil {
		return "", err
	}
	defer unix.Close(pidfd)
	_ = inputReader.Close()
	_ = outputWriter.Close()

	child, _, runErr := readComputeChild(cmd.Process.Pid)
	if runErr == nil && pidfd >= 0 {
		task.trace("child started pid=%d start_ticks=%s", child.pid, child.startTicks)
		runErr = task.supervise(ctx, child, pidfd, input, output)
	} else {
		runErr = fmt.Errorf("child identity or pidfd unavailable: pidfd=%d error=%v", pidfd, runErr)
	}
	if runErr != nil && runErr != ctx.Err() {
		// EOF releases the trusted helper if its control path failed.
		_ = input.Close()
	}
	if exited, err := computeChildExited(pidfd, computeChildBound); err != nil || !exited {
		// Report the failed bound without telling the Worker its handler is done.
		// The test binary's outer timeout also supervises this sole Wait.
		task.test.Errorf("child %d did not reach exit before the cleanup deadline: %v", cmd.Process.Pid, err)
		if child.pid != 0 && pidfd >= 0 {
			_ = task.requestSignal(child, pidfd, syscall.SIGKILL)
		}
		_ = input.Close()
	} else {
		task.trace("child exit observed via pidfd pid=%d", child.pid)
	}
	waitErr := cmd.Wait()
	_ = input.Close()
	if cmd.ProcessState == nil {
		return "", errors.Join(runErr, waitErr, errors.New("Wait returned without a child process state"))
	}
	exit := cmd.ProcessState.Sys().(syscall.WaitStatus)
	task.trace("Wait completed pid=%d status=%#x exit_code=%d signal=%d", cmd.Process.Pid, uint32(exit), exit.ExitStatus(), exit.Signal())
	task.reaped <- exit

	// Hold real pending pipe cleanup after Wait so the test can inspect the
	// compute slot and production application join at this boundary.
	select {
	case <-task.allowIOCleanup:
	case <-time.After(computeChildBound):
		task.test.Error("test did not release the I/O cleanup checkpoint")
	}
	if err := output.SetReadDeadline(time.Now().Add(computeChildBound)); err != nil {
		return "", errors.Join(runErr, err)
	}
	const outputLimit = 1024
	data, readErr := io.ReadAll(io.LimitReader(output, outputLimit+1))
	closeErr := output.Close()
	task.trace("I/O cleanup completed buffered_bytes=%d read_error=%v close_error=%v", len(data), readErr, closeErr)
	if len(data) > outputLimit {
		readErr = errors.Join(readErr, errors.New("helper output exceeded its bound"))
	}
	if runErr != nil {
		return "", errors.Join(runErr, readErr, closeErr)
	}
	if err := errors.Join(waitErr, readErr, closeErr); err != nil {
		return "", err
	}
	return string(data), nil
}

func TestWorkerComputeSubprocessLifecycle(t *testing.T) {
	executable, err := os.Executable()
	if err != nil || !filepath.IsAbs(executable) {
		t.Fatalf("absolute test executable: %q, %v", executable, err)
	}
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Skipf("Linux pidfd unavailable: %v", err)
	}
	_ = unix.Close(fd)

	for _, tc := range []struct {
		mode      string
		timeoutMS int64
		status    runtime.JobStatus
		err       error
		signal    syscall.Signal
	}{
		{"normal", 8000, runtime.JobSucceeded, nil, 0},
		{"timeout", 1000, runtime.JobCancelled, context.DeadlineExceeded, syscall.SIGTERM},
		{"shutdown", 8000, runtime.JobCancelled, context.Canceled, syscall.SIGKILL},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			start := time.Now()
			trace := func(format string, args ...any) {
				t.Logf("+%s %s", time.Since(start), fmt.Sprintf(format, args...))
			}
			ioGate := make(chan struct{})
			allowIO := sync.OnceFunc(func() { close(ioGate) })
			defer allowIO()
			task := &computeSubprocessTask{test: t, trace: trace, executable: executable, mode: tc.mode,
				slot: make(chan struct{}, 1), ready: make(chan computeChildIdentity, 1),
				allowIOCleanup: ioGate, reaped: make(chan syscall.WaitStatus, 1)}
			local := make(chan runtime.Result, 1)
			service := shutdownExecutionService{execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				trace("task admitted job=%d attempt=%d", req.JobId, req.AttemptId)
				defer trace("application handler completed")
				timeout, err := runtime.TaskTimeoutFromMilliseconds(req.TimeoutMs)
				if err != nil {
					return nil, err
				}
				taskCtx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()
				result := runtime.ExecuteJob(taskCtx, runtime.Job{ID: int(req.JobId), Timeout: timeout, Task: task})
				local <- result
				return &runtimepb.ExecuteJobResponse{JobId: int64(result.JobID), AttemptId: req.AttemptId,
					Status: result.Status.String(), Output: result.Output, Error: errorString(result.Err)}, nil
			}}
			p, client := shutdownWorker(t, service, 3*time.Second)
			p.shutdownMetrics = func(context.Context) error {
				trace("application join and transport stop observed")
				return nil
			}
			type rpcReply struct {
				response *runtimepb.ExecuteJobResponse
				err      error
			}
			reply := make(chan rpcReply, 1)
			rpcCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			go func() {
				got, err := client.ExecuteJob(rpcCtx, &runtimepb.ExecuteJobRequest{JobId: 42, AttemptId: 7, TimeoutMs: tc.timeoutMS})
				reply <- rpcReply{got, err}
			}()

			child := awaitWorker(t, task.ready)
			if len(task.slot) != 1 {
				t.Fatal("started child does not own its compute slot")
			}
			stopped := make(chan error, 1)
			shutdown := func() {
				trace("worker shutdown requested")
				go func() {
					err := p.shutdown()
					trace("worker shutdown completed: %v", err)
					stopped <- err
				}()
			}
			if tc.mode == "shutdown" {
				shutdown()
				awaitWorker(t, p.lifecycle.ctx.Done())
				if _, leave, ok := p.lifecycle.tryEnter(context.Background()); ok {
					leave()
					t.Fatal("shutdown did not seal application admission")
				}
				trace("shutdown admission sealed")
			}
			exit := awaitWorker(t, task.reaped)
			if tc.signal == 0 && (!exit.Exited() || exit.ExitStatus() != 0) ||
				tc.signal != 0 && (!exit.Signaled() || exit.Signal() != tc.signal) {
				t.Fatalf("unexpected wait status %#x (%s), want exit/signal %v", uint32(exit), tc.mode, tc.signal)
			}
			if _, err := os.Stat(fmt.Sprintf("/proc/%d", child.pid)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("child PID %d still present after Wait: %v", child.pid, err)
			}
			p.lifecycle.mu.Lock()
			active := len(p.lifecycle.active)
			p.lifecycle.mu.Unlock()
			if len(task.slot) != 1 || active != 1 {
				t.Fatalf("ownership dropped before I/O cleanup: slot=%d active_handlers=%d", len(task.slot), active)
			}
			select {
			case <-local:
				t.Fatal("Task returned before I/O cleanup")
			case <-p.lifecycle.done:
				t.Fatal("application lifecycle joined before I/O cleanup")
			case <-p.shutdownDone:
				t.Fatal("Worker shutdown completed before I/O cleanup")
			default:
			}
			trace("after reap, before I/O cleanup: PID absent, slot owned, handler tracked")
			if tc.mode == "shutdown" {
				awaitWorker(t, p.serveDone)
				trace("gRPC Serve exited while application handler still owns cleanup")
			}
			allowIO()
			result := awaitWorker(t, local)
			wantOutput := ""
			if tc.status == runtime.JobSucceeded {
				wantOutput = computeChildOutput
			}
			if result.JobID != 42 || result.Status != tc.status || result.Output != wantOutput ||
				!errors.Is(result.Err, tc.err) || len(task.slot) != 0 {
				t.Fatalf("result=%+v slot=%d", result, len(task.slot))
			}
			got := awaitWorker(t, reply)
			if tc.mode == "shutdown" && got.err != nil {
				trace("caller transport ended: %v", got.err)
			} else if got.err != nil || got.response.GetJobId() != 42 || got.response.GetAttemptId() != 7 ||
				got.response.GetStatus() != tc.status.String() || got.response.GetOutput() != wantOutput ||
				got.response.GetError() != errorString(tc.err) {
				t.Fatalf("RPC result=%v error=%v", got.response, got.err)
			}
			if tc.mode != "shutdown" {
				shutdown()
			}
			if err := awaitWorker(t, stopped); err != nil {
				t.Fatal(err)
			}
			awaitWorker(t, p.lifecycle.done)
		})
	}
}
