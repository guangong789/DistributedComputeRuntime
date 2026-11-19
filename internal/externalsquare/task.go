package externalsquare

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	TaskType        = "external_square"
	MaxPayloadBytes = 5
	outputLimit     = 1024 // Per stream, including bytes preceding an overflow.
	terminationWait = 300 * time.Millisecond
)

var ErrBusy = errors.New("external_square capacity is full")

func ParsePayload(payload string) (uint64, error) {
	if len(payload) > 0 && len(payload) <= MaxPayloadBytes {
		n, err := strconv.ParseUint(payload, 10, 16)
		if err == nil && strconv.FormatUint(n, 10) == payload {
			return n, nil
		}
	}
	return 0, errors.New("external_square payload must be a canonical decimal integer from 0 to 65535")
}

// Program is configured by the Worker before serving. All its tasks share one slot.
type Program struct {
	path string
	slot chan struct{}
}

func NewProgram(path string) (*Program, error) {
	if runtime.GOOS != "linux" {
		return nil, errors.New("external_square requires Linux")
	}
	if !filepath.IsAbs(path) {
		return nil, errors.New("external_square executable path must be absolute")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return nil, errors.New("external_square requires an executable regular file")
	}
	return &Program{path: path, slot: make(chan struct{}, 1)}, nil
}

type Task struct {
	program *Program
	input   string
	n       uint64
}

func (p *Program) Task(payload string) (Task, error) {
	n, err := ParsePayload(payload)
	if err != nil {
		return Task{}, err
	}
	return Task{program: p, input: payload, n: n}, nil
}

func (t Task) Execute(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	select {
	case t.program.slot <- struct{}{}:
	default:
		return "", ErrBusy
	}
	defer func() { <-t.program.slot }()

	cmd := exec.CommandContext(ctx, t.program.path)
	cmd.Dir = filepath.Dir(t.program.path)
	cmd.Env = []string{"LC_ALL=C"}
	cmd.Stdin = strings.NewReader(t.input + "\n")
	stdout, stderr := new(boundedOutput), new(boundedOutput)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = terminationWait

	// Run owns the sole Wait and its I/O joins. Keep the slot while the child
	// is still being reaped, even if the RPC or Worker shutdown budget expires.
	runErr := cmd.Run()
	var exit *exec.ExitError
	exitedWithError := errors.As(runErr, &exit)
	if err := ctx.Err(); err != nil {
		if exitedWithError {
			return "", fmt.Errorf("external_square: %v: %w", exit, err)
		}
		return "", err
	}
	switch {
	case exitedWithError:
		return "", fmt.Errorf("external_square: %v", exit)
	case errors.Is(runErr, exec.ErrWaitDelay):
		return "", errors.New("external_square child I/O did not finish")
	case runErr != nil:
		return "", errors.New("external_square executable could not run")
	case stdout.overflow || stderr.overflow:
		return "", errors.New("external_square child output exceeded its byte limit")
	}
	expected := strconv.FormatUint(t.n*t.n, 10)
	if string(stdout.data[:stdout.size]) != expected+"\n" {
		return "", errors.New("external_square child returned an invalid or incorrect result")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return expected, nil
}

type boundedOutput struct {
	data     [outputLimit]byte
	size     int
	overflow bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	n := copy(b.data[b.size:], p)
	b.size += n
	b.overflow = b.overflow || n < len(p)
	return len(p), nil // Drain excess bytes without growing memory or stopping the pipe.
}
