package externalcompute

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
)

const (
	streamLimit     = 1024
	terminationWait = 300 * time.Millisecond
)

var ErrBusy = errors.New("external compute capacity is full")

type Program struct {
	name, path string
	slot       *Slot
}

func NewProgram(name, path string, slot *Slot) (*Program, error) {
	if runtime.GOOS != "linux" {
		return nil, fmt.Errorf("%s requires Linux", name)
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("%s executable path must be absolute", name)
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return nil, fmt.Errorf("%s requires an executable regular file", name)
	}
	if slot == nil {
		return nil, errors.New("external compute slot is not configured")
	}
	return &Program{name: name, path: path, slot: slot}, nil
}

// ObserveCUDAProbe records a completed local probe through the Worker slot's
// observer. The Program's ordinary executions do not emit probe outcomes.
func (p *Program) ObserveCUDAProbe(outcome metrics.ProbeOutcome) {
	if p.slot.observer != nil {
		p.slot.observer.CUDAProbeFinished(outcome)
	}
}

// Run owns the child and its slot through I/O cleanup and result verification.
// afterFailedExit, when supplied, receives bounded stderr for an unsuccessful
// exit without context cancellation. Overflow and signal exits supply no text.
func (p *Program) Run(ctx context.Context, owner metrics.ComputeOwner, input string, verify func(string) (string, error), afterFailedExit func(string)) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if len(input) >= streamLimit {
		return "", fmt.Errorf("%s input exceeded its byte limit", p.name)
	}
	if !p.slot.tryAcquire(owner) {
		return "", ErrBusy
	}
	defer p.slot.release()

	cmd := exec.CommandContext(ctx, p.path)
	cmd.Dir = filepath.Dir(p.path)
	cmd.Env = []string{"LC_ALL=C"}
	cmd.Stdin = strings.NewReader(input + "\n")
	stdout, stderr := new(boundedOutput), new(boundedOutput)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = terminationWait

	// Run owns the sole Wait and its I/O joins. Keep the slot while the child
	// is still being reaped, even if the RPC or Worker shutdown budget expires.
	runErr := runCommand(cmd)
	var exit *exec.ExitError
	exitedWithError := errors.As(runErr, &exit)
	if err := ctx.Err(); err != nil {
		if exitedWithError {
			return "", fmt.Errorf("%s: %v: %w", p.name, exit, err)
		}
		return "", err
	}
	switch {
	case exitedWithError:
		if afterFailedExit != nil {
			diagnostic := ""
			if exit.ExitCode() > 0 && !stderr.overflow {
				diagnostic = string(stderr.data[:stderr.size])
			}
			afterFailedExit(diagnostic)
		}
		return "", fmt.Errorf("%s: %v", p.name, exit)
	case errors.Is(runErr, exec.ErrWaitDelay):
		return "", fmt.Errorf("%s child I/O did not finish", p.name)
	case runErr != nil:
		return "", fmt.Errorf("%s executable could not run", p.name)
	case stdout.overflow || stderr.overflow:
		return "", fmt.Errorf("%s child output exceeded its byte limit", p.name)
	}
	result, err := verify(string(stdout.data[:stdout.size]))
	if err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return result, nil
}

type boundedOutput struct {
	data     [streamLimit]byte
	size     int
	overflow bool
}

func (b *boundedOutput) Write(p []byte) (int, error) {
	n := copy(b.data[b.size:], p)
	b.size += n
	b.overflow = b.overflow || n < len(p)
	return len(p), nil // Drain excess bytes without growing memory or stopping the pipe.
}
