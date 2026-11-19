package externalsquare

import (
	"context"
	"errors"
	"strconv"

	"github.com/guangong789/DistributedComputeRuntime/internal/externalcompute"
)

const (
	TaskType        = "external_square"
	MaxPayloadBytes = 5
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
	child *externalcompute.Program
}

func NewProgram(path string, slot *externalcompute.Slot) (*Program, error) {
	child, err := externalcompute.NewProgram(TaskType, path, slot)
	if err != nil {
		return nil, err
	}
	return &Program{child: child}, nil
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
	output, err := t.program.child.Run(ctx, t.input, func(stdout string) (string, error) {
		expected := strconv.FormatUint(t.n*t.n, 10)
		if stdout != expected+"\n" {
			return "", errors.New("external_square child returned an invalid or incorrect result")
		}
		return expected, nil
	}, nil)
	if errors.Is(err, externalcompute.ErrBusy) {
		return "", ErrBusy
	}
	return output, err
}
