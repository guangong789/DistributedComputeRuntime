package cudavector

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/guangong789/DistributedComputeRuntime/internal/externalcompute"
	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
)

const (
	TaskType        = "cuda_vector_transform"
	vectorLength    = 64
	maxPayloadBytes = vectorLength*5 + vectorLength - 1
)

var errPayload = errors.New("cuda_vector_transform payload must contain 64 canonical comma-separated decimal integers from 0 to 65535")

func ParsePayload(payload string) ([vectorLength]uint64, error) {
	var values [vectorLength]uint64
	if len(payload) > maxPayloadBytes || strings.Count(payload, ",") != vectorLength-1 {
		return values, errPayload
	}
	for i, part := range strings.Split(payload, ",") {
		n, err := strconv.ParseUint(part, 10, 16)
		if err != nil || strconv.FormatUint(n, 10) != part {
			return values, errPayload
		}
		values[i] = n
	}
	return values, nil
}

type Task struct {
	program         *externalcompute.Program
	input, expected string
}

func NewTask(program *externalcompute.Program, payload string) (Task, error) {
	values, err := ParsePayload(payload)
	if err != nil {
		return Task{}, err
	}
	var output [vectorLength]string
	for i, n := range values {
		output[i] = strconv.FormatUint(n*n+3*uint64(i)+11, 10)
	}
	return Task{program: program, input: payload, expected: strings.Join(output[:], ",")}, nil
}

func (t Task) Execute(ctx context.Context) (string, error) {
	return t.execute(ctx, metrics.ComputeJob)
}

func (t Task) ExecuteProbe(ctx context.Context) (string, error) {
	return t.execute(ctx, metrics.ComputeProbe)
}

func (t Task) execute(ctx context.Context, owner metrics.ComputeOwner) (string, error) {
	var stage string
	var backendUnavailable bool
	output, err := t.program.Run(ctx, owner, t.input, func(stdout string) (string, error) {
		if stdout != t.expected+"\n" {
			return "", errors.New("cuda_vector_transform child returned an invalid or incorrect result")
		}
		return t.expected, nil
	}, func(stderr string) {
		stage, backendUnavailable = classifyFailure(stderr)
	})
	if stage != "" {
		err = &failedExit{error: err, stage: stage, backendUnavailable: backendUnavailable}
	}
	return output, err
}
