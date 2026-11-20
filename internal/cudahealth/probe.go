package cudahealth

import (
	"context"
	"errors"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalcompute"
	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	"github.com/guangong789/DistributedComputeRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
)

const (
	probeTimeout = 3 * time.Second
	probeInput   = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63"
)

// Result describes the observation committed to the Worker registration.
// UNSPECIFIED means no evidence was published. Output is set only for Ready.
type Result struct {
	State  runtimepb.CudaHealthState
	Output string
}

// Probe runs one bounded check on the Worker's configured CUDA program and
// shared slot. The caller owns its lifetime and keeps heartbeat independent.
func Probe(parent context.Context, registration *workerheartbeat.Registration, program *externalcompute.Program) (result Result, err error) {
	if program != nil {
		defer func() {
			outcome := metrics.ProbeNoObservation
			switch result.State {
			case runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY:
				outcome = metrics.ProbeReady
			case runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE:
				outcome = metrics.ProbeUnavailable
			}
			program.ObserveCUDAProbe(outcome)
		}()
	}
	if err := parent.Err(); err != nil {
		return Result{}, err
	}
	if program == nil {
		return Result{}, errors.New("CUDA health check requires a configured program")
	}
	task, err := cudavector.NewTask(program, probeInput)
	if err != nil {
		return Result{}, err
	}

	if registration == nil {
		return Result{}, nil
	}
	check := registration.BeginCUDAHealthCheck()
	if check == nil {
		return Result{}, nil
	}
	ctx, cancel := context.WithTimeout(parent, probeTimeout)
	defer cancel()

	output, err := task.ExecuteProbe(ctx)
	var state runtimepb.CudaHealthState
	if interrupted := ctx.Err(); interrupted != nil {
		err = interrupted
	} else if err == nil {
		state = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY
	} else if cudavector.BackendUnavailable(err) {
		state = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE
	}
	if !check.Finish(state) {
		return Result{}, err
	}
	return Result{State: state, Output: output}, err
}
