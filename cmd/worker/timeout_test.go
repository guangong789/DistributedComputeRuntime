package main

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerTimeoutValidationBeforeTaskConstruction(t *testing.T) {
	_, client := shutdownWorker(t, &WorkerService{}, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, ms := range []int64{0, -1, math.MinInt64, 9223372036855, 1 << 58, 1<<58 + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(ms), func(t *testing.T) {
			for _, taskType := range []string{"external_square", "unknown"} {
				response, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12001, AttemptId: 1, TaskType: taskType, Payload: "invalid", TimeoutMs: ms})
				// Unknown task would return a task Failed response if buildTask ran.
				if response != nil || status.Code(err) != codes.InvalidArgument {
					t.Fatalf("task=%s timeout=%d response=%v error=%v", taskType, ms, response, err)
				}
			}
		})
	}
	for _, ms := range []int64{5000, 9223372036854} {
		response, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12002, AttemptId: 2, TaskType: "hash", Payload: "hello", TimeoutMs: ms})
		if err != nil || response.GetStatus() != "Succeeded" || response.GetAttemptId() != 2 {
			t.Fatalf("valid timeout=%d response=%v error=%v", ms, response, err)
		}
	}
}

func TestWorkerTaskTimeoutIsCancelledResponse(t *testing.T) {
	_, client := shutdownWorker(t, &WorkerService{}, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12003, AttemptId: 3, TaskType: "sleep", Payload: "1h", TimeoutMs: 10})
	if err != nil || response.GetStatus() != "Cancelled" || response.GetOutput() != "" || response.GetError() != context.DeadlineExceeded.Error() || response.GetAttemptId() != 3 || ctx.Err() != nil {
		t.Fatalf("task timeout became an RPC error or Success: response=%v error=%v", response, err)
	}
}
