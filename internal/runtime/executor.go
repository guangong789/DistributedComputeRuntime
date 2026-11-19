package runtime

import (
	"context"
	"errors"
	"time"
)

// ExecuteJob executes a task and maps its output and error to a job result.
func ExecuteJob(ctx context.Context, job Job) Result {
	output, err := job.Task.Execute(ctx)
	// Authoritative generic Success gate, including tasks that ignore context.
	// Genuine execution failures retain their original classification.
	if err == nil {
		err = resultContextError(ctx, time.Now())
	}

	if err != nil {
		status := JobFailed

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = JobCancelled
		}

		return Result{
			JobID:  job.ID,
			Status: status,
			Output: "",
			Err:    err,
		}
	}

	return Result{
		JobID:  job.ID,
		Status: JobSucceeded,
		Output: output,
		Err:    nil,
	}
}
