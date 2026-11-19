package runtime

import "time"

type JobStatus int

const (
	JobRunning JobStatus = iota + 1
	JobSucceeded
	JobFailed
	JobCancelled
)

func (s JobStatus) String() string {
	switch s {
	case JobRunning:
		return "Running"
	case JobSucceeded:
		return "Succeeded"
	case JobFailed:
		return "Failed"
	case JobCancelled:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

type Job struct {
	ID      int
	Timeout time.Duration
	Task    Task
	Status  JobStatus
}

type Result struct {
	JobID  int
	Status JobStatus
	Output string
	Err    error
}
