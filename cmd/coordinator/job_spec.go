package main

import (
	"fmt"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	runtime "github.com/guangong789/DistributedComputeRuntime/internal/runtime"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkExistingSubmission is read-only. A local claim alone is not evidence of
// durable admission. Specifications are compared by their exact stored values.
func (s *CoordinatorServer) checkExistingSubmission(spec JobSpec) error {
	record, found, err := s.jobStore.Load(spec.JobID)
	if err != nil {
		return fmt.Errorf("load admitted job: %w", err)
	}
	if !found {
		return nil
	}
	if jobSpecFromRecord(record) != spec {
		return ErrJobSpecConflict
	}
	return errJobAlreadyAdmitted
}

// JobSpec is a value snapshot of a job's inputs. Execution treats it as immutable.
type JobSpec struct {
	JobID     int64
	TaskType  string
	Payload   string
	TimeoutMs int64
}

const retiredTaskType = "zk_preimage_prove"

func jobSpecFromRequest(req *runtimepb.SubmitJobRequest) JobSpec {
	return JobSpec{
		JobID:     req.JobId,
		TaskType:  req.TaskType,
		Payload:   req.Payload,
		TimeoutMs: req.TimeoutMs,
	}
}

func jobSpecFromRecord(record JobRecord) JobSpec {
	return JobSpec{
		JobID:     record.JobID,
		TaskType:  record.TaskType,
		Payload:   record.Payload,
		TimeoutMs: record.TimeoutMs,
	}
}

// validateJobSpec checks the original specification without changing it.
func validateJobSpec(spec JobSpec) error {
	if spec.TaskType == retiredTaskType {
		return status.Errorf(codes.FailedPrecondition, "task type %q has been retired", retiredTaskType)
	}
	if _, err := runtime.TaskTimeoutFromMilliseconds(spec.TimeoutMs); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	switch spec.TaskType {
	case externalsquare.TaskType:
		if _, err := externalsquare.ParsePayload(spec.Payload); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	case cudavector.TaskType:
		if _, err := cudavector.ParsePayload(spec.Payload); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}
	return nil
}
