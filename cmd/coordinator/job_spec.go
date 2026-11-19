package main

import (
	"fmt"

	"github.com/guangong789/DistributedZKRuntime/internal/cudavector"
	"github.com/guangong789/DistributedZKRuntime/internal/externalsquare"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// checkExistingSubmission is read-only. A local claim alone is not evidence of
// durable admission. Legacy ZK equivalence classifies duplicates only; Save still
// requires exact stored bytes and no result is replayed to an unbound caller.
func (s *CoordinatorServer) checkExistingSubmission(spec JobSpec) error {
	record, found, err := s.jobStore.Load(spec.JobID)
	if err != nil {
		return fmt.Errorf("load admitted job: %w", err)
	}
	if !found {
		return nil
	}
	stored := jobSpecFromRecord(record)
	if stored != spec {
		if stored.TaskType != "zk_preimage_prove" {
			return ErrJobSpecConflict
		}
		canonical, err := validateJobSpec(stored)
		if err != nil || canonical != spec {
			return ErrJobSpecConflict
		}
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

func (s *CoordinatorServer) checkZKAvailable(taskType string) error {
	if s.zkDisabled && taskType == "zk_preimage_prove" {
		return status.Error(codes.FailedPrecondition, "ZK tasks are disabled")
	}
	return nil
}

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

// validateJobSpec returns a validated execution snapshot. SubmitJob persists its
// normalized copy; recovery retains its exact stored spec for writes and uses
// the normalized copy only for execution. Other task payloads remain unchanged.
// Budgets must be explicit and representable: invalid legacy values require
// operator correction rather than silently acquiring a default execution budget.
func validateJobSpec(spec JobSpec) (JobSpec, error) {
	if _, err := runtime.TaskTimeoutFromMilliseconds(spec.TimeoutMs); err != nil {
		return JobSpec{}, status.Error(codes.InvalidArgument, err.Error())
	}
	switch spec.TaskType {
	case "zk_preimage_prove":
		payload, err := zk.ParsePreimagePublicPayload(spec.Payload)
		if err != nil {
			return JobSpec{}, status.Error(codes.InvalidArgument, err.Error())
		}
		spec.Payload = payload.CanonicalJSON
	case externalsquare.TaskType:
		if _, err := externalsquare.ParsePayload(spec.Payload); err != nil {
			return JobSpec{}, status.Error(codes.InvalidArgument, err.Error())
		}
	case cudavector.TaskType:
		if _, err := cudavector.ParsePayload(spec.Payload); err != nil {
			return JobSpec{}, status.Error(codes.InvalidArgument, err.Error())
		}
	}
	return spec, nil
}
