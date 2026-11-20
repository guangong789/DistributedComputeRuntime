package runtimepb_test

import (
	"encoding/hex"
	"testing"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestExistingProtocolWireCompatibility(t *testing.T) {
	// Golden bytes cover every pre-GetJob message, preserving existing field
	// numbers and wire meanings for old synchronous clients and Workers.
	for _, tc := range []struct {
		name    string
		message proto.Message
		wire    string
	}{
		{"ExecuteJobRequest", &runtimepb.ExecuteJobRequest{JobId: 7, TaskType: "hash", Payload: "x", TimeoutMs: 9, AttemptId: 3}, "08071204686173681a017820092803"},
		{"ExecuteJobResponse", &runtimepb.ExecuteJobResponse{JobId: 7, Status: "Failed", Output: "x", Error: "e", AttemptId: 3}, "080712064661696c65641a01782201652803"},
		{"SubmitJobRequest", &runtimepb.SubmitJobRequest{JobId: 7, TaskType: "hash", Payload: "x", TimeoutMs: 9}, "08071204686173681a01782009"},
		{"SubmitJobResponse", &runtimepb.SubmitJobResponse{JobId: 7, Status: "Failed", Output: "x", Error: "e", AttemptId: 3}, "080712064661696c65641a01782201652803"},
		{"RegisterWorkerRequest", &runtimepb.RegisterWorkerRequest{WorkerId: "w", Address: "a"}, "0a0177120161"},
		{"RegisterWorkerResponse", &runtimepb.RegisterWorkerResponse{Accepted: true, Error: "e"}, "0801120165"},
		{"HeartbeatRequest", &runtimepb.HeartbeatRequest{WorkerId: "w"}, "0a0177"},
		{"HeartbeatResponse", &runtimepb.HeartbeatResponse{Accepted: true}, "0801"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire, err := proto.Marshal(tc.message)
			if err != nil || hex.EncodeToString(wire) != tc.wire {
				t.Fatalf("wire changed: %x, %v", wire, err)
			}
		})
	}
	for _, service := range []struct {
		name    protoreflect.Name
		methods []protoreflect.Name
	}{
		{"WorkerService", []protoreflect.Name{"ExecuteJob"}},
		{"CoordinatorService", []protoreflect.Name{"RegisterWorker", "SubmitJob", "Heartbeat", "GetJob", "ListWorkers", "DiagnoseJob"}},
	} {
		descriptor := runtimepb.File_proto_worker_proto.Services().ByName(service.name)
		if descriptor == nil || descriptor.Methods().Len() != len(service.methods) {
			t.Fatalf("service changed: %s", service.name)
		}
		for i, method := range service.methods {
			if got := descriptor.Methods().Get(i); got.Name() != method || got.IsStreamingClient() || got.IsStreamingServer() {
				t.Fatalf("RPC signature changed: %s", got.FullName())
			}
		}
	}
}

func TestGetJobProtocol(t *testing.T) {
	service := runtimepb.File_proto_worker_proto.Services().ByName("CoordinatorService")
	method := service.Methods().ByName("GetJob")
	if method.Input().FullName() != "runtime.GetJobRequest" || method.Output().FullName() != "runtime.GetJobResponse" || runtimepb.CoordinatorService_GetJob_FullMethodName != "/runtime.CoordinatorService/GetJob" {
		t.Fatal("GetJob RPC signature changed")
	}
	fields := (&runtimepb.GetJobResponse{}).ProtoReflect().Descriptor().Fields()
	for i, name := range []protoreflect.Name{"job_id", "state", "attempt_id", "worker_id", "has_worker_result", "output", "error"} {
		if field := fields.ByName(name); field == nil || field.Number() != protoreflect.FieldNumber(i+1) {
			t.Fatalf("snapshot field contract changed: %s", name)
		}
	}
	if fields.Len() != 7 || fields.ByName("state").Enum().FullName() != "runtime.JobState" {
		t.Fatal("snapshot exposed additional fields or lost typed state")
	}
	for _, present := range []bool{false, true} {
		original := &runtimepb.GetJobResponse{JobId: -1, State: runtimepb.JobState_JOB_STATE_SUCCEEDED, AttemptId: 2, WorkerId: "worker", HasWorkerResult: present}
		wire, err := proto.Marshal(original)
		if err != nil {
			t.Fatal(err)
		}
		var decoded runtimepb.GetJobResponse
		if err := proto.Unmarshal(wire, &decoded); err != nil || !proto.Equal(original, &decoded) {
			t.Fatalf("empty result presence lost on wire: %v", err)
		}
	}
}
