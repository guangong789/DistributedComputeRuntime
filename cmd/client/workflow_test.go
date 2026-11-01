package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/jobobservation"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workflowClient struct {
	runtimepb.CoordinatorServiceClient
	ack                   *runtimepb.SubmitJobResponse
	admissionErr, readErr error
	snapshots             []*runtimepb.GetJobResponse
	submits, reads        int
	afterSubmit           func()
	afterRead             func()
}

func (c *workflowClient) SubmitJob(context.Context, *runtimepb.SubmitJobRequest, ...grpc.CallOption) (*runtimepb.SubmitJobResponse, error) {
	c.submits++
	if c.afterSubmit != nil {
		c.afterSubmit()
	}
	return c.ack, c.admissionErr
}
func (c *workflowClient) GetJob(_ context.Context, r *runtimepb.GetJobRequest, _ ...grpc.CallOption) (*runtimepb.GetJobResponse, error) {
	c.reads++
	if c.afterRead != nil {
		c.afterRead()
	}
	if c.readErr != nil {
		return nil, c.readErr
	}
	if len(c.snapshots) == 0 {
		return nil, status.Error(codes.Internal, "unexpected additional query")
	}
	s := c.snapshots[0]
	c.snapshots = c.snapshots[1:]
	if s != nil && r.JobId != s.JobId {
		return nil, status.Error(codes.Internal, "wrong requested JobID")
	}
	return s, nil
}
func workflowConfig() clientExecutionConfig {
	return clientExecutionConfig{JobTimeout: time.Second, RPCTimeout: time.Second, ObserveTimeout: time.Second, PollInterval: time.Nanosecond}
}
func workflowAck() *runtimepb.SubmitJobResponse {
	return &runtimepb.SubmitJobResponse{JobId: 3001, Status: "Accepted"}
}
func jobSnapshot(state runtimepb.JobState, present bool, output, message string) *runtimepb.GetJobResponse {
	return &runtimepb.GetJobResponse{JobId: 3001, State: state, AttemptId: 2, WorkerId: "worker", HasWorkerResult: present, Output: output, Error: message}
}

func TestClientAsyncWorkflow(t *testing.T) {
	for _, tc := range []struct {
		name     string
		states   []runtimepb.JobState
		terminal *runtimepb.GetJobResponse
		failure  bool
	}{
		{"queued running", []runtimepb.JobState{runtimepb.JobState_JOB_STATE_QUEUED, runtimepb.JobState_JOB_STATE_RUNNING}, jobSnapshot(runtimepb.JobState_JOB_STATE_SUCCEEDED, true, "AQID", ""), false},
		{"recovering", []runtimepb.JobState{runtimepb.JobState_JOB_STATE_RECOVERING}, jobSnapshot(runtimepb.JobState_JOB_STATE_SUCCEEDED, true, "AQID", ""), false},
		{"accepted empty", nil, jobSnapshot(runtimepb.JobState_JOB_STATE_SUCCEEDED, true, "", ""), false},
		{"legacy success without Worker result", nil, jobSnapshot(runtimepb.JobState_JOB_STATE_SUCCEEDED, false, "AQID", ""), false},
		{"Worker failed", nil, jobSnapshot(runtimepb.JobState_JOB_STATE_FAILED, true, "", "worker failed"), true},
		{"Coordinator failed", nil, jobSnapshot(runtimepb.JobState_JOB_STATE_FAILED, false, "", ""), true},
		{"Worker cancelled", nil, jobSnapshot(runtimepb.JobState_JOB_STATE_CANCELLED, true, "", "task cancelled"), true},
		{"Coordinator cancelled", nil, jobSnapshot(runtimepb.JobState_JOB_STATE_CANCELLED, false, "", ""), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &workflowClient{ack: workflowAck()}
			for _, state := range tc.states {
				client.snapshots = append(client.snapshots, jobSnapshot(state, false, "nonempty does not mean complete", ""))
			}
			client.snapshots = append(client.snapshots, tc.terminal)
			var out bytes.Buffer
			err := runClient(context.Background(), client, workflowConfig(), &out)
			var jobErr *jobobservation.TerminalJobError
			if (err != nil) != tc.failure || tc.failure && (!errors.As(err, &jobErr) || jobErr.HasWorkerResult != tc.terminal.HasWorkerResult || jobErr.Message != tc.terminal.Error) {
				t.Fatalf("terminal outcome=%v", err)
			}
			if client.submits != 1 || client.reads != len(tc.states)+1 {
				t.Fatalf("resubmission or incomplete polling: %d/%d", client.submits, client.reads)
			}
			for _, want := range []string{"status=" + jobobservation.StateName(tc.terminal.State), "attempt=2", "has_worker_result=" + strconv.FormatBool(tc.terminal.HasWorkerResult), "output=" + strconv.Quote(tc.terminal.Output), "error=" + strconv.Quote(tc.terminal.Error)} {
				if !strings.Contains(out.String(), want) {
					t.Fatalf("output %q missing %q", out.String(), want)
				}
			}
			if !tc.terminal.HasWorkerResult && strings.Contains(out.String(), "proof_bytes=") {
				t.Fatal("unaccepted bytes presented as a Worker proof")
			}
			if tc.terminal.State == runtimepb.JobState_JOB_STATE_SUCCEEDED && tc.terminal.HasWorkerResult && tc.terminal.Output == "" && !strings.Contains(out.String(), "proof_bytes=0") {
				t.Fatal("accepted empty output was lost")
			}
		})
	}
}

func TestClientWorkflowRejectsInvalidAdmission(t *testing.T) {
	for _, field := range []string{"nil", "job", "status", "attempt", "output", "error"} {
		t.Run(field, func(t *testing.T) {
			ack := workflowAck()
			switch field {
			case "nil":
				ack = nil
			case "job":
				ack.JobId++
			case "status":
				ack.Status = "Succeeded"
			case "attempt":
				ack.AttemptId = 1
			case "output":
				ack.Output = "fabricated"
			case "error":
				ack.Error = "fabricated"
			}
			client := &workflowClient{ack: ack}
			var out bytes.Buffer
			if err := runClient(context.Background(), client, workflowConfig(), &out); err == nil || !strings.Contains(err.Error(), "admission acknowledgement") || client.submits != 1 || client.reads != 0 || out.Len() != 0 {
				t.Fatalf("invalid ack accepted: %v, output=%q", err, out.String())
			}
		})
	}
}

func TestClientWorkflowErrorsDoNotResubmit(t *testing.T) {
	for _, admission := range []bool{true, false} {
		for _, code := range []codes.Code{codes.AlreadyExists, codes.NotFound, codes.Unavailable, codes.Internal, codes.PermissionDenied} {
			t.Run(strconv.FormatBool(admission)+"/"+code.String(), func(t *testing.T) {
				injected := status.Error(code, "original classification")
				client := &workflowClient{ack: workflowAck()}
				if admission {
					client.admissionErr = injected
				} else {
					client.readErr = injected
				}
				var out bytes.Buffer
				err := runClient(context.Background(), client, workflowConfig(), &out)
				stage := "GetJob observation"
				wantReads := 1
				if admission {
					stage = "admission"
					wantReads = 0
				}
				if status.Code(err) != code || !strings.Contains(err.Error(), stage) || client.submits != 1 || client.reads != wantReads || out.Len() != 0 {
					t.Fatalf("error misclassified/resubmitted: %v %d/%d", err, client.submits, client.reads)
				}
			})
		}
	}
}

func TestClientObservationEndsWithoutCancellingJob(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(strconv.FormatBool(deadline), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &workflowClient{ack: workflowAck()}
			cfg := workflowConfig()
			cfg.PollInterval = time.Hour
			want := codes.Canceled
			if deadline {
				cfg.ObserveTimeout = time.Nanosecond
				want = codes.DeadlineExceeded
			} else {
				client.afterSubmit = cancel
			}
			var out bytes.Buffer
			err := runClient(ctx, client, cfg, &out)
			if status.Code(err) != want || !strings.Contains(err.Error(), "observation ended") || !strings.Contains(err.Error(), "does not cancel") || client.submits != 1 || client.reads != 0 || out.Len() != 0 {
				t.Fatalf("observation lifetime changed Job outcome: %v", err)
			}
		})
	}
}

func TestClientCancellationDuringGetJob(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshot := jobSnapshot(runtimepb.JobState_JOB_STATE_QUEUED, false, "", "")
	client := &workflowClient{ack: workflowAck(), snapshots: []*runtimepb.GetJobResponse{snapshot}, afterRead: cancel}
	var out bytes.Buffer
	err := runClient(ctx, client, workflowConfig(), &out)
	if status.Code(err) != codes.Canceled || !strings.Contains(err.Error(), "observation ended") || client.submits != 1 || client.reads != 1 || snapshot.State != runtimepb.JobState_JOB_STATE_QUEUED || out.Len() != 0 {
		t.Fatalf("cancelled read mutated/resubmitted job: %v", err)
	}
}

func TestClientGetJobPreservesEarlierParentDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want, _ := ctx.Deadline()
	client := demoSubmitClient{submit: func(context.Context, *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
		return workflowAck(), nil
	}}
	reader := &deadlineWorkflowClient{demoSubmitClient: client, want: want, t: t}
	cfg := workflowConfig()
	cfg.ObserveTimeout, cfg.RPCTimeout = time.Minute, time.Minute
	var out bytes.Buffer
	if err := runClient(ctx, reader, cfg, &out); err != nil {
		t.Fatal(err)
	}
}

type deadlineWorkflowClient struct {
	demoSubmitClient
	want time.Time
	t    *testing.T
}

func (c *deadlineWorkflowClient) GetJob(ctx context.Context, _ *runtimepb.GetJobRequest, _ ...grpc.CallOption) (*runtimepb.GetJobResponse, error) {
	if got, ok := ctx.Deadline(); !ok || got != c.want {
		c.t.Fatal("observation/RPC budget extended parent's deadline")
	}
	return jobSnapshot(runtimepb.JobState_JOB_STATE_SUCCEEDED, true, "", ""), nil
}

func TestClientRejectsUnknownSnapshots(t *testing.T) {
	for _, snapshot := range []*runtimepb.GetJobResponse{nil, jobSnapshot(runtimepb.JobState_JOB_STATE_UNSPECIFIED, false, "", ""), jobSnapshot(runtimepb.JobState(99), false, "", ""), {JobId: 1, State: runtimepb.JobState_JOB_STATE_SUCCEEDED}} {
		client := &workflowClient{ack: workflowAck(), snapshots: []*runtimepb.GetJobResponse{snapshot}}
		var out bytes.Buffer
		if err := runClient(context.Background(), client, workflowConfig(), &out); err == nil || client.submits != 1 || client.reads != 1 || out.Len() != 0 {
			t.Fatalf("invalid snapshot accepted: %v", err)
		}
	}
}

func TestClientObservationFlagsAndReconnect(t *testing.T) {
	for _, id := range []int64{0, -1, math.MinInt64, math.MaxInt64} {
		fs := flag.NewFlagSet("client", flag.ContinueOnError)
		cfg := clientExecutionFlags(fs)
		if err := fs.Parse([]string{"-observe-job", strconv.FormatInt(id, 10), "-observe-timeout", "2m", "-poll-interval", "1ns"}); err != nil || cfg.validate() != nil || cfg.ObserveJobID == nil || *cfg.ObserveJobID != id || cfg.ObserveTimeout != 2*time.Minute {
			t.Fatalf("observation flags=%+v, %v", cfg, err)
		}
		snapshot := jobSnapshot(runtimepb.JobState_JOB_STATE_SUCCEEDED, true, "", "")
		snapshot.JobId = id
		client := &workflowClient{snapshots: []*runtimepb.GetJobResponse{snapshot}}
		var out bytes.Buffer
		if err := runClient(context.Background(), client, *cfg, &out); err != nil || client.submits != 0 || client.reads != 1 || strings.Contains(out.String(), "proof_bytes=") {
			t.Fatalf("reconnect resubmitted: %v", err)
		}
	}
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	cfg := clientExecutionFlags(fs)
	if cfg.PollInterval != 250*time.Millisecond || cfg.ObserveTimeout != 30*time.Second {
		t.Fatal("observation defaults changed")
	}
}
