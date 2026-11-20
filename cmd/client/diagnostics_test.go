package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type diagnosticStub struct {
	workers func(context.Context) (*runtimepb.ListWorkersResponse, error)
	job     func(context.Context, int64) (*runtimepb.DiagnoseJobResponse, error)
}

func (c diagnosticStub) ListWorkers(ctx context.Context, _ *runtimepb.ListWorkersRequest, _ ...grpc.CallOption) (*runtimepb.ListWorkersResponse, error) {
	return c.workers(ctx)
}

func (c diagnosticStub) DiagnoseJob(ctx context.Context, req *runtimepb.DiagnoseJobRequest, _ ...grpc.CallOption) (*runtimepb.DiagnoseJobResponse, error) {
	return c.job(ctx, req.JobId)
}

func TestDiagnosticArgumentsFailBeforeCredentials(t *testing.T) {
	for _, args := range [][]string{
		{"diagnose"}, {"diagnose", "workers"}, {"diagnose", "job"}, {"diagnose", "job", "7", "extra"},
		{"diagnose", "job", "x"}, {"diagnose", "job", "9223372036854775808"}, {"workers", "extra"}, {"unknown"},
		{"-rpc-timeout=0s", "workers"}, {"-observe-job=7", "workers"}, {"-rpc-timeout=private-marker", "workers"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			var out, stderr bytes.Buffer
			// No credential flags were supplied. Exit 2 distinguishes argument
			// rejection from any attempt to configure the connection.
			if code := runCLI(t.Context(), args, &out, &stderr); code != 2 || out.Len() != 0 ||
				stderr.Len() == 0 || strings.Contains(stderr.String(), "mTLS") || strings.Contains(stderr.String(), "private-marker") {
				t.Fatalf("invalid args exit=%d stdout=%q stderr=%q", code, &out, &stderr)
			}
		})
	}
	var out, stderr bytes.Buffer
	if code := runCLI(t.Context(), []string{"-h"}, &out, &stderr); code != 0 || !strings.Contains(out.String(), "workers | diagnose job") ||
		!strings.Contains(out.String(), "-observe-job") || stderr.Len() != 0 {
		t.Fatalf("help exit=%d stdout=%q stderr=%q", code, &out, &stderr)
	}
}

func TestDiagnosticCommandsMakeOneBoundedRPC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			args    []string
			jobID   int64
			timeout time.Duration
			wait    time.Duration
		}{
			{[]string{"workers"}, 0, time.Minute, diagnosticTimeout},
			{[]string{"diagnose", "job", "0"}, 0, time.Second, time.Second},
			{[]string{"diagnose", "job", "-9223372036854775808"}, -9223372036854775808, time.Minute, diagnosticTimeout},
			{[]string{"diagnose", "job", "9223372036854775807"}, 9223372036854775807, time.Minute, diagnosticTimeout},
		} {
			cmd, err := parseClientCommand(tc.args)
			if err != nil {
				t.Fatal(err)
			}
			var listCalls, jobCalls int
			checkDeadline := func(ctx context.Context) {
				t.Helper()
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) != tc.wait {
					t.Fatalf("per-RPC deadline = %v, want %v", time.Until(deadline), tc.wait)
				}
			}
			client := diagnosticStub{
				workers: func(ctx context.Context) (*runtimepb.ListWorkersResponse, error) {
					listCalls++
					checkDeadline(ctx)
					return &runtimepb.ListWorkersResponse{}, nil
				},
				job: func(ctx context.Context, id int64) (*runtimepb.DiagnoseJobResponse, error) {
					jobCalls++
					checkDeadline(ctx)
					if id != tc.jobID {
						t.Fatalf("Job ID = %d, want %d", id, tc.jobID)
					}
					return &runtimepb.DiagnoseJobResponse{JobId: id}, nil
				},
			}
			var out bytes.Buffer
			if err := runDiagnostic(t.Context(), client, cmd, tc.timeout, &out); err != nil || out.Len() == 0 {
				t.Fatalf("diagnostic output %q, err %v", &out, err)
			}
			switch cmd.kind {
			case workersCommand:
				if listCalls != 1 || jobCalls != 0 {
					t.Fatalf("workers: ListWorkers=%d DiagnoseJob=%d", listCalls, jobCalls)
				}
			case diagnoseJobCommand:
				if listCalls != 0 || jobCalls != 1 {
					t.Fatalf("diagnose job: ListWorkers=%d DiagnoseJob=%d", listCalls, jobCalls)
				}
			default:
				t.Fatal("diagnostic routed to existing workflow")
			}
		}
	})
}

func TestDiagnosticCancellationAndParentDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var out bytes.Buffer
		var calls int
		client := diagnosticStub{workers: func(ctx context.Context) (*runtimepb.ListWorkersResponse, error) {
			calls++
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}}
		cmd := clientCommand{kind: workersCommand}
		parent, cancel := context.WithCancel(t.Context())
		cancel()
		if err := runDiagnostic(parent, client, cmd, time.Minute, &out); status.Code(err) != codes.Canceled || calls != 0 {
			t.Fatalf("canceled before call: %v, calls=%d", err, calls)
		}
		parent, cancel = context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()
		start := time.Now()
		if err := runDiagnostic(parent, client, cmd, time.Minute, &out); status.Code(err) != codes.DeadlineExceeded || calls != 1 ||
			time.Since(start) != 250*time.Millisecond || out.Len() != 0 {
			t.Fatalf("earlier parent deadline: %v, calls=%d, elapsed=%s", err, calls, time.Since(start))
		}
		parent, cancel = context.WithCancel(t.Context())
		defer cancel()
		client.workers = func(ctx context.Context) (*runtimepb.ListWorkersResponse, error) {
			calls++
			cancel()
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		if err := runDiagnostic(parent, client, cmd, time.Minute, &out); status.Code(err) != codes.Canceled || calls != 2 || out.Len() != 0 {
			t.Fatalf("in-flight cancellation: %v, calls=%d", err, calls)
		}
	})
}

func TestDiagnosticRPCErrorIsSanitized(t *testing.T) {
	for _, tc := range []struct {
		code codes.Code
		want string
	}{
		{codes.PermissionDenied, "access is not permitted"},
		{codes.Unauthenticated, "rejected RPC authentication"},
		{codes.NotFound, "Job not found"},
		{codes.Unavailable, "temporarily unavailable"},
		{codes.DeadlineExceeded, "timed out"},
		{codes.ResourceExhausted, "resource or response size limit"},
		{codes.Canceled, "canceled"},
		{codes.Internal, "diagnostics failed"},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			calls := 0
			client := diagnosticStub{job: func(context.Context, int64) (*runtimepb.DiagnoseJobResponse, error) {
				calls++
				return nil, status.Error(tc.code, "private internal detail")
			}}
			var out bytes.Buffer
			err := runDiagnostic(t.Context(), client, clientCommand{kind: diagnoseJobCommand}, time.Second, &out)
			if status.Code(err) != tc.code || err == nil || !strings.Contains(err.Error(), tc.want) ||
				strings.Contains(err.Error(), "private") || calls != 1 || out.Len() != 0 {
				t.Fatalf("error=%v calls=%d stdout=%q", err, calls, &out)
			}
		})
	}
	// An authenticated application may use arbitrary status text. Only
	// pre-RPC transport failures may be interpreted as a handshake failure.
	err := status.Error(codes.Unavailable, `connection error: desc = "transport: authentication handshake failed: private"`)
	if got := diagnosticRPCError(err, true); status.Code(got) != codes.Unavailable || strings.Contains(got.Error(), "private") {
		t.Fatalf("trusted application status misclassified: %v", got)
	}
}

func TestDiagnosticRejectsNonmatchingJob(t *testing.T) {
	var out bytes.Buffer
	client := diagnosticStub{job: func(context.Context, int64) (*runtimepb.DiagnoseJobResponse, error) {
		return &runtimepb.DiagnoseJobResponse{JobId: 99}, nil
	}}
	if err := runDiagnostic(t.Context(), client, clientCommand{kind: diagnoseJobCommand, jobID: 7}, time.Second, &out); err == nil || out.Len() != 0 {
		t.Fatalf("nonmatching response printed: %q, %v", &out, err)
	}
}

func TestDiagnosticWorkerOutput(t *testing.T) {
	var out bytes.Buffer
	if err := renderWorkers(&out, &runtimepb.ListWorkersResponse{}, false); err != nil ||
		out.String() != "Workers\n-------\n  Coordinator snapshot:      unknown\n\nNo registered Workers in this snapshot.\n" {
		t.Fatalf("empty snapshot: %q, %v", &out, err)
	}
	ready := runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY
	unavailable := runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE
	fresh := runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_FRESH
	expired := runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_EXPIRED
	for _, tc := range []struct {
		name                            string
		managed                         bool
		health                          runtimepb.CudaHealthState
		freshness                       runtimepb.CudaEvidenceFreshness
		ttl                             *durationpb.Duration
		wantMode, wantHealth, wantFresh string
		wantTTL                         string
	}{
		{"legacy", false, 0, 0, nil, "unmanaged", "not applicable", "not applicable", "not applicable"},
		{"ready", true, ready, fresh, durationpb.New(3 * time.Second), "managed", "Ready", "Fresh", "3s"},
		{"unavailable", true, unavailable, fresh, durationpb.New(0), "managed", "Unavailable", "Fresh", "0s"},
		{"managed unknown", true, 0, 0, nil, "managed", "Unknown", "no accepted evidence", "not applicable"},
		{"expired ready", true, ready, expired, durationpb.New(time.Minute), "managed", "Unknown", "Expired", "not applicable"},
		{"missing ttl", true, ready, fresh, nil, "managed", "Ready", "Fresh", "unknown"},
		{"future health", true, 87, fresh, durationpb.New(time.Second), "managed", "Unknown", "Fresh", "1s"},
		{"future freshness", true, ready, 79, nil, "managed", "Unknown", "Unknown", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			snapshot := &runtimepb.ListWorkersResponse{
				WorkerSnapshotAt: timestamppb.New(time.Date(2025, 3, 1, 23, 4, 5, 0, time.UTC)),
				Workers: []*runtimepb.WorkerDiagnostic{{
					WorkerId: "worker-a", Liveness: runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_DEAD,
					ExternalTaskTypes: []string{"cuda_vector_transform"}, CudaHealthManaged: tc.managed,
					EffectiveCudaHealth: tc.health, CudaEvidenceFreshness: tc.freshness, CudaFreshFor: tc.ttl,
				}},
			}
			if err := renderWorkers(&out, snapshot, false); err != nil {
				t.Fatal(err)
			}
			requireDiagnosticText(t, out.String(), "Coordinator snapshot: 2025-03-01T23:04:05Z", "[Dead] worker-a",
				"Last-seen age: unknown", `Configured external: cuda_vector_transform`,
				"Eligible workloads: (none)", "CUDA mode: "+tc.wantMode, "Effective CUDA health: "+tc.wantHealth,
				"Evidence freshness: "+tc.wantFresh, "Remaining CUDA TTL: "+tc.wantTTL)
		})
	}
	out.Reset()
	if err := renderWorkers(&out, &runtimepb.ListWorkersResponse{
		WorkerSnapshotAt: &timestamppb.Timestamp{Seconds: 1, Nanos: -1},
		Workers: []*runtimepb.WorkerDiagnostic{{WorkerId: "x\n\x1b", Liveness: 73,
			LastSeenAge: durationpb.New(0), EligibleTaskTypes: []string{"hash", "task\u202etest"}}},
	}, false); err != nil {
		t.Fatal(err)
	}
	requireDiagnosticText(t, out.String(), "Coordinator snapshot: unknown", `[Unknown] "x\n\x1b"`,
		"Last-seen age: 0s", `Eligible workloads: hash, "task\u202etest"`)
	if strings.ContainsRune(out.String(), '\x1b') || strings.ContainsRune(out.String(), '\u202e') {
		t.Fatal("dynamic field emitted a terminal control or bidi character")
	}
}

func TestDiagnosticJobOutput(t *testing.T) {
	for _, tc := range []struct {
		name, text     string
		state          runtimepb.JobState
		interpretation runtimepb.PlacementInterpretation
		counts         [3]uint32
	}{
		{"no configured", "No configured compatible Worker", runtimepb.JobState_JOB_STATE_QUEUED, runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_CONFIGURED_WORKER, [3]uint32{}},
		{"none alive", "No compatible Worker recorded Alive", runtimepb.JobState_JOB_STATE_QUEUED, runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ALIVE_WORKER, [3]uint32{1, 0, 0}},
		{"none eligible", "No eligible Worker at sample time", runtimepb.JobState_JOB_STATE_RECOVERING, runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ELIGIBLE_WORKER, [3]uint32{1, 1, 0}},
		{"eligible", "Scheduling progress is unknown", runtimepb.JobState_JOB_STATE_QUEUED, runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_ELIGIBLE_WORKER_EXISTS, [3]uint32{2, 2, 1}},
		{"running", "Live Attempt authority: Unknown", runtimepb.JobState_JOB_STATE_RUNNING, runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_UNKNOWN, [3]uint32{1, 1, 1}},
		{"terminal", "Not applicable to a terminal Job", runtimepb.JobState_JOB_STATE_SUCCEEDED, runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NOT_APPLICABLE, [3]uint32{1, 1, 1}},
		{"changed", "JOB CHANGED DURING DIAGNOSIS", runtimepb.JobState_JOB_STATE_QUEUED, runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_JOB_CHANGED, [3]uint32{1, 1, 1}},
		{"future", "Job -9 [Unknown]", 99, 100, [3]uint32{0, 0, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			record := &runtimepb.DiagnoseJobResponse{
				JobId: -9, State: tc.state, Interpretation: tc.interpretation,
				ConfiguredWorkers: tc.counts[0], AliveCompatibleWorkers: tc.counts[1], EligibleWorkers: tc.counts[2],
			}
			if tc.name == "terminal" {
				record.AttemptId, record.WorkerId, record.HasWorkerResult = 2, "gpu", true
				record.TaskType = "cuda_vector_transform"
				record.WorkerSnapshotAt = timestamppb.New(time.Date(2025, 3, 1, 23, 4, 5, 0, time.UTC))
			}
			if err := renderJobDiagnosis(&out, record, false); err != nil {
				t.Fatal(err)
			}
			text := out.String()
			requireDiagnosticText(t, text, "Job -9 [", tc.text, "Current placement", "Interpretation")
			switch tc.name {
			case "terminal":
				requireDiagnosticText(t, text, "Recorded AttemptID: 2", "Recorded Worker: gpu", "Worker result present: true",
					"Coordinator snapshot: 2025-03-01T23:04:05Z", "Task type: cuda_vector_transform")
			case "eligible":
				requireDiagnosticText(t, text, "Configured compatible: 2", "Compatible recorded Alive: 2", "Eligible Workers: 1")
			case "future":
				requireDiagnosticText(t, text, "Coordinator snapshot: unknown", "Task type: unknown", "Recorded Worker: unknown", "Category: Unknown")
			case "changed":
				requireDiagnosticText(t, text, "Category: JOB_CHANGED", "Placement interpretation unavailable")
				if strings.Contains(text, "At least one eligible Worker") {
					t.Fatal("changed Job was interpreted from counts")
				}
			}
			if strings.Contains(text, "queued because") || strings.Contains(text, "Slot Busy") {
				t.Fatal("CLI inferred a historical queue cause")
			}
		})
	}
}

func requireText(t *testing.T, text string, want ...string) {
	t.Helper()
	for _, part := range want {
		if !strings.Contains(text, part) {
			t.Errorf("missing %q in:\n%s", part, text)
		}
	}
}

// Semantic assertions ignore alignment spaces. Presentation tests check their
// exact placement separately.
func requireDiagnosticText(t *testing.T, text string, fields ...string) {
	t.Helper()
	claims := strings.Split(text, "\n")
	for i, line := range claims {
		claims[i] = strings.Join(strings.Fields(line), " ")
	}
	requireText(t, strings.Join(claims, "\n"), fields...)
}
