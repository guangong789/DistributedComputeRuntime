package main

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const unsafeDiagnosticText = "fixture\x1b[31m\r\n\t\x7f\x1b]8;;fixture\x07\u009b\u202e"

func TestDiagnosticPresentationWorkerLayout(t *testing.T) {
	long := "vector_" + strings.Repeat("transform_", 12) + unsafeDiagnosticText
	snapshot := &runtimepb.ListWorkersResponse{
		WorkerSnapshotAt: timestamppb.New(time.Date(2025, 3, 1, 23, 4, 5, 0, time.UTC)),
		Workers: []*runtimepb.WorkerDiagnostic{
			{WorkerId: "z", Liveness: runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_DEAD},
			{
				WorkerId: unsafeDiagnosticText, Liveness: runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_ALIVE,
				LastSeenAge: durationpb.New(25 * time.Millisecond), EligibleTaskTypes: []string{"hash", long},
				ExternalTaskTypes: []string{"hash", "a,b", "unknown"},
				CudaHealthManaged: true, EffectiveCudaHealth: runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY,
				CudaEvidenceFreshness: runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_FRESH,
				CudaFreshFor:          durationpb.New(time.Second),
			},
		},
	}
	before := proto.Clone(snapshot)
	var plain, colored bytes.Buffer
	if err := renderWorkers(&plain, snapshot, false); err != nil {
		t.Fatal(err)
	}
	if err := renderWorkers(&colored, snapshot, true); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, snapshot) {
		t.Fatal("formatter mutated a Worker snapshot")
	}
	wantID := "[Alive] " + strconv.QuoteToASCII(unsafeDiagnosticText)
	text := plain.String()
	requireText(t, text, "Workers\n-------\n",
		`Configured external:       hash, "a,b", "unknown"`,
		"Eligible workloads:\n    hash\n    "+strconv.QuoteToASCII(long), wantID, "[Dead] z")
	assertDiagnosticAlignedField(t, text, "Coordinator snapshot:", "2025-03-01T23:04:05Z")
	assertDiagnosticAlignedField(t, text, "Last-seen age:", "25ms")
	if strings.Index(text, wantID) >= strings.Index(text, "[Dead] z") {
		t.Fatalf("Worker sections are not sorted by ID: %q", text)
	}
	requireText(t, colored.String(), "\x1b[1;34mWorkers\x1b[0m", "\x1b[32mAlive\x1b[0m",
		"\x1b[32mReady\x1b[0m", "\x1b[31mDead\x1b[0m", "\x1b[35m-------\x1b[0m")
	assertDiagnosticStyleOnly(t, text, colored.String())
}

func TestDiagnosticPresentationJobChanged(t *testing.T) {
	snapshot := &runtimepb.DiagnoseJobResponse{
		JobId: -9223372036854775808, State: runtimepb.JobState_JOB_STATE_RUNNING,
		AttemptId: 2, WorkerId: unsafeDiagnosticText, TaskType: unsafeDiagnosticText,
		ConfiguredWorkers: 3, AliveCompatibleWorkers: 2, EligibleWorkers: 1,
		Interpretation: runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_JOB_CHANGED,
	}
	before := proto.Clone(snapshot)
	var plain, colored bytes.Buffer
	if err := renderJobDiagnosis(&plain, snapshot, false); err != nil {
		t.Fatal(err)
	}
	if err := renderJobDiagnosis(&colored, snapshot, true); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, snapshot) {
		t.Fatal("formatter mutated a Job snapshot")
	}
	wantValue := strconv.QuoteToASCII(unsafeDiagnosticText)
	requireDiagnosticText(t, plain.String(), "Job -9223372036854775808 [Running]\n------------------------\nJOB CHANGED DURING DIAGNOSIS", "Task type: "+wantValue, "Recorded Worker: "+wantValue,
		"Current placement", "Coordinator snapshot: unknown", "Configured compatible: 3",
		"Compatible recorded Alive: 2", "Eligible Workers: 1", "Category: JOB_CHANGED", "Live Attempt authority: Unknown",
		"Placement interpretation unavailable because the durable record changed during diagnosis.")
	requireText(t, colored.String(), "\x1b[1;33mJOB CHANGED DURING DIAGNOSIS\x1b[0m",
		"\x1b[33mUnknown\x1b[0m", "\x1b[33mJOB_CHANGED\x1b[0m")
	if strings.Contains(plain.String(), "At least one eligible Worker") {
		t.Fatal("formatter inferred a placement cause from the counts")
	}
	assertDiagnosticStyleOnly(t, plain.String(), colored.String())
}

func assertDiagnosticStyleOnly(t *testing.T, plain, colored string) {
	t.Helper()
	replacer := strings.NewReplacer("\x1b[1;34m", "", "\x1b[32m", "", "\x1b[33m", "",
		"\x1b[1;33m", "", "\x1b[31m", "", "\x1b[35m", "", "\x1b[0m", "")
	if unstyled := replacer.Replace(colored); unstyled != plain {
		t.Fatalf("color changed text or alignment:\nplain: %q\ncolored: %q", plain, unstyled)
	}
	for _, r := range plain {
		if r != '\n' && (r < ' ' || r >= 127) {
			t.Fatalf("raw terminal control or non-ASCII character %U in output", r)
		}
	}
}

func assertDiagnosticAlignedField(t *testing.T, text, label, value string) {
	t.Helper()
	const valueColumn = 29
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "  "+label) && strings.HasSuffix(line, value) {
			if strings.Index(line, value) != valueColumn {
				t.Fatalf("%s value misaligned: %q", label, line)
			}
			return
		}
	}
	t.Fatalf("missing aligned field %s %q", label, value)
}
