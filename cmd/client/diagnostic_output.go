package main

import (
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/jobobservation"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const diagnosticLabelWidth = len("Compatible recorded Alive:")

type diagnosticOutput struct {
	strings.Builder
	color bool
}

func diagnosticColor(out io.Writer) bool {
	if _, disabled := os.LookupEnv("NO_COLOR"); disabled {
		return false
	}
	term := os.Getenv("TERM")
	file, ok := out.(*os.File)
	return ok && file != nil && term != "" && !strings.EqualFold(term, "dumb") && diagnosticTerminal(file)
}

func (d *diagnosticOutput) style(code, value string) string {
	if !d.color {
		return value
	}
	return "\x1b[" + code + "m" + value + "\x1b[0m"
}

func (d *diagnosticOutput) heading(title string) {
	fmt.Fprintln(d, d.style("1;34", title))
}

func (d *diagnosticOutput) title(title, status string) {
	fmt.Fprint(d, d.style("1;34", title))
	if status != "" {
		fmt.Fprint(d, "  [", d.state(status), "]")
	}
	fmt.Fprintln(d)
	fmt.Fprintln(d, d.style("35", strings.Repeat("-", len(title))))
}

func (d *diagnosticOutput) field(label, value string) {
	// Pad the ASCII label before writing a possibly styled value.
	fmt.Fprintf(d, "  %-*s %s\n", diagnosticLabelWidth, label+":", value)
}

func (d *diagnosticOutput) state(value string) string {
	switch value {
	case "Alive", "Ready", "Fresh", "Succeeded":
		return d.style("32", value)
	case "Unknown", "unknown", "Expired", "no accepted evidence", "UNKNOWN", "JOB_CHANGED":
		return d.style("33", value)
	case "Dead", "Unavailable", "Failed":
		return d.style("31", value)
	default:
		return value
	}
}

func (d *diagnosticOutput) workloads(label string, names []string) {
	if len(names) == 0 {
		d.field(label, "(none)")
		return
	}
	values := make([]string, len(names))
	for i, name := range names {
		values[i] = diagnosticAtom(name)
	}
	inline := strings.Join(values, ", ")
	if len(inline) <= 48 {
		d.field(label, inline)
		return
	}
	// Preserve complete values without estimating terminal or Unicode width.
	fmt.Fprintf(d, "  %s:\n", label)
	for _, value := range values {
		fmt.Fprintf(d, "    %s\n", value)
	}
}

func renderWorkers(out io.Writer, snapshot *runtimepb.ListWorkersResponse, color bool) error {
	text := diagnosticOutput{color: color}
	text.title("Workers", "")
	text.field("Coordinator snapshot", text.state(diagnosticTimestamp(snapshot.WorkerSnapshotAt)))
	if len(snapshot.Workers) == 0 {
		fmt.Fprintln(&text, "\nNo registered Workers in this snapshot.")
	}
	workers := slices.Clone(snapshot.Workers)
	slices.SortStableFunc(workers, func(a, b *runtimepb.WorkerDiagnostic) int {
		return strings.Compare(a.GetWorkerId(), b.GetWorkerId())
	})
	for _, worker := range workers {
		fmt.Fprintln(&text)
		if worker == nil {
			fmt.Fprintln(&text, "Worker record unavailable")
			continue
		}
		fmt.Fprintf(&text, "  [%s] %s\n", text.state(diagnosticLiveness(worker.Liveness)), text.style("1;34", diagnosticField(worker.WorkerId)))
		text.workloads("Eligible workloads", worker.EligibleTaskTypes)
		text.workloads("Configured external", worker.ExternalTaskTypes)
		text.field("Last-seen age", text.state(diagnosticDuration(worker.LastSeenAge)))
		mode, health, freshness, ttl := diagnosticCUDA(worker)
		text.field("CUDA mode", mode)
		text.field("Effective CUDA health", text.state(health))
		text.field("Evidence freshness", text.state(freshness))
		text.field("Remaining CUDA TTL", text.state(ttl))
	}
	_, err := io.WriteString(out, text.String())
	return err
}

func diagnosticCUDA(worker *runtimepb.WorkerDiagnostic) (mode, health, freshness, ttl string) {
	if !worker.CudaHealthManaged {
		return "unmanaged", "not applicable", "not applicable", "not applicable"
	}
	switch worker.CudaEvidenceFreshness {
	case runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_FRESH:
		health = "Unknown"
		switch worker.EffectiveCudaHealth {
		case runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY:
			health = "Ready"
		case runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE:
			health = "Unavailable"
		}
		return "managed", health, "Fresh", diagnosticDuration(worker.CudaFreshFor)
	case runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_EXPIRED:
		return "managed", "Unknown", "Expired", "not applicable"
	case runtimepb.CudaEvidenceFreshness_CUDA_EVIDENCE_FRESHNESS_UNSPECIFIED:
		return "managed", "Unknown", "no accepted evidence", "not applicable"
	default:
		return "managed", "Unknown", "Unknown", "unknown"
	}
}

func diagnosticLiveness(state runtimepb.RecordedWorkerLiveness) string {
	switch state {
	case runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_ALIVE:
		return "Alive"
	case runtimepb.RecordedWorkerLiveness_RECORDED_WORKER_LIVENESS_DEAD:
		return "Dead"
	default:
		return "Unknown"
	}
}

func renderJobDiagnosis(out io.Writer, snapshot *runtimepb.DiagnoseJobResponse, color bool) error {
	text := diagnosticOutput{color: color}
	text.title("Job "+strconv.FormatInt(snapshot.JobId, 10), jobobservation.StateName(snapshot.State))
	if snapshot.Interpretation == runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_JOB_CHANGED {
		fmt.Fprintln(&text, text.style("1;33", "JOB CHANGED DURING DIAGNOSIS"))
	}
	text.field("Task type", text.state(diagnosticField(snapshot.TaskType)))
	text.field("Recorded AttemptID", strconv.FormatInt(snapshot.AttemptId, 10))
	text.field("Recorded Worker", text.state(diagnosticField(snapshot.WorkerId)))
	text.field("Worker result present", strconv.FormatBool(snapshot.HasWorkerResult))

	fmt.Fprintln(&text)
	text.heading("Current placement")
	text.field("Coordinator snapshot", text.state(diagnosticTimestamp(snapshot.WorkerSnapshotAt)))
	text.field("Configured compatible", strconv.FormatUint(uint64(snapshot.ConfiguredWorkers), 10))
	text.field("Compatible recorded Alive", strconv.FormatUint(uint64(snapshot.AliveCompatibleWorkers), 10))
	text.field("Eligible Workers", strconv.FormatUint(uint64(snapshot.EligibleWorkers), 10))

	fmt.Fprintln(&text)
	text.heading("Interpretation")
	category, explanation := diagnosticInterpretation(snapshot.Interpretation)
	text.field("Category", text.state(category))
	if snapshot.State == runtimepb.JobState_JOB_STATE_RUNNING {
		text.field("Live Attempt authority", text.state("Unknown"))
	}
	fmt.Fprintf(&text, "  %s\n", explanation)
	_, err := io.WriteString(out, text.String())
	return err
}

func diagnosticInterpretation(interpretation runtimepb.PlacementInterpretation) (category, explanation string) {
	switch interpretation {
	case runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_CONFIGURED_WORKER:
		return "NO_CONFIGURED_WORKER", "No configured compatible Worker at sample time."
	case runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ALIVE_WORKER:
		return "NO_ALIVE_WORKER", "No compatible Worker recorded Alive at sample time."
	case runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NO_ELIGIBLE_WORKER:
		return "NO_ELIGIBLE_WORKER", "No eligible Worker at sample time."
	case runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_ELIGIBLE_WORKER_EXISTS:
		return "ELIGIBLE_WORKER_EXISTS", "At least one eligible Worker at sample time. Scheduling progress is unknown."
	case runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_NOT_APPLICABLE:
		return "NOT_APPLICABLE", "Not applicable to a terminal Job."
	case runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_JOB_CHANGED:
		return "JOB_CHANGED", "Placement interpretation unavailable because the durable record changed during diagnosis."
	case runtimepb.PlacementInterpretation_PLACEMENT_INTERPRETATION_UNKNOWN:
		return "UNKNOWN", "Unknown."
	default:
		return "Unknown", "Unknown."
	}
}

func diagnosticTimestamp(stamp *timestamppb.Timestamp) string {
	if !stamp.IsValid() {
		return "unknown"
	}
	return stamp.AsTime().Format(time.RFC3339Nano)
}

func diagnosticDuration(duration *durationpb.Duration) string {
	if !duration.IsValid() || duration.Seconds < 0 || duration.Nanos < 0 {
		return "unknown"
	}
	value := duration.AsDuration()
	if roundTrip := durationpb.New(value); roundTrip.Seconds != duration.Seconds || roundTrip.Nanos != duration.Nanos {
		return "unknown"
	}
	return value.String()
}

func diagnosticField(value string) string {
	if value == "" {
		return "unknown"
	}
	return diagnosticAtom(value)
}

// Only simple identifiers render unquoted. Quoting everything else preserves
// delimiters, Unicode, and control bytes without introducing terminal commands.
func diagnosticAtom(value string) string {
	if value != "" && value != "unknown" && strings.IndexFunc(value, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.')
	}) == -1 {
		return value
	}
	return strconv.QuoteToASCII(value)
}
