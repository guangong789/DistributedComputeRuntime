package main

import (
	"context"
	"errors"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedZKRuntime/internal/metrics/prometheus"
	prom "github.com/prometheus/client_golang/prometheus"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type benchmarkObservationClient struct {
	runtimepb.CoordinatorServiceClient
	submit func(context.Context, *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error)
	get    func(context.Context, *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error)
}

func (c benchmarkObservationClient) SubmitJob(ctx context.Context, r *runtimepb.SubmitJobRequest, _ ...grpc.CallOption) (*runtimepb.SubmitJobResponse, error) {
	return c.submit(ctx, r)
}
func (c benchmarkObservationClient) GetJob(ctx context.Context, r *runtimepb.GetJobRequest, _ ...grpc.CallOption) (*runtimepb.GetJobResponse, error) {
	return c.get(ctx, r)
}

func benchmarkAccepted(_ context.Context, r *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
	return &runtimepb.SubmitJobResponse{JobId: r.JobId, Status: "Accepted"}, nil
}

func TestSubmitBenchmarkObservationKeepsClientOccupied(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	secondRead, release := make(chan struct{}), make(chan struct{}, 1)
	defer close(release)
	reads := 0
	client := benchmarkObservationClient{submit: benchmarkAccepted, get: func(ctx context.Context, r *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error) {
		reads++
		if reads == 1 {
			return &runtimepb.GetJobResponse{JobId: r.JobId, State: runtimepb.JobState_JOB_STATE_QUEUED}, nil
		}
		close(secondRead)
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &runtimepb.GetJobResponse{JobId: r.JobId, State: runtimepb.JobState_JOB_STATE_FAILED, AttemptId: 2, WorkerId: "worker", HasWorkerResult: true, Output: "original output", Error: "original error"}, nil
	}}
	done := make(chan submitBenchmarkSample, 1)
	go func() {
		done <- observeSubmitBenchmarkJob(ctx, client, &runtimepb.SubmitJobRequest{JobId: 7}, time.Now())
	}()
	select {
	case <-secondRead:
	case <-ctx.Done():
		t.Fatal("observation did not continue after Queued")
	}
	select {
	case <-done:
		t.Fatal("Accepted/nonterminal snapshot released the logical client")
	default:
	}
	release <- struct{}{}
	select {
	case s := <-done:
		if !s.AdmissionAccepted || s.Polls != 2 || s.TerminalState != "Failed" || s.TerminalError != "Failed" || s.ObservationError != "" || s.Censored || s.snapshot.Output != "original output" || s.snapshot.Error != "original error" || !s.HasWorkerResult || s.CompletionNanoseconds < s.RPCNanoseconds || s.TerminalNS <= s.AcknowledgementNS {
			t.Fatalf("workflow evidence: %+v", s)
		}
	case <-ctx.Done():
		t.Fatal("terminal result missing")
	}
}

func TestSubmitBenchmarkObservationOutcomeStages(t *testing.T) {
	for _, state := range []runtimepb.JobState{runtimepb.JobState_JOB_STATE_SUCCEEDED, runtimepb.JobState_JOB_STATE_FAILED, runtimepb.JobState_JOB_STATE_CANCELLED} {
		t.Run(state.String(), func(t *testing.T) {
			client := benchmarkObservationClient{submit: benchmarkAccepted, get: func(_ context.Context, r *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error) {
				return &runtimepb.GetJobResponse{JobId: r.JobId, State: state, HasWorkerResult: true}, nil
			}}
			s := observeSubmitBenchmarkJob(t.Context(), client, &runtimepb.SubmitJobRequest{JobId: 7}, time.Now())
			if s.snapshot == nil || !s.HasWorkerResult || s.TerminalHasOutput || s.TerminalHasError || s.Censored || s.AdmissionError != "" || s.ObservationError != "" || (s.TerminalError != "") != (state != runtimepb.JobState_JOB_STATE_SUCCEEDED) {
				t.Fatalf("empty accepted result/state lost: %+v", s)
			}
		})
	}
	for _, stage := range []string{"admission", "invalid ack", "read", "cancel observation"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			submits, reads := 0, 0
			client := benchmarkObservationClient{submit: func(ctx context.Context, r *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
				submits++
				if stage == "admission" {
					return nil, status.Error(codes.Unavailable, "uncertain admission")
				}
				ack, err := benchmarkAccepted(ctx, r)
				if stage == "invalid ack" {
					ack.AttemptId = 1
				}
				return ack, err
			}, get: func(context.Context, *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error) {
				reads++
				if stage == "cancel observation" {
					cancel()
				}
				return nil, status.Error(codes.DeadlineExceeded, "observation unavailable")
			}}
			s := observeSubmitBenchmarkJob(ctx, client, &runtimepb.SubmitJobRequest{JobId: 7}, time.Now())
			admission := stage == "admission" || stage == "invalid ack"
			if submits != 1 || s.snapshot != nil || s.TerminalNS != 0 || s.TerminalState != "" || s.TerminalError != "" || admission && (reads != 0 || s.AdmissionError == "" || s.AdmissionAccepted) || !admission && (reads != 1 || !s.Censored || s.ObservationError == "" || !s.AdmissionAccepted) {
				t.Fatalf("stage/resubmission: %+v, submits=%d reads=%d", s, submits, reads)
			}
		})
	}
}

func busyCapacitySample() capacitySample {
	s := capacitySample{submitBenchmarkSample: submitBenchmarkSample{JobID: 7, AdmissionAccepted: true, SubmissionNS: 10, AcknowledgementNS: 20, TerminalNS: 110, RPCNanoseconds: 10, CompletionNanoseconds: 100, RPCCode: "OK", ResponseState: "Accepted", TerminalState: "Failed", TerminalWorker: "worker-a", AttemptID: 2, DurableState: "Failed", DurableAttempt: 2, AuditOK: true},
		Config: "B", Client: 0, Issued: true, ResponseJobID: 7, SpecMatches: true, StartNS: 10, EndNS: 110}
	identifySubmitBenchmarkSample(&s.submitBenchmarkSample, "test", "measured", 1)
	return s
}

func TestCapacityIdentityAndCompletionWindow(t *testing.T) {
	first := busyCapacitySample()
	rows := make(map[int64]capacitySample)
	if err := collectCapacitySample(rows, first, "test", "B", 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := collectCapacitySample(rows, first, "test", "B", 2, 2); err == nil {
		t.Fatal("duplicate sample overwritten")
	}
	wrong := first
	wrong.JobID++
	if err := collectCapacitySample(rows, wrong, "test", "B", 2, 2); err == nil {
		t.Fatal("inconsistent identity accepted")
	}
	warmup := first
	warmup.Cohort = "warmup"
	if err := collectCapacitySample(make(map[int64]capacitySample), warmup, "test", "B", 2, 2); err == nil {
		t.Fatal("warmup counted as measured")
	}
	if err := applyCapacityAudit([]capacitySample{first}, []capacityDurable{{jobID: 7}, {jobID: 7}}); err == nil {
		t.Fatal("duplicate audit overwritten")
	}
	if err := applyCapacityAudit([]capacitySample{first}, []capacityDurable{{jobID: 8}}); err == nil {
		t.Fatal("unexpected audit accepted")
	}
	if err := applyCapacityAudit([]capacitySample{first, first}, nil); err == nil {
		t.Fatal("duplicate samples accepted by audit")
	}
	second := first
	second.JobID, second.StartNS, second.EndNS = 8, 30, 230
	if window, err := capacityObservationWindow([]capacitySample{first, second}); err != nil || window != 220 {
		t.Fatalf("terminal observation window=%d, %v", window, err)
	}
	second.Censored = true
	if _, err := capacityObservationWindow([]capacitySample{first, second}); err == nil {
		t.Fatal("censored observation counted as complete")
	}
	first.TerminalNS, first.EndNS = 0, 0
	if _, err := capacityObservationWindow([]capacitySample{first}); err == nil {
		t.Fatal("Accepted counted as terminal")
	}
}

func TestSubmitBenchmarkQuiescenceRequiresSessionSettlement(t *testing.T) {
	s := &CoordinatorServer{activeJobs: map[int64]struct{}{7: {}}, queuedSessions: 1}
	f := &submitBenchmarkFixture{process: newCoordinatorProcess(s, time.Second)}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.waitIdle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("terminal write/active claim treated as settled")
	}
	delete(s.activeJobs, 7)
	if err := f.waitIdle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("reserved session treated as settled")
	}
	s.queuedSessions = 0
	s.queuedReconciliations = map[int64]*queuedReconciliation{7: {}}
	if err := f.waitIdle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("pending reconciliation treated as settled")
	}
	delete(s.queuedReconciliations, 7)
	f.proofs.begin()
	if err := f.waitIdle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("active Prove treated as settled")
	}
	f.proofs.end(false)
	_, leave, ok := s.lifecycle.tryEnter(t.Context())
	if !ok {
		t.Fatal("cannot create final lifecycle-release boundary")
	}
	if err := f.waitIdle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("unreleased lifecycle ownership treated as settled")
	}
	leave()
	if err := f.waitIdle(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCapacityCancelledJobIsTerminal(t *testing.T) {
	registry := prom.NewRegistry()
	observer, err := prommetrics.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	before := gatherIntegrationMetrics(t, registry)
	observer.JobSubmitted(metrics.SourceSubmit)
	observer.ObserveJobAdmission(metrics.AdmissionAccepted, time.Millisecond)
	observer.ObserveJobExecution(metrics.SourceSubmit, time.Millisecond)
	observer.AttemptStarted(metrics.SourceSubmit)
	observer.AttemptFinished(metrics.SourceSubmit, metrics.AttemptTaskCancelled, time.Millisecond)
	observer.JobTerminal(metrics.SourceSubmit, metrics.TerminalCancelled)
	s := busyCapacitySample()
	s.TerminalState, s.DurableState, s.TerminalError = "Cancelled", "Cancelled", "Cancelled"
	s.AttemptID, s.DurableAttempt, s.HasWorkerResult = 1, 1, true
	r := capacityReport{RunID: submitBenchmarkRunID, Invocation: "test", Config: "B", Iterations: 1, Issued: 1, Attempts: 1, Calls: 1,
		Samples: []capacitySample{s}, RPCStatus: make(map[string]int), Durable: make(map[string]int),
		Workers: []capacityWorkerReport{{WorkerID: "worker-a", Capacity: 1, Calls: 1, Events: []capacityEvent{{Kind: "rpc", JobID: 7, AttemptID: 1, Code: "OK"}}}}}
	finalizeCapacityAccounting(&r, before, gatherIntegrationMetrics(t, registry))
	if !r.AccountingOK || r.Completed != 1 || r.Succeeded != 0 || r.ExpectedOutcomes {
		t.Fatalf("logical cancellation misclassified: %+v", r)
	}
}
