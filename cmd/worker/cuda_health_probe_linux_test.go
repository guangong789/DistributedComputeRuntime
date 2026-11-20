package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudahealth"
	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalcompute"
	"github.com/guangong789/DistributedComputeRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedComputeRuntime/internal/metrics/prometheus"
	"github.com/guangong789/DistributedComputeRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	probeReady       = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_READY
	probeUnavailable = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNAVAILABLE
	noObservation    = runtimepb.CudaHealthState_CUDA_HEALTH_STATE_UNSPECIFIED
)

func cudaProbeFixtureVector() (string, string) {
	var input, output [64]string
	for i := range input {
		input[i] = strconv.Itoa(i)
		output[i] = strconv.Itoa(i*i + 3*i + 11)
	}
	return strings.Join(input[:], ","), strings.Join(output[:], ",")
}

func cudaProbeFixture() int {
	mode, err := os.ReadFile("probe-mode")
	if err != nil {
		return 92
	}
	_, expected := cudaProbeFixtureVector()
	const syncPrefix = "cudaGetDeviceCount=cudaSuccess(0)\ncudaGetDeviceProperties=cudaSuccess(0)\ncudaSetDevice=cudaSuccess(0)\n" +
		"device=0 name=test cc=8.6 uuid=12345678-1234-1234-1234-123456789abc\ncudaMalloc=cudaSuccess(0)\n" +
		"host_to_device=cudaSuccess(0)\nkernel_launch=cudaSuccess(0)\n"
	switch string(mode) {
	case "ready":
		fmt.Println(expected)
		return 0
	case "mismatch":
		fmt.Println("99" + expected[2:])
		return 0
	case "no device":
		fmt.Fprintln(os.Stderr, "cudaGetDeviceCount=cudaErrorNoDevice(100)")
	case "synchronization fault":
		fmt.Fprint(os.Stderr, syncPrefix+"cudaDeviceSynchronize=cudaErrorLaunchFailure(719)\ncudaFree=cudaSuccess(0)\ncudaDeviceReset=cudaSuccess(0)\n")
	case "truncated synchronization fault":
		fmt.Fprint(os.Stderr, syncPrefix+"cudaDeviceSynchronize=cudaErrorLaunchFailure(719)\n")
	case "invalid CUDA argument":
		fmt.Fprintln(os.Stderr, "cudaGetDeviceCount=cudaErrorInvalidValue(1)")
	case "inconclusive":
		fmt.Fprintln(os.Stderr, "external execution failed")
	case "hold":
		return holdComputeFixture()
	case "held ready":
		if os.WriteFile("ready", []byte(strconv.Itoa(os.Getpid())), 0600) != nil {
			return 93
		}
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			_, err := os.Stat("release")
			if err == nil {
				fmt.Println(expected)
				return 0
			}
			if !errors.Is(err, os.ErrNotExist) {
				return 94
			}
			time.Sleep(5 * time.Millisecond)
		}
		return 95
	default:
		return 96
	}
	return 1
}

func probeFixtureWorker(t *testing.T, mode string, slot *externalcompute.Slot) (*WorkerService, string, string) {
	t.Helper()
	worker, squareDir := externalSquareWorker(t, slot)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "probe-mode"), []byte(mode), 0600); err != nil {
		t.Fatal(err)
	}
	program, err := externalcompute.NewProgram(cudavector.TaskType, computeFixturePath(t, dir, cudaVectorFixtureName), slot)
	if err != nil {
		t.Fatal(err)
	}
	worker.cudaVector = program
	return worker, dir, squareDir
}

func probeRegistration() *workerheartbeat.Registration {
	return workerheartbeat.NewRegistration(&runtimepb.RegisterWorkerRequest{
		WorkerId: "worker-a", Address: "localhost:50051", RegistrationKey: bytes.Repeat([]byte{1}, 32), CudaHealthManaged: true,
	})
}

func assertProbeSlotReleased(t *testing.T, worker *WorkerService) {
	t.Helper()
	got, err := worker.ExecuteJob(t.Context(), externalSquareRequest("0", 4000))
	if err != nil || got.GetStatus() != "Succeeded" || got.GetOutput() != "0" {
		t.Fatalf("external square could not use slot after probe: %v, %v", got, err)
	}
}

func TestCUDAHealthProbeClassification(t *testing.T) {
	for _, tc := range []struct {
		mode string
		want runtimepb.CudaHealthState
	}{
		{"ready", probeReady}, {"no device", probeUnavailable}, {"synchronization fault", probeUnavailable},
		{"mismatch", noObservation}, {"invalid CUDA argument", noObservation},
		{"truncated synchronization fault", noObservation}, {"inconclusive", noObservation},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			t.Parallel()
			slot, endpoint := computeMetrics(t)
			worker, _, _ := probeFixtureWorker(t, tc.mode, slot)
			reg := probeRegistration()
			var obs *runtimepb.CudaHealthObservation
			client := &fakeCoordinatorClient{
				register: func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
					return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: bytes.Repeat([]byte{2}, 32)}, nil
				},
				heartbeat: func(_ context.Context, req *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
					obs = req.GetCudaHealth()
					return &runtimepb.HeartbeatResponse{Accepted: true, CudaHealthProbeAnchor: 21}, nil
				},
			}
			if err := reg.Register(t.Context(), client); err != nil {
				t.Fatal(err)
			}
			if got, err := cudahealth.Probe(t.Context(), reg, worker.cudaVector); err != nil || got.State != noObservation {
				t.Fatalf("probe without anchor: %+v, %v", got, err)
			}
			if err := reg.Send(t.Context(), client); err != nil {
				t.Fatal(err)
			}
			got, err := cudahealth.Probe(t.Context(), reg, worker.cudaVector)
			if got.State != tc.want || (err == nil) != (tc.want == probeReady) {
				t.Fatalf("probe: %+v, %v; want %s", got, err, tc.want)
			}
			_, wantOutput := cudaProbeFixtureVector()
			if (tc.want == probeReady && got.Output != wantOutput) || (tc.want != probeReady && got.Output != "") {
				t.Fatal("unexpected verified output")
			}
			if err := reg.Send(t.Context(), client); err != nil {
				t.Fatal(err)
			}
			if tc.want == noObservation {
				if obs != nil {
					t.Fatal("inconclusive result published health")
				}
			} else if obs.GetObservationSeq() != 1 || obs.GetProbeAnchor() != 21 || obs.GetState() != tc.want {
				t.Fatal("wrong observation for completed probe")
			}
			assertProbeSlotReleased(t, worker)
			samples := requireComputeScrape(t, endpoint)
			for state, outcome := range map[runtimepb.CudaHealthState]string{
				probeReady: "ready", probeUnavailable: "unavailable", noObservation: "no_observation",
			} {
				count, last := 0.0, 0.0
				if state == noObservation {
					count++ // The first call had no heartbeat anchor.
				}
				if state == tc.want {
					count++
					last++
				}
				label := fmt.Sprintf(`{outcome="%s"}`, outcome)
				requireComputeSample(t, samples, "dzkr_cuda_probe_outcomes_total"+label, count)
				requireComputeSample(t, samples, "dzkr_cuda_probe_last_outcome"+label, last)
			}
			requireComputeSample(t, samples, `dzkr_compute_slot_hold_duration_seconds_count{owner="probe"}`, 1)
			requireComputeSample(t, samples, `dzkr_compute_slot_hold_duration_seconds_count{owner="job"}`, 1)
		})
	}
}

type completedCUDAProbe struct {
	result cudahealth.Result
	err    error
}

func runProbeAsync(ctx context.Context, reg *workerheartbeat.Registration, worker *WorkerService) <-chan completedCUDAProbe {
	done := make(chan completedCUDAProbe, 1)
	go func() {
		result, err := cudahealth.Probe(ctx, reg, worker.cudaVector)
		done <- completedCUDAProbe{result, err}
	}()
	return done
}

func TestCUDAHealthProbeCancellationAndCapacity(t *testing.T) {
	slot, endpoint := computeMetrics(t)
	worker, dir, _ := probeFixtureWorker(t, "hold", slot)
	beats := make(chan *runtimepb.CudaHealthObservation, 8)
	service := shutdownCoordinator{
		register: func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
			return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: bytes.Repeat([]byte{2}, 32)}, nil
		},
		heartbeat: func(_ context.Context, req *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
			select {
			case beats <- req.GetCudaHealth():
			default:
			}
			return &runtimepb.HeartbeatResponse{Accepted: true, CudaHealthProbeAnchor: 17}, nil
		},
	}
	address, ca, _, _, _ := startShutdownCoordinator(t, service)
	transport := shutdownClientCredentials(t, ca, address)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reg := probeRegistration()
	if err := registerWithCoordinator(ctx, reg, address, transport); err != nil {
		t.Fatal(err)
	}
	conn, err := newCoordinatorConnection(address, transport)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Complete the RPC synchronously so admission has the first anchor.
	if err := reg.Send(ctx, runtimepb.NewCoordinatorServiceClient(conn)); err != nil {
		t.Fatal(err)
	}
	if first := awaitWorker(t, beats); first != nil {
		t.Fatal("unexpected health before probe")
	}
	// Use a second independently admitted registration to demonstrate that the
	// slot, as well as the single-check guard, prevents overlapping children.
	contender := probeRegistration()
	client := &fakeCoordinatorClient{
		register:  service.register,
		heartbeat: service.heartbeat,
	}
	if err := contender.Register(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := contender.Send(ctx, client); err != nil {
		t.Fatal(err)
	}
	awaitWorker(t, beats)
	probeCtx, stopProbe := context.WithCancel(ctx)
	defer stopProbe()
	done := runProbeAsync(probeCtx, reg, worker)
	pid := awaitExternalSquareMarker(t, dir, "ready")
	if probe, err := cudahealth.Probe(ctx, reg, worker.cudaVector); err != nil || probe.State != noObservation {
		t.Fatalf("overlapping check on one registration: %+v, %v", probe, err)
	}
	if probe, err := cudahealth.Probe(ctx, contender, worker.cudaVector); !errors.Is(err, externalcompute.ErrBusy) || probe.State != noObservation {
		t.Fatalf("overlapping check ignored shared slot: %+v, %v", probe, err)
	}
	if _, err := worker.ExecuteJob(ctx, externalSquareRequest("0", 4000)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("external job bypassed the probe slot: %v", err)
	}
	if _, err := worker.ExecuteJob(ctx, cudaVectorRequest("0", 4000)); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("CUDA job bypassed the probe slot: %v", err)
	}
	held := slot.Snapshot()
	if !held.Occupied || held.Owner != metrics.ComputeProbe || held.HoldAge <= 0 ||
		held.Denials(metrics.ComputeJob, metrics.ComputeProbe) != 2 {
		t.Fatalf("probe hold and denials: %+v", held)
	}
	samples := requireComputeScrape(t, endpoint)
	requireComputeSample(t, samples, `dzkr_compute_slot_occupied{owner="probe"}`, 1)
	requireComputeSample(t, samples, `dzkr_compute_slot_occupied{owner="job"}`, 0)
	requireComputeSample(t, samples, `dzkr_compute_slot_denials_total{holder="probe",requester="job"}`, 2)
	requireComputeSample(t, samples, `dzkr_compute_slot_denials_total{holder="probe",requester="probe"}`, 1)
	requireComputeSample(t, samples, `dzkr_compute_slot_hold_duration_seconds_count{owner="probe"}`, 0)
	if samples[`dzkr_compute_slot_hold_age_seconds{owner="probe"}`] < held.HoldAge.Seconds() {
		t.Fatal("scraped probe age is below the preceding snapshot")
	}

	beatDone := make(chan error, 1)
	go func() { beatDone <- runHeartbeat(ctx, reg, address, transport) }()
	if obs := awaitWorker(t, beats); obs != nil {
		t.Fatal("heartbeat reported health while the child was still blocked")
	}
	if _, _, err := readComputeChild(pid); err != nil {
		t.Fatalf("child was not running during the heartbeat: %v", err)
	}
	// Scrape across cancellation and release. Each scrape must report at most
	// one holder even if it runs concurrently with slot and probe updates.
	scraped := make(chan error, 1)
	go func() {
		for range 12 {
			samples, err := scrapeCompute(endpoint)
			if err != nil {
				scraped <- err
				return
			}
			if samples[`dzkr_compute_slot_occupied{owner="probe"}`]+samples[`dzkr_compute_slot_occupied{owner="job"}`] > 1 {
				scraped <- errors.New("simultaneous slot owners")
				return
			}
		}
		scraped <- nil
	}()
	stopProbe()
	got := awaitWorker(t, done)
	assertInterruptedProbe(t, dir, pid, got, context.Canceled)
	if err := awaitWorker(t, scraped); err != nil {
		t.Fatal(err)
	}
	assertCompletedProbeMetrics(t, slot, endpoint, 3)
	requireComputeSample(t, requireComputeScrape(t, endpoint), `dzkr_compute_slot_denials_total{holder="probe",requester="job"}`, 2)
	if obs := awaitWorker(t, beats); obs != nil {
		t.Fatal("cancelled probe produced a health observation")
	}
	cancel()
	if err := awaitWorker(t, beatDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("heartbeat shutdown: %v", err)
	}
	assertProbeSlotReleased(t, worker)
}

func assertInterruptedProbe(t *testing.T, dir string, pid int, got completedCUDAProbe, want error) {
	t.Helper()
	if !errors.Is(got.err, want) || got.result.State != noObservation {
		t.Fatalf("interrupted probe: %+v, %v", got.result, got.err)
	}
	if gotPID := awaitExternalSquareMarker(t, dir, "term"); gotPID != pid {
		t.Fatal("TERM marker from the wrong child")
	}
	if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("probe child still present after return: %v", err)
	}
}

func TestCUDAHealthProbeTimeoutAndStaleCompletion(t *testing.T) {
	for _, mode := range []string{"hold", "held ready"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			slot, endpoint := computeMetrics(t)
			worker, dir, _ := probeFixtureWorker(t, mode, slot)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reg := probeRegistration()
			token := bytes.Repeat([]byte{2}, 32)
			unknown := false
			var obs *runtimepb.CudaHealthObservation
			client := &fakeCoordinatorClient{
				register: func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
					return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: token}, nil
				},
				heartbeat: func(_ context.Context, req *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
					obs = req.GetCudaHealth()
					return &runtimepb.HeartbeatResponse{Accepted: !unknown, CudaHealthProbeAnchor: 17}, nil
				},
			}
			if err := reg.Register(ctx, client); err != nil {
				t.Fatal(err)
			}
			if err := reg.Send(ctx, client); err != nil {
				t.Fatal(err)
			}
			done := runProbeAsync(ctx, reg, worker)
			pid := awaitExternalSquareMarker(t, dir, "ready")
			if mode == "held ready" {
				unknown, token = true, bytes.Repeat([]byte{3}, 32)
				if err := reg.Send(ctx, client); err != nil {
					t.Fatal(err)
				}
				unknown = false
				if err := reg.Send(ctx, client); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
					t.Fatal(err)
				}
				if got := awaitWorker(t, done); got.err != nil || got.result.State != noObservation || got.result.Output != "" {
					t.Fatalf("stale completion was published: %+v, %v", got.result, got.err)
				}
			} else {
				assertInterruptedProbe(t, dir, pid, awaitWorker(t, done), context.DeadlineExceeded)
			}
			if err := reg.Send(ctx, client); err != nil || obs != nil {
				t.Fatalf("interrupted or stale probe was sent: %v", err)
			}
			assertCompletedProbeMetrics(t, slot, endpoint, 1)
			assertProbeSlotReleased(t, worker)
		})
	}
}

func TestCUDAHealthProbeBusyMetrics(t *testing.T) {
	slot, endpoint := computeMetrics(t)
	worker, _, squareDir := probeFixtureWorker(t, "ready", slot)
	reg := probeRegistration()
	var observation *runtimepb.CudaHealthObservation
	client := &fakeCoordinatorClient{
		register: func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
			return &runtimepb.RegisterWorkerResponse{Accepted: true, SessionToken: bytes.Repeat([]byte{2}, 32)}, nil
		},
		heartbeat: func(_ context.Context, request *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
			observation = request.GetCudaHealth()
			return &runtimepb.HeartbeatResponse{Accepted: true, CudaHealthProbeAnchor: 17}, nil
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := reg.Register(ctx, client); err != nil {
		t.Fatal(err)
	}
	if err := reg.Send(ctx, client); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		result, err := worker.ExecuteJob(ctx, externalSquareRequest("7", 4000))
		if err == nil && (result.GetStatus() != "Succeeded" || result.GetOutput() != "49") {
			err = errors.New("job did not return the expected result")
		}
		done <- err
	}()
	awaitExternalSquareMarker(t, squareDir, "ready")
	if got, err := cudahealth.Probe(ctx, reg, worker.cudaVector); !errors.Is(err, externalcompute.ErrBusy) || got.State != noObservation {
		t.Fatalf("probe while a job held the slot: %+v, %v", got, err)
	}
	if err := reg.Send(ctx, client); err != nil || observation != nil {
		t.Fatalf("busy probe published an observation: %v", err)
	}
	if held := slot.Snapshot(); !held.Occupied || held.Owner != metrics.ComputeJob || held.Denials(metrics.ComputeProbe, metrics.ComputeJob) != 1 {
		t.Fatalf("job hold and denial: %+v", held)
	}
	samples := requireComputeScrape(t, endpoint)
	requireComputeSample(t, samples, `dzkr_compute_slot_occupied{owner="job"}`, 1)
	requireComputeSample(t, samples, `dzkr_compute_slot_denials_total{holder="job",requester="probe"}`, 1)
	requireComputeSample(t, samples, `dzkr_compute_slot_denials_total{holder="probe",requester="probe"}`, 0)
	requireComputeSample(t, samples, `dzkr_compute_slot_hold_duration_seconds_count{owner="probe"}`, 0)
	requireComputeSample(t, samples, `dzkr_cuda_probe_outcomes_total{outcome="no_observation"}`, 1)
	requireComputeSample(t, samples, `dzkr_cuda_probe_last_outcome{outcome="no_observation"}`, 1)

	if err := os.WriteFile(filepath.Join(squareDir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := awaitWorker(t, done); err != nil {
		t.Fatal(err)
	}
	if idle := slot.Snapshot(); idle.Occupied || idle.Denials(metrics.ComputeProbe, metrics.ComputeJob) != 1 {
		t.Fatalf("job release lost attribution: %+v", idle)
	}
	samples = requireComputeScrape(t, endpoint)
	requireComputeSample(t, samples, `dzkr_compute_slot_occupied{owner="job"}`, 0)
	requireComputeSample(t, samples, `dzkr_compute_slot_hold_duration_seconds_count{owner="job"}`, 1)
}

func computeMetrics(t *testing.T) (*externalcompute.Slot, string) {
	t.Helper()
	registry := prom.NewRegistry()
	slot, err := prommetrics.NewWorkerSlot(registry)
	if err != nil {
		t.Fatal(err)
	}
	server, err := prommetrics.StartHTTP("127.0.0.1:0", registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return slot, "http://" + server.Addr() + "/metrics"
}

func scrapeCompute(endpoint string) (map[string]float64, error) {
	client := http.Client{Timeout: 2 * time.Second}
	response, err := client.Get(endpoint)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics HTTP status: %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 32<<10))
	if err != nil {
		return nil, err
	}
	samples := make(map[string]float64)
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "dzkr_") {
			continue
		}
		series, raw, _ := strings.Cut(line, " ")
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, err
		}
		samples[series] = value
	}
	return samples, nil
}

func requireComputeScrape(t *testing.T, endpoint string) map[string]float64 {
	t.Helper()
	samples, err := scrapeCompute(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	return samples
}

func requireComputeSample(t *testing.T, samples map[string]float64, series string, want float64) {
	t.Helper()
	got, found := samples[series]
	if !found || got != want {
		t.Fatalf("scraped %s = %g (present: %t), want %g", series, got, found, want)
	}
	t.Logf("scraped %s %g", series, got)
}

func assertCompletedProbeMetrics(t *testing.T, slot *externalcompute.Slot, endpoint string, noObservations float64) {
	t.Helper()
	if idle := slot.Snapshot(); idle.Occupied || idle.LastHold <= 0 || idle.HoldAge != 0 {
		t.Fatalf("probe release: %+v", idle)
	}
	samples := requireComputeScrape(t, endpoint)
	requireComputeSample(t, samples, `dzkr_compute_slot_occupied{owner="probe"}`, 0)
	requireComputeSample(t, samples, `dzkr_compute_slot_hold_age_seconds{owner="probe"}`, 0)
	requireComputeSample(t, samples, `dzkr_compute_slot_hold_duration_seconds_count{owner="probe"}`, 1)
	requireComputeSample(t, samples, `dzkr_cuda_probe_outcomes_total{outcome="no_observation"}`, noObservations)
	if samples[`dzkr_compute_slot_hold_duration_seconds_sum{owner="probe"}`] <= 0 {
		t.Fatal("completed probe hold recorded no time")
	}
}
