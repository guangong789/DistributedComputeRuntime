package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/guangong789/DistributedComputeRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedComputeRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type diagnosticTransportFixture struct {
	runtimepb.UnimplementedCoordinatorServiceServer
	listed, diagnosed, submitted, observed atomic.Int32
	oversized                              atomic.Bool
}

func (s *diagnosticTransportFixture) ListWorkers(context.Context, *runtimepb.ListWorkersRequest) (*runtimepb.ListWorkersResponse, error) {
	s.listed.Add(1)
	id := "worker-fixture"
	if s.oversized.Load() {
		id = strings.Repeat("w", maxDiagnosticResponseBytes)
	}
	return &runtimepb.ListWorkersResponse{Workers: []*runtimepb.WorkerDiagnostic{{WorkerId: id}}}, nil
}

func (s *diagnosticTransportFixture) DiagnoseJob(_ context.Context, req *runtimepb.DiagnoseJobRequest) (*runtimepb.DiagnoseJobResponse, error) {
	s.diagnosed.Add(1)
	if req.JobId != 42 {
		return nil, status.Error(codes.NotFound, "private fixture detail")
	}
	return &runtimepb.DiagnoseJobResponse{JobId: req.JobId, State: runtimepb.JobState_JOB_STATE_QUEUED, TaskType: "hash"}, nil
}

func (s *diagnosticTransportFixture) GetJob(_ context.Context, req *runtimepb.GetJobRequest) (*runtimepb.GetJobResponse, error) {
	s.observed.Add(1)
	return &runtimepb.GetJobResponse{JobId: req.JobId, State: runtimepb.JobState_JOB_STATE_SUCCEEDED, AttemptId: 1, HasWorkerResult: true}, nil
}

func (s *diagnosticTransportFixture) SubmitJob(_ context.Context, req *runtimepb.SubmitJobRequest) (*runtimepb.SubmitJobResponse, error) {
	s.submitted.Add(1)
	return &runtimepb.SubmitJobResponse{JobId: req.JobId, Status: "Accepted"}, nil
}

func TestDiagnosticCLIMTLSAndLegacyCommands(t *testing.T) {
	const ordinaryURI = "spiffe://distributedzk.local/client/ordinary"
	const unlistedURI = "spiffe://distributedzk.local/client/unlisted"
	ca := testpki.New(t)
	serverFiles := ca.Issue(t, testpki.Coordinator, nil)
	coordinator, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
	diagnostic, _ := rpcauth.ParsePrincipal(testpki.Client)
	ordinary, _ := rpcauth.ParsePrincipal(ordinaryURI)
	policy, err := rpcauth.NewPolicy(coordinator, []rpcauth.Principal{diagnostic, ordinary}, nil, diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	material, err := rpcauth.LoadMaterial(serverFiles.Cert, serverFiles.Key, serverFiles.CA, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig, err := rpcauth.NewServerTLS(material, policy)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.UnaryInterceptor(policy.UnaryInterceptor()))
	fixture := &diagnosticTransportFixture{}
	runtimepb.RegisterCoordinatorServiceServer(server, fixture)
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })

	cliArgs := func(identity string) []string {
		t.Helper()
		files := ca.Issue(t, identity, nil)
		return []string{"-coordinator", listener.Addr().String(), "-tls-cert", files.Cert, "-tls-key", files.Key, "-tls-ca", files.CA, "-identity", identity}
	}
	check := func(args []string, expectedCode int, expected string) {
		t.Helper()
		var out, stderr bytes.Buffer
		code := runCLI(t.Context(), args, &out, &stderr)
		if code != expectedCode {
			t.Fatalf("unexpected exit %d, want %d: %s", code, expectedCode, &stderr)
		}
		if code == 0 {
			if stderr.Len() != 0 {
				t.Fatal("successful command wrote to stderr")
			}
			requireDiagnosticText(t, out.String(), expected)
		} else {
			if out.Len() != 0 || strings.Contains(stderr.String(), "private fixture detail") ||
				strings.Contains(stderr.String(), "private-marker") || strings.Contains(stderr.String(), "key.pem") {
				t.Fatal("failure printed a snapshot or leaked raw details")
			}
			requireText(t, stderr.String(), expected)
		}
	}

	operator := cliArgs(testpki.Client)
	standard := cliArgs(ordinaryURI)
	unlisted := cliArgs(unlistedURI)
	check(append(operator, "workers"), 0, "[Unknown] worker-fixture")
	check(append(operator, "diagnose", "job", "42"), 0, "Job 42 [Queued]")
	if fixture.listed.Load() != 1 || fixture.diagnosed.Load() != 1 || fixture.submitted.Load() != 0 || fixture.observed.Load() != 0 {
		t.Fatal("diagnostic command made extra RPCs")
	}
	check(append(standard, "workers"), 1, "PermissionDenied")
	check(append(standard, "diagnose", "job", "42"), 1, "PermissionDenied")
	check(append(unlisted, "workers"), 1, "mTLS handshake failed")
	check(append(operator, "-coordinator-server-name=private-marker.invalid", "workers"), 1, "mTLS handshake failed")
	if fixture.listed.Load() != 1 || fixture.diagnosed.Load() != 1 {
		t.Fatal("rejected peer reached a diagnostic handler")
	}
	check(append(operator, "diagnose", "job", "0"), 1, "NotFound")
	fixture.oversized.Store(true)
	check(append(operator, "workers"), 1, "ResourceExhausted")
	if fixture.listed.Load() != 2 || fixture.diagnosed.Load() != 2 {
		t.Fatal("failed diagnostic was retried")
	}
	check(append(standard, "-observe-job=42"), 0, "job=42 status=Succeeded")
	check(append(standard, "-job-timeout=2s", "-observe-timeout=2s"), 0, "job=3001 status=Succeeded")
	if fixture.observed.Load() != 2 || fixture.submitted.Load() != 1 {
		t.Fatal("legacy default or observation workflow changed")
	}
	t.Log("mTLS handler counts: one call per successful diagnostic, zero calls for unlisted or unauthorized peers, one call per authenticated failure")
}
