package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func startExecutionWorker(t *testing.T, ca *testpki.CA, service runtimepb.WorkerServiceServer) (string, *workerTransport) {
	t.Helper()
	files := ca.Issue(t, testpki.Worker, nil)
	transport, err := loadWorkerTransport("worker-a", "localhost:50050", rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator})
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.newServer(service, newWorkerLifecycle())
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); <-done })
	return listener.Addr().String(), transport
}
func executionCoordinatorClient(t *testing.T, ca *testpki.CA, address string) runtimepb.WorkerServiceClient {
	t.Helper()
	files := ca.Issue(t, testpki.Coordinator, nil)
	p, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
	material, err := rpcauth.LoadMaterial(files.Cert, files.Key, files.CA, p)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := material.WorkerCredentials("worker-a", address)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(transport))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return runtimepb.NewWorkerServiceClient(conn)
}

func TestWorkerExecutionMTLSAuthorizedTasks(t *testing.T) {
	ca := testpki.New(t)
	address, _ := startExecutionWorker(t, ca, &WorkerService{})
	client := executionCoordinatorClient(t, ca, address)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct{ task, payload string }{{"hash", "hello"}, {"sleep", "0s"}} {
		r, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 11001, AttemptId: 7, TaskType: tc.task, Payload: tc.payload, TimeoutMs: 1000})
		if err != nil || r.GetStatus() != "Succeeded" || r.GetJobId() != 11001 || r.GetAttemptId() != 7 {
			t.Fatalf("authorized task: %v", err)
		}
	}

}

func TestWorkerExecutionMTLSRejectsBeforeBusiness(t *testing.T) {
	ca := testpki.New(t)
	var calls atomic.Int32
	worker := &WorkerService{}
	service := shutdownExecutionService{execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return worker.ExecuteJob(ctx, req)
	}}
	address, _ := startExecutionWorker(t, ca, service)
	req := &runtimepb.ExecuteJobRequest{JobId: 11003, AttemptId: 1, TaskType: "sleep", Payload: "1ms", TimeoutMs: 2000}

	for _, tc := range []struct {
		name, identity                string
		change                        func(*x509.Certificate)
		untrusted, missing, plaintext bool
	}{
		{name: "Client cannot execute", identity: testpki.Client},
		{name: "Worker cannot execute or forge metadata", identity: testpki.Worker},
		{name: "other Worker cannot execute", identity: "spiffe://distributedzk.local/worker/other"},
		{name: "unauthorized Coordinator", identity: testpki.Coordinator, change: func(c *x509.Certificate) {
			u, _ := url.Parse("spiffe://distributedzk.local/coordinator/other")
			c.URIs = []*url.URL{u}
		}},
		{name: "missing certificate", missing: true}, {name: "untrusted certificate", identity: testpki.Coordinator, untrusted: true},
		{name: "expired", identity: testpki.Coordinator, change: func(c *x509.Certificate) { c.NotAfter = time.Now().Add(-time.Minute) }},
		{name: "future", identity: testpki.Coordinator, change: func(c *x509.Certificate) { c.NotBefore = time.Now().Add(time.Minute) }},
		{name: "wrong EKU", identity: testpki.Coordinator, change: func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} }},
		{name: "plaintext", plaintext: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var transport credentials.TransportCredentials
			if tc.plaintext {
				transport = insecure.NewCredentials()
			} else {
				config := &tls.Config{MinVersion: tls.VersionTLS13, RootCAs: ca.Roots(), ServerName: "127.0.0.1"}
				if !tc.missing {
					issuer := ca
					if tc.untrusted {
						issuer = testpki.New(t)
					}
					files := issuer.Issue(t, tc.identity, tc.change)
					config.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &files.Pair, nil }
				}
				transport = credentials.NewTLS(config)
			}
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(transport))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("role", "coordinator", "identity", testpki.Coordinator))
			response, err := runtimepb.NewWorkerServiceClient(conn).ExecuteJob(ctx, req)
			if err == nil || response != nil {
				t.Fatal("forbidden caller entered business execution or returned a task-level result")
			}
			if calls.Load() != 0 {
				t.Fatal("unauthorized caller reached the business handler")
			}
		})
	}
	client := executionCoordinatorClient(t, ca, address)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Unknown methods also reject before the Worker business handler.
	connFiles := ca.Issue(t, testpki.Coordinator, nil)
	p, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
	material, err := rpcauth.LoadMaterial(connFiles.Cert, connFiles.Key, connFiles.CA, p)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := material.WorkerCredentials("worker-a", address)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(transport))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.Invoke(ctx, "/runtime.WorkerService/Unknown", req, &runtimepb.ExecuteJobResponse{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("unknown method: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("unknown method had task side effects")
	}
	// Authentication cannot bypass strict Worker-side payload validation.
	req.Payload = "invalid duration"
	response, err := client.ExecuteJob(ctx, req)
	if err != nil || response.GetStatus() != "failed" || response.GetError() == "" || calls.Load() != 1 {
		t.Fatal("authorized malformed payload did not return a task failure")
	}
}

func TestWorkerExecutionMTLSStartupValidation(t *testing.T) {
	ca := testpki.New(t)
	for _, tc := range []struct {
		name   string
		change func(*x509.Certificate)
	}{
		{"ClientAuth only", func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth} }},
		{"ServerAuth only", func(c *x509.Certificate) { c.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			files := ca.Issue(t, testpki.Worker, tc.change)
			config := rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator}
			if transport, err := loadWorkerTransport("worker-a", "localhost:50050", config); err == nil || transport != nil {
				t.Fatal("single-use Worker certificate accepted")
			}
		})
	}
	if transport, err := loadWorkerTransport("worker-a", "localhost:50050", rpcauth.CoordinatorClientConfig{ServerIdentity: testpki.Coordinator}); err == nil || transport != nil {
		t.Fatal("missing startup material accepted")
	}
}
