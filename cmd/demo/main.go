// Command demo runs a temporary, authenticated hash-only Coordinator/Worker pair.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"go/version"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/guangong789/DistributedComputeRuntime/internal/jobobservation"
	"github.com/guangong789/DistributedComputeRuntime/internal/rpcauth"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const clientIdentity = "spiffe://distributedzk.local/client/client-1"
const workerIdentity = "spiffe://distributedzk.local/worker/worker-1"

func main() {
	showCLI := flag.Bool("show-cli", false, "show the Diagnostics CLI using temporary authorized mTLS credentials")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()
	if err := runDemo(ctx, os.Stdout, *showCLI); err != nil {
		fmt.Fprintln(os.Stderr, "Demo failed:", err)
		os.Exit(1)
	}
}

func offlineEnv() []string {
	env := make([]string, 0, len(os.Environ())+5)
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		switch name {
		case "GOTOOLCHAIN", "GOPROXY", "GOSUMDB", "GOMAXPROCS", "GOFLAGS":
			continue
		}
		env = append(env, item)
	}
	return append(env, "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "GOMAXPROCS=2", "GOFLAGS=")
}

func prerequisites(ctx context.Context, repo string) error {
	if runtime.GOOS != "linux" {
		return errors.New("this demo requires Linux/WSL and GNU utilities")
	}
	for _, tool := range []string{"go", "bash", "openssl", "git", "realpath", "mktemp", "stat", "cmp", "ln", "chmod", "mkdir", "rm", "grep", "dirname"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("missing prerequisite: %s", tool)
		}
	}
	data, err := os.ReadFile(filepath.Join(repo, "go.mod"))
	if err != nil {
		return errors.New("run the demo from the repository root")
	}
	if !strings.HasPrefix(string(data), "module github.com/guangong789/DistributedComputeRuntime\n") {
		return errors.New("unexpected repository module")
	}
	required := ""
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "go ") {
			required = "go" + strings.TrimSpace(strings.TrimPrefix(line, "go "))
		}
	}
	checks := []struct {
		program string
		args    []string
		check   func(string) bool
	}{
		{"go", []string{"env", "GOVERSION"}, func(s string) bool {
			return version.IsValid(required) && version.Compare(strings.TrimSpace(s), required) >= 0
		}},
		{"openssl", []string{"version"}, func(s string) bool { return strings.HasPrefix(s, "OpenSSL 3.") }},
	}
	for _, tool := range []string{"realpath", "mktemp", "stat", "cmp", "ln", "chmod", "mkdir", "rm", "grep", "dirname"} {
		checks = append(checks, struct {
			program string
			args    []string
			check   func(string) bool
		}{tool, []string{"--version"}, func(s string) bool { return strings.Contains(s, "GNU") }})
	}
	for _, check := range checks {
		cmd := exec.CommandContext(ctx, check.program, check.args...)
		cmd.Env = offlineEnv()
		if output, err := cmd.Output(); err != nil || !check.check(string(output)) {
			return fmt.Errorf("prerequisite version check failed: %s", check.program)
		}
	}
	info, err := os.Stat(filepath.Join(repo, "scripts/dev-pki/bootstrap.sh"))
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("development PKI bootstrap script missing")
	}
	return ctx.Err()
}

func stage(ctx context.Context, duration time.Duration, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	return fn(ctx)
}

func connect(ctx context.Context, address, pki string, coordinator *child) (*grpc.ClientConn, error) {
	principal, err := rpcauth.ParsePrincipal(clientIdentity)
	if err != nil {
		return nil, err
	}
	creds, err := (rpcauth.CoordinatorClientConfig{
		CertFile: filepath.Join(pki, "client-1/cert.pem"), KeyFile: filepath.Join(pki, "client-1/key.pem"),
		CAFile: filepath.Join(pki, "ca/ca.crt"), ServerIdentity: rpcauth.CoordinatorIdentity,
	}).Credentials(address, principal)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds), grpc.WithPerRPCCredentials(rpcauth.PeerValidityCredentials{}))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-coordinator.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			if err := coordinator.alive(); err == nil {
				return conn, nil
			}
			break
		}
		if !conn.WaitForStateChange(ctx, state) {
			break
		}
	}
	conn.Close()
	return nil, fmt.Errorf("authenticated Coordinator readiness failed: %w", errors.Join(ctx.Err(), coordinator.alive()))
}

func runDemo(ctx context.Context, out io.Writer, showCLI bool) (result error) {
	fmt.Fprintln(out, "Distributed Compute Runtime | Local Distributed Demo\nReal Coordinator + Worker | gRPC/mTLS + SQLite\n\nInfrastructure")
	repo, err := os.Getwd()
	if err != nil {
		return err
	}
	if err := stage(ctx, 10*time.Second, func(ctx context.Context) error { return prerequisites(ctx, repo) }); err != nil {
		return err
	}
	// Never honor a TMPDIR that could place private demo material in the repo.
	workspace, err := os.MkdirTemp("/tmp", "dzkr-demo-")
	if err != nil {
		return err
	}
	var children []*child
	defer func() {
		for i := len(children) - 1; i >= 0; i-- {
			result = errors.Join(result, children[i].stop())
		}
		if err := os.RemoveAll(workspace); err != nil {
			result = errors.Join(result, fmt.Errorf("remove temporary workspace: %w", err))
		}
		if result == nil {
			fmt.Fprintln(out, "\nCleanup\n  [6/6] [OK] Owned children reaped; temporary workspace removed\n\nDemo completed successfully.")
		}
	}()
	// MkdirTemp uses 0700. Resolve /tmp aliases before invoking the PKI script,
	// which deliberately refuses symlink components in credential output paths.
	resolvedWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return err
	}
	workspace = resolvedWorkspace
	pki := filepath.Join(workspace, "pki")
	if err := stage(ctx, 30*time.Second, func(ctx context.Context) error {
		return runTool(ctx, repo, filepath.Join(workspace, "pki.log"), "bash", filepath.Join(repo, "scripts/dev-pki/bootstrap.sh"), "--out", pki)
	}); err != nil {
		return fmt.Errorf("PKI preparation: %w", err)
	}
	buildArgs := []string{"build", "-mod=readonly", "-p=1", "-o", workspace + string(os.PathSeparator), "./cmd/coordinator", "./cmd/worker"}
	if showCLI {
		buildArgs = append(buildArgs, "./cmd/client")
	}
	if err := stage(ctx, 60*time.Second, func(ctx context.Context) error {
		return runTool(ctx, repo, filepath.Join(workspace, "build.log"), "go", buildArgs...)
	}); err != nil {
		return fmt.Errorf("offline build: %w", err)
	}
	fmt.Fprintln(out, "  [1/6] [OK] Prerequisites, isolated PKI and offline build")
	tlsArgs := func(node string) []string {
		return []string{"-tls-cert", filepath.Join(pki, node, "cert.pem"), "-tls-key", filepath.Join(pki, node, "key.pem"), "-tls-ca", filepath.Join(pki, "ca/ca.crt")}
	}
	db := filepath.Join(workspace, "jobs.db")
	args := append([]string{"-listen", "127.0.0.1:0", "-metrics-listen", "127.0.0.1:0", "-shutdown-timeout", "3s", "-db", db, "-allow-client", clientIdentity, "-allow-worker", workerIdentity}, tlsArgs("coordinator")...)
	if showCLI {
		args = append(args, "-allow-diagnostics-client", clientIdentity)
	}
	c, err := startChild(repo, filepath.Join(workspace, "coordinator.log"), "coordinator mTLS listening on ", filepath.Join(workspace, "coordinator"), args...)
	if err != nil {
		return fmt.Errorf("Coordinator start: %w", err)
	}
	children = append(children, c)
	var conn *grpc.ClientConn
	var address string
	if err := stage(ctx, 10*time.Second, func(ctx context.Context) error {
		line, err := c.waitReady(ctx)
		if err != nil {
			return err
		}
		address = strings.TrimPrefix(line, "coordinator mTLS listening on ")
		host, port, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" || port == "0" {
			return errors.New("Coordinator reported invalid loopback endpoint")
		}
		conn, err = connect(ctx, address, pki, c)
		return err
	}); err != nil {
		return fmt.Errorf("Coordinator readiness: %w", err)
	}
	defer conn.Close()
	fmt.Fprintln(out, "  [2/6] [OK] Coordinator authenticated gRPC READY")
	// Worker registration requires its advertised port before startup. Reserve
	// an ephemeral port, then release it; any competing bind fails this run.
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	workerAddress := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		return err
	}
	args = append([]string{"-id", "worker-1", "-listen", workerAddress, "-advertise", workerAddress, "-coordinator", address, "-metrics-listen", "127.0.0.1:0", "-shutdown-timeout", "3s"}, tlsArgs("worker-1")...)
	w, err := startChild(repo, filepath.Join(workspace, "worker.log"), "worker registered successfully", filepath.Join(workspace, "worker"), args...)
	if err != nil {
		return fmt.Errorf("Worker start: %w", err)
	}
	children = append(children, w)
	if err := stage(ctx, 10*time.Second, func(ctx context.Context) error { _, err := w.waitReady(ctx); return err }); err != nil {
		return fmt.Errorf("Worker registration: %w", err)
	}
	fmt.Fprintln(out, "  [3/6] [OK] Worker registered using mTLS")
	runCLI := func(command ...string) error {
		return stage(ctx, 5*time.Second, func(ctx context.Context) error {
			args := append([]string{"-coordinator", address}, tlsArgs("client-1")...)
			cmd := exec.CommandContext(ctx, filepath.Join(workspace, "client"), append(args, command...)...)
			cmd.Env = offlineEnv()
			// Pass the terminal file through so Client decides whether to use color.
			cmd.Stdout = out
			cmd.Stderr = os.Stderr
			cmd.WaitDelay = time.Second
			return cmd.Run()
		})
	}
	if showCLI {
		fmt.Fprintln(out)
		if err := runCLI("workers"); err != nil {
			return fmt.Errorf("diagnostics workers: %w", err)
		}
	}
	req := &runtimepb.SubmitJobRequest{JobId: 1, TaskType: "hash", Payload: "Distributed Compute Runtime local demo", TimeoutMs: 5000}
	fmt.Fprintf(out, "\nTask Execution\n  Submitting task: %s\n", req.TaskType)
	client := runtimepb.NewCoordinatorServiceClient(conn)
	var ack *runtimepb.SubmitJobResponse
	if err := stage(ctx, 5*time.Second, func(ctx context.Context) error {
		var err error
		ack, err = client.SubmitJob(ctx, req)
		return err
	}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	if err := jobobservation.ValidateAdmission(ack, req.JobId); err != nil {
		return err
	}
	fmt.Fprintf(out, "  JobID: %d | Admission: Accepted | Awaiting background execution via GetJob\n", ack.JobId)
	var resp *runtimepb.GetJobResponse
	if err := stage(ctx, 30*time.Second, func(ctx context.Context) error {
		var err error
		resp, err = jobobservation.Wait(ctx, req.JobId, jobobservation.DefaultPollInterval, func(ctx context.Context, id int64) (*runtimepb.GetJobResponse, error) {
			return client.GetJob(ctx, &runtimepb.GetJobRequest{JobId: id})
		})
		return err
	}); err != nil {
		return err
	}
	if err := jobobservation.TerminalError(resp); err != nil {
		return err
	}
	if err := validateResponse(req, resp); err != nil {
		return err
	}
	fmt.Fprintf(out, "  [4/6] [OK] Background hash task completed (GetJob)\n  JobID: %d | AttemptID: %d | Final State: %s | HasWorkerResult: %t\n", resp.JobId, resp.AttemptId, jobobservation.StateName(resp.State), resp.HasWorkerResult)
	if showCLI {
		fmt.Fprintln(out)
		if err := runCLI("diagnose", "job", strconv.FormatInt(resp.JobId, 10)); err != nil {
			return fmt.Errorf("diagnostics diagnose job: %w", err)
		}
	}
	fmt.Fprintf(out, "\nVerification\n  [OK] SHA-256 independently verified by demo\n  SHA-256: %s\n", resp.Output)
	// Stop before opening a read-only SQLite connection; no concurrent writers.
	var shutdownErr error
	for i := len(children) - 1; i >= 0; i-- {
		shutdownErr = errors.Join(shutdownErr, children[i].stop())
	}
	children = nil
	if shutdownErr != nil {
		return fmt.Errorf("service shutdown: %w", shutdownErr)
	}
	if err := stage(ctx, 5*time.Second, func(ctx context.Context) error { return verifySQLite(ctx, db, req) }); err != nil {
		return fmt.Errorf("SQLite verification: %w", err)
	}
	fmt.Fprintln(out, "  [5/6] [OK] SQLite terminal state and full task specification verified")
	return ctx.Err()
}
