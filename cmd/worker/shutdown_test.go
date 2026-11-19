package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func awaitWorker[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("Worker lifecycle barrier timed out")
		var zero T
		return zero
	}
}

type shutdownExecutionService struct {
	runtimepb.UnimplementedWorkerServiceServer
	execute func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error)
}

func (s shutdownExecutionService) ExecuteJob(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
	return s.execute(ctx, req)
}

func shutdownWorker(t *testing.T, service runtimepb.WorkerServiceServer, timeout time.Duration) (*workerProcess, runtimepb.WorkerServiceClient) {
	t.Helper()
	p := newWorkerProcess(timeout)
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Worker, nil)
	transport, err := loadWorkerTransport("worker-a", "localhost:50050", rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator})
	if err != nil {
		t.Fatal(err)
	}
	server, err := transport.newServer(service, p.lifecycle)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p.start(server, listener)
	t.Cleanup(func() { _ = p.shutdown(); awaitWorker(t, p.cleanupDone) })
	return p, executionCoordinatorClient(t, ca, listener.Addr().String())
}

func TestWorkerShutdownConfiguration(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    time.Duration
		invalid bool
	}{
		{"", 30 * time.Second, false}, {"75ms", 75 * time.Millisecond, false}, {"0", 0, true}, {"-1s", 0, true}, {"bad", 0, true}, {"999999999999999999999s", 0, true},
	} {
		t.Run(tc.value, func(t *testing.T) {
			fs := flag.NewFlagSet("worker", flag.ContinueOnError)
			value := workerShutdownFlag(fs)
			var args []string
			if tc.value != "" {
				args = []string{"-shutdown-timeout", tc.value}
			}
			err := fs.Parse(args)
			if err == nil && *value <= 0 {
				err = runWorker(context.Background(), workerConfig{ShutdownTimeout: *value})
			}
			if (err != nil) != tc.invalid || (!tc.invalid && *value != tc.want) {
				t.Fatalf("duration=%v error=%v", *value, err)
			}
		})
	}
}

func TestWorkerShutdownIdle(t *testing.T) {
	p, _ := shutdownWorker(t, &WorkerService{}, time.Second)
	var cleanups atomic.Int32
	p.shutdownMetrics = func(ctx context.Context) error {
		if ctx.Err() != nil || p.lifecycle.ctx.Err() == nil {
			t.Error("cleanup inherited lifecycle cancellation")
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > time.Second {
			t.Error("missing overall cleanup deadline")
		}
		cleanups.Add(1)
		return nil
	}
	if err := p.shutdown(); err != nil {
		t.Fatal(err)
	}
	awaitWorker(t, p.serveDone)
	awaitWorker(t, p.cleanupDone)
	if err := p.shutdown(); err != nil || cleanups.Load() != 1 {
		t.Fatalf("repeat shutdown=%v cleanups=%d", err, cleanups.Load())
	}
}

func TestWorkerShutdownAdmissionAndContexts(t *testing.T) {
	l := newWorkerLifecycle()
	type key struct{}
	parent, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, "kept"), time.Minute)
	defer cancel()
	identity := &peer.Peer{}
	parent = peer.NewContext(metadata.NewIncomingContext(parent, metadata.Pairs("trace", "kept")), identity)
	first, leave, _ := l.tryEnter(parent)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, done, ok := l.tryEnter(parent)
			if ok {
				defer done()
				<-ctx.Done()
			}
		}()
	}
	close(start)
	l.beginShutdown()
	l.cancelWork()
	joined := make(chan struct{})
	go func() { wg.Wait(); close(joined) }()
	awaitWorker(t, joined)
	if _, _, ok := l.tryEnter(parent); ok {
		t.Fatal("admitted while draining")
	}
	md, _ := metadata.FromIncomingContext(first)
	p, _ := peer.FromContext(first)
	d1, _ := first.Deadline()
	d2, _ := parent.Deadline()
	if first.Value(key{}) != "kept" || md.Get("trace")[0] != "kept" || p != identity || d1 != d2 || !errors.Is(first.Err(), context.Canceled) {
		t.Fatal("RPC context semantics changed")
	}
	select {
	case <-l.done:
		t.Fatal("lost active handler")
	default:
	}
	leave()
	leave()
	l.beginShutdown()
	awaitWorker(t, l.done)
	other := newWorkerLifecycle()
	rpc, cancelRPC := context.WithCancel(context.Background())
	child, finish, _ := other.tryEnter(rpc)
	cancelRPC()
	if !errors.Is(child.Err(), context.Canceled) {
		t.Fatal("RPC parent cancellation lost")
	}
	finish()
	other.beginShutdown()
	other.cancelWork()
	awaitWorker(t, other.done)
}

func TestWorkerDrainingRejectsBeforeBusiness(t *testing.T) {
	var calls atomic.Int32
	worker := &WorkerService{}
	p, client := shutdownWorker(t, shutdownExecutionService{execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return worker.ExecuteJob(ctx, req)
	}}, time.Second)
	p.lifecycle.beginShutdown() // Leave transport open to test application rejection.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12001, AttemptId: 1, TaskType: "hash", Payload: "input", TimeoutMs: 1000})
	if resp != nil || status.Code(err) != codes.Unavailable || calls.Load() != 0 {
		t.Fatalf("draining entered business: %v", err)
	}
}

func TestWorkerShutdownCancelsRealTask(t *testing.T) {
	started := make(chan struct{})
	local := make(chan *runtimepb.ExecuteJobResponse, 1)
	service := shutdownExecutionService{execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		close(started)
		resp, err := (&WorkerService{}).ExecuteJob(ctx, req)
		local <- resp
		return resp, err
	}}
	p, client := shutdownWorker(t, service, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12004, AttemptId: 7, TaskType: "sleep", Payload: "30s", TimeoutMs: 60000})
		result <- err
	}()
	awaitWorker(t, started)
	if err := p.shutdown(); err != nil {
		t.Fatal(err)
	}
	resp := awaitWorker(t, local)
	if resp == nil || resp.Status != "Cancelled" || resp.JobId != 12004 || resp.AttemptId != 7 {
		t.Fatalf("cooperative cancellation changed result: %v", resp)
	}
	// Response delivery races transport Stop; the local task must be Cancelled
	// regardless of whether the caller receives that result or a transport error.
	awaitWorker(t, result)
}

func TestWorkerShutdownMetricsBudget(t *testing.T) {
	p := newWorkerProcess(40 * time.Millisecond)
	release := make(chan struct{})
	var once sync.Once
	defer func() { once.Do(func() { close(release) }); awaitWorker(t, p.cleanupDone) }()
	entered := make(chan struct{})
	p.shutdownMetrics = func(ctx context.Context) error {
		if ctx.Err() != nil || p.lifecycle.ctx.Err() == nil {
			t.Error("cleanup context inherits cancellation")
		}
		close(entered)
		<-release
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- p.shutdown() }()
	awaitWorker(t, entered)
	if err := awaitWorker(t, result); !errors.Is(err, errWorkerShutdownIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked cleanup: %v", err)
	}
}

func TestWorkerShutdownWaitsForStartupAndBackground(t *testing.T) {
	for _, phase := range []string{"resource publication", "registration or heartbeat cleanup"} {
		t.Run(phase, func(t *testing.T) {
			p := newWorkerProcess(40 * time.Millisecond)
			release := make(chan struct{})
			var once sync.Once
			defer func() { once.Do(func() { close(release) }); awaitWorker(t, p.cleanupDone) }()
			if phase == "resource publication" {
				p.resourcesReady = release
			} else {
				p.backgroundDone = release
			}
			var cleaned atomic.Bool
			p.shutdownMetrics = func(context.Context) error { cleaned.Store(true); return nil }
			if err := p.shutdown(); !errors.Is(err, errWorkerShutdownIncomplete) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unfinished owner: %v", err)
			}
			if cleaned.Load() {
				t.Fatal("cleaned resources before background owner finished")
			}
			once.Do(func() { close(release) })
			awaitWorker(t, p.cleanupDone)
			if cleaned.Load() {
				t.Fatal("resumed normal cleanup after timeout")
			}
		})
	}
}

var errWorkerListenerCleanupIncomplete = errors.New("listener observation cleanup incomplete")

// Observe the original listener, independently of reachability of its old address.
// An observation failure stays a failure even if cleanup subsequently closes it.
func observeWorkerListenerClosed(listener net.Listener, budget time.Duration) (err error) {
	type acceptResult struct {
		err      error
		accepted bool
	}
	result := make(chan acceptResult, 1)
	acceptDone := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	go func() {
		defer close(acceptDone)
		conn, acceptErr := listener.Accept()
		if conn != nil {
			_ = conn.Close()
		}
		result <- acceptResult{err: acceptErr, accepted: conn != nil}
	}()
	defer func() {
		if err == nil {
			return
		}
		closed := make(chan error, 1)
		go func() { closed <- listener.Close() }()
		cleanup, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
		defer cancelCleanup()
		var pendingAccept <-chan struct{} = acceptDone
		var pendingClose <-chan error = closed
		for pendingAccept != nil || pendingClose != nil {
			select {
			case <-pendingAccept:
				pendingAccept = nil
			case closeErr := <-pendingClose:
				pendingClose = nil
				if closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
					err = errors.Join(err, fmt.Errorf("%w: Close: %v", errWorkerListenerCleanupIncomplete, closeErr))
				}
			case <-cleanup.Done():
				err = errors.Join(err, fmt.Errorf("%w: Accept joined=%t Close returned=%t", errWorkerListenerCleanupIncomplete, pendingAccept == nil, pendingClose == nil))
				return
			}
		}
	}()
	select {
	case observed := <-result:
		select {
		case <-acceptDone:
		case <-ctx.Done():
			return fmt.Errorf("original listener Accept did not join: %w", ctx.Err())
		}
		if observed.accepted || !errors.Is(observed.err, net.ErrClosed) {
			return fmt.Errorf("original listener accepted=%t error=%v; want net.ErrClosed", observed.accepted, observed.err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("original listener closure not observed: %w", ctx.Err())
	}
}

func TestWorkerShutdownListenerClosureObservation(t *testing.T) {
	for _, alreadyClosed := range []bool{true, false} {
		t.Run(map[bool]string{true: "closed_listener", false: "open_listener"}[alreadyClosed], func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			if alreadyClosed {
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
			err = observeWorkerListenerClosed(listener, 100*time.Millisecond)
			if alreadyClosed && err != nil {
				t.Fatalf("closed listener was rejected: %v", err)
			}
			if !alreadyClosed && (!errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errWorkerListenerCleanupIncomplete)) {
				t.Fatalf("open listener must fail observation and join after cleanup: %v", err)
			}
		})
	}
}

func TestWorkerShutdownUnexpectedServeAndPartialStartup(t *testing.T) {
	for _, serving := range []bool{false, true} {
		t.Run(map[bool]string{false: "partial_startup", true: "unexpected_serve"}[serving], func(t *testing.T) {
			p := newWorkerProcess(time.Second)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := p.shutdown(); err != nil {
					t.Errorf("fixture shutdown: %v", err)
				}
				if err := observeWorkerListenerClosed(listener, 200*time.Millisecond); err != nil {
					t.Error(err)
				}
			})
			if serving {
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
				p.start(grpc.NewServer(), listener)
				if err := awaitWorker(t, p.serveResult); err == nil {
					t.Fatal("Serve failure was hidden")
				}
			} else {
				p.listener = listener
			}
			var cleaned atomic.Bool
			p.shutdownMetrics = func(context.Context) error { cleaned.Store(true); return nil }
			if err := p.shutdown(); err != nil || !cleaned.Load() {
				t.Fatalf("partial cleanup: %v", err)
			}
		})
	}
}

type blockingCloseListener struct {
	net.Listener
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (l *blockingCloseListener) Close() error {
	l.once.Do(func() { close(l.entered); <-l.release })
	return l.Listener.Close()
}

func TestWorkerShutdownSupervisesTransportStop(t *testing.T) {
	p := newWorkerProcess(40 * time.Millisecond)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	var once sync.Once
	defer func() { once.Do(func() { close(release) }); awaitWorker(t, p.cleanupDone) }()
	blocked := &blockingCloseListener{Listener: listener, entered: make(chan struct{}), release: release}
	// Serve must enter Accept before Stop to exercise the blocking listener Close.
	accepted := make(chan struct{})
	wrapped := &observedAcceptListener{Listener: blocked, entered: accepted}
	p.start(grpc.NewServer(), wrapped)
	awaitWorker(t, accepted)
	result := make(chan error, 1)
	go func() { result <- p.shutdown() }()
	awaitWorker(t, blocked.entered)
	if err := awaitWorker(t, result); !errors.Is(err, errWorkerShutdownIncomplete) {
		t.Fatalf("Stop blocked controller: %v", err)
	}
	select {
	case <-p.serveDone:
		t.Fatal("Serve completion confused with timeout decision")
	default:
	}
}

type observedAcceptListener struct {
	net.Listener
	entered chan struct{}
	once    sync.Once
}

func (l *observedAcceptListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.entered) })
	return l.Listener.Accept()
}
