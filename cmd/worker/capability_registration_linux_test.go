package main

import (
	"context"
	"path/filepath"
	"slices"
	"testing"

	"github.com/guangong789/DistributedComputeRuntime/internal/cudavector"
	"github.com/guangong789/DistributedComputeRuntime/internal/externalsquare"
	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
)

func TestWorkerCapabilityAdvertisement(t *testing.T) {
	for _, tc := range []struct {
		name         string
		square, cuda bool
		want         []string
	}{
		{name: "builtins only"},
		{name: "external CPU only", square: true, want: []string{externalsquare.TaskType}},
		{name: "CUDA only", cuda: true, want: []string{cudavector.TaskType}},
		{name: "both external tasks", square: true, cuda: true, want: []string{externalsquare.TaskType, cudavector.TaskType}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registrations := make(chan *runtimepb.RegisterWorkerRequest, 2)
			service := shutdownCoordinator{
				register: func(ctx context.Context, req *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
					select {
					case registrations <- req:
						return &runtimepb.RegisterWorkerResponse{Accepted: true}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				},
				heartbeat: func(context.Context, *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
					// Model the Coordinator losing the registration.
					return &runtimepb.HeartbeatResponse{Accepted: false}, nil
				},
			}
			address, ca, _, _, _ := startShutdownCoordinator(t, service)
			cfg := workerTestConfig(t, ca, address)
			if tc.square {
				cfg.ExternalSquarePath = computeFixturePath(t, t.TempDir(), externalSquareFixtureName)
			}
			if tc.cuda {
				cfg.CUDAVectorPath = computeFixturePath(t, t.TempDir(), cudaVectorFixtureName)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- runWorker(ctx, cfg) }()
			for _, phase := range []string{"initial registration", "heartbeat re-registration"} {
				got := awaitWorker(t, registrations)
				if got.GetWorkerId() != cfg.ID || got.GetAddress() != cfg.AdvertiseAddress ||
					!slices.Equal(got.GetExternalTaskTypes(), tc.want) {
					t.Errorf("%s=%v, want %v", phase, got, tc.want)
				}
			}
			cancel()
			if err := awaitWorker(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}

	t.Run("bad CUDA executable", func(t *testing.T) {
		service := shutdownCoordinator{register: func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
			t.Error("invalid executable reached Worker registration")
			return &runtimepb.RegisterWorkerResponse{Accepted: false}, nil
		}}
		address, ca, _, _, _ := startShutdownCoordinator(t, service)
		cfg := workerTestConfig(t, ca, address)
		cfg.CUDAVectorPath = filepath.Join(t.TempDir(), "missing")
		if err := runWorker(t.Context(), cfg); err == nil {
			t.Fatal("Worker accepted invalid CUDA configuration")
		}
	})
}
