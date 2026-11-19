package main

import (
	"context"
	"errors"
	"flag"
	"strconv"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/jobobservation"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

type clientExecutionConfig struct {
	JobTimeout     time.Duration
	RPCTimeout     time.Duration
	ObserveTimeout time.Duration
	PollInterval   time.Duration
	ObserveJobID   *int64
}

func clientExecutionFlags(fs *flag.FlagSet) *clientExecutionConfig {
	c := &clientExecutionConfig{}
	fs.DurationVar(&c.JobTimeout, "job-timeout", 5*time.Second, "positive Worker task timeout in whole milliseconds")
	fs.DurationVar(&c.RPCTimeout, "rpc-timeout", 30*time.Second, "positive per-RPC timeout for admission and GetJob")
	fs.DurationVar(&c.ObserveTimeout, "observe-timeout", 30*time.Second, "positive total GetJob observation timeout; expiration does not cancel the Job")
	fs.DurationVar(&c.PollInterval, "poll-interval", jobobservation.DefaultPollInterval, "positive interval between GetJob queries")
	fs.Func("observe-job", "observe this signed JobID without submitting (for reconnect or lost admission acknowledgement)", func(value string) error {
		id, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			c.ObserveJobID = &id
		}
		return err
	})
	return c
}

func (c clientExecutionConfig) validate() error {
	if c.JobTimeout < time.Millisecond || c.JobTimeout%time.Millisecond != 0 {
		return errors.New("-job-timeout must be positive and representable in whole milliseconds")
	}
	if c.RPCTimeout <= 0 {
		return errors.New("-rpc-timeout must be positive")
	}
	if c.ObserveTimeout <= 0 || c.PollInterval <= 0 {
		return errors.New("-observe-timeout and -poll-interval must be positive")
	}
	return nil
}

// The Worker timeout and parent RPC deadline remain independent of the
// Coordinator's configured attempt lease. time.Duration bounds conversion to
// milliseconds, and validation prevents lossy sub-millisecond rounding.
func submitDemo(ctx context.Context, client runtimepb.CoordinatorServiceClient, c clientExecutionConfig) (*runtimepb.SubmitJobResponse, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, c.RPCTimeout)
	defer cancel()
	return client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
		JobId: 3001, TaskType: "hash", Payload: "Distributed Compute Runtime client",
		TimeoutMs: c.JobTimeout.Milliseconds(),
	})
}
