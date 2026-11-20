package main

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	diagnosticTimeout          = 2 * time.Second
	maxDiagnosticResponseBytes = 256 << 10
)

type commandKind uint8

const (
	workflowCommand commandKind = iota
	workersCommand
	diagnoseJobCommand
)

type clientCommand struct {
	kind  commandKind
	jobID int64
}

func parseClientCommand(args []string) (clientCommand, error) {
	if len(args) == 0 {
		return clientCommand{kind: workflowCommand}, nil
	}
	switch args[0] {
	case "workers":
		if len(args) != 1 {
			return clientCommand{}, errors.New("usage: client [flags] workers")
		}
		return clientCommand{kind: workersCommand}, nil
	case "diagnose":
		if len(args) != 3 || args[1] != "job" {
			return clientCommand{}, errors.New("usage: client [flags] diagnose job <job-id>")
		}
		id, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return clientCommand{}, errors.New("job ID must be a signed 64-bit decimal integer")
		}
		return clientCommand{kind: diagnoseJobCommand, jobID: id}, nil
	default:
		return clientCommand{}, errors.New("unknown command (use -h)")
	}
}

type diagnosticRPCs interface {
	ListWorkers(context.Context, *runtimepb.ListWorkersRequest, ...grpc.CallOption) (*runtimepb.ListWorkersResponse, error)
	DiagnoseJob(context.Context, *runtimepb.DiagnoseJobRequest, ...grpc.CallOption) (*runtimepb.DiagnoseJobResponse, error)
}

func runDiagnostic(parent context.Context, client diagnosticRPCs, command clientCommand, timeout time.Duration, out io.Writer) error {
	ctx, cancel := context.WithTimeout(parent, min(timeout, diagnosticTimeout))
	defer cancel()
	if err := ctx.Err(); err != nil {
		return diagnosticRPCError(status.FromContextError(err).Err(), false)
	}
	var remote peer.Peer
	options := []grpc.CallOption{
		grpc.Peer(&remote), grpc.WaitForReady(false),
		// Commit the sent request without keeping a buffer for replay.
		grpc.MaxRetryRPCBufferSize(0), grpc.MaxCallRecvMsgSize(maxDiagnosticResponseBytes),
	}
	switch command.kind {
	case workersCommand:
		snapshot, err := client.ListWorkers(ctx, &runtimepb.ListWorkersRequest{}, options...)
		if err != nil {
			return diagnosticRPCError(err, remote.AuthInfo != nil)
		}
		if snapshot == nil {
			return errors.New("Coordinator returned no Worker snapshot")
		}
		return renderWorkers(out, snapshot, diagnosticColor(out))
	case diagnoseJobCommand:
		snapshot, err := client.DiagnoseJob(ctx, &runtimepb.DiagnoseJobRequest{JobId: command.jobID}, options...)
		if err != nil {
			return diagnosticRPCError(err, remote.AuthInfo != nil)
		}
		if snapshot == nil || snapshot.JobId != command.jobID {
			return errors.New("Coordinator returned no matching Job snapshot")
		}
		return renderJobDiagnosis(out, snapshot, diagnosticColor(out))
	default:
		return errors.New("invalid diagnostic command")
	}
}

func diagnosticRPCError(err error, authenticatedPeer bool) error {
	rpcStatus := status.Convert(err)
	code := rpcStatus.Code()
	switch code {
	case codes.PermissionDenied:
		return status.Error(code, "diagnostic access is not permitted")
	case codes.Unauthenticated:
		return status.Error(code, "Coordinator rejected RPC authentication")
	case codes.NotFound:
		return status.Error(code, "Job not found")
	case codes.Unavailable:
		// grpc-go surfaces TLS failures as Unavailable. TLS 1.3 can receive
		// a peer alert after its local handshake, while reading the preface.
		message := rpcStatus.Message()
		if !authenticatedPeer && (strings.HasPrefix(message, `connection error: desc = "transport: authentication handshake failed:`) ||
			strings.HasPrefix(message, `connection error: desc = "error reading server preface: remote error: tls:`)) {
			return status.Error(codes.Unauthenticated, "Coordinator mTLS handshake failed")
		}
		return status.Error(code, "diagnostics temporarily unavailable; retry later")
	case codes.DeadlineExceeded:
		return status.Error(code, "diagnostic request timed out")
	case codes.ResourceExhausted:
		return status.Error(code, "diagnostic resource or response size limit exceeded")
	case codes.Canceled:
		return status.Error(code, "diagnostic request canceled")
	default:
		return status.Error(code, "Coordinator diagnostics failed")
	}
}
