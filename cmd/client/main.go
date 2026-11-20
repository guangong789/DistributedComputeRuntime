package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	runtimepb "github.com/guangong789/DistributedComputeRuntime/proto"
	"google.golang.org/grpc"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runCLI(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func runCLI(ctx context.Context, args []string, out, stderr io.Writer) int {
	flags := flag.NewFlagSet("client", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	address := flags.String("coordinator", "localhost:50050", "coordinator address")
	execution := clientExecutionFlags(flags)
	security := clientSecurityFlags(flags)
	if err := flags.Parse(args); errors.Is(err, flag.ErrHelp) {
		flags.SetOutput(out)
		fmt.Fprintln(out, "Usage: client [flags] [workers | diagnose job <job-id>]")
		fmt.Fprintln(out, "With no subcommand, submit the demo Job or observe with -observe-job.")
		flags.PrintDefaults()
		return 0
	} else if err != nil {
		fmt.Fprintln(stderr, "invalid client flags (use -h)")
		return 2
	}
	command, err := parseClientCommand(flags.Args())
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if command.kind == workflowCommand {
		if err := execution.validate(); err != nil {
			fmt.Fprintln(stderr, "client execution configuration:", err)
			return 1
		}
	} else if execution.RPCTimeout <= 0 || execution.ObserveJobID != nil {
		fmt.Fprintln(stderr, "diagnostics require a positive -rpc-timeout and cannot be combined with -observe-job")
		return 2
	}

	transport, err := security.credentials(*address)
	if err != nil {
		if command.kind == workflowCommand {
			fmt.Fprintln(stderr, "client mTLS configuration:", err)
		} else {
			fmt.Fprintln(stderr, "client mTLS configuration failed; check the certificate, private key, CA and identities")
		}
		return 1
	}
	options := []grpc.DialOption{grpc.WithTransportCredentials(transport)}
	if command.kind != workflowCommand {
		options = append(options, grpc.WithDisableRetry())
	}
	conn, err := grpc.NewClient(*address, options...)
	if err != nil {
		if command.kind == workflowCommand {
			fmt.Fprintln(stderr, err)
		} else {
			fmt.Fprintln(stderr, "could not initialize the Coordinator connection")
		}
		return 1
	}
	defer conn.Close()
	client := runtimepb.NewCoordinatorServiceClient(conn)
	if command.kind == workflowCommand {
		err = runClient(ctx, client, *execution, out)
	} else {
		err = runDiagnostic(ctx, client, command, execution.RPCTimeout, out)
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}
