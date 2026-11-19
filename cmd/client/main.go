package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
)

func main() {
	coordinatorAddr := flag.String(
		"coordinator",
		"localhost:50050",
		"coordinator address",
	)

	execution := clientExecutionFlags(flag.CommandLine)
	security := clientSecurityFlags(flag.CommandLine)
	flag.Parse()
	if err := execution.validate(); err != nil {
		log.Fatalf("client execution configuration: %v", err)
	}
	transportCredentials, err := security.credentials(*coordinatorAddr)
	if err != nil {
		log.Fatalf("client mTLS configuration: %v", err)
	}

	conn, err := grpc.NewClient(
		*coordinatorAddr,
		grpc.WithTransportCredentials(transportCredentials),
	)
	if err != nil {
		log.Fatal(err)
	}
	client := runtimepb.NewCoordinatorServiceClient(conn)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err = runClient(ctx, client, *execution, os.Stdout)
	stop()
	_ = conn.Close()
	if err != nil {
		log.Fatal(err)
	}
}
