[简体中文](README.md) | English

# Distributed Compute Runtime

A Go runtime for reliable distributed execution with one Coordinator, multiple Workers, gRPC over mTLS, and SQLite. It runs built-in hash and sleep tasks and trusted external CPU and CUDA programs.

## Architecture

~~~mermaid
flowchart LR
    client["Client"] -->|"gRPC / mTLS"| coordinator["Coordinator"]
    coordinator <-->|"gRPC / mTLS"| workers["Workers"]
    coordinator <--> sqlite["SQLite"]
    workers --> compute["Trusted Linux compute executable"]
~~~

- The **Coordinator** owns durable admission, scheduling, Worker registration and liveness, Attempt authorization, leases, retries, and recovery.
- The **Worker** runs built-in tasks and owns external process admission, execution, and cleanup.
- The **Client** submits once and uses GetJob to observe the durable outcome. The CLI submits a hash task by default. Use [observe-only mode](cmd/client/execution.go) to reconnect to an existing JobID.

## Durability and result acceptance

- **Admission.** SubmitJob persists a Queued Job and returns Accepted with the same JobID and AttemptID 0. Coordinator-owned execution continues independently of the submission RPC. Durable JobIDs are unique and JobSpec fields are compared by exact stored values. GetJob returns the current durable state and the accepted result when present.
- **Attempts and fencing.** The Coordinator confirms durable publication of a generation before dispatch. It checks the reply's JobID and AttemptID and checks the current context and lease again at terminal persistence. A result from an expired or superseded Attempt cannot overwrite a newer generation. [Attempt execution](cmd/coordinator/attempt_execution.go)
- **Restart and uncertain writes.** Startup moves eligible Running Jobs to Recovering while preserving their generation and original JobSpec. The next Attempt advances beyond the highest known generation. The Coordinator discovers Queued and Recovering work and reconciles uncertain writes against SQLite before retry. [Attempt allocation](cmd/coordinator/attempts.go) · [Reconciliation](cmd/coordinator/queued_session.go)

## Workloads

| TaskType | Execution |
|---|---|
| `hash` | Built-in SHA-256 over a nonempty input string |
| `sleep` | Built-in cancellable wait for a Go duration |
| `external_square` | Trusted Linux CPU executable with a bounded integer protocol |
| `cuda_vector_transform` | Standalone CUDA executable with a fixed vector protocol and independent CPU result verification |

The two external tasks share one local slot. A canceled child receives SIGTERM, followed by SIGKILL after the grace period if needed. The Worker retains the slot through reaping and I/O cleanup, and shutdown joins the admitted handlers. Build and configure trusted programs using the [CPU workload guide](cmd/external-square/README.md) and the [CUDA workload guide](cmd/cuda-vector/README.md).

## Security and observability

TLS 1.3 and mTLS authenticate peers. RPC authorization checks each role, binds WorkerID to the certificate identity, and validates the advertised Worker endpoint. The Coordinator exposes Prometheus metrics for admission, Jobs, Attempts, Worker liveness, and recovery. CUDA failures can include an allowlisted stage in Worker logs. The stage is local to the Worker and carries no raw child output.

## Quick start

Run the local Linux or WSL demo from the repository root.

~~~sh
go run -mod=readonly ./cmd/demo
~~~

The demo uses the toolchain pinned in [go.mod](go.mod), Bash, OpenSSL 3, Git, GNU utilities, and cached Go dependencies. It starts real Coordinator and Worker processes with isolated development mTLS, submits a hash Job, and independently checks the GetJob result and SQLite row. It reaps its children and removes its temporary workspace.

For manual development startup, use [the development PKI bootstrap](scripts/dev-pki/bootstrap.sh) or supply a trusted CA and matching certificates. Configure `-tls-ca`, `-tls-cert`, and `-tls-key` for all three programs. The Coordinator needs authorized Client and Worker identities and a writable SQLite path. The Worker needs a reachable Coordinator and an advertised address matching its certificate. Use `go run -mod=readonly ./cmd/coordinator -help` or the corresponding `worker` and `client` commands to see the flags.

~~~sh
go build -mod=readonly ./...
make test-fast
~~~

See the [Go test workflow](scripts/README.md) for full regression and Race profiles.

## Historical databases

New `zk_preimage_prove` submissions are rejected with `FailedPrecondition` before admission. If the configured SQLite database contains a Queued, Running, or Recovering row for that retired task type, the Coordinator refuses startup before schema initialization or recovery writes. Finish pending work with a compatible prior release or configure another database. The original rows are retained. Historical terminal rows remain readable through GetJob.

The [GitHub repository](https://github.com/guangong789/DistributedComputeRuntime) and Go module use `github.com/guangong789/DistributedComputeRuntime`.
For compatibility with existing credentials and monitoring, deployments use the SPIFFE trust domain `distributedzk.local`, the existing development CA subject, and the Prometheus prefix `dzkr_`.
