[简体中文](README.md) | English

# Distributed Compute Runtime

**Reliable Heterogeneous Compute Runtime with Operational Diagnostics**

A Go runtime for reliable distributed execution with one Coordinator, multiple Workers, gRPC over mTLS, and SQLite. It runs built-in hash and sleep tasks and trusted external CPU and CUDA programs.

## Architecture

~~~mermaid
flowchart LR
    client["Client"] -->|"gRPC / mTLS"| coordinator["Coordinator"]
    coordinator <-->|"gRPC / mTLS"| workers["Workers"]
    coordinator <--> sqlite["SQLite"]
    workers --> compute["Trusted Linux compute executable"]
~~~

- The **Coordinator** owns durable admission, capability-aware scheduling, Worker registration sessions and heartbeat liveness, Attempt authorization, leases, retries, and recovery.
- The **Worker** runs built-in tasks and owns external process admission, execution, and cleanup.
- The **Client** submits once and uses GetJob to observe the durable outcome. The CLI submits a hash task by default. Use [observe-only mode](cmd/client/execution.go) to reconnect to an existing JobID.

## Durability and result acceptance

- **Admission.** SubmitJob persists a Queued Job and returns Accepted with the same JobID and AttemptID 0. Coordinator-owned execution continues independently of the submission RPC. Resubmitting an existing JobID creates no second Job. JobSpec fields are immutable and compared by exact stored values. GetJob returns the current durable state and the accepted result when present.
- **Attempts and fencing.** The Coordinator confirms durable publication of a generation before dispatch. It checks the reply's JobID and AttemptID and checks the current context and lease again at terminal persistence. A result from an expired or superseded Attempt cannot overwrite a newer generation. [Attempt execution](cmd/coordinator/attempt_execution.go)
- **Restart and uncertain writes.** Startup moves eligible Running Jobs to Recovering while preserving their generation and original JobSpec. The next Attempt advances beyond the highest known generation. The Coordinator discovers Queued and Recovering work and reconciles uncertain writes against SQLite before retry. [Attempt allocation](cmd/coordinator/attempts.go) · [Reconciliation](cmd/coordinator/queued_session.go)

## Workloads

| TaskType | Execution |
|---|---|
| `hash` | Built-in SHA-256 over a nonempty input string |
| `sleep` | Built-in cancellable wait for a Go duration |
| `external_square` | Trusted Linux CPU executable with a bounded integer protocol |
| `cuda_vector_transform` | Standalone CUDA executable with a fixed vector protocol and independent CPU result verification |

On Linux, the two external tasks share one local slot and run as direct children with parent-death protection. Cancellation or timeout requests SIGTERM and allows 300 ms before SIGKILL escalation. The Worker holds the slot through child reaping and I/O cleanup. Graceful shutdown joins admitted handlers within the configured budget. If the budget expires, the Worker exits with an error. A kernel or driver stall can leave cleanup incomplete. Build and configure trusted programs using the [CPU workload guide](cmd/external-square/README.md) and the [CUDA workload guide](cmd/cuda-vector/README.md).

## Security and observability

TLS 1.3 and mTLS authenticate peers. RPC authorization checks each role, binds WorkerID to the certificate identity, and validates the advertised Worker endpoint. The Coordinator exposes Prometheus metrics for admission, Jobs, Attempts, Worker liveness, and recovery. CUDA failures can include an allowlisted stage in Worker logs. The stage is local to the Worker and carries no raw child output.

The Coordinator and Worker each expose metrics on a separate, unauthenticated plain HTTP endpoint bound to loopback by default. Worker metrics cover the [local compute slot and probe](cmd/cuda-vector/README.md#worker-compute-metrics). Both endpoints sit outside the gRPC mTLS boundary. Use external access controls before exposing either endpoint to an untrusted network.

### Read-only diagnostics

Diagnostics are denied by default on the existing Coordinator mTLS listener. Authorize the same Client certificate identity with both `-allow-client` and `-allow-diagnostics-client`. Ordinary GetJob access alone grants no diagnostics access. Worker and Coordinator identities are ineligible.

Command syntax. Values in square and angle brackets are placeholders.

~~~text
client [flags] workers
client [flags] diagnose job <signed-job-id>
~~~

Set the target with `-coordinator` and supply `-tls-ca`, `-tls-cert`, and `-tls-key` before the subcommand. `workers` calls `ListWorkers` once. `diagnose job` calls `DiagnoseJob` once. Each call has a deadline of at most two seconds. Invalid arguments fail before connection setup. Query errors exit nonzero without printing a successful snapshot.

`workers` shows the Coordinator sample time, recorded liveness, configured external workloads, currently eligible workloads, and effective CUDA health and freshness. `diagnose job` shows durable state, the recorded Attempt and Worker, accepted Worker result presence, and counts of configured compatible Workers, compatible Workers recorded as Alive, and eligible Workers. If revalidation finds a different or missing durable Job record, `JOB_CHANGED` takes precedence over state-based interpretation. When both reads agree, live Attempt authority for a `Running` Job is `Unknown`, and terminal placement is `Not Applicable`. Expired managed health appears as `Unknown` with an `Expired` marker.

Historical reasons for queue delays, current remote Worker slot ownership, and future scheduling progress remain undetermined. The Job reads, Registry snapshot and Worker metrics have separate sampling boundaries. Their combination is a non-atomic view.

## Quick start

Run from the repository root on Linux or WSL with OpenSSL 3 and cached Go dependencies.

~~~sh
go run -mod=readonly ./cmd/demo
go run -mod=readonly ./cmd/demo -show-cli
~~~

The first command runs the `hash` demo, checks the `GetJob` result and SQLite record, and cleans up the temporary environment. The second also displays real output from the [diagnostics CLI](#read-only-diagnostics) commands `workers` and `diagnose job` using temporary mTLS credentials authorized for diagnostics.

## Design Decisions and Operational Boundaries

### Execution Generations, Leases, and Result Fencing

The persisted `AttemptID` identifies the execution generation. Its lease bounds the authorization period. Before writing a terminal result, the Coordinator rechecks the generation and lease to prevent an expired or superseded Attempt from overwriting a newer accepted result.

### Why Can't Diagnostics Reconstruct Historical Queue Causes?

Candidate counts describe the Coordinator's sampled state. Job reads and the Worker registry snapshot form a non-atomic view. If the reread finds a changed or missing Job record, `JOB_CHANGED` takes precedence. An eligible Worker at the sample time cannot establish why the Job was previously queued.

### Single-Coordinator and Recovery Boundaries

One Coordinator owns Worker registration sessions, execution authorization, and recovery coordination. SQLite stores Job state so the Coordinator can recover eligible unfinished Jobs after restart. This deployment has no automatic standby Coordinator takeover.

### Why Can an Alive Worker Still Be Ineligible for CUDA?

`Alive` is the Coordinator's recorded liveness. Reachability requires a separate check. CUDA placement also requires configured capability. Managed placement additionally requires unexpired `Ready` evidence accepted by the Coordinator for the current session. Stock binaries use unmanaged mode.

### External Compute and Health Probes Share an Execution Slot

External CPU and CUDA Jobs and the explicitly invoked one-shot CUDA probe compete for one Worker-local slot. The Worker holds it through child reaping, I/O cleanup, and required result verification. The slot limits local concurrency. GPU isolation and Coordinator capacity reservation are outside its scope.

## Milestone scope and verification

This single-Coordinator milestone includes the execution and diagnostics above. Managed CUDA placement with accepted health evidence and an explicitly invoked one-shot probe were verified in controlled runs. Stock binaries use unmanaged CUDA placement. [CUDA health and probe boundary](cmd/cuda-vector/README.md#cuda-health-and-probing)

Deferred work includes the production periodic probe lifecycle, unattended managed CUDA activation, GPU utilization-based placement, dynamic capacity scheduling, remote Worker slot queries, fleet metrics aggregation, diagnostic event history, Kubernetes integration, AI model serving, multi-Coordinator consensus, and an interactive or full-screen TUI.

I0-01 verified layered builds and focused normal and race tests on its review snapshot. It reused the same implementation's Fast (78 passed), Core (860), Full (1,362), and Race (860) profiles, the real-process Hash Demo, and the mTLS CLI smoke test. Core, Full, and Race each skipped the opt-in GPU test. I0-01 also reused controlled RTX 3060 evidence with 64/64 one-shot outputs verified. Automated fixtures covered failure classification. Recovery from an actual GPU driver fault and production-scale behavior remain unverified. See the [Go test workflow](scripts/README.md) for full regression and Race profiles.

## Historical databases

New `zk_preimage_prove` submissions are rejected with `FailedPrecondition` before admission. If the configured SQLite database contains a Queued, Running, or Recovering row for that retired task type, the Coordinator refuses startup before schema initialization or recovery writes. Finish pending work with a compatible prior release or configure another database. The original rows are retained. Historical terminal rows remain readable through GetJob.

The [GitHub repository](https://github.com/guangong789/DistributedComputeRuntime) and Go module use `github.com/guangong789/DistributedComputeRuntime`.
For compatibility with existing credentials and monitoring, deployments use the SPIFFE trust domain `distributedzk.local`, the existing development CA subject, and the Prometheus prefix `dzkr_`.
