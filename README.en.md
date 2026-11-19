[简体中文](README.md) | English

# DistributedZKRuntime

## Project Introduction

DistributedZKRuntime is a Go distributed job runtime with one Coordinator, multiple Workers, gRPC/mTLS communication, and SQLite-backed job state and full task specifications. The runtime manages execution attempts, retries, leases, stale-result fencing, cancellation, and restart recovery. A Groth16 proof of a MiMC preimage relation on BN254 is a specialized workload: Workers generate proofs, and the Coordinator independently verifies the results.

The project focuses on result acceptance when remote computation outlives a request, a Worker fails, or the Coordinator restarts. The five design decisions below explain how proof verification, attempt identity, and resource lifetimes distinguish an eligible result from an obsolete attempt.

## Architecture Overview

```mermaid
flowchart LR
    client["Client"] -->|"gRPC / mTLS"| coordinator["Coordinator"]
    coordinator <-->|"gRPC / mTLS"| workers["Workers"]
    coordinator <--> sqlite["SQLite"]
    classDef clientRole fill:#dbeafe,stroke:#2563eb,color:#172554
    classDef coordinatorRole fill:#dcfce7,stroke:#16a34a,color:#14532d
    classDef workerRole fill:#fef3c7,stroke:#d97706,color:#78350f
    classDef storageRole fill:#ede9fe,stroke:#7c3aed,color:#3b0764
    class client clientRole
    class coordinator coordinatorRole
    class workers workerRole
    class sqlite storageRole
```

`SubmitJob` durably admits a Job in `Queued` state. Successful admission returns `Accepted` with the same JobID and `AttemptID=0`, without terminal execution results. After admission, Coordinator-owned background scheduling and execution are independent of the submitting RPC's lifetime. Clients use `GetJob` to observe durable state and terminal results when available. Resubmissions of admitted Jobs are rejected under the existing JobID/specification conflict rules.

## Core Capabilities

- **Distributed execution:** Registration, heartbeat, liveness detection, and hash, sleep, and preimage proof tasks.
- **Reliability:** AttemptIDs, leases, result fencing, failure retries, cancellation, SQLite recovery, and bounded shutdown.
- **Trust and capacity:** mTLS authorization, public payload validation, independent Groth16 verification, and Worker proving admission.
- **Observability:** Semantic runtime metrics and a Prometheus metrics endpoint.

## Design Decisions

### 1. How can the Coordinator verify a result without knowing the Worker's secret?

Putting the secret in the submission payload would expose a value intended to remain on the Worker. The production task, `zk_preimage_prove`, therefore submits only a public digest and a `witness_ref`. The Worker resolves the secret through its local WitnessStore, and [ZKPreimageTask](internal/runtime/zk_preimage_task.go) generates a Groth16 proof of `MiMC(secret) == digest`. With the pinned dependencies, this BN254/R1CS circuit has 331 constraints.

A Worker reporting Succeeded is not enough to establish that relation. The [Coordinator](cmd/coordinator/zk_preimage_verifier.go) actually invokes Verify with the matching verifying key and public digest, without obtaining the secret or repeating the private computation. Holding private inputs and checking the result are separate responsibilities: the Worker produces the proof, and the Coordinator uses it to assess the public statement.

### 2. Why does mTLS still need WorkerID binding and RPC authorization?

mTLS establishes a connection's peer identity, but does not by itself decide which operations that peer may perform. The project requires TLS 1.3/mTLS and uses [method authorization](internal/rpcauth/policy.go) to distinguish roles: Clients submit jobs, Workers register and heartbeat, and only the configured Coordinator can invoke a Worker's ExecuteJob. Connection identity and application permissions are checked separately.

WorkerID is still a field supplied by the caller, so registration and heartbeat bind it to the certificate's Worker identity. Registration also [checks that the advertised address matches the certificate hostname](cmd/coordinator/transport.go); outbound connections verify DNS/IP SANs and the expected URI identity. These checks connect the logical Worker selected for scheduling to the actual peer and endpoint, rather than trusting a request field alone.

### 3. Why can a mathematically valid Worker result still be rejected?

Suppose Attempt 1's lease expires and the Coordinator cancels it and starts Attempt 2, while the old Worker is still finishing a synchronous proof. Even a correct proof from that Worker must not overwrite the new attempt's result. JobID identifies the job, a monotonic AttemptID distinguishes generations, and a lease defines each attempt's eligibility window. Proof validity and attempt validity are independent conditions.

The [execution path](cmd/coordinator/execution.go) checks response JobID, AttemptID, and the current lease before verifying a proof. Cancellation or lease expiry can also occur during Verify, so persistTerminalResult checks the parent context and lease again before saving terminal state. That check and Save hold the same mutex to exclude local generation replacement between them. Old computation may overlap a retry, but a late valid proof does not automatically become an acceptable result.

### 4. How does the Coordinator preserve attempt identity across crashes and recovery?

An in-memory AttemptID could reset to 1 after restart and reuse an old generation. SQLite JobRecords therefore retain the generation and full task specification. [Attempt allocation](cmd/coordinator/attempts.go) uses `nextAttempt = max(inMemoryAttempt, persistedAttempt) + 1`, saves JobRunning first, publishes the in-memory AttemptID and lease, and then dispatches. A failed Save publishes no attempt; a persisted Attempt 7 advances to Attempt 8 in a fresh process.

Startup recovery marks Running records with valid task specifications as Recovering, retaining their generation and inputs. The [Coordinator](cmd/coordinator/jobs.go) discovers durable Queued and Recovering Jobs and manages them through Coordinator-owned execution sessions. An Attempt is dispatched only after its publication is durably confirmed; AttemptIDs and leases fence authorization and result acceptance. Conditional repair and online reconciliation retain responsibility for uncertain writes while preserving existing terminal results and newer generations. Clients independently observe persisted terminal results through `GetJob`.

### 5. Why doesn't cancelling a Groth16 task immediately release its concurrency slot?

Go context cancellation is cooperative; the current synchronous Groth16 Prove call does not stop automatically. A request can end or the Coordinator can retry while the old proof still consumes CPU. Releasing its slot immediately would admit new computation before the previous work finished, allowing repeated cancellation to push actual concurrency beyond the configured capacity.

The Worker's shared [ProvingLimiter](internal/runtime/proving_limiter.go) therefore retains the slot until the task actually exits and returns ResourceExhausted when full. [ZKPreimageTask](internal/runtime/zk_preimage_task.go) checks cancellation before Prove and the result deadline after proof generation and encoding. Late output can be discarded while unfinished computation still counts against capacity. Worker shutdown also joins actual handlers, tying resource release to computation completion.

## Quick Start

Run the real local demo from the repository root on **Linux/WSL**:

```sh
go run -mod=readonly ./cmd/demo
```

You need a local toolchain meeting the Go **1.27.1** requirement in [go.mod](go.mod), Bash, OpenSSL 3, Git, the required GNU utilities, and cached Go dependencies. The demo's child commands build offline; they do not install tools or download missing dependencies.

The demo starts real Coordinator/Worker processes, generates isolated temporary development PKI, admits a deterministic hash task over gRPC/mTLS, observes its terminal state through `GetJob`, and independently checks the SHA-256 output and SQLite terminal state. Both services use `-disable-zk=true`, so no Groth16 keys or private witness are required. On success, it reaps owned child processes and removes temporary files. Real ZK execution and proof verification are covered separately by [integration tests](cmd/coordinator/zk_preimage_integration_test.go).

For a build check alone, use:

```sh
go build -mod=readonly ./...
```

A build check neither starts the demo nor provisions runtime materials. Starting services manually in the default ZK mode still requires a trusted CA and role- and endpoint-matching mTLS certificates, identity allowlists, matching Groth16 proving/verifying keys, Worker-local witness data, and a writable SQLite path. See the [Go test workflow](scripts/README.md) for test entry points and environment requirements.

## Scope and Limitations

The runtime uses one Coordinator and multiple Workers, with SQLite-backed job state and restart recovery; multi-Coordinator high availability is not supported. AttemptIDs and leases govern result acceptance. Old computation can overlap a retry, so result fencing does not guarantee exactly-once execution: performing a computation and accepting its result are separate events.
