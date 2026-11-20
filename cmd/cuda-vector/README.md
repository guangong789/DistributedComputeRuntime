# CUDA vector transform

Build the standalone program on Linux with the CUDA toolkit. This example
targets compute capability 8.6, as used by the RTX 3060 Laptop GPU.

```sh
nvcc -std=c++17 -O2 -arch=sm_86 --cudart=shared \
  -o /absolute/path/cuda-vector ./cmd/cuda-vector/transform.cu
```

Use a trusted executable and installation directory protected from job
submitters. The deployed process needs GPU access, a compatible NVIDIA driver,
and `libcudart` available to the system dynamic linker. Build the Coordinator
and Worker with the usual Go toolchain. On one GPU-enabled Worker, set
`-cuda-vector-executable=/absolute/path/cuda-vector`. The program selects the
first visible CUDA device. Use a positive job timeout and a Coordinator lease
that allow for device initialization as well as execution and cleanup.

Submit `cuda_vector_transform` with exactly 64 comma-separated decimal
integers in `[0, 65535]`. The payload is at most 383 bytes and has no whitespace
or leading zeros except `0` itself. For each zero-based index `i`, one of 64
GPU threads computes `output[i] = input[i] * input[i] + 3*i + 11`. The largest
possible output is `4294836425`, which fits in an unsigned 32-bit integer.
The Worker computes the same formula independently with 64-bit integers and
requires byte-for-byte equality with the canonical output line. Observe the
terminal result using `GetJob`, which returns the CSV without the final LF.

The Worker starts the program directly with no arguments and only `LC_ALL=C`
in its environment. The child reads the payload followed by LF and writes
the result followed by LF to stdout. Stderr carries separate CUDA device, API,
and cleanup diagnostics. The Worker caps capture at 1 KiB per output stream
and keeps stderr out of RPC results. Execution must exit zero, pass result
verification, and complete before cancellation for the Worker to report success.

The CUDA task and [`external_square`](../external-square/README.md) share one
Worker slot. The Worker returns `FailedPrecondition` without the configured
executable and `ResourceExhausted` when the slot is held. On cancellation, it
requests SIGTERM, allows 300 ms before SIGKILL escalation, and holds the slot
through reaping and I/O cleanup. A signal alone does not establish that a
running kernel has stopped or GPU resources have been released. The
[direct-child parent-death protection](../external-square/README.md) also
applies to this executable. Deploy it as one unprivileged process without
descendants or credential changes.

## CUDA health and probing

Stock binaries advertise configured CUDA capability and use recorded Worker
liveness for unmanaged placement. Startup has no managed CUDA health flag
or periodic probe goroutine. Controlled fixtures explicitly invoke the
one-shot probe with the configured CUDA executable under a three-second
execution context and verify its output.
The probe shares the external-compute slot with Jobs. A held probe can
cause a Job to receive `ResourceExhausted`. A held Job can make a probe
skip with No Observation.

A local Ready result supplies a candidate observation. Managed CUDA placement
requires fresh Ready evidence accepted by the Coordinator for the current
session. Evidence expires at the original Coordinator-issued anchor time
plus the TTL. With no accepted evidence or after expiry, effective health
is Unknown. Unavailable requires a recognized backend failure.
Busy, cancellation, timeout, output mismatch, and inconclusive errors
produce No Observation. A later health transition affects future placement
and leaves already authorized Attempts running under their existing leases.

## Worker compute metrics

The Worker serves local metrics at `http://127.0.0.1:9091/metrics`
by default. Keep the endpoint on loopback or a trusted monitoring network.
Stock Workers run no probes, so probe outcome series stay at their initial
values during normal operation.

| Metric family | Meaning |
|---|---|
| `dzkr_compute_slot_occupied` | Current occupancy by owner (`job` or `probe`) |
| `dzkr_compute_slot_hold_age_seconds` | Age of the current hold by owner, or zero for an idle category |
| `dzkr_compute_slot_denials_total` | Denials by requester and actual holder at admission |
| `dzkr_compute_slot_hold_duration_seconds` | Histogram of completed holds by owner, including child execution, reaping, cleanup, and verification |
| `dzkr_cuda_probe_outcomes_total` | Local probe completions by outcome (`ready`, `unavailable`, `no_observation`) |
| `dzkr_cuda_probe_last_outcome` | One for the latest local outcome, or zero for every outcome before the first completion |

Probe Busy skips are the sum of denials with `requester="probe"`. Their
No Observation outcome describes the same calls. Coordinator acceptance
and freshness are evaluated separately from local probe outcomes. Metrics
are process-local, and counters and histograms reset on Worker restart.
A scrape and a separate local snapshot can observe different instants.
