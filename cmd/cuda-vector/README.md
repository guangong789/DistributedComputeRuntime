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
running kernel has stopped or GPU resources have been released. Deploy the
executable as one process without descendants. GPU-aware placement and
orphan prevention after Worker SIGKILL are outside this contract.
