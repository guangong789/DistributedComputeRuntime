# External square task

On Linux, build the CPU executable with
`go build -mod=readonly -o /absolute/path/external-square ./cmd/external-square`.
Enable it on a Worker with
`-external-square-executable=/absolute/path/external-square`.
The program and its installation directory must be trusted and protected from
replacement by job submitters. Use one configured Worker for this workload.

`SubmitJob` accepts task type `external_square` with a canonical decimal
payload from `0` through `65535`. The payload contains digits only, with no
leading zero except for `0`. Set a positive `timeout_ms` as for other tasks.
For example, payload `37` yields `GetJob.output` equal to `1369` on success.

The Worker invokes the program directly with no arguments. It sets the working
directory to the executable's directory and the environment to `LC_ALL=C`.
The program receives the payload followed by one LF on stdin. It must exit
with code zero and write exactly the canonical decimal square followed by one
LF to stdout. The Worker verifies the value independently and returns it
without the LF. Each output stream is limited to 1 KiB of captured bytes.
A nonzero exit, an overflow or an incorrect result fails the task. Raw stderr
is kept out of the RPC result.

An unconfigured Worker returns `FailedPrecondition`. A configured Worker runs
one child at a time and returns `ResourceExhausted` while that slot is held.
On cancellation it requests SIGTERM and allows 300 ms before escalating to
SIGKILL. The handler and slot remain owned through direct-child reaping and I/O
cleanup. The Worker shutdown deadline can expire before the OS completes that
cleanup. Worker SIGKILL can leave a child alive.

The executable must run as a single process without spawning descendants.
The child shares the Worker's OS privileges. The launch contract does not
provide filesystem or network isolation.
