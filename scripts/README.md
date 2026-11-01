# Go test workflow

Prerequisites: the Go toolchain required by `go.mod`, Python 3.9+, Make, and a
POSIX system (Linux/macOS). No Python packages or additional test runners are
required. Existing Go dependencies must be available locally for offline use;
the wrapper adds no network or service requirement. Go retains its normal module
resolution behavior and build caches; test-result caching is disabled.

Run from the repository root:

| Command | Purpose | Coverage boundary |
|---|---|---|
| `make test-fast` | Focused storage, admission, and recovery feedback | Explicit Coordinator subset; never sufficient alone for final verification |
| `make test-core` | Complete Coordinator regression | Other packages excluded |
| `make test-full` | Default Go suite, `./...` | No benchmarks, opt-in experiments, Python harnesses, or race instrumentation |
| `make test-race` | Complete Coordinator suite with race detector | Add race runs for other concurrent packages when their implementation changes |
| `make test-report` | Reparse the latest saved run of each profile | Executes no tests; historical evidence only |
| `make test-infra` | Isolated runner/parser self-tests | Does not invoke project Go tests |

Core/Full/Race use `-mod=readonly -count=1 -timeout=10m`. Fast uses a two-minute
timeout. All use `go test -json`. The Go timeout applies per test binary, not as
an overall wrapper/build deadline. `PYTHON=python3` may override the interpreter.

## Reports and diagnosis

Each invocation creates an independent ignored directory:
`.test-reports/<profile>/<UTC timestamp>-<unique suffix>/`. No previous run is
overwritten. `<profile>/latest.json` points to the last announced run, including
an unfinished run. Concurrent invocations retain separate artifacts; the latest
pointer uses last-writer-wins ordering.

Each directory contains:

- `events.jsonl`: complete, unfiltered Go stdout event stream.
- `stderr.log`: complete Go stderr, including compiler/launcher diagnostics.
- `run.json`: command, profile, UTC timestamps, wall duration, raw process return
  code, normalized exit code, interruption and runner-error information.
- `summary.json` and `summary.txt`: structured and readable reports.

The console reports package statuses, **leaf-test** counts, failed test identities,
bounded diagnostic tails, and five slow top-level tests. Parents with observed
descendants are excluded from counts. A parent failing after passing children is
shown separately as a failed container. Package/build failures do not invent test
failures. Top-level durations include children and are not added to child times.

Read the summary first, then inspect as much detail as the investigation needs:

```bash
cat .test-reports/core/<run>/summary.txt
less .test-reports/core/<run>/stderr.log
less .test-reports/core/<run>/events.jsonl
```

Nonzero Go exits remain failures and retain their exit code in direct wrapper
invocations. Make itself normally returns 2 for a failed recipe; `run.json`
retains the underlying code. A zero Go exit with malformed/incomplete output,
missing selected tests, or a reporting error returns a nonzero wrapper status.
SIGINT/SIGTERM are forwarded to the test process group; after five seconds it is
killed if still running. A second signal forces termination. SIGKILL/power loss
cannot run cleanup; saved incomplete metadata must not be treated as a PASS.

`make test-report` reparses raw data instead of trusting a saved PASS. It exits
nonzero for missing, failed, or invalid reports. **All saved runs are labelled
historical, with unknown applicability to the current worktree.** No worktree
fingerprinting is implemented. Raw logs are retained without an automatic cleanup
policy; remove old run directories deliberately when no longer needed.

## Fast selection and development practice

Edit `scripts/go-test-fast.txt` to add exact top-level test names, one per line.
Comments are allowed; regex patterns, duplicate names, and an empty manifest are
rejected. The generated `-run` expression is anchored. Every listed test must
appear in the JSON stream; renamed/missing tests fail reporting rather than
silently reducing coverage. Subtests of selected tests run normally.

During edits, run focused tests, classify failures, and inspect relevant functions,
helpers, and production behavior before changing assertions. Classifications:
production regression, intentional contract change, fixture/instrumentation
coupling, environment/flakiness, or test-infrastructure failure.

Failures across **three or more unrelated test files** trigger a shared-root-cause
check: admission preconditions, fixture defaults, Create/Save interception, state
transitions, and Coordinator initialization. This checkpoint permits justified
multi-file fixes. Preserve identity, fencing, result acceptance, cancellation,
recovery, and uncertain-commit assertions; explain intentional contract updates.

Before completing a milestone, run relevant Core/Full/Race checks. A Full report
already includes Coordinator coverage; avoid redundant reruns when nothing has
changed. Fast alone is development feedback. This tooling does not solve fixture
coupling or prove absence of races outside the tested paths.

## Troubleshooting

- **Build failure:** inspect package/build status and `stderr.log`; zero failed
  leaf tests does not imply successful compilation.
- **Malformed/incomplete JSON:** inspect raw output and `run.json`; check process
  interruption, disk errors, toolchain changes, or unexpected stdout. Rerun the
  affected profile after resolving the cause.
- **Timeout/panic/race:** inspect the retained stack/race output; do not increase
  budgets or weaken assertions merely to obtain a green run.
- **Missing report:** run the desired profile. Report inspection never executes
  tests or presents an older run as current verification.

Optional CI can invoke the same targets and archive `.test-reports/` after failures.
No CI service configuration, crypto-fixture sharing, or fixture framework is added.
