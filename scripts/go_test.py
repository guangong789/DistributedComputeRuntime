#!/usr/bin/env python3
"""Fixed Go test profiles and conservative, streaming JSONL reports (stdlib only)."""

import argparse
from collections import Counter, deque
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[1]
PROFILES = ("fast", "core", "full", "race")
HISTORY = "UNKNOWN: historical snapshot; current worktree applicability is not verified"
TERMINAL = {"pass", "fail", "skip"}
ACTIONS = TERMINAL | {"start", "run", "pause", "cont", "output", "build-output", "build-fail"}
MAX_GROUPS = 5


def utc():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    # Unique temporary names also allow simultaneous runs of the same profile.
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                     prefix=".write-", delete=False) as handle:
        tmp = Path(handle.name)
        try:
            json.dump(value, handle, indent=2, allow_nan=False)
            handle.write("\n")
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise
    try:
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


class Tail:
    """Bound per-owner diagnostics while retaining the complete files separately."""

    def __init__(self):
        self.lines = deque(maxlen=10)
        self.omitted = 0

    def add(self, output):
        for line in output.splitlines():
            if len(self.lines) == self.lines.maxlen:
                self.omitted += 1
            if len(line) > 300:
                line = line[:300] + " ... [line truncated]"
            self.lines.append(line)

    def data(self):
        return {"lines": list(self.lines), "omitted_lines": self.omitted}


def summarize(run_dir):
    """Read one event at a time; retain states and bounded diagnostic tails only."""
    meta = json.loads((run_dir / "run.json").read_text(encoding="utf-8"))
    if not isinstance(meta, dict):
        raise ValueError("run.json must contain an object")
    issues, issue_count = [], 0

    def issue(message):
        nonlocal issue_count
        issue_count += 1
        if len(issues) < 12:
            issues.append(message)

    if meta.get("profile") not in PROFILES:
        issue("missing or unknown profile")
    command = meta.get("command")
    if not isinstance(command, list) or not command or any(not isinstance(arg, str) for arg in command):
        command = None
        issue("missing or invalid invoked command")
    for field in ("started_at", "ended_at"):
        try:
            timestamp = datetime.fromisoformat(meta[field])
            if timestamp.tzinfo is None:
                raise ValueError("timestamp has no timezone")
        except (KeyError, TypeError, ValueError):
            issue("missing or invalid " + field)
    code = meta.get("exit_code")
    if type(code) is not int or not 0 <= code <= 255:
        code = None
        issue("missing or invalid process exit code")
    wall = meta.get("wall_seconds")
    if type(wall) not in (int, float) or not math.isfinite(wall) or wall < 0:
        wall = None
        issue("missing or invalid wall duration")
    if meta.get("complete") is not True or not meta.get("ended_at"):
        issue("runner did not record completion")
    if meta.get("runner_error"):
        issue("runner error: " + str(meta["runner_error"]))

    packages, tests, tails, build_failures = {}, {}, {}, set()

    def tail(key):
        return tails.setdefault(key, Tail())

    raw = run_dir / "events.jsonl"
    if not raw.is_file():
        issue("missing raw JSONL report")
    else:
        with raw.open("rb") as stream:
            for number, line in enumerate(stream, 1):
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        raise ValueError("event is not an object")
                    action = event.get("Action")
                    package = event.get("Package") or event.get("ImportPath")
                    name = event.get("Test", "")
                    if not isinstance(action, str) or action not in ACTIONS:
                        raise ValueError("unsupported or missing Action")
                    if not isinstance(package, str) or not package or not isinstance(name, str):
                        raise ValueError("missing/invalid package or test identifier")
                    output = event.get("Output", "")
                    elapsed = event.get("Elapsed", 0)
                    if not isinstance(output, str):
                        raise ValueError("Output is not text")
                    if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed < 0:
                        raise ValueError("invalid Elapsed")
                except (ValueError, UnicodeError) as error:
                    issue(f"malformed event at line {number}: {error}")
                    continue

                key = (package, name)
                if output:
                    tail(key).add(output)
                if action.startswith("build-"):
                    if action == "build-fail":
                        build_failures.add(package)
                    continue

                pkg = packages.setdefault(package, {"package": package, "started": False,
                                                     "status": None, "elapsed": None})
                if action == "start":
                    if pkg["started"] or pkg["status"]:
                        issue(f"duplicate/out-of-order package start: {package}")
                    pkg["started"] = True
                elif name:
                    test = tests.setdefault(key, {"package": package, "test": name,
                                                 "started": False, "status": None, "elapsed": None})
                    if action == "run":
                        if test["started"] or test["status"]:
                            issue(f"duplicate/out-of-order test run: {package}: {name}")
                        test["started"] = True
                    elif action in TERMINAL:
                        if test["status"]:
                            issue(f"duplicate test completion: {package}: {name}")
                        test.update(status=action, elapsed=elapsed)
                elif action in TERMINAL:
                    if pkg["status"]:
                        issue(f"duplicate package completion: {package}")
                    pkg.update(status=action, elapsed=elapsed)

    for pkg in packages.values():
        if not pkg["started"] or not pkg["status"]:
            issue(f"incomplete package: {pkg['package']}")
    for test in tests.values():
        if not test["started"] or not test["status"]:
            issue(f"incomplete test: {test['package']}: {test['test']}")
        if test["status"] == "fail" and packages[test["package"]]["status"] != "fail":
            issue(f"failed test without failed package: {test['package']}: {test['test']}")
    if not packages:
        issue("no package completion evidence")
    if not tests:
        issue("no tests observed")

    # Parents with observed descendants are containers, not additional tests.
    parents = set()
    for package, name in tests:
        parts = name.split("/")
        parents.update((package, "/".join(parts[:i])) for i in range(1, len(parts)))
    leaves = [t for key, t in tests.items() if key not in parents]
    counts = Counter(t["status"] for t in leaves if t["status"] in TERMINAL)
    failed = [t for t in leaves if t["status"] == "fail"]
    failing_ancestors = set()
    for (package, name), test in tests.items():
        if test["status"] == "fail":
            parts = name.split("/")
            failing_ancestors.update((package, "/".join(parts[:i])) for i in range(1, len(parts)))
    containers = [t for key, t in tests.items()
                  if key in parents and key not in failing_ancestors and t["status"] == "fail"]
    unfinished = [t for t in tests.values() if not t["status"]]

    expected = meta.get("required_tests", [])
    if not isinstance(expected, list) or any(not isinstance(name, str) for name in expected):
        issue("invalid required_tests metadata")
        expected = []
    observed = {name for (_, name), t in tests.items() if t["started"] and "/" not in name}
    if meta.get("profile") == "fast":
        if not expected:
            issue("fast selection is empty")
        missing = sorted(set(expected) - observed)
        unexpected = sorted(observed - set(expected))
        if missing:
            issue("fast tests not observed: " + ", ".join(missing))
        if unexpected:
            issue("unexpected fast tests: " + ", ".join(unexpected))

    stderr = Tail()
    stderr_path = run_dir / "stderr.log"
    if stderr_path.is_file():
        with stderr_path.open(encoding="utf-8", errors="replace") as stream:
            for line in stream:
                stderr.add(line)
    else:
        issue("missing stderr capture")

    has_failure = (code not in (None, 0) or build_failures or
                   any(p["status"] == "fail" for p in packages.values()) or
                   any(t["status"] == "fail" for t in tests.values()) or meta.get("interrupted_signal"))
    result = "FAIL" if has_failure else "ERROR" if issue_count else "PASS"
    diagnostics = []
    owners = [(t["package"], t["test"]) for t in failed + containers + unfinished]
    owners += [(p["package"], "") for p in packages.values() if p["status"] != "pass"]
    owners += [(p, "") for p in sorted(build_failures)]
    for owner in dict.fromkeys(owners):
        diagnostics.append({"package": owner[0], "test": owner[1],
                            **tails.get(owner, Tail()).data()})
    slow = sorted((t for t in tests.values() if "/" not in t["test"] and t["elapsed"] is not None),
                  key=lambda t: t["elapsed"], reverse=True)[:5]
    return {"profile": meta.get("profile", "unknown"), "result": result, "exit_code": code,
            "interrupted_signal": meta.get("interrupted_signal"),
            "started_at": meta.get("started_at"), "ended_at": meta.get("ended_at"),
            "wall_seconds": wall, "command": command, "applicability": HISTORY,
            "raw_path": str(raw), "stderr_path": str(stderr_path), "integrity": "complete" if not issue_count else "error",
            "issues": issues, "issue_count": issue_count,
            "counts": {"passed": counts["pass"], "failed": counts["fail"], "skipped": counts["skip"]},
            "counting": "leaf tests; observed parent containers excluded",
            "packages": list(packages.values()), "build_failures": sorted(build_failures),
            "failed_tests": failed, "failed_containers": containers, "unfinished_tests": unfinished,
            "slow_tests": slow, "diagnostics": diagnostics, "stderr": stderr.data()}


def render(summary):
    s = summary
    c = s["counts"]
    command = shlex.join(s["command"]) if s["command"] else "unknown"
    if len(command) > 240:
        command = command[:240] + " ... [command truncated; see run.json]"
    lines = [f"PROFILE   {s['profile']}", f"RESULT    {s['result']} (process exit: {s['exit_code']})",
             f"RUN       {s['started_at']}",
             f"TESTS     {c['passed']} passed / {c['failed']} failed / {c['skipped']} skipped (leaf tests)",
             f"WALL      {s['wall_seconds']}s", f"INTEGRITY {s['integrity']}",
             f"COMMAND   {command}",
             f"SCOPE     {s['applicability']}"]
    if s["interrupted_signal"]:
        lines.append(f"INTERRUPTED signal {s['interrupted_signal']}")
    for pkg in s["packages"]:
        lines.append(f"PACKAGE   {pkg['package']}: {pkg['status'] or 'INCOMPLETE'} ({pkg['elapsed']}s)")
    for pkg in s["build_failures"]:
        lines.append(f"BUILD FAIL {pkg}")
    for label, tests in (("FAILED", s["failed_tests"]), ("FAILED CONTAINER", s["failed_containers"]),
                         ("UNFINISHED", s["unfinished_tests"])):
        for test in tests[:MAX_GROUPS]:
            lines.append(f"{label} {test['package']}: {test['test']}")
        if len(tests) > MAX_GROUPS:
            lines.append(f"  ... {len(tests) - MAX_GROUPS} more {label.lower()} entries; see summary.json/raw log")
    for message in s["issues"]:
        lines.append("REPORT ERROR " + message)
    if s["issue_count"] > len(s["issues"]):
        lines.append(f"  ... {s['issue_count'] - len(s['issues'])} more integrity issues")
    if s["result"] != "PASS":
        for diag in s["diagnostics"][:MAX_GROUPS]:
            lines.append(f"OUTPUT {diag['package']}: {diag['test'] or '(package)'}")
            lines.extend("  " + line for line in diag["lines"])
            if diag["omitted_lines"]:
                lines.append(f"  ... {diag['omitted_lines']} earlier diagnostic lines omitted")
        if len(s["diagnostics"]) > MAX_GROUPS:
            lines.append(f"  ... {len(s['diagnostics']) - MAX_GROUPS} diagnostic groups omitted; see raw log")
        lines.extend("STDERR    " + line for line in s["stderr"]["lines"])
        if s["stderr"]["omitted_lines"]:
            lines.append(f"  ... {s['stderr']['omitted_lines']} earlier stderr lines omitted")
    for test in s["slow_tests"]:
        lines.append(f"SLOW      {test['package']}: {test['test']} {test['elapsed']}s (top-level, includes children)")
    lines.extend([f"RAW       {s['raw_path']}", f"STDERR    {s['stderr_path']}"])
    return "\n".join(lines) + "\n"


def exit_status(summary):
    code = summary["exit_code"]
    return code if code not in (None, 0) else 0 if summary["result"] == "PASS" else 2


def fast_tests(path):
    names = [line.split("#", 1)[0].strip() for line in path.read_text().splitlines()]
    names = [name for name in names if name]
    if not names or len(names) != len(set(names)) or any(not re.fullmatch(r"Test\w+", n) for n in names):
        raise ValueError("fast selection must contain unique, exact Test names and must not be empty")
    return names


def profile_command(profile):
    required = fast_tests(ROOT / "scripts/go-test-fast.txt") if profile == "fast" else []
    command = ["go", "test", "-json"]
    if profile == "race":
        command.append("-race")
    command += ["-mod=readonly", "-count=1", "-timeout=2m" if profile == "fast" else "-timeout=10m"]
    command.append("./..." if profile == "full" else "./cmd/coordinator")
    if required:
        command += ["-run", "^(" + "|".join(required) + ")$"]
    return command, required


def execute(profile, command, reports, cwd=ROOT, required=()):
    profile_dir = reports / profile
    profile_dir.mkdir(parents=True, exist_ok=True)
    run_dir = Path(tempfile.mkdtemp(prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-"), dir=profile_dir))
    meta = {"schema_version": 1, "profile": profile, "command": command, "cwd": str(cwd),
            "started_at": utc(), "ended_at": None, "wall_seconds": None, "exit_code": None,
            "complete": False, "required_tests": list(required), "interrupted_signal": None}
    atomic_json(run_dir / "run.json", meta)
    atomic_json(profile_dir / "latest.json", {"run": run_dir.name})
    print(f"Running {profile}; full command recorded in run.json\nArtifacts: {run_dir}", flush=True)
    start, process, interrupted_at = time.monotonic(), None, None
    previous = {}

    def forward(signum, _frame):
        nonlocal interrupted_at
        if interrupted_at is None:
            interrupted_at = time.monotonic()
            meta["interrupted_signal"] = signum
            delivered = signum
        else:
            delivered = signal.SIGKILL
        if process is not None:
            try:
                os.killpg(process.pid, delivered)
            except ProcessLookupError:
                pass

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, forward)
        with (run_dir / "events.jsonl").open("wb") as raw, (run_dir / "stderr.log").open("wb") as stderr:
            process = subprocess.Popen(command, cwd=cwd, stdout=raw, stderr=stderr, start_new_session=True)
            if meta["interrupted_signal"]:
                os.killpg(process.pid, meta["interrupted_signal"])
            while True:
                try:
                    returned = process.wait(timeout=0.2)
                    break
                except subprocess.TimeoutExpired:
                    if interrupted_at is not None and time.monotonic() - interrupted_at > 5:
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
            meta["process_returncode"] = returned
            meta["exit_code"] = returned if returned >= 0 else 128 - returned
    except OSError as error:
        meta["runner_error"] = str(error)
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        meta.update(ended_at=utc(), wall_seconds=round(time.monotonic() - start, 3), complete=True)
        # Preserve the test status even if writing/summarizing artifacts fails.
        try:
            atomic_json(run_dir / "run.json", meta)
            summary = summarize(run_dir)
            atomic_json(run_dir / "summary.json", summary)
            human = render(summary)
            (run_dir / "summary.txt").write_text(human, encoding="utf-8")
            print(human, end="")
        except (OSError, ValueError, TypeError, KeyError) as error:
            print(f"REPORT ERROR: {error}; inspect {run_dir}", file=sys.stderr)
            meta["runner_error"] = "report generation failed: " + str(error)
            try:
                atomic_json(run_dir / "run.json", meta)
            except OSError:
                pass  # Disk failure may also prevent persisting the report error.
            return meta["exit_code"] or 2
    return exit_status(summary)


def show_reports(reports):
    good = True
    for profile in PROFILES:
        pointer = reports / profile / "latest.json"
        if not pointer.is_file():
            print(f"PROFILE {profile}: MISSING report ({pointer})\n")
            good = False
            continue
        try:
            run = json.loads(pointer.read_text(encoding="utf-8"))["run"]
            if not isinstance(run, str) or Path(run).name != run or run in (".", ".."):
                raise ValueError("invalid run pointer")
            run_dir = pointer.parent / run
            summary = summarize(run_dir)  # Reparse; never trust a saved PASS blindly.
            if summary["profile"] != profile:
                raise ValueError("profile does not match saved run")
            print(render(summary))
            good = good and summary["result"] == "PASS"
        except (OSError, ValueError, TypeError, KeyError) as error:
            print(f"PROFILE {profile}: ERROR reading report: {error}\n")
            good = False
    return 0 if good else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports", type=Path, default=ROOT / ".test-reports")
    sub = parser.add_subparsers(dest="action", required=True)
    sub.add_parser("run").add_argument("profile", choices=PROFILES)
    sub.add_parser("report")
    args = parser.parse_args()
    try:
        if args.action == "report":
            return show_reports(args.reports.resolve())
        command, required = profile_command(args.profile)
        return execute(args.profile, command, args.reports.resolve(), required=required)
    except (OSError, ValueError) as error:
        print(f"TEST INFRASTRUCTURE ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
