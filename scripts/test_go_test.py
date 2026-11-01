"""Synthetic reporting and subprocess checks; no project Go tests are invoked."""

from contextlib import redirect_stderr, redirect_stdout
import io
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import go_test as tool


def event(action, name=None, package="example/p", **extra):
    value = {"Action": action, "Package": package, **extra}
    if name is not None:
        value["Test"] = name
    return value


def passing(package="example/p", name="TestOK"):
    return [event("start", package=package), event("run", name, package),
            event("pass", name, package, Elapsed=0.2), event("pass", package=package, Elapsed=0.3)]


class Reports(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="dzkr-test-infra-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def run_dir(self, events, code=0, profile="core", required=(), **overrides):
        directory = Path(tempfile.mkdtemp(dir=self.root))
        meta = {"profile": profile, "exit_code": code, "complete": True,
                "started_at": "2026-01-01T00:00:00+00:00", "ended_at": "2026-01-01T00:00:01+00:00",
                "wall_seconds": 1.0, "command": ["go", "test", "-json"], "required_tests": list(required)}
        meta.update(overrides)
        (directory / "run.json").write_text(json.dumps(meta))
        (directory / "events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events))
        (directory / "stderr.log").write_text("")
        return directory

    def test_success_and_top_level_slow_times(self):
        summary = tool.summarize(self.run_dir(passing()))
        self.assertEqual((summary["result"], tool.exit_status(summary)), ("PASS", 0))
        self.assertEqual(summary["counts"], {"passed": 1, "failed": 0, "skipped": 0})
        self.assertEqual(summary["packages"][0]["status"], "pass")
        self.assertIn("RAW", tool.render(summary))

    def test_failing_leaf_and_parent_are_not_double_counted(self):
        events = [event("start"), event("run", "TestParent"), event("run", "TestParent/leaf"),
                  event("output", "TestParent/leaf", Output="fixture.go:72: wrong value\n"),
                  event("fail", "TestParent/leaf"), event("fail", "TestParent"), event("fail")]
        summary = tool.summarize(self.run_dir(events, code=7))
        self.assertEqual((summary["result"], tool.exit_status(summary)), ("FAIL", 7))
        self.assertEqual(summary["counts"]["failed"], 1)
        self.assertEqual([t["test"] for t in summary["failed_tests"]], ["TestParent/leaf"])
        self.assertIn("example/p: TestParent/leaf", tool.render(summary))
        self.assertIn("fixture.go:72", tool.render(summary))

    def test_failure_in_parent_after_passing_child_is_visible(self):
        events = [event("start"), event("run", "TestParent"), event("run", "TestParent/leaf"),
                  event("pass", "TestParent/leaf"), event("fail", "TestParent"), event("fail")]
        summary = tool.summarize(self.run_dir(events, code=0))
        self.assertEqual(summary["counts"], {"passed": 1, "failed": 0, "skipped": 0})
        self.assertEqual(summary["result"], "FAIL")
        self.assertNotEqual(tool.exit_status(summary), 0)
        self.assertIn("FAILED CONTAINER example/p: TestParent", tool.render(summary))

    def test_skipped_test_and_package_without_tests(self):
        events = [event("start"), event("run", "TestSkip"), event("skip", "TestSkip"), event("pass"),
                  event("start", package="example/empty"), event("skip", package="example/empty")]
        summary = tool.summarize(self.run_dir(events))
        self.assertEqual(summary["result"], "PASS")
        self.assertEqual(summary["counts"]["skipped"], 1)

    def test_build_failure_without_test_events(self):
        events = [{"Action": "build-output", "ImportPath": "example/p", "Output": "x.go:2: undefined: missing\n"},
                  {"Action": "build-fail", "ImportPath": "example/p"}]
        directory = self.run_dir(events, code=1)
        (directory / "stderr.log").write_text("FAIL example/p [build failed]\n")
        summary = tool.summarize(directory)
        self.assertEqual((summary["result"], tool.exit_status(summary)), ("FAIL", 1))
        self.assertEqual(summary["counts"]["failed"], 0)
        self.assertIn("BUILD FAIL example/p", tool.render(summary))
        self.assertIn("undefined: missing", tool.render(summary))

    def test_panic_timeout_and_incomplete_zero_exit(self):
        for code, message in [(2, "panic: broken"), (1, "panic: test timed out after 2m"), (0, "truncated")]:
            with self.subTest(code=code):
                events = [event("start"), event("run", "TestBroken"),
                          event("output", "TestBroken", Output=message)]
                summary = tool.summarize(self.run_dir(events, code=code))
                self.assertEqual(summary["result"], "FAIL" if code else "ERROR")
                self.assertNotEqual(tool.exit_status(summary), 0)
                self.assertEqual(summary["counts"]["failed"], 0)
                self.assertIn(message, tool.render(summary))
                self.assertIn("UNFINISHED", tool.render(summary))

    def test_race_failure_is_not_hidden(self):
        events = [event("start"), event("run", "TestRace"),
                  event("output", "TestRace", Output="WARNING: DATA RACE\nwriter.go:12\n"),
                  event("fail", "TestRace"), event("fail")]
        summary = tool.summarize(self.run_dir(events, code=66))
        self.assertEqual(tool.exit_status(summary), 66)
        self.assertEqual(summary["result"], "FAIL")
        self.assertIn("DATA RACE", tool.render(summary))

    def test_malformed_json_and_invalid_events_never_pass(self):
        bad_lines = ['{"Action":', 'null', '[]', '{}',
                     '{"Action":"pass","Package":[]}',
                     '{"Action":"unknown","Package":"p"}',
                     '{"Action":"output","Package":"p","Output":42}',
                     '{"Action":"pass","Package":"p","Elapsed":NaN}',
                     '{"Action":"pass","Package":"p","Elapsed":true}']
        for bad in bad_lines:
            with self.subTest(line=bad):
                directory = self.run_dir(passing())
                with (directory / "events.jsonl").open("a") as stream:
                    stream.write(bad + "\n")
                summary = tool.summarize(directory)
                self.assertEqual(summary["result"], "ERROR")
                self.assertEqual(tool.exit_status(summary), 2)
                self.assertIn("malformed event", tool.render(summary))

    def test_nonzero_exit_overrides_passing_events_without_inventing_failures(self):
        summary = tool.summarize(self.run_dir(passing(), code=23))
        self.assertEqual((summary["result"], tool.exit_status(summary)), ("FAIL", 23))
        self.assertEqual(summary["counts"]["failed"], 0)

    def test_interruption_even_with_zero_exit_does_not_pass(self):
        summary = tool.summarize(self.run_dir(passing(), interrupted_signal=signal.SIGTERM))
        self.assertEqual((summary["result"], tool.exit_status(summary)), ("FAIL", 2))
        self.assertIn("INTERRUPTED signal", tool.render(summary))

    def test_package_only_failure_is_visible_without_inventing_failed_tests(self):
        events = passing()[:-1] + [event("output", Output="package cleanup failed\n"), event("fail")]
        summary = tool.summarize(self.run_dir(events, code=1))
        self.assertEqual((summary["result"], tool.exit_status(summary)), ("FAIL", 1))
        self.assertEqual(summary["counts"], {"passed": 1, "failed": 0, "skipped": 0})
        self.assertIn("package cleanup failed", tool.render(summary))

    def test_slow_limit_and_nested_success_counts(self):
        events = [event("start"), event("run", "TestParent"), event("run", "TestParent/child"),
                  event("pass", "TestParent/child", Elapsed=0.1), event("pass", "TestParent", Elapsed=0.2)]
        for i in range(7):
            events += [event("run", f"TestSlow{i}"), event("pass", f"TestSlow{i}", Elapsed=i + 1)]
        events.append(event("pass"))
        summary = tool.summarize(self.run_dir(events))
        self.assertEqual(summary["result"], "PASS")
        self.assertEqual(summary["counts"]["passed"], 8)
        self.assertEqual(len(summary["slow_tests"]), 5)
        self.assertEqual(summary["slow_tests"][0]["test"], "TestSlow6")
        self.assertTrue(all("/" not in t["test"] for t in summary["slow_tests"]))

    def test_identical_names_in_different_packages(self):
        events = passing("example/a") + [event("start", package="example/b"),
                  event("run", "TestOK", "example/b"), event("fail", "TestOK", "example/b"),
                  event("fail", package="example/b")]
        summary = tool.summarize(self.run_dir(events, code=1))
        self.assertEqual(summary["counts"], {"passed": 1, "failed": 1, "skipped": 0})
        self.assertEqual(summary["failed_tests"][0]["package"], "example/b")

    def test_many_failures_have_bounded_explicitly_truncated_output(self):
        events = [event("start")]
        for i in range(40):
            name = f"TestFailure{i}"
            events += [event("run", name), event("output", name, Output=("x" * 500 + "\n") * 30),
                       event("fail", name)]
        events.append(event("fail"))
        summary = tool.summarize(self.run_dir(events, code=1))
        human = tool.render(summary)
        self.assertEqual(summary["counts"]["failed"], 40)
        self.assertLess(len(human), 19000)
        self.assertIn("35 more failed entries", human)
        self.assertIn("diagnostic groups omitted", human)
        self.assertIn("earlier diagnostic lines omitted", human)
        self.assertIn("[line truncated]", human)

    def test_fast_zero_partial_or_unexpected_selection_fails(self):
        cases = [([], ["TestExpected"]), (passing(), []),
                 (passing(), ["TestOK", "TestMissing"]), (passing(), ["TestOther"])]
        for events, required in cases:
            with self.subTest(required=required):
                summary = tool.summarize(self.run_dir(events, profile="fast", required=required))
                self.assertEqual((summary["result"], tool.exit_status(summary)), ("ERROR", 2))

    def test_missing_raw_stderr_and_unfinished_metadata(self):
        for filename in ["events.jsonl", "stderr.log"]:
            directory = self.run_dir(passing())
            (directory / filename).unlink()
            self.assertEqual(tool.summarize(directory)["result"], "ERROR")
        self.assertEqual(tool.summarize(self.run_dir(passing(), complete=False))["result"], "ERROR")

    def test_invalid_metadata_cannot_pass(self):
        cases = [{"command": "go test"}, {"profile": "unknown"}, {"started_at": None},
                 {"ended_at": "not a timestamp"}, {"exit_code": True}, {"wall_seconds": -1},
                 {"required_tests": [None]}]
        for overrides in cases:
            with self.subTest(overrides=overrides):
                summary = tool.summarize(self.run_dir(passing(), **overrides))
                self.assertEqual((summary["result"], tool.exit_status(summary)), ("ERROR", 2))

    def test_duplicate_completions_are_errors(self):
        summary = tool.summarize(self.run_dir(passing() + [event("pass")]))
        self.assertEqual((summary["result"], tool.exit_status(summary)), ("ERROR", 2))

    def test_saved_reports_missing_historical_and_corrupt(self):
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(tool.show_reports(self.root), 1)
        self.assertEqual(output.getvalue().count("MISSING report"), 4)
        for profile in tool.PROFILES:
            directory = self.run_dir(passing(), profile=profile,
                                     required=["TestOK"] if profile == "fast" else [])
            profile_dir = self.root / profile
            profile_dir.mkdir()
            moved = profile_dir / "old-run"
            directory.rename(moved)
            (profile_dir / "latest.json").write_text(json.dumps({"run": "old-run"}))
            # A saved PASS is never trusted instead of the raw data.
            (moved / "summary.json").write_text('{"result":"PASS"}')
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(tool.show_reports(self.root), 0)
        self.assertEqual(output.getvalue().count(tool.HISTORY), 4)
        (self.root / "core/old-run/events.jsonl").write_text('{"truncated":')
        with redirect_stdout(io.StringIO()):
            self.assertEqual(tool.show_reports(self.root), 1)

    def test_manifest_exact_selection_and_profile_commands(self):
        names = tool.fast_tests(tool.ROOT / "scripts/go-test-fast.txt")
        self.assertGreater(len(names), 0)
        for profile in tool.PROFILES:
            command, required = tool.profile_command(profile)
            self.assertIn("-json", command)
            self.assertIn("-count=1", command)
            self.assertIn("-mod=readonly", command)
            self.assertEqual("-race" in command, profile == "race")
            self.assertIn("./..." if profile == "full" else "./cmd/coordinator", command)
            if profile == "fast":
                regex = command[-1]
                self.assertEqual(required, names)
                self.assertTrue(all(re.fullmatch(regex, name) for name in names))
                self.assertIsNone(re.fullmatch(regex, names[0] + "Unrelated"))
        manifest = self.root / "fast.txt"
        for content in ["", "# only comments", "TestOne\nTestOne", "Test.*", "TestOne/child"]:
            manifest.write_text(content)
            with self.assertRaises(ValueError):
                tool.fast_tests(manifest)

    def execute_fake(self, code=0, malformed=False):
        body = "print('broken')" if malformed else "print(" + repr("\n".join(json.dumps(e) for e in passing())) + ")"
        command = [sys.executable, "-c", "import sys; " + body + f"; print('stderr retained', file=sys.stderr); sys.exit({code})"]
        with redirect_stdout(io.StringIO()):
            result = tool.execute("core", command, self.root)
        run = json.loads((self.root / "core/latest.json").read_text())["run"]
        return result, self.root / "core" / run

    def test_runner_exit_codes_and_artifacts(self):
        for code in [0, 23]:
            with self.subTest(code=code):
                result, directory = self.execute_fake(code)
                self.assertEqual(result, code)
                meta = json.loads((directory / "run.json").read_text())
                self.assertEqual(meta["exit_code"], code)
                self.assertTrue(meta["complete"])
                self.assertGreater(meta["wall_seconds"], 0)
                self.assertIn("Action", (directory / "events.jsonl").read_text())
                self.assertIn("stderr retained", (directory / "stderr.log").read_text())
                self.assertTrue((directory / "summary.txt").is_file())
                self.assertEqual(json.loads((directory / "summary.json").read_text())["result"], "FAIL" if code else "PASS")
        result, _ = self.execute_fake(malformed=True)
        self.assertEqual(result, 2)

    def test_report_write_failure_preserves_failure_and_never_passes(self):
        real = tool.atomic_json

        def fail_summary(path, value):
            if path.name == "summary.json":
                raise OSError("injected report write failure")
            real(path, value)

        for code in [0, 23]:
            with self.subTest(code=code), patch.object(tool, "atomic_json", side_effect=fail_summary), redirect_stderr(io.StringIO()):
                result, directory = self.execute_fake(code)
                self.assertEqual(result, code or 2)
                summary = tool.summarize(directory)
                self.assertNotEqual(summary["result"], "PASS")

    def test_launch_failure_is_infrastructure_error(self):
        with redirect_stdout(io.StringIO()):
            result = tool.execute("core", [str(self.root / "missing-go")], self.root)
        self.assertEqual(result, 2)

    @unittest.skipUnless(os.name == "posix", "process-group forwarding requires POSIX")
    def test_interruption_is_forwarded_and_partial_run_cannot_pass(self):
        ready = self.root / "ready"
        child = "import os,pathlib,time; " + "print(" + repr(json.dumps(event("start"))) + ",flush=True); "
        child += "print(" + repr(json.dumps(event("run", "TestBlocked"))) + ",flush=True); "
        child += f"pathlib.Path({str(ready)!r}).write_text(str(os.getpid())); time.sleep(30)"
        wrapper = (f"import sys,pathlib; sys.path.insert(0,{str(tool.ROOT / 'scripts')!r}); import go_test; "
                   f"sys.exit(go_test.execute('core',{[sys.executable, '-c', child]!r},pathlib.Path({str(self.root)!r})))")
        process = subprocess.Popen([sys.executable, "-B", "-c", wrapper], stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 5
            while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertTrue(ready.exists(), "fake test subprocess did not become ready")
            process.send_signal(signal.SIGTERM)
            stdout, stderr = process.communicate(timeout=8)
            self.assertEqual(process.returncode, 143, stdout + stderr)
            self.assertIn("RESULT    FAIL", stdout)
            self.assertIn("UNFINISHED", stdout)
            run = json.loads((self.root / "core/latest.json").read_text())["run"]
            meta = json.loads((self.root / "core" / run / "run.json").read_text())
            self.assertEqual(meta["interrupted_signal"], signal.SIGTERM)
        finally:
            if ready.exists():
                try:
                    os.killpg(int(ready.read_text()), signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if process.poll() is None:
                process.kill()
            process.communicate()


if __name__ == "__main__":
    unittest.main()
