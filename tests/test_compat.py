"""Version-compatibility regression tests (review D0), ported.

The whole package must (a) parse as Python 3.9 syntax and (b) import and run
on the oldest interpreter available through the Windows ``py`` launcher —
the review's repro showed an import-time NameError on 3.12 from a function
annotation evaluated before the class it names was defined. The syntax floor
is checked here on every interpreter; the runtime floor needs a real old
interpreter, so that test skips when none is installed.

Note: ``ast.parse(feature_version=(3, 9))`` catches 3.10+ syntax only; it
cannot catch name-resolution problems like the original defect. The
``py -3.12`` runtime check below is the one that catches those.

The smoke script runs with PYTHONPATH pointing at ``src`` ONLY (no vendor):
independence from joons-agent-guard is part of the contract.
"""
import ast
import os
import shutil
import subprocess
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(PROJECT_ROOT, "src")


def _project_py_files():
    for base_dir in (os.path.join(PROJECT_ROOT, "src", "joons_allocator"),
                     os.path.join(PROJECT_ROOT, "tests")):
        for base, dirs, files in os.walk(base_dir):
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for name in sorted(files):
                if name.endswith(".py"):
                    yield os.path.join(base, name)


class TestSyntaxFloor(unittest.TestCase):
    def test_every_project_file_parses_as_python_3_9(self):
        checked = 0
        for path in _project_py_files():
            with open(path, "rb") as handle:
                ast.parse(handle.read(), filename=path, feature_version=(3, 9))
            checked += 1
        self.assertGreaterEqual(checked, 15)  # 9 package modules + 11 test files


def _find_old_interpreter():
    """Return the argv prefix for a Python 3.12 via the py launcher, or None."""
    if os.name != "nt" or not shutil.which("py"):
        return None
    probe = subprocess.run(
        ["py", "-3.12", "-c", "import sys; print(sys.version_info[:2])"],
        capture_output=True, text=True, timeout=60)
    if probe.returncode != 0 or "(3, 12)" not in probe.stdout:
        return None
    return ["py", "-3.12"]


_SMOKE_SCRIPT = r"""
import os
import asyncio
import joons_allocator
from joons_allocator import Allocator, AsyncAllocator

db = os.environ["JOONS_SMOKE_DB"]
a = Allocator(db)
result = a.submit({"task": "work"}, principal="principal-a", request_id="req-1",
                  require_approval=True)
assert result.status == "accepted", result
assert a.get(result.task_id).state == "awaiting_approval"
assert a.lease("worker-1") is None  # gated
assert a.approve(result.task_id, "op-1")
task = a.lease("worker-1")
assert task.task_id == result.task_id
assert a.complete("worker-1", task, "done")
final_state = a.get(task.task_id).state
assert final_state == "succeeded"
assert [e.action for e in a.history(task.task_id)] == ["approve"]
a.close()


async def main():
    async with AsyncAllocator(db) as aa:
        r = await aa.submit({"task": "async-work"}, principal="principal-a",
                            request_id="req-2")
        assert r.status == "accepted", r
        t = await aa.lease("worker-2")
        assert t.task_id == r.task_id
        assert await aa.complete("worker-2", t, "ok")
        assert (await aa.get(t.task_id)).state == "succeeded"

asyncio.run(main())
print("smoke-ok", joons_allocator.__version__, final_state)
"""


class TestOldestInterpreter(unittest.TestCase):
    def test_import_and_quick_flow_on_py_312(self):
        launcher = _find_old_interpreter()
        if launcher is None:
            self.skipTest("py -3.12 is not available on this machine")
        env = dict(os.environ)
        env["PYTHONPATH"] = SRC  # src only: the package must not need vendor
        env["JOONS_SMOKE_DB"] = os.path.join(
            os.environ.get("TEMP", PROJECT_ROOT), "joons_smoke_312.sqlite")
        if os.path.exists(env["JOONS_SMOKE_DB"]):
            os.remove(env["JOONS_SMOKE_DB"])
        proc = subprocess.run(launcher + ["-c", _SMOKE_SCRIPT], capture_output=True,
                              text=True, env=env, timeout=120)
        self.assertEqual(proc.returncode, 0, msg=proc.stderr)
        self.assertIn("smoke-ok", proc.stdout)
        self.assertIn("succeeded", proc.stdout)


if __name__ == "__main__":
    unittest.main()
