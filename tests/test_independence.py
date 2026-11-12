"""Independence test: joons_allocator must import and work with ONLY the
standard library — joons_agent_guard must NOT be importable anywhere in the
package. The child runs in isolated mode (``python -I``, which also ignores
PYTHONPATH) and gets exactly one path entry: the project's ``src`` directory.
``vendor`` is never on its sys.path."""
import os
import subprocess
import sys
import tempfile
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(PROJECT_ROOT, "src")

_SCRIPT = r"""
import sys

src, db = sys.argv[1], sys.argv[2]
sys.path.insert(0, src)


class _HideGuard:
    # The guard may be installed in this interpreter (e.g. the CI job that
    # tests the optional integration). Hide it so the child proves that
    # joons_allocator works when the guard is absent.
    def find_spec(self, name, path=None, target=None):
        if name == "joons_agent_guard" or name.startswith("joons_agent_guard."):
            raise ModuleNotFoundError("No module named %r (hidden by test)" % name, name=name)
        return None


sys.meta_path.insert(0, _HideGuard())

# the hard requirement: no guard reachable, none in sys.modules, ever
try:
    import joons_agent_guard  # noqa: F401
except ModuleNotFoundError:
    pass
else:
    raise AssertionError("joons_agent_guard must not be importable")
assert not [m for m in sys.modules if m.startswith("joons_agent_guard")]

import joons_allocator

a = joons_allocator.Allocator(db)
result = a.submit({"task": "work", "n": 1}, principal="p", request_id="r-1")
assert result.status == "accepted", result
replay = a.submit({"task": "work", "n": 1}, principal="p", request_id="r-1")
assert (replay.status, replay.task_id) == ("duplicate", result.task_id)
task = a.lease("worker-1")
assert task.task_id == result.task_id
assert task.state == "running"
assert a.complete("worker-1", task, "ok")
assert a.get(task.task_id).state == "succeeded"
assert a.workers()[0].worker_id == "worker-1"
assert a.heartbeat("worker-1")
assert "joons_agent_guard" not in sys.modules
print("independence-ok", joons_allocator.__version__)
"""


class TestGuardIndependence(unittest.TestCase):
    def test_package_imports_and_works_without_the_guard(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "queue.sqlite")
            proc = subprocess.run(
                [sys.executable, "-I", "-c", _SCRIPT, SRC, db],
                capture_output=True, text=True, timeout=120)
        self.assertEqual(proc.returncode, 0,
                         msg="%s\n%s" % (proc.stdout, proc.stderr))
        self.assertIn("independence-ok", proc.stdout)

    def test_src_path_used_by_this_test_holds_no_guard(self):
        self.assertTrue(os.path.isdir(os.path.join(SRC, "joons_allocator")))
        self.assertFalse(os.path.exists(os.path.join(SRC, "joons_agent_guard")))


if __name__ == "__main__":
    unittest.main()
