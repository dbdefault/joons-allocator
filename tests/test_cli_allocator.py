"""Tests for the joons-allocator CLI (``src/joons_allocator/__main__.py``):
in-process ``main(argv)`` calls and end-to-end subprocess runs of
``python -m joons_allocator``. Standard library only.

Read-only tests prepare the database with raw ``sqlite3`` so they pin the
CLI's SELECT behaviour against the SPEC tables (``tasks``, ``workers``,
``task_audit``) independently of the facade, including missing-table and
never-writes guarantees. Write-command tests go through the ``Allocator``
facade. The tests need the core package to exist and cannot pass before it
does; the CLI itself is exercised exactly as a user would.
"""
from f2_test_helpers import lease_token
import contextlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(PROJECT_ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from joons_allocator import Allocator  # noqa: E402
from joons_allocator import __main__ as cli  # noqa: E402


def _child_env():
    env = dict(os.environ)
    existing = env.get("PYTHONPATH")
    env["PYTHONPATH"] = SRC + (os.pathsep + existing if existing else "")
    return env


# The SPEC tables, created directly so read-only tests do not need the facade.
_SCHEMA = (
    "CREATE TABLE tasks ("
    "task_id TEXT PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL, "
    "result TEXT, priority INTEGER NOT NULL DEFAULT 0, resource TEXT, "
    "worker_id TEXT, lease_token TEXT, created REAL NOT NULL, "
    "updated REAL NOT NULL, version INTEGER NOT NULL)",
    "CREATE TABLE workers ("
    "worker_id TEXT PRIMARY KEY, registered REAL NOT NULL, last_seen REAL NOT NULL, "
    "current_task TEXT, consecutive_failures INTEGER NOT NULL, paused INTEGER NOT NULL)",
    "CREATE TABLE task_audit ("
    "id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, action TEXT NOT NULL, "
    "before_state TEXT NOT NULL, after_state TEXT NOT NULL, decided_by TEXT NOT NULL, "
    "note TEXT NOT NULL, ts REAL NOT NULL)",
)


def _raw_db(path, tables=("tasks", "workers", "task_audit")):
    """Create the requested SPEC tables directly, without the facade."""
    conn = sqlite3.connect(path)
    names = {"tasks": _SCHEMA[0], "workers": _SCHEMA[1], "task_audit": _SCHEMA[2]}
    for table in tables:
        conn.execute(names[table])
    conn.commit()
    return conn


def _insert_task(conn, task_id, state="queued", priority=0, created=1.0,
                 resource=None, result=None, worker_id=None):
    payload = json.dumps({"n": task_id}, sort_keys=True, separators=(",", ":"))
    conn.execute(
        "INSERT INTO tasks (task_id, state, payload, result, priority, resource, "
        "worker_id, lease_token, created, updated, version) "
        "VALUES (?,?,?,?,?,?,?,NULL,?,?,1)",
        (task_id, state, payload, result, priority, resource, worker_id,
         created, created + 0.5))
    conn.commit()


class CliTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name
        self.path = os.path.join(self._tmp, "alloc.sqlite")

    # -- in-process --------------------------------------------------------
    def run_cli(self, *argv):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main(["--db", self.path, *argv])
        return code, out.getvalue()

    def run_cli_json(self, *argv):
        code, text = self.run_cli(*argv)
        return code, json.loads(text)

    # -- subprocess --------------------------------------------------------
    def run_child(self, *argv):
        proc = subprocess.run(
            [sys.executable, "-m", "joons_allocator", "--db", self.path, *argv],
            capture_output=True, text=True, env=_child_env(), cwd=PROJECT_ROOT)
        self.assertEqual(proc.stderr, "", "the CLI must not print stack traces")
        return proc.returncode, proc.stdout

    def run_child_json(self, *argv):
        code, text = self.run_child(*argv)
        return code, json.loads(text)

    # -- shared helpers ------------------------------------------------------
    def touch_db_file(self):
        with open(self.path, "wb"):
            pass

    def make_allocator(self):
        allocator = Allocator(self.path)
        self.addCleanup(allocator.close)
        return allocator

    def task_state(self, task_id):
        allocator = self.make_allocator()
        task = allocator.get(task_id)
        return None if task is None else task.state


class ReadOnlyCommandTests(CliTestCase):
    """list / queue / show / history / workers over raw SPEC tables."""

    def test_list_json_and_order(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        _insert_task(conn, "t-b", created=2.0)
        _insert_task(conn, "t-a", created=1.0)
        code, data = self.run_cli_json("--json", "list")
        self.assertEqual(code, 0)
        self.assertEqual([t["task_id"] for t in data["tasks"]], ["t-a", "t-b"])
        self.assertEqual(data["tasks"][0]["state"], "queued")
        self.assertEqual(data["tasks"][0]["priority"], 0)

    def test_list_text_table(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        _insert_task(conn, "t-1", result="done")
        code, text = self.run_cli("list")
        self.assertEqual(code, 0)
        self.assertIn("TASK_ID", text)
        self.assertIn("t-1", text)
        self.assertIn("done", text)

    def test_list_state_filter(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        _insert_task(conn, "t-q", state="queued")
        _insert_task(conn, "t-r", state="running", worker_id="w-1")
        code, data = self.run_cli_json("--json", "list", "--state", "running")
        self.assertEqual(code, 0)
        self.assertEqual([t["task_id"] for t in data["tasks"]], ["t-r"])
        code, text = self.run_cli("list", "--state", "bogus")
        self.assertEqual(code, 2)  # bad usage: not a state

    def test_list_missing_tasks_table_is_empty(self):
        self.touch_db_file()
        code, data = self.run_cli_json("--json", "list")
        self.assertEqual(code, 0)
        self.assertEqual(data["tasks"], [])

    def test_list_missing_db(self):
        code, text = self.run_cli("list")
        self.assertEqual(code, 1)
        self.assertIn("database not found", text)

    def test_queue_priority_then_oldest(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        _insert_task(conn, "t-high-late", priority=5, created=9.0)
        _insert_task(conn, "t-high-early", priority=5, created=2.0)
        _insert_task(conn, "t-low", priority=1, created=1.0)
        _insert_task(conn, "t-running", state="running", worker_id="w-1", created=0.5)
        _insert_task(conn, "t-done", state="succeeded", result="ok", created=0.1)
        code, data = self.run_cli_json("--json", "queue")
        self.assertEqual(code, 0)
        self.assertEqual([t["task_id"] for t in data["queue"]],
                         ["t-high-early", "t-high-late", "t-low"])

    def test_queue_text_and_empty(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        code, text = self.run_cli("queue")
        self.assertEqual(code, 0)
        self.assertIn("no queued tasks", text)
        _insert_task(conn, "t-1", resource="lockA")
        code, text = self.run_cli("queue")
        self.assertEqual(code, 0)
        self.assertIn("lockA", text)

    def test_show_json(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        _insert_task(conn, "t-1", priority=3, resource="lockA", worker_id="w-1")
        code, data = self.run_cli_json("--json", "show", "t-1")
        self.assertEqual(code, 0)
        task = data["task"]
        self.assertEqual(task["task_id"], "t-1")
        self.assertEqual(task["payload"], {"n": "t-1"})
        self.assertEqual(task["priority"], 3)
        self.assertEqual(task["resource"], "lockA")
        self.assertEqual(task["worker_id"], "w-1")

    def test_show_text_and_missing(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        _insert_task(conn, "t-1")
        code, text = self.run_cli("show", "t-1")
        self.assertEqual(code, 0)
        self.assertIn("task_id: t-1", text)
        code, text = self.run_cli("show", "nope")
        self.assertEqual(code, 1)
        self.assertIn("no such task", text)

    def test_show_missing_tasks_table(self):
        self.touch_db_file()
        code, text = self.run_cli("show", "t-1")
        self.assertEqual(code, 1)
        self.assertIn("no such task", text)

    def test_history_rows_and_missing_task(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        _insert_task(conn, "t-1")
        conn.execute("INSERT INTO task_audit (task_id, action, before_state, "
                     "after_state, decided_by, note, ts) "
                     "VALUES ('t-1','approve','awaiting_approval','queued','op','n',1.0)")
        conn.commit()
        code, data = self.run_cli_json("--json", "history", "t-1")
        self.assertEqual(code, 0)
        self.assertEqual(data["history"][0]["action"], "approve")
        self.assertEqual(data["history"][0]["decided_by"], "op")
        code, text = self.run_cli("history", "nope")
        self.assertEqual(code, 1)
        self.assertIn("no such task", text)

    def test_history_without_audit_table(self):
        conn = _raw_db(self.path, tables=("tasks",))
        self.addCleanup(conn.close)
        _insert_task(conn, "t-1")
        code, data = self.run_cli_json("--json", "history", "t-1")
        self.assertEqual(code, 0)
        self.assertEqual(data["history"], [])

    def test_workers_json_and_text(self):
        conn = _raw_db(self.path)
        self.addCleanup(conn.close)
        conn.execute("INSERT INTO workers VALUES ('w-1', 1.0, 2.0, 't-1', 0, 0)")
        conn.execute("INSERT INTO workers VALUES ('w-2', 0.5, 3.0, NULL, 2, 1)")
        conn.commit()
        code, data = self.run_cli_json("--json", "workers")
        self.assertEqual(code, 0)
        self.assertEqual([w["worker_id"] for w in data["workers"]], ["w-2", "w-1"])
        self.assertTrue(data["workers"][0]["paused"])
        self.assertIsNone(data["workers"][0]["current_task"])
        code, text = self.run_cli("workers")
        self.assertEqual(code, 0)
        self.assertIn("w-1", text)
        self.assertIn("yes", text)

    def test_workers_missing_table(self):
        self.touch_db_file()
        code, data = self.run_cli_json("--json", "workers")
        self.assertEqual(code, 0)
        self.assertEqual(data["workers"], [])

    def test_read_commands_never_modify_the_file(self):
        conn = _raw_db(self.path)
        _insert_task(conn, "t-1")
        conn.close()
        with open(self.path, "rb") as fh:
            before = fh.read()
        for argv in (("list",), ("queue",), ("show", "t-1"), ("history", "t-1"),
                     ("workers",)):
            code, _ = self.run_cli(*argv)
            self.assertEqual(code, 0)
        with open(self.path, "rb") as fh:
            self.assertEqual(fh.read(), before)


class SubmitTests(CliTestCase):
    """submit goes through the Allocator facade; no --yes."""

    def setUp(self):
        super().setUp()
        self.touch_db_file()  # the CLI works on an existing database file

    def test_submit_accepted(self):
        code, data = self.run_cli_json("submit", "--payload", '{"a": 1}', "--json")
        self.assertEqual(code, 0)
        self.assertEqual(data["status"], "accepted")
        allocator = self.make_allocator()
        task = allocator.get(data["task_id"])
        self.assertIsNotNone(task)
        self.assertEqual(task.state, "queued")
        self.assertEqual(task.payload, {"a": 1})
        self.assertEqual(task.priority, 0)
        self.assertIsNone(task.resource)

    def test_submit_with_priority_and_resource(self):
        code, data = self.run_cli_json(
            "submit", "--payload", '{"a": 1}', "--priority", "7",
            "--resource", "lockA", "--json")
        self.assertEqual(code, 0)
        allocator = self.make_allocator()
        task = allocator.get(data["task_id"])
        self.assertEqual(task.priority, 7)
        self.assertEqual(task.resource, "lockA")

    def test_submit_duplicate_request_id(self):
        first = self.run_cli_json("submit", "--payload", '{"a": 1}',
                                  "--request-id", "req-1", "--json")
        self.assertEqual(first[0], 0)
        second = self.run_cli_json("submit", "--payload", '{"a": 1}',
                                   "--request-id", "req-1", "--json")
        self.assertEqual(second[0], 0)
        self.assertEqual(second[1]["status"], "duplicate")
        self.assertEqual(second[1]["task_id"], first[1]["task_id"])
        code, data = self.run_cli_json("--json", "list")
        self.assertEqual(len(data["tasks"]), 1)

    def test_submit_conflict(self):
        first = self.run_cli_json("submit", "--payload", '{"a": 1}',
                                  "--request-id", "req-1", "--json")
        self.assertEqual(first[0], 0)
        code, data = self.run_cli_json("submit", "--payload", '{"a": 2}',
                                       "--request-id", "req-1", "--json")
        self.assertEqual(code, 1)
        self.assertEqual(data["status"], "conflict")
        code, data = self.run_cli_json("--json", "list")
        self.assertEqual(len(data["tasks"]), 1)  # nothing written

    def test_submit_bad_json(self):
        code, text = self.run_cli("submit", "--payload", '{"a": ')
        self.assertEqual(code, 2)
        self.assertIn("invalid --payload JSON", text)

    def test_submit_rejects_non_finite_numbers(self):
        for token in ("NaN", "Infinity"):
            code, text = self.run_cli("submit", "--payload", '{"x": %s}' % token)
            self.assertEqual(code, 2, token)
            self.assertIn("non-finite", text)

    def test_submit_rejects_non_object_payload(self):
        code, text = self.run_cli("submit", "--payload", "[1, 2]")
        self.assertEqual(code, 2)
        self.assertIn("JSON object", text)

    def test_submit_missing_db(self):
        missing = os.path.join(self._tmp, "nope.sqlite")
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            code = cli.main(["--db", missing, "submit", "--payload", "{}"])
        self.assertEqual(code, 1)
        self.assertIn("database not found", out.getvalue())

    def test_submit_subprocess_roundtrip(self):
        code, data = self.run_child_json("submit", "--payload", '{"via": "subprocess"}',
                                         "--priority", "2", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(data["status"], "accepted")
        code, data = self.run_child_json("--json", "queue")
        self.assertEqual(code, 0)
        self.assertEqual(data["queue"][0]["priority"], 2)
        allocator = self.make_allocator()
        self.assertEqual(allocator.get(data["queue"][0]["task_id"]).payload,
                         {"via": "subprocess"})


class DecisionTests(CliTestCase):
    """approve/pause/resume/cancel/retry/recover: plan without --yes, act with it."""

    def setUp(self):
        super().setUp()
        self.allocator = self.make_allocator()

    def submit(self, require_approval=False, request_id="r1", payload=None):
        result = self.allocator.submit(payload or {"task": "work"},
                                       require_approval=require_approval,
                                       request_id=request_id)
        self.assertEqual(result.status, "accepted")
        return result.task_id

    def lease(self, worker_id="w-1", request_id="r1"):
        task_id = self.submit(request_id=request_id)
        self.assertIsNotNone(self.allocator.lease(worker_id))
        return task_id

    def test_plan_without_yes_changes_nothing(self):
        task_id = self.submit(require_approval=True)
        code, text = self.run_cli("approve", task_id, "--by", "op")
        self.assertEqual(code, 2)
        self.assertIn("would approve", text)
        self.assertIn("--yes", text)
        self.assertEqual(self.task_state(task_id), "awaiting_approval")
        self.assertEqual(self.allocator.history(task_id), [])  # nothing audited

    def test_approve_applies_and_audits(self):
        task_id = self.submit(require_approval=True)
        code, data = self.run_cli_json("approve", task_id, "--by", "op",
                                       "--note", "go", "--yes", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(data["state"], "queued")
        self.assertEqual(self.task_state(task_id), "queued")
        code, data = self.run_cli_json("--json", "history", task_id)
        entry = data["history"][0]
        self.assertEqual((entry["action"], entry["before_state"], entry["after_state"],
                          entry["decided_by"], entry["note"]),
                         ("approve", "awaiting_approval", "queued", "op", "go"))

    def test_approve_twice_is_refused(self):
        task_id = self.submit(require_approval=True)
        self.assertEqual(self.run_cli("approve", task_id, "--by", "op", "--yes")[0], 0)
        code, text = self.run_cli("approve", task_id, "--by", "op", "--yes")
        self.assertEqual(code, 1)
        self.assertIn("cannot approve", text)
        self.assertIn("awaiting_approval", text)

    def test_pause_then_resume(self):
        task_id = self.submit()
        self.assertEqual(self.run_cli("pause", task_id, "--by", "op", "--yes")[0], 0)
        self.assertEqual(self.task_state(task_id), "paused")
        self.assertEqual(self.run_cli("resume", task_id, "--by", "op", "--yes")[0], 0)
        self.assertEqual(self.task_state(task_id), "queued")

    def test_pause_awaiting_approval_is_refused(self):
        task_id = self.submit(require_approval=True)
        code, text = self.run_cli("pause", task_id, "--by", "op", "--yes")
        self.assertEqual(code, 1)
        self.assertIn("queued", text)  # allowed-from list is named

    def test_cancel_from_awaiting_approval(self):
        task_id = self.submit(require_approval=True)
        self.assertEqual(self.run_cli("cancel", task_id, "--by", "op", "--yes")[0], 0)
        self.assertEqual(self.task_state(task_id), "cancelled")
        code, _ = self.run_cli("cancel", task_id, "--by", "op", "--yes")
        self.assertEqual(code, 1)  # cancelled is terminal

    def test_unknown_task_is_not_found(self):
        code, text = self.run_cli("approve", "nope", "--by", "op", "--yes")
        self.assertEqual(code, 1)
        self.assertIn("no such task", text)

    def test_retry_after_failure(self):
        task_id = self.lease()
        self.assertTrue(self.allocator.fail("w-1", task_id, "boom", lease_token=lease_token(self.allocator, task_id)))
        self.assertEqual(self.task_state(task_id), "failed")
        code, data = self.run_cli_json("retry", task_id, "--by", "op", "--yes", "--json")
        self.assertEqual(code, 0)
        self.assertEqual(data["state"], "queued")
        self.assertEqual(self.task_state(task_id), "queued")
        code, _ = self.run_cli("retry", task_id, "--by", "op", "--yes")
        self.assertEqual(code, 1)  # no longer failed

    def test_recover_running_task(self):
        task_id = self.lease("w-1")
        code, data = self.run_cli_json(
            "recover", task_id, "--worker", "w-1", "--by", "op", "--yes", "--json", "--lease-token", lease_token(self.allocator, task_id))
        self.assertEqual(code, 0)
        self.assertEqual(data["state"], "failed")
        self.assertEqual(self.task_state(task_id), "failed")
        code, data = self.run_cli_json("--json", "history", task_id)
        self.assertEqual(data["history"][-1]["action"], "recover")

    def test_recover_wrong_worker_is_refused(self):
        task_id = self.lease("w-1")
        code, text = self.run_cli("recover", task_id, "--worker", "w-other",
                                  "--by", "op", "--yes", "--lease-token", lease_token(self.allocator, task_id))
        self.assertEqual(code, 1)
        self.assertEqual(self.task_state(task_id), "running")  # untouched

    def test_missing_yes_exits_2(self):
        task_id = self.submit()
        for argv in (("pause", task_id, "--by", "op"),
                     ("retry", task_id, "--by", "op"),
                     ("cancel", task_id, "--by", "op")):
            code, text = self.run_cli(*argv)
            self.assertEqual(code, 2, argv)
            self.assertIn("re-run with --yes", text)

    def test_missing_by_exits_2(self):
        task_id = self.submit()
        code, _ = self.run_cli("approve", task_id, "--yes")
        self.assertEqual(code, 2)

    def test_missing_worker_for_recover_exits_2(self):
        code, _ = self.run_cli("recover", "t-1", "--by", "op", "--yes")
        self.assertEqual(code, 2)


class SubprocessTests(CliTestCase):
    """End-to-end ``python -m joons_allocator`` runs."""

    def test_child_help(self):
        proc = subprocess.run([sys.executable, "-m", "joons_allocator", "--help"],
                              capture_output=True, text=True, env=_child_env(),
                              cwd=PROJECT_ROOT)
        self.assertEqual(proc.returncode, 0)
        self.assertIn("--db", proc.stdout)

    def test_child_missing_db_flag(self):
        proc = subprocess.run([sys.executable, "-m", "joons_allocator", "list"],
                              capture_output=True, text=True, env=_child_env(),
                              cwd=PROJECT_ROOT)
        self.assertEqual(proc.returncode, 2)

    def test_child_list_on_fresh_database(self):
        self.touch_db_file()
        code, data = self.run_child_json("--json", "list")
        self.assertEqual(code, 0)
        self.assertEqual(data["tasks"], [])

    def test_child_text_output(self):
        self.touch_db_file()
        code, text = self.run_child("list")
        self.assertEqual(code, 0)
        self.assertIn("no tasks", text)

    def test_child_decision_roundtrip(self):
        allocator = self.make_allocator()
        result = allocator.submit({"k": "v"}, require_approval=True,
                                  request_id="r-child")
        allocator.close()
        task_id = result.task_id
        code, text = self.run_child("approve", task_id, "--by", "op", "--yes")
        self.assertEqual(code, 0)
        self.assertIn("approved", text)
        code, data = self.run_child_json("--json", "show", task_id)
        self.assertEqual(data["task"]["state"], "queued")


if __name__ == "__main__":
    unittest.main()
