"""Tests for joons_allocator.admission: the three verdicts, the ONE
transaction behind them, and an 8-process spawn race on submit. Standard
library only.

Ported from the prototype: the ReplayGuard/pending/reconcile/
abort and shared-file-detection tests are gone (those features were removed
— the submissions ledger is written in the same transaction as the task
row), and the verdict tests were reworked for the SubmitResult API."""
import multiprocessing
import os
import sqlite3
import tempfile
import unittest

from joons_allocator import Admission
from joons_allocator.admission import payload_digest
from joons_allocator.store import TaskStore

PRINCIPAL = "principal-a"
REQUEST_ID = "req-001"
PAYLOAD = {"task": "sum", "args": [1, 2], "meta": {"origin": "test"}}


def _submit_worker(db_path, principal, request_id, payload):
    """Spawn-pool target: one process submitting the same request."""
    store = TaskStore(db_path)
    try:
        admission = Admission(store)
        result = admission.submit(payload, principal=principal, request_id=request_id)
        return (result.status, result.task_id)
    except Exception as exc:  # report instead of crashing the pool
        return ("error", repr(exc))
    finally:
        store.close()


class AdmissionTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name

    def path(self, name):
        return os.path.join(self._tmp, name)

    def open_admission(self, name="queue.sqlite"):
        store = TaskStore(self.path(name))
        self.addCleanup(store.close)
        return store, Admission(store)

    def ledger_row(self, path, principal=PRINCIPAL, request_id=REQUEST_ID):
        """The submissions row (digest, task_id) for one request, or None."""
        conn = sqlite3.connect(path)
        try:
            return conn.execute(
                "SELECT digest, task_id FROM submissions WHERE principal=? AND request_id=?",
                (principal, request_id)).fetchone()
        finally:
            conn.close()


class TestVerdicts(AdmissionTestCase):
    def test_accepted_creates_queued_task_and_ledger_row(self):
        path = self.path("queue.sqlite")
        store, admission = self.open_admission()
        result = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        self.assertEqual((result.status, result.task_id), ("accepted", result.task_id))
        self.assertTrue(result.task_id)
        task = store.get(result.task_id)
        self.assertEqual((task.state, task.payload, task.version), ("queued", PAYLOAD, 1))
        row = self.ledger_row(path)
        self.assertIsNotNone(row)
        self.assertEqual(row[0], payload_digest({"payload": PAYLOAD, "priority": 0, "resource": None, "require_approval": False}))
        self.assertEqual(row[1], result.task_id)
        self.assertEqual([t.task_id for t in store.list()], [result.task_id])

    def test_one_file_holds_both_tables(self):
        path = self.path("queue.sqlite")
        store, admission = self.open_admission()
        result = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        conn = sqlite3.connect(path)
        try:
            tasks = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
            ledger = conn.execute(
                "SELECT task_id FROM submissions WHERE principal=? AND request_id=?",
                (PRINCIPAL, REQUEST_ID)).fetchone()
        finally:
            conn.close()
        self.assertEqual(tasks, 1)
        self.assertEqual(ledger[0], result.task_id)

    def test_duplicate_returns_same_task_id(self):
        store, admission = self.open_admission()
        first = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        second = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        self.assertEqual(first.status, "accepted")
        self.assertEqual((second.status, second.task_id), ("duplicate", first.task_id))
        self.assertEqual(len(store.list()), 1)

    def test_conflict_on_different_payload(self):
        store, admission = self.open_admission()
        first = admission.submit({"task": "a"}, principal=PRINCIPAL, request_id="req-x")
        second = admission.submit({"task": "b"}, principal=PRINCIPAL, request_id="req-x")
        self.assertEqual(first.status, "accepted")
        self.assertEqual((second.status, second.task_id), ("conflict", None))
        self.assertEqual(len(store.list()), 1)
        # the original payload is still what the ledger holds
        third = admission.submit({"task": "a"}, principal=PRINCIPAL, request_id="req-x")
        self.assertEqual((third.status, third.task_id), ("duplicate", first.task_id))

    def test_new_request_id_is_a_new_task(self):
        store, admission = self.open_admission()
        first = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id="req-1")
        second = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id="req-2")
        self.assertEqual((first.status, second.status), ("accepted", "accepted"))
        self.assertNotEqual(first.task_id, second.task_id)
        self.assertEqual(len(store.list()), 2)

    def test_principals_are_independent(self):
        store, admission = self.open_admission()
        first = admission.submit(PAYLOAD, principal="principal-a", request_id="req-1")
        second = admission.submit(PAYLOAD, principal="principal-b", request_id="req-1")
        self.assertEqual((first.status, second.status), ("accepted", "accepted"))
        self.assertNotEqual(first.task_id, second.task_id)

    def test_explicit_task_id_is_honoured(self):
        store, admission = self.open_admission()
        result = admission.submit({"task": "a"}, task_id="my-task", principal=PRINCIPAL,
                                  request_id="req-1")
        self.assertEqual((result.status, result.task_id), ("accepted", "my-task"))
        self.assertEqual(store.get("my-task").payload, {"task": "a"})

    def test_explicit_duplicate_task_id_raises_and_writes_nothing(self):
        path = self.path("queue.sqlite")
        store, admission = self.open_admission()
        self.assertEqual(admission.submit({"task": "a"}, task_id="my-task").status, "accepted")
        with self.assertRaises(ValueError):
            admission.submit({"task": "b"}, task_id="my-task")
        self.assertEqual(store.get("my-task").payload, {"task": "a"})
        self.assertIsNone(self.ledger_row(path, PRINCIPAL, "req-never"))  # nothing occupied

    def test_duplicate_replay_returns_the_original_task_id(self):
        store, admission = self.open_admission()
        first = admission.submit({"task": "a"}, task_id="task-first",
                                 principal=PRINCIPAL, request_id="req-1")
        replay = admission.submit({"task": "a"}, task_id="task-other",
                                  principal=PRINCIPAL, request_id="req-1")
        self.assertEqual((replay.status, replay.task_id), ("duplicate", first.task_id))
        self.assertEqual([t.task_id for t in store.list()], ["task-first"])

    def test_require_approval_admits_as_awaiting(self):
        store, admission = self.open_admission()
        result = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID,
                                  require_approval=True)
        self.assertEqual(result.status, "accepted")
        self.assertEqual(store.get(result.task_id).state, "awaiting_approval")
        # the replay answers duplicate even in the gated state
        replay = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID, require_approval=True)
        self.assertEqual((replay.status, replay.task_id), ("duplicate", result.task_id))

    def test_require_approval_rejects_non_bool(self):
        _, admission = self.open_admission()
        with self.assertRaises(TypeError):
            admission.submit(PAYLOAD, require_approval="yes")

    def test_priority_and_resource_are_stored(self):
        store, admission = self.open_admission()
        result = admission.submit({"task": "a"}, priority=9, resource="res-a",
                                  principal=PRINCIPAL, request_id=REQUEST_ID)
        task = store.get(result.task_id)
        self.assertEqual((task.priority, task.resource), (9, "res-a"))

    def test_payload_digest_is_stable(self):
        self.assertEqual(payload_digest({"b": 1, "a": 2}), payload_digest({"a": 2, "b": 1}))
        self.assertNotEqual(payload_digest({"a": 1}), payload_digest({"a": 2}))


class TestStrictPayloads(AdmissionTestCase):
    """Review D3: non-strict payloads are refused before anything is claimed."""

    def test_non_strict_payload_rejected_before_claim(self):
        path = self.path("queue.sqlite")
        store, admission = self.open_admission()
        with self.assertRaises(ValueError):
            admission.submit({"task": "x", "score": float("nan")},
                             principal=PRINCIPAL, request_id=REQUEST_ID)
        with self.assertRaises(TypeError):
            admission.submit({7: "int key"}, principal=PRINCIPAL, request_id=REQUEST_ID)
        with self.assertRaises(TypeError):
            admission.submit("not-a-dict", principal=PRINCIPAL, request_id=REQUEST_ID)
        # the request id was never occupied, and no task row exists
        self.assertIsNone(self.ledger_row(path))
        self.assertEqual(store.list(), [])
        result = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        self.assertEqual(result.status, "accepted")


class TestOneTransaction(AdmissionTestCase):
    """The task insert and the ledger row commit together or not at all."""

    def setUp(self):
        super().setUp()
        self.store = TaskStore(self.path("queue.sqlite"))
        self.addCleanup(self.store.close)
        self.admission = Admission(self.store)

    def _fail_inserts_matching(self, marker):
        """While ``state['active']``, matching INSERTs raise. Returns (fired, state)."""
        state = {"active": True}
        real_execute = self.store._db.execute
        fired = []

        def flaky_execute(sql, *args, **kwargs):
            if state["active"] and marker in sql:
                fired.append(sql)
                raise sqlite3.OperationalError("injected failure")
            return real_execute(sql, *args, **kwargs)

        self.store._db.execute = flaky_execute
        self.addCleanup(lambda: delattr(self.store._db, "execute"))
        return fired, state

    def test_ledger_failure_rolls_back_the_task_row(self):
        fired, state = self._fail_inserts_matching("INSERT INTO submissions")
        with self.assertRaises(sqlite3.OperationalError):
            self.admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        self.assertEqual(len(fired), 1)  # the ledger insert was attempted once
        # the task row rolled back with the failed ledger row: no orphan
        self.assertEqual(self.store.list(), [])
        # the request was never consumed: a clean retry succeeds
        state["active"] = False
        result = self.admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        self.assertEqual(result.status, "accepted")
        self.assertIsNotNone(self.store.get(result.task_id))

    def test_task_failure_rolls_back_and_keeps_the_request_free(self):
        fired, state = self._fail_inserts_matching("INSERT INTO tasks")
        with self.assertRaises(sqlite3.OperationalError):
            self.admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        self.assertEqual(len(fired), 1)
        self.assertEqual(self.store.list(), [])
        self.assertIsNone(self.ledger_row(self.path("queue.sqlite")))
        state["active"] = False
        result = self.admission.submit(PAYLOAD, principal=PRINCIPAL, request_id=REQUEST_ID)
        self.assertEqual(result.status, "accepted")


class TestSubmitRace8Processes(AdmissionTestCase):
    def test_eight_processes_submit_one_request(self):
        path = self.path("race.sqlite")
        store = TaskStore(path)  # pre-create the schema
        store.close()
        ctx = multiprocessing.get_context("spawn")
        args = [(path, PRINCIPAL, "req-race", PAYLOAD) for _ in range(8)]
        with ctx.Pool(8) as pool:
            results = pool.starmap(_submit_worker, args)
        statuses = [status for status, _ in results]
        self.assertEqual(statuses.count("error"), 0, msg=repr(results))
        self.assertEqual(statuses.count("accepted"), 1, msg=repr(results))
        self.assertEqual(statuses.count("duplicate"), 7, msg=repr(results))
        winner = next(task_id for status, task_id in results if status == "accepted")
        for status, task_id in results:
            self.assertTrue(task_id is None or task_id == winner, msg=repr(results))
        # exactly one durable task row, and the winner owns it
        reopened = TaskStore(path)
        self.addCleanup(reopened.close)
        tasks = reopened.list()
        self.assertEqual(len(tasks), 1)
        self.assertEqual(tasks[0].task_id, winner)
        self.assertEqual(tasks[0].state, "queued")
        # after the dust settles, every replay sees the same duplicate
        admission = Admission(reopened)
        for _ in range(3):
            result = admission.submit(PAYLOAD, principal=PRINCIPAL, request_id="req-race")
            self.assertEqual((result.status, result.task_id), ("duplicate", winner))


if __name__ == "__main__":
    unittest.main()
