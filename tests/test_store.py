"""Tests for joons_allocator.store: temp-file databases, thread races, and
8-process spawn races over one SQLite file. Standard library only.

Ported from the prototype ( lease_worker ->
worker_id, plus the new priority/resource columns and the result/reason
caps)."""
import multiprocessing
import os
import sqlite3
import tempfile
import threading
import time
import unittest
import uuid

from joons_allocator.store import (INSERT_SQL, MAX_REASON_CHARS, MAX_RESULT_CHARS,
                                   STATES, TRANSITIONS, Task, TaskStore,
                                   canonical_json, check_reason, check_result)


def _cas_worker(db_path, task_id):
    """Spawn-pool target: one process trying the same queued -> running claim."""
    store = TaskStore(db_path)
    try:
        return store._begin_lease(task_id, "worker-race", uuid.uuid4().hex)
    finally:
        store.close()


def _insert_worker(db_path, task_id):
    """Spawn-pool target: one process inserting the same task_id."""
    store = TaskStore(db_path)
    try:
        return store.insert(task_id, {"n": 1})
    finally:
        store.close()


class StoreTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name

    def db_path(self, name="tasks.sqlite"):
        return os.path.join(self._tmp, name)

    def open_store(self, path=None):
        store = TaskStore(self.db_path() if path is None else path)
        self.addCleanup(store.close)
        return store


class TestBasics(StoreTestCase):
    def test_insert_and_get(self):
        store = self.open_store()
        payload = {"task": "sum", "args": [1, 2]}
        self.assertTrue(store.insert("task-1", payload))
        task = store.get("task-1")
        self.assertIsInstance(task, Task)
        self.assertEqual(task.task_id, "task-1")
        self.assertEqual(task.state, "queued")
        self.assertEqual(task.payload, payload)
        self.assertIsNone(task.result)
        self.assertEqual(task.priority, 0)  # default priority
        self.assertIsNone(task.resource)
        self.assertIsNone(task.worker_id)  # lease identity all-or-nothing
        self.assertIsNone(task.lease_token)
        self.assertEqual(task.version, 1)
        self.assertGreater(task.created, 0)
        self.assertEqual(task.created, task.updated)

    def test_insert_with_priority_and_resource(self):
        store = self.open_store()
        self.assertTrue(store.insert("task-1", {"n": 1}, priority=7, resource="res-a"))
        task = store.get("task-1")
        self.assertEqual((task.priority, task.resource), (7, "res-a"))

    def test_duplicate_insert_is_rejected(self):
        store = self.open_store()
        self.assertTrue(store.insert("task-1", {"a": 1}))
        self.assertFalse(store.insert("task-1", {"a": 2}))
        task = store.get("task-1")
        self.assertEqual(task.payload, {"a": 1})
        self.assertEqual(task.version, 1)

    def test_get_missing_returns_none(self):
        store = self.open_store()
        self.assertIsNone(store.get("nope"))

    def test_payload_is_stored_canonically(self):
        path = self.db_path()
        store = self.open_store(path)
        store.insert("task-1", {"b": 2, "a": 1})
        conn = sqlite3.connect(path)
        try:
            (raw,) = conn.execute("SELECT payload FROM tasks WHERE task_id='task-1'").fetchone()
        finally:
            conn.close()
        self.assertEqual(raw, '{"a":1,"b":2}')

    def test_list_filters_by_state(self):
        store = self.open_store()
        store.insert("task-1", {"n": 1})
        store.insert("task-2", {"n": 2})
        store.insert("task-3", {"n": 3})
        self.assertTrue(store._begin_lease("task-2", "worker-1", "token-1"))
        self.assertTrue(store._compare_and_set("task-3", "queued", "cancelled"))
        self.assertEqual([t.task_id for t in store.list()], ["task-1", "task-2", "task-3"])
        self.assertEqual([t.task_id for t in store.list(state="queued")], ["task-1"])
        self.assertEqual([t.task_id for t in store.list(state="running")], ["task-2"])
        self.assertEqual(store.list(state="succeeded"), [])
        with self.assertRaises(ValueError):
            store.list(state="bogus")

    def test_invalid_ids_are_rejected(self):
        store = self.open_store()
        with self.assertRaises(ValueError):
            store.insert("", {})
        with self.assertRaises(ValueError):
            store.get("")
        with self.assertRaises(ValueError):
            store.insert("task-1", {}, resource="")  # empty resource name
        with self.assertRaises(TypeError):
            store.insert("task-1", {}, priority="high")  # not an int
        with self.assertRaises(TypeError):
            store.insert("task-1", {}, priority=True)  # bool is not an int here


class TestCompareAndSet(StoreTestCase):
    def test_happy_path_and_result(self):
        store = self.open_store()
        store.insert("task-1", {})
        self.assertTrue(store._begin_lease("task-1", "worker-1", "token-1"))
        task = store.get("task-1")
        self.assertEqual((task.state, task.version), ("running", 2))
        self.assertEqual((task.worker_id, task.lease_token), ("worker-1", "token-1"))
        self.assertGreaterEqual(task.updated, task.created)
        self.assertTrue(store._compare_and_set("task-1", "running", "succeeded", result="42"))
        task = store.get("task-1")
        self.assertEqual((task.state, task.version, task.result), ("succeeded", 3, "42"))
        self.assertIsNone(task.worker_id)  # identity cleared with the settle
        self.assertIsNone(task.lease_token)

    def test_expected_version(self):
        store = self.open_store()
        store.insert("task-1", {})
        self.assertTrue(store._compare_and_set("task-1", "queued", "paused", expected_version=1))
        self.assertFalse(store._compare_and_set("task-1", "paused", "queued", expected_version=1))
        self.assertTrue(store._compare_and_set("task-1", "paused", "queued", expected_version=2))
        self.assertEqual(store.get("task-1").version, 3)

    def test_result_none_does_not_clobber(self):
        store = self.open_store()
        store.insert("task-1", {})
        self.assertTrue(store._compare_and_set("task-1", "queued", "paused", result="attempt 1"))
        self.assertTrue(store._compare_and_set("task-1", "paused", "queued"))
        self.assertEqual(store.get("task-1").result, "attempt 1")

    def test_missing_task_is_false(self):
        store = self.open_store()
        self.assertFalse(store._compare_and_set("nope", "queued", "cancelled"))

    def test_stale_state_is_false(self):
        store = self.open_store()
        store.insert("task-1", {})
        self.assertFalse(store._compare_and_set("task-1", "running", "succeeded"))
        self.assertEqual(store.get("task-1").state, "queued")

    def test_illegal_transitions_raise(self):
        store = self.open_store()
        store.insert("task-1", {})
        for expected, new in [("queued", "succeeded"), ("queued", "queued"),
                              ("queued", "running"), ("running", "cancelled"),
                              ("failed", "queued"), ("succeeded", "running"),
                              ("cancelled", "queued")]:
            with self.assertRaises(ValueError):
                store._compare_and_set("task-1", expected, new)

    def test_unknown_states_raise(self):
        store = self.open_store()
        store.insert("task-1", {})
        with self.assertRaises(ValueError):
            store._compare_and_set("task-1", "queued", "weird")
        with self.assertRaises(ValueError):
            store._compare_and_set("task-1", "weird", "running")
        with self.assertRaises(TypeError):
            store._compare_and_set("task-1", "queued", "paused", result=42)


class TestRetry(StoreTestCase):
    def test_retry_failed_to_queued(self):
        store = self.open_store()
        store.insert("task-1", {})
        self.assertTrue(store._begin_lease("task-1", "worker-1", "token-1"))
        self.assertTrue(store._compare_and_set("task-1", "running", "failed", result="boom"))
        self.assertTrue(store.retry("task-1"))
        task = store.get("task-1")
        self.assertEqual((task.state, task.version, task.result), ("queued", 4, None))
        self.assertFalse(store.retry("task-1"))  # queued again, not failed
        self.assertTrue(store._begin_lease("task-1", "worker-2", "token-2"))

    def test_retry_only_from_failed(self):
        store = self.open_store()
        store.insert("task-1", {})
        self.assertFalse(store.retry("task-1"))  # queued
        self.assertTrue(store._compare_and_set("task-1", "queued", "cancelled"))
        self.assertFalse(store.retry("task-1"))
        self.assertEqual(store.get("task-1").state, "cancelled")


class TestCanonicalJson(StoreTestCase):
    """Review D3: canonical_json must produce strict JSON or a clear error."""

    def test_rejects_nan_and_infinity(self):
        for bad in ({"x": float("nan")}, {"x": float("inf")}, {"x": float("-inf")}):
            with self.assertRaises(ValueError):
                canonical_json(bad)
        with self.assertRaises(ValueError):
            canonical_json([1.0, float("nan")])

    def test_rejects_non_str_keys_with_clear_error(self):
        with self.assertRaises(TypeError) as caught:
            canonical_json({1: "a"})
        self.assertIn("keys must be str", str(caught.exception))
        with self.assertRaises(TypeError):
            canonical_json({"outer": {2: "b"}})
        with self.assertRaises(TypeError):
            canonical_json({"outer": [{3: "c"}]})

    def test_valid_payloads_unchanged(self):
        self.assertEqual(canonical_json({"b": 2, "a": [1, {"c": 3}]}), '{"a":[1,{"c":3}],"b":2}')
        self.assertEqual(canonical_json("plain"), '"plain"')

    def test_insert_rejects_non_strict_payloads(self):
        store = self.open_store()
        with self.assertRaises(ValueError):
            store.insert("task-1", {"x": float("nan")})
        self.assertIsNone(store.get("task-1"))


class TestCaps(StoreTestCase):
    """Results are capped at 1 MiB JSON-encodable text, reasons at 4096
    chars; validation happens BEFORE any mutation and never truncates."""

    def test_result_cap(self):
        self.assertIsNone(check_result(None))
        check_result("x" * MAX_RESULT_CHARS)  # exactly at the cap is fine
        with self.assertRaises(ValueError):
            check_result("x" * (MAX_RESULT_CHARS + 1))
        with self.assertRaises(TypeError):
            check_result(42)

    def test_result_must_be_json_encodable(self):
        with self.assertRaises(ValueError):
            check_result("\ud800")  # a lone surrogate is not JSON-encodable

    def test_reason_cap(self):
        check_reason("x" * MAX_REASON_CHARS)  # exactly at the cap is fine
        with self.assertRaises(ValueError):
            check_reason("x" * (MAX_REASON_CHARS + 1))
        with self.assertRaises(TypeError):
            check_reason(None)

    def test_cas_rejects_oversize_result_before_moving(self):
        store = self.open_store()
        store.insert("task-1", {})
        with self.assertRaises(ValueError):
            store._compare_and_set("task-1", "queued", "paused",
                                  result="x" * (MAX_RESULT_CHARS + 1))
        self.assertEqual(store.get("task-1").state, "queued")  # untouched


class TestStateMachineTable(StoreTestCase):
    def test_table_shape(self):
        self.assertEqual(STATES, ("queued", "running", "succeeded", "failed", "cancelled",
                                  "awaiting_approval", "paused"))
        self.assertEqual(TRANSITIONS["queued"], frozenset({"cancelled", "paused"}))
        self.assertEqual(TRANSITIONS["running"], frozenset({"succeeded", "failed"}))
        self.assertEqual(TRANSITIONS["succeeded"], frozenset())
        self.assertEqual(TRANSITIONS["failed"], frozenset())
        self.assertEqual(TRANSITIONS["cancelled"], frozenset())
        self.assertEqual(TRANSITIONS["awaiting_approval"], frozenset({"queued", "cancelled"}))
        self.assertEqual(TRANSITIONS["paused"], frozenset({"queued", "cancelled"}))

    def test_operator_states_via_cas(self):
        store = self.open_store()
        store.insert("task-1", {})
        self.assertTrue(store._compare_and_set("task-1", "queued", "paused"))
        self.assertTrue(store._compare_and_set("task-1", "paused", "queued"))
        self.assertTrue(store._compare_and_set("task-1", "queued", "paused"))
        self.assertTrue(store._compare_and_set("task-1", "paused", "cancelled"))
        self.assertFalse(store._compare_and_set("task-1", "paused", "queued"))  # cancelled now
        # awaiting_approval is entered only at admission, never from queued
        with self.assertRaises(ValueError):
            store._compare_and_set("task-1", "queued", "awaiting_approval")
        # white-box: Admission creates awaiting_approval rows with this shape
        store._db.execute(
            INSERT_SQL, ("task-2", "awaiting_approval", "{}", 0, None,
                         time.time(), time.time()))
        self.assertTrue(store._compare_and_set("task-2", "awaiting_approval", "queued"))


class TestDurability(StoreTestCase):
    def test_reopen_same_file(self):
        path = self.db_path()
        store = self.open_store(path)
        store.insert("task-1", {"n": 1})
        self.assertTrue(store._begin_lease("task-1", "worker-1", "token-1"))
        store.close()
        reopened = self.open_store(path)
        task = reopened.get("task-1")
        self.assertEqual((task.state, task.version), ("running", 2))
        self.assertTrue(reopened._compare_and_set("task-1", "running", "succeeded", result="ok"))
        self.assertEqual(reopened.get("task-1").result, "ok")


class TestThreadRace(StoreTestCase):
    def test_single_cas_winner_shared_instance(self):
        store = self.open_store()
        store.insert("task-1", {"n": 1})
        workers = 16
        barrier = threading.Barrier(workers)
        results, lock = [], threading.Lock()

        def run(index):
            barrier.wait()
            worker = "worker-%d" % index
            won = store._begin_lease("task-1", worker, "token-%d" % index)
            with lock:
                results.append((won, worker))

        threads = [threading.Thread(target=run, args=(index,)) for index in range(workers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual([won for won, _ in results].count(True), 1)
        winner = next(worker for won, worker in results if won)
        task = store.get("task-1")
        self.assertEqual((task.state, task.version), ("running", 2))
        self.assertEqual(task.worker_id, winner)


class TestProcessRaces(StoreTestCase):
    def test_cas_race_8_processes(self):
        path = self.db_path()
        store = self.open_store(path)
        store.insert("task-1", {"n": 1})
        store.close()
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(8) as pool:
            results = pool.starmap(_cas_worker, [(path, "task-1")] * 8)
        self.assertEqual(results.count(True), 1, msg=repr(results))
        reopened = self.open_store(path)
        task = reopened.get("task-1")
        self.assertEqual((task.state, task.version), ("running", 2))
        self.assertEqual(task.worker_id, "worker-race")

    def test_insert_race_8_processes(self):
        path = self.db_path()
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(8) as pool:
            results = pool.starmap(_insert_worker, [(path, "task-1")] * 8)
        self.assertEqual(results.count(True), 1, msg=repr(results))
        store = self.open_store(path)
        self.assertEqual(len(store.list()), 1)
        self.assertEqual(store.get("task-1").version, 1)


if __name__ == "__main__":
    unittest.main()
