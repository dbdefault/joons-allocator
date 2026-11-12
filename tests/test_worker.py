"""Tests for joons_allocator.worker: single-process flows, injected-failure
rollbacks, a 6-process spawn drain of 30 tasks over 3 shared resources, and
a crash-then-recover flow. Standard library only.

Ported from the prototype ( the guard-backed lock and
circuit assertions were replaced by the equivalent workers-table /
resource-exclusivity assertions, because exclusivity and the worker slot now
live inside the lease transaction)."""
from f2_test_helpers import lease_token
import json
import multiprocessing
import os
import sqlite3
import tempfile
import time
import unittest

from joons_allocator.control import Control
from joons_allocator.store import TaskStore
from joons_allocator.worker import RECOVERED_RESULT, Dispatcher

RESOURCES = ["res-a", "res-b", "res-c"]


def _drain_worker(db_path, worker_id):
    """Spawn-pool target: drain the queue until lease() returns None."""
    store = TaskStore(db_path)
    records = []
    try:
        dispatcher = Dispatcher(store)
        while True:
            task = dispatcher.lease(worker_id)
            if task is None:
                break
            start = time.time()
            time.sleep(0.01)
            end = time.time()
            done = dispatcher.complete(worker_id, getattr(task, 'task_id', task), "done", lease_token=lease_token(dispatcher, task))
            records.append((task.task_id, task.resource, start, end, done))
    except Exception as exc:  # report instead of crashing the pool
        records.append(("error", repr(exc)))
    finally:
        store.close()
    return records


def _crash_worker(db_path, marker_path):
    """Spawn target: lease a task, report it, then die hard holding everything."""
    store = TaskStore(db_path)
    dispatcher = Dispatcher(store)
    task = dispatcher.lease("worker-crash")
    info = {"task_id": task.task_id if task else None}
    with open(marker_path, "w", encoding="utf-8") as handle:
        json.dump(info, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os._exit(1)  # die hard: no cleanup, the running task and slot stay behind


class WorkerTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name

    def db_path(self, name="queue.sqlite"):
        return os.path.join(self._tmp, name)

    def make_dispatcher(self, path=None, hooks=None):
        path = self.db_path() if path is None else path
        store = TaskStore(path)
        self.addCleanup(store.close)
        return store, Dispatcher(store, hooks=hooks)

    def slot(self, dispatcher, worker_id):
        for info in dispatcher.workers():
            if info.worker_id == worker_id:
                return info
        return None


class TestLease(WorkerTestCase):
    def test_lease_empty_queue_returns_none(self):
        _, dispatcher = self.make_dispatcher()
        self.assertIsNone(dispatcher.lease("worker-1"))

    def test_lease_claims_oldest_and_marks_running(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        store.insert("task-2", {"task": "b"})
        task = dispatcher.lease("worker-1")
        self.assertEqual(task.task_id, "task-1")
        self.assertEqual((task.state, task.version), ("running", 2))
        self.assertEqual((task.worker_id, task.lease_token), ("worker-1", task.lease_token))
        self.assertTrue(task.lease_token)
        self.assertEqual(self.slot(dispatcher, "worker-1").current_task, "task-1")
        self.assertEqual(dispatcher.lease("worker-2").task_id, "task-2")

    def test_lease_skips_task_whose_resource_is_running(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"}, resource="res-a")
        store.insert("task-2", {"task": "b"})
        first = dispatcher.lease("worker-1")
        self.assertEqual(first.task_id, "task-1")
        second = dispatcher.lease("worker-2")
        self.assertEqual(second.task_id, "task-2")  # task-1 skipped: res-a held

    def test_lease_returns_none_when_all_queued_resources_held(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"}, resource="res-a")
        self.assertEqual(dispatcher.lease("worker-1").task_id, "task-1")
        self.assertIsNone(dispatcher.lease("worker-2"))
        self.assertEqual(store.get("task-1").state, "running")

    def test_lease_twice_same_worker_returns_none_without_churn(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        self.assertEqual(dispatcher.lease("worker-1").task_id, "task-1")
        store.insert("task-2", {"task": "b"})
        self.assertIsNone(dispatcher.lease("worker-1"))  # slot busy with task-1
        task = store.get("task-2")
        self.assertEqual((task.state, task.version), ("queued", 1))  # untouched
        self.assertIsNone(task.result)
        self.assertEqual(self.slot(dispatcher, "worker-1").current_task, "task-1")

    def test_paused_worker_gets_no_lease(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        self.assertFalse(dispatcher.heartbeat("worker-1"))  # unknown: no auto-register
        self.assertFalse(dispatcher.pause_worker("worker-1"))  # unknown: nothing to pause
        first = dispatcher.lease("worker-1")  # auto-registers
        self.assertEqual(first.task_id, "task-1")
        self.assertTrue(dispatcher.complete("worker-1", "task-1", "ok", lease_token=lease_token(dispatcher, "task-1")))
        self.assertTrue(dispatcher.pause_worker("worker-1"))
        store.insert("task-2", {"task": "b"})
        self.assertIsNone(dispatcher.lease("worker-1"))  # paused
        self.assertEqual(store.get("task-2").state, "queued")
        self.assertTrue(dispatcher.resume_worker("worker-1"))
        self.assertEqual(dispatcher.lease("worker-1").task_id, "task-2")
        self.assertFalse(dispatcher.resume_worker("worker-ghost"))

    def test_heartbeat_unknown_worker_is_false(self):
        _, dispatcher = self.make_dispatcher()
        self.assertFalse(dispatcher.heartbeat("worker-1"))
        self.assertEqual(dispatcher.workers(), [])

    def test_invalid_resource_is_rejected_at_insert(self):
        store, dispatcher = self.make_dispatcher()
        with self.assertRaises(ValueError):
            store.insert("task-1", {"task": "a"}, resource=7)
        with self.assertRaises(ValueError):
            store.insert("task-1", {"task": "a"}, resource="")
        self.assertEqual(store.list(), [])
        self.assertIsNone(dispatcher.lease("worker-1"))


class TestSettle(WorkerTestCase):
    def test_complete_succeeds_and_clears_the_slot(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"}, resource="res-a")
        task = dispatcher.lease("worker-1")
        self.assertTrue(dispatcher.complete("worker-1", getattr(task, 'task_id', task), "42", lease_token=lease_token(dispatcher, task)))
        done = store.get("task-1")
        self.assertEqual((done.state, done.result, done.version), ("succeeded", "42", 3))
        self.assertIsNone(done.worker_id)
        self.assertIsNone(done.lease_token)
        slot = self.slot(dispatcher, "worker-1")
        self.assertIsNone(slot.current_task)
        self.assertEqual(slot.consecutive_failures, 0)
        self.assertFalse(dispatcher.complete("worker-1", "task-1", "again", lease_token=lease_token(dispatcher, "task-1")))

    def test_complete_without_lease_returns_false(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        self.assertFalse(dispatcher.complete("worker-1", "task-1", "x", lease_token=lease_token(dispatcher, "task-1")))
        self.assertFalse(dispatcher.complete("worker-1", "missing", "x", lease_token=lease_token(dispatcher, "missing")))
        self.assertEqual(store.get("task-1").state, "queued")
        self.assertIsNone(self.slot(dispatcher, "worker-1"))

    def test_complete_and_fail_require_str(self):
        _, dispatcher = self.make_dispatcher()
        with self.assertRaises(TypeError):
            dispatcher.complete("worker-1", "task-1", None, lease_token=lease_token(dispatcher, "task-1"))
        with self.assertRaises(TypeError):
            dispatcher.fail("worker-1", "task-1", None, lease_token=lease_token(dispatcher, "task-1"))

    def test_complete_result_is_capped(self):
        from joons_allocator.store import MAX_RESULT_CHARS
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        task = dispatcher.lease("worker-1")
        with self.assertRaises(ValueError):
            dispatcher.complete("worker-1", getattr(task, 'task_id', task), "x" * (MAX_RESULT_CHARS + 1), lease_token=lease_token(dispatcher, task))
        self.assertEqual(store.get("task-1").state, "running")  # validated before mutation

    def test_fail_reason_is_capped(self):
        from joons_allocator.store import MAX_REASON_CHARS
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        task = dispatcher.lease("worker-1")
        with self.assertRaises(ValueError):
            dispatcher.fail("worker-1", getattr(task, 'task_id', task), "x" * (MAX_REASON_CHARS + 1), lease_token=lease_token(dispatcher, task))
        self.assertEqual(store.get("task-1").state, "running")

    def test_fail_marks_failed_and_bumps_consecutive_failures(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"}, resource="res-a")
        task = dispatcher.lease("worker-1")
        self.assertTrue(dispatcher.fail("worker-1", getattr(task, 'task_id', task), "boom", lease_token=lease_token(dispatcher, task)))
        failed = store.get("task-1")
        self.assertEqual((failed.state, failed.result, failed.version), ("failed", "boom", 3))
        self.assertIsNone(failed.worker_id)
        slot = self.slot(dispatcher, "worker-1")
        self.assertIsNone(slot.current_task)
        self.assertEqual(slot.consecutive_failures, 1)
        # a later success resets the counter
        store.insert("task-2", {"task": "b"})
        retry_task = dispatcher.lease("worker-1")
        self.assertEqual(retry_task.task_id, "task-2")
        self.assertTrue(dispatcher.complete("worker-1", getattr(retry_task, 'task_id', retry_task), "ok", lease_token=lease_token(dispatcher, retry_task)))
        self.assertEqual(self.slot(dispatcher, "worker-1").consecutive_failures, 0)

    def test_fail_without_lease_returns_false(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        self.assertFalse(dispatcher.fail("worker-1", "task-1", "boom", lease_token=lease_token(dispatcher, "task-1")))
        self.assertEqual(store.get("task-1").state, "queued")


class TestSettleLeaseOwnership(WorkerTestCase):
    """review2 W3: only the leasing worker settles, and nothing leaks."""

    def setUp(self):
        super().setUp()
        self.store, self.dispatcher = self.make_dispatcher()
        self.store.insert("task-1", {"task": "a"}, resource="res-x")
        self.assertEqual(self.dispatcher.lease("worker-1").task_id, "task-1")

    def test_w3_mismatched_worker_cannot_settle_and_nothing_leaks(self):
        self.assertFalse(self.dispatcher.complete("worker-2", "task-1", "from-wrong-worker", lease_token=lease_token(self.dispatcher, "task-1")))
        self.assertFalse(self.dispatcher.fail("worker-2", "task-1", "from-wrong-worker", lease_token=lease_token(self.dispatcher, "task-1")))
        task = self.store.get("task-1")
        self.assertEqual(task.state, "running")
        self.assertEqual(self.slot(self.dispatcher, "worker-1").current_task, "task-1")
        # the real worker's own settle works and clears the slot
        self.assertTrue(self.dispatcher.complete("worker-1", "task-1", "real result", lease_token=lease_token(self.dispatcher, "task-1")))
        self.assertEqual(self.store.get("task-1").state, "succeeded")
        self.assertIsNone(self.slot(self.dispatcher, "worker-1").current_task)

    def test_w3_lease_identity_is_visible_and_cleared(self):
        task = self.store.get("task-1")
        self.assertEqual(task.worker_id, "worker-1")
        self.assertTrue(task.lease_token)
        self.assertTrue(self.dispatcher.complete("worker-1", "task-1", "ok", lease_token=lease_token(self.dispatcher, "task-1")))
        done = self.store.get("task-1")
        self.assertIsNone(done.worker_id)
        self.assertIsNone(done.lease_token)


class TestAtomicStateChanges(WorkerTestCase):
    """review2 W4, reworked: with the one-transaction lease/settle there is
    nothing to compensate — an injected failure inside the transaction must
    roll back EVERYTHING it already did."""

    def setUp(self):
        super().setUp()
        self.store, self.dispatcher = self.make_dispatcher()

    def _fail_statements_matching(self, marker):
        """While ``state['active']``, matching statements raise. Returns the toggle."""
        state = {"active": True}
        real_execute = self.store._db.execute

        def flaky_execute(sql, *args, **kwargs):
            if state["active"] and marker in sql:
                raise sqlite3.OperationalError("injected failure")
            return real_execute(sql, *args, **kwargs)

        self.store._db.execute = flaky_execute
        self.addCleanup(lambda: delattr(self.store._db, "execute"))
        return state

    def test_injected_failure_in_the_claim_rolls_back_cleanly(self):
        self.store.insert("task-1", {"task": "a"})
        state = self._fail_statements_matching("WHERE task_id=? AND state='queued'")
        with self.assertRaises(sqlite3.OperationalError):
            self.dispatcher.lease("worker-1")
        task = self.store.get("task-1")
        self.assertEqual((task.state, task.version), ("queued", 1))
        self.assertIsNone(task.worker_id)
        self.assertIsNone(task.lease_token)
        self.assertIsNone(self.slot(self.dispatcher, "worker-1"))
        self.assertFalse(self.store._db.in_transaction)
        # and the queue is fully usable again
        state["active"] = False
        self.assertEqual(self.dispatcher.lease("worker-1").task_id, "task-1")

    def test_injected_failure_in_the_settle_rolls_back_cleanly(self):
        self.store.insert("task-1", {"task": "a"})
        task = self.dispatcher.lease("worker-1")
        state = self._fail_statements_matching("consecutive_failures=CASE WHEN")
        with self.assertRaises(sqlite3.OperationalError):
            self.dispatcher.complete("worker-1", getattr(task, 'task_id', task), "ok", lease_token=lease_token(self.dispatcher, task))
        self.assertEqual(self.store.get("task-1").state, "running")  # nothing moved
        self.assertEqual(self.slot(self.dispatcher, "worker-1").current_task, "task-1")
        self.assertFalse(self.store._db.in_transaction)
        # the settle can be retried cleanly
        state["active"] = False
        self.assertTrue(self.dispatcher.complete("worker-1", getattr(task, 'task_id', task), "ok", lease_token=lease_token(self.dispatcher, task)))
        self.assertEqual(self.store.get("task-1").state, "succeeded")


class TestOperator(WorkerTestCase):
    def test_retry_only_from_failed(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        self.assertFalse(dispatcher.retry("task-1"))  # queued, not failed
        task = dispatcher.lease("worker-1")
        self.assertTrue(dispatcher.fail("worker-1", getattr(task, 'task_id', task), "boom", lease_token=lease_token(dispatcher, task)))
        self.assertTrue(dispatcher.retry("task-1"))
        task = store.get("task-1")
        self.assertEqual((task.state, task.version, task.result), ("queued", 4, None))
        self.assertEqual(dispatcher.lease("worker-2").task_id, "task-1")

    def test_cancel_queued_only(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        store.insert("task-2", {"task": "b"})
        self.assertTrue(dispatcher.cancel("task-1"))
        self.assertEqual(store.get("task-1").state, "cancelled")
        self.assertFalse(dispatcher.cancel("task-1"))  # already cancelled
        running = dispatcher.lease("worker-1")
        self.assertFalse(dispatcher.cancel(running.task_id))  # running: not cancellable
        self.assertTrue(dispatcher.complete("worker-1", getattr(running, 'task_id', running), "ok", lease_token=lease_token(dispatcher, running)))
        self.assertFalse(dispatcher.cancel(running.task_id))  # succeeded
        self.assertFalse(dispatcher.cancel("missing"))

    def test_recover_requires_running_task(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        self.assertFalse(dispatcher.recover("task-1", "worker-1", lease_token=lease_token(dispatcher, "task-1")))  # queued
        self.assertFalse(dispatcher.recover("missing", "worker-1", lease_token=lease_token(dispatcher, "missing")))
        task = dispatcher.lease("worker-1")
        self.assertTrue(dispatcher.complete("worker-1", getattr(task, 'task_id', task), "ok", lease_token=lease_token(dispatcher, task)))
        self.assertFalse(dispatcher.recover("task-1", "worker-1", lease_token=lease_token(dispatcher, "task-1")))  # succeeded

    def test_recover_with_wrong_worker_is_refused_wholesale(self):
        store, dispatcher = self.make_dispatcher()
        store.insert("task-1", {"task": "a"})
        store.insert("task-2", {"task": "b"})  # innocent bystander
        self.assertEqual(dispatcher.lease("worker-1").task_id, "task-1")
        self.assertFalse(dispatcher.recover("task-1", "worker-typo", lease_token=lease_token(dispatcher, "task-1")))
        # nothing happened: the task is still running and the slot still taken
        self.assertEqual(store.get("task-1").state, "running")
        self.assertEqual(self.slot(dispatcher, "worker-1").current_task, "task-1")
        self.assertEqual(store.get("task-2").version, 1)  # bystander untouched
        # the genuine recovery still works end-to-end
        self.assertTrue(dispatcher.recover("task-1", "worker-1", lease_token=lease_token(dispatcher, "task-1")))
        self.assertEqual(store.get("task-1").state, "failed")
        self.assertIsNone(self.slot(dispatcher, "worker-1").current_task)
        self.assertTrue(dispatcher.retry("task-1"))
        self.assertEqual(dispatcher.lease("worker-1").task_id, "task-1")


class TestCrashRecovery(WorkerTestCase):
    def test_crash_holding_task_then_recover(self):
        path = self.db_path()
        store, dispatcher = self.make_dispatcher(path)
        store.insert("task-1", {"task": "a", "resource": "res-x"}, resource="res-x")
        store.insert("task-2", {"task": "b", "resource": "res-x"}, resource="res-x")
        marker = os.path.join(self._tmp, "crash.json")
        ctx = multiprocessing.get_context("spawn")
        proc = ctx.Process(target=_crash_worker, args=(path, marker))
        proc.start()
        proc.join(timeout=60)
        self.assertEqual(proc.exitcode, 1)
        with open(marker, encoding="utf-8") as handle:
            info = json.load(handle)
        self.assertTrue(info["task_id"])
        task_id, worker_id = info["task_id"], "worker-crash"
        # no expiry: the task stays running, the slot stays taken
        self.assertEqual(store.get(task_id).state, "running")
        self.assertEqual(self.slot(dispatcher, worker_id).current_task, task_id)
        # the still-held resource blocks leasing the queued task-2 on it
        self.assertIsNone(dispatcher.lease("worker-2"))
        # explicit recovery: slot cleared, task failed
        self.assertTrue(dispatcher.recover(task_id, worker_id, lease_token=lease_token(dispatcher, task_id)))
        failed = store.get(task_id)
        self.assertEqual(failed.state, "failed")
        self.assertEqual(failed.result, RECOVERED_RESULT)
        self.assertIsNone(self.slot(dispatcher, worker_id).current_task)
        # the resource is free again: task-2 is leasable now
        leased = dispatcher.lease("worker-2")
        self.assertEqual(leased.task_id, "task-2")
        self.assertTrue(dispatcher.complete("worker-2", "task-2", "done", lease_token=lease_token(dispatcher, "task-2")))
        # requeue and finish the recovered task on another worker
        self.assertTrue(dispatcher.retry(task_id))
        leased = dispatcher.lease("worker-2")
        self.assertEqual(leased.task_id, task_id)
        self.assertTrue(dispatcher.complete("worker-2", task_id, "done after recovery", lease_token=lease_token(dispatcher, task_id)))
        self.assertEqual(store.get(task_id).state, "succeeded")
        self.assertFalse(dispatcher.recover(task_id, worker_id, lease_token=lease_token(dispatcher, task_id)))


class TestDrain(WorkerTestCase):
    def test_six_processes_drain_30_tasks_over_3_resources(self):
        path = self.db_path()
        store, _ = self.make_dispatcher(path)
        expected = set()
        for i in range(30):
            task_id = "task-%02d" % i
            store.insert(task_id, {"task": "work"}, resource=RESOURCES[i % 3])
            expected.add(task_id)
        store.close()  # children open their own connections
        ctx = multiprocessing.get_context("spawn")
        args = [(path, "worker-%d" % w) for w in range(6)]
        with ctx.Pool(6) as pool:
            batches = pool.starmap(_drain_worker, args)
        records, errors = [], []
        for batch in batches:
            for record in batch:
                (errors if record[0] == "error" else records).append(record)
        self.assertEqual(errors, [], msg=repr(errors))
        self.assertEqual(len(records), 30, msg=repr(records))
        self.assertEqual({record[0] for record in records}, expected)  # each exactly once
        self.assertTrue(all(record[4] for record in records), msg=repr(records))
        # no two tasks on one resource overlapped (resource exclusivity)
        for resource in RESOURCES:
            spans = sorted((record[2], record[3]) for record in records
                           if record[1] == resource)
            self.assertEqual(len(spans), 10, msg=resource)
            for (_, prev_end), (start, _) in zip(spans, spans[1:]):
                self.assertGreaterEqual(start, prev_end, msg=resource)
        # and no worker ever held two tasks at once (one task per worker)
        for worker_index in range(6):
            spans = sorted(
                (record[2], record[3]) for record in batches[worker_index])
            for (_, prev_end), (start, _) in zip(spans, spans[1:]):
                self.assertGreaterEqual(start, prev_end, msg="worker-%d" % worker_index)
        # the store agrees: everything succeeded, nothing left behind
        reopened = TaskStore(path)
        self.addCleanup(reopened.close)
        states = [task.state for task in reopened.list()]
        self.assertEqual(states.count("succeeded"), 30)
        self.assertEqual(len(states), 30)


if __name__ == "__main__":
    unittest.main()
