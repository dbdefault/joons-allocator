"""Tests for the allocation policy: priority DESC then created ASC (FIFO),
resource exclusivity, and the one-running-task-per-worker slot — including
spawn-process races. Standard library only."""
from f2_test_helpers import lease_token
import multiprocessing
import os
import tempfile
import unittest
from unittest import mock

import joons_allocator.store as store_module
from joons_allocator.store import TaskStore
from joons_allocator.worker import Dispatcher


class _Clock:
    """Fake clock for joons_allocator.store: one predictable timestamp per call."""

    def __init__(self, start=1000.0):
        self._now = start

    def time(self):
        self._now += 1.0
        return self._now


class _FrozenClock:
    """A clock that never moves: every insert gets the same timestamp."""

    def __init__(self, value=2000.0):
        self.value = value

    def time(self):
        return self.value


class PolicyTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name

    def db_path(self, name="queue.sqlite"):
        return os.path.join(self._tmp, name)

    def open_stack(self, path=None):
        path = self.db_path() if path is None else path
        store = TaskStore(path)
        self.addCleanup(store.close)
        return store, Dispatcher(store)


class TestPriorityFifo(PolicyTestCase):
    def test_queue_orders_by_priority_then_created(self):
        store, _ = self.open_stack()
        with mock.patch.object(store_module, "time", _Clock(1000.0)):
            store.insert("t-a", {"n": 1}, priority=0)   # created 1001
            store.insert("t-b", {"n": 2}, priority=5)   # created 1002
            store.insert("t-c", {"n": 3}, priority=5)   # created 1003
            store.insert("t-d", {"n": 4}, priority=10)  # created 1004
        self.assertEqual([t.task_id for t in store.queue()],
                         ["t-d", "t-b", "t-c", "t-a"])

    def test_lease_follows_the_same_order(self):
        store, dispatcher = self.open_stack()
        with mock.patch.object(store_module, "time", _Clock(1000.0)):
            store.insert("t-a", {"n": 1}, priority=0)
            store.insert("t-b", {"n": 2}, priority=5)
            store.insert("t-c", {"n": 3}, priority=5)
            store.insert("t-d", {"n": 4}, priority=10)
        leased = []
        while True:
            task = dispatcher.lease("solo")
            if task is None:
                break
            leased.append(task.task_id)
            self.assertTrue(dispatcher.complete("solo", getattr(task, 'task_id', task), "ok", lease_token=lease_token(dispatcher, task)))
        self.assertEqual(leased, ["t-d", "t-b", "t-c", "t-a"])

    def test_equal_timestamps_tie_break_by_task_id(self):
        store, _ = self.open_stack()
        with mock.patch.object(store_module, "time", _FrozenClock(2000.0)):
            store.insert("t-b", {"n": 1})
            store.insert("t-a", {"n": 2})
        # same created, same priority: the deterministic tie-break is task_id
        self.assertEqual([t.task_id for t in store.queue()], ["t-a", "t-b"])

    def test_negative_and_mixed_priorities(self):
        store, _ = self.open_stack()
        with mock.patch.object(store_module, "time", _Clock(1000.0)):
            store.insert("t-zero", {"n": 0})              # 0, created 1001
            store.insert("t-neg", {"n": -1}, priority=-5)  # -5, created 1002
            store.insert("t-pos", {"n": 1}, priority=3)    # 3, created 1003
        self.assertEqual([t.task_id for t in store.queue()],
                         ["t-pos", "t-zero", "t-neg"])

    def test_retried_task_keeps_its_created_order(self):
        store, dispatcher = self.open_stack()
        with mock.patch.object(store_module, "time", _Clock(1000.0)):
            store.insert("t-old", {"n": 1}, priority=5)  # created 1001
            store.insert("t-new", {"n": 2}, priority=5)  # created 1002
        first = dispatcher.lease("w1")
        self.assertEqual(first.task_id, "t-old")
        self.assertTrue(dispatcher.fail("w1", "t-old", "boom", lease_token=lease_token(dispatcher, "t-old")))
        self.assertTrue(dispatcher.retry("t-old"))
        # retry does NOT reset created: t-old (1001) still precedes t-new (1002)
        self.assertEqual([t.task_id for t in store.queue()], ["t-old", "t-new"])
        self.assertEqual(dispatcher.lease("w2").task_id, "t-old")


class TestResourceExclusivity(PolicyTestCase):
    def test_second_task_on_held_resource_is_skipped_not_blocked_forever(self):
        store, dispatcher = self.open_stack()
        store.insert("t-gpu-1", {}, resource="gpu")
        store.insert("t-gpu-2", {}, resource="gpu")
        store.insert("t-free", {})
        first = dispatcher.lease("w1")
        self.assertEqual(first.task_id, "t-gpu-1")
        second = dispatcher.lease("w2")
        self.assertEqual(second.task_id, "t-free")  # t-gpu-2 skipped: gpu held
        # completing frees the resource: the skipped task is leasable again
        self.assertTrue(dispatcher.complete("w1", "t-gpu-1", "ok", lease_token=lease_token(dispatcher, "t-gpu-1")))
        third = dispatcher.lease("w3")
        self.assertEqual(third.task_id, "t-gpu-2")

    def test_exclusive_resource_never_runs_two_tasks_at_once(self):
        store, dispatcher = self.open_stack()
        store.insert("t-1", {}, resource="gpu")
        self.assertEqual(dispatcher.lease("w1").task_id, "t-1")
        store.insert("t-2", {}, resource="gpu")
        store.insert("t-3", {}, resource="other")
        self.assertEqual(dispatcher.lease("w2").task_id, "t-3")  # gpu busy, other free
        self.assertIsNone(dispatcher.lease("w3"))  # only the busy gpu task remains
        self.assertFalse(dispatcher.retry("t-1"))  # running, not failed: not retryable
        self.assertTrue(dispatcher.complete("w1", "t-1", "ok", lease_token=lease_token(dispatcher, "t-1")))
        self.assertEqual(dispatcher.lease("w3").task_id, "t-2")  # gpu free again

    def test_no_resource_tasks_are_never_excluded(self):
        store, dispatcher = self.open_stack()
        store.insert("t-1", {}, resource="gpu")
        store.insert("t-2", {})
        self.assertEqual(dispatcher.lease("w1").task_id, "t-1")
        self.assertEqual(dispatcher.lease("w2").task_id, "t-2")


def _single_lease_worker(db_path, worker_id):
    """Spawn-pool target: ONE lease attempt with the given worker id."""
    store = TaskStore(db_path)
    try:
        task = Dispatcher(store).lease(worker_id)
        return task.task_id if task else None
    finally:
        store.close()


class TestOneTaskPerWorker(PolicyTestCase):
    def test_worker_with_a_running_task_gets_no_lease(self):
        store, dispatcher = self.open_stack()
        store.insert("t-1", {})
        store.insert("t-2", {})
        self.assertEqual(dispatcher.lease("solo").task_id, "t-1")
        self.assertIsNone(dispatcher.lease("solo"))
        self.assertEqual(store.get("t-2").state, "queued")
        self.assertTrue(dispatcher.complete("solo", "t-1", "ok", lease_token=lease_token(dispatcher, "t-1")))
        self.assertEqual(dispatcher.lease("solo").task_id, "t-2")

    def test_six_spawn_processes_share_one_worker_id(self):
        path = self.db_path()
        store = TaskStore(path)
        for i in range(6):
            store.insert("t-%d" % i, {"n": i})
        store.close()  # children open their own connections
        ctx = multiprocessing.get_context("spawn")
        with ctx.Pool(6) as pool:
            results = pool.starmap(_single_lease_worker,
                                   [(path, "solo")] * 6)
        claimed = [task_id for task_id in results if task_id is not None]
        self.assertEqual(len(claimed), 1, msg=repr(results))
        reopened = TaskStore(path)
        self.addCleanup(reopened.close)
        tasks = reopened.list()
        self.assertEqual(len([t for t in tasks if t.state == "running"]), 1)
        running = next(t for t in tasks if t.state == "running")
        self.assertEqual((running.task_id, running.worker_id), (claimed[0], "solo"))
        self.assertEqual(len([t for t in tasks if t.state == "queued"]), 5)


if __name__ == "__main__":
    unittest.main()
