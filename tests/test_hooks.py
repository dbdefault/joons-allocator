"""Tests for the Dispatcher hooks: call order, veto semantics, and
hook-failure safety (a hook exception is logged and never corrupts
allocator state). Standard library only."""
from f2_test_helpers import lease_token
import os
import tempfile
import unittest

from joons_allocator.store import TaskStore
from joons_allocator.worker import Dispatcher

LOGGER_NAME = "joons_allocator.worker"


class Recorder:
    """Hooks implementation recording every call; each hook can be told to
    raise (always, or once), and before_lease can veto named task ids."""

    def __init__(self):
        self.calls = []
        self.fail = set()       # hook names that always raise
        self.fail_once = set()  # hook names that raise on their first call only
        self.veto = set()
        self._counts = {}

    def _should_raise(self, name):
        self._counts[name] = self._counts.get(name, 0) + 1
        if name in self.fail:
            return True
        return name in self.fail_once and self._counts[name] == 1

    def before_lease(self, worker_id, task):
        self.calls.append(("before_lease", worker_id, task.task_id))
        if self._should_raise("before_lease"):
            raise RuntimeError("before_lease boom")
        return task.task_id not in self.veto

    def after_lease(self, worker_id, task):
        self.calls.append(("after_lease", worker_id, task.task_id))
        if self._should_raise("after_lease"):
            raise RuntimeError("after_lease boom")

    def after_settle(self, worker_id, task, success):
        self.calls.append(("after_settle", worker_id, task.task_id, success))
        if self._should_raise("after_settle"):
            raise RuntimeError("after_settle boom")

    def after_recover(self, worker_id, task):
        self.calls.append(("after_recover", worker_id, task.task_id))
        if self._should_raise("after_recover"):
            raise RuntimeError("after_recover boom")


class CommitProbe:
    """after_lease opens a FRESH connection and checks the task is already
    running: the hook must run only AFTER the lease transaction committed."""

    def __init__(self, path):
        self.path = path
        self.calls = []

    def after_lease(self, worker_id, task):
        probe = TaskStore(self.path)
        try:
            state = probe.get(task.task_id).state
        finally:
            probe.close()
        self.calls.append(state)


class HooksTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name
        self.path = os.path.join(self._tmp, "queue.sqlite")
        self.store = TaskStore(self.path)
        self.addCleanup(self.store.close)
        self.recorder = Recorder()
        self.dispatcher = Dispatcher(self.store, hooks=self.recorder)

    def slot(self, worker_id):
        for info in self.dispatcher.workers():
            if info.worker_id == worker_id:
                return info
        return None


class TestCallOrder(HooksTestCase):
    def test_lease_then_complete_calls_in_order(self):
        self.store.insert("task-1", {"task": "a"})
        task = self.dispatcher.lease("worker-1")
        self.assertEqual(task.task_id, "task-1")
        self.assertEqual(self.recorder.calls,
                         [("before_lease", "worker-1", "task-1"),
                          ("after_lease", "worker-1", "task-1")])
        self.assertTrue(self.dispatcher.complete("worker-1", "task-1", "ok", lease_token=lease_token(self.dispatcher, "task-1")))
        self.assertEqual(self.recorder.calls,
                         [("before_lease", "worker-1", "task-1"),
                          ("after_lease", "worker-1", "task-1"),
                          ("after_settle", "worker-1", "task-1", True)])

    def test_fail_records_success_false(self):
        self.store.insert("task-1", {"task": "a"})
        task = self.dispatcher.lease("worker-1")
        self.assertTrue(self.dispatcher.fail("worker-1", getattr(task, 'task_id', task), "boom", lease_token=lease_token(self.dispatcher, task)))
        self.assertEqual(self.recorder.calls[-1],
                         ("after_settle", "worker-1", "task-1", False))

    def test_recover_fires_after_recover(self):
        self.store.insert("task-1", {"task": "a"})
        self.dispatcher.lease("worker-1")
        self.assertTrue(self.dispatcher.recover("task-1", "worker-1", lease_token=lease_token(self.dispatcher, "task-1")))
        self.assertEqual(self.recorder.calls[-1],
                         ("after_recover", "worker-1", "task-1"))

    def test_refused_actions_fire_no_hooks(self):
        self.store.insert("task-1", {"task": "a"})
        # nothing queued for a paused worker: no before_lease, no after_lease
        self.dispatcher.lease("worker-1")
        self.dispatcher.pause_worker("worker-1")
        self.store.insert("task-2", {"task": "b"})
        self.recorder.calls.clear()
        self.assertIsNone(self.dispatcher.lease("worker-1"))
        # settling from the wrong worker moves nothing and fires no hook
        self.assertFalse(self.dispatcher.complete("worker-2", "task-1", "nope", lease_token=lease_token(self.dispatcher, "task-1")))
        self.assertEqual(self.recorder.calls, [])

    def test_hooks_see_committed_state(self):
        probe = CommitProbe(self.path)
        dispatcher = Dispatcher(self.store, hooks=probe)
        self.store.insert("task-1", {"task": "a"})
        dispatcher.lease("worker-1")
        self.assertEqual(probe.calls, ["running"])


class TestVeto(HooksTestCase):
    def test_veto_skips_the_candidate_and_claims_the_next(self):
        self.store.insert("task-1", {"task": "a"})
        self.store.insert("task-2", {"task": "b"})
        self.recorder.veto.add("task-1")
        task = self.dispatcher.lease("worker-1")
        self.assertEqual(task.task_id, "task-2")  # vetoed candidate skipped
        self.assertEqual(self.store.get("task-1").state, "queued")
        self.assertEqual([c[2] for c in self.recorder.calls if c[0] == "before_lease"],
                         ["task-1", "task-2"])
        self.assertEqual([c[2] for c in self.recorder.calls if c[0] == "after_lease"],
                         ["task-2"])

    def test_veto_everything_returns_none_and_still_registers(self):
        self.store.insert("task-1", {"task": "a"})
        self.recorder.veto.add("task-1")
        self.assertIsNone(self.dispatcher.lease("worker-1"))
        self.assertEqual(self.store.get("task-1").state, "queued")
        slot = self.slot("worker-1")
        self.assertIsNotNone(slot)  # auto-registered by the attempt
        self.assertIsNone(slot.current_task)


class TestHookFailureSafety(HooksTestCase):
    def test_before_lease_exception_vetoes_and_is_logged(self):
        self.store.insert("task-1", {"task": "a"})
        self.recorder.fail.add("before_lease")
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self.assertIsNone(self.dispatcher.lease("worker-1"))
        self.assertEqual(self.store.get("task-1").state, "queued")
        self.assertFalse(self.store._db.in_transaction)

    def test_before_lease_exception_does_not_block_the_next_candidate(self):
        self.store.insert("task-1", {"task": "a"})
        self.store.insert("task-2", {"task": "b"})
        self.recorder.fail_once.add("before_lease")  # raise for task-1 only
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            task = self.dispatcher.lease("worker-1")
        self.assertEqual(task.task_id, "task-2")
        self.assertEqual(self.store.get("task-1").state, "queued")

    def test_after_lease_exception_keeps_the_lease(self):
        self.store.insert("task-1", {"task": "a"})
        self.recorder.fail.add("after_lease")
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            task = self.dispatcher.lease("worker-1")
        self.assertEqual(task.task_id, "task-1")
        self.assertEqual(self.store.get("task-1").state, "running")
        self.assertEqual(self.slot("worker-1").current_task, "task-1")

    def test_after_settle_exception_keeps_the_settle(self):
        self.store.insert("task-1", {"task": "a"})
        task = self.dispatcher.lease("worker-1")
        self.recorder.fail.add("after_settle")
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self.assertTrue(self.dispatcher.complete("worker-1", getattr(task, 'task_id', task), "ok", lease_token=lease_token(self.dispatcher, task)))
        self.assertEqual(self.store.get("task-1").state, "succeeded")
        self.assertIsNone(self.slot("worker-1").current_task)

    def test_after_recover_exception_keeps_the_recover(self):
        self.store.insert("task-1", {"task": "a"})
        self.dispatcher.lease("worker-1")
        self.recorder.fail.add("after_recover")
        with self.assertLogs(LOGGER_NAME, level="WARNING"):
            self.assertTrue(self.dispatcher.recover("task-1", "worker-1", lease_token=lease_token(self.dispatcher, "task-1")))
        failed = self.store.get("task-1")
        self.assertEqual(failed.state, "failed")
        self.assertIsNone(self.slot("worker-1").current_task)


class TestPartialHooks(HooksTestCase):
    def test_missing_hook_methods_are_noops(self):
        class OnlyAfterLease:
            def __init__(self):
                self.leased = []

            def after_lease(self, worker_id, task):
                self.leased.append(task.task_id)

        hooks = OnlyAfterLease()
        dispatcher = Dispatcher(self.store, hooks=hooks)
        self.store.insert("task-1", {"task": "a"})
        task = dispatcher.lease("worker-1")
        self.assertEqual(task.task_id, "task-1")
        self.assertEqual(hooks.leased, ["task-1"])
        self.assertTrue(dispatcher.complete("worker-1", "task-1", "ok", lease_token=lease_token(dispatcher, "task-1")))
        self.assertEqual(dispatcher.recover("task-1", "worker-1", lease_token=lease_token(dispatcher, "task-1")), False)

    def test_no_hooks_is_the_default(self):
        dispatcher = Dispatcher(self.store)
        self.store.insert("task-1", {"task": "a"})
        task = dispatcher.lease("worker-1")
        self.assertEqual(task.task_id, "task-1")
        self.assertTrue(dispatcher.complete("worker-1", "task-1", "ok", lease_token=lease_token(dispatcher, "task-1")))


if __name__ == "__main__":
    unittest.main()
