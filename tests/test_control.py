"""Tests for joons_allocator.control: audited operator decisions, the
approval gate in Admission, and the audit trail ordering. Standard library
only.

Ported from the prototype ( the guard lock/circuit
assertions were replaced by workers-table assertions), plus the review2 W1
regression: a stale recover of a re-leased task is refused wholesale."""
from f2_test_helpers import lease_token
import os
import tempfile
import unittest

from joons_allocator import Admission
from joons_allocator.control import AuditEvent, Control
from joons_allocator.store import TaskStore
from joons_allocator.worker import RECOVERED_RESULT, Dispatcher

PAYLOAD = {"task": "work", "resource": "res-a"}


class ControlTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name
        path = os.path.join(self._tmp, "queue.sqlite")
        self.store = TaskStore(path)
        self.addCleanup(self.store.close)
        self.dispatcher = Dispatcher(self.store)
        self.control = Control(self.store, self.dispatcher)
        self.admission = Admission(self.store)

    def insert(self, task_id="task-1", payload=PAYLOAD, resource=None):
        self.assertTrue(self.store.insert(task_id, payload, resource=resource))
        return task_id

    def admit(self, request_id="req-1", payload=PAYLOAD, require_approval=False):
        result = self.admission.submit(payload, principal="principal-a",
                                       request_id=request_id,
                                       require_approval=require_approval)
        self.assertEqual(result.status, "accepted")
        return result.task_id

    def slot(self, worker_id):
        for info in self.dispatcher.workers():
            if info.worker_id == worker_id:
                return info
        return None


class TestDecisions(ControlTestCase):
    def test_approve_moves_to_queued_with_audit(self):
        task_id = self.admit(require_approval=True)
        self.assertEqual(self.store.get(task_id).state, "awaiting_approval")
        self.assertTrue(self.control.approve(task_id, "op-1", note="go"))
        task = self.store.get(task_id)
        self.assertEqual((task.state, task.version), ("queued", 2))
        trail = self.control.history(task_id)
        self.assertEqual(len(trail), 1)
        event = trail[0]
        self.assertIsInstance(event, AuditEvent)
        self.assertEqual(event.task_id, task_id)
        self.assertEqual((event.action, event.before_state, event.after_state),
                         ("approve", "awaiting_approval", "queued"))
        self.assertEqual((event.decided_by, event.note), ("op-1", "go"))
        self.assertGreater(event.ts, 0)
        self.assertEqual(self.dispatcher.lease("worker-1").task_id, task_id)

    def test_approve_refused_leaves_no_audit(self):
        task_id = self.insert()
        self.assertFalse(self.control.approve(task_id, "op-1"))  # queued, not awaiting
        self.assertEqual(self.control.history(task_id), [])
        self.assertEqual(self.store.get(task_id).state, "queued")
        self.assertFalse(self.control.approve("missing", "op-1"))

    def test_pause_resume_cycle(self):
        task_id = self.insert()
        self.assertTrue(self.control.pause(task_id, "op-1"))
        self.assertEqual(self.store.get(task_id).state, "paused")
        self.assertIsNone(self.dispatcher.lease("worker-1"))  # paused tasks are not leased
        self.assertTrue(self.control.resume(task_id, "op-2", note="back"))
        self.assertEqual(self.store.get(task_id).state, "queued")
        self.assertEqual([e.action for e in self.control.history(task_id)],
                         ["pause", "resume"])
        self.assertEqual(self.dispatcher.lease("worker-1").task_id, task_id)

    def test_pause_only_from_queued(self):
        task_id = self.insert()
        self.assertTrue(self.control.pause(task_id, "op-1"))
        self.assertFalse(self.control.pause(task_id, "op-1"))  # paused, not queued
        awaiting = self.admit(request_id="req-1", require_approval=True)
        self.assertFalse(self.control.pause(awaiting, "op-1"))  # awaiting_approval
        self.assertTrue(self.control.resume(task_id, "op-2"))
        running = self.dispatcher.lease("worker-1")
        self.assertEqual(running.task_id, task_id)
        self.assertFalse(self.control.pause(running.task_id, "op-1"))  # running

    def test_cancel_from_queued_awaiting_and_paused(self):
        awaiting = self.admit(request_id="req-a", require_approval=True)
        self.assertTrue(self.control.cancel(awaiting, "op-1"))
        queued = self.insert("task-b", {"task": "b"})
        self.assertTrue(self.control.cancel(queued, "op-1"))
        paused = self.insert("task-c", {"task": "c"})
        self.assertTrue(self.control.pause(paused, "op-1"))
        self.assertTrue(self.control.cancel(paused, "op-1"))
        for task_id in (awaiting, queued):
            self.assertEqual(self.store.get(task_id).state, "cancelled")
            self.assertEqual([e.action for e in self.control.history(task_id)], ["cancel"])
        self.assertEqual(self.store.get(paused).state, "cancelled")
        self.assertEqual([e.action for e in self.control.history(paused)],
                         ["pause", "cancel"])

    def test_cancel_never_touches_running(self):
        task_id = self.insert()
        task = self.dispatcher.lease("worker-1")
        self.assertFalse(self.control.cancel(task.task_id, "op-1"))
        self.assertEqual(self.store.get(task_id).state, "running")
        self.assertEqual(self.control.history(task_id), [])

    def test_history_ordered_by_id(self):
        task_id = self.insert()
        self.control.pause(task_id, "op-1")
        self.control.resume(task_id, "op-1")
        self.control.pause(task_id, "op-2", note="hold")
        self.control.resume(task_id, "op-2")
        trail = self.control.history(task_id)
        self.assertEqual([e.id for e in trail], sorted(e.id for e in trail))
        self.assertEqual([e.action for e in trail], ["pause", "resume", "pause", "resume"])
        self.assertEqual([e.decided_by for e in trail], ["op-1", "op-1", "op-2", "op-2"])
        self.assertEqual([e.note for e in trail], ["", "", "hold", ""])


class TestRetryRecover(ControlTestCase):
    def test_retry_with_audit(self):
        task_id = self.insert()
        task = self.dispatcher.lease("worker-1")
        self.assertTrue(self.dispatcher.fail("worker-1", task, "boom", lease_token=lease_token(self.dispatcher, task)))
        self.assertTrue(self.control.retry(task_id, "op-1", note="try again"))
        task = self.store.get(task_id)
        self.assertEqual((task.state, task.version, task.result), ("queued", 4, None))
        trail = self.control.history(task_id)
        self.assertEqual([(e.action, e.before_state, e.after_state) for e in trail],
                         [("retry", "failed", "queued")])
        self.assertFalse(self.control.retry(task_id, "op-1"))  # queued now, not failed
        self.assertEqual(len(self.control.history(task_id)), 1)

    def test_recover_with_audit(self):
        task_id = self.insert()
        self.dispatcher.lease("worker-1")
        self.assertTrue(self.control.recover(task_id, "worker-1", "op-2", note="worker died", lease_token=lease_token(self.control, task_id)))
        failed = self.store.get(task_id)
        self.assertEqual((failed.state, failed.result), ("failed", RECOVERED_RESULT))
        # the dead worker's slot was cleared
        self.assertIsNone(self.slot("worker-1").current_task)
        trail = self.control.history(task_id)
        self.assertEqual([(e.action, e.before_state, e.after_state, e.decided_by)
                          for e in trail],
                         [("recover", "running", "failed", "op-2")])
        self.assertFalse(self.control.recover(task_id, "worker-1", "op-2", lease_token=lease_token(self.control, task_id)))  # not running
        self.assertEqual(len(self.control.history(task_id)), 1)


class TestRecoverLeaseOwnership(ControlTestCase):
    """review2 W1: recover acts only on the exact (task_id, worker, lease)."""

    def test_w1_stale_recover_never_touches_a_re_leased_task(self):
        self.insert("task-1", {"task": "a"}, resource="res-x")
        self.assertEqual(self.dispatcher.lease("worker-1").task_id, "task-1")
        self.assertTrue(self.control.recover("task-1", "worker-1", "op", lease_token=lease_token(self.control, "task-1")))  # genuine crash
        self.assertTrue(self.dispatcher.retry("task-1"))
        self.assertEqual(self.dispatcher.lease("worker-2").task_id, "task-1")
        task = self.store.get("task-1")
        self.assertEqual((task.state, task.worker_id), ("running", "worker-2"))

        # the stale recovery arrives (double-run CLI, retrying automation, ...)
        self.assertFalse(self.control.recover("task-1", "worker-1", "op", lease_token=lease_token(self.control, "task-1")))
        task = self.store.get("task-1")
        self.assertEqual((task.state, task.worker_id), ("running", "worker-2"))
        self.assertEqual(self.slot("worker-2").current_task, "task-1")
        # the new worker finishes its own lease normally
        self.assertTrue(self.dispatcher.complete("worker-2", "task-1", "still working", lease_token=lease_token(self.dispatcher, "task-1")))
        self.assertEqual(self.store.get("task-1").state, "succeeded")
        # the audit trail only ever saw the genuine recovery
        self.assertEqual([(e.action, e.before_state, e.after_state)
                          for e in self.control.history("task-1")],
                         [("recover", "running", "failed")])


class TestApprovalGate(ControlTestCase):
    def test_require_approval_admits_as_awaiting(self):
        self.insert("task-direct", {"task": "direct"})
        task_id = self.admit(request_id="req-a", require_approval=True)
        self.assertEqual(self.store.get(task_id).state, "awaiting_approval")
        # the ledger recorded the request with its task id
        import sqlite3
        conn = sqlite3.connect(self.store.path)
        try:
            row = conn.execute(
                "SELECT task_id FROM submissions WHERE principal='principal-a' "
                "AND request_id='req-a'").fetchone()
        finally:
            conn.close()
        self.assertEqual(row[0], task_id)
        # workers only see the directly inserted task: the gate holds them back
        self.assertEqual(self.dispatcher.lease("worker-1").task_id, "task-direct")
        self.assertTrue(self.control.approve(task_id, "op-1"))
        self.assertEqual(self.dispatcher.lease("worker-2").task_id, task_id)

    def test_require_approval_rejects_non_bool(self):
        with self.assertRaises(TypeError):
            self.admission.submit(PAYLOAD, principal="principal-a", request_id="req-b",
                                  require_approval="yes")


if __name__ == "__main__":
    unittest.main()
