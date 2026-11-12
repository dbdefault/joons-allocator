"""Focused tests added by the J10 mutation campaign
(review3/MUTATION_REPORT.md), ported to joons_allocator. Each test kills a
mutant that survived the original suite and passes on the real sources."""

import os
import sqlite3
import tempfile
import unittest

from joons_allocator import Admission, Control, Dispatcher, TaskStore


class TestAuditAtomicity(unittest.TestCase):
    """Kills M13 ('audit row written after COMMIT').

    A decision and its audit row must commit or roll back together: when the
    audit INSERT fails, the state change must not survive. The mutant moved
    the INSERT after the write transaction, so the state change committed
    first and an audit failure left an unaudited decision behind.
    """

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = os.path.join(tmp.name, "queue.sqlite")
        self.store = TaskStore(path)
        self.addCleanup(self.store.close)
        dispatcher = Dispatcher(self.store)
        self.control = Control(self.store, dispatcher)
        admission = Admission(self.store)
        result = admission.submit({"task": "work"}, principal="principal-a",
                                  request_id="req-1", require_approval=True)
        self.assertEqual(result.status, "accepted")
        self.task_id = result.task_id

    def test_failed_audit_insert_rolls_back_the_decision(self):
        real_execute = self.store._db.execute
        fired = []

        def flaky_execute(sql, *args, **kwargs):
            if "INSERT INTO task_audit" in sql:
                fired.append(sql)
                raise sqlite3.OperationalError("injected audit failure")
            return real_execute(sql, *args, **kwargs)

        self.store._db.execute = flaky_execute
        try:
            with self.assertRaises(sqlite3.OperationalError):
                self.control.approve(self.task_id, "op-1", note="go")
        finally:
            del self.store._db.execute  # restore the real connection method

        self.assertEqual(len(fired), 1)  # the audit insert was attempted once
        # the decision rolled back with the failed audit row: no unaudited move
        self.assertEqual(self.store.get(self.task_id).state, "awaiting_approval")
        self.assertEqual(self.control.history(self.task_id), [])
        # a clean retry succeeds and the trail is written
        self.assertTrue(self.control.approve(self.task_id, "op-1", note="go"))
        self.assertEqual(self.store.get(self.task_id).state, "queued")
        self.assertEqual([event.action for event in self.control.history(self.task_id)],
                         ["approve"])


if __name__ == "__main__":
    unittest.main()
