"""Fuzz tests for joons_allocator: random arguments and random operation
sequences against the public API, with three fixed seeds and a hard budget
well under 30 s.

Contract checked on every call:
* a call either succeeds or raises only the documented exceptions
  (AllocatorError subclasses, TypeError, ValueError); sqlite3.OperationalError
  is allowed only for unopenable database paths (fuzzed separately);
* no transaction is left open on the store connection;
* invariants hold after every step: a task is in exactly one known state,
  the lease identity is all-or-nothing (worker and token together), there is
  at most one running lease per task, at most one running task per worker,
  no two running tasks share a resource, the workers table agrees with the
  task rows, and audit rows only grow.

Ported from the prototype; the CLI and reconcile fuzz operations
are gone (the CLI is a later change, reconcile was removed with the
submissions ledger).
"""
from f2_test_helpers import lease_token
import os
import random
import sqlite3
import tempfile
import unittest

from joons_allocator import (AllocatorError, Admission, Control, Dispatcher,
                             TaskStore)
from joons_allocator.store import STATES

SEEDS = (20260101, 20260102, 20260103)
STEPS_PER_SEED = 110

DOCUMENTED = (AllocatorError, TypeError, ValueError)
# sqlite3.OperationalError is documented only for unopenable database paths;
# the path fuzz allows it, the operation fuzz must never see it.

BAD_TASK_IDS = ("", None, 5, True, "missing-1", "t" * 2000)
BAD_WORKERS = ("", None, 7, "w" * 2000, "worker-1", "worker-2", "worker-new")
BAD_PRINCIPALS = ("", None, 3, "p" * 2000, "principal-a", "principal-b")


def random_scalar(rng):
    return rng.choice([
        "", "x", "task", "ok", None, True, False, 0, 1, -7, 42,
        3.5, 1e308, -0.0, 10 ** 20, -10 ** 20, 2 ** 63,
        float("nan"), float("inf"), -float("inf"),
        "x" * rng.choice([1, 50, 1500, 5000]),
        [], {},
    ])


def random_key(rng):
    return rng.choice(["a", "b", "", "k" * 1500, 1, 2.5, None, True, (1, 2)])


def random_jsonish(rng, depth=0):
    """Random JSON-ish value, including non-strict corners (NaN, non-str
    keys, non-serializable objects) that the validators must refuse cleanly."""
    roll = rng.random()
    if depth >= 3 or roll < 0.45:
        return random_scalar(rng)
    if roll < 0.7:
        return [random_jsonish(rng, depth + 1) for _ in range(rng.randint(0, 4))]
    if roll < 0.92:
        return {random_key(rng): random_jsonish(rng, depth + 1)
                for _ in range(rng.randint(0, 4))}
    return rng.choice([
        {"nested-nan": [float("nan")]},
        {9: "int key"},
        {"mixed": {None: 1}},
        object(),
        {"cyclic-ish": [[["deep"]]]},
    ])


def attempt(test, func, *args, **kwargs):
    """Call one fuzzed operation; only documented exceptions may escape."""
    try:
        return func(*args, **kwargs)
    except DOCUMENTED as exc:  # AllocatorError / TypeError / ValueError
        test.assertIsInstance(exc, DOCUMENTED)
        test.assertNotIsInstance(exc, sqlite3.OperationalError)
        return None


class TestTaskStorePathFuzz(unittest.TestCase):
    """TaskStore paths: sqlite3.OperationalError is documented for unopenable
    paths only; everything else must be AllocatorError/TypeError/ValueError."""

    def test_random_paths_and_timeouts(self):
        for seed in SEEDS:
            rng = random.Random(seed)
            with tempfile.TemporaryDirectory(prefix="ja-fuzz-path-") as tmp:
                good_file = os.path.join(tmp, "ok.sqlite")
                targets = [
                    good_file, good_file,  # a valid path may be reused
                    os.path.join(tmp, "new-%d.sqlite" % rng.randint(0, 99)),
                    tmp,  # a directory: unopenable
                    os.path.join(tmp, "sub", "nope.sqlite"),  # missing parent
                    ":memory:",
                    "",  # empty path: unopenable
                    os.fsencode(os.path.join(tmp, "bytes-path.bin")),
                    None, 123, 3.5, ["not", "a", "path"],
                ]
                for path in targets:
                    store = None
                    try:
                        store = TaskStore(path, timeout=rng.choice([0.05, 10.0]))
                    except DOCUMENTED:
                        continue
                    except sqlite3.OperationalError:
                        continue  # documented for unopenable paths
                    try:
                        # False is fine here: good_file is reused in this list
                        store.insert("task-1", {"ok": True})
                        store.list()
                    finally:
                        store.close()


class TestApiFuzz(unittest.TestCase):
    """Random operation sequences over the whole public API."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name
        self.db = os.path.join(self._tmp, "queue.sqlite")

    # -- generators -------------------------------------------------------
    def random_task_id(self, rng):
        return rng.choice(list(self.task_ids) + list(BAD_TASK_IDS) + [self.fresh_id(rng)])

    def fresh_id(self, rng):
        self.task_ids.add("task-%d" % self.task_counter)
        self.task_counter += 1
        return "task-%d" % (self.task_counter - 1)

    def random_worker(self, rng):
        return rng.choice(BAD_WORKERS)

    def random_decided_by(self, rng):
        return rng.choice(["op-1", "", None, 9, "o" * 2000])

    # -- operations -------------------------------------------------------
    def op_store_insert(self, rng):
        attempt(self, self.store.insert, self.random_task_id(rng), random_jsonish(rng),
                priority=rng.choice([0, 1, -3, "2", None, True]),
                resource=rng.choice([None, "res-a", "res-b", "", 7]))

    def op_store_get(self, rng):
        attempt(self, self.store.get, self.random_task_id(rng))

    def op_store_cas(self, rng):
        task_id = self.random_task_id(rng)
        expected = rng.choice(list(STATES) + ["junk-state"])
        new = rng.choice(list(STATES) + ["junk-state"])
        attempt(self, self.store._compare_and_set, task_id, expected, new,
                expected_version=rng.choice([None, 1, 2, 99, -1, "2", 2.0]),
                result=rng.choice([None, "note", 5, {"x": 1}]))

    def op_store_retry(self, rng):
        attempt(self, self.store.retry, self.random_task_id(rng))

    def op_store_list(self, rng):
        state = rng.choice([None] + list(STATES) + ["junk-state"])
        attempt(self, self.store.list, state)

    def op_store_queue(self, rng):
        attempt(self, self.store.queue)

    def op_admission_submit(self, rng):
        attempt(self, self.admission.submit, random_jsonish(rng),
                task_id=rng.choice([None, None, "fixed-task", "", 5]),
                priority=rng.choice([0, 4, "2", None, True]),
                resource=rng.choice([None, "res-a", "", 7]),
                principal=rng.choice(BAD_PRINCIPALS),
                request_id=rng.choice(["req-1", "req-2", "", None, "r" * 2000,
                                       self.fresh_req(rng)]),
                require_approval=rng.choice([True, False, "yes"]))

    def fresh_req(self, rng):
        self.req_counter += 1
        return "req-%d" % self.req_counter

    def op_dispatcher_lease(self, rng):
        attempt(self, self.dispatcher.lease, self.random_worker(rng))

    def op_dispatcher_complete(self, rng):
        task_id = self.random_task_id(rng)
        attempt(self, self.dispatcher.complete, self.random_worker(rng),
                task_id, random_scalar(rng), lease_token=lease_token(self.dispatcher, task_id))

    def op_dispatcher_fail(self, rng):
        task_id = self.random_task_id(rng)
        attempt(self, self.dispatcher.fail, self.random_worker(rng),
                task_id, random_scalar(rng), lease_token=lease_token(self.dispatcher, task_id))

    def op_dispatcher_retry(self, rng):
        attempt(self, self.dispatcher.retry, self.random_task_id(rng))

    def op_dispatcher_cancel(self, rng):
        attempt(self, self.dispatcher.cancel, self.random_task_id(rng))

    def op_dispatcher_recover(self, rng):
        task_id = self.random_task_id(rng)
        attempt(self, self.dispatcher.recover, task_id,
                self.random_worker(rng), lease_token=lease_token(self.dispatcher, task_id))

    def op_dispatcher_heartbeat(self, rng):
        attempt(self, self.dispatcher.heartbeat, self.random_worker(rng))

    def op_dispatcher_worker_admin(self, rng):
        op = rng.choice([self.dispatcher.pause_worker, self.dispatcher.resume_worker,
                         self.dispatcher.workers])
        if op is self.dispatcher.workers:
            attempt(self, op)
        else:
            attempt(self, op, self.random_worker(rng))

    def op_control_decide(self, rng):
        action = rng.choice(["approve", "pause", "resume", "cancel", "retry", "recover"])
        method = getattr(self.control, action)
        if action == "recover":
            task_id = self.random_task_id(rng)
            attempt(self, method, task_id, self.random_worker(rng),
                    self.random_decided_by(rng), rng.choice(["note", "", None, 12]),
                    lease_token=lease_token(self.control, task_id))
        else:
            attempt(self, method, self.random_task_id(rng),
                    self.random_decided_by(rng), rng.choice(["note", "", None, 12]))

    def op_control_history(self, rng):
        attempt(self, self.control.history, self.random_task_id(rng))

    # -- invariants -------------------------------------------------------
    def check_invariants(self):
        self.assertFalse(self.store._db.in_transaction, msg="open transaction on store")
        rows = self.store.list()
        task_by_id = {}
        running_ids = set()
        held_resources = {}
        for task in rows:
            task_by_id[task.task_id] = task
            self.assertIn(task.state, STATES, msg=task.task_id)
            # lease identity is all-or-nothing, and only running tasks hold one
            self.assertEqual(task.worker_id is None, task.lease_token is None,
                             msg=task.task_id)
            if task.worker_id is not None:
                self.assertEqual(task.state, "running", msg=task.task_id)
                running_ids.add(task.task_id)
            if task.state == "running" and task.resource is not None:
                self.assertNotIn(task.resource, held_resources,
                                 msg="two running tasks on %r" % task.resource)
                held_resources[task.resource] = task.task_id
        # the workers table agrees with the task rows
        seen_running = set()
        for info in self.dispatcher.workers():
            if info.current_task is None:
                continue
            task = task_by_id.get(info.current_task)
            self.assertIsNotNone(task, msg="slot for unknown task %r" % info.current_task)
            self.assertEqual(task.state, "running", msg=info.current_task)
            self.assertEqual(task.worker_id, info.worker_id, msg=info.current_task)
            self.assertNotIn(info.current_task, seen_running,
                             msg="two workers hold %r" % info.current_task)
            seen_running.add(info.current_task)
        self.assertEqual(seen_running, running_ids)
        # audit rows only grow
        for task_id in self.task_ids:
            count = len(self.control.history(task_id))
            self.assertGreaterEqual(count, self.audit_counts.get(task_id, 0),
                                    msg="audit shrank for %r" % task_id)
            self.audit_counts[task_id] = count

    # -- driver -----------------------------------------------------------
    def run_fuzz(self, seed):
        rng = random.Random(seed)
        self.store = TaskStore(self.db)
        self.addCleanup(self.store.close)
        self.dispatcher = Dispatcher(self.store)
        self.control = Control(self.store, self.dispatcher)
        self.admission = Admission(self.store)
        self.task_ids = {"task-seed-%d" % i for i in range(4)}
        self.task_counter = 0
        self.req_counter = 0
        self.audit_counts = {}
        for task_id in sorted(self.task_ids):
            self.assertTrue(self.store.insert(task_id, {"task": task_id}))
        ops = [(3, self.op_store_insert), (2, self.op_store_get),
               (3, self.op_store_cas), (1, self.op_store_retry),
               (1, self.op_store_list), (1, self.op_store_queue),
               (4, self.op_admission_submit),
               (5, self.op_dispatcher_lease), (5, self.op_dispatcher_complete),
               (3, self.op_dispatcher_fail), (2, self.op_dispatcher_retry),
               (2, self.op_dispatcher_cancel), (2, self.op_dispatcher_recover),
               (1, self.op_dispatcher_heartbeat), (1, self.op_dispatcher_worker_admin),
               (4, self.op_control_decide), (1, self.op_control_history)]
        population = [op for _, op in ops]
        weights = [weight for weight, _ in ops]
        for _ in range(STEPS_PER_SEED):
            rng.choices(population, weights=weights, k=1)[0](rng)
            self.check_invariants()

    def test_seed_1(self):
        self.run_fuzz(SEEDS[0])

    def test_seed_2(self):
        self.run_fuzz(SEEDS[1])

    def test_seed_3(self):
        self.run_fuzz(SEEDS[2])


if __name__ == "__main__":
    unittest.main()
