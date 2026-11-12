"""Worker side of the queue: dispatching, completion, and crash recovery.

Guarantees and limits
---------------------
* Every state change is ONE ``BEGIN IMMEDIATE`` transaction on the ONE SQLite
  file. Resource exclusivity and the one-running-task-per-worker slot are
  checked INSIDE the lease transaction, so no external lock or circuit
  breaker is needed and a crash cannot leave a half-leased task behind.
* ``lease`` picks, in one transaction, the queued task with the highest
  priority, then the oldest ``created`` (FIFO, ties broken by task_id),
  whose resource is not held by a task in state ``running`` — for a worker
  that is registered-or-new (auto-register), not paused, and holds no
  running task. Returns ``Task`` or None; a busy resource or busy worker is
  not an error.
* The ``workers`` table is the worker slot: ``current_task`` is non-NULL
  exactly while that worker holds a running lease. ``complete`` / ``fail``
  settle the task and clear the slot in the same transaction; ``fail``
  bumps ``consecutive_failures``, ``complete`` resets it to 0.
* ``complete`` / ``fail`` verify the caller is the leasing worker (the task
  row still names that worker) and move nothing otherwise — a mismatched
  worker can never strand the real worker's slot.
* Nothing expires by age. A worker that dies mid-task leaves the task
  ``running`` and its slot taken. Only the explicit
  ``recover(task_id, expected_worker)`` acts on that, and it is
  all-or-nothing: one transaction verifies the task is running AND still
  leased by ``expected_worker`` (compare-and-set on the stored lease token)
  and moves it to ``failed``, clearing the slot in the same transaction. A
  stale or mistyped worker matches nothing: a recovery call can never yank
  a task that was re-leased by another worker.
* Hooks: an optional ``hooks=`` object with the methods
  ``before_lease(worker_id, task) -> bool``, ``after_lease(worker_id,
  task)``, ``after_settle(worker_id, task, success)`` and
  ``after_recover(worker_id, task)``. ``before_lease`` runs per candidate
  BEFORE the claim (inside the lease transaction) and may veto it: False —
  or a raised exception, which is logged — skips that candidate. The other
  hooks run only AFTER the transaction committed. Hook exceptions are
  logged (logger ``joons_allocator.worker``) and never corrupt allocator
  state; a missing hook method is a no-op.
"""
import logging
import time
import uuid
from dataclasses import dataclass
from typing import List, Optional

from . import _db
from .errors import require_ids
from .store import Task, TaskStore, check_reason, check_result, task_from_row

LOGGER = logging.getLogger("joons_allocator.worker")

RECOVERED_RESULT = "recovered after worker loss"

_SELECT_TASK = ("SELECT task_id, state, payload, result, priority, resource, worker_id, "
                "lease_token, created, updated, version FROM tasks")

# The allocation order of TaskStore.queue()/lease: highest priority first,
# then oldest created (FIFO), ties broken by task_id.
_QUEUE_ORDER = "ORDER BY priority DESC, created, task_id"


@dataclass(frozen=True)
class WorkerInfo:
    """One immutable snapshot of a ``workers`` row.

    ``current_task`` is the running task this worker holds (None when idle);
    ``consecutive_failures`` counts ``fail`` since the last ``complete``;
    ``paused`` workers get no lease until resumed.
    """

    worker_id: str
    registered: float
    last_seen: float
    current_task: Optional[str]
    consecutive_failures: int
    paused: bool


class Dispatcher:
    """Hands queued tasks to workers; one SQLite transaction per state change."""

    def __init__(self, store: TaskStore, *, hooks=None):
        self._store = store
        self._hooks = hooks

    # -- worker side --------------------------------------------------------
    def lease(self, worker_id: str) -> Optional[Task]:
        """Claim the best queued task this worker may run now, or None.

        One transaction: auto-registers the worker, refuses a paused worker
        or one that already holds a running task, then walks the queued
        tasks in allocation order (priority DESC, created ASC) and claims
        the first candidate whose resource is free and whose
        ``before_lease`` hook (if any) does not veto. A lost claim or a
        refused candidate moves on to the next one; None means no leasable
        task right now.
        """
        require_ids(worker_id=worker_id)
        db = self._store._db
        token = uuid.uuid4().hex
        claimed: Optional[Task] = None
        with _db.write_txn(db):
            now = time.time()
            db.execute(
                "INSERT OR IGNORE INTO workers (worker_id, registered, last_seen, "
                "current_task, consecutive_failures, paused) VALUES (?, ?, ?, NULL, 0, 0)",
                (worker_id, now, now))
            row = db.execute("SELECT paused, current_task FROM workers WHERE worker_id=?",
                             (worker_id,)).fetchone()
            if row[0]:
                return None  # paused: registered, but nothing leased
            if row[1] is not None:
                return None  # one running task per worker: the slot is taken
            queued = db.execute(
                _SELECT_TASK + " WHERE state='queued' " + _QUEUE_ORDER).fetchall()
            for row in queued:
                candidate = task_from_row(row)
                if candidate.resource is not None and db.execute(
                        "SELECT 1 FROM tasks WHERE resource=? AND state='running' LIMIT 1",
                        (candidate.resource,)).fetchone():
                    continue  # resource busy: not this task (not an error)
                if self._veto(worker_id, candidate):
                    continue
                cur = db.execute(
                    "UPDATE tasks SET state='running', updated=?, version=version+1, "
                    "worker_id=?, lease_token=? WHERE task_id=? AND state='queued'",
                    (time.time(), worker_id, token, candidate.task_id))
                if cur.rowcount != 1:
                    continue  # lost the race (defensive inside one transaction)
                db.execute("UPDATE workers SET current_task=?, last_seen=? WHERE worker_id=?",
                           (candidate.task_id, time.time(), worker_id))
                claimed = self._read(db, candidate.task_id)
                break
        if claimed is not None:
            self._fire("after_lease", worker_id, claimed)
        return claimed

    def complete(self, worker_id: str, task_id, result: str, *, lease_token=None) -> bool:
        """Finish a leased task: ``running -> succeeded`` and the slot cleared,
        in one transaction. Only the leasing worker may settle."""
        task_id, lease_token = _lease_identity(task_id, lease_token)
        require_ids(worker_id=worker_id, task_id=task_id)
        if not isinstance(result, str):
            raise TypeError("result must be a str")
        check_result(result)
        return self._settle(worker_id, task_id, "succeeded", result, success=True, lease_token=lease_token)

    def fail(self, worker_id: str, task_id, reason: str, *, lease_token=None) -> bool:
        """Finish a leased task with a failure: ``running -> failed``, the
        reason stored, the slot cleared, ``consecutive_failures`` bumped —
        all in one transaction. Only the leasing worker may settle."""
        task_id, lease_token = _lease_identity(task_id, lease_token)
        require_ids(worker_id=worker_id, task_id=task_id)
        check_reason(reason)
        return self._settle(worker_id, task_id, "failed", reason, success=False, lease_token=lease_token)

    def retry(self, task_id: str) -> bool:
        """``failed -> queued`` (version bump) so the task runs again."""
        require_ids(task_id=task_id)
        return self._store.retry(task_id)

    def cancel(self, task_id: str) -> bool:
        """``queued -> cancelled`` only; running tasks must be failed or recovered first."""
        require_ids(task_id=task_id)
        return self._store._compare_and_set(task_id, "queued", "cancelled")

    def recover(self, task_id: str, expected_worker: str, *, lease_token) -> bool:
        """Operator recovery for a worker presumed dead mid-task.

        All-or-nothing in ONE transaction: verifies the task is running AND
        still leased by ``expected_worker`` (compare-and-set on the stored
        lease token) and moves it to ``failed`` with a ``recovered`` result,
        clearing the worker slot in the same statement batch. A stale
        recovery of a task re-leased by another worker, or a mistyped worker
        id, matches nothing: the call returns False and nothing is touched.
        ``retry`` re-queues the failed task afterwards.
        """
        require_ids(task_id=task_id, expected_worker=expected_worker, lease_token=lease_token)
        db = self._store._db
        recovered: Optional[Task] = None
        with _db.write_txn(db):
            row = db.execute("SELECT state, worker_id, lease_token FROM tasks WHERE task_id=?",
                             (task_id,)).fetchone()
            if row is None or row[0] != "running" or row[1] != expected_worker or row[2] != lease_token:
                return False  # not this worker's running lease any more: nothing touched
            now = time.time()
            cur = db.execute(
                "UPDATE tasks SET state='failed', result=?, updated=?, version=version+1, "
                "worker_id=NULL, lease_token=NULL WHERE task_id=? AND state='running' "
                "AND worker_id=? AND lease_token=?",
                (RECOVERED_RESULT, now, task_id, expected_worker, row[2]))
            if cur.rowcount != 1:
                return False  # defensive: the CAS lost inside its own transaction
            db.execute(
                "UPDATE workers SET current_task=NULL, last_seen=? "
                "WHERE worker_id=? AND current_task=?",
                (now, expected_worker, task_id))
            recovered = self._read(db, task_id)
        self._fire("after_recover", expected_worker, recovered)
        return True

    def abandon(self, task_id: str, worker_id: str, *, lease_token=None) -> bool:
        """Give a live lease back to the queue: ``running -> queued``.

        The deliberate inverse of ``begin_lease`` (the only ``running ->
        queued`` move in the package): joons_allocator.aio calls it when a
        coroutine was cancelled while ``lease`` was in flight and the claim
        landed anyway. Requires the task to still be ``running`` and leased
        by ``worker_id``; clears the worker slot in the same transaction.
        True when the task ended up ``queued`` again. Like ``complete``, it
        requires the leased Task or its ``lease_token`` so a stale caller can
        never hand back a newer lease of the same task.
        """
        task_id, lease_token = _lease_identity(task_id, lease_token)
        require_ids(task_id=task_id, worker_id=worker_id)
        db = self._store._db
        now = time.time()
        with _db.write_txn(db):
            cur = db.execute(
                "UPDATE tasks SET state='queued', updated=?, version=version+1, "
                "worker_id=NULL, lease_token=NULL WHERE task_id=? AND state='running' "
                "AND worker_id=? AND lease_token=?", (now, task_id, worker_id, lease_token))
            if cur.rowcount == 1:
                db.execute(
                    "UPDATE workers SET current_task=NULL WHERE worker_id=? AND current_task=?",
                    (worker_id, task_id))
        return cur.rowcount == 1

    # -- worker registry ----------------------------------------------------
    def heartbeat(self, worker_id: str) -> bool:
        """Refresh ``last_seen`` for a registered worker; False for an unknown
        one (heartbeat never auto-registers: leasing does)."""
        require_ids(worker_id=worker_id)
        with _db.write_txn(self._store._db):
            cur = self._store._db.execute(
                "UPDATE workers SET last_seen=? WHERE worker_id=?", (time.time(), worker_id))
        return cur.rowcount == 1

    def pause_worker(self, worker_id: str) -> bool:
        """Stop handing new leases to this worker (its running task is
        unaffected). False for an unknown worker; never auto-registers."""
        require_ids(worker_id=worker_id)
        with _db.write_txn(self._store._db):
            cur = self._store._db.execute(
                "UPDATE workers SET paused=1, last_seen=? WHERE worker_id=?",
                (time.time(), worker_id))
        return cur.rowcount == 1

    def resume_worker(self, worker_id: str) -> bool:
        """Hand leases to this worker again. False for an unknown worker."""
        require_ids(worker_id=worker_id)
        with _db.write_txn(self._store._db):
            cur = self._store._db.execute(
                "UPDATE workers SET paused=0, last_seen=? WHERE worker_id=?",
                (time.time(), worker_id))
        return cur.rowcount == 1

    def workers(self) -> List[WorkerInfo]:
        """All registered workers, ordered by worker_id."""
        rows = _db.query_all(
            self._store._db,
            "SELECT worker_id, registered, last_seen, current_task, consecutive_failures, "
            "paused FROM workers ORDER BY worker_id")
        return [WorkerInfo(row[0], row[1], row[2], row[3], row[4], bool(row[5]))
                for row in rows]

    # -- internals --------------------------------------------------------
    def _settle(self, worker_id: str, task_id: str, new_state: str, result: str, *,
                success: bool, lease_token=None) -> bool:
        db = self._store._db
        settled: Optional[Task] = None
        with _db.write_txn(db):
            row = db.execute("SELECT state, worker_id, lease_token FROM tasks WHERE task_id=?",
                             (task_id,)).fetchone()
            if row is None or row[0] != "running" or row[1] != worker_id or row[2] != lease_token:
                return False  # only the leasing worker may settle; nothing moved
            now = time.time()
            db.execute(
                "UPDATE tasks SET state=?, updated=?, version=version+1, result=?, "
                "worker_id=NULL, lease_token=NULL WHERE task_id=? AND state='running' "
                "AND worker_id=? AND lease_token=?", (new_state, now, result, task_id, worker_id, lease_token))
            db.execute(
                "UPDATE workers SET current_task=NULL, last_seen=?, "
                "consecutive_failures=CASE WHEN ? THEN 0 ELSE consecutive_failures+1 END "
                "WHERE worker_id=?", (now, 1 if success else 0, worker_id))
            settled = self._read(db, task_id)
        self._fire("after_settle", worker_id, settled, success)
        return True

    def _read(self, db, task_id: str) -> Optional[Task]:
        row = db.execute(_SELECT_TASK + " WHERE task_id=?", (task_id,)).fetchone()
        return task_from_row(row) if row else None

    def _veto(self, worker_id: str, task: Task) -> bool:
        """True when the candidate may not be leased (hook said no, or blew up)."""
        try:
            hook = self._hook("before_lease")
            return False if hook is None else not hook(worker_id, task)
        except Exception:
            LOGGER.warning("before_lease hook failed for worker %r, task %r; "
                           "skipping the candidate", worker_id, task.task_id, exc_info=True)
            return True

    def _fire(self, name: str, *args) -> None:
        """Run a post-commit hook; its failure never affects committed state."""
        try:
            hook = self._hook(name)
            if hook is not None:
                hook(*args)
        except Exception:
            LOGGER.warning("%s hook failed; the committed allocator state is unaffected",
                           name, exc_info=True)

    def _hook(self, name: str):
        if self._hooks is None:
            return None
        return getattr(self._hooks, name, None)


def _lease_identity(task, lease_token):
    if isinstance(task, Task):
        if task.lease_token is None:
            raise ValueError("Task has no lease token")
        if lease_token is not None and lease_token != task.lease_token:
            raise ValueError("lease token does not match Task")
        lease_token = task.lease_token
        task = task.task_id
    if lease_token is None:
        raise TypeError("lease_token is required when passing a task id; pass the leased Task or lease_token=")
    require_ids(lease_token=lease_token)
    return task, lease_token
