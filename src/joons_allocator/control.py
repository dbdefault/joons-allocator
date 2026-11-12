"""Operator control: explicit decisions over the queue, with an audit trail.

Guarantees and limits
---------------------
* ``approve`` / ``pause`` / ``resume`` / ``cancel`` / ``retry`` re-read the
  task INSIDE the write transaction and change state only along the
  transition table; the audit row commits in the SAME transaction, so a
  decision becomes visible with its trail or not at all. False means
  "nothing changed" (task missing or state not allowed) and no audit row is
  written for a refused decision.
* ``recover`` changes the task, clears the slot and records its audit row
  in one transaction; its recovery hook runs after commit.
* The ``task_audit`` table is append-only: rows are never updated or deleted.
* States behind the operator decisions (``awaiting_approval``, ``paused``)
  are invisible to ``Dispatcher.lease``, which only scans ``queued``.
"""
import time
from dataclasses import dataclass
from typing import List

from . import _db
from .errors import require_ids
from .store import TaskStore
from .worker import Dispatcher, RECOVERED_RESULT

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS task_audit ("
    "id INTEGER PRIMARY KEY, task_id TEXT NOT NULL, action TEXT NOT NULL, "
    "before_state TEXT NOT NULL, after_state TEXT NOT NULL, decided_by TEXT NOT NULL, "
    "note TEXT NOT NULL, ts REAL NOT NULL)",
)

_SELECT = ("SELECT id, task_id, action, before_state, after_state, decided_by, note, ts "
           "FROM task_audit")

_INSERT = ("INSERT INTO task_audit (task_id, action, before_state, after_state, decided_by, "
           "note, ts) VALUES (?, ?, ?, ?, ?, ?, ?)")

# action -> (states the decision is allowed from, resulting state)
DECISIONS = {
    "approve": (("awaiting_approval",), "queued"),
    "pause": (("queued",), "paused"),
    "resume": (("paused",), "queued"),
    "cancel": (("queued", "awaiting_approval", "paused"), "cancelled"),
}

RETRY_FROM = "failed"
RECOVER_FROM = "running"


@dataclass(frozen=True)
class AuditEvent:
    """One append-only audit row: an operator decision over one task."""

    id: int
    task_id: str
    action: str
    before_state: str
    after_state: str
    decided_by: str
    note: str
    ts: float


class Control:
    """Explicit operator decisions over the queue, each with an audit trail."""

    def __init__(self, store: TaskStore, dispatcher: Dispatcher):
        self._store = store
        self._dispatcher = dispatcher
        with _db.write_txn(self._store._db):  # the store's own connection
            for stmt in _SCHEMA:
                self._store._db.execute(stmt)

    # -- audited decisions (state change + audit row in one transaction) ----
    def approve(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``awaiting_approval -> queued`` so workers can lease the task."""
        return self._decide("approve", task_id, decided_by, note)

    def pause(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``queued -> paused``: the task stops being leasable."""
        return self._decide("pause", task_id, decided_by, note)

    def resume(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``paused -> queued``."""
        return self._decide("resume", task_id, decided_by, note)

    def cancel(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``queued | awaiting_approval | paused -> cancelled`` (never running)."""
        return self._decide("cancel", task_id, decided_by, note)

    def retry(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``failed -> queued`` (version bump) with an audit row, atomically."""
        require_ids(task_id=task_id, decided_by=decided_by)
        _require_note(note)
        now = time.time()
        with _db.write_txn(self._store._db):
            row = self._store._db.execute(
                "SELECT state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None or row[0] != RETRY_FROM:
                return False
            self._store._db.execute(
                "UPDATE tasks SET state='queued', updated=?, version=version+1, result=NULL "
                "WHERE task_id=? AND state='failed'", (now, task_id))
            self._store._db.execute(
                _INSERT, (task_id, "retry", RETRY_FROM, "queued", decided_by, note, now))
        return True

    def recover(self, task_id: str, expected_worker: str, decided_by: str,
                note: str = "", *, lease_token) -> bool:
        """Atomically recover a matching lease and append its audit event."""
        require_ids(task_id=task_id, expected_worker=expected_worker, decided_by=decided_by)
        require_ids(lease_token=lease_token)
        _require_note(note)
        db = self._store._db
        with _db.write_txn(db):
            row = db.execute("SELECT state, worker_id, lease_token FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None or row[0] != "running" or row[1] != expected_worker or row[2] != lease_token:
                return False
            now = time.time()
            cur = db.execute("UPDATE tasks SET state='failed', result=?, updated=?, version=version+1, worker_id=NULL, lease_token=NULL WHERE task_id=? AND state='running' AND worker_id=? AND lease_token=?", (RECOVERED_RESULT, now, task_id, expected_worker, row[2]))
            if cur.rowcount != 1:
                return False
            db.execute("UPDATE workers SET current_task=NULL, last_seen=? WHERE worker_id=? AND current_task=?", (now, expected_worker, task_id))
            db.execute(_INSERT, (task_id, "recover", "running", "failed", decided_by, note, now))
            recovered = self._dispatcher._read(db, task_id)
        self._dispatcher._fire("after_recover", expected_worker, recovered)
        return True

    def history(self, task_id: str) -> List[AuditEvent]:
        """All audit rows for one task, oldest (lowest id) first."""
        require_ids(task_id=task_id)
        rows = _db.query_all(self._store._db, _SELECT + " WHERE task_id=? ORDER BY id",
                             (task_id,))
        return [AuditEvent(*row) for row in rows]

    # -- internals --------------------------------------------------------
    def _decide(self, action: str, task_id: str, decided_by: str, note: str) -> bool:
        require_ids(task_id=task_id, decided_by=decided_by)
        _require_note(note)
        allowed_from, after = DECISIONS[action]
        now = time.time()
        with _db.write_txn(self._store._db):
            row = self._store._db.execute(
                "SELECT state FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None or row[0] not in allowed_from:
                return False  # refused: nothing written, nothing audited
            self._store._db.execute(
                "UPDATE tasks SET state=?, updated=?, version=version+1 "
                "WHERE task_id=? AND state=?", (after, now, task_id, row[0]))
            self._store._db.execute(
                _INSERT, (task_id, action, row[0], after, decided_by, note, now))
        return True


def _require_note(note: str) -> None:
    if not isinstance(note, str):
        raise TypeError("note must be a str")
