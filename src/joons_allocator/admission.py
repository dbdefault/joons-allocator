"""Idempotent admission of tasks: the submissions ledger.

``Admission.submit(...)`` returns a :class:`SubmitResult` with one of three
statuses:

* ``accepted``  - the request is new: the task row AND the submissions row
  were written in ONE transaction; this pair happens exactly once.
* ``duplicate`` - the same (principal, request_id) was already admitted with
  the same request digest; the original task_id is returned again and
  nothing is written.
* ``conflict``  - the same (principal, request_id) was used with a different
  payload or scheduling options (a different digest); nothing is written.

There is no ``pending`` outcome and nothing to reconcile or abort: the
ledger row and the task row commit together or not at all, because both live
in the one SQLite file and one ``BEGIN IMMEDIATE`` transaction. This
replaces the prototype's cross-file ReplayGuard protocol (with its pending /
reconcile / abort machinery); see CHANGELOG.

The ledger does not authenticate anybody: ``principal`` must come from a
trusted transport, never from a client-supplied header.
"""
import hashlib
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, Optional

from . import _db
from .errors import require_ids
from .store import INSERT_SQL, TaskStore, _require_priority, canonical_json

_SUBMISSIONS_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS submissions ("
    "principal TEXT NOT NULL, request_id TEXT NOT NULL, digest TEXT NOT NULL, "
    "task_id TEXT NOT NULL, created REAL NOT NULL, "
    "PRIMARY KEY(principal, request_id))"
)


@dataclass(frozen=True)
class SubmitResult:
    """The verdict of one submit.

    ``status`` is ``accepted`` (new task created), ``duplicate`` (replay of
    the same request: ``task_id`` is the original id) or ``conflict`` (same
    request id with a different payload or scheduling options; ``task_id`` is None).
    """

    status: str
    task_id: Optional[str]


def payload_digest(payload: Any) -> str:
    """SHA-256 over the canonical JSON of the payload; a payload-only helper, separate from the submission digest."""
    return hashlib.sha256(canonical_json(payload).encode("ascii")).hexdigest()


class Admission:
    """At-most-once task admission through the one-file submissions ledger."""

    def __init__(self, store: TaskStore):
        self._store = store
        with _db.write_txn(store._db):
            store._db.execute(_SUBMISSIONS_SCHEMA)

    def submit(self, payload: Any, *, task_id: Optional[str] = None, priority: int = 0,
               resource: Optional[str] = None, principal: str = "default",
               request_id: Optional[str] = None,
               require_approval: bool = False) -> SubmitResult:
        """Admit one request exactly once; see the module docstring for statuses.

        ``task_id`` defaults to a fresh uuid4 hex. With ``request_id`` the
        submissions ledger decides the verdict; without one the submit is
        fire-and-forget (no idempotency promise) and a caller-provided
        task_id that already exists raises ValueError BEFORE anything is
        written. With ``require_approval=True`` the task is created in state
        ``awaiting_approval`` (workers never see it until an operator moves
        it along via ``joons_allocator.control.Control.approve``).

        Every argument is validated before the transaction, so a rejected
        submit never occupies its request_id.
        """
        require_ids(principal=principal)
        if request_id is not None:
            require_ids(request_id=request_id)
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")
        if task_id is None:
            task_id = uuid.uuid4().hex
        else:
            require_ids(task_id=task_id)
        _require_priority(priority)
        if resource is not None:
            require_ids(resource=resource)
        if not isinstance(require_approval, bool):
            raise TypeError("require_approval must be a bool")
        encoded = canonical_json(payload)  # strict-JSON check before anything is written
        digest = payload_digest({"payload": payload, "priority": priority, "resource": resource, "require_approval": require_approval})
        now = time.time()
        state = "awaiting_approval" if require_approval else "queued"
        with _db.write_txn(self._store._db):
            db = self._store._db
            if request_id is not None:
                row = db.execute(
                    "SELECT digest, task_id FROM submissions WHERE principal=? AND request_id=?",
                    (principal, request_id)).fetchone()
                if row is not None:
                    if row[0] == digest:
                        return SubmitResult("duplicate", row[1])
                    return SubmitResult("conflict", None)
            try:
                db.execute(INSERT_SQL, (task_id, state, encoded, priority, resource, now, now))
            except sqlite3.IntegrityError:
                raise ValueError("task %r already exists" % task_id) from None
            if request_id is not None:
                db.execute(
                    "INSERT INTO submissions (principal, request_id, digest, task_id, created) "
                    "VALUES (?, ?, ?, ?, ?)", (principal, request_id, digest, task_id, now))
        return SubmitResult("accepted", task_id)
