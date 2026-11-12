"""Durable task store: one owned SQLite table with compare-and-set transitions.

Guarantees and limits
---------------------
* One ``tasks`` table with a fixed column list; every write goes through
  this package's ``_db.write_txn`` (``BEGIN IMMEDIATE`` ... ``COMMIT``), so
  cross-process races lose cleanly instead of interleaving.
* ``compare_and_set`` moves a task only along the explicit transition table:

    queued --> cancelled | paused
    running --> succeeded | failed
    failed  --> queued     (only via ``retry()``, never via compare_and_set)
    awaiting_approval --> queued (approve) | cancelled
    paused --> queued (resume) | cancelled

  ``queued -> running`` is deliberately NOT in the table: a public
  compare_and_set cannot create a lease-less running zombie. The only path
  into ``running`` is :meth:`TaskStore._begin_lease`, which stamps the lease
  identity in the same statement (the inverse, the internal
  ``running -> queued`` of a cancelled in-flight async lease, lives on
  ``Dispatcher.abandon`` in joons_allocator.worker).
* Nothing expires by age; states change only through these methods.
* A TaskStore instance may be shared between threads (statements are
  serialized through the connection's re-entrant lock); across processes,
  SQLite's own file locking serializes writers.
* Caps: a stored ``result`` is at most 1,048,576 characters of JSON-encodable text, a
  failure ``reason`` at most 4096 characters; both are validated BEFORE any
  mutation. Payloads must be strict JSON (NaN/Infinity rejected).
* The schema also holds the ``workers`` table (leased by
  joons_allocator.worker.Dispatcher). The ``submissions`` ledger belongs to
  joons_allocator.admission.Admission, which creates it itself.
"""
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, List, Optional

from . import _db
from .errors import require_ids

STATES = ("queued", "running", "succeeded", "failed", "cancelled",
          "awaiting_approval", "paused")

# Explicit transition table. ``failed -> queued`` is deliberately absent: it
# exists only as TaskStore.retry(). ``queued -> running`` is deliberately
# absent too: the only path into ``running`` is begin_lease (which stamps the
# lease identity in the same statement), used by
# joons_allocator.worker.Dispatcher.lease. The operator states are driven
# through joons_allocator.control.Control, which writes the audit row in the
# same transaction as the state change.
TRANSITIONS = {
    "queued": frozenset({"cancelled", "paused"}),
    "running": frozenset({"succeeded", "failed"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
    "awaiting_approval": frozenset({"queued", "cancelled"}),
    "paused": frozenset({"queued", "cancelled"}),
}

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS tasks ("
    "task_id TEXT PRIMARY KEY, state TEXT NOT NULL, payload TEXT NOT NULL, "
    "result TEXT, priority INTEGER NOT NULL DEFAULT 0, resource TEXT, "
    "worker_id TEXT, lease_token TEXT, created REAL NOT NULL, "
    "updated REAL NOT NULL, version INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS workers ("
    "worker_id TEXT PRIMARY KEY, registered REAL NOT NULL, last_seen REAL NOT NULL, "
    "current_task TEXT, consecutive_failures INTEGER NOT NULL DEFAULT 0, "
    "paused INTEGER NOT NULL DEFAULT 0)",
)

# Shared with joons_allocator.admission so the admission insert writes exactly
# the same row shape as TaskStore.insert. A plain INSERT: a caller-provided
# duplicate task_id raises sqlite3.IntegrityError, which each caller turns
# into its own verdict (False for TaskStore.insert, ValueError for a submit).
INSERT_SQL = (
    "INSERT INTO tasks (task_id, state, payload, result, priority, resource, "
    "worker_id, lease_token, created, updated, version) "
    "VALUES (?, ?, ?, NULL, ?, ?, NULL, NULL, ?, ?, 1)"
)

_SELECT = ("SELECT task_id, state, payload, result, priority, resource, worker_id, "
           "lease_token, created, updated, version FROM tasks")

#: A stored result is capped at 1,048,576 characters of JSON-encodable text.
MAX_RESULT_CHARS = 1024 * 1024

#: A failure reason is capped at 4096 characters.
MAX_REASON_CHARS = 4096


def canonical_json(payload: Any) -> str:
    """Canonical JSON of a payload: sorted keys, no whitespace, ASCII.

    The same logical payload always produces the same string, so digests and
    stored rows stay comparable. Raises ValueError for NaN/Infinity (not
    JSON: RFC 8259 has no such tokens) and TypeError for non-str dict keys,
    in both cases with a message that names the offending value.
    """
    _require_str_keys(payload)
    try:
        return json.dumps(payload, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=True, allow_nan=False)
    except ValueError as exc:
        raise ValueError("payload is not strict JSON: %s" % exc) from None


def _require_str_keys(value: Any) -> None:
    """Reject non-str dict keys anywhere in the payload with a clear error.

    json.dumps(sort_keys=True) would fail on mixed-type keys with a bare
    ordering error ('<' not supported between 'int' and 'str'); this says
    what is wrong instead.
    """
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(
                    "payload dict keys must be str, got %s (%r)" % (type(key).__name__, key))
            _require_str_keys(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _require_str_keys(item)


def check_result(result: Optional[str]) -> Optional[str]:
    """Validate an optional stored result BEFORE any mutation.

    A result is a str (or None) of at most :data:`MAX_RESULT_CHARS` (1,048,576 characters)
    characters that must be JSON-encodable text — a str is always a legal
    JSON string EXCEPT one holding lone surrogates, which also cannot be
    stored as SQLite TEXT (UTF-8), so those are rejected. Raises
    TypeError/ValueError naming the problem; never truncates.
    """
    if result is None:
        return None
    if not isinstance(result, str):
        raise TypeError("result must be a str or None")
    if len(result) > MAX_RESULT_CHARS:
        raise ValueError("result must be at most %d characters, got %d"
                         % (MAX_RESULT_CHARS, len(result)))
    try:
        result.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("result is not JSON-encodable text: %s" % exc) from None
    return result


def check_reason(reason: str) -> str:
    """Validate a failure reason BEFORE any mutation: a str of at most
    :data:`MAX_REASON_CHARS` (4096) characters. Never truncates."""
    if not isinstance(reason, str):
        raise TypeError("reason must be a str")
    if len(reason) > MAX_REASON_CHARS:
        raise ValueError("reason must be at most %d characters, got %d"
                         % (MAX_REASON_CHARS, len(reason)))
    return reason


def _require_state(name: str, value: str) -> None:
    if not isinstance(value, str) or value not in STATES:
        raise ValueError("%s must be one of %s, got %r" % (name, ", ".join(STATES), value))


def _require_priority(priority: int) -> None:
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise TypeError("priority must be an int, got %r" % (priority,))


@dataclass(frozen=True)
class Task:
    """One immutable snapshot of a ``tasks`` row; ``payload`` is the parsed JSON.

    ``worker_id`` / ``lease_token`` identify the current lease: set by
    :meth:`TaskStore._begin_lease` (Dispatcher.lease) and cleared on every
    settle or transition out of ``running``; None for a task not running.
    ``resource`` is the optional exclusivity name: while the task is
    ``running``, no other task naming the same resource may be leased.
    """

    task_id: str
    state: str
    payload: Any
    result: Optional[str]
    priority: int
    resource: Optional[str]
    worker_id: Optional[str]
    lease_token: Optional[str]
    created: float
    updated: float
    version: int


def task_from_row(row) -> Task:
    """Build the immutable Task snapshot for one raw ``tasks`` row."""
    return Task(row[0], row[1], json.loads(row[2]), row[3], row[4], row[5],
                row[6], row[7], row[8], row[9], row[10])


class TaskStore:
    """Durable tasks + compare-and-set, shared by every process using the same DB file."""

    def __init__(self, path, *, timeout: float = 10.0):
        raw = os.fspath(path)
        self._path = raw if raw == ":memory:" else os.path.abspath(raw)
        self._db = _db.connect(self._path, timeout)
        with _db.write_txn(self._db):
            for stmt in _SCHEMA:
                self._db.execute(stmt)

    @property
    def path(self) -> str:
        """Absolute path of the SQLite file this store holds a connection to."""
        return self._path

    # -- core API ---------------------------------------------------------
    def insert(self, task_id: str, payload: Any, *, priority: int = 0,
               resource: Optional[str] = None) -> bool:
        """Create the task in state ``queued`` (version 1). False if the id exists.

        ``priority`` orders leasing (higher first, then oldest created);
        ``resource`` is the optional exclusivity name validated like any
        other identifier.
        """
        require_ids(task_id=task_id)
        _require_priority(priority)
        if resource is not None:
            require_ids(resource=resource)
        encoded = canonical_json(payload)
        now = time.time()
        with _db.write_txn(self._db):
            try:
                self._db.execute(INSERT_SQL, (task_id, "queued", encoded,
                                              priority, resource, now, now))
                inserted = True
            except sqlite3.IntegrityError:
                inserted = False
        return inserted

    def get(self, task_id: str) -> Optional[Task]:
        require_ids(task_id=task_id)
        row = _db.query_one(self._db, _SELECT + " WHERE task_id=?", (task_id,))
        return task_from_row(row) if row else None

    def _compare_and_set(self, task_id: str, expected_state: str, new_state: str, *,
                        expected_version: Optional[int] = None,
                        result: Optional[str] = None) -> bool:
        """Atomic compare-and-set of the state; False on any mismatch.

        The update happens only if the current state is ``expected_state``
        (and, when given, the ``version`` is ``expected_version``) and the
        move is in the transition table. Illegal transitions raise ValueError
        instead of returning False, so caller bugs are visible; a lost race
        (row not in ``expected_state`` / wrong version) returns False. A win
        bumps ``version`` and ``updated``, stores ``result`` when it is not
        None (an existing result is never cleared here), and clears the lease
        identity (a stale lease identity never survives a transition).
        """
        require_ids(task_id=task_id)
        _require_state("expected_state", expected_state)
        _require_state("new_state", new_state)
        if new_state not in TRANSITIONS[expected_state]:
            raise ValueError(
                "illegal transition %s -> %s; allowed from %s: %s"
                % (expected_state, new_state, expected_state,
                   ", ".join(sorted(TRANSITIONS[expected_state])) or "none"))
        check_result(result)
        if expected_version is not None and (
                isinstance(expected_version, bool) or not isinstance(expected_version, int)):
            raise TypeError("expected_version must be an int or None")
        sql = ("UPDATE tasks SET state=?, updated=?, version=version+1, "
               "result=COALESCE(?, result), worker_id=NULL, lease_token=NULL "
               "WHERE task_id=? AND state=?")
        params: List[Any] = [new_state, time.time(), result, task_id, expected_state]
        if expected_version is not None:
            sql += " AND version=?"
            params.append(expected_version)
        with _db.write_txn(self._db):
            cur = self._db.execute(sql, params)
        return cur.rowcount == 1

    # -- lease identity -----------------------------------------------------
    # The invariant: worker_id / lease_token are non-NULL exactly while the
    # task is 'running'. begin_lease stamps them with the claim, finish_lease /
    # fail_lease clear them with the settle, and any other compare_and_set out
    # of a state clears them too.

    def _begin_lease(self, task_id: str, worker_id: str, token: str) -> bool:
        """Claim one lease atomically: ``queued -> running`` + lease identity.

        This is the exactly-once gate of ``Dispatcher.lease``: a single
        guarded UPDATE stamps the leasing worker and a fresh lease token on
        the row while moving the state, so races lose cleanly (False) and
        every observer can see who holds the task.
        """
        require_ids(task_id=task_id, worker_id=worker_id, token=token)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "UPDATE tasks SET state='running', updated=?, version=version+1, "
                "worker_id=?, lease_token=? WHERE task_id=? AND state='queued'",
                (time.time(), worker_id, token, task_id))
        return cur.rowcount == 1

    def _finish_lease(self, task_id: str, worker_id: str, new_state: str,
                     result: Optional[str]) -> bool:
        """Settle a lease: ``running -> succeeded|failed`` for the leasing worker only.

        One guarded UPDATE: the move happens only while the row still names
        ``worker_id`` as the leasing worker (and is running), so a
        mismatched caller moves nothing. Clears the lease identity; stores
        ``result`` when it is not None (an existing result is never cleared).
        """
        require_ids(task_id=task_id, worker_id=worker_id)
        if new_state not in ("succeeded", "failed"):
            raise ValueError(
                "a lease finishes into 'succeeded' or 'failed', got %r" % (new_state,))
        check_result(result)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "UPDATE tasks SET state=?, updated=?, version=version+1, "
                "result=COALESCE(?, result), worker_id=NULL, lease_token=NULL "
                "WHERE task_id=? AND state='running' AND worker_id=?",
                (new_state, time.time(), result, task_id, worker_id))
        return cur.rowcount == 1

    def _fail_lease(self, task_id: str, worker_id: str, result: str) -> bool:
        """Recovery move: ``running -> failed`` for the exact lease on the row.

        One transaction: reads the lease identity, refuses unless the task is
        running and still leased by ``worker_id``, then compare-and-sets on
        the stored lease token (a concurrent re-lease would have replaced
        it). Clears the lease identity and stores ``result``. False means
        nothing was touched.
        """
        require_ids(task_id=task_id, worker_id=worker_id)
        check_result(result)
        with _db.write_txn(self._db):
            row = self._db.execute(
                "SELECT state, worker_id, lease_token FROM tasks WHERE task_id=?",
                (task_id,)).fetchone()
            if row is None or row[0] != "running" or row[1] != worker_id:
                return False
            cur = self._db.execute(
                "UPDATE tasks SET state='failed', updated=?, version=version+1, "
                "result=?, worker_id=NULL, lease_token=NULL "
                "WHERE task_id=? AND state='running' AND worker_id=? AND lease_token=?",
                (time.time(), result, task_id, worker_id, row[2]))
        return cur.rowcount == 1

    def retry(self, task_id: str) -> bool:
        """Recovery path: ``failed -> queued``, the only way to make that move.

        True if the task was ``failed``; the previous failure result is
        cleared and ``version`` is bumped. False for any other state.
        """
        require_ids(task_id=task_id)
        with _db.write_txn(self._db):
            cur = self._db.execute(
                "UPDATE tasks SET state='queued', updated=?, version=version+1, result=NULL "
                "WHERE task_id=? AND state='failed'", (time.time(), task_id))
        return cur.rowcount == 1

    def list(self, state: Optional[str] = None) -> List[Task]:
        """All tasks (oldest first), or only those in ``state``."""
        if state is not None:
            _require_state("state", state)
        sql, params = _SELECT, ()
        if state is not None:
            sql += " WHERE state=?"
            params = (state,)
        rows = _db.query_all(self._db, sql + " ORDER BY created, task_id", params)
        return [task_from_row(row) for row in rows]

    def queue(self) -> List[Task]:
        """Queued tasks in exactly the allocation order of ``Dispatcher.lease``:
        highest priority first, then oldest ``created`` (FIFO), ties broken
        by task_id so the order is deterministic even for equal timestamps."""
        rows = _db.query_all(
            self._db, _SELECT + " WHERE state='queued' "
            "ORDER BY priority DESC, created, task_id")
        return [task_from_row(row) for row in rows]

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        _db.close(self._db)

    def __enter__(self) -> "TaskStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
