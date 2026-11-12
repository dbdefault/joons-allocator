"""Operator command line for joons-allocator.

``python -m joons_allocator --db PATH [--json] COMMAND`` submits tasks,
inspects the queue, and runs the audited operator decisions:

Read-only commands — plain ``SELECT`` statements on a connection opened with
the sqlite URI flag ``mode=ro``: they never create tables, never change state,
and treat a missing table as empty:

* ``list [--state S]``  - list tasks, oldest first
* ``queue``             - queued tasks in lease order (highest priority, then
                          oldest created)
* ``show TASK``         - show one task
* ``history TASK``      - show one task's audit trail
* ``workers``           - list registered workers

Write commands — through the :class:`~joons_allocator.Allocator` facade on the
database named by ``--db``:

* ``submit --payload JSON [--priority N] [--resource R] [--request-id ID]``
  (no confirmation; the facade makes it idempotent per request id)
* ``approve | pause | resume | cancel TASK --by NAME [--note N] --yes``
* ``retry TASK --by NAME [--note N] --yes``
* ``recover TASK --worker W --lease-token TOKEN --by NAME [--note N] --yes``

The mutating decisions change state only with ``--yes``; without it they print
the plan (what would happen) and exit 2.

Output is plain text by default and JSON with ``--json``. Errors print a
one-line message without a stack trace. Exit codes: 0 ok, 1 not found /
nothing changed / decision refused, 2 bad usage (arguments, invalid payload
JSON, non-finite numbers, invalid identifiers) or confirmation required.
"""
import argparse
import json
import os
import pathlib
import sqlite3
from typing import List, Optional

from joons_allocator import Allocator
from joons_allocator.errors import AllocatorError, InvalidIdentifier

# States and the operator decision surface per ARCHITECTURE.md ("Task model"). The
# facade enforces the real transitions; these drive CLI validation, the plan
# text, and the "allowed from" messages.
STATES = ("awaiting_approval", "queued", "paused", "running",
          "succeeded", "failed", "cancelled")

DECISIONS = {
    "approve": (("awaiting_approval",), "queued"),
    "pause": (("queued",), "paused"),
    "resume": (("paused",), "queued"),
    "cancel": (("queued", "awaiting_approval", "paused"), "cancelled"),
}
RETRY_FROM = ("failed",)
RECOVER_FROM = ("running",)

_TASK_COLUMNS = ("task_id, state, payload, result, priority, resource, "
                 "worker_id, lease_token, created, updated, version")
_LIST_COLUMNS = "task_id, state, priority, resource, result, version"
_AUDIT_COLUMNS = ("id, task_id, action, before_state, after_state, decided_by, "
                  "note, ts")
_WORKER_COLUMNS = ("worker_id, registered, last_seen, current_task, "
                   "consecutive_failures, paused")

_PAST = {
    "approve": "approved", "pause": "paused", "resume": "resumed",
    "cancel": "cancelled", "retry": "retried", "recover": "recovered",
}


# ---------------------------------------------------------------------------
# small output / formatting helpers
# ---------------------------------------------------------------------------

def _print(opts, lines, payload) -> None:
    """Emit the human-readable lines, or the JSON payload, on stdout."""
    if opts.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for line in lines:
            print(line)


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    """Two-space-aligned text columns; the last column is not padded."""
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = ["  ".join(h.ljust(w) for h, w in zip(headers, widths)).rstrip()]
    for row in rows:
        lines.append("  ".join(c.ljust(w) for c, w in zip(row, widths)).rstrip())
    return lines


def _require_db(opts) -> bool:
    """Refuse to run against a path that does not exist (exit 1, report it)."""
    if os.path.isfile(opts.db):
        return True
    _print(opts, ["database not found: %s" % opts.db],
           {"error": "database not found", "db": str(opts.db)})
    return False


def _connect_read(path):
    """Open the database strictly read-only (sqlite URI mode=ro).

    A read command therefore never performs hot-journal rollback recovery and
    never rewrites a database file left mid-transaction by a crashed process:
    it fails with an error instead of mutating the evidence.
    """
    uri = pathlib.Path(os.fspath(path)).resolve().as_uri() + "?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=10.0, isolation_level=None)
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def _select(conn, sql, params=()):
    try:
        return conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return []  # the component was never used in this database
        raise


def _select_one(conn, sql, params=()):
    rows = _select(conn, sql, params)
    return rows[0] if rows else None


def _current_state(task_id, conn):
    row = _select_one(conn, "SELECT state FROM tasks WHERE task_id=?", (task_id,))
    return row[0] if row else None


def _task_dict(row) -> dict:
    """One tasks row (SPEC column order) as a JSON-ready dict."""
    (task_id, state, payload, result, priority, resource, worker_id,
     lease_token, created, updated, version) = row
    return {"task_id": task_id, "state": state, "payload": json.loads(payload),
            "result": result, "priority": priority, "resource": resource,
            "worker_id": worker_id, "lease_token": lease_token,
            "created": created, "updated": updated, "version": version}


# ---------------------------------------------------------------------------
# read-only commands
# ---------------------------------------------------------------------------

def _cmd_list(opts) -> int:
    if not _require_db(opts):
        return 1
    conn = _connect_read(opts.db)
    try:
        sql, params = "SELECT %s FROM tasks" % _LIST_COLUMNS, ()
        if opts.state is not None:
            sql += " WHERE state=?"
            params = (opts.state,)
        rows = _select(conn, sql + " ORDER BY created, task_id", params)
    finally:
        conn.close()
    if opts.json:
        _print(opts, [], {"tasks": [{"task_id": r[0], "state": r[1],
                                     "priority": r[2], "resource": r[3],
                                     "result": r[4], "version": r[5]}
                                    for r in rows]})
        return 0
    if not rows:
        _print(opts, ["no tasks"], {})
        return 0
    lines = _table(["TASK_ID", "STATE", "PRIORITY", "RESOURCE", "VERSION", "RESULT"],
                   [[r[0], r[1], str(r[2]), r[3] if r[3] is not None else "-",
                     str(r[5]), r[4] if r[4] is not None else "-"] for r in rows])
    _print(opts, lines, {})
    return 0


def _cmd_queue(opts) -> int:
    if not _require_db(opts):
        return 1
    conn = _connect_read(opts.db)
    try:
        rows = _select(conn,
                       "SELECT %s FROM tasks WHERE state='queued' "
                       "ORDER BY priority DESC, created, task_id" % _LIST_COLUMNS)
    finally:
        conn.close()
    if opts.json:
        _print(opts, [], {"queue": [{"task_id": r[0], "state": r[1],
                                     "priority": r[2], "resource": r[3],
                                     "result": r[4], "version": r[5]}
                                    for r in rows]})
        return 0
    if not rows:
        _print(opts, ["no queued tasks"], {})
        return 0
    lines = _table(["TASK_ID", "STATE", "PRIORITY", "RESOURCE", "VERSION", "RESULT"],
                   [[r[0], r[1], str(r[2]), r[3] if r[3] is not None else "-",
                     str(r[5]), r[4] if r[4] is not None else "-"] for r in rows])
    _print(opts, lines, {})
    return 0


def _cmd_show(opts) -> int:
    if not _require_db(opts):
        return 1
    conn = _connect_read(opts.db)
    try:
        row = _select_one(conn, "SELECT %s FROM tasks WHERE task_id=?" % _TASK_COLUMNS,
                          (opts.task,))
    finally:
        conn.close()
    if row is None:
        _print(opts, ["no such task: %s" % opts.task],
               {"error": "not found", "task_id": opts.task})
        return 1
    task = _task_dict(row)
    lines = [
        "task_id: %s" % task["task_id"],
        "state: %s" % task["state"],
        "version: %s" % task["version"],
        "priority: %s" % task["priority"],
        "resource: %s" % (task["resource"] if task["resource"] is not None else "-"),
        "payload: %s" % json.dumps(task["payload"], sort_keys=True),
        "result: %s" % (task["result"] if task["result"] is not None else "(none)"),
    ]
    if task["worker_id"] is not None:
        lines.append("worker: %s" % task["worker_id"])
    lines.append("lease_token: %s" % (task["lease_token"] or "-"))
    _print(opts, lines, {"task": task})
    return 0


def _cmd_history(opts) -> int:
    if not _require_db(opts):
        return 1
    conn = _connect_read(opts.db)
    try:
        task = _select_one(conn, "SELECT task_id FROM tasks WHERE task_id=?", (opts.task,))
        rows = _select(conn, "SELECT %s FROM task_audit WHERE task_id=? ORDER BY id"
                       % _AUDIT_COLUMNS, (opts.task,))
    finally:
        conn.close()
    if task is None:
        _print(opts, ["no such task: %s" % opts.task],
               {"error": "not found", "task_id": opts.task})
        return 1
    entries = [{"id": r[0], "task_id": r[1], "action": r[2], "before_state": r[3],
                "after_state": r[4], "decided_by": r[5], "note": r[6], "ts": r[7]}
               for r in rows]
    if opts.json:
        _print(opts, [], {"history": entries})
        return 0
    if not rows:
        _print(opts, ["no audit rows for %s" % opts.task], {})
        return 0
    lines = _table(["ID", "ACTION", "BEFORE", "AFTER", "BY", "NOTE"],
                   [[str(r[0]), r[2], r[3], r[4], r[5], r[6] if r[6] else "-"] for r in rows])
    _print(opts, lines, {})
    return 0


def _cmd_workers(opts) -> int:
    if not _require_db(opts):
        return 1
    conn = _connect_read(opts.db)
    try:
        rows = _select(conn, "SELECT w.worker_id, w.registered, w.last_seen, w.current_task, w.consecutive_failures, w.paused, t.lease_token FROM workers w LEFT JOIN tasks t ON t.task_id=w.current_task AND t.state='running' ORDER BY w.registered, w.worker_id")
    finally:
        conn.close()
    if opts.json:
        _print(opts, [], {"workers": [{"worker_id": r[0], "registered": r[1],
                                       "last_seen": r[2], "current_task": r[3],
                                       "consecutive_failures": r[4],
                                       "paused": bool(r[5]), "lease_token": r[6]} for r in rows]})
        return 0
    if not rows:
        _print(opts, ["no workers"], {})
        return 0
    lines = _table(["WORKER_ID", "REGISTERED", "LAST_SEEN", "CURRENT_TASK",
                    "FAILURES", "PAUSED", "LEASE_TOKEN"],
                   [[r[0], repr(r[1]), repr(r[2]),
                     r[3] if r[3] is not None else "-",
                     str(r[4]), "yes" if r[5] else "no", r[6] or "-"] for r in rows])
    _print(opts, lines, {})
    return 0


# ---------------------------------------------------------------------------
# write commands (submit needs no --yes; decisions are plan + exit 2 without it)
# ---------------------------------------------------------------------------

def _reject_non_finite(token: str) -> float:
    """json.loads hook: reject NaN/Infinity anywhere in a submitted payload."""
    raise ValueError("non-finite number %s is not allowed in the payload" % token)


def _cmd_submit(opts) -> int:
    if not _require_db(opts):
        return 1
    try:
        payload = json.loads(opts.payload, parse_constant=_reject_non_finite)
    except ValueError as exc:
        _print(opts, ["error: invalid --payload JSON: %s" % exc],
               {"error": "invalid payload", "detail": str(exc)})
        return 2
    if not isinstance(payload, dict):
        _print(opts, ["error: --payload must be a JSON object"],
               {"error": "invalid payload"})
        return 2
    allocator = Allocator(opts.db)
    try:
        result = allocator.submit(payload, priority=opts.priority,
                                  resource=opts.resource,
                                  request_id=opts.request_id)
    finally:
        allocator.close()
    if result.status == "conflict":
        request = opts.request_id if opts.request_id is not None else "-"
        _print(opts, ["submit conflict on request id '%s': a different payload or options were "
                      "already admitted under it; nothing written" % request],
               {"status": result.status, "task_id": None})
        return 1
    _print(opts, ["submitted task '%s'; status %s" % (result.task_id, result.status)],
           {"task_id": result.task_id, "status": result.status})
    return 0


def _plan(opts, action, allowed_from, after, actor) -> int:
    conn = _connect_read(opts.db)
    try:
        current = _current_state(opts.task, conn)
    finally:
        conn.close()
    would = current in allowed_from
    if current is None:
        line = "no such task: '%s'; nothing changed; re-run with --yes to apply" % opts.task
    elif would:
        detail = "" if action != "recover" else ", cleaning up worker '%s'" % actor
        note_text = getattr(opts, "note", "")
        note = "" if not note_text else " (note: '%s')" % note_text
        line = ("would %s task '%s' (%s -> %s)%s by '%s'%s; nothing changed; "
                "re-run with --yes to apply"
                % (action, opts.task, current, after, detail, actor, note))
    else:
        line = ("would not apply %s to task '%s': state is '%s', allowed from: %s; "
                "nothing changed; re-run with --yes to apply"
                % (action, opts.task, current, ", ".join(allowed_from)))
    _print(opts, [line], {"task_id": opts.task, "action": action, "applied": False,
                          "would_apply": would, "current_state": current})
    return 2


def _mutate(opts, action, allowed_from, after, decided_by, note, worker=None) -> int:
    if not _require_db(opts):
        return 1
    if not opts.yes:
        return _plan(opts, action, allowed_from, after,
                     worker if action == "recover" else decided_by)
    allocator = Allocator(opts.db)
    try:
        if action == "retry":
            applied = allocator.retry(opts.task, decided_by, note)
        elif action == "recover":
            applied = allocator.recover(opts.task, worker, decided_by, note, lease_token=opts.lease_token)
        else:
            applied = getattr(allocator, action)(opts.task, decided_by, note)
    finally:
        allocator.close()
    if applied:
        _print(opts, ["%s task '%s'; state is now '%s'" % (_PAST[action], opts.task, after)],
               {"task_id": opts.task, "action": action, "applied": True, "state": after})
        return 0
    conn = _connect_read(opts.db)
    try:
        current = _current_state(opts.task, conn)
    finally:
        conn.close()
    if current is None:
        line = "no such task: '%s'; nothing changed" % opts.task
    else:
        line = ("cannot %s task '%s': state is '%s', allowed from: %s; nothing changed"
                % (action, opts.task, current, ", ".join(allowed_from)))
    _print(opts, [line], {"task_id": opts.task, "action": action, "applied": False,
                          "current_state": current})
    return 1


def _cmd_decide(opts) -> int:
    allowed_from, after = DECISIONS[opts.command]
    return _mutate(opts, opts.command, allowed_from, after,
                   opts.decided_by, opts.note)


def _cmd_retry(opts) -> int:
    return _mutate(opts, "retry", RETRY_FROM, "queued", opts.decided_by, opts.note)


def _cmd_recover(opts) -> int:
    return _mutate(opts, "recover", RECOVER_FROM, "failed",
                   opts.decided_by, opts.note, worker=opts.worker)


# ---------------------------------------------------------------------------
# argument parsing
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="joons_allocator",
        usage="%(prog)s --db PATH [--json] COMMAND ...",
        description="Operator tool for joons-allocator: submit tasks, inspect tasks, "
                    "workers and audit trails, and run the audited operator decisions "
                    "(approve, pause, resume, cancel, retry, recover).",
        epilog="exit codes: 0 ok, 1 not found / nothing changed / decision refused, "
               "2 bad usage or confirmation required",
    )
    parser.add_argument("--db", default=None, metavar="PATH",
                        help="path to the allocator's SQLite database file (required)")
    parser.add_argument("--json", action="store_true", default=False,
                        help="emit machine-readable JSON instead of plain text")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND", required=True)

    def _common(p):
        # accept the global flags after the command as well; hidden from the
        # per-command help because the top-level -h documents them
        p.add_argument("--db", default=argparse.SUPPRESS, metavar="PATH", help=argparse.SUPPRESS)
        p.add_argument("--json", action="store_true", default=argparse.SUPPRESS,
                       help=argparse.SUPPRESS)

    def _decision(p):
        p.add_argument("task", help="task id to act on")
        p.add_argument("--by", required=True, dest="decided_by", metavar="NAME",
                       help="operator name recorded in the audit trail")
        p.add_argument("--note", default="", metavar="N",
                       help="optional note recorded in the audit trail")
        p.add_argument("--yes", action="store_true",
                       help="actually apply the decision; without it only print the plan")
        _common(p)

    p = sub.add_parser("submit", help="admit one task (idempotent per request id)")
    p.add_argument("--payload", required=True, metavar="JSON",
                   help="task payload as a JSON object")
    p.add_argument("--priority", type=int, default=0, metavar="N",
                   help="scheduling priority; higher is picked first (default 0)")
    p.add_argument("--resource", default=None, metavar="NAME",
                   help="optional name of the exclusive resource the task needs")
    p.add_argument("--request-id", default=None, dest="request_id", metavar="ID",
                   help="client request id; resubmits with the same id and payload "
                        "return the original task")
    _common(p)
    p.set_defaults(func=_cmd_submit)

    p = sub.add_parser("list", help="list tasks, oldest first")
    p.add_argument("--state", default=None, choices=STATES,
                   help="only list tasks in this state")
    _common(p)
    p.set_defaults(func=_cmd_list)

    p = sub.add_parser("queue", help="list queued tasks in lease order")
    _common(p)
    p.set_defaults(func=_cmd_queue)

    p = sub.add_parser("show", help="show one task")
    p.add_argument("task", help="task id to inspect")
    _common(p)
    p.set_defaults(func=_cmd_show)

    p = sub.add_parser("history", help="show one task's audit trail")
    p.add_argument("task", help="task id to inspect")
    _common(p)
    p.set_defaults(func=_cmd_history)

    p = sub.add_parser("workers", help="list registered workers")
    _common(p)
    p.set_defaults(func=_cmd_workers)

    for name in ("approve", "pause", "resume", "cancel"):
        p = sub.add_parser(name, help="%s a task (audited)" % name)
        _decision(p)
        p.set_defaults(func=_cmd_decide)

    p = sub.add_parser("retry", help="re-queue a failed task (audited)")
    p.add_argument("task", help="task id to act on")
    p.add_argument("--by", required=True, dest="decided_by", metavar="NAME",
                   help="operator name recorded in the audit trail")
    p.add_argument("--note", default="", metavar="N",
                   help="optional note recorded in the audit trail")
    p.add_argument("--yes", action="store_true",
                   help="actually retry; without it only print the plan")
    _common(p)
    p.set_defaults(func=_cmd_retry)

    p = sub.add_parser("recover", help="recover a task whose worker died (audited)")
    p.add_argument("task", help="task id to act on")
    p.add_argument("--lease-token", required=True, help="expected lease token")
    p.add_argument("--worker", required=True, metavar="W",
                   help="worker id presumed dead (its slot and locks are cleaned up)")
    p.add_argument("--by", required=True, dest="decided_by", metavar="NAME",
                   help="operator name recorded in the audit trail")
    p.add_argument("--note", default="", metavar="N",
                   help="optional note recorded in the audit trail")
    p.add_argument("--yes", action="store_true",
                   help="actually recover; without it only print the plan")
    _common(p)
    p.set_defaults(func=_cmd_recover)
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Run one command; return the process exit code (0/1/2)."""
    parser = _build_parser()
    try:
        opts = parser.parse_args(argv)
        if not getattr(opts, "db", None):
            parser.error("the following arguments are required: --db")
    except SystemExit as exc:  # usage error or --help
        return exc.code if isinstance(exc.code, int) else 0
    try:
        return int(opts.func(opts))
    except InvalidIdentifier as exc:  # bad caller-supplied identifier: bad usage
        _print(opts, ["error: %s" % exc], {"error": str(exc)})
        return 2
    except (AllocatorError, ValueError, TypeError, sqlite3.Error) as exc:
        _print(opts, ["error: %s" % exc], {"error": str(exc)})
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
