"""Contract tests for the Allocator facade, written against the documented API (API.md, ARCHITECTURE.md).

The joons_allocator package does not exist yet; every test here encodes a
documented promise: submit idempotency (accepted / duplicate / conflict),
auto task ids, priority DESC then FIFO lease order, resource exclusivity,
one running task per worker, worker pause/resume/heartbeat, approval gating,
wrong-worker guards, retry/cancel transitions, recover, ordered audit
history, durability across close/reopen, identifier and NaN validation.
Standard library only plus pytest; no network; nothing is marked xfail.
"""
from f2_test_helpers import lease_token
import re
import time

import pytest

from joons_allocator import (
    Allocator,
    AllocatorError,
    InvalidIdentifier,
    SubmitResult,
)

_HEX32 = re.compile(r"^[0-9a-f]{32}$")
_REJECTS = (AllocatorError, ValueError, TypeError)  # non-dict payload / non-str result are TypeError
_ACTION_WORDS = ("resume", "pause", "approve", "cancel", "retry", "recover")


def _db(tmp_path, name="alloc.sqlite"):
    return str(tmp_path / name)


def _submit(a, payload, *, step=0.05, **kwargs):
    """Pause briefly before each submit so 'created' strictly increases
    between submissions; the spec's FIFO tie-break needs distinct
    timestamps to be observable."""
    if step:
        time.sleep(step)
    return a.submit(payload, **kwargs)


def _attempt(op, *args, **kwargs):
    """Call op; a rejected call either returns a falsy result or raises an
    allocator error. Either way the caller observes something falsy."""
    try:
        return op(*args, **kwargs)
    except _REJECTS:
        return None


def _worker(info_list, worker_id):
    for info in info_list:
        if getattr(info, "worker_id", None) == worker_id:
            return info
    return None


def _event_action(event):
    """Best-effort action label of one audit event: prefer an explicit
    action-like field, then any field bag, then the event's repr."""
    fields = None
    bag = getattr(event, "__dict__", None)
    if isinstance(bag, dict):
        fields = dict(bag)
    if fields is None:
        asdict = getattr(event, "_asdict", None)
        if callable(asdict):
            try:
                fields = dict(asdict())
            except Exception:
                fields = None
    if fields is None and isinstance(event, dict):
        fields = dict(event)
    if fields:
        for key in ("action", "kind", "event", "type", "operation", "decision"):
            value = fields.get(key)
            if value is not None:
                text = str(value).lower()
                for word in _ACTION_WORDS:
                    if word in text:
                        return word
        text = " ".join(str(v) for v in fields.values()).lower()
    else:
        text = str(event).lower()
    for word in _ACTION_WORDS:
        if word in text:
            return word
    return None


def _actions(events):
    out = []
    for event in events:
        label = _event_action(event)
        if label is not None:
            out.append(label)
    return out


def test_submit_accepted_auto_id_and_roundtrip(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        r1 = _submit(a, {"k": [1, 2, {"deep": True}]})
        assert isinstance(r1, SubmitResult)
        assert r1.status == "accepted"
        assert _HEX32.fullmatch(r1.task_id)  # auto uuid4 hex
        r2 = _submit(a, {"other": 1})        # no request_id: new task each time
        assert r2.status == "accepted"
        assert r2.task_id != r1.task_id
        t = a.get(r1.task_id)
        assert t is not None
        assert t.state == "queued"
        assert t.payload == {"k": [1, 2, {"deep": True}]}
        assert t.priority == 0
        assert t.resource is None
        assert t.worker_id is None


def test_duplicate_same_principal_request_payload(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        first = _submit(a, {"n": 1}, principal="team-a", request_id="req-1")
        assert first.status == "accepted"
        dup = _submit(a, {"n": 1}, principal="team-a", request_id="req-1")
        assert dup.status == "duplicate"
        assert dup.task_id == first.task_id
        # the key is the (principal, request_id) pair
        other = _submit(a, {"n": 1}, principal="team-b", request_id="req-1")
        assert other.status == "accepted"
        assert other.task_id != first.task_id
        assert {t.task_id for t in a.list()} == {first.task_id, other.task_id}


def test_conflict_different_payload_writes_nothing(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        first = _submit(a, {"n": 1}, principal="team-a", request_id="req-1")
        assert first.status == "accepted"
        conflict = _submit(a, {"n": 999}, principal="team-a", request_id="req-1")
        assert conflict.status == "conflict"
        tasks = a.list()
        assert [t.task_id for t in tasks] == [first.task_id]  # nothing written
        t = a.get(first.task_id)
        assert t.payload == {"n": 1}
        assert t.state == "queued"
        g = a.lease("w1")
        assert g.task_id == first.task_id  # original task untouched, leasable


def test_queue_and_lease_order_priority_desc_then_fifo(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        ids = {}
        for name, priority in (("p0a", 0), ("p5a", 5), ("p0b", 0),
                               ("p10", 10), ("p5b", 5)):
            r = _submit(a, {"name": name}, priority=priority)
            assert r.status == "accepted"
            ids[name] = r.task_id
        expected = [ids["p10"], ids["p5a"], ids["p5b"], ids["p0a"], ids["p0b"]]
        assert [t.task_id for t in a.queue()] == expected
        worker = "w-order"
        for tid in expected:
            g = a.lease(worker)
            assert g is not None and g.task_id == tid
            assert a.complete(worker, getattr(g, 'task_id', g), "ok", lease_token=lease_token(a, g))
        assert a.lease(worker) is None
        assert a.queue() == []


def test_lease_skips_resource_held_by_running_until_complete(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1}, resource="res-a")
        t2 = _submit(a, {"n": 2}, resource="res-a")
        t3 = _submit(a, {"n": 3})
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        g2 = a.lease("w2")
        assert g2.task_id == t3.task_id  # t2 skipped: resource held by running t1
        assert a.complete("w1", getattr(t1, 'task_id', t1), "ok", lease_token=lease_token(a, t1))
        assert a.complete("w2", getattr(t3, 'task_id', t3), "ok", lease_token=lease_token(a, t3))  # one running task per worker
        g3 = a.lease("w2")
        assert g3.task_id == t2.task_id  # leasable again after complete


def test_resource_freed_after_fail(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1}, resource="res-b")
        t2 = _submit(a, {"n": 2}, resource="res-b")
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        assert a.fail("w1", getattr(t1, 'task_id', t1), "boom", lease_token=lease_token(a, t1))
        assert a.get(t1.task_id).state == "failed"
        g2 = a.lease("w2")
        assert g2.task_id == t2.task_id  # leasable again after fail


def test_empty_queue_leases_none(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        assert a.queue() == []
        assert a.lease("w0") is None
        assert a.get("no-such-task") is None
        # lease auto-registers the worker even when there is nothing to hand out
        assert [w.worker_id for w in a.workers()] == ["w0"]
        assert a.workers()[0].current_task is None


def test_one_running_task_per_worker(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1})
        t2 = _submit(a, {"n": 2})
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        assert a.lease("w1") is None  # already running one task
        assert _worker(a.workers(), "w1").current_task == t1.task_id
        assert a.complete("w1", getattr(t1, 'task_id', t1), "ok", lease_token=lease_token(a, t1))
        g2 = a.lease("w1")
        assert g2.task_id == t2.task_id  # slot freed by complete
        assert a.lease("w1") is None


def test_pause_worker_blocks_until_resume(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1})
        t2 = _submit(a, {"n": 2})
        t3 = _submit(a, {"n": 3})
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        assert a.pause_worker("w1")
        assert a.lease("w1") is None  # paused workers get nothing
        assert _worker(a.workers(), "w1").paused
        g2 = a.lease("w2")
        assert g2.task_id == t2.task_id  # other workers unaffected
        assert a.complete("w1", getattr(t1, 'task_id', t1), "ok", lease_token=lease_token(a, t1))  # a paused worker may still finish
        assert a.resume_worker("w1")
        g3 = a.lease("w1")
        assert g3.task_id == t3.task_id
        assert not _worker(a.workers(), "w1").paused


def test_heartbeat_updates_last_seen(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1})
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        before = _worker(a.workers(), "w1").last_seen
        time.sleep(0.05)
        assert a.heartbeat("w1")
        after = _worker(a.workers(), "w1").last_seen
        assert after > before
        assert _worker(a.workers(), "w1").current_task == t1.task_id


def test_workers_lists_auto_registered(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        assert a.workers() == []
        t1 = _submit(a, {"n": 1})
        t2 = _submit(a, {"n": 2})
        g1 = a.lease("w-alpha")
        assert g1.task_id == t1.task_id
        g2 = a.lease("w-beta")
        assert g2.task_id == t2.task_id
        assert {w.worker_id for w in a.workers()} == {"w-alpha", "w-beta"}


def test_complete_fail_wrong_worker_returns_false(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1})
        t2 = _submit(a, {"n": 2})
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        assert a.complete("w2", getattr(t1, 'task_id', t1), "sneaky", lease_token=lease_token(a, t1)) is False
        assert a.fail("w2", getattr(t1, 'task_id', t1), "sneaky", lease_token=lease_token(a, t1)) is False
        assert a.complete("w1", "no-such-task", "x", lease_token=lease_token(a, "no-such-task")) is False
        assert a.fail("w1", "no-such-task", "x", lease_token=lease_token(a, "no-such-task")) is False
        t = a.get(t1.task_id)
        assert t.state == "running"
        assert t.worker_id == "w1"
        assert t.result is None
        assert a.complete("w1", getattr(t1, 'task_id', t1), "ok", lease_token=lease_token(a, t1))
        t = a.get(t1.task_id)
        assert t.state == "succeeded"
        assert t.result == "ok"
        assert a.get(t2.task_id).state == "queued"  # untouched


def test_fail_then_retry_requeues(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1})
        t2 = _submit(a, {"n": 2})
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        assert a.fail("w1", getattr(t1, 'task_id', t1), "boom", lease_token=lease_token(a, t1))
        assert a.get(t1.task_id).state == "failed"
        assert t1.task_id not in [x.task_id for x in a.queue()]
        assert a.lease("w1").task_id == t2.task_id  # failed task not leasable
        a.retry(t1.task_id, "ops")
        assert a.get(t1.task_id).state == "queued"
        assert t1.task_id in [x.task_id for x in a.queue()]
        g3 = a.lease("w2")
        assert g3.task_id == t1.task_id  # retried task leasable again


def test_retry_only_from_failed(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        tq = _submit(a, {"n": "queued"})                 # priority 0, never leased
        tr = _submit(a, {"n": "running"}, priority=5)
        tc = _submit(a, {"n": "cancelled"})
        ts = _submit(a, {"n": "succeeded"}, priority=4)
        g1 = a.lease("w1")
        assert g1.task_id == tr.task_id
        g2 = a.lease("w2")
        assert g2.task_id == ts.task_id
        assert a.complete("w2", getattr(ts, 'task_id', ts), "done", lease_token=lease_token(a, ts))
        a.cancel(tc.task_id, "ops")
        for tid, state in ((tq.task_id, "queued"), (tr.task_id, "running"),
                           (tc.task_id, "cancelled"), (ts.task_id, "succeeded")):
            assert not _attempt(a.retry, tid, "ops")
            assert a.get(tid).state == state


def test_cancel_only_from_queued_paused_awaiting(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        tq = _submit(a, {"n": "queued"})
        ta = _submit(a, {"n": "awaiting"}, require_approval=True)
        tp = _submit(a, {"n": "paused"})
        trn = _submit(a, {"n": "running"}, priority=9)
        tf = _submit(a, {"n": "failed"}, priority=8)
        tok = _submit(a, {"n": "succeeded"}, priority=7)
        g1 = a.lease("w1")
        assert g1.task_id == trn.task_id
        g2 = a.lease("w2")
        assert g2.task_id == tf.task_id
        g3 = a.lease("w3")
        assert g3.task_id == tok.task_id
        assert a.complete("w3", getattr(tok, 'task_id', tok), "done", lease_token=lease_token(a, tok))
        assert a.fail("w2", getattr(tf, 'task_id', tf), "boom", lease_token=lease_token(a, tf))
        a.pause(tp.task_id, "ops")
        assert a.get(tp.task_id).state == "paused"
        # allowed: queued, awaiting_approval, paused
        a.cancel(tq.task_id, "ops")
        assert a.get(tq.task_id).state == "cancelled"
        a.cancel(ta.task_id, "ops")
        assert a.get(ta.task_id).state == "cancelled"
        a.cancel(tp.task_id, "ops")
        assert a.get(tp.task_id).state == "cancelled"
        # rejected: running, failed, succeeded
        for tid, state in ((trn.task_id, "running"), (tf.task_id, "failed"),
                           (tok.task_id, "succeeded")):
            assert not _attempt(a.cancel, tid, "ops")
            assert a.get(tid).state == state
        assert a.queue() == []


def test_require_approval_not_leasable_until_approve(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        r = _submit(a, {"n": 1}, require_approval=True)
        assert r.status == "accepted"
        assert a.get(r.task_id).state == "awaiting_approval"
        assert a.queue() == []
        assert a.lease("w1") is None  # awaiting_approval tasks are not leasable
        a.approve(r.task_id, "ops")
        assert a.get(r.task_id).state == "queued"
        assert [t.task_id for t in a.queue()] == [r.task_id]
        g = a.lease("w1")
        assert g.task_id == r.task_id


def test_recover_wrong_worker_changes_nothing(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1}, resource="res-c")
        t2 = _submit(a, {"n": 2}, resource="res-c")
        g1 = a.lease("w1")
        assert g1.task_id == t1.task_id
        assert not _attempt(a.recover, t1.task_id, "w9", "ops", lease_token=g1.lease_token)
        t = a.get(t1.task_id)
        assert t.state == "running"
        assert t.worker_id == "w1"
        assert a.lease("w2") is None  # resource still held by the live lease
        assert a.complete("w1", getattr(t1, 'task_id', t1), "still-ours", lease_token=lease_token(a, t1))
        assert a.get(t1.task_id).state == "succeeded"


def test_recover_by_leasing_worker_frees_slot_and_resource(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        ta = _submit(a, {"n": 1}, resource="res-d")
        tb = _submit(a, {"n": 2}, resource="res-d")
        g1 = a.lease("w1")
        assert g1.task_id == ta.task_id
        a.recover(ta.task_id, "w1", "ops", lease_token=lease_token(a, ta.task_id))
        t = a.get(ta.task_id)
        assert t.state != "running"
        w1 = _worker(a.workers(), "w1")
        assert w1 is None or not w1.current_task  # worker slot freed
        assert not _attempt(a.recover, ta.task_id, "w1", "ops", lease_token=lease_token(a, ta.task_id))  # nothing left
        g2 = a.lease("w2")
        assert g2 is not None and g2.resource == "res-d"  # resource freed
        assert "recover" in _actions(a.history(ta.task_id))


def test_history_records_operator_decisions_in_order(tmp_path):
    with Allocator(_db(tmp_path, "hist.sqlite")) as a:
        x = _submit(a, {"n": 1}, require_approval=True)
        sizes = [len(a.history(x.task_id))]
        a.approve(x.task_id, "ops")
        sizes.append(len(a.history(x.task_id)))
        a.pause(x.task_id, "ops")
        sizes.append(len(a.history(x.task_id)))
        a.resume(x.task_id, "ops")
        sizes.append(len(a.history(x.task_id)))
        a.cancel(x.task_id, "ops")
        sizes.append(len(a.history(x.task_id)))
        assert all(nxt > prev for prev, nxt in zip(sizes, sizes[1:]))
        assert _actions(a.history(x.task_id)) == ["approve", "pause", "resume", "cancel"]

        y = _submit(a, {"n": 2})
        g = a.lease("w1")
        assert g.task_id == y.task_id
        assert a.fail("w1", getattr(y, 'task_id', y), "boom", lease_token=lease_token(a, y))
        a.retry(y.task_id, "ops")
        assert "retry" in _actions(a.history(y.task_id))

        z = _submit(a, {"n": 3})
        g_retry = a.lease("w1")
        assert g_retry.task_id == y.task_id  # a retried task keeps its FIFO place
        assert a.complete("w1", getattr(y, 'task_id', y), "ok", lease_token=lease_token(a, y))
        g2 = a.lease("w1")
        assert g2.task_id == z.task_id
        a.recover(z.task_id, "w1", "ops", lease_token=lease_token(a, z.task_id))
        assert "recover" in _actions(a.history(z.task_id))


def test_state_survives_close_and_reopen(tmp_path):
    db = _db(tmp_path, "persist.sqlite")
    with Allocator(db) as a:
        q1 = _submit(a, {"n": 1}, priority=1)
        r1 = _submit(a, {"n": 2}, priority=9, resource="res-p")
        ap = _submit(a, {"n": 3}, require_approval=True)
        f1 = _submit(a, {"n": 4}, priority=7)
        c1 = _submit(a, {"n": 5})
        ok1 = _submit(a, {"n": 6}, priority=8)
        g1 = a.lease("w1")
        assert g1.task_id == r1.task_id
        g2 = a.lease("w2")
        assert g2.task_id == ok1.task_id
        assert a.complete("w2", getattr(ok1, 'task_id', ok1), "done", lease_token=lease_token(a, ok1))
        g3 = a.lease("w3")
        assert g3.task_id == f1.task_id
        assert a.fail("w3", getattr(f1, 'task_id', f1), "boom", lease_token=lease_token(a, f1))
        a.cancel(c1.task_id, "ops")
        assert len(a.list()) == 6
    with Allocator(db) as b:
        expected = {
            q1.task_id: "queued",
            r1.task_id: "running",
            ap.task_id: "awaiting_approval",
            f1.task_id: "failed",
            c1.task_id: "cancelled",
            ok1.task_id: "succeeded",
        }
        for tid, state in expected.items():
            t = b.get(tid)
            assert t is not None
            assert t.state == state
        assert [t.task_id for t in b.queue()] == [q1.task_id]
        assert {w.worker_id for w in b.workers()} == {"w1", "w2", "w3"}
        w1 = _worker(b.workers(), "w1")
        assert w1.current_task == r1.task_id
        assert "cancel" in _actions(b.history(c1.task_id))
        # the reopened allocator keeps working on durable leases
        assert b.complete("w1", getattr(r1, 'task_id', r1), "late", lease_token=lease_token(b, r1))
        assert b.get(r1.task_id).state == "succeeded"
        g4 = b.lease("w4")
        assert g4.task_id == q1.task_id


def test_context_manager_closes_cleanly(tmp_path):
    db = _db(tmp_path, "ctx.sqlite")
    with Allocator(db) as a:
        r = _submit(a, {"ctx": True})
    with Allocator(db) as b:
        t = b.get(r.task_id)
        assert t is not None and t.state == "queued"
        assert b.lease("w1").task_id == r.task_id


def test_invalid_identifiers_raise(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t = _submit(a, {"n": 1})
        for bad in ("", 123, None):
            with pytest.raises(InvalidIdentifier):
                a.lease(bad)
        with pytest.raises(ValueError):  # InvalidIdentifier is a ValueError
            a.lease("")
        with pytest.raises(InvalidIdentifier):
            a.submit({"n": 2}, principal="")
        with pytest.raises(InvalidIdentifier):
            a.heartbeat("")
        with pytest.raises(InvalidIdentifier):
            a.pause_worker("")
        with pytest.raises(InvalidIdentifier):
            a.complete("", getattr(t, 'task_id', t), "r", lease_token=lease_token(a, t))
        with pytest.raises(InvalidIdentifier):
            a.retry(t.task_id, "")
        assert issubclass(InvalidIdentifier, AllocatorError)
        assert issubclass(InvalidIdentifier, ValueError)
        assert a.get(t.task_id).state == "queued"  # nothing above mutated state


def test_identifier_length_boundary(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        assert a.lease("w" * 1024) is None  # exactly at the limit is valid
        with pytest.raises(InvalidIdentifier):
            a.lease("w" * 1025)
        r = a.submit({"n": 1}, task_id="t" * 1024)
        assert r.status == "accepted"
        assert a.get("t" * 1024) is not None
        with pytest.raises(InvalidIdentifier):
            a.submit({"n": 2}, task_id="t" * 1025)


def test_nan_payload_rejected(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        with pytest.raises(_REJECTS):
            a.submit({"x": float("nan")})
        with pytest.raises(_REJECTS):
            a.submit({"x": float("inf")})
        with pytest.raises(_REJECTS):
            a.submit(float("nan"))
        assert a.list() == []  # nothing written


def test_nan_result_rejected(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        r = _submit(a, {"n": 1})
        g = a.lease("w1")
        assert g.task_id == r.task_id
        rejected = False
        try:
            out = a.complete("w1", getattr(r, 'task_id', r), float("nan"), lease_token=lease_token(a, r))
            rejected = out is False
        except _REJECTS:
            rejected = True
        assert rejected
        t = a.get(r.task_id)
        assert t.state == "running"
        assert t.result is None
        assert a.complete("w1", getattr(r, 'task_id', r), "fine", lease_token=lease_token(a, r))  # state intact afterwards
        assert a.get(r.task_id).result == "fine"


def test_list_state_filter(tmp_path):
    with Allocator(_db(tmp_path)) as a:
        t1 = _submit(a, {"n": 1})
        t2 = _submit(a, {"n": 2})
        t3 = _submit(a, {"n": 3}, require_approval=True)
        g = a.lease("w1")
        assert g.task_id == t1.task_id
        assert {t.task_id for t in a.list()} == {t1.task_id, t2.task_id, t3.task_id}
        assert [t.task_id for t in a.list(state="queued")] == [t2.task_id]
        assert [t.task_id for t in a.list(state="running")] == [t1.task_id]
        assert [t.task_id for t in a.list(state="awaiting_approval")] == [t3.task_id]
        assert a.list(state="cancelled") == []
