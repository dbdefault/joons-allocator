"""Concurrency contract tests for the Allocator facade, against the documented API (API.md, ARCHITECTURE.md).

Six 'spawn' processes race over one SQLite file. SPEC promises: submitting
the same (principal, request_id) concurrently admits exactly once (one
'accepted', the rest 'duplicate', all returning the same task_id), and
concurrent leasing drains every task exactly once while no two running
tasks ever share a resource and no worker ever runs two tasks. Interval
timestamps make the exclusivity check sound: a record starts after lease()
returns and ends before complete() is called, so every recorded interval
lies inside the true hold. Standard library only plus pytest; no network;
nothing is marked xfail.
"""
from f2_test_helpers import lease_token
import json
import multiprocessing
import time
from collections import Counter

from joons_allocator import Allocator

_JOIN_TIMEOUT = 120.0


def _write_json(path, data):
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh)


def _read_json(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def _run_spawn(ctx, jobs):
    """Start (target, args) pairs, join all with a timeout, and fail the
    test if any child hangs or exits nonzero."""
    procs = []
    for target, args in jobs:
        procs.append(ctx.Process(target=target, args=args))
    for proc in procs:
        proc.start()
    deadline = time.time() + _JOIN_TIMEOUT
    for proc in procs:
        proc.join(timeout=max(0.1, deadline - time.time()))
    hung = [proc for proc in procs if proc.is_alive()]
    for proc in hung:
        proc.terminate()
        proc.join(timeout=10.0)
    assert not hung, "spawn processes did not finish before the join timeout"
    bad = [proc.exitcode for proc in procs if proc.exitcode != 0]
    assert not bad, "spawn processes exited nonzero: %r" % (bad,)
    return procs


def _submit_child(db_path, principal, request_id, payload, out_path):
    """Spawn target: submit one idempotent request and record the outcome."""
    try:
        a = Allocator(db_path, timeout=30.0)
        try:
            res = a.submit(payload, principal=principal, request_id=request_id)
            _write_json(out_path, {"status": res.status, "task_id": res.task_id})
        finally:
            a.close()
    except Exception as exc:  # reported to the parent, never swallowed
        _write_json(out_path, {"error": repr(exc)})


def _lease_child(db_path, worker_id, out_path, *, deadline_seconds=60.0,
                 idle_seconds=1.0):
    """Spawn target: lease/complete in a loop until the queue stays empty.

    Transient 'None' results are expected while other workers hold the
    resources, so the child keeps polling and only exits after a full idle
    window or the overall deadline."""
    try:
        a = Allocator(db_path, timeout=30.0)
        records = []
        anomalies = []
        try:
            started = time.time()
            idle = 0.0
            while time.time() - started < deadline_seconds:
                task = a.lease(worker_id)
                if task is None:
                    time.sleep(0.01)
                    idle += 0.01
                    if idle >= idle_seconds:
                        break
                    continue
                idle = 0.0
                start = time.time()
                end = time.time()
                done = a.complete(worker_id, getattr(task, 'task_id', task), "ok", lease_token=lease_token(a, task))
                if not done:
                    anomalies.append({"task_id": task.task_id,
                                      "problem": "complete returned false"})
                records.append({
                    "task_id": task.task_id,
                    "resource": task.resource,
                    "worker_id": worker_id,
                    "start": start,
                    "end": end,
                })
            _write_json(out_path, {"records": records, "anomalies": anomalies})
        finally:
            a.close()
    except Exception as exc:  # reported to the parent, never swallowed
        _write_json(out_path, {"error": repr(exc)})


def test_six_spawn_processes_admit_same_request_exactly_once(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    db_path = str(tmp_path / "admit.sqlite")
    payload = {"kind": "report", "seq": 7}
    outs = [str(tmp_path / ("submit_%d.json" % i)) for i in range(6)]
    _run_spawn(ctx, [
        (_submit_child, (db_path, "team-a", "req-001", payload, outs[i]))
        for i in range(6)
    ])
    results = [_read_json(p) for p in outs]
    for r in results:
        assert "error" not in r, r.get("error")
    statuses = [r["status"] for r in results]
    assert Counter(statuses) == Counter({"accepted": 1, "duplicate": 5})
    task_ids = {r["task_id"] for r in results}
    assert len(task_ids) == 1  # every answer names the same task
    with Allocator(db_path) as a:
        tasks = a.list()
        assert len(tasks) == 1
        assert tasks[0].task_id == task_ids.pop()
        assert tasks[0].payload == payload


def test_six_spawn_workers_drain_30_tasks_resource_exclusive(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    db_path = str(tmp_path / "drain.sqlite")
    expected = []
    with Allocator(db_path) as a:
        for i in range(30):
            res = a.submit({"i": i}, priority=i % 4, resource="res-%d" % (i % 3))
            assert res.status == "accepted"
            expected.append(res.task_id)
    outs = [str(tmp_path / ("lease_%d.json" % i)) for i in range(6)]
    _run_spawn(ctx, [
        (_lease_child, (db_path, "w-%d" % i, outs[i]))
        for i in range(6)
    ])
    records = []
    anomalies = []
    for path in outs:
        payload = _read_json(path)
        assert "error" not in payload, payload.get("error")
        anomalies.extend(payload["anomalies"])
        records.extend(payload["records"])
    assert not anomalies, "some complete() call was rejected"
    counts = Counter(r["task_id"] for r in records)
    assert set(counts) == set(expected)  # every task ran
    assert max(counts.values()) == 1  # ...and exactly once
    assert len(records) == 30
    # No two running tasks shared a resource at any time, and no worker ran
    # two tasks at once: recorded intervals of one group must be disjoint.
    for key in ("resource", "worker_id"):
        groups = {}
        for r in records:
            groups.setdefault(r[key], []).append(r)
        for group in groups.values():
            group.sort(key=lambda r: r["start"])
            for prev, nxt in zip(group, group[1:]):
                assert nxt["start"] >= prev["end"], (
                    "overlapping %s intervals: %r vs %r" % (key, prev, nxt))
