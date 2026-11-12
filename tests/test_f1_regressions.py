"""Regression tests for review probes."""
import asyncio
import sqlite3
import pytest
from joons_allocator import Allocator, TaskStore
from joons_allocator.aio import AsyncAllocator

def test_stale_lease_is_fenced():
    with Allocator(":memory:") as a:
        a.submit({}, task_id="t")
        old = a.lease("w")
        assert a.recover("t", "w", "op", lease_token=old.lease_token)
        assert a.retry("t", "op")
        new = a.lease("w")
        assert new.lease_token != old.lease_token
        assert not a.complete("w", old, "old")
        assert not a.fail("w", "t", "old", lease_token=old.lease_token)
        assert not a.recover("t", "w", "op", lease_token=old.lease_token)
        assert not a._dispatcher.abandon("t", "w", lease_token=old.lease_token)
        assert a.complete("w", new, "new")

def test_async_fencing():
    async def run():
        async with AsyncAllocator(":memory:") as a:
            await a.submit({}, task_id="t")
            old = await a.lease("w")
            assert await a.recover("t", "w", "op", lease_token=old.lease_token)
            assert await a.retry("t", "op")
            new = await a.lease("w")
            assert not await a.complete("w", old, "old")
            assert not await a.fail("w", "t", "old", lease_token=old.lease_token)
            assert await a.complete("w", new, "new")
    asyncio.run(run())

@pytest.mark.parametrize("decision", ["approve", "pause", "resume", "cancel", "retry", "recover"])
def test_audit_failure_rolls_back_decision_and_slot(decision):
    with Allocator(":memory:") as a:
        a.submit({}, task_id="t", require_approval=(decision == "approve"))
        if decision == "resume":
            assert a.pause("t", "op")
        if decision in ("retry", "recover"):
            task = a.lease("w")
            if decision == "retry":
                assert a.fail("w", task, "failure")
        before = a.get("t")
        workers = a.workers()
        history = a.history("t")
        a._store._db.execute("CREATE TRIGGER reject_audit BEFORE INSERT ON task_audit BEGIN SELECT RAISE(ABORT, 'audit failed'); END")
        with pytest.raises(sqlite3.IntegrityError):
            if decision == "recover":
                a.recover("t", "w", "op", lease_token=task.lease_token)
            else:
                getattr(a, decision)("t", "op")
        assert a.get("t") == before
        assert a.workers() == workers
        assert a.history("t") == history

def test_raw_store_transitions_are_private():
    with TaskStore(":memory:") as store:
        for name in ("begin_lease", "finish_lease", "fail_lease", "compare_and_set"):
            assert not hasattr(store, name)
            assert hasattr(store, "_" + name)

def test_request_fingerprint_includes_options():
    with Allocator(":memory:") as a:
        first = a.submit({"x": 1}, request_id="r")
        assert a.submit({"x": 1}, request_id="r").task_id == first.task_id
        for opts in ({"priority": 1}, {"resource": "gpu"}, {"require_approval": True}):
            assert a.submit({"x": 1}, request_id="r", **opts).status == "conflict"
        assert len(a.list()) == 1


def test_cli_recover_token(tmp_path):
    from joons_allocator.__main__ import main
    db = tmp_path / "queue.sqlite"
    with Allocator(db) as a:
        a.submit({}, task_id="t")
        old = a.lease("w")
        assert a.recover("t", "w", "op", lease_token=old.lease_token)
        assert a.retry("t", "op")
        new = a.lease("w")
        args = ["--db", str(db), "recover", "t", "--worker", "w", "--by", "op", "--yes", "--lease-token"]
        assert main(args + [old.lease_token]) == 1
        assert a.get("t").state == "running"
        assert main(args + [new.lease_token]) == 0
        assert a.get("t").state == "failed"

def test_recovery_hook_runs_after_commit():
    from joons_allocator import Dispatcher
    from joons_allocator.control import Control
    with TaskStore(":memory:") as store:
        class Hooks:
            def after_recover(self, worker, task):
                assert not store._db.in_transaction
                assert control.history(task.task_id)[0].action == "recover"
                observed.append(task.state)
        observed = []
        dispatcher = Dispatcher(store, hooks=Hooks())
        control = Control(store, dispatcher)
        store.insert("t", {})
        task = dispatcher.lease("w")
        assert control.recover("t", "w", "op", lease_token=task.lease_token)
        assert observed == ["failed"]
