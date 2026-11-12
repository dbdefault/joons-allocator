"""Mandatory fencing and hook lookup regressions."""
import asyncio
import json
import pytest
import joons_allocator as package
from joons_allocator import Allocator, AsyncAllocator, Dispatcher, TaskStore
from joons_allocator.__main__ import main

def test_exports_are_complete_and_exist():
    classes = {"Allocator", "AllocatorError", "Admission", "AsyncAllocator", "AuditEvent", "Control", "Dispatcher", "InvalidIdentifier", "SubmitResult", "Task", "TaskStore", "WorkerInfo"}
    assert classes <= set(package.__all__)
    assert all(hasattr(package, name) for name in package.__all__)

def test_fencing_mandatory_and_rejects_old_generation():
    with Allocator(":memory:") as a:
        a.submit({}, task_id="t")
        old = a.lease("w")
        for method in (a.complete, a.fail, a._dispatcher.complete, a._dispatcher.fail):
            with pytest.raises(TypeError, match="lease_token"):
                method("w", "t", "x")
        for method, args in ((a.recover, ("t", "w", "op")), (a._control.recover, ("t", "w", "op")), (a._dispatcher.recover, ("t", "w"))):
            with pytest.raises(TypeError, match="lease_token"):
                method(*args)
        assert a.recover("t", "w", "op", lease_token=old.lease_token)
        assert a.retry("t", "op")
        current = a.lease("w")
        assert not a.complete("w", old, "old")
        assert not a.fail("w", "t", "old", lease_token=old.lease_token)
        assert not a.recover("t", "w", "op", lease_token=old.lease_token)
        assert a.complete("w", current, "ok")

def test_async_rejects_unfenced_calls():
    async def run():
        async with AsyncAllocator(":memory:") as a:
            await a.submit({}, task_id="t")
            task = await a.lease("w")
            for method in (a.complete, a.fail):
                with pytest.raises(TypeError, match="lease_token"):
                    await method("w", "t", "x")
            with pytest.raises(TypeError, match="lease_token"):
                await a.recover("t", "w", "op")
            assert await a.complete("w", task, "ok")
    asyncio.run(run())

@pytest.mark.parametrize("method", ["after_lease", "after_settle", "after_recover"])
def test_raising_hook_lookup_does_not_escape_after_commit(method):
    class Hooks:
        def __getattr__(self, name):
            if name == method:
                raise RuntimeError("hook lookup failed")
            raise AttributeError(name)
    with TaskStore(":memory:") as store:
        store.insert("t", {})
        dispatcher = Dispatcher(store, hooks=Hooks())
        task = dispatcher.lease("w")
        assert task and store.get("t").state == "running"
        if method == "after_recover":
            assert dispatcher.recover("t", "w", lease_token=task.lease_token)
            assert store.get("t").state == "failed"
        else:
            assert dispatcher.complete("w", task, "ok")
            assert store.get("t").state == "succeeded"

def test_before_hook_lookup_failure_vetoes():
    class Hooks:
        @property
        def before_lease(self):
            raise RuntimeError("broken")
    with TaskStore(":memory:") as store:
        store.insert("t", {})
        assert Dispatcher(store, hooks=Hooks()).lease("w") is None
        assert store.get("t").state == "queued"

def test_cli_shows_token_and_requires_it(tmp_path, capsys):
    db = tmp_path / "db.sqlite"
    with Allocator(db) as a:
        a.submit({}, task_id="t")
        task = a.lease("w")
        for cmd in (["show", "t"], ["workers"]):
            assert main(["--db", str(db), *cmd]) == 0
            assert task.lease_token in capsys.readouterr().out
            assert main(["--db", str(db), "--json", *cmd]) == 0
            assert task.lease_token in json.dumps(json.loads(capsys.readouterr().out))
        assert main(["--db", str(db), "recover", "t", "--worker", "w", "--by", "op", "--yes"]) == 2
        assert a.get("t").state == "running"
        assert main(["--db", str(db), "recover", "t", "--worker", "w", "--by", "op", "--yes", "--lease-token", task.lease_token]) == 0
