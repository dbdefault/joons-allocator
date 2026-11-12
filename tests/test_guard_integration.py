import os
import subprocess
import sys
from dataclasses import dataclass

import pytest

guard = pytest.importorskip("joons_agent_guard")
from joons_allocator.integrations.guard import GuardHooks


@dataclass
class Task:
    task_id: str = "task"
    resource: str = "resource"


@pytest.fixture
def objects():
    circuit = guard.CircuitBreaker(":memory:")
    locks = guard.ResourceLock(":memory:")
    yield circuit, locks, GuardHooks(circuit, locks)
    circuit.close()
    locks.close()


def test_circuit_veto_and_half_open(objects):
    circuit, locks, hooks = objects
    assert circuit.begin("worker", "old")
    assert not hooks.before_lease("worker", Task())
    assert circuit.finish("worker", "old", success=False)
    assert hooks.before_lease("worker", Task())  # open admits one recovery attempt
    assert circuit.snapshot("worker").state == "half_open"
    assert not hooks.before_lease("worker", Task("other", "other"))


def test_lock_veto_rolls_back_circuit(objects):
    circuit, locks, hooks = objects
    assert locks.acquire("resource", "other", "other-task")
    assert not hooks.before_lease("worker", Task())
    assert circuit.snapshot("worker").pending_job is None
    assert locks.status("resource").owner == "other"


@pytest.mark.parametrize("success", [True, False])
def test_settle_and_repeat(objects, success):
    circuit, locks, hooks = objects
    task = Task()
    assert hooks.before_lease("worker", task)
    assert hooks.before_lease("worker", task)
    hooks.after_lease("worker", task)
    hooks.after_settle("worker", task, success)
    hooks.after_settle("worker", task, success)
    assert locks.status(task.resource) is None
    state = circuit.snapshot("worker")
    assert state.pending_job is None
    assert state.state == ("closed" if success else "open")
    assert state.consecutive_failures == (0 if success else 1)


def test_recover_and_repeat(objects):
    circuit, locks, hooks = objects
    task = Task()
    assert hooks.before_lease("worker", task)
    hooks.after_recover("worker", task)
    hooks.after_recover("worker", task)
    assert locks.status(task.resource) is None
    assert circuit.snapshot("worker").pending_job is None


def test_errors_are_swallowed_and_cleanup_continues(objects, caplog):
    circuit, locks, _ = objects
    class Broken:
        def __getattr__(self, name):
            def fail(*args, **kwargs):
                raise RuntimeError("broken")
            return fail
    task = Task()
    assert not GuardHooks(Broken(), locks).before_lease("worker", task)
    hooks = GuardHooks(circuit, locks)
    assert hooks.before_lease("worker", task)
    GuardHooks(Broken(), locks).after_settle("worker", task, False)
    assert locks.status(task.resource) is None
    assert locks.acquire(task.resource, "worker", task.task_id)
    GuardHooks(circuit, Broken()).after_recover("worker", task)
    assert circuit.snapshot("worker").pending_job is None
    assert "Guard" in caplog.text


def test_import_is_lazy():
    code = "import sys; import joons_allocator.integrations.guard; assert 'joons_agent_guard' not in sys.modules"
    subprocess.run([sys.executable, "-B", "-c", code], check=True, env=os.environ.copy(),
                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))

