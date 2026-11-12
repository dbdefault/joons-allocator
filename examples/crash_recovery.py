"""Recover a task after its worker is confirmed lost."""
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator

with TemporaryDirectory() as directory:
    with Allocator(Path(directory) / "queue.sqlite") as allocator:
        allocator.submit({"work": "process"}, task_id="task-1")
        task = allocator.lease("worker-1")
        assert task
        print(f"{task.task_id}: {task.state} on worker-1")
        # Only recover after establishing that the worker cannot continue.
        assert allocator.recover(task.task_id, "worker-1", "operator", "worker lost", lease_token=task.lease_token)
        print(f"recovered: {allocator.get(task.task_id).state}")
        assert allocator.retry(task.task_id, "operator")
        task = allocator.lease("worker-2")
        assert task and allocator.complete("worker-2", task, "done")
        print(f"retried: {allocator.get(task.task_id).state}")
