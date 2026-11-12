"""Observe priority, FIFO, resource exclusivity, and worker controls."""
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator

with TemporaryDirectory() as directory:
    with Allocator(Path(directory) / "queue.sqlite") as allocator:
        allocator.submit({}, task_id="low", priority=0)
        allocator.submit({}, task_id="first", priority=10, resource="shared")
        allocator.submit({}, task_id="second", priority=10, resource="shared")
        print("queue:", ", ".join(task.task_id for task in allocator.queue()))
        assert [task.task_id for task in allocator.queue()] == ["first", "second", "low"]
        first = allocator.lease("worker-1")
        assert first and first.task_id == "first"
        # The shared resource is busy, so another worker takes the lower priority task.
        low = allocator.lease("worker-2")
        assert low and low.task_id == "low"
        assert allocator.pause_worker("worker-2")
        assert allocator.complete("worker-2", low, "done")
        assert allocator.lease("worker-2") is None
        assert allocator.resume_worker("worker-2")
        assert allocator.lease("worker-2") is None  # resource still held
        print("shared resource: second waits; worker-2 paused and resumed")
        assert allocator.complete("worker-1", first, "done")
        second = allocator.lease("worker-2")
        assert second and second.task_id == "second"
        assert allocator.complete("worker-2", second, "done")
        print("shared resource: first, then second")
