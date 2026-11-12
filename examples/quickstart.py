"""Submit and complete a task using the allocator facade."""
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator

with TemporaryDirectory() as directory:
    with Allocator(Path(directory) / "queue.sqlite") as allocator:
        submitted = allocator.submit({"message": "hello"}, task_id="greeting", request_id="request-1")
        duplicate = allocator.submit({"message": "hello"}, request_id="request-1")
        print(f"submit: {submitted.status}; repeat: {duplicate.status}")
        task = allocator.lease("worker-1")
        assert task and task.task_id == submitted.task_id
        assert allocator.complete("worker-1", task, "done")
        print(f"{task.task_id}: {allocator.get(task.task_id).state}")
