"""Run a small fleet with the asyncio adapter."""
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator.aio import AsyncAllocator

async def main():
    with TemporaryDirectory() as directory:
        async with AsyncAllocator(Path(directory) / "queue.sqlite") as allocator:
            for number in range(4):
                await allocator.submit({"number": number}, task_id=f"task-{number}")

            async def worker(worker_id):
                completed = []
                while True:
                    task = await allocator.lease(worker_id)
                    if task is None:
                        return completed
                    assert await allocator.complete(worker_id, task, "done")
                    completed.append(task.task_id)

            results = await asyncio.gather(worker("worker-1"), worker("worker-2"))
            print(f"completed: {sum(map(len, results))} tasks")
            assert len(await allocator.list("succeeded")) == 4

asyncio.run(main())
