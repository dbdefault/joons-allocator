## Outcomes

`Task` and `WorkerInfo` are shallow frozen snapshots. A Task's nested payload can be mutable; modifying it does not update the database. IDs and tokens must be nonempty strings of at most 1024 characters; invalid identifiers raise `InvalidIdentifier` (a `ValueError`). Invalid argument types and values raise `TypeError` or `ValueError`; database and serialization errors can propagate.

| Facade method | Success | Unknown ID, wrong state or ownership | Invalid input |
|---|---|---|---|
| `submit(payload, ...)` | `SubmitResult`: `accepted` or `duplicate` | Conflicting request: `conflict` with no task ID; duplicate explicit task ID without replay raises `ValueError` | Invalid IDs, non-dict/non-JSON payload, priority, resource or approval flag raise |
| `get(task_id)` | `Task` snapshot | Unknown: `None` | Invalid task ID raises |
| `list(state=None)` | List of task snapshots | No matches: `[]` | Invalid state raises `ValueError` |
| `queue()` | Queued task snapshots in allocation order | Empty: `[]` | No arguments |
| `history(task_id)` | List of audit snapshots | Unknown or no events: `[]` | Invalid task ID raises |
| `workers()` | List of worker snapshots | No workers: `[]` | No arguments |
| `lease(worker_id)` | Leased `Task` | `None` for no eligible task, paused or busy worker; unknown worker is registered | Invalid worker ID raises |
| `complete(worker_id, task_or_id, result, *, lease_token=...)` | `True` | Unknown task, wrong state, worker or token: `False` | ID without token raises `TypeError`; invalid ID, token or text raises |
| `fail(worker_id, task_or_id, reason, *, lease_token=...)` | `True` | Unknown task, wrong state, worker or token: `False` | ID without token raises `TypeError`; invalid ID, token or reason raises |
| `heartbeat(worker_id)` | `True` | Unknown worker: `False` | Invalid worker ID raises |
| `pause_worker(worker_id)`, `resume_worker(worker_id)` | `True` for a known worker | Unknown worker: `False` | Invalid worker ID raises |
| `approve`, `pause`, `resume`, `cancel`, `retry` | `True` when the decision applies | Unknown task or wrong state: `False` | Invalid IDs or note raise |
| `recover(task_id, expected_worker, decided_by, note='', *, lease_token)` | `True` | Unknown task, wrong state, worker or token: `False` | Missing token raises `TypeError`; invalid IDs, token or note raise |
| `close()` | `None` | Not applicable | Later operations may raise SQLite errors |

Settlement takes worker first; recovery takes task first. A `Task` supplied to settlement must carry its lease token. AsyncAllocator follows the same outcomes, except its methods are awaited and cancellation may occur while an operation is in flight.

> Submission payloads must be JSON-serializable dicts. Results are text strings (up to 1,048,576 characters); failure reasons are limited to 4096 characters. The idempotency fingerprint includes payload, priority, resource and require_approval; changing any causes a conflict. Existing ledgers from before this change require new request IDs.
>
> Pass the leased Task to complete/fail or pass its ID with `lease_token=task.lease_token`; recover requires that keyword and the CLI requires `--lease-token`. ID-only complete/fail calls raise `TypeError`.

﻿# JOONS Allocator API

The public package exports `__version__` ("0.1.0"), `STATES`, `TRANSITIONS` and the objects below. Identifiers must be nonempty strings of at most 1024 characters. Database paths accept path-like values; `timeout` is the SQLite lock timeout in seconds. Database operations may raise `sqlite3.Error`; closed connections may raise `sqlite3.ProgrammingError`. Invalid identifiers raise `InvalidIdentifier`; invalid values and types can raise `ValueError` and `TypeError`. Unless stated otherwise, boolean mutations return `True` when applied and `False` when the task, worker, state or ownership does not match. Examples use independent temporary databases.

## Allocator

`Allocator(path, *, timeout: float = 10.0)`

One durable task queue on one SQLite file.

`path` is the database location, exposed by the `path` property. Supports `with` and `close()`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator
with TemporaryDirectory() as d:
    with Allocator(Path(d) / "db.sqlite") as a:
        result = a.submit({"work": 1}, request_id="request")
        task = a.lease("worker")
        if task:
            a.complete("worker", task, "done")
        print(a.get(result.task_id))
```

### Allocator.approve

`approve(self, task_id: str, decided_by: str, note: str = '') -> bool`

``awaiting_approval -> queued``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.approve("task", "operator"))
    obj.close()
```

### Allocator.cancel

`cancel(self, task_id: str, decided_by: str, note: str = '') -> bool`

``queued | awaiting_approval | paused -> cancelled`` (never running).

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.cancel("task", "operator"))
    obj.close()
```

### Allocator.close

`close(self) -> None`

Close the owned connection.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.close())
```

### Allocator.complete

`complete(self, worker_id: str, task_id: str | Task, result: str, *, lease_token=None) -> bool`

Finish a leased task: ``running -> succeeded``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.complete("worker", "task", "done", lease_token="example-token"))
    obj.close()
```

### Allocator.fail

`fail(self, worker_id: str, task_id: str | Task, reason: str, *, lease_token=None) -> bool`

Finish a leased task with a failure: ``running -> failed``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.fail("worker", "task", "failed", lease_token="example-token"))
    obj.close()
```

### Allocator.get

`get(self, task_id: str) -> joons_allocator.store.Task | None`

The task snapshot, or None when the id is unknown.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a `Task` snapshot or `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.get("task"))
    obj.close()
```

### Allocator.heartbeat

`heartbeat(self, worker_id: str) -> bool`

Refresh the worker's ``last_seen``; False for an unknown worker.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.heartbeat("worker"))
    obj.close()
```

### Allocator.history

`history(self, task_id: str) -> List[joons_allocator.control.AuditEvent]`

All audit rows for one task, oldest first.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.history("task"))
    obj.close()
```

### Allocator.lease

`lease(self, worker_id: str) -> joons_allocator.store.Task | None`

Claim the best queued task for this worker, or None.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a `Task` snapshot or `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.lease("worker"))
    obj.close()
```

### Allocator.list

`list(self, state: str | None = None) -> List[joons_allocator.store.Task]`

All tasks (oldest first), or only those in ``state``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.list())
    obj.close()
```

### Allocator.pause

`pause(self, task_id: str, decided_by: str, note: str = '') -> bool`

``queued -> paused``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.pause("task", "operator"))
    obj.close()
```

### Allocator.pause_worker

`pause_worker(self, worker_id: str) -> bool`

Stop handing new leases to this worker.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.pause_worker("worker"))
    obj.close()
```

### Allocator.queue

`queue(self) -> List[joons_allocator.store.Task]`

Queued tasks in exactly the allocation order of ``lease``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.queue())
    obj.close()
```

### Allocator.recover

`recover(self, task_id: str, expected_worker: str, decided_by: str, note: str = '', *, lease_token) -> bool`

Recover a task from a worker presumed dead, with an audit row.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.recover("task", "worker", "operator", lease_token="example-token"))
    obj.close()
```

### Allocator.resume

`resume(self, task_id: str, decided_by: str, note: str = '') -> bool`

``paused -> queued``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.resume("task", "operator"))
    obj.close()
```

### Allocator.resume_worker

`resume_worker(self, worker_id: str) -> bool`

Hand leases to this worker again.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.resume_worker("worker"))
    obj.close()
```

### Allocator.retry

`retry(self, task_id: str, decided_by: str, note: str = '') -> bool`

``failed -> queued`` with an audit row.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.retry("task", "operator"))
    obj.close()
```

### Allocator.submit

`submit(self, payload: Any, *, task_id: str | None = None, priority: int = 0, resource: str | None = None, principal: str = 'default', request_id: str | None = None, require_approval: bool = False) -> joons_allocator.admission.SubmitResult`

Admit one task exactly once; see Admission.submit for the verdicts.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `SubmitResult`: `accepted`, `duplicate`, or `conflict` (with `task_id=None` on conflict). Duplicate explicit task IDs can raise `ValueError`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.submit({"work": 1}))
    obj.close()
```

### Allocator.workers

`workers(self) -> List[joons_allocator.worker.WorkerInfo]`

All registered workers, ordered by worker_id.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    obj = Allocator(Path(d) / "db.sqlite")
    obj.submit({"work": 1}, task_id="task")
    print(obj.workers())
    obj.close()
```

## AsyncAllocator

`AsyncAllocator(path, *, timeout: float = 10.0)`

Coroutine wrapper owning one sync Allocator stack on one path.

Methods corresponding to the facade are awaitable and have the same results and validation. `aclose()` waits for in-flight work; calls after close raise `RuntimeError`. Cancelled lease calls attempt to return any late lease to the queue; cancelled operations may still complete. `discarded_errors()` returns recorded late exceptions. Supports `async with`.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import AsyncAllocator
async def main():
    with TemporaryDirectory() as d:
        async with AsyncAllocator(Path(d) / "db.sqlite") as a:
            result = await a.submit({"work": 1})
            task = await a.lease("worker")
            if task:
                await a.complete("worker", task, "done")
            print(await a.get(result.task_id))
asyncio.run(main())
```

### AsyncAllocator.aclose

`aclose(self) -> None`

Close the owned synchronous Allocator. Idempotent.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `None`.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.aclose())
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.approve

`approve(self, task_id, decided_by, note='') -> bool`

``awaiting_approval -> queued``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.approve("task", "operator"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.cancel

`cancel(self, task_id, decided_by, note='') -> bool`

``queued | awaiting_approval | paused -> cancelled`` (never running).

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.cancel("task", "operator"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.complete

`complete(self, worker_id: str, task_id: str | Task, result: str, *, lease_token=None) -> bool`

Finish a leased task: ``running -> succeeded``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.complete("worker", "task", "done", lease_token="example-token"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.discarded_errors

`discarded_errors(self) -> tuple`

Worker-thread exceptions that reached no awaiter (caller cancelled, or the call landed during ``aclose``), oldest first, capped. Check this after catching ``CancelledError`` from a write whose outcome is unknown.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a tuple of recorded late errors.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(obj.discarded_errors())
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.fail

`fail(self, worker_id: str, task_id: str | Task, reason: str, *, lease_token=None) -> bool`

Finish a leased task with a failure: ``running -> failed``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.fail("worker", "task", "failed", lease_token="example-token"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.get

`get(self, task_id)`

The task snapshot, or None when the id is unknown.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a `Task` snapshot or `None`.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.get("task"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.heartbeat

`heartbeat(self, worker_id) -> bool`

Refresh the worker's ``last_seen``; False for an unknown worker.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.heartbeat("worker"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.history

`history(self, task_id)`

All audit rows for one task, oldest first.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.history("task"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.lease

`lease(self, worker_id: str)`

Claim the best queued task for this worker, or None.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a `Task` snapshot or `None`.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.lease("worker"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.list

`list(self, state=None)`

All tasks (oldest first), or only those in ``state``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.list())
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.pause

`pause(self, task_id, decided_by, note='') -> bool`

``queued -> paused``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.pause("task", "operator"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.pause_worker

`pause_worker(self, worker_id) -> bool`

Stop handing new leases to this worker.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.pause_worker("worker"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.queue

`queue(self)`

Queued tasks in exactly the allocation order of ``lease``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.queue())
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.recover

`recover(self, task_id, expected_worker, decided_by, note='', *, lease_token) -> bool`

Recover a task from a worker presumed dead, with an audit row.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.recover("task", "worker", "operator", lease_token="example-token"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.resume

`resume(self, task_id, decided_by, note='') -> bool`

``paused -> queued``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.resume("task", "operator"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.resume_worker

`resume_worker(self, worker_id) -> bool`

Hand leases to this worker again.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.resume_worker("worker"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.retry

`retry(self, task_id, decided_by, note='') -> bool`

``failed -> queued`` with an audit row.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.retry("task", "operator"))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.submit

`submit(self, payload, *, task_id=None, priority=0, resource=None, principal='default', request_id=None, require_approval=False)`

Admit one task exactly once; see Allocator.submit for the verdicts.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `SubmitResult`: `accepted`, `duplicate`, or `conflict` (with `task_id=None` on conflict). Duplicate explicit task IDs can raise `ValueError`.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.submit({"work": 1}))
        await obj.aclose()
asyncio.run(main())
```

### AsyncAllocator.workers

`workers(self)`

All registered workers, ordered by worker_id.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
async def main():
    with TemporaryDirectory() as d:
        obj = AsyncAllocator(Path(d) / "db.sqlite")
        await obj.submit({"work": 1}, task_id="task")
        print(await obj.workers())
        await obj.aclose()
asyncio.run(main())
```

## TaskStore

`TaskStore(path, *, timeout: float = 10.0)`

Durable tasks + compare-and-set, shared by every process using the same DB file.

`path` is the database location, exposed by the `path` property. Supports `with` and `close()`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import TaskStore
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        store.insert("task", {"work": 1})
        print(store.queue())
```

### TaskStore.close

`close(self) -> None`

See the corresponding facade operation.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = store
    print(obj.close())
```

### TaskStore.get

`get(self, task_id: str) -> joons_allocator.store.Task | None`

See the corresponding facade operation.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a `Task` snapshot or `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = store
    print(obj.get("task"))
    store.close()
```

### TaskStore.insert

`insert(self, task_id: str, payload: Any, *, priority: int = 0, resource: str | None = None) -> bool`

Create the task in state ``queued`` (version 1). False if the id exists.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = store
    print(obj.insert("task", {"work": 1}))
    store.close()
```

### TaskStore.list

`list(self, state: str | None = None) -> List[joons_allocator.store.Task]`

All tasks (oldest first), or only those in ``state``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = store
    print(obj.list())
    store.close()
```

### TaskStore.queue

`queue(self) -> List[joons_allocator.store.Task]`

Queued tasks in exactly the allocation order of ``Dispatcher.lease``: highest priority first, then oldest ``created`` (FIFO), ties broken by task_id so the order is deterministic even for equal timestamps.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = store
    print(obj.queue())
    store.close()
```

### TaskStore.retry

`retry(self, task_id: str) -> bool`

Recovery path: ``failed -> queued``, the only way to make that move.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = store
    print(obj.retry("task"))
    store.close()
```

## Admission

`Admission(store: joons_allocator.store.TaskStore)`

At-most-once task admission through the one-file submissions ledger.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import TaskStore, Admission
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        print(Admission(store).submit({"work": 1}, request_id="request"))
```

### Admission.submit

`submit(self, payload: Any, *, task_id: str | None = None, priority: int = 0, resource: str | None = None, principal: str = 'default', request_id: str | None = None, require_approval: bool = False) -> joons_allocator.admission.SubmitResult`

Admit one request exactly once; see the module docstring for statuses.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `SubmitResult`: `accepted`, `duplicate`, or `conflict` (with `task_id=None` on conflict). Duplicate explicit task IDs can raise `ValueError`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Admission(store)
    print(obj.submit({"work": 1}))
    store.close()
```

## Dispatcher

`Dispatcher(store: joons_allocator.store.TaskStore, *, hooks=None)`

Hands queued tasks to workers; one SQLite transaction per state change.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import TaskStore, Dispatcher
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        store.insert("task", {"work": 1})
        dispatcher = Dispatcher(store)
        task = dispatcher.lease("worker")
        if task:
            dispatcher.complete("worker", task, "done")
```

### Dispatcher.abandon

`abandon(self, task_id, worker_id: str, *, lease_token=None) -> bool`

Give a live lease back to the queue: ``running -> queued``. Like ``complete``,
it requires the leased ``Task`` (or ``lease_token=``); an id-only call raises
``TypeError`` and a stale token returns ``False`` without touching the newer lease.

Returns ``True`` when the task ended up ``queued`` again.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import TaskStore, Dispatcher
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    leased = obj.lease("worker")
    print(obj.abandon(leased, "worker"))
    store.close()
```

### Dispatcher.cancel

`cancel(self, task_id: str) -> bool`

``queued -> cancelled`` only; running tasks must be failed or recovered first.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.cancel("task"))
    store.close()
```

### Dispatcher.complete

`complete(self, worker_id: str, task_id: str | Task, result: str, *, lease_token=None) -> bool`

Finish a leased task: ``running -> succeeded`` and the slot cleared, in one transaction. Only the leasing worker may settle.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.complete("worker", "task", "done", lease_token="example-token"))
    store.close()
```

### Dispatcher.fail

`fail(self, worker_id: str, task_id: str | Task, reason: str, *, lease_token=None) -> bool`

Finish a leased task with a failure: ``running -> failed``, the reason stored, the slot cleared, ``consecutive_failures`` bumped — all in one transaction. Only the leasing worker may settle.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.fail("worker", "task", "failed", lease_token="example-token"))
    store.close()
```

### Dispatcher.heartbeat

`heartbeat(self, worker_id: str) -> bool`

Refresh ``last_seen`` for a registered worker; False for an unknown one (heartbeat never auto-registers: leasing does).

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.heartbeat("worker"))
    store.close()
```

### Dispatcher.lease

`lease(self, worker_id: str) -> joons_allocator.store.Task | None`

Claim the best queued task this worker may run now, or None.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a `Task` snapshot or `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.lease("worker"))
    store.close()
```

### Dispatcher.pause_worker

`pause_worker(self, worker_id: str) -> bool`

Stop handing new leases to this worker (its running task is unaffected). False for an unknown worker; never auto-registers.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.pause_worker("worker"))
    store.close()
```

### Dispatcher.recover

`recover(self, task_id: str, expected_worker: str, *, lease_token) -> bool`

Operator recovery for a worker presumed dead mid-task.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.recover("task", "worker", lease_token="example-token"))
    store.close()
```

### Dispatcher.resume_worker

`resume_worker(self, worker_id: str) -> bool`

Hand leases to this worker again. False for an unknown worker.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.resume_worker("worker"))
    store.close()
```

### Dispatcher.retry

`retry(self, task_id: str) -> bool`

``failed -> queued`` (version bump) so the task runs again.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.retry("task"))
    store.close()
```

### Dispatcher.workers

`workers(self) -> List[joons_allocator.worker.WorkerInfo]`

All registered workers, ordered by worker_id.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Dispatcher(store)
    print(obj.workers())
    store.close()
```

## Control

`Control(store: joons_allocator.store.TaskStore, dispatcher: joons_allocator.worker.Dispatcher)`

Explicit operator decisions over the queue, each with an audit trail.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        result = Admission(store).submit({"work": 1}, require_approval=True)
        control = Control(store, Dispatcher(store))
        control.approve(result.task_id, "operator")
        print(control.history(result.task_id))
```

### Control.approve

`approve(self, task_id: str, decided_by: str, note: str = '') -> bool`

``awaiting_approval -> queued`` so workers can lease the task.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Control(store, Dispatcher(store))
    print(obj.approve("task", "operator"))
    store.close()
```

### Control.cancel

`cancel(self, task_id: str, decided_by: str, note: str = '') -> bool`

``queued | awaiting_approval | paused -> cancelled`` (never running).

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Control(store, Dispatcher(store))
    print(obj.cancel("task", "operator"))
    store.close()
```

### Control.history

`history(self, task_id: str) -> List[joons_allocator.control.AuditEvent]`

All audit rows for one task, oldest (lowest id) first.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a list of matching snapshots or records, possibly empty.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Control(store, Dispatcher(store))
    print(obj.history("task"))
    store.close()
```

### Control.pause

`pause(self, task_id: str, decided_by: str, note: str = '') -> bool`

``queued -> paused``: the task stops being leasable.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Control(store, Dispatcher(store))
    print(obj.pause("task", "operator"))
    store.close()
```

### Control.recover

`recover(self, task_id: str, expected_worker: str, decided_by: str, note: str = '', *, lease_token) -> bool`

Recover a matching worker and lease token, atomically writing the audit row.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Control(store, Dispatcher(store))
    print(obj.recover("task", "worker", "operator", lease_token="example-token"))
    store.close()
```

### Control.resume

`resume(self, task_id: str, decided_by: str, note: str = '') -> bool`

``paused -> queued``.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Control(store, Dispatcher(store))
    print(obj.resume("task", "operator"))
    store.close()
```

### Control.retry

`retry(self, task_id: str, decided_by: str, note: str = '') -> bool`

``failed -> queued`` (version bump) with an audit row, atomically.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns a boolean indicating whether the operation applied.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
with TemporaryDirectory() as d:
    store = TaskStore(Path(d) / "db.sqlite")
    store.insert("task", {"work": 1})
    obj = Control(store, Dispatcher(store))
    print(obj.retry("task", "operator"))
    store.close()
```

## GuardHooks

`GuardHooks(circuit=None, locks=None)`



`circuit` and `locks` are optional guard objects. Construction requires the optional `joons_agent_guard` package and raises `ImportError` if absent. Hook exceptions are logged and swallowed; `before_lease` returns `False` on error.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import TaskStore, Dispatcher
from joons_allocator.integrations.guard import GuardHooks
try:
    from joons_agent_guard import CircuitBreaker, ResourceLock
except ImportError:
    print("Install the optional guard to run this example")
else:
    with TemporaryDirectory() as d:
        with TaskStore(Path(d) / "tasks.sqlite") as store, CircuitBreaker(Path(d) / "guard.sqlite") as circuit, ResourceLock(Path(d) / "guard.sqlite") as locks:
            dispatcher = Dispatcher(store, hooks=GuardHooks(circuit, locks))
            store.insert("task", {"work": 1}, resource="resource")
            task = dispatcher.lease("worker")
            if task:
                dispatcher.complete("worker", task, "done")
```

### GuardHooks.after_lease

`after_lease(self, worker_id, task)`

See the corresponding facade operation.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
from joons_allocator.integrations.guard import GuardHooks
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        store.insert("task", {"work": 1})
        task = store.get("task")
        try:
            obj = GuardHooks()
        except ImportError:
            print("Optional guard is not installed")
        else:
            print(obj.after_lease("worker", task))
```

### GuardHooks.after_recover

`after_recover(self, worker_id, task)`

See the corresponding facade operation.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
from joons_allocator.integrations.guard import GuardHooks
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        store.insert("task", {"work": 1})
        task = store.get("task")
        try:
            obj = GuardHooks()
        except ImportError:
            print("Optional guard is not installed")
        else:
            print(obj.after_recover("worker", task))
```

### GuardHooks.after_settle

`after_settle(self, worker_id, task, success)`

See the corresponding facade operation.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `None`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
from joons_allocator.integrations.guard import GuardHooks
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        store.insert("task", {"work": 1})
        task = store.get("task")
        try:
            obj = GuardHooks()
        except ImportError:
            print("Optional guard is not installed")
        else:
            print(obj.after_settle("worker", task, True))
```

### GuardHooks.before_lease

`before_lease(self, worker_id, task)`

See the corresponding facade operation.

Parameters are as named in the signature; shared identifiers, validation and database exceptions are described above. 
Returns `True` to allow the lease, otherwise `False`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator, AsyncAllocator, TaskStore, Admission, Dispatcher, Control
from joons_allocator.integrations.guard import GuardHooks
with TemporaryDirectory() as d:
    with TaskStore(Path(d) / "db.sqlite") as store:
        store.insert("task", {"work": 1})
        task = store.get("task")
        try:
            obj = GuardHooks()
        except ImportError:
            print("Optional guard is not installed")
        else:
            print(obj.before_lease("worker", task))
```

## Parameters and behavior

`payload` is strict JSON with string object keys; NaN and infinity are rejected. `priority` is an integer, higher first; `resource` is an optional exclusive resource name. `principal` and `request_id` scope deduplication; the caller is responsible for trusting the principal. `require_approval` starts a task awaiting approval. `task_id`, `worker_id`, `expected_worker`, and `token` identify the relevant task, worker, lease holder and lease. `expected_state`, `new_state` and `expected_version` control compare-and-set. `decided_by` and `note` identify and annotate an operator decision. `result` is optional text of at most 1,048,576 characters and must encode as UTF-8; `reason` is text of at most 4096 characters. `success` is the guard settlement outcome.

Raw TaskStore lease and state mutators are private; use Dispatcher and Control for coordinated transitions. Recovery, slot clearing and the audit event commit atomically; the recovery hook runs afterward.

## Task

`Task(task_id: str, state: str, payload: Any, result: str | None, priority: int, resource: str | None, worker_id: str | None, lease_token: str | None, created: float, updated: float, version: int) -> None`

One shallow frozen snapshot of a ``tasks`` row; ``payload`` is parsed JSON and nested objects may be mutable. Frozen data record; constructor parameters are its fields and it returns a new instance. Wrong constructor arguments raise `TypeError`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator
with TemporaryDirectory() as d:
    with Allocator(Path(d) / "db.sqlite") as a:
        result = a.submit({"work": 1}, request_id="request")
        task = a.lease("worker")
        if task:
            a.complete("worker", task, "done")
        print(a.get(result.task_id))
```

## SubmitResult

`SubmitResult(status: str, task_id: str | None) -> None`

The verdict of one submit. Immutable data record; constructor parameters are its fields and it returns a new instance. Wrong constructor arguments raise `TypeError`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator
with TemporaryDirectory() as d:
    with Allocator(Path(d) / "db.sqlite") as a:
        result = a.submit({"work": 1}, request_id="request")
        task = a.lease("worker")
        if task:
            a.complete("worker", task, "done")
        print(a.get(result.task_id))
```

## AuditEvent

`AuditEvent(id: int, task_id: str, action: str, before_state: str, after_state: str, decided_by: str, note: str, ts: float) -> None`

One append-only audit row: an operator decision over one task. Immutable data record; constructor parameters are its fields and it returns a new instance. Wrong constructor arguments raise `TypeError`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator
with TemporaryDirectory() as d:
    with Allocator(Path(d) / "db.sqlite") as a:
        result = a.submit({"work": 1}, request_id="request")
        task = a.lease("worker")
        if task:
            a.complete("worker", task, "done")
        print(a.get(result.task_id))
```

## WorkerInfo

`WorkerInfo(worker_id: str, registered: float, last_seen: float, current_task: str | None, consecutive_failures: int, paused: bool) -> None`

One shallow frozen snapshot of a ``workers`` row. Frozen data record; constructor parameters are its fields and it returns a new instance. Wrong constructor arguments raise `TypeError`.

Example:

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator
with TemporaryDirectory() as d:
    with Allocator(Path(d) / "db.sqlite") as a:
        result = a.submit({"work": 1}, request_id="request")
        task = a.lease("worker")
        if task:
            a.complete("worker", task, "done")
        print(a.get(result.task_id))
```

## canonical_json

`canonical_json(payload: Any) -> str`

Canonical JSON of a payload: sorted keys, no whitespace, ASCII.

The same logical payload always produces the same string, so digests and
stored rows stay comparable. Raises ValueError for NaN/Infinity (not
JSON: RFC 8259 has no such tokens) and TypeError for non-str dict keys,
in both cases with a message that names the offending value.


Accepts the argument shown in the signature; returns the validated value, digest, snapshot or `None` for `require_ids`. Invalid inputs may raise `InvalidIdentifier`, `TypeError` or `ValueError`.

```python
from tempfile import TemporaryDirectory
from joons_allocator import canonical_json
with TemporaryDirectory() as directory:
    print(canonical_json({"work": 1}))
```

## check_result

`check_result(result: str | None) -> str | None`

Validate an optional stored result BEFORE any mutation.

A result is a str (or None) of at most :data:`MAX_RESULT_CHARS` (1,048,576 characters)
characters that must be JSON-encodable text — a str is always a legal
JSON string EXCEPT one holding lone surrogates, which also cannot be
stored as SQLite TEXT (UTF-8), so those are rejected. Raises
TypeError/ValueError naming the problem; never truncates.


Accepts the argument shown in the signature; returns the validated value, digest, snapshot or `None` for `require_ids`. Invalid inputs may raise `InvalidIdentifier`, `TypeError` or `ValueError`.

```python
from tempfile import TemporaryDirectory
from joons_allocator import check_result
with TemporaryDirectory() as directory:
    print(check_result("done"))
```

## check_reason

`check_reason(reason: str) -> str`

Validate a failure reason BEFORE any mutation: a str of at most
:data:`MAX_REASON_CHARS` (4096) characters. Never truncates.


Accepts the argument shown in the signature; returns the validated value, digest, snapshot or `None` for `require_ids`. Invalid inputs may raise `InvalidIdentifier`, `TypeError` or `ValueError`.

```python
from tempfile import TemporaryDirectory
from joons_allocator import check_reason
with TemporaryDirectory() as directory:
    print(check_reason("failed"))
```

## payload_digest

`payload_digest(payload: Any) -> str`

SHA-256 over the canonical JSON of the payload; a payload-only helper; submission fingerprints also include priority, resource and approval requirement.


Accepts the argument shown in the signature; returns the validated value, digest, snapshot or `None` for `require_ids`. Invalid inputs may raise `InvalidIdentifier`, `TypeError` or `ValueError`.

```python
from tempfile import TemporaryDirectory
from joons_allocator import payload_digest
with TemporaryDirectory() as directory:
    print(payload_digest({"work": 1}))
```

## require_ids

`require_ids(**values)`

Raise InvalidIdentifier unless every value is a non-empty str of at
most MAX_IDENTIFIER_CHARS.


Accepts the argument shown in the signature; returns the validated value, digest, snapshot or `None` for `require_ids`. Invalid inputs may raise `InvalidIdentifier`, `TypeError` or `ValueError`.

```python
from tempfile import TemporaryDirectory
from joons_allocator.errors import require_ids
with TemporaryDirectory() as directory:
    print(require_ids(task_id="task"))
```

## task_from_row

`task_from_row(row) -> joons_allocator.store.Task`

Build the immutable Task snapshot for one raw ``tasks`` row.


Accepts the argument shown in the signature; returns the validated value, digest, snapshot or `None` for `require_ids`. Invalid inputs may raise `InvalidIdentifier`, `TypeError` or `ValueError`.

```python
from tempfile import TemporaryDirectory
from joons_allocator.store import task_from_row
with TemporaryDirectory() as directory:
    print(task_from_row(("task", "queued", "{}", None, 0, None, None, None, 0.0, 0.0, 1)))
```

## Errors

`AllocatorError` is the package exception base. `InvalidIdentifier` subclasses both `AllocatorError` and `ValueError`. Both accept ordinary exception message arguments and return exception instances; neither raises on construction. Standard exceptions can also propagate.

```python
from tempfile import TemporaryDirectory
from joons_allocator import AllocatorError, InvalidIdentifier
with TemporaryDirectory() as directory:
    error = InvalidIdentifier("invalid identifier")
    print(isinstance(error, AllocatorError))
```

## CLI

`python -m joons_allocator --db PATH [--json] COMMAND` (also `joons-allocator`) accepts `--db` and `--json` before or after the command. `-h` / `--help` displays help. Read commands open an existing database read-only; submit and decisions also require an existing database.

| Command | Arguments and options | Purpose |
|---|---|---|
| `submit` | `--payload JSON` (required), `--priority N`, `--resource NAME`, `--request-id ID` | Submit a JSON object |
| `list` | `--state STATE` | List tasks, optionally filtered |
| `queue` | none | List queued tasks in lease order |
| `show`, `history` | `task` | Show a task or its audit history |
| `workers` | none | List workers |
| `approve`, `pause`, `resume`, `cancel`, `retry` | `task --by NAME [--note N] [--yes]` | Audited decision |
| `recover` | `task --worker W --by NAME [--note N] [--yes]` | Recover a worker's task |

Without `--yes`, decisions print a plan and return 2. Exit codes: 0 success, 1 not found / nothing changed / decision refused (also runtime validation or database errors), 2 bad usage, invalid identifier or confirmation required.

