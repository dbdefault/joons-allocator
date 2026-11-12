# JOONS Allocator 0.1.0

Lightweight task allocation and worker coordination for AI agents.

> Part of the [JOONS ecosystem](https://github.com/dbdefault/joons-ecosystem): MCP-Based AI Infrastructure Ecosystem.

![Preview](https://img.shields.io/badge/status-Preview-orange) [![CI](https://github.com/dbdefault/joons-allocator/actions/workflows/ci.yml/badge.svg)](https://github.com/dbdefault/joons-allocator/actions/workflows/ci.yml) | [한국어](README.ko.md)

## 1. What it is

JOONS Allocator is a single-machine task allocator backed by one SQLite file. The `Allocator` facade combines task storage, admission, dispatch and operator control. The core uses only the Python standard library. This is a preview; the API may change.

## 2. The problem it solves

Agents need a durable way to submit work, assign it to available workers, avoid overlapping use of a named resource and record operator decisions. The allocator coordinates these operations locally without a server.

## 3. Features

- Transactional submission and leasing in one SQLite database, with audit records for operator decisions.
- Priority ordering, with oldest-first ordering among equal priorities.
- One running task per worker and exclusive use of named resources among running tasks.
- Idempotent submissions scoped by principal and request ID.
- Approval, cancellation, pause, resume, retry and explicit recovery controls.
- Worker registration on lease, heartbeat, pause and resume.
- Command-line interface and an asyncio facade.
- Optional JOONS Agent Guard hooks; the core does not require the guard.

### Not in 0.1.0

Planned and out of scope: multi-machine or network coordination, automatic task timeout expiry, scheduled or delayed tasks, task dependencies, worker capacity greater than one, and a web UI.

## 4. Architecture

An `Allocator` owns a database and composes `TaskStore`, `Admission`, `Dispatcher` and `Control`. Resource availability is checked in the lease transaction. Recovery and its audit record commit in the same transaction. See [Architecture](ARCHITECTURE.md).

## 5. Installation

From a source checkout:

```sh
pip install .
```

From the Git repository:

```sh
pip install git+https://github.com/dbdefault/joons-allocator
```

For the optional guard integration, install the `guard` extra:

```sh
pip install ".[guard]"
```

## 6. Quick start

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator

with TemporaryDirectory() as directory:
    with Allocator(Path(directory) / "tasks.sqlite") as allocator:
        submitted = allocator.submit({"operation": "add", "values": [1, 2]})
        task = allocator.lease("worker-1")
        if task is not None:
            allocator.complete("worker-1", task, '3')
            print(allocator.get(submitted.task_id))
```

## 7. Submitting and allocating tasks

`submit` requires a JSON-serializable dict payload and optional `priority`, `resource`, `principal`, `request_id` and `require_approval`. Higher priority is leased first; equal priorities are ordered by creation time. A task with a resource is skipped while a running task holds that resource.

With a request ID, submission returns `accepted` for a new request, `duplicate` with the original task ID for the same principal, request ID, payload, priority, resource, and approval requirement, or `conflict` for the same key with different payload or options. Existing ledgers created before this digest change require new request IDs. Approval-required tasks wait until an operator approves them.

```python
from pathlib import Path
from tempfile import TemporaryDirectory
from joons_allocator import Allocator

with TemporaryDirectory() as directory:
    with Allocator(Path(directory) / "tasks.sqlite") as allocator:
        submission = allocator.submit(
            {"operation": "render"}, priority=10, resource="renderer",
            principal="service", request_id="request-1", require_approval=True,
        )
        allocator.approve(submission.task_id, "operator")
        task = allocator.lease("worker-1")
        if task is not None:
            allocator.complete("worker-1", task, 'done')
```

## 8. Workers

`lease(worker_id)` registers a new worker automatically and returns a task or `None`. A paused worker or a worker already running a task cannot lease another. Use `heartbeat`, `workers`, `pause_worker` and `resume_worker` to manage workers. Results are text strings. Pass the leased `Task` to `complete` or `fail` (or pass its ID with `lease_token=task.lease_token`) to fence stale executions. ID-only calls raise `TypeError`. Recovery requires `lease_token=task.lease_token`; CLI `show` and `workers` display the current token for the required `--lease-token` option; an operator can `retry` a failed task or explicitly `recover` a task for its expected worker. Operator decisions are recorded in task history.

## 9. CLI usage

The CLI is available as `python -m joons_allocator` or `joons-allocator`. Supply the database before the command; `--json` is optional.

```sh
python -m joons_allocator --db tasks.sqlite submit --payload '{"action":"render"}' --priority 10 --resource renderer --request-id request-1
python -m joons_allocator --db tasks.sqlite --json queue
python -m joons_allocator --db tasks.sqlite list --state awaiting_approval
python -m joons_allocator --db tasks.sqlite approve task-1 --by operator --yes
python -m joons_allocator --db tasks.sqlite history task-1
python -m joons_allocator --db tasks.sqlite recover task-1 --worker worker-1 --lease-token TOKEN --by operator --yes
```

The CLI requires an existing database, including for submit. Read commands open it read-only. Operator mutations require `--yes`. Exit code `0` means success, `1` means not found, nothing changed or a decision was refused, and `2` means bad usage or confirmation required.

## 10. Supported environments

Python 3.9–3.13 on Windows, macOS and Linux; one machine with a local disk and one SQLite file per allocator.

## 11. Limitations

- This is a preview, and the core is being completed.
- Coordination is limited to a single machine; there is no server or distributed mode.
- Tasks do not expire automatically. Recovery is an explicit operator action.
- A worker has one running task slot. Scheduling, dependencies and a web UI are outside this release.
- Results are limited to 1,048,576 characters of UTF-8-encodable text and reasons to 4096 characters; NaN and Infinity are rejected.
- Optional guard hook failures are logged and must not corrupt allocator state; external guard cleanup can require attention after an error.

## 12. JOONS Allocator vs JOONS Agent Guard

| | JOONS Allocator | JOONS Agent Guard |
|---|---|---|
| Purpose | Local task allocation and worker coordination | Guard primitives such as circuits and resource locks |
| Core dependency | Python standard library | Optional for the allocator |
| Resources | Excludes concurrent running tasks with the same resource in its own database | Can provide separate resource locks through hooks |

To combine them, install the `guard` extra and provide `GuardHooks(circuit=..., locks=...)` from `joons_allocator.integrations.guard` to a `Dispatcher` via `hooks=`. The hooks use the supplied guard circuit and locks around task leasing and cleanup. The facade examples above do not require the guard.

## 13. License

Apache-2.0.

## 14. Contributing

See [Contributing](CONTRIBUTING.md).

