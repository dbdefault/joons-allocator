# Guarantees and limits

JOONS Allocator coordinates tasks on one machine using one SQLite file. Each state change uses a `BEGIN IMMEDIATE` transaction. A submission and its idempotency record commit together; a lease atomically checks and claims the task and worker slot. SQLite provides transaction durability subject to its configuration and the underlying filesystem.

## Submission and allocation

A `(principal, request_id)` identifies a submission. Repeating the same payload, priority, resource and approval requirement returns `duplicate` and the original task ID; a different payload or option returns `conflict` without creating a task. A new request returns `accepted`.

A lease selects the highest-priority eligible queued task, then the oldest by creation time (ties by task ID). A resource named by a running task cannot be leased by another task, and each worker can hold at most one running task. These checks occur in the lease transaction. Pausing a worker prevents new leases but does not interrupt its running task.

## Recovery and audit

Running tasks never expire automatically. After confirming a worker cannot continue, an operator calls `recover(task_id, expected_worker, decided_by, note, lease_token=token)`; recovery checks the worker and lease token, moves the task to `failed`, and releases its worker slot. An operator may then `retry` it. Recovery does not stop a worker or undo external side effects, so the operator must avoid overlapping execution and account for repeated effects.

The task audit trail is append-only through the allocator API. Successful operator decisions, including recovery, commit their state changes and audit entries in one transaction. Direct database changes are outside these guarantees.

## Optional Guard hooks

The allocator works without JOONS Agent Guard. Advanced users can attach `GuardHooks` to a `Dispatcher` to add a Guard circuit check and optional external resource locks. A pre-lease hook can veto a candidate; post-commit hooks finish or release Guard state on settlement or recovery. Hook errors are logged, and post-commit failures do not roll back allocator state; external Guard state can therefore require attention. The built-in single-file resource and worker guarantees do not require Guard.

## Scope

This preview is single-machine, with one task per worker. It does not provide multi-machine or network coordination, automatic task expiry, scheduled or delayed tasks, task dependencies, worker capacity above one, or a web UI; these are planned. It does not provide exactly-once execution of external work. Results are limited to 1,048,576 characters of text and failure reasons to 4096 characters.
