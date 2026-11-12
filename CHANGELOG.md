# Changelog

All notable changes to joons-allocator are documented here. Format follows
Keep a Changelog; the project has no fixed release cadence.

## [0.1.0 Preview] - Unreleased

First preview of JOONS Allocator: a rename and rework of the joons-jobs
0.0.1 prototype. The design is unchanged at heart — one SQLite file per
allocator, single machine, no server, no distributed mode, every state
change one `BEGIN IMMEDIATE` transaction, nothing expires by age — but the
package is renamed, dependency-free, and composed behind one facade.
Preview release; the API may still change before 1.0.

### Migration from joons-jobs

| joons-jobs 0.0.1 | joons-allocator 0.1.0 |
| --- | --- |
| `import joons_jobs` | `import joons_allocator` |
| `Job` | `Task` |
| `JobStore` | `TaskStore` |
| `job_id` / `job_audit` | `task_id` / `task_audit` |
| `python -m joons_jobs --db PATH ...` | `joons-allocator --db PATH ...` (also `python -m joons_allocator`) |
| four components wired by hand (`JobStore` + `ReplayGuard` + `ResourceLock` + `CircuitBreaker`) | one `Allocator(path)` owning the database file |
| errors `GuardError` / `InvalidIdentifier` from `joons_agent_guard.errors` | `AllocatorError` / `InvalidIdentifier` from `joons_allocator.errors` |

`Admission`, `Dispatcher` and `Control` keep their names and stay public for
advanced use; every CLI command drops the old "job" wording for "task".

### Added

- `Allocator` facade (`joons_allocator.allocator`): owns one SQLite file and
  composes `TaskStore` + `Admission` + `Dispatcher` + `Control`, so one
  object covers submit, inspect, lease, settle, recover and audit; the
  component classes remain public.
- Priority + FIFO allocation: tasks take a `priority` (default 0);
  `lease` picks the highest-priority, oldest `created` queued task and
  `queue()` lists queued tasks in exactly that order.
- Worker registry: a `workers` table (`registered`, `last_seen`,
  `current_task`, `consecutive_failures`, `paused`); `lease` auto-registers
  the worker, `heartbeat` refreshes `last_seen`, `workers()` lists them, and
  `pause_worker` / `resume_worker` stop and restart leasing per worker.
- Resource exclusivity without external locks: a task may name a `resource`
  column value, and the lease transaction never picks a task whose resource
  is held by a running task — no separate lock table, no lock dance.
- Optional integration `joons_allocator.integrations.guard`
  (`GuardHooks(circuit=None, locks=None)` accepted as `hooks=`): reuses
  joons-agent-guard's circuit breaker and resource locks after the allocator
  commits; installs only the `guard` extra and imports lazily.
- CLI: `submit` (`--payload`, `--priority`, `--resource`, `--request-id`),
  `queue` and `workers` join the operator commands; read commands open the
  database strictly read-only (`mode=ro`).

### Changed

- Safe settlement is mandatory: `complete`, `fail` and `abandon` take the
  leased `Task` (or its id with `lease_token=`), and `recover` requires
  `lease_token=`; an id-only call raises `TypeError` and a stale token returns
  `False`, so an earlier execution can never settle or recover a newer lease of
  the same task. The CLI `show` and `workers` commands print the token and
  `recover` requires `--lease-token`.
- Every operator decision (approve, pause, resume, cancel, retry, recover)
  commits its state change, worker-slot update and audit row in one
  transaction; hooks run only after the commit, and a broken hook object can
  never raise after a lease has committed.
- The raw `TaskStore` lease and state mutators are private; use `Dispatcher`,
  `Control` or the `Allocator` facade.
- Submission fingerprints cover the payload, priority, resource and approval
  requirement: repeating a request id with different options is a `conflict`.

- No runtime dependency on joons-agent-guard: the core imports and works
  with the standard library only, with its own `_db` (autocommit connect,
  per-connection re-entrant lock, `BEGIN IMMEDIATE` write transactions,
  nested-write refusal) and `errors` (`AllocatorError`,
  `InvalidIdentifier`, `require_ids`). The guard returns as an optional
  extra and only behind the integration hooks.
- Single-file atomic submission: an idempotent `submissions` table
  (`principal`, `request_id`, `digest`, `task_id`) is written in the same
  transaction as the task row, so a submit verdict (`accepted`,
  `duplicate`, `conflict`) commits together with its effect — no crash
  window between the claim and the queue.

### Removed

- The `pending` verdict, `Admission.abort` and `Admission.reconcile`, and
  the `os.path.samefile` cross-file detection. They existed to repair a
  two-transaction window in the old separate-file design (claim committed
  apart from the effect); because submission is now one transaction in one
  file, there is no window left to repair, and every submit is either
  visible with its task or did not happen.
- The guard's claim ledger (`requests` / `outbound_requests` tables) as
  allocator storage: replaced by the `submissions` table above.

## 0.0.1 - unreleased (joons-jobs prototype)

Superseded by [0.1.0 Preview]; see the migration table above for what
carried over.
