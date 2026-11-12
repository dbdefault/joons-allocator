"""joons_allocator: lightweight task allocation and worker coordination for
AI agents, on ONE SQLite file with standard-library-only dependencies.

Ships:

* :class:`Allocator`     - the facade owning one DB file (submit/get/lease/...)
* :class:`AsyncAllocator`- asyncio adapter over the facade (cancellation-safe)
* :class:`TaskStore`     - durable rows + explicit-state-machine compare-and-set
* :class:`Admission`     - at-most-once submit through the submissions ledger
* :class:`Dispatcher`    - worker leasing with resource exclusivity and
  one-running-task-per-worker, both checked inside the lease transaction
* :class:`Control`       - audited operator decisions (append-only task_audit)

The package is independent of joons-agent-guard: it imports nothing but the
standard library. The optional guard integration lives in
``joons_allocator.integrations.guard`` (own distribution requirement).
"""
from .admission import Admission, SubmitResult, payload_digest
from .allocator import Allocator
from .aio import AsyncAllocator
from .control import AuditEvent, Control
from .errors import AllocatorError, InvalidIdentifier
from .store import (STATES, TRANSITIONS, Task, TaskStore, canonical_json,
                    check_reason, check_result)
from .worker import Dispatcher, WorkerInfo

__version__ = "0.1.0"

__all__ = [
    "Allocator", "AllocatorError", "Admission", "AsyncAllocator", "AuditEvent", "Control",
    "Dispatcher", "InvalidIdentifier", "STATES", "SubmitResult", "Task",
    "TaskStore", "TRANSITIONS", "WorkerInfo", "canonical_json", "check_reason",
    "check_result", "payload_digest", "__version__",
]
