"""The Allocator facade: one object owning one SQLite file.

Composes TaskStore + Admission + Dispatcher + Control so a user needs one
object; the component classes stay public for advanced use (e.g. a
Dispatcher constructed with ``hooks=``). Every operator decision writes one
append-only ``task_audit`` row in the same transaction.
"""
from typing import Any, List, Optional

from .admission import Admission, SubmitResult
from .control import AuditEvent, Control
from .store import Task, TaskStore
from .worker import Dispatcher, WorkerInfo

__all__ = ["Allocator"]


class Allocator:
    """One durable task queue on one SQLite file.

    Create it with the path of the queue file (or ``:memory:``); the schema
    is created on first use. Instances may be shared between threads; across
    processes, open one Allocator per process on the same file. ``close()``
    (or the context manager) releases the connection.
    """

    def __init__(self, path, *, timeout: float = 10.0):
        self._store = TaskStore(path, timeout=timeout)
        self._admission = Admission(self._store)
        self._dispatcher = Dispatcher(self._store)
        self._control = Control(self._store, self._dispatcher)

    @property
    def path(self) -> str:
        """Absolute path of the SQLite file this allocator owns."""
        return self._store.path

    # -- submission --------------------------------------------------------
    def submit(self, payload: Any, *, task_id: Optional[str] = None, priority: int = 0,
               resource: Optional[str] = None, principal: str = "default",
               request_id: Optional[str] = None,
               require_approval: bool = False) -> SubmitResult:
        """Admit one task exactly once; see Admission.submit for the verdicts."""
        return self._admission.submit(payload, task_id=task_id, priority=priority,
                                      resource=resource, principal=principal,
                                      request_id=request_id,
                                      require_approval=require_approval)

    # -- reads ---------------------------------------------------------------
    def get(self, task_id: str) -> Optional[Task]:
        """The task snapshot, or None when the id is unknown."""
        return self._store.get(task_id)

    def list(self, state: Optional[str] = None) -> List[Task]:
        """All tasks (oldest first), or only those in ``state``."""
        return self._store.list(state)

    def queue(self) -> List[Task]:
        """Queued tasks in exactly the allocation order of ``lease``."""
        return self._store.queue()

    def history(self, task_id: str) -> List[AuditEvent]:
        """All audit rows for one task, oldest first."""
        return self._control.history(task_id)

    def workers(self) -> List[WorkerInfo]:
        """All registered workers, ordered by worker_id."""
        return self._dispatcher.workers()

    # -- worker side ---------------------------------------------------------
    def lease(self, worker_id: str) -> Optional[Task]:
        """Claim the best queued task for this worker, or None."""
        return self._dispatcher.lease(worker_id)

    def complete(self, worker_id: str, task_id, result: str, *, lease_token=None) -> bool:
        """Finish a leased task: ``running -> succeeded``."""
        return self._dispatcher.complete(worker_id, task_id, result, lease_token=lease_token)

    def fail(self, worker_id: str, task_id, reason: str, *, lease_token=None) -> bool:
        """Finish a leased task with a failure: ``running -> failed``."""
        return self._dispatcher.fail(worker_id, task_id, reason, lease_token=lease_token)

    def heartbeat(self, worker_id: str) -> bool:
        """Refresh the worker's ``last_seen``; False for an unknown worker."""
        return self._dispatcher.heartbeat(worker_id)

    def pause_worker(self, worker_id: str) -> bool:
        """Stop handing new leases to this worker."""
        return self._dispatcher.pause_worker(worker_id)

    def resume_worker(self, worker_id: str) -> bool:
        """Hand leases to this worker again."""
        return self._dispatcher.resume_worker(worker_id)

    # -- operator decisions (audited) ----------------------------------------
    def approve(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``awaiting_approval -> queued``."""
        return self._control.approve(task_id, decided_by, note=note)

    def pause(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``queued -> paused``."""
        return self._control.pause(task_id, decided_by, note=note)

    def resume(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``paused -> queued``."""
        return self._control.resume(task_id, decided_by, note=note)

    def cancel(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``queued | awaiting_approval | paused -> cancelled`` (never running)."""
        return self._control.cancel(task_id, decided_by, note=note)

    def retry(self, task_id: str, decided_by: str, note: str = "") -> bool:
        """``failed -> queued`` with an audit row."""
        return self._control.retry(task_id, decided_by, note=note)

    def recover(self, task_id: str, expected_worker: str, decided_by: str,
                note: str = "", *, lease_token) -> bool:
        """Recover a task from a worker presumed dead, with an audit row."""
        return self._control.recover(task_id, expected_worker, decided_by, note=note, lease_token=lease_token)

    # -- lifecycle --------------------------------------------------------
    def close(self) -> None:
        """Close the owned connection."""
        self._store.close()

    def __enter__(self) -> "Allocator":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
