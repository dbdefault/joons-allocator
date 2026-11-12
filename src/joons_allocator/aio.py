"""Asyncio adapter for the Allocator facade: coroutine wrappers over the sync stack.

The adapter owns one synchronous Allocator built on the database path given
at construction and dispatches every blocking call with
:func:`asyncio.to_thread` (Python >= 3.9), so a contended SQLite write waits
on a worker thread instead of stalling the event loop. Method names and
semantics mirror the sync API — they become coroutines.

The constructor is synchronous and creates the schema on the calling
thread; construct the adapter before entering the event loop when that
matters. Concurrent coroutines sharing one adapter are safe: the underlying
sync objects serialize their own statements with their re-entrant guard
locks, and separate adapters on one file are serialized by SQLite itself,
exactly as with threads.

Lifecycle: ``await adapter.aclose()`` closes the owned Allocator, and
``async with`` does the same on exit. ``aclose`` first waits for every
in-flight call to land on the worker thread (so a call is never broken
mid-flight by a closing adapter), sets the closed flag immediately (new
calls raise ``RuntimeError``), and is idempotent. Do not start new calls
after closing.

Cancellation: ``asyncio`` cannot abort a call that is already running on a
worker thread — cancelling the awaiting task delivers ``CancelledError``
immediately while the thread keeps running the synchronous call to
completion. Every adapter call is therefore dispatched through
``asyncio.shield`` and tracked, and the consequences are handled per call:

* ``lease`` — a cancelled lease may still land and claim a task whose id
  the caller never learns. The adapter waits for the landed call and hands
  the task back to the queue immediately via ``Dispatcher.abandon``
  (``running -> queued``, the worker slot cleared), so a cancelled lease
  does not strand a running task. If the loop shuts down before the thread
  lands, the backstop is the sync recovery path: recover the task by its
  worker.
* ``complete`` / ``fail`` — the outcome may still be applied by the landed
  thread. When it was, a retry of the same call returns ``True`` ("already
  settled by your cancelled attempt") instead of the ambiguous ``False``; a
  genuinely refused settle keeps returning ``False``.
* Any other call — the outcome applies or fails on the thread.

Exceptions from a worker thread whose awaiting task was cancelled are never
silently dropped: they are logged (logger ``joons_allocator.aio``) and kept
retrievable via ``adapter.discarded_errors()`` — check it after catching
``CancelledError`` from a write whose outcome you do not know; the recovery
recipe for a lost settle is scan-``running``-by-worker + ``recover``.
"""
import asyncio
import collections
import logging
import time

from .allocator import Allocator

__all__ = ["AsyncAllocator"]

LOGGER = logging.getLogger("joons_allocator.aio")

_LOST_MAX = 64


class AsyncAllocator:
    """Coroutine wrapper owning one sync Allocator stack on one path."""

    def __init__(self, path, *, timeout: float = 10.0):
        self._sync = Allocator(path, timeout=timeout)
        self._closed = False
        self._inflight = set()  # shielded inner futures / compensation tasks
        self._lost = collections.deque(maxlen=_LOST_MAX)
        self._settled_after_cancel = set()  # (worker_id, task_id) pairs

    def discarded_errors(self) -> tuple:
        """Worker-thread exceptions that reached no awaiter (caller cancelled,
        or the call landed during ``aclose``), oldest first, capped. Check
        this after catching ``CancelledError`` from a write whose outcome is
        unknown."""
        return tuple(self._lost)

    # -- dispatch machinery --------------------------------------------------
    def _spawn(self, coro):
        """Run a background coroutine as a tracked task (``aclose`` waits)."""
        task = asyncio.ensure_future(coro)
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)
        return task

    async def _call(self, name: str, *args, **kwargs):
        """Run one sync call on a worker thread, shielded and tracked.

        On cancellation the thread call keeps running; a late exception is
        logged and kept retrievable instead of vanishing.
        """
        if self._closed:
            raise RuntimeError("this adapter is closed")
        inner = asyncio.ensure_future(
            asyncio.to_thread(self._sync_method(name), *args, **kwargs))
        self._inflight.add(inner)
        inner.add_done_callback(self._inflight.discard)
        try:
            return await asyncio.shield(inner)
        except asyncio.CancelledError:
            inner.add_done_callback(lambda fut, n=name: self._record_lost(n, fut))
            raise

    def _sync_method(self, name: str):
        return getattr(self._sync, name)

    def _record_lost(self, name: str, fut) -> None:
        if fut.cancelled():
            return
        try:
            exc = fut.exception()
        except asyncio.InvalidStateError:
            return  # the future completed with a result: nothing to record
        if exc is not None:
            self._lost.append((time.time(), name, exc))
            LOGGER.warning("joons_allocator aio: worker-thread exception in %s "
                           "reached no awaiter: %r", name, exc)

    async def aclose(self) -> None:
        """Close the owned synchronous Allocator. Idempotent.

        Waits for every in-flight call to land first, so an adapter is never
        closed out from under a running call. New calls raise RuntimeError
        from the moment the close begins.
        """
        if self._closed:
            return
        self._closed = True
        if self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)
        await asyncio.to_thread(self._sync.close)

    async def __aenter__(self) -> "AsyncAllocator":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    # -- worker side ---------------------------------------------------------
    async def lease(self, worker_id: str):
        """Claim the best queued task for this worker, or None.

        Cancellation-safe: if the awaiting task is cancelled, the shielded
        thread call may still land and claim a task — the adapter waits for
        that landing and hands the task back to the queue immediately
        (``Dispatcher.abandon``), so a cancelled lease never strands a
        running task. If the loop shuts down before the thread lands, the
        backstop is the sync recovery path: recover the task by its worker.
        """
        if self._closed:
            raise RuntimeError("this adapter is closed")
        inner = asyncio.ensure_future(asyncio.to_thread(self._sync.lease, worker_id))
        self._inflight.add(inner)
        inner.add_done_callback(self._inflight.discard)
        try:
            return await asyncio.shield(inner)
        except asyncio.CancelledError:
            self._record_lost("lease", inner)
            self._spawn(self._compensate_lease(worker_id, inner))
            raise

    async def _compensate_lease(self, worker_id: str, inner):
        try:
            task = await inner
        except asyncio.CancelledError:
            return  # the thread call never started: nothing was claimed
        except Exception as exc:
            self._lost.append((time.time(), "lease", exc))
            LOGGER.warning("joons_allocator aio: worker-thread exception in lease "
                           "reached no awaiter: %r", exc)
            return
        if task is not None:
            try:
                await asyncio.to_thread(self._sync._dispatcher.abandon,
                                        task.task_id, worker_id, lease_token=task.lease_token)
            except Exception as exc:
                self._lost.append((time.time(), "lease.abandon", exc))
                LOGGER.warning("joons_allocator aio: lease compensation failed "
                               "(recover the task by hand): %r", exc)

    async def complete(self, worker_id: str, task_id, result: str, *, lease_token=None) -> bool:
        """Finish a leased task: ``running -> succeeded``.

        Cancellation-safe: the outcome may still be applied by the landed
        thread; a retry of the same call then returns True ("already settled
        by your cancelled attempt") instead of the ambiguous False. False
        still means genuinely refused (wrong worker, unknown task).
        """
        return await self._settle("complete", worker_id, task_id, result, lease_token=lease_token)

    async def fail(self, worker_id: str, task_id, reason: str, *, lease_token=None) -> bool:
        """Finish a leased task with a failure: ``running -> failed``.

        Same cancellation semantics as ``complete``.
        """
        return await self._settle("fail", worker_id, task_id, reason, lease_token=lease_token)

    async def _settle(self, name: str, worker_id: str, task_id, arg: str, *, lease_token=None) -> bool:
        if self._closed:
            raise RuntimeError("this adapter is closed")
        from .worker import _lease_identity
        task_id, lease_token = _lease_identity(task_id, lease_token)
        key = (worker_id, task_id, lease_token, name)
        sync_method = getattr(self._sync, name)
        inner = asyncio.ensure_future(
            asyncio.to_thread(sync_method, worker_id, task_id, arg, lease_token=lease_token))
        self._inflight.add(inner)
        inner.add_done_callback(self._inflight.discard)
        try:
            outcome = await asyncio.shield(inner)
        except asyncio.CancelledError:

            def landed(fut, worker_id=worker_id, task_id=task_id):
                if fut.cancelled():
                    return
                exc = fut.exception()
                if exc is not None:
                    self._record_lost(name, fut)
                    return
                if fut.result() is True:
                    self._settled_after_cancel.add(key)

            inner.add_done_callback(landed)
            raise
        if outcome is False and key in self._settled_after_cancel:
            self._settled_after_cancel.discard(key)
            return True  # the caller's own cancelled attempt already settled it
        return outcome

    # -- submission and reads ---------------------------------------------
    async def submit(self, payload, *, task_id=None, priority=0, resource=None,
                     principal="default", request_id=None,
                     require_approval=False):
        """Admit one task exactly once; see Allocator.submit for the verdicts."""
        return await self._call("submit", payload, task_id=task_id, priority=priority,
                                resource=resource, principal=principal,
                                request_id=request_id, require_approval=require_approval)

    async def get(self, task_id):
        """The task snapshot, or None when the id is unknown."""
        return await self._call("get", task_id)

    async def list(self, state=None):
        """All tasks (oldest first), or only those in ``state``."""
        return await self._call("list", state)

    async def queue(self):
        """Queued tasks in exactly the allocation order of ``lease``."""
        return await self._call("queue")

    async def history(self, task_id):
        """All audit rows for one task, oldest first."""
        return await self._call("history", task_id)

    async def workers(self):
        """All registered workers, ordered by worker_id."""
        return await self._call("workers")

    # -- worker registry ----------------------------------------------------
    async def heartbeat(self, worker_id) -> bool:
        """Refresh the worker's ``last_seen``; False for an unknown worker."""
        return await self._call("heartbeat", worker_id)

    async def pause_worker(self, worker_id) -> bool:
        """Stop handing new leases to this worker."""
        return await self._call("pause_worker", worker_id)

    async def resume_worker(self, worker_id) -> bool:
        """Hand leases to this worker again."""
        return await self._call("resume_worker", worker_id)

    # -- operator decisions (audited) ----------------------------------------
    async def approve(self, task_id, decided_by, note="") -> bool:
        """``awaiting_approval -> queued``."""
        return await self._call("approve", task_id, decided_by, note=note)

    async def pause(self, task_id, decided_by, note="") -> bool:
        """``queued -> paused``."""
        return await self._call("pause", task_id, decided_by, note=note)

    async def resume(self, task_id, decided_by, note="") -> bool:
        """``paused -> queued``."""
        return await self._call("resume", task_id, decided_by, note=note)

    async def cancel(self, task_id, decided_by, note="") -> bool:
        """``queued | awaiting_approval | paused -> cancelled`` (never running)."""
        return await self._call("cancel", task_id, decided_by, note=note)

    async def retry(self, task_id, decided_by, note="") -> bool:
        """``failed -> queued`` with an audit row."""
        return await self._call("retry", task_id, decided_by, note=note)

    async def recover(self, task_id, expected_worker, decided_by, note="", *, lease_token) -> bool:
        """Recover a task from a worker presumed dead, with an audit row."""
        return await self._call("recover", task_id, expected_worker, decided_by, note=note, lease_token=lease_token)
