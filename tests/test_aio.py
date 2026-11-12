"""Tests for joons_allocator.aio: the asyncio adapter over the Allocator
facade.

Each test drives the adapter with ``asyncio.run`` inside a plain test
function (no async-plugin requirement). Standard library only.

Ported from the prototype; the cancellation regressions (A1-A4)
were reworked for the new seams (the one-transaction Dispatcher instead of
guard locks/circuit)."""
from f2_test_helpers import lease_token
import asyncio
import os
import sqlite3
import tempfile
import threading
import time
import unittest

from joons_allocator.aio import AsyncAllocator

RESOURCES = ("res-a", "res-b")


class AioTestCase(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self._tmp = tmp.name

    def db_path(self, name="queue.sqlite"):
        return os.path.join(self._tmp, name)


class TestFleetDrain(AioTestCase):
    def test_ten_workers_drain_forty_tasks_over_two_resources(self):
        async def scenario():
            db = self.db_path()
            async with AsyncAllocator(db) as admission:
                payloads = [{"task": "work-%02d" % i} for i in range(40)]
                verdicts = await asyncio.gather(*[
                    admission.submit(payload, principal="service-a",
                                     request_id="req-%02d" % i,
                                     resource=RESOURCES[i % 2])
                    for i, payload in enumerate(payloads)])
            self.assertEqual({result.status for result in verdicts}, {"accepted"})
            expected = {result.task_id for result in verdicts}

            records = []
            tick_counter = [0]

            async def ticker():
                while True:
                    tick_counter[0] += 1
                    await asyncio.sleep(0.005)

            ticker_task = asyncio.create_task(ticker())

            async def worker(worker_id):
                async with AsyncAllocator(db) as allocator:
                    while True:
                        task = await allocator.lease(worker_id)
                        if task is None:
                            return
                        start = time.time()
                        await asyncio.sleep(0.005)
                        end = time.time()
                        done = await allocator.complete(worker_id, task, "ok", lease_token=lease_token(allocator, task))
                        records.append((task.task_id, task.resource, start, end, done))

            workers = [asyncio.create_task(worker("worker-%d" % i)) for i in range(10)]
            await asyncio.gather(*workers)
            ticker_task.cancel()

            self.assertEqual(len(records), 40)
            self.assertEqual({r[0] for r in records}, expected)  # exactly once
            self.assertTrue(all(r[4] for r in records))
            for resource in RESOURCES:
                spans = sorted((r[2], r[3]) for r in records if r[1] == resource)
                self.assertEqual(len(spans), 20, msg=resource)
                for (_, prev_end), (start, _) in zip(spans, spans[1:]):
                    self.assertGreaterEqual(start, prev_end, msg=resource)
            self.assertGreater(tick_counter[0], 0)  # the loop ran throughout
            async with AsyncAllocator(db) as probe:
                states = [task.state for task in await probe.list()]
            self.assertEqual(states.count("succeeded"), 40)

        asyncio.run(scenario())


class TestDuplicateSubmissions(AioTestCase):
    def test_concurrent_submits_answer_one_accepted(self):
        async def scenario():
            db = self.db_path()
            async with AsyncAllocator(db) as admission:
                results = await asyncio.gather(*[
                    admission.submit({"task": "once"}, principal="service-a",
                                     request_id="req-dup")
                    for _ in range(6)])
            statuses = [result.status for result in results]
            self.assertEqual(statuses.count("accepted"), 1)
            self.assertEqual(statuses.count("duplicate"), 5, msg=repr(results))
            winner = next(result.task_id for result in results if result.status == "accepted")
            for result in results:
                self.assertTrue(result.task_id is None or result.task_id == winner)

            # a later replay is answered from the submissions ledger
            async with AsyncAllocator(db) as admission:
                replay = await admission.submit({"task": "once"}, principal="service-a",
                                                request_id="req-dup")
                self.assertEqual((replay.status, replay.task_id), ("duplicate", winner))
                self.assertEqual(len(await admission.list()), 1)

        asyncio.run(scenario())


class TestApprovalFlow(AioTestCase):
    def test_approval_gate_through_the_async_facade(self):
        async def scenario():
            db = self.db_path()
            async with AsyncAllocator(db) as admission, \
                    AsyncAllocator(db) as control, \
                    AsyncAllocator(db) as dispatcher:
                result = await admission.submit({"task": "work"}, principal="service-a",
                                                request_id="req-1", require_approval=True)
                self.assertEqual(result.status, "accepted")
                task = await control.get(result.task_id)
                self.assertEqual(task.state, "awaiting_approval")
                self.assertIsNone(await dispatcher.lease("worker-1"))  # gated
                self.assertEqual(await control.history(result.task_id), [])

                self.assertTrue(await control.approve(result.task_id, "op-1", note="go"))
                self.assertEqual([e.action for e in await control.history(result.task_id)],
                                 ["approve"])
                leased = await dispatcher.lease("worker-1")
                self.assertEqual(leased.task_id, result.task_id)
                self.assertTrue(await dispatcher.complete("worker-1", result.task_id, "ok", lease_token=lease_token(dispatcher, result.task_id)))
                self.assertEqual((await control.get(result.task_id)).state, "succeeded")

        asyncio.run(scenario())


class TestLoopLivenessUnderLock(AioTestCase):
    def test_event_loop_ticks_while_a_write_waits_on_a_locked_db(self):
        async def scenario():
            db = self.db_path()
            opener = AsyncAllocator(db)  # create the schema BEFORE locking the file
            await opener.aclose()
            ticks = [0]
            stop = [False]

            async def ticker():
                while not stop[0]:
                    ticks[0] += 1
                    await asyncio.sleep(0.005)

            async with AsyncAllocator(db) as admission:
                # a raw connection holds the file's write lock (check_same_
                # thread=False: the loop thread will release it later)
                holder = sqlite3.connect(db, isolation_level=None, timeout=30,
                                         check_same_thread=False)
                holder.execute("PRAGMA busy_timeout=30000")
                holder.execute("BEGIN IMMEDIATE")
                holder.execute("CREATE TABLE IF NOT EXISTS keepalive (x TEXT)")
                tick_task = asyncio.create_task(ticker())
                loop = asyncio.get_running_loop()

                def release():
                    holder.execute("ROLLBACK")
                    holder.close()

                started = time.time()
                loop.call_later(0.35, release)
                ticks_before = ticks[0]
                result = await admission.submit({"task": "work"}, principal="service-a",
                                                request_id="req-1")
                elapsed = time.time() - started
                self.assertEqual(result.status, "accepted")
                self.assertGreaterEqual(elapsed, 0.3)  # the write really waited
                # and the loop kept ticking the whole time
                self.assertGreaterEqual(ticks[0] - ticks_before, 10)

            stop[0] = True
            await tick_task
            async with AsyncAllocator(db) as probe:
                task = await probe.get(result.task_id)
                self.assertEqual(task.state, "queued")

        asyncio.run(scenario())


class TestLifecycle(AioTestCase):
    def test_aclose_is_idempotent_and_async_with_closes(self):
        async def scenario():
            db = self.db_path()
            adapter = AsyncAllocator(db)
            await adapter.aclose()
            await adapter.aclose()  # idempotent
            with self.assertRaises(RuntimeError):
                await adapter.history("task-1")

            async with AsyncAllocator(db) as admission:
                result = await admission.submit({"task": "w"}, principal="s",
                                                request_id="req-1")
                self.assertEqual(result.status, "accepted")
            with self.assertRaises(RuntimeError):
                await admission.submit({"task": "w"}, principal="s", request_id="req-2")

        asyncio.run(scenario())


class TestCancellation(AioTestCase):
    """Regression tests for review5, ported: A-1 cancelled lease, A-2 close
    during an in-flight call, A-3 retry after a cancelled complete, A-4 late
    worker-thread exceptions are retrievable."""

    def _dispatcher_of(self, adapter):
        return adapter._sync._dispatcher

    def test_a1_cancelled_lease_returns_the_task_to_queued(self):
        async def scenario():
            db = self.db_path()
            async with AsyncAllocator(db) as adapter:
                await adapter.submit({"task": "a"}, task_id="task-1",
                                     resource="res-x")
                # slow the claim down so the task is cancelled mid-flight
                dispatcher = self._dispatcher_of(adapter)
                real_lease = dispatcher.lease

                def slow_lease(worker_id):
                    time.sleep(0.25)
                    return real_lease(worker_id)

                dispatcher.lease = slow_lease
                task_ref = asyncio.create_task(adapter.lease("worker-1"))
                await asyncio.sleep(0.05)
                task_ref.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task_ref
                # the claim landed after the cancellation; the adapter hands
                # the task back to the queue instead of stranding it running
                await asyncio.sleep(0.35)
                del dispatcher.lease
                snapshot = await adapter.get("task-1")
                self.assertEqual(snapshot.state, "queued")
                self.assertIsNone(snapshot.worker_id)
                self.assertIsNone(snapshot.lease_token)
                slot = (await adapter.workers())[0]
                self.assertIsNone(slot.current_task)
                # and the queue is fully usable again
                leased = await adapter.lease("worker-1")
                self.assertEqual(leased.task_id, "task-1")
                self.assertTrue(await adapter.complete("worker-1", "task-1", "ok", lease_token=lease_token(adapter, "task-1")))

        asyncio.run(scenario())

    def test_a2_aclose_waits_for_inflight_calls(self):
        async def scenario():
            db = self.db_path()
            adapter = AsyncAllocator(db)
            await adapter.submit({"task": "a"}, task_id="task-1")
            # park the lease inside the before_lease hook until the gate opens
            gate = threading.Event()
            entered = threading.Event()

            class _GateHooks:
                def before_lease(self, worker_id, task):
                    entered.set()
                    gate.wait(5)
                    return True

            self._dispatcher_of(adapter)._hooks = _GateHooks()
            lease_task = asyncio.create_task(adapter.lease("worker-1"))
            for _ in range(100):
                if entered.is_set():
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(entered.is_set())

            close_task = asyncio.create_task(adapter.aclose())
            await asyncio.sleep(0.1)  # aclose waits for the in-flight call
            gate.set()
            # the in-flight lease is NOT broken: its result is delivered
            task = await lease_task
            self.assertEqual(task.state, "running")
            await close_task
            with self.assertRaises(RuntimeError):
                await adapter.lease("worker-1")

            # the delivered task is recovered through a fresh adapter
            async with AsyncAllocator(db) as fresh:
                self.assertTrue(await fresh.recover("task-1", "worker-1", "op", lease_token=lease_token(fresh, "task-1")))
                self.assertTrue(await fresh.retry("task-1", "op"))
                again = await fresh.lease("worker-2")
                self.assertEqual(again.task_id, "task-1")
                self.assertTrue(await fresh.complete("worker-2", "task-1", "ok", lease_token=lease_token(fresh, "task-1")))
            async with AsyncAllocator(db) as probe:
                self.assertEqual((await probe.get("task-1")).state, "succeeded")

        asyncio.run(scenario())

    def test_a3_retry_after_cancelled_complete_reports_already_completed(self):
        async def scenario():
            db = self.db_path()
            async with AsyncAllocator(db) as adapter:
                await adapter.submit({"task": "a"}, task_id="task-1")
                dispatcher = self._dispatcher_of(adapter)
                real_settle = dispatcher._settle

                def slow_settle(worker_id, task_id, new_state, result, *, success, lease_token=None):
                    time.sleep(0.25)
                    return real_settle(worker_id, task_id, new_state, result,
                                       success=success, lease_token=lease_token)

                dispatcher._settle = slow_settle
                self.assertEqual((await adapter.lease("worker-1")).task_id, "task-1")

                task_ref = asyncio.create_task(
                    adapter.complete("worker-1", "task-1", "ok", lease_token=lease_token(adapter, "task-1")))
                await asyncio.sleep(0.05)
                task_ref.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task_ref
                await asyncio.sleep(0.35)  # let the landed thread be observed
                # the cancelled attempt applied the outcome anyway
                self.assertEqual((await adapter.get("task-1")).state, "succeeded")

                # the retry is unambiguous: the caller's own cancelled attempt
                # already settled it -> True; a different worker -> refused
                self.assertTrue(await adapter.complete("worker-1", "task-1", "ok", lease_token=lease_token(adapter, "task-1")))
                self.assertFalse(await adapter.complete("worker-2", "task-1", "nope", lease_token=lease_token(adapter, "task-1")))
                # and the marker is consumed: a third call is refused again
                self.assertFalse(await adapter.complete("worker-1", "task-1", "ok", lease_token=lease_token(adapter, "task-1")))

        asyncio.run(scenario())

    def test_a4_late_worker_exception_is_retrievable(self):
        async def scenario():
            db = self.db_path()
            async with AsyncAllocator(db) as adapter:
                await adapter.submit({"task": "a"}, task_id="task-1")
                self.assertEqual((await adapter.lease("worker-1")).task_id, "task-1")
                dispatcher = self._dispatcher_of(adapter)

                def broken_settle(worker_id, task_id, new_state, result, *, success, lease_token=None):
                    time.sleep(0.25)
                    raise RuntimeError("settle blew up on the thread")

                dispatcher._settle = broken_settle

                task_ref = asyncio.create_task(
                    adapter.complete("worker-1", "task-1", "ok", lease_token=lease_token(adapter, "task-1")))
                await asyncio.sleep(0.05)
                task_ref.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task_ref
                await asyncio.sleep(0.35)  # let the thread land and be recorded

                lost = adapter.discarded_errors()
                self.assertTrue(any(isinstance(exc, RuntimeError) and "blew up" in str(exc)
                                    for _, name, exc in lost), msg=repr(lost))
                self.assertTrue(any(name == "complete" for _, name, _ in lost))
                # the failure is observable: the task stayed running
                self.assertEqual((await adapter.get("task-1")).state, "running")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
