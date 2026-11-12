"""Optional JOONS Agent Guard hooks."""
import logging

_LOG = logging.getLogger("joons_allocator.integrations.guard")


class GuardHooks:
    def __init__(self, circuit=None, locks=None):
        try:
            import joons_agent_guard  # noqa: F401
        except ModuleNotFoundError as exc:
            if exc.name != "joons_agent_guard":
                raise
            raise ImportError(
                "JOONS Agent Guard is not installed: pip install joons-agent-guard"
            ) from exc
        self.circuit = circuit
        self.locks = locks

    def before_lease(self, worker_id, task):
        claimed = False
        try:
            if self.circuit is not None:
                if not self.circuit.begin(worker_id, task.task_id):
                    # A repeated call for the same lease is already claimed.
                    if self.circuit.snapshot(worker_id).pending_job != task.task_id:
                        return False
                else:
                    claimed = True
            if task.resource and self.locks is not None:
                if not self.locks.acquire(task.resource, worker_id, task.task_id):
                    holder = self.locks.status(task.resource)
                    if holder is None or (holder.owner, holder.job_id) != (worker_id, task.task_id):
                        if claimed:
                            self.circuit.reset(worker_id, expected_job_id=task.task_id)
                        return False
            return True
        except Exception:
            _LOG.exception("Guard before_lease failed")
            if claimed:
                try:
                    self.circuit.reset(worker_id, expected_job_id=task.task_id)
                except Exception:
                    _LOG.exception("Guard circuit rollback failed")
            return False

    def after_lease(self, worker_id, task):
        pass

    def after_settle(self, worker_id, task, success):
        if self.circuit is not None:
            try:
                self.circuit.finish(worker_id, task.task_id, success=success,
                                    reason=None if success else "task failed")
            except Exception:
                _LOG.exception("Guard circuit finish failed")
        try:
            if task.resource and self.locks is not None:
                self.locks.release(task.resource, worker_id, task.task_id)
        except Exception:
            _LOG.exception("Guard lock release failed")

    def after_recover(self, worker_id, task):
        try:
            if task.resource and self.locks is not None:
                self.locks.break_lock(task.resource, expected_job_id=task.task_id)
        except Exception:
            _LOG.exception("Guard lock recovery failed")
        if self.circuit is not None:
            try:
                self.circuit.reset(worker_id, expected_job_id=task.task_id)
            except Exception:
                _LOG.exception("Guard circuit recovery failed")
