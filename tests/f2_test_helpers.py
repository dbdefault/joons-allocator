"""Test helper for adapting legacy ID-based calls to fenced operations."""
_tokens = {}
def lease_token(owner, task_id):
    if getattr(task_id, "lease_token", None):
        return task_id.lease_token
    task_id = getattr(task_id, "task_id", task_id)
    if not isinstance(task_id, str) or not task_id or len(task_id) > 1024:
        return "unknown-token"
    owner = getattr(owner, "_sync", owner)
    store = getattr(owner, "_store", None)
    key = (id(owner), task_id)
    if store is not None:
        task = store.get(task_id)
        if task is not None and task.lease_token:
            _tokens[key] = task.lease_token
    return _tokens.get(key, "unknown-token")
