"""abandon() is fenced by the lease token like complete()/fail()."""
import pytest

from joons_allocator import Allocator


def test_abandon_requires_token_and_rejects_stale_lease(tmp_path):
    with Allocator(str(tmp_path / "a.db")) as a:
        a.submit({"n": 1}, task_id="t")
        old = a.lease("w")
        d = a._dispatcher
        with pytest.raises(TypeError):
            d.abandon("t", "w")                                   # id-only is refused
        assert d.abandon(old, "w")                                 # current lease: back to queued
        assert a.get("t").state == "queued"
        new = a.lease("w")
        assert new.lease_token != old.lease_token
        assert not d.abandon("t", "w", lease_token=old.lease_token)  # stale token: nothing happens
        assert a.get("t").state == "running"
        assert a.get("t").lease_token == new.lease_token
