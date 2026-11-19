"""Block 1 / PDF §4.2 - pending writes (``put_writes``) are durable and replay-safe.

LangGraph commits every task's output through ``put_writes`` *before* the next
super-step, which is what makes "a pod died mid-turn" recoverable: the finished
tasks of the interrupted super-step must not run again. The rules the State
contract enforces (``internal/state/checkpoint_parts.go`` ``PutWrites``):

* ``(checkpoint, task, index)`` is unique;
* replaying the *identical* write is a no-op;
* a different payload under that key is KEY_REUSE_MISMATCH;
* the ``WRITES_IDX_MAP`` specials (``__error__``/``__scheduled__``/
  ``__interrupt__``/``__resume__``) use negative indexes and must survive.
"""

from __future__ import annotations

import uuid

import pytest
from langgraph.checkpoint.base import WRITES_IDX_MAP

from ahp_state import AHPCheckpointSaver, StateAPIClient, StateAPIError

from .fake_state import FakeState, FakeStateServer, StateDoc

@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


class Harness:
    def __init__(self, server: FakeStateServer):
        self.server = server
        self.boot = server.bootstrap(f"ctx-{uuid.uuid4().hex[:8]}")
        self.client = StateAPIClient(server.url, token=self.boot["token"])
        self.saver = AHPCheckpointSaver(self.client, self.boot["thread_id"],
                                        self.boot["fence"], namespace="graph")
        self.cp = {
            "v": 1, "id": f"cp-{uuid.uuid4().hex[:8]}", "ts": "2026-11-01",
            "channel_values": {"messages": []}, "channel_versions": {},
            "versions_seen": {}, "pending_sends": [],
        }

    async def open(self) -> dict:
        await self.saver.aput({"configurable": {"thread_id": self.boot["thread_id"]}},
                              self.cp, {"source": "input"}, {})
        return {"configurable": {"thread_id": self.boot["thread_id"],
                                 "checkpoint_id": self.cp["id"]}}


async def test_pending_writes_round_trip_decoded(server):
    h = Harness(server)
    cfg = await h.open()
    await h.saver.aput_writes(cfg, [("messages", ["partial"]), ("counter", 3)], "task-a")

    tup = await h.saver.aget_tuple(cfg)
    assert sorted(tup.pending_writes, key=lambda w: w[1]) == [
        ("task-a", "counter", 3), ("task-a", "messages", ["partial"])]


async def test_replay_of_identical_write_is_a_noop(server):
    """A retried task re-sends its writes; the durable set must not duplicate."""
    h = Harness(server)
    cfg = await h.open()
    for _ in range(3):
        await h.saver.aput_writes(cfg, [("messages", ["x"]), ("notes", "y")], "task-b")
    tup = await h.saver.aget_tuple(cfg)
    assert len(tup.pending_writes) == 2


async def test_prefix_resend_is_absorbed(server):
    """The executor re-sends the whole accumulated write list plus the new item."""
    h = Harness(server)
    cfg = await h.open()
    await h.saver.aput_writes(cfg, [("messages", ["first"])], "task-c")
    await h.saver.aput_writes(cfg, [("messages", ["first"]), ("output", "second")], "task-c")
    tup = await h.saver.aget_tuple(cfg)
    assert sorted(w[1] for w in tup.pending_writes) == ["messages", "output"]


async def test_different_payload_under_same_task_and_index_is_rejected(server):
    """Silently keeping the first value would corrupt the resume; it must fail."""
    h = Harness(server)
    cfg = await h.open()
    await h.saver.aput_writes(cfg, [("messages", ["v1"])], "task-d")
    with pytest.raises(StateAPIError) as exc:
        await h.saver.aput_writes(cfg, [("messages", ["v2"])], "task-d")
    assert exc.value.code == "KEY_REUSE_MISMATCH"


async def test_lost_response_after_commit_replays(server):
    h = Harness(server)
    cfg = await h.open()
    server.state.drop_response_after_commit("PutWrites")
    with pytest.raises(StateAPIError):
        await h.saver.aput_writes(cfg, [("messages", ["durable"])], "task-e")

    server.state._drop_after_commit.clear()
    await h.saver.aput_writes(cfg, [("messages", ["durable"])], "task-e")   # retry
    tup = await h.saver.aget_tuple(cfg)
    assert tup.pending_writes == [("task-e", "messages", ["durable"])]


async def test_special_channels_keep_their_negative_index(server):
    h = Harness(server)
    cfg = await h.open()
    await h.saver.aput_writes(cfg, [("__interrupt__", {"value": "confirm?"})], "task-f")

    rows = await h.client.get_writes(h.boot["thread_id"], "graph", h.cp["id"])
    assert [r["index"] for r in rows] == [WRITES_IDX_MAP["__interrupt__"]] == [-3]
    tup = await h.saver.aget_tuple(cfg)
    assert tup.pending_writes == [("task-f", "__interrupt__", {"value": "confirm?"})]


async def test_writes_from_two_tasks_are_kept_apart(server):
    h = Harness(server)
    cfg = await h.open()
    await h.saver.aput_writes(cfg, [("a_out", 1), ("shared", "from-a")], "task-1")
    await h.saver.aput_writes(cfg, [("b_out", 2), ("shared", "from-b")], "task-2")
    tup = await h.saver.aget_tuple(cfg)
    by_task = {w[0] for w in tup.pending_writes}
    assert by_task == {"task-1", "task-2"}
    assert len(tup.pending_writes) == 4


async def test_writes_before_the_checkpoint_are_replayed_after_put(server):
    """The real executor races ``put_writes`` ahead of ``put`` for one checkpoint."""
    server_state = FakeState(StateDoc())
    with FakeStateServer(server_state) as srv:
        boot = srv.bootstrap(f"ctx-{uuid.uuid4().hex[:8]}")
        client = StateAPIClient(srv.url, token=boot["token"])
        saver = AHPCheckpointSaver(client, boot["thread_id"], boot["fence"])
        cp = {"v": 1, "id": "cp-race", "ts": "2026-11-01", "channel_values": {},
              "channel_versions": {}, "versions_seen": {}, "pending_sends": []}
        cfg = {"configurable": {"thread_id": boot["thread_id"],
                                "checkpoint_id": "cp-race"}}
        await saver.aput_writes(cfg, [("messages", ["early"])], "task-race")
        await saver.aput({"configurable": {"thread_id": boot["thread_id"]}},
                         cp, {"source": "input"}, {})
        tup = await saver.aget_tuple(cfg)
        assert tup.pending_writes == [("task-race", "messages", ["early"])]


async def test_stale_epoch_cannot_record_writes(server):
    h = Harness(server)
    cfg = await h.open()
    server.re_claim(h.boot["fence"]["run_id"], worker="takeover")
    with pytest.raises(StateAPIError) as exc:
        await h.saver.aput_writes(cfg, [("messages", ["zombie"])], "task-g")
    assert exc.value.code in ("STALE_EPOCH", "LEASE_LOST", "PERMISSION_DENIED")
