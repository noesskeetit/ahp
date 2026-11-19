"""Block 1 / PDF rev 3.2 §4.2 - AHPCheckpointSaver synchronous durability.

Requirement under test (docs/coverage.md §4.2 "AHPCheckpointSaver + durability=sync,
pending writes"; §9.8 "Смена формата снимка LangGraph"):

  * a checkpoint is durable **before** ``aput`` returns (no buffering, no batching);
  * replay of the identical write is idempotent, reuse with a different body fails;
  * a lost response after a durable commit never produces a second write;
  * a stale lease epoch is surfaced, not swallowed;
  * the server holds opaque bytes only; the SDK re-reads parts by
    ``(channel, version)`` and the read path never carries blob bytes.

The double in :mod:`.fake_state` mirrors the real State contract (see its
docstring for the mirrored sources), so a green run here is evidence about the
SDK - not about the platform, which needs a live stand.
"""

from __future__ import annotations

import base64
import json
import uuid

import pytest

from ahp_state import AHPCheckpointSaver, StateAPIClient, StateAPIError

from .fake_state import FakeState, FakeStateServer, StateDoc

def _checkpoint(**over) -> dict:
    cp = {
        "v": 1,
        "id": f"cp-{uuid.uuid4().hex}",
        "ts": "2026-11-01T00:00:00.000000+00:00",
        "channel_values": {"messages": [{"role": "user", "content": "hello"}]},
        "channel_versions": {"messages": "00000000000000000000000000000001.0.1"},
        "versions_seen": {"agent": {"messages": "00000000000000000000000000000001.0.1"}},
        "pending_sends": [],
        "updated_channels": ["messages"],
    }
    cp.update(over)
    return cp


def _config(thread_id: str, checkpoint_id: str | None = None) -> dict:
    # The root graph's LangGraph namespace is empty; "graph" is the *AHP*
    # namespace the saver owns (see AHPCheckpointSaver._ns_of).
    cfg = {"thread_id": thread_id}
    if checkpoint_id is not None:
        cfg["checkpoint_id"] = checkpoint_id
    return {"configurable": cfg}


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


class Harness:
    """One thread + run + lease, plus a saver bound to that lease."""

    def __init__(self, server: FakeStateServer):
        self.server = server
        self.boot = server.bootstrap(f"ctx-{uuid.uuid4().hex[:8]}")
        self.client = StateAPIClient(server.url, token=self.boot["token"])
        self.saver = AHPCheckpointSaver(self.client, self.boot["thread_id"],
                                        self.boot["fence"], namespace="graph")

    def reader(self) -> AHPCheckpointSaver:
        """An independent process-like reader: fresh client, fresh caches."""
        fresh = StateAPIClient(self.server.url, token=self.boot["token"])
        return AHPCheckpointSaver(fresh, self.boot["thread_id"],
                                  self.boot["fence"], namespace="graph")

    async def stored(self, cp_id: str) -> dict:
        return await self.client.get_checkpoint(self.boot["thread_id"], "graph", cp_id)


async def test_checkpoint_is_durable_when_aput_returns(server):
    h = Harness(server)
    cp = _checkpoint()

    config = await h.saver.aput(_config(h.boot["thread_id"]), cp, {"source": "input"},
                                {"messages": cp["channel_versions"]["messages"]})
    assert config["configurable"]["checkpoint_id"] == cp["id"]

    tup = await h.reader().aget_tuple(_config(h.boot["thread_id"], cp["id"]))
    assert tup is not None, "checkpoint not visible to an independent reader"
    assert tup.checkpoint["channel_values"]["messages"] == cp["channel_values"]["messages"]
    assert tup.checkpoint["channel_versions"] == cp["channel_versions"]
    assert tup.checkpoint["updated_channels"] == ["messages"], \
        "unknown/future checkpoint keys must survive the round-trip unchanged"


async def test_put_is_not_batched(server):
    """One ``aput`` is one PutCheckpoint round-trip; nothing waits for a flush."""
    h = Harness(server)
    for _ in range(3):
        cp = _checkpoint()
        await h.saver.aput(_config(h.boot["thread_id"]), cp, {"source": "loop"},
                           {"messages": cp["channel_versions"]["messages"]})
        listed = await h.client.list_checkpoints(h.boot["thread_id"], "graph", 10)
        assert listed[0]["id"] == cp["id"], "write was deferred, not synchronous"


async def test_replay_of_identical_checkpoint_is_idempotent(server):
    h = Harness(server)
    cp = _checkpoint()
    cfg = _config(h.boot["thread_id"])

    version = cp["channel_versions"]["messages"]
    await h.saver.aput(cfg, cp, {"source": "input"}, {"messages": version})
    again = await h.saver.aput(cfg, cp, {"source": "input"}, {"messages": version})
    assert again["configurable"]["checkpoint_id"] == cp["id"]

    listed = await h.client.list_checkpoints(h.boot["thread_id"], "graph", 10)
    assert [c["id"] for c in listed].count(cp["id"]) == 1


async def test_different_body_under_same_checkpoint_id_is_rejected(server):
    h = Harness(server)
    cp = _checkpoint()
    cfg = _config(h.boot["thread_id"])
    version = cp["channel_versions"]["messages"]
    await h.saver.aput(cfg, cp, {"source": "input"}, {"messages": version})

    corrupted = dict(cp, channel_values={"messages": [{"role": "user", "content": "evil"}]})
    with pytest.raises(StateAPIError) as exc:
        await h.saver.aput(cfg, corrupted, {"source": "input"}, {"messages": version})
    assert exc.value.code in ("CONFLICT", "KEY_REUSE_MISMATCH")

    tup = await h.saver.aget_tuple(_config(h.boot["thread_id"], cp["id"]))
    assert tup.checkpoint["channel_values"]["messages"][0]["content"] == "hello"


async def test_lost_response_after_commit_does_not_duplicate(server):
    """Commit-then-lose-response: the retry replays instead of writing twice."""
    h = Harness(server)
    cp = _checkpoint()
    cfg = _config(h.boot["thread_id"])

    version = cp["channel_versions"]["messages"]
    server.state.drop_response_after_commit("PutCheckpoint")
    with pytest.raises(StateAPIError):
        await h.saver.aput(cfg, cp, {"source": "input"}, {"messages": version})

    listed = await h.client.list_checkpoints(h.boot["thread_id"], "graph", 10)
    assert [c["id"] for c in listed].count(cp["id"]) == 1, "durable commit must have landed"

    server.state._drop_after_commit.clear()
    await h.saver.aput(cfg, cp, {"source": "input"}, {"messages": version})   # replay
    listed = await h.client.list_checkpoints(h.boot["thread_id"], "graph", 10)
    assert [c["id"] for c in listed].count(cp["id"]) == 1


async def test_stale_epoch_is_surfaced_not_swallowed(server):
    h = Harness(server)
    server.re_claim(h.boot["fence"]["run_id"], worker="takeover")

    with pytest.raises(StateAPIError) as exc:
        await h.saver.aput(_config(h.boot["thread_id"]), _checkpoint(), {"source": "loop"},
                           {"messages": "1"})
    assert exc.value.code in ("STALE_EPOCH", "LEASE_LOST", "PERMISSION_DENIED")


async def test_server_stores_opaque_bytes_only(server):
    h = Harness(server)
    cp = _checkpoint(channel_values={"messages": [{"role": "user", "content": "x"}],
                                     "counter": 7})
    await h.saver.aput(_config(h.boot["thread_id"]), cp, {"source": "input"},
                       {"messages": cp["channel_versions"]["messages"], "counter": "1"})

    header = await h.stored(cp["id"])
    envelope = json.loads(json.dumps(header["metadata"]))
    assert isinstance(envelope, dict), "the metadata envelope must be valid JSON"

    stored = server.state.doc.d
    for part in header["parts"]:
        blob = stored["blobs"][json.dumps(
            [h.boot["thread_id"], "graph", part["channel"], part["version"]])]
        packed = json.loads(base64.b64decode(blob))
        assert set(packed) >= {"t", "d"}, "parts must hold a typed serde tuple"
        base64.b64decode(packed["d"])            # opaque payload, never parsed by the server
        assert not isinstance(packed["d"], (dict, list))


async def test_reads_return_part_references_not_bytes(server):
    h = Harness(server)
    cp = _checkpoint()
    await h.saver.aput(_config(h.boot["thread_id"]), cp, {"source": "input"},
                       {"messages": cp["channel_versions"]["messages"]})

    header = await h.stored(cp["id"])
    assert header["parts"], "the checkpoint must carry part references"
    assert all("data" not in p for p in header["parts"]), \
        "reads must return references; the SDK must re-fetch each blob"

    tup = await h.reader().aget_tuple(_config(h.boot["thread_id"], cp["id"]))
    assert tup.checkpoint["channel_values"] == cp["channel_values"]


async def test_channel_values_from_earlier_checkpoints_are_reassembled(server):
    """channel_versions may point at blobs written by an older checkpoint."""
    h = Harness(server)
    first = _checkpoint(channel_values={"messages": [{"role": "user", "content": "one"}]},
                        channel_versions={"messages": "00000000000000000000000000000001.0.1"})
    await h.saver.aput(_config(h.boot["thread_id"]), first, {"source": "input"},
                       {"messages": "00000000000000000000000000000001.0.1"})

    second = _checkpoint(channel_values={"messages": [{"role": "user", "content": "one"}],
                                         "counter": 3},
                         channel_versions={"messages": "00000000000000000000000000000001.0.1",
                                           "counter": "00000000000000000000000000000002.0.1"})
    await h.saver.aput(_config(h.boot["thread_id"]), second, {"source": "loop"},
                       {"counter": "00000000000000000000000000000002.0.1"})

    tup = await h.reader().aget_tuple(_config(h.boot["thread_id"], second["id"]))
    assert tup.checkpoint["channel_values"]["counter"] == 3
    assert tup.checkpoint["channel_values"]["messages"][0]["content"] == "one", \
        "an unchanged channel must be re-read from its original blob version"


async def test_get_tuple_absent_returns_none(server):
    h = Harness(server)
    assert await h.saver.aget_tuple(_config(h.boot["thread_id"], "cp-missing")) is None
    assert await h.saver.aget_tuple(_config(h.boot["thread_id"])) is None


async def test_root_checkpoint_ns_stays_langgraphs_empty_string(server):
    """The AHP namespace is a wire detail - LangGraph must never see it.

    ``prepare_next_tasks`` hashes ``checkpoint_ns`` into every task id, so a
    saver that reports its own namespace here hands back pending writes whose
    task id cannot be matched to a task: interrupts silently disappear from
    ``aget_state`` even though they are durable.
    """
    h = Harness(server)
    cp = _checkpoint()

    cfg = await h.saver.aput(_config(h.boot["thread_id"]), cp, {"source": "input"},
                             {"messages": cp["channel_versions"]["messages"]})
    assert cfg["configurable"]["checkpoint_ns"] == ""

    tup = await h.reader().aget_tuple(cfg)
    assert tup.config["configurable"]["checkpoint_ns"] == ""
    assert tup.parent_config is None
    assert (await h.stored(cp["id"]))["namespace"] == "graph"


async def test_nested_checkpoint_ns_round_trips_without_colliding(server):
    """A subgraph's checkpoints live under their own AHP namespace, reversibly."""
    from ahp_state import wire

    h = Harness(server)
    nested = "ask|approval"                     # LangGraph's own nested namespace
    cp = _checkpoint(id=f"cp-{uuid.uuid4().hex[:8]}",
                     channel_values={"messages": ["sub"]},
                     updated_channels=["messages"])
    cfg = await h.saver.aput({"configurable": {"thread_id": h.boot["thread_id"],
                                               "checkpoint_ns": nested}},
                             cp, {"source": "loop"},
                             {"messages": cp["channel_versions"]["messages"]})
    assert cfg["configurable"]["checkpoint_ns"] == nested

    tup = await h.reader().aget_tuple(cfg)
    assert tup.checkpoint["channel_values"]["messages"] == ["sub"]
    assert tup.config["configurable"]["checkpoint_ns"] == nested

    ahp_ns = f"graph/{wire.encode_id(nested)}"
    assert (await h.client.get_checkpoint(h.boot["thread_id"], ahp_ns, cp["id"]))["id"]
    # the root namespace of the same thread holds only root checkpoints
    root_ids = [c["id"] for c in await h.client.list_checkpoints(
        h.boot["thread_id"], "graph", 10)]
    assert cp["id"] not in root_ids
