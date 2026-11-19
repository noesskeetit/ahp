"""Block 1 / PDF §4.2 + §9.8 - serialization compatibility of snapshots.

Coverage row §9.8 "Смена формата снимка LangGraph" demands that a checkpoint
written today is still readable after the format moves. The saver therefore
never re-derives a checkpoint field by field: everything except
``channel_values`` rides as one opaque serde blob inside a **versioned**
envelope, so a future LangGraph key is carried verbatim instead of dropped.

It also never falls back to ``pickle`` (the old adapter did): the platform
must be able to store, replay and audit a snapshot from another process or
language, and ``pickle`` is neither portable nor safe. Serialization goes
through the LangGraph ``serde`` (ormsgpack + JSON-Plus), the same encoder
LangGraph's own savers use.
"""

from __future__ import annotations

import base64
import dataclasses
import json
import uuid

import pytest
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.types import Interrupt
from pydantic import BaseModel

from ahp_state import AHPCheckpointSaver, StateAPIClient, wire
from ahp_state.wire import WireError

from .fake_state import FakeState, FakeStateServer, StateDoc

class Point(BaseModel):
    x: int
    y: str


@dataclasses.dataclass
class Box:
    items: list


VALUES = {
    "messages": [HumanMessage(content="hi"),
                 AIMessage(content="", tool_calls=[{"name": "lookup",
                                                    "args": {"q": "a"}, "id": "c1"}]),
                 ToolMessage(content="42", tool_call_id="c1")],
    "pydantic": Point(x=1, y="a"),
    "dataclass": Box(items=[1, 2, 3]),
    "bytes": b"\x00\x01\xff",
    "number": 12345678901234567890,
    "nested": {"a": [1, {"b": None}], "c": {"d": True}},
    "interrupts": [Interrupt(value={"question": "confirm?"}, id="int-1")],
    "empty_channel": None,
}


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


def _saver(server):
    boot = server.bootstrap(f"ctx-{uuid.uuid4().hex[:8]}")
    client = StateAPIClient(server.url, token=boot["token"])
    return AHPCheckpointSaver(client, boot["thread_id"], boot["fence"]), boot


def _checkpoint(values: dict, versions: dict) -> dict:
    return {"v": 1, "id": f"cp-{uuid.uuid4().hex[:8]}", "ts": "2026-11-01",
            "channel_values": values, "channel_versions": versions,
            "versions_seen": {"node": dict(versions)}, "pending_sends": []}


async def test_every_channel_value_survives_the_round_trip(server):
    saver, boot = _saver(server)
    versions = {k: f"0000000000000000000000000000000{i}.0.1" for i, k in enumerate(VALUES)}
    cp = _checkpoint(dict(VALUES), versions)

    await saver.aput({"configurable": {"thread_id": boot["thread_id"]}}, cp,
                     {"source": "input"}, versions)
    tup = await saver.aget_tuple({"configurable": {"thread_id": boot["thread_id"],
                                                   "checkpoint_id": cp["id"]}})
    got = tup.checkpoint["channel_values"]
    for key, want in VALUES.items():
        assert repr(got[key]) == repr(want), f"{key}: {got[key]!r} != {want!r}"
    assert got["messages"][1].tool_calls == VALUES["messages"][1].tool_calls


async def test_no_pickle_anywhere_on_the_wire(server):
    """Pickle is neither portable nor safe; the State store must hold serde bytes."""
    saver, boot = _saver(server)
    versions = {k: f"0000000000000000000000000000000{i}.0.1" for i, k in enumerate(VALUES)}
    cp = _checkpoint(dict(VALUES), versions)
    await saver.aput({"configurable": {"thread_id": boot["thread_id"]}}, cp,
                     {"source": "input"}, versions)
    await saver.aput_writes({"configurable": {"thread_id": boot["thread_id"],
                                              "checkpoint_id": cp["id"]}},
                            [("values", VALUES)], "task-1")

    # JsonPlusSerializer has a pickle fallback; reaching it would make a snapshot
    # unreadable from another process/version, so the serde *type tag* is checked.
    allowed = {"msgpack", "json", "bytes", "empty", "null"}
    stored = []
    for data in server.state.doc.d["blobs"].values():
        stored.append(json.loads(base64.b64decode(data))["t"])
    for row in server.state.doc.d["writes"].values():
        stored.append(json.loads(base64.b64decode(row["data"]))["t"])
    assert stored, "nothing was stored"
    assert set(stored) <= allowed, f"non-portable serde types: {set(stored) - allowed}"


async def test_future_checkpoint_keys_are_carried_verbatim(server):
    """A LangGraph format change must not lose state (§9.8)."""
    saver, boot = _saver(server)
    versions = {"messages": "00000000000000000000000000000001.0.1"}
    cp = _checkpoint({"messages": ["x"]}, versions)
    cp["channel_values"]["messages"] = ["x"]
    cp["future_field"] = {"added_by": "langgraph-next", "values": [1, 2, 3]}
    cp["updated_channels"] = ["messages"]

    await saver.aput({"configurable": {"thread_id": boot["thread_id"]}}, cp,
                     {"source": "input"}, versions)
    tup = await saver.aget_tuple({"configurable": {"thread_id": boot["thread_id"],
                                                   "checkpoint_id": cp["id"]}})
    assert tup.checkpoint["future_field"] == cp["future_field"]
    assert tup.checkpoint["versions_seen"] == cp["versions_seen"]
    assert tup.checkpoint["updated_channels"] == ["messages"]


async def test_foreign_envelope_fails_loudly(server):
    """A snapshot we did not write must never decode into an empty checkpoint."""
    saver, boot = _saver(server)
    cp = _checkpoint({"messages": ["x"]}, {"messages": "1"})
    await saver.aput({"configurable": {"thread_id": boot["thread_id"]}}, cp,
                     {"source": "input"}, {"messages": "1"})

    key = wire.__dict__  # touch to keep the import meaningful
    stored_key = json.dumps([boot["thread_id"], "graph", cp["id"]])
    header = server.state.doc.d["checkpoints"][stored_key]
    header["metadata"] = {"source": "input"}          # a foreign writer's shape
    with pytest.raises(WireError):
        await saver.aget_tuple({"configurable": {"thread_id": boot["thread_id"],
                                                 "checkpoint_id": cp["id"]}})


async def test_envelope_version_is_checked():
    serde = JsonPlusSerializer()
    envelope = wire.pack_envelope(serde, {"v": 1}, {"source": "input"}, None)
    body, metadata, parent = wire.unpack_envelope(serde, envelope)
    assert (body, metadata, parent) == ({"v": 1}, {"source": "input"}, None)
    envelope["sdk"] = 99
    with pytest.raises(WireError):
        wire.unpack_envelope(serde, envelope)


async def test_a_fresh_saver_instance_reads_the_same_bytes(server):
    """No process-local cache may be the only place a value exists."""
    saver_a, boot = _saver(server)
    versions = {"messages": "00000000000000000000000000000001.0.1"}
    cp = _checkpoint({"messages": ["shared"]}, versions)
    await saver_a.aput({"configurable": {"thread_id": boot["thread_id"]}}, cp,
                       {"source": "input"}, versions)

    saver_b = AHPCheckpointSaver(StateAPIClient(server.url, token=boot["token"]),
                                 boot["thread_id"], boot["fence"])
    tup = await saver_b.aget_tuple({"configurable": {"thread_id": boot["thread_id"],
                                                     "checkpoint_id": cp["id"]}})
    assert tup.checkpoint["channel_values"]["messages"] == ["shared"]


@pytest.mark.parametrize("channel", [
    "branch:to:node",                      # LangGraph branch channels
    "__pregel_tasks",                      # internal channels
    "uživatel",                            # non-ASCII
    "has space",
    "pipe|channel",
    "x" * 400,
])
def test_identifier_encoding_is_reversible(channel):
    assert wire.decode_id(wire.encode_id(channel)) == channel
    encoded = wire.encode_id(channel)
    assert len(encoded.encode()) <= 512
    assert all(c.isalnum() or c in "._-:@/" for c in encoded), encoded


def test_blob_packing_is_typed_and_opaque():
    serde = JsonPlusSerializer()
    data = wire.dump_value(serde, {"a": 1})
    assert wire.unpack_blob(data)[0] in ("msgpack", "json")
    assert wire.load_value(serde, data) == {"a": 1}
    assert wire.is_empty_blob(wire.empty_blob()) is True
    assert wire.is_empty_blob(data) is False


def test_malformed_blob_is_explicit():
    with pytest.raises(WireError):
        wire.unpack_blob(base64.b64encode(b"not-json").decode())
