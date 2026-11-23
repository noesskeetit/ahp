"""Request counts and fencing for the compact journal SDK and adapters."""

from contextlib import asynccontextmanager
from copy import deepcopy

import pytest
from langchain_core.messages import HumanMessage
from langgraph.store.base import GetOp, PutOp

from ahp_state import AHPJournalCheckpointSaver, AHPJournalClient, AHPJournalStore
from ahp_state.client import StateConflictError


class JournalTransport:
    def __init__(self):
        self.calls = []
        self.checkpoints = {}
        self.kv = {}
        self.stale = False
        self.control = None

    @asynccontextmanager
    async def fork_for_sync(self):
        yield self

    async def request_state(self, method, path, *, params=None, body=None):
        self.calls.append((method, path, params, body))
        if method == "POST" and self.stale:
            raise StateConflictError("STALE_EPOCH", "lease lost")
        if path.endswith(":acquire"):
            return {"epoch": 1}
        if path.endswith(":claim-turn"):
            return {"epoch": 1, "input_seq": 5}
        if path.endswith("/vm:claim"):
            return {"epoch": 1, "input_seq": body["turn_seq"]}
        if path.endswith("/vm:cancel"):
            return {"status": "ACTIVE"}
        if path.endswith(":heartbeat"):
            return {"epoch": 1, **({"control": self.control} if self.control else {})}
        if path.endswith("/events:append"):
            return {"head_seq": 20, "control": self.control}
        if path.endswith(("/writes", "/intents:mutate", "/tools:begin-standalone")):
            return {}
        if path.endswith("/checkpoints"):
            if method == "POST":
                checkpoint = deepcopy(body["checkpoint"])
                if "chat" in body:
                    checkpoint["chat_event_id"] = "chat:" + checkpoint["id"]
                self.checkpoints[checkpoint["id"]] = {"checkpoint": checkpoint, "writes": None}
                return {"checkpoint_id": checkpoint["id"], "control": self.control}
            if params.get("list") == "true":
                assert params["include_tuples"] == "true"
                requested = params.get("before")
                ids = [key for key in reversed(self.checkpoints) if not requested or key < requested]
                ids = ids[: params["limit"]]
                return {"checkpoint_ids": ids, "checkpoint_tuples": [self.checkpoints[key] for key in ids]}
            checkpoint_id = params.get("checkpoint_id") or next(reversed(self.checkpoints))
            return self.checkpoints[checkpoint_id]
        if path.endswith("/kv"):
            requested = params.get("key")
            keys = [requested] if isinstance(requested, str) else requested
            return [row for (namespace, key), row in self.kv.items()
                    if namespace == params["namespace"] and (not keys or key in keys)]
        if path.endswith("/kv:mutate"):
            for change in body["kv"]:
                k = (change["namespace"], change["key"])
                version = self.kv.get(k, {}).get("version", 0)
                assert version == change["expected_version"]
                self.kv[k] = {**change, "version": version + 1, "updated_at": "2026-11-19T00:00:00Z"}
            return {"first_seq": 42, "head_seq": 42}
        raise AssertionError((method, path))


def _checkpoint(i, messages):
    return {"id": f"cp-{i:03}", "channel_versions": {}, "channel_values": {"messages": messages}}


async def test_vm_claim_turn_scope_and_followup_share_the_lease_epoch():
    transport = JournalTransport()
    journal = AHPJournalClient(transport, "session", harness_type="langgraph")
    await journal.claim_vm_turn(turn_seq=5, generation=1, execution_id="exec-1")
    assert transport.calls[-1][1:] == (
        "/v1/state/sessions/session/vm:claim", None,
        {"turn_seq": 5, "generation": 1, "execution_id": "exec-1", "ttl_ms": 30000},
    )
    transport.control = {"turn_seq": 5, "pending_steer": [{"seq": 7, "turn_seq": 5}]}
    assert (await journal.heartbeat())["control"] == transport.control
    ack = {"turn_seq": 5, "up_to_seq": 7, "count": 1}
    await journal.append("event", 7, [], steer_ack=ack)
    assert transport.calls[-1][3]["turn_seq"] == 5
    assert transport.calls[-1][3]["steer_ack"] == ack
    await journal.put_writes("c", [])
    assert transport.calls[-1][3]["turn_seq"] == 5
    await journal.mutate_kv("kv", [])
    assert transport.calls[-1][3]["turn_seq"] == 5
    await journal.mutate_tool_intent({"call_id": "call", "status": "DISPATCHED"})
    assert transport.calls[-1][3]["transition"]["turn_seq"] == 5
    old_guard = journal.standalone_tools()
    await old_guard.begin("call", "tool", bytes(32))
    assert transport.calls[-1][3]["turn_seq"] == 5
    await journal.cancel_turn(turn_seq=5)
    assert transport.calls[-1][1:] == ("/v1/state/sessions/session/vm:cancel", None, {"turn_seq": 5})

    transport.control = {"turn_seq": 8, "next_turn": {"input_seq": 8, "epoch": 1}}
    await journal.append("finish", 7, [], finish_turn="IDLE")
    assert journal.epoch == 1 and journal.turn_seq == 8
    assert old_guard.fenced
    transport.control = {"turn_seq": 5, "cancel_requested": True}
    assert "control" not in await journal.heartbeat()
    await journal.put_checkpoint("old", {"id": "old"}, turn_seq=5)
    assert transport.calls[-1][3]["turn_seq"] == 5
    await journal.put_checkpoint("new", {"id": "new"})
    assert transport.calls[-1][3]["turn_seq"] == 8
    transport.control = None
    await journal.append("finish-next", 20, [], finish_turn="IDLE")
    assert journal.turn_seq is None


async def test_pull_turn_does_not_send_vm_control_fields():
    transport = JournalTransport()
    journal = AHPJournalClient(transport, "session", harness_type="langgraph")
    await journal.claim_turn()
    await journal.append("event", 5, [])
    assert "turn_seq" not in transport.calls[-1][3]


async def test_checkpoint_saver_acks_with_materialized_input_and_pins_the_turn():
    transport = JournalTransport()
    journal = AHPJournalClient(transport, "session", harness_type="langgraph")
    await journal.claim_vm_turn(turn_seq=5, generation=1, execution_id="exec-1")
    saver = AHPJournalCheckpointSaver(transport, "session", journal, chat_channel="messages")
    transport.control = {"pending_steer": [{"seq": 7, "turn_seq": 5}]}
    config = {"configurable": {"thread_id": "session"}}
    config = await saver.aput(config, _checkpoint(1, []), {}, {})
    assert "steer_ack" not in transport.calls[-1][3]
    assert saver.control == transport.control
    ack = {"turn_seq": 5, "up_to_seq": 7, "count": 1}
    transport.control = {"cancel_requested": True}
    config = await saver.aput(config, _checkpoint(2, [HumanMessage(id="steer-7", content="stop")]),
                              {}, {}, steer_ack=ack)
    assert transport.calls[-1][3]["steer_ack"] == ack
    assert transport.calls[-1][3]["turn_seq"] == 5
    assert transport.calls[-1][3]["chat"]["upserts"][0]["content"] == "stop"
    assert saver.control == transport.control

    transport.control = {"next_turn": {"input_seq": 8, "epoch": 1}}
    await journal.append("finish", 7, [], finish_turn="IDLE")
    assert saver.control is None
    await saver.aput_writes(config, [("node", "old turn")], "task")
    assert transport.calls[-1][3]["turn_seq"] == 5


async def test_checkpoint_chat_hot_path_reads_current_tuple_and_lists_in_one_get():
    transport = JournalTransport()
    saver = AHPJournalCheckpointSaver(transport, "session", {"epoch": 1}, chat_channel="messages")
    config = {"configurable": {"thread_id": "session"}}
    messages = []
    for i in range(110):
        messages.append(HumanMessage(id=str(i), content=f"message {i}"))
        transport.calls.clear()
        config = await saver.aput(config, _checkpoint(i, messages), {"step": i}, {})
        assert [call[0] for call in transport.calls] == ["POST"]
        assert transport.calls[0][3]["chat"]["upserts"][-1]["id"] == str(i)
    transport.calls.clear()
    assert (await saver.aget_tuple(config)).checkpoint["id"] == "cp-109"
    assert [call[0] for call in transport.calls] == ["GET"]
    transport.calls.clear()
    page = [item async for item in saver.alist(config, limit=20)]
    assert [item.metadata["step"] for item in page] == list(range(109, 89, -1))
    assert [call[0] for call in transport.calls] == ["GET"]


async def test_store_batches_puts_and_preserves_get_order_without_a_session_read():
    transport = JournalTransport()
    store = AHPJournalStore(transport, "session", 1)
    ns = ("ns",)
    results = await store.abatch([
        PutOp(ns, "a", {"v": 1}), PutOp(ns, "b", {"v": 2}), GetOp(ns, "a"),
        PutOp(ns, "a", {"v": 3}), PutOp(ns, "a", {"v": 4}), GetOp(ns, "a"),
    ])
    assert results[2].value == {"v": 1}
    assert results[-1].value == {"v": 4}
    posts = [call for call in transport.calls if call[0] == "POST"]
    assert len(posts) == 2
    assert all(call[1] == "/v1/state/kv:mutate" and "expected_seq" not in call[3] for call in posts)
    assert [call[1] for call in transport.calls if call[0] == "GET"] == ["/v1/state/kv"] * 4


async def test_vm_store_writes_are_bound_to_the_invocation_turn():
    transport = JournalTransport()
    store = AHPJournalStore(transport, "session", 1, turn_seq=5)
    await store.abatch([PutOp(("ns",), "a", {"v": 1})])
    assert transport.calls[-1][3]["turn_seq"] == 5


async def test_lease_context_fences_and_does_not_release_an_obsolete_lease():
    transport = JournalTransport()
    journal = AHPJournalClient(transport, "session", harness_type="adk")
    with pytest.raises(StateConflictError, match="lease lost"):
        async with journal.lease(ttl_ms=200):
            transport.stale = True
            await journal.heartbeat()
    assert journal.fenced
    assert not any((body or {}).get("release") for _, _, _, body in transport.calls)


async def test_chat_cache_is_discarded_on_release_and_rejects_large_projections():
    transport = JournalTransport()
    journal = AHPJournalClient(transport, "session", harness_type="langgraph")
    await journal.acquire()
    saver = AHPJournalCheckpointSaver(transport, "session", journal, chat_channel="messages")
    config = {"configurable": {"thread_id": "session"}}
    messages = [HumanMessage(id=f"{i:04}-" + "x" * 110, content="message") for i in range(350)]
    config = await saver.aput(config, _checkpoint(1, messages), {}, {})
    assert saver._chat_slot[0] is None
    transport.calls.clear()
    config = await saver.aput(config, _checkpoint(2, messages[:1]), {}, {})
    assert [call[0] for call in transport.calls] == ["GET", "POST"]
    assert saver._chat_slot[0] is not None
    await journal.release()
    assert saver._chat_slot[0] is None


def test_sync_client_maintains_lease_and_sync_list_batches_tuples():
    from ahp_state import SyncAHPJournalClient

    transport = JournalTransport()
    sync = SyncAHPJournalClient(transport, "session", harness_type="langgraph")
    sync.acquire()
    assert sync.epoch == 1
    assert sync.heartbeat()["epoch"] == 1
    saver = AHPJournalCheckpointSaver(transport, "session", {"epoch": sync.epoch}, chat_channel="messages")
    config = {"configurable": {"thread_id": "session"}}
    config = saver.put(config, _checkpoint(1, []), {"step": 1}, {})
    transport.calls.clear()
    assert [row.metadata for row in saver.list(config, limit=1)] == [{"step": 1}]
    assert [call[0] for call in transport.calls] == ["GET"]
    sync.release()
