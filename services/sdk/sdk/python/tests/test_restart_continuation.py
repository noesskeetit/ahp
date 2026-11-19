"""E2E: Crash-restart continuation through SDK client.

Scenario (pilot requirement):
  Process A: Claim run → PutCheckpoint (superstep 1) → PutWrites (pending)
  [HARD CRASH — client closed, process "dies"]
  Process B (new): Claim same run (new epoch) → GetCheckpoint (latest) → GetPart
                   → GetWrites → apply pending writes → PutCheckpoint (superstep 2)

Proves: state persists across fresh adapter/process via State HTTP.
Requires a live stand, found through the hand-out in ``AHP_STATE_HANDSHAKE``
(mTLS); tunnel variables remain overrides - see ``tests/_live_stand.py``.
"""
import json
import os
import ssl
import sys
import uuid
import base64
import asyncio

import pytest
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ahp_state.client import StateAPIClient, StateAPIError
from ahp_state.checkpoint_saver import AHPCheckpointSaver

from _live_stand import STAND, claim_own_run


def _make_client(token: str = None) -> StateAPIClient:
    return STAND.client(token)


def _get_token() -> str:
    """The task token the hand-out was issued with (never hardcoded)."""
    return STAND.token


pytestmark = STAND.skip_mark()


@pytest.mark.asyncio
async def test_crash_restart_continuation():
    """Full crash-restart: A writes cp+writes, B reads and continues."""
    token = _get_token()

    # === PROCESS A ===
    client_a = _make_client(token)
    uid = uuid.uuid4().hex[:8]

    # A: create the work, then claim it. This file used to claim whatever the
    # queue happened to hold, which made it unreproducible on its own: run it
    # against an empty scope and `lease` came back null.
    thread = await client_a.create_thread(f"ctx-restart-{uid}", f"restart-{uid}")
    created = await client_a.create_run(thread["id"], f"run-restart-{uid}",
                                        payload={"task": "crash-restart"})
    lease = await claim_own_run(client_a, created["id"], f"restart-a-{uid}")
    thread_id = lease["thread"]["id"]
    run_id = lease["run"]["id"]
    assert run_id == created["id"], "the stand adopted a run other than the one created"
    worker_a = lease["worker"]
    epoch_a = lease["epoch"]
    fence_a = {"run_id": run_id, "worker": worker_a, "epoch": epoch_a}
    print(f"\n[A] claimed: thread={thread_id[-12:]} run={run_id[-12:]} epoch={epoch_a}")

    # A: Put checkpoint (superstep 1)
    saver_a = AHPCheckpointSaver(client_a, thread_id, fence_a, namespace="graph")
    cp1_id = f"cp-s1-{uid}"
    checkpoint_s1 = {
        "v": 1,
        "id": cp1_id,
        "ts": "2026-10-31T15:20:00Z",
        "channel_values": {
            "messages": [{"role": "user", "content": "restart-test"}],
            "step": 1,
        },
        "channel_versions": {"messages": 1, "step": 1},
        "versions_seen": {},
        "pending_sends": [],
    }
    config_a = {"configurable": {"thread_id": thread_id, "checkpoint_id": ""}}
    result = await saver_a.aput(config_a, checkpoint_s1, {"source": "process-a", "step": 1}, {"messages": 1, "step": 1})
    assert result["configurable"]["checkpoint_id"] == cp1_id, f"put failed: {result}"
    print(f"[A] PutCheckpoint s1: {cp1_id}")

    # A: Put writes (pending, not yet checkpointed)
    write_config = {"configurable": {"thread_id": thread_id, "checkpoint_id": cp1_id}}
    await saver_a.aput_writes(
        write_config,
        [("messages", [{"role": "assistant", "content": "response from A"}]), ("step", 2)],
        task_id="task-restart",
    )
    print(f"[A] PutWrites: 2 pending writes")

    # A: HARD CRASH (close client, simulate process death)
    await client_a.close()
    print(f"[A] *** CRASH *** (client closed)")

    # Put A's run back into the queue for B to adopt. Measured against the live
    # stand: `ReleaseRun` accepts exactly done | failed | canceled | pending, and
    # only `pending` leaves the run claimable again - `failed` and `canceled` are
    # terminal, and `crashed`, which this file used to send, is INVALID_ARGUMENT.
    # That error was swallowed by `except StateAPIError: pass`, so A's lease
    # stayed alive and phase B could never claim the run: the scenario as written
    # could not have worked. Production reaches the same state by letting the
    # lease TTL expire; this is the explicit, fast version of that.
    release_client = _make_client(token)
    await release_client.release(fence_a, status="pending")
    await release_client.close()
    await asyncio.sleep(0.5)

    # === PROCESS B (new process, new client) ===
    client_b = _make_client(token)

    # B: Claim — drain FIFO until we get A's run (it was just released)
    lease_b = await claim_own_run(client_b, run_id, f"restart-b-{uid}")

    thread_id_b = lease_b["thread"]["id"]
    run_id_b = lease_b["run"]["id"]
    worker_b = lease_b["worker"]
    epoch_b = lease_b["epoch"]
    fence_b = {"run_id": run_id_b, "worker": worker_b, "epoch": epoch_b}
    print(f"\n[B] claimed: thread={thread_id_b[-12:]} run={run_id_b[-12:]} epoch={epoch_b}")

    # Verify same thread and run
    assert thread_id_b == thread_id, f"Thread mismatch: {thread_id_b} != {thread_id}"
    assert run_id_b == run_id, f"Run mismatch: {run_id_b} != {run_id}"
    assert epoch_b >= epoch_a, f"Epoch not advanced: {epoch_b} < {epoch_a}"
    print(f"[B] Same thread+run confirmed, epoch: {epoch_a} → {epoch_b}")

    # B: Read latest checkpoint via SDK
    saver_b = AHPCheckpointSaver(client_b, thread_id_b, fence_b, namespace="graph")
    read_config = {"configurable": {"thread_id": thread_id_b, "checkpoint_id": ""}}
    tuple_result = await saver_b.aget_tuple(read_config)
    assert tuple_result is not None, "B: get_tuple returned None — checkpoint lost after crash!"
    assert tuple_result.checkpoint["id"] == cp1_id, \
        f"B: wrong checkpoint: {tuple_result.checkpoint['id']} != {cp1_id}"
    assert tuple_result.checkpoint["channel_values"]["step"] == 1, \
        f"B: wrong step: {tuple_result.checkpoint['channel_values'].get('step')}"
    print(f"[B] GetCheckpoint: id={tuple_result.checkpoint['id']} step=1 ✅")

    # B: Read pending writes
    writes = tuple_result.pending_writes
    assert len(writes) >= 1, f"B: no pending writes found (got {len(writes)})"
    print(f"[B] GetWrites: {len(writes)} pending writes ✅")
    # Verify the write content. pending_writes is a sequence of
    # (task_id, channel, value) triples - the shape the saver actually returns
    # (see ahp_state/tests/test_pending_writes.py) - not objects with attributes;
    # this file was written against an older langgraph and never ran to find out.
    write_channels = [w[1] for w in writes]
    assert "messages" in write_channels, f"B: 'messages' write not found: {write_channels}"
    assert "step" in write_channels, f"B: 'step' write not found: {write_channels}"

    # B: Apply pending writes and persist superstep 2
    cp2_id = f"cp-s2-{uid}"
    checkpoint_s2 = {
        "v": 2,
        "id": cp2_id,
        "ts": "2026-10-31T15:21:00Z",
        "channel_values": {
            "messages": [
                {"role": "user", "content": "restart-test"},
                {"role": "assistant", "content": "response from A"},
            ],
            "step": 2,
        },
        "channel_versions": {"messages": 2, "step": 2},
        "versions_seen": {},
        "pending_sends": [],
    }
    config_b = {"configurable": {"thread_id": thread_id_b, "checkpoint_id": cp1_id}}
    result_b = await saver_b.aput(config_b, checkpoint_s2, {"source": "process-b", "step": 2}, {"messages": 2, "step": 2})
    assert result_b["configurable"]["checkpoint_id"] == cp2_id, f"B: put s2 failed: {result_b}"
    print(f"[B] PutCheckpoint s2: {cp2_id} ✅")

    # B: Verify s2 is now the latest
    verify_config = {"configurable": {"thread_id": thread_id_b, "checkpoint_id": ""}}
    verify = await saver_b.aget_tuple(verify_config)
    assert verify is not None, "B: verify get_tuple returned None"
    assert verify.checkpoint["id"] == cp2_id, f"B: verify wrong: {verify.checkpoint['id']} != {cp2_id}"
    assert verify.checkpoint["channel_values"]["step"] == 2, \
        f"B: verify wrong step: {verify.checkpoint['channel_values'].get('step')}"
    assert len(verify.checkpoint["channel_values"]["messages"]) == 2, \
        f"B: verify messages count: {len(verify.checkpoint['channel_values']['messages'])}"
    print(f"[B] Verify: latest=s2, step=2, messages=2 ✅")

    # B: Cleanup
    await client_b.release(fence_b, status="done")
    await client_b.close()
    print(f"\n[B] DONE: crash-restart continuation verified")
