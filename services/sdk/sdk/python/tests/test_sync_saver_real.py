"""Sync-saver test against a live State stand, over mTLS.

Verifies that the full sync interface (put, get_tuple, put_writes, list, delete_thread)
works against the real State HTTP server with mTLS.

The stand comes from the hand-out in ``AHP_STATE_HANDSHAKE`` (URL, CA, client
cert/key, task token), so any lane that owns a stand runs this file unchanged;
the old tunnel variables still work as overrides - see ``tests/_live_stand.py``.
"""
import json
import os
import ssl
import sys
import uuid

import pytest
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ahp_state.client import StateAPIClient, StateAPIError, StateTimeoutError
from ahp_state.checkpoint_saver import AHPCheckpointSaver

from _live_stand import STAND


def _get_bootstrap_token() -> str:
    """The task token the hand-out was issued with (never hardcoded)."""
    return STAND.token


def make_mtls_client(token: str = None) -> StateAPIClient:
    return STAND.client(token)


async def _setup() -> tuple:
    """Set up a run for sync-saver testing. Returns (client, fence, thread_id)."""
    token = _get_bootstrap_token()
    client = make_mtls_client(token)
    uid = uuid.uuid4().hex[:8]

    thread = await client.create_thread(f"ctx-sync-{uid}", f"sync-{uid}")
    await client.create_run(thread["id"], f"run-{uid}", payload={"task": "sync-saver-test"})
    await client.close()

    claim_client = make_mtls_client(None)
    claim = await claim_client.claim_run("sync-saver-test", ttl_ms=120000)
    lease = claim["lease"]
    thread_id = lease["thread"]["id"]
    fence = {
        "run_id": lease["run"]["id"],
        "worker": lease["worker"],
        "epoch": lease["epoch"],
    }
    kv_client = make_mtls_client(claim["task_token"])
    await claim_client.close()

    return kv_client, fence, thread_id


pytestmark = STAND.skip_mark()


@pytest.mark.asyncio
async def test_sync_saver_full_roundtrip():
    """Full sync-saver roundtrip: put → get_tuple → put_writes → list → delete_thread."""
    client, fence, thread_id = await _setup()
    saver = AHPCheckpointSaver(client, thread_id, fence, namespace="graph")

    # 1. put (sync)
    checkpoint = {
        "v": 1,
        "id": f"cp-{uuid.uuid4().hex[:8]}",
        "ts": "2026-10-31T14:00:00Z",
        "channel_values": {"messages": [{"role": "user", "content": "hello"}]},
        "channel_versions": {"messages": 1},
        "versions_seen": {"agent": {"messages": 1}},
        "pending_sends": [],
    }
    config = {"configurable": {"thread_id": thread_id, "checkpoint_id": ""}}
    result_config = saver.put(config, checkpoint, {"source": "test"}, {"messages": 1})
    assert result_config["configurable"]["checkpoint_id"] == checkpoint["id"], \
        f"put() did not return checkpoint_id: {result_config}"

    # 2. get_tuple (sync) - read back
    read_config = {"configurable": {"thread_id": thread_id, "checkpoint_id": checkpoint["id"]}}
    tuple_result = saver.get_tuple(read_config)
    assert tuple_result is not None, "get_tuple returned None after put"
    assert tuple_result.checkpoint["id"] == checkpoint["id"], \
        f"Wrong checkpoint id: {tuple_result.checkpoint['id']}"
    assert tuple_result.checkpoint["channel_values"]["messages"] == [{"role": "user", "content": "hello"}], \
        f"channel_values mismatch: {tuple_result.checkpoint['channel_values']}"

    # 3. put_writes (sync)
    write_config = {"configurable": {"thread_id": thread_id, "checkpoint_id": checkpoint["id"]}}
    saver.put_writes(write_config, [("messages", [{"role": "assistant", "content": "hi"}])], "task-1")

    # 4. list (sync)
    results = list(saver.list({"configurable": {"thread_id": thread_id, "checkpoint_id": ""}}))
    assert len(results) >= 1, f"list() returned {len(results)} checkpoints, expected >= 1"
    assert results[0].checkpoint["id"] == checkpoint["id"], \
        f"list() returned wrong checkpoint: {results[0].checkpoint['id']}"

    # 5. delete_thread (sync)
    saver.delete_thread(thread_id)

    # Verify thread is gone (get_tuple should return None)
    tuple_after_delete = saver.get_tuple(read_config)
    assert tuple_after_delete is None, "get_tuple should return None after delete_thread"

    # Cleanup (fresh client — sync _run_sync closed the original event loop)
    try:
        cleanup_client = make_mtls_client(_get_bootstrap_token())
        await cleanup_client.release(fence, status="done")
        await cleanup_client.close()
    except (StateTimeoutError, StateAPIError, RuntimeError):
        pass
    try:
        await client.close()
    except RuntimeError:
        pass


@pytest.mark.asyncio
async def test_sync_saver_put_and_reread():
    """Minimal sync-saver: put two checkpoints, read both back."""
    client, fence, thread_id = await _setup()
    saver = AHPCheckpointSaver(client, thread_id, fence, namespace="graph")

    cp1_id = f"cp1-{uuid.uuid4().hex[:6]}"
    cp2_id = f"cp2-{uuid.uuid4().hex[:6]}"

    # Put first checkpoint
    cp1 = {
        "v": 1, "id": cp1_id, "ts": "2026-10-31T14:00:01Z",
        "channel_values": {"step": "one"}, "channel_versions": {"step": 1},
        "versions_seen": {}, "pending_sends": [],
    }
    saver.put({"configurable": {"thread_id": thread_id, "checkpoint_id": ""}},
              cp1, {"source": "t"}, {"step": 1})

    # Put second checkpoint
    cp2 = {
        "v": 1, "id": cp2_id, "ts": "2026-10-31T14:00:02Z",
        "channel_values": {"step": "two"}, "channel_versions": {"step": 2},
        "versions_seen": {"agent": {"step": 2}}, "pending_sends": [],
    }
    saver.put({"configurable": {"thread_id": thread_id, "checkpoint_id": cp1_id}},
              cp2, {"source": "t"}, {"step": 2})

    # Read back both
    t1 = saver.get_tuple({"configurable": {"thread_id": thread_id, "checkpoint_id": cp1_id}})
    t2 = saver.get_tuple({"configurable": {"thread_id": thread_id, "checkpoint_id": cp2_id}})

    assert t1 is not None, "First checkpoint not found"
    assert t1.checkpoint["id"] == cp1_id
    assert t1.checkpoint["channel_values"]["step"] == "one"

    assert t2 is not None, "Second checkpoint not found"
    assert t2.checkpoint["id"] == cp2_id
    assert t2.checkpoint["channel_values"]["step"] == "two"

    # Cleanup (fresh client — sync _run_sync closed the original event loop)
    try:
        cleanup_client = make_mtls_client(_get_bootstrap_token())
        await cleanup_client.release(fence, status="done")
        await cleanup_client.close()
    except (StateTimeoutError, StateAPIError, RuntimeError):
        pass
    try:
        await client.close()
    except RuntimeError:
        pass
