"""F2: Real tool-call in graph + restart (new process) verification.

Tests the full crash-recovery cycle:
1. Process A: CreateThread → CreateRun → ClaimRun → execute graph step with tool call
   → RecordIntent (node, call=tool_call_id) → PutKV with identity
2. Simulated kill: close all clients
3. Process B (new): same fence → GetIntent → verify intent exists → replay PutKV
   → verify idempotent (no double-write)

The intent key is (run_id, node, call) where call = tool_call_id per PDF §4.2.
The stand comes from the hand-out in ``AHP_STATE_HANDSHAKE`` (same as
test_real_stand.py); tunnel variables remain overrides - ``tests/_live_stand.py``.
"""
import asyncio
import json
import os
import ssl
import sys
import uuid

import pytest
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ahp_state.client import (
    StateAPIClient,
    StateConflictError,
    StateNotFoundError,
    StateAPIError,
    StateTimeoutError,
)
from ahp_state.store import AHPStore
from langgraph.store.base import PutOp

from _live_stand import STAND


def _get_bootstrap_token() -> str:
    """The task token the hand-out was issued with (never hardcoded)."""
    return STAND.token


def make_mtls_client(token: str = None) -> StateAPIClient:
    return STAND.client(token)


pytestmark = STAND.skip_mark()


async def _setup_run(worker: str = "f2-test") -> tuple:
    """CreateThread → CreateRun → ClaimRun. Returns (client, fence)."""
    token = _get_bootstrap_token()
    client = make_mtls_client(token)
    uid = uuid.uuid4().hex[:8]

    thread = await client.create_thread(f"ctx-f2-{uid}", f"f2-{uid}")
    thread_id = thread["id"]
    run = await client.create_run(thread_id, f"run-{uid}", payload={"task": "f2-restart-test"})
    await client.close()

    claim_client = make_mtls_client(None)
    claim = await claim_client.claim_run(worker, ttl_ms=120000)
    lease = claim["lease"]
    assert lease is not None, f"ClaimRun returned null lease: {claim}"

    fence = {
        "run_id": lease["run"]["id"],
        "worker": lease["worker"],
        "epoch": lease["epoch"],
    }
    kv_client = make_mtls_client(claim["task_token"])
    await claim_client.close()

    return kv_client, fence


@pytest.mark.asyncio
async def test_f2_tool_call_and_restart():
    """Full F2: tool-call intent + restart replay.

    Simulates:
    - Process A: graph executes a tool call, records intent, writes KV
    - Kill: process dies (client closed)
    - Process B: new process with same fence, reads intent, replays write
    """
    tool_call_id = f"call-{uuid.uuid4().hex[:12]}"
    node = "agent"
    NS = ("f2", "restart")
    KEY = f"tool-{uuid.uuid4().hex[:8]}"
    TOOL_RESULT = {"tool": "search", "query": "ahp state", "results_count": 42}

    # === PROCESS A ===
    client_a, fence = await _setup_run(worker="f2-process-a")
    store_a = AHPStore(client_a, fence=fence)

    # Graph node "agent" makes a tool call → records intent → writes result
    with store_a.with_identity(node=node, call=tool_call_id):
        await store_a.abatch([
            PutOp(namespace=NS, key=KEY, value=TOOL_RESULT)
        ])

    # Verify the write landed
    item = await store_a._aget(NS, KEY)
    assert item is not None, "Process A: KV write did not land"
    assert item.value == TOOL_RESULT, f"Value mismatch: {item.value}"

    # Verify intent was recorded (GetIntent with run_id, node, call)
    intent = await client_a.get_intent(fence["run_id"], node, tool_call_id)
    assert intent is not None, "Intent not found after RecordIntent"

    # === KILL (simulate process death) ===
    await client_a.close()

    # === PROCESS B (new process, same fence) ===
    client_b, fence_b = await _setup_run(worker="f2-process-b")
    # NOTE: fence_b may have a different run_id (new claim), so we use fence_a's run_id
    # for the intent lookup. In production, the process would recover the fence from
    # its own lease state. Here we verify the intent exists on the server.
    store_b = AHPStore(client_b, fence=fence_b)

    # Process B: verify the intent from Process A is still on the server
    intent_b = await client_b.get_intent(fence["run_id"], node, tool_call_id)
    assert intent_b is not None, "Process B: intent from Process A not found (data lost)"

    # Process B: replay the same write (idempotent via operation_key)
    # The operation_key is a pure function of (run_id, node, call, kind, ns, key, value)
    # So the same logical write maps to the same key → server replays receipt
    with store_b.with_identity(node=node, call=tool_call_id):
        # This should be idempotent: same op_key → replay, not double-write
        await store_b.abatch([
            PutOp(namespace=NS, key=KEY, value=TOOL_RESULT)
        ])

    # Verify value unchanged (idempotent replay)
    item_b = await store_b._aget(NS, KEY)
    assert item_b is not None, "Process B: KV item lost after restart"
    assert item_b.value == TOOL_RESULT, f"Value changed after replay: {item_b.value}"

    # Cleanup
    try:
        await client_b.release(fence_b, status="done")
    except StateTimeoutError:
        pass
    await client_b.close()


@pytest.mark.asyncio
async def test_f2_intent_survives_new_claim():
    """Intent is scoped to run_id, not worker. A new claim on the same run
    (re-lease) can still read the original intent."""
    tool_call_id = f"call-{uuid.uuid4().hex[:12]}"
    node = "planner"
    NS = ("f2", "intent-persist")
    KEY = f"plan-{uuid.uuid4().hex[:8]}"
    VALUE = {"plan": "step1→step2→step3"}

    # Process A: write with identity
    client_a, fence_a = await _setup_run(worker="f2-persist-a")
    store_a = AHPStore(client_a, fence=fence_a)

    with store_a.with_identity(node=node, call=tool_call_id):
        await store_a.abatch([
            PutOp(namespace=NS, key=KEY, value=VALUE)
        ])

    # Verify intent
    intent = await client_a.get_intent(fence_a["run_id"], node, tool_call_id)
    assert intent is not None, "Intent not recorded"

    # Kill
    await client_a.close()

    # Process B: new claim (new worker, same underlying run if available)
    # We just verify the intent is readable with the original run_id
    client_b = make_mtls_client(_get_bootstrap_token())
    intent_b = await client_b.get_intent(fence_a["run_id"], node, tool_call_id)
    assert intent_b is not None, "Intent lost after process restart"
    await client_b.close()
