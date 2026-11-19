"""Real HTTP test: AHP SDK store against a live State stand, over mTLS.

The stand is found through the hand-out the platform mounts
(``AHP_STATE_HANDSHAKE``): it names the URL, the CA, the client certificate and
key, and the task token, so any lane that owns a stand can run this file without
editing it. The old tunnel inputs (``AHP_STATE_URL`` / ``AHP_STATE_CERTS``) still
work as overrides - see ``tests/_live_stand.py``. No address, certificate path or
token is hardcoded here; with no stand configured the file skips, and the reason
says which input is missing.
"""
import asyncio
import os
import sys
import uuid
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _live_stand import STAND
from ahp_state.client import (
    StateAPIClient,
    StateConflictError,
    StateNotFoundError,
    StateAPIError,
    StateTimeoutError,
)
from ahp_state.store import AHPStore

pytestmark = STAND.skip_mark()


def _get_bootstrap_token() -> str:
    """The task token the hand-out was issued with (never hardcoded)."""
    return STAND.token


def make_mtls_client(token: str = None) -> StateAPIClient:
    """Create StateAPIClient with mTLS to the real stand."""
    return STAND.client(token)


async def setup_run() -> tuple:
    """CreateThread → CreateRun → ClaimRun. Returns (client_with_token, fence)."""
    token = _get_bootstrap_token()
    client = make_mtls_client(token)

    uid = uuid.uuid4().hex[:8]
    thread = await client.create_thread(f"ctx-sdk-{uid}", f"sdk-{uid}")
    thread_id = thread["id"]

    run = await client.create_run(thread_id, f"run-{uid}", payload={"task": "sdk-real-test"})
    await client.close()

    # ClaimRun (mTLS only, NO Bearer token)
    claim_client = make_mtls_client(None)
    claim = await claim_client.claim_run("mac-sdk-ci", ttl_ms=120000)
    lease = claim["lease"]
    assert lease is not None, f"ClaimRun returned null lease: {claim}"

    fence = {
        "run_id": lease["run"]["id"],
        "worker": lease["worker"],
        "epoch": lease["epoch"],
    }

    # Use the new task token for KV operations
    kv_client = make_mtls_client(claim["task_token"])
    await claim_client.close()

    return kv_client, fence


@pytest.mark.asyncio
async def test_kv_roundtrip_put_get_delete_get404():
    """KV roundtrip: put → get → delete → get(404/None)."""
    client, fence = await setup_run()
    ahp_store = AHPStore(client, fence=fence)
    NS = ("sdk", "roundtrip")
    KEY = f"rt-{uuid.uuid4().hex[:8]}"
    VALUE = {"hello": "real-http", "ts": "2026-10-31"}

    await ahp_store._put_cas(NS, KEY, VALUE)
    item = await ahp_store._aget(NS, KEY)
    assert item is not None, "GetKV after PutKV returned None"
    assert item.value == VALUE, f"Value mismatch: {item.value} != {VALUE}"

    await ahp_store._delete_cas(NS, KEY)
    item = await ahp_store._aget(NS, KEY)
    assert item is None, f"Expected None after delete, got: {item}"

    try:
        await client.release(fence, status="done")
    except StateTimeoutError:
        pass
    await client.close()


@pytest.mark.asyncio
async def test_cas_conflict_retry():
    """CAS conflict: put(v1) → stale put(expected=0) → 409 → retry succeeds."""
    client, fence = await setup_run()
    ahp_store = AHPStore(client, fence=fence)
    NS = ("sdk", "cas-test")
    KEY = f"cas-{uuid.uuid4().hex[:8]}"

    await ahp_store._put_cas(NS, KEY, {"v": 1})
    item = await ahp_store._aget(NS, KEY)
    assert item is not None
    assert item.value == {"v": 1}

    op_key = ahp_store._op_key(NS, KEY, "put", {"stale": True})
    with pytest.raises(StateConflictError) as exc_info:
        await client.put_kv(fence, list(NS), KEY, op_key, 0, {"stale": True})
    assert "CONFLICT" in exc_info.value.code or "STALE" in exc_info.value.code

    await ahp_store._put_cas(NS, KEY, {"v": 2})
    item = await ahp_store._aget(NS, KEY)
    assert item.value == {"v": 2}

    try:
        await client.release(fence, status="done")
    except StateTimeoutError:
        pass
    await client.close()


@pytest.mark.asyncio
async def test_concurrent_put_conflict():
    """Two sequential puts same key: second gets CONFLICT.

    Note: true simultaneous creates may return INVALID_ARGUMENT from the server
    (unique-constraint race). This test uses sequential puts to validate the
    CAS contract deterministically.
    """
    client, fence = await setup_run()
    NS = ("sdk", "concurrent")
    KEY = f"conc-{uuid.uuid4().hex[:8]}"

    op_uid = uuid.uuid4().hex[:8]
    # First put succeeds (create)
    await client.put_kv(fence, list(NS), KEY, f"conc-{op_uid}-first", 0, {"writer": "a"})

    # Second put with same expected_version=0 → CONFLICT (item exists)
    with pytest.raises(StateConflictError) as exc_info:
        await client.put_kv(fence, list(NS), KEY, f"conc-{op_uid}-second", 0, {"writer": "b"})
    assert "CONFLICT" in exc_info.value.code or "STALE" in exc_info.value.code

    try:
        await client.release(fence, status="done")
    except StateTimeoutError:
        pass
    await client.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v", "--tb=short"]))
