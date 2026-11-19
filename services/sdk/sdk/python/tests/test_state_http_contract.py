"""Integration test: AHP SDK against the production State HTTP contract.

Validates the exact wire format and error mapping of the State API as defined
by internal/stateapi (transport.WriteError + contracts.Error):

  POST /v1/state/{Operation}
  Body: JSON with nested fence {"run_id", "worker", "epoch"}
  Error response: {"code": "<STABLE_CODE>", "message": "..."}

HTTP status → typed exception:
  400 → StateValidationError   (INVALID_ARGUMENT, KEY_REUSE_MISMATCH)
  403 → StateForbiddenError    (FORBIDDEN)
  404 → StateNotFoundError     (NOT_FOUND)
  409 → StateConflictError     (CONFLICT, STALE_EPOCH, LEASE_LOST)
  5xx → StateServerError       (INTERNAL)

This test runs against httpx.MockTransport — no live endpoint required.
The e2e test at the bottom runs against a real mTLS stand found through the
hand-out in AHP_STATE_HANDSHAKE, and skips with a precise reason when there is
no stand to talk to.
"""

from __future__ import annotations

import json
import uuid
import pytest

import httpx

from ahp_state.client import (
    StateAPIClient,
    StateAPIError,
    StateConflictError,
    StateNotFoundError,
    StateForbiddenError,
    StateValidationError,
    StateServerError,
    StateTimeoutError,
)
from ahp_state.store import AHPStore

FENCE = {"run_id": "run-test-1", "worker": "pod-xyz", "epoch": 42}
NS = ["agent", "memory"]
KEY = "test_key"
VALUE = {"content": "hello", "score": 3.14}


# ---------------------------------------------------------------------------
# Mock transport that simulates the State HTTP server contract
# ---------------------------------------------------------------------------

class StateContractMock:
    """Simulates the production State HTTP server responses.

    Records every request for assertion; returns canned responses based on
    the operation name and configured scenario.
    """

    def __init__(self):
        self.requests: list[dict] = []
        self._responses: dict[str, tuple[int, dict]] = {}
        self._default_kv_version = 1

    def set_response(self, operation: str, status: int, body: dict):
        self._responses[operation] = (status, body)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        # Extract operation name from /v1/state/{Op}
        parts = path.split("/")
        op = parts[-1] if len(parts) >= 4 else "Unknown"
        body = json.loads(request.content) if request.content else {}
        self.requests.append({"op": op, "path": path, "body": body,
                              "method": request.method})

        # Check for configured error response
        if op in self._responses:
            status, resp_body = self._responses[op]
            return httpx.Response(status, json=resp_body)

        # Default success responses per operation
        if op == "GetKV":
            if self._default_kv_version is None:
                return httpx.Response(404, json={"code": "NOT_FOUND", "message": "kv item"})
            return httpx.Response(200, json={
                "namespace": NS, "key": KEY, "value": VALUE,
                "version": self._default_kv_version,
                "updated_at": "2026-01-01T00:00:00Z",
            })
        elif op == "PutKV":
            new_version = (self._default_kv_version or 0) + 1
            self._default_kv_version = new_version
            return httpx.Response(200, json={
                "namespace": NS, "key": KEY, "value": VALUE,
                "version": new_version, "updated_at": "2026-01-01T00:00:01Z",
            })
        elif op == "DeleteKV":
            return httpx.Response(200, json={})
        elif op == "SearchKV":
            return httpx.Response(200, json={"items": []})
        else:
            return httpx.Response(200, json={})

    def as_httpx_transport(self) -> httpx.MockTransport:
        # Dispatcher always calls the CURRENT self.handle (supports mid-test swap)
        dispatcher = self._dispatch
        return httpx.MockTransport(dispatcher)

    def _dispatch(self, request: httpx.Request) -> httpx.Response:
        return self.handle(request)

    def make_client(self) -> StateAPIClient:
        http = httpx.AsyncClient(transport=self.as_httpx_transport(),
                                 base_url="https://state-stand.internal")
        return StateAPIClient("https://state-stand.internal", http_client=http)


# ---------------------------------------------------------------------------
# Contract tests: wire format
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_putkv_nested_fence_contract():
    """PutKV must send fence as nested {run_id, worker, epoch} (not flat)."""
    mock = StateContractMock()
    client = mock.make_client()

    await client.put_kv(FENCE, NS, KEY, "op-key-1", 0, VALUE)

    req = mock.requests[-1]
    assert req["method"] == "POST"
    assert req["path"] == "/v1/state/PutKV"
    # Fence must be nested
    assert "fence" in req["body"]
    fence = req["body"]["fence"]
    assert fence == {"run_id": "run-test-1", "worker": "pod-xyz", "epoch": 42}
    # No flat fence fields at top level
    assert "run_id" not in req["body"]
    assert "worker" not in req["body"]
    assert "epoch" not in req["body"]
    # KV fields
    assert req["body"]["namespace"] == NS
    assert req["body"]["key"] == KEY
    assert req["body"]["operation_key"] == "op-key-1"
    assert req["body"]["expected_version"] == 0
    assert req["body"]["value"] == VALUE


@pytest.mark.asyncio
async def test_deletekv_nested_fence_contract():
    """DeleteKV must send fence nested + require expected_version >= 1."""
    mock = StateContractMock()
    mock._default_kv_version = 3
    client = mock.make_client()

    await client.delete_kv(FENCE, NS, KEY, "op-key-del", 3)

    req = mock.requests[-1]
    assert req["path"] == "/v1/state/DeleteKV"
    assert req["body"]["fence"] == {"run_id": "run-test-1", "worker": "pod-xyz", "epoch": 42}
    assert req["body"]["expected_version"] == 3
    assert req["body"]["operation_key"] == "op-key-del"


@pytest.mark.asyncio
async def test_getkv_no_fence():
    """GetKV is a read — must NOT include fence in the body."""
    mock = StateContractMock()
    client = mock.make_client()

    await client.get_kv(NS, KEY)

    req = mock.requests[-1]
    assert req["path"] == "/v1/state/GetKV"
    assert "fence" not in req["body"]
    assert req["body"]["namespace"] == NS
    assert req["body"]["key"] == KEY


# ---------------------------------------------------------------------------
# Contract tests: error mapping (HTTP status → typed exception)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_error_404_not_found():
    """HTTP 404 NOT_FOUND → StateNotFoundError."""
    mock = StateContractMock()
    mock.set_response("GetKV", 404, {"code": "NOT_FOUND", "message": "kv item"})
    client = mock.make_client()

    with pytest.raises(StateNotFoundError) as exc:
        await client.get_kv(NS, KEY)
    assert exc.value.code == "NOT_FOUND"


@pytest.mark.asyncio
async def test_error_409_conflict():
    """HTTP 409 CONFLICT → StateConflictError."""
    mock = StateContractMock()
    mock.set_response("PutKV", 409, {"code": "CONFLICT", "message": "kv version conflict"})
    client = mock.make_client()

    with pytest.raises(StateConflictError) as exc:
        await client.put_kv(FENCE, NS, KEY, "op-1", 0, VALUE)
    assert exc.value.code == "CONFLICT"


@pytest.mark.asyncio
async def test_error_409_stale_epoch():
    """HTTP 409 STALE_EPOCH → StateConflictError (fence expired)."""
    mock = StateContractMock()
    mock.set_response("PutKV", 409, {"code": "STALE_EPOCH", "message": "lease epoch mismatch"})
    client = mock.make_client()

    with pytest.raises(StateConflictError) as exc:
        await client.put_kv(FENCE, NS, KEY, "op-1", 1, VALUE)
    assert exc.value.code == "STALE_EPOCH"


@pytest.mark.asyncio
async def test_error_403_forbidden():
    """HTTP 403 FORBIDDEN → StateForbiddenError (bad token / scope)."""
    mock = StateContractMock()
    mock.set_response("PutKV", 403, {"code": "FORBIDDEN", "message": "token revoked"})
    client = mock.make_client()

    with pytest.raises(StateForbiddenError) as exc:
        await client.put_kv(FENCE, NS, KEY, "op-1", 0, VALUE)
    assert exc.value.code == "FORBIDDEN"


@pytest.mark.asyncio
async def test_error_400_invalid_argument():
    """HTTP 400 INVALID_ARGUMENT → StateValidationError."""
    mock = StateContractMock()
    mock.set_response("PutKV", 400, {"code": "INVALID_ARGUMENT", "message": "negative version"})
    client = mock.make_client()

    with pytest.raises(StateValidationError) as exc:
        await client.put_kv(FENCE, NS, KEY, "op-1", -1, VALUE)
    assert exc.value.code == "INVALID_ARGUMENT"


@pytest.mark.asyncio
async def test_error_400_key_reuse_mismatch():
    """HTTP 400 KEY_REUSE_MISMATCH → StateValidationError."""
    mock = StateContractMock()
    mock.set_response("PutKV", 400, {"code": "KEY_REUSE_MISMATCH", "message": "different digest"})
    client = mock.make_client()

    with pytest.raises(StateValidationError) as exc:
        await client.put_kv(FENCE, NS, KEY, "op-1", 1, VALUE)
    assert exc.value.code == "KEY_REUSE_MISMATCH"


@pytest.mark.asyncio
async def test_error_500_internal():
    """HTTP 500 → StateServerError."""
    mock = StateContractMock()
    mock.set_response("PutKV", 500, {"code": "INTERNAL", "message": "internal server error"})
    client = mock.make_client()

    with pytest.raises(StateServerError) as exc:
        await client.put_kv(FENCE, NS, KEY, "op-1", 0, VALUE)
    assert exc.value.code == "INTERNAL"


@pytest.mark.asyncio
async def test_error_503_unavailable():
    """HTTP 503 → StateServerError."""
    mock = StateContractMock()
    mock.set_response("GetKV", 503, {"code": "INTERNAL", "message": "service unavailable"})
    client = mock.make_client()

    with pytest.raises(StateServerError):
        await client.get_kv(NS, KEY)


# ---------------------------------------------------------------------------
# Contract tests: Store CAS semantics (FIX a)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_store_cas_uses_server_version():
    """Store._put_cas must read current version and use it as expected_version."""
    mock = StateContractMock()
    mock._default_kv_version = 5
    client = mock.make_client()
    store = AHPStore(client, fence=FENCE)

    await store._put_cas(tuple(NS), KEY, VALUE)

    # The PutKV call should use expected_version=5 (from GetKV), not 0
    put_reqs = [r for r in mock.requests if r["op"] == "PutKV"]
    assert len(put_reqs) == 1
    assert put_reqs[0]["body"]["expected_version"] == 5


@pytest.mark.asyncio
async def test_store_cas_create_uses_zero():
    """Store._put_cas for a NEW key (NOT_FOUND on read) must use expected_version=0."""
    mock = StateContractMock()
    mock._default_kv_version = None  # GetKV returns 404
    client = mock.make_client()
    store = AHPStore(client, fence=FENCE)

    await store._put_cas(tuple(NS), "new_key", {"fresh": True})

    put_reqs = [r for r in mock.requests if r["op"] == "PutKV"]
    assert len(put_reqs) == 1
    assert put_reqs[0]["body"]["expected_version"] == 0


@pytest.mark.asyncio
async def test_store_cas_retry_on_conflict():
    """Store._put_cas retries on CONFLICT (re-reads version and re-issues)."""
    mock = StateContractMock()
    mock._default_kv_version = 1
    put_attempts = {"n": 0}
    original_handle = mock.handle

    def conflict_then_success(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("PutKV"):
            put_attempts["n"] += 1
            if put_attempts["n"] == 1:
                return httpx.Response(409, json={"code": "CONFLICT", "message": "version conflict"})
        return original_handle(request)

    mock.handle = conflict_then_success
    client = mock.make_client()
    store = AHPStore(client, fence=FENCE)

    # _put_cas is the best-effort path (no identity)
    await store._put_cas(tuple(NS), KEY, VALUE)

    # Must have retried: 2 PutKV attempts total
    assert put_attempts["n"] == 2, f"Expected 2 PutKV attempts, got {put_attempts['n']}"
    # The recorded (successful) PutKV used the server version
    put_reqs = [r for r in mock.requests if r["op"] == "PutKV"]
    assert len(put_reqs) == 1  # only the successful one was recorded by original_handle
    assert put_reqs[0]["body"]["expected_version"] == 1


# ---------------------------------------------------------------------------
# Contract tests: Stable operation key (FIX c)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_op_key_stable_across_instances():
    """Operation key is content-derived: same inputs → same key, always."""
    client1 = StateContractMock().make_client()
    client2 = StateContractMock().make_client()
    store1 = AHPStore(client1, fence=FENCE)
    store2 = AHPStore(client2, fence=FENCE)

    key1 = store1._op_key(NS, KEY, "put", VALUE)
    key2 = store2._op_key(NS, KEY, "put", VALUE)
    assert key1 == key2

    # Different value → different key
    key3 = store1._op_key(NS, KEY, "put", {"different": True})
    assert key1 != key3

    # Different namespace → different key
    key4 = store1._op_key(["other", "ns"], KEY, "put", VALUE)
    assert key1 != key4


@pytest.mark.asyncio
async def test_op_key_no_counter_reset():
    """Two fresh stores produce the same op_key for the same logical op.

    A counter-based implementation would produce different keys after
    one store had already issued other operations.
    """
    mock = StateContractMock()
    client = mock.make_client()
    store = AHPStore(client, fence=FENCE)

    # Issue some other operations first (advancing any internal counter)
    await store._put_cas(tuple(NS), "other_key_1", {"a": 1})
    await store._put_cas(tuple(NS), "other_key_2", {"b": 2})

    # Now a FRESH store instance
    store2 = AHPStore(client, fence=FENCE)

    # Same logical op → same key regardless of prior operations
    k1 = store._op_key(NS, KEY, "put", VALUE)
    k2 = store2._op_key(NS, KEY, "put", VALUE)
    assert k1 == k2


# ---------------------------------------------------------------------------
# Contract tests: transport-level timeout (F1 fix)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_timeout_maps_to_state_timeout_error():
    """httpx.TimeoutException from the transport maps to StateTimeoutError.

    Verifies F1: call() catches httpx.TimeoutException and raises
    StateTimeoutError (not raw httpx.ConnectTimeout / ReadTimeout).
    """
    def timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("connection timed out", request=request)

    transport = httpx.MockTransport(timeout_handler)
    http = httpx.AsyncClient(transport=transport, base_url="https://state-stand.internal")
    client = StateAPIClient("https://state-stand.internal", http_client=http)

    with pytest.raises(StateTimeoutError) as exc_info:
        await client.get_kv(NS, KEY)

    assert exc_info.value.code == "TIMEOUT"
    assert "ConnectTimeout" in exc_info.value.message
    assert isinstance(exc_info.value, StateAPIError)


@pytest.mark.asyncio
async def test_read_timeout_maps_to_state_timeout_error():
    """httpx.ReadTimeout also maps to StateTimeoutError."""
    def read_timeout_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("read timed out", request=request)

    transport = httpx.MockTransport(read_timeout_handler)
    http = httpx.AsyncClient(transport=transport, base_url="https://state-stand.internal")
    client = StateAPIClient("https://state-stand.internal", http_client=http)

    with pytest.raises(StateTimeoutError) as exc_info:
        await client.put_kv(FENCE, NS, KEY, "op-1", 0, VALUE)

    assert exc_info.value.code == "TIMEOUT"
    assert "ReadTimeout" in exc_info.value.message


@pytest.mark.asyncio
async def test_transport_error_maps_to_state_api_error():
    """Non-timeout httpx.HTTPError maps to StateAPIError('TRANSPORT', ...)."""
    def connect_error_handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    transport = httpx.MockTransport(connect_error_handler)
    http = httpx.AsyncClient(transport=transport, base_url="https://state-stand.internal")
    client = StateAPIClient("https://state-stand.internal", http_client=http)

    with pytest.raises(StateAPIError) as exc_info:
        await client.get_kv(NS, KEY)

    assert exc_info.value.code == "TRANSPORT"
    assert "ConnectError" in exc_info.value.message
    assert not isinstance(exc_info.value, StateTimeoutError)


# ---------------------------------------------------------------------------
# E2E against a real stand (skipped without a hand-out)
# ---------------------------------------------------------------------------

from _live_stand import STAND, claim_own_run


@pytest.mark.asyncio
@STAND.skip_mark()
async def test_e2e_stand_kv_roundtrip():
    """Live E2E: the same wire shapes against a real mTLS stand, no transport mock.

    Everything above runs on ``httpx.MockTransport``, so it proves what the client
    *sends*; this proves what a real stand *answers*. The stand is found through
    the hand-out (``AHP_STATE_HANDSHAKE``), the queue is entered by creating our
    own run, and the namespace is unique per run: this file used to claim whatever
    the FIFO held, reuse one fixed key, and release with ``status="completed"``,
    which the stand rejects - the enum is done | failed | canceled | pending.
    """
    uid = uuid.uuid4().hex[:8]
    http = httpx.AsyncClient(verify=STAND.context(), timeout=30.0)
    client = StateAPIClient(STAND.url, token=STAND.token, http_client=http)

    thread = await client.create_thread(f"ctx-e2e-{uid}", f"e2e-{uid}")
    created = await client.create_run(thread["id"], f"run-e2e-{uid}", {"task": f"e2e-{uid}"})
    lease = await claim_own_run(client, created["id"], f"sdk-e2e-{uid}")
    fence = {"run_id": lease["run"]["id"], "worker": lease["worker"], "epoch": lease["epoch"]}

    test_ns = ["e2e", uid]
    test_key = f"roundtrip-{uid}"
    test_value = {"hello": "world", "ts": "2026-10-31"}

    # Create (version 0 -> 1)
    await client.put_kv(fence, test_ns, test_key, f"e2e-{uid}-1", 0, test_value)

    # Read back
    result = await client.get_kv(test_ns, test_key)
    assert result["value"] == test_value
    assert result["version"] == 1

    # Update (version 1 -> 2)
    new_value = {"hello": "world-v2"}
    await client.put_kv(fence, test_ns, test_key, f"e2e-{uid}-2", 1, new_value)

    # Verify conflict on stale version
    with pytest.raises(StateConflictError):
        await client.put_kv(fence, test_ns, test_key, f"e2e-{uid}-3", 0, {"stale": True})

    # Delete
    await client.delete_kv(fence, test_ns, test_key, f"e2e-{uid}-del", 2)

    # Verify NOT_FOUND after delete
    with pytest.raises(StateNotFoundError):
        await client.get_kv(test_ns, test_key)

    # Release
    await client.call("Release", {"run_id": fence["run_id"], "worker": fence["worker"],
                                  "epoch": fence["epoch"], "status": "done"})
    await http.aclose()


# ---------------------------------------------------------------------------
# Regression: timestamp normalization (Go RFC3339Nano variable fractional)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ts_str,frac_len", [
    ("2026-10-31T08:37:18.59483+00:00", 5),
    ("2026-10-31T08:37:18.594830+00:00", 6),
    ("2026-10-31T08:37:18.594830000+00:00", 9),
    ("2026-10-31T08:37:18.5+00:00", 1),
    ("2026-10-31T08:37:18.59483Z", 5),
    ("2026-10-31T08:37:18Z", 0),
    ("2026-10-31T08:37:18.123+00:00", 3),
    ("2026-10-31T08:37:18.123456789Z", 9),
])
def test_timestamp_normalization(ts_str, frac_len):
    """_ts normalizes Go RFC3339Nano timestamps to Python 3.9 compatible format.

    Go trims trailing zeros from the fractional part, producing 1-9 digits.
    Python 3.9 datetime.fromisoformat only accepts 3 or 6. The _ts() method
    must pad/truncate to exactly 6 digits and handle the Z suffix.
    """
    from datetime import datetime
    from ahp_state.store import AHPStore
    normalized = AHPStore._ts(ts_str)
    dt = datetime.fromisoformat(normalized)  # must not raise
    assert dt.year == 2026
    assert dt.month == 10
    assert dt.day == 31
