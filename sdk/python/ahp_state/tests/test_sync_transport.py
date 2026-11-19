"""Pool lifetime and authority propagation; real socket conformance is separate."""
import asyncio
import contextvars
from types import SimpleNamespace

import httpx
import pytest

from ahp_state.client import StateAPIClient
from ahp_state.store import AHPStore
from ahp_state.sync import invoke_sync


def test_sync_fresh_pool_preserves_current_token_and_context_and_closes():
    context = contextvars.ContextVar("test_identity")
    pools, observed = [], []

    def factory():
        def handle(request):
            observed.append((request.headers["authorization"], context.get(), AHPStore.current_identity()))
            return httpx.Response(200, json={"value": {"ok": True}})
        pool = httpx.AsyncClient(transport=httpx.MockTransport(handle), timeout=7, trust_env=False)
        pools.append(pool)
        return pool

    parent = StateAPIClient("https://state.invalid", token="old", http_client_factory=factory)
    current = parent.with_token("current")
    adapter = SimpleNamespace(_client=current)
    store = AHPStore(current, {"run_id": "run", "worker": "worker", "epoch": 1})
    token = context.set("caller")
    try:
        for i in range(3):
            with store.with_identity(node="node", call=str(i)):
                result = invoke_sync(adapter, lambda view: view._client.get_kv(["test"], "key"))
                assert result["value"] == {"ok": True}
        assert [x[2]["call"] for x in observed] == ["0", "1", "2"]
        assert all(x[:2] == ("Bearer current", "caller") for x in observed)
        assert adapter._client is current
        assert not pools[0].is_closed
        assert all(p.is_closed and p.timeout.read == 7 for p in pools[1:])
    finally:
        context.reset(token)
        asyncio.run(parent.close())


@pytest.mark.parametrize("error", [ValueError("failed"), asyncio.CancelledError()])
def test_pool_closed_on_operation_failure_or_cancellation(error):
    pools = []
    def factory():
        pool = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))
        pools.append(pool)
        return pool
    parent = StateAPIClient("https://state.invalid", http_client_factory=factory)
    async def fail(view):
        raise error
    try:
        with pytest.raises(type(error)):
            invoke_sync(SimpleNamespace(_client=parent), fail)
        assert len(pools) == 2 and pools[1].is_closed
        assert not pools[0].is_closed
    finally:
        asyncio.run(parent.close())


async def test_injected_pool_remains_async_compatible_but_sync_requires_factory():
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))) as pool:
        client = StateAPIClient("https://state.invalid", http_client=pool)
        assert await client.get_kv(["test"], "key") == {}
        with pytest.raises(RuntimeError, match="http_client_factory"):
            await asyncio.to_thread(invoke_sync, SimpleNamespace(_client=client), lambda view: view._client.get_kv([], "key"))
        assert not pool.is_closed


async def test_sync_on_running_loop_refuses_without_starting_transport():
    client = StateAPIClient("https://state.invalid")
    try:
        with pytest.raises(RuntimeError, match="event-loop thread"):
            invoke_sync(SimpleNamespace(_client=client), lambda view: view._client.get_kv([], "key"))
    finally:
        await client.close()


def test_reused_factory_pool_rejected_without_closing_callers_pool():
    pool = httpx.AsyncClient()
    client = StateAPIClient("https://state.invalid", http_client=pool, http_client_factory=lambda: pool)
    try:
        with pytest.raises(RuntimeError, match="fresh independent"):
            invoke_sync(SimpleNamespace(_client=client), lambda view: view._client.get_kv([], "key"))
        assert not pool.is_closed
    finally:
        asyncio.run(pool.aclose())
