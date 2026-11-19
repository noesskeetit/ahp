"""Client transport checks; actual PG/mTLS/native acceptance is a separate leg."""
import asyncio
import sys
import subprocess

import httpx
import pytest

from ahp_memory import AsyncMemoryClient, MemoryClient, MemoryError


def test_sync_keys_platform_errors_and_redirect_refusal():
    requests = []

    def handle(request):
        requests.append(request)
        assert request.headers["Authorization"] == "Bearer task"
        if request.url.path.endswith("remember"):
            assert request.headers["Idempotency-Key"] == "same-key"
            return httpx.Response(409, json={"error": {"code": "idempotency_conflict", "message": "key already used"}})
        return httpx.Response(307, headers={"Location": "https://other.invalid/credential-sink"}, json={})

    with httpx.Client(transport=httpx.MockTransport(handle), follow_redirects=True) as transport:
        client = MemoryClient("https://memory.invalid", lambda: "task", http_client=transport)
        with pytest.raises(ValueError):
            client.remember("", {})
        assert not requests
        with pytest.raises(MemoryError) as error:
            client.remember("same-key", {})
        assert error.value.status == 409
        assert error.value.code == "idempotency_conflict"
        with pytest.raises(MemoryError) as error:
            client.settings()
        assert error.value.status == 307
        assert len(requests) == 2
        client.close()
        assert not transport.is_closed


@pytest.mark.asyncio
async def test_async_cancellation_is_not_a_retry():
    started = asyncio.Event()
    calls = 0

    async def handle(request):
        nonlocal calls
        calls += 1
        started.set()
        await asyncio.Future()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as transport:
        client = AsyncMemoryClient("https://memory.invalid", lambda: "task", http_client=transport)
        task = asyncio.create_task(client.settings())
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert calls == 1
        await client.aclose()
        assert not transport.is_closed


def test_memory_import_and_origin_do_not_need_langgraph():
    subprocess.run([sys.executable, "-c", "import ahp_memory,sys; assert 'langgraph' not in sys.modules"], check=True)
    for origin in ("https://user:secret@host", "https://host/path", "https://host?key=secret", "file:///tmp/memory"):
        with pytest.raises(ValueError):
            MemoryClient(origin, lambda: "task")


def test_response_size_and_nonobject_json_are_rejected():
    for content in (b"[]", b"x" * ((4 << 20) + 1)):
        with httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(200, content=content))) as transport:
            client = MemoryClient("https://memory.invalid", lambda: "task", http_client=transport)
            with pytest.raises(MemoryError) as error:
                client.settings()
            assert error.value.code == "invalid_response"
