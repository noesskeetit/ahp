"""API-only AHP Memory clients, independent of agent frameworks.

A write receipt in ``queued`` state is not extraction success. No automatic
retry is performed; after a transport error, repeat the same payload and key.
The supplied HTTP client must carry the workload's mTLS certificate. Raw human
credentials are not accepted by this task API.
"""
from __future__ import annotations

import inspect
from typing import Any, Callable
from urllib.parse import quote, urlencode, urlsplit

import httpx


class MemoryError(Exception):
    """A structured error from the AHP owner API."""

    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message
        super().__init__(f"memory API: {status} {code}: {message}")


def _origin(base_url: str) -> str:
    url = urlsplit(base_url)
    if (url.scheme not in ("http", "https") or not url.hostname
            or url.username is not None or url.password is not None
            or url.query or url.fragment or url.path not in ("", "/")):
        raise ValueError("memory: an API origin is required")
    return base_url.rstrip("/") + "/v1/memory/"


def _mutation_key(key: str) -> str:
    if not isinstance(key, str) or not key.strip():
        raise ValueError("memory: mutation requires an idempotency key")
    return key


def _headers(token: str, key: str | None) -> dict[str, str]:
    if not isinstance(token, str) or not token.strip():
        raise ValueError("memory: empty task token")
    if key is not None and not key.strip():
        raise ValueError("memory: mutation requires an idempotency key")
    result = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    if key is not None:
        result["Idempotency-Key"] = key
    return result


def _decode(response: httpx.Response, body: bytes) -> dict[str, Any]:
    if len(body) > 4 << 20:
        raise MemoryError(response.status_code, "invalid_response", "response exceeds 4 MiB")
    try:
        import json
        result = json.loads(body)
    except (ValueError, UnicodeError):
        raise MemoryError(response.status_code, "invalid_response", "API returned invalid JSON") from None
    if not isinstance(result, dict):
        raise MemoryError(response.status_code, "invalid_response", "API response must be an object")
    if not 200 <= response.status_code < 300:
        error = result.get("error", result)
        if not isinstance(error, dict) or not isinstance(error.get("code"), str):
            raise MemoryError(response.status_code, "invalid_response", "invalid error response")
        raise MemoryError(response.status_code, error["code"], str(error.get("message", "request failed")))
    return result


def _query(scope: str, project_id: str) -> str:
    return urlencode({"scope": scope, "project_id": project_id})


class MemoryClient:
    """Synchronous agent client; supplied clients remain caller-owned."""

    def __init__(self, base_url: str, token: Callable[[], str], *,
                 http_client: httpx.Client | None = None, timeout: float = 30):
        self._base = _origin(base_url)
        self._token = token
        self._owns_http = http_client is None
        self._http = http_client or httpx.Client(timeout=timeout, trust_env=False)

    def close(self) -> None:
        """Close only a transport created by this client."""
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> MemoryClient:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _call(self, method: str, path: str, payload: dict[str, Any] | None = None,
              key: str | None = None) -> dict[str, Any]:
        headers = _headers(self._token(), key)
        with self._http.stream(method, self._base + path, json=payload,
                               headers=headers, follow_redirects=False) as response:
            body = bytearray()
            for chunk in response.iter_bytes():
                body.extend(chunk)
                if len(body) > 4 << 20:
                    raise MemoryError(response.status_code, "invalid_response", "response exceeds 4 MiB")
            return _decode(response, bytes(body))

    def remember(self, key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Queue a durable write with State source references."""
        return self._call("POST", "remember", request, _mutation_key(key))

    def search(self, request: dict[str, Any]) -> dict[str, Any]:
        """Recall facts within the server-bound owner/project."""
        return self._call("POST", "search", request)

    def context(self, request: dict[str, Any]) -> dict[str, Any]:
        """Read project facts and optional personal preferences."""
        return self._call("POST", "context", request)

    def list(self, scope: str, project_id: str = "", *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        """List a bounded page of authorized facts."""
        return self._call("GET", "items?" + _query(scope, project_id) + f"&limit={limit}&offset={offset}")

    def correct(self, key: str, ref: str, request: dict[str, Any]) -> dict[str, Any]:
        """Queue a correction of a server-issued memory reference."""
        return self._call("PATCH", "items/" + quote(ref, safe=""), request, _mutation_key(key))

    def invalidate(self, key: str, ref: str, request: dict[str, Any]) -> dict[str, Any]:
        """Queue invalidation for future retrieval."""
        return self._call("POST", "items/" + quote(ref, safe="") + "/invalidate", request, _mutation_key(key))

    def operation(self, operation_id: str, scope: str, project_id: str = "") -> dict[str, Any]:
        """Read the durable operation receipt."""
        return self._call("GET", "operations/" + quote(operation_id, safe="") + "?" + _query(scope, project_id))

    def settings(self, project_id: str = "") -> dict[str, Any]:
        """Read settings without extending project activity."""
        return self._call("GET", "settings?" + urlencode({"project_id": project_id}))

    def status(self, scope: str, project_id: str = "") -> dict[str, Any]:
        """Read bank lifecycle and queue status without bank IDs."""
        return self._call("GET", "status?" + _query(scope, project_id))


class MemoryManagementClient(MemoryClient):
    """Sanctioned ingress/control client; the server requires authority claims.

    Constructing this class does not grant memory:review or memory:delete.
    Agent tokens remain forbidden even when they hold those capabilities.
    """

    def update_settings(self, key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Update settings with ingress/control and memory:review authority."""
        return self._call("PATCH", "settings", request, _mutation_key(key))

    def clear(self, key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Close a generation with ingress/control and memory:delete authority."""
        return self._call("POST", "clear", request, _mutation_key(key))

    def retry(self, key: str, operation_id: str, request: dict[str, Any]) -> dict[str, Any]:
        """Retry a failed operation with ingress/control authority."""
        return self._call("POST", "operations/" + quote(operation_id, safe="") + "/retry", request, _mutation_key(key))


class AsyncMemoryClient:
    """Async agent client; asyncio cancellation propagates without retries."""

    def __init__(self, base_url: str, token: Callable[[], Any], *,
                 http_client: httpx.AsyncClient | None = None, timeout: float = 30):
        self._base = _origin(base_url)
        self._token = token
        self._owns_http = http_client is None
        self._http = http_client or httpx.AsyncClient(timeout=timeout, trust_env=False)

    async def aclose(self) -> None:
        """Close only a transport created by this client."""
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> AsyncMemoryClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def _call(self, method: str, path: str, payload: dict[str, Any] | None = None,
                    key: str | None = None) -> dict[str, Any]:
        token = self._token()
        if inspect.isawaitable(token):
            token = await token
        headers = _headers(token, key)
        async with self._http.stream(method, self._base + path, json=payload,
                                    headers=headers, follow_redirects=False) as response:
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > 4 << 20:
                    raise MemoryError(response.status_code, "invalid_response", "response exceeds 4 MiB")
            return _decode(response, bytes(body))

    async def remember(self, key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Queue a durable write with State source references."""
        return await self._call("POST", "remember", request, _mutation_key(key))

    async def search(self, request: dict[str, Any]) -> dict[str, Any]:
        """Recall facts within the server-bound owner/project."""
        return await self._call("POST", "search", request)

    async def context(self, request: dict[str, Any]) -> dict[str, Any]:
        """Read project facts and optional personal preferences."""
        return await self._call("POST", "context", request)

    async def list(self, scope: str, project_id: str = "", *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        """List a bounded page of authorized facts."""
        return await self._call("GET", "items?" + _query(scope, project_id) + f"&limit={limit}&offset={offset}")

    async def correct(self, key: str, ref: str, request: dict[str, Any]) -> dict[str, Any]:
        """Queue a correction of a server-issued memory reference."""
        return await self._call("PATCH", "items/" + quote(ref, safe=""), request, _mutation_key(key))

    async def invalidate(self, key: str, ref: str, request: dict[str, Any]) -> dict[str, Any]:
        """Queue invalidation for future retrieval."""
        return await self._call("POST", "items/" + quote(ref, safe="") + "/invalidate", request, _mutation_key(key))

    async def operation(self, operation_id: str, scope: str, project_id: str = "") -> dict[str, Any]:
        """Read the durable operation receipt."""
        return await self._call("GET", "operations/" + quote(operation_id, safe="") + "?" + _query(scope, project_id))

    async def settings(self, project_id: str = "") -> dict[str, Any]:
        """Read settings without extending project activity."""
        return await self._call("GET", "settings?" + urlencode({"project_id": project_id}))

    async def status(self, scope: str, project_id: str = "") -> dict[str, Any]:
        """Read bank lifecycle and queue status without bank IDs."""
        return await self._call("GET", "status?" + _query(scope, project_id))


class AsyncMemoryManagementClient(AsyncMemoryClient):
    """Async management face with the same server-side authority requirements."""

    async def update_settings(self, key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Update settings with ingress/control and memory:review authority."""
        return await self._call("PATCH", "settings", request, _mutation_key(key))

    async def clear(self, key: str, request: dict[str, Any]) -> dict[str, Any]:
        """Close a generation with ingress/control and memory:delete authority."""
        return await self._call("POST", "clear", request, _mutation_key(key))

    async def retry(self, key: str, operation_id: str, request: dict[str, Any]) -> dict[str, Any]:
        """Retry a failed operation with ingress/control authority."""
        return await self._call("POST", "operations/" + quote(operation_id, safe="") + "/retry", request, _mutation_key(key))
