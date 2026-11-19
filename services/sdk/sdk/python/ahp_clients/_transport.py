"""Bounded, single-attempt owner API transport. HTTP clients remain caller-owned."""
from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Iterator, AsyncIterator
from typing import Any
from urllib.parse import urlsplit

import httpx

MAX_BYTES = 16 << 20
JSON = dict[str, Any]


class APIError(Exception):
    """Platform refusal or indeterminate transport outcome; never a retry directive."""

    def __init__(self, status: int, code: str, message: str, *, reason: str = "",
                 outcome_unknown: bool = False):
        self.status, self.code, self.message = status, code, message
        self.reason, self.outcome_unknown = reason, outcome_unknown
        super().__init__(f"AHP API: {status} {code}: {message}")


def required(value: str, name: str = "operation key") -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} is required")
    return value


def origin(value: str) -> str:
    u = urlsplit(value)
    if (u.scheme != "https" or not u.hostname or u.username is not None
            or u.password is not None or u.path not in ("", "/")
            or any(c in value for c in "%?#") or u.query or u.fragment):
        raise ValueError("a bare HTTPS owner origin is required")
    return value.rstrip("/")


def decode(raw: bytes, status: int = 200) -> JSON:
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError):
        raise APIError(status, "INVALID_RESPONSE", "invalid JSON response") from None
    if not isinstance(value, dict):
        raise APIError(status, "INVALID_RESPONSE", "response must be an object")
    if not 200 <= status < 300:
        error = value.get("error", value)
        if not isinstance(error, dict):
            error = {}
        raise APIError(status, str(error.get("code", "HTTP_ERROR")),
                       str(error.get("message", "request rejected"))[:512],
                       reason=str(error.get("reason", "")))
    return value


def headers(token: str, accept: str) -> dict[str, str]:
    required(token, "current task token")
    return {"Authorization": "Bearer " + token, "Content-Type": "application/json", "Accept": accept}


def _bounded_lines(chunks: Iterator[bytes]) -> Iterator[bytes]:
    pending = bytearray()
    for chunk in chunks:
        # Bound each record, including records split across transport chunks.
        for part in chunk.splitlines(keepends=True):
            pending.extend(part)
            if len(pending) > MAX_BYTES:
                raise APIError(200, "INVALID_RESPONSE", "stream frame exceeds 16 MiB")
            if pending.endswith(b"\n"):
                yield bytes(pending).rstrip(b"\r\n")
                pending.clear()
    if pending:
        yield bytes(pending)


class Transport:
    def __init__(self, base_url: str, token: Callable[[], str], *, http_client: httpx.Client):
        if http_client is None:
            raise ValueError("caller-owned mTLS HTTP client is required")
        self.base, self.token, self.http = origin(base_url), token, http_client

    def call(self, path: str, payload: JSON, *, mutation: bool = False) -> JSON:
        try:
            with self.http.stream("POST", self.base + path, json=payload,
                                  headers=headers(self.token(), "application/json"), follow_redirects=False) as r:
                body = bytearray()
                for chunk in r.iter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_BYTES:
                        raise APIError(r.status_code, "INVALID_RESPONSE", "response exceeds 16 MiB", outcome_unknown=mutation)
                try:
                    return decode(bytes(body), r.status_code)
                except APIError as error:
                    if mutation and (200 <= r.status_code < 300 or r.status_code >= 500):
                        error.outcome_unknown = True
                    raise
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN" if mutation else "UNAVAILABLE", "transport interrupted; reconcile the same operation",
                           outcome_unknown=mutation) from None

    def lines(self, path: str, payload: JSON, *, sse: bool = False) -> Iterator[bytes]:
        try:
            accept = "text/event-stream" if sse else "application/x-ndjson"
            with self.http.stream("POST", self.base + path, json=payload,
                                  headers=headers(self.token(), accept), follow_redirects=False) as r:
                if r.status_code != 200:
                    body = bytearray()
                    for chunk in r.iter_bytes():
                        body.extend(chunk)
                        if len(body) > MAX_BYTES:
                            raise APIError(r.status_code, "INVALID_RESPONSE", "response exceeds 16 MiB")
                    decode(bytes(body), r.status_code)
                if r.headers.get("content-type", "").split(";")[0] != accept:
                    raise APIError(r.status_code, "INVALID_RESPONSE", "unexpected stream content type")
                yield from _bounded_lines(r.iter_bytes())
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN", "stream interrupted; resume with the same key and cursor", outcome_unknown=True) from None


class AsyncTransport:
    def __init__(self, base_url: str, token: Callable[[], Any], *, http_client: httpx.AsyncClient):
        if http_client is None:
            raise ValueError("caller-owned mTLS HTTP client is required")
        self.base, self.token, self.http = origin(base_url), token, http_client

    async def _headers(self, accept: str) -> dict[str, str]:
        token = self.token()
        if inspect.isawaitable(token):
            token = await token
        return headers(token, accept)

    async def call(self, path: str, payload: JSON, *, mutation: bool = False) -> JSON:
        try:
            async with self.http.stream("POST", self.base + path, json=payload,
                                        headers=await self._headers("application/json"), follow_redirects=False) as r:
                body = bytearray()
                async for chunk in r.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > MAX_BYTES:
                        raise APIError(r.status_code, "INVALID_RESPONSE", "response exceeds 16 MiB", outcome_unknown=mutation)
                try:
                    return decode(bytes(body), r.status_code)
                except APIError as error:
                    if mutation and (200 <= r.status_code < 300 or r.status_code >= 500):
                        error.outcome_unknown = True
                    raise
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN" if mutation else "UNAVAILABLE", "transport interrupted; reconcile the same operation",
                           outcome_unknown=mutation) from None

    async def lines(self, path: str, payload: JSON, *, sse: bool = False) -> AsyncIterator[bytes]:
        try:
            accept = "text/event-stream" if sse else "application/x-ndjson"
            async with self.http.stream("POST", self.base + path, json=payload,
                                        headers=await self._headers(accept), follow_redirects=False) as r:
                if r.status_code != 200:
                    body = bytearray()
                    async for chunk in r.aiter_bytes():
                        body.extend(chunk)
                        if len(body) > MAX_BYTES:
                            raise APIError(r.status_code, "INVALID_RESPONSE", "response exceeds 16 MiB")
                    decode(bytes(body), r.status_code)
                if r.headers.get("content-type", "").split(";")[0] != accept:
                    raise APIError(r.status_code, "INVALID_RESPONSE", "unexpected stream content type")
                pending = bytearray()
                async for chunk in r.aiter_bytes():
                    for part in chunk.splitlines(keepends=True):
                        pending.extend(part)
                        if len(pending) > MAX_BYTES:
                            raise APIError(200, "INVALID_RESPONSE", "stream frame exceeds 16 MiB")
                        if pending.endswith(b"\n"):
                            yield bytes(pending).rstrip(b"\r\n")
                            pending.clear()
                if pending:
                    yield bytes(pending)
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        except httpx.HTTPError:
            raise APIError(0, "UNKNOWN", "stream interrupted; resume with the same key and cursor", outcome_unknown=True) from None
