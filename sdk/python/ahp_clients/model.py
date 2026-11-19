"""Public AHP Model owner API; explicit recovery uses the same operation key."""
from __future__ import annotations
from typing import Any
from ._transport import Transport, AsyncTransport, APIError, JSON, decode, required


def _request(key: str, messages: list[JSON], model: str, tools: list[JSON] | None,
             max_tokens: int, after: int = 0) -> JSON:
    required(key)
    if not messages or after < 0:
        raise ValueError("messages and nonnegative cursor are required")
    return {"key": key, "messages": messages, "model": model, "tools": tools or [],
            "max_tokens": max_tokens, "after": after}


class _Frames:
    def __init__(self, after: int):
        self.id, self.cursor, self.terminal = "", after, False

    def accept(self, raw: bytes) -> JSON:
        try:
            f = decode(raw)
        except APIError as error:
            error.outcome_unknown = True
            raise
        kind = f.get("type")
        invalid = False
        if kind == "accepted":
            invalid = bool(self.id) or not isinstance(f.get("generation_id"), str) or not f["generation_id"]
            self.id = f.get("generation_id", "")
        elif kind == "delta":
            d = f.get("delta")
            if not isinstance(d, dict):
                raise APIError(200, "INVALID_RESPONSE", "delta must be an object", outcome_unknown=True)
            invalid = not self.id or d.get("generation_id") != self.id or d.get("sequence") != self.cursor + 1
            if not invalid:
                self.cursor = d["sequence"]
        elif kind == "terminal":
            g = f.get("generation")
            if not isinstance(g, dict):
                raise APIError(200, "INVALID_RESPONSE", "generation must be an object", outcome_unknown=True)
            invalid = not self.id or g.get("id") != self.id or g.get("delta_sequence", 0) != self.cursor
            self.terminal = not invalid
        elif kind == "error":
            raise APIError(200, str(f.get("code", "UNKNOWN")), "model stream refused", outcome_unknown=True)
        elif kind != "heartbeat":
            invalid = True
        if invalid:
            raise APIError(200, "INVALID_RESPONSE", "invalid stream identity or cursor", outcome_unknown=True)
        return f


class ModelClient:
    """Task-token client. The supplied mTLS HTTP client remains caller-owned."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = Transport(base_url, token, http_client=http_client)

    def submit(self, key: str, messages: list[JSON], *, model: str = "",
                      tools: list[JSON] | None = None, max_tokens: int = 0) -> JSON:
        """Execute once; recover a lost response using exactly this key and payload."""
        return self._wire.call("/v1/model/SubmitModel", _request(key, messages, model, tools, max_tokens), mutation=True)

    def get(self, generation_id: str) -> JSON:
        """Read durable generation status and accumulated output."""
        return self._wire.call("/v1/model/GetModel", {"id": required(generation_id, "generation_id")})

    def cancel(self, generation_id: str) -> JSON:
        """Request durable cancellation; closing a subscription does not cancel."""
        return self._wire.call("/v1/model/CancelModel", {"id": required(generation_id, "generation_id")}, mutation=True)

    def stream(self, key: str, messages: list[JSON], *, model: str = "",
                      tools: list[JSON] | None = None, max_tokens: int = 0, after: int = 0):
        """Yield bounded NDJSON frames; resume explicitly with same payload/key and cursor."""
        request = _request(key, messages, model, tools, max_tokens, after)
        frames = _Frames(after)
        source = self._wire.lines("/v1/model/StreamModel", request)
        try:
            for raw in source:
                yield frames.accept(raw)
                if frames.terminal:
                    return
            raise APIError(0, "UNKNOWN", "stream ended before terminal; resume same key/cursor", outcome_unknown=True)
        finally:
            source.close()


class AsyncModelClient:
    """Task-token client. The supplied mTLS HTTP client remains caller-owned."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = AsyncTransport(base_url, token, http_client=http_client)

    async def submit(self, key: str, messages: list[JSON], *, model: str = "",
                      tools: list[JSON] | None = None, max_tokens: int = 0) -> JSON:
        """Execute once; recover a lost response using exactly this key and payload."""
        return await self._wire.call("/v1/model/SubmitModel", _request(key, messages, model, tools, max_tokens), mutation=True)

    async def get(self, generation_id: str) -> JSON:
        """Read durable generation status and accumulated output."""
        return await self._wire.call("/v1/model/GetModel", {"id": required(generation_id, "generation_id")})

    async def cancel(self, generation_id: str) -> JSON:
        """Request durable cancellation; closing a subscription does not cancel."""
        return await self._wire.call("/v1/model/CancelModel", {"id": required(generation_id, "generation_id")}, mutation=True)

    async def stream(self, key: str, messages: list[JSON], *, model: str = "",
                      tools: list[JSON] | None = None, max_tokens: int = 0, after: int = 0):
        """Yield bounded NDJSON frames; resume explicitly with same payload/key and cursor."""
        request = _request(key, messages, model, tools, max_tokens, after)
        frames = _Frames(after)
        source = self._wire.lines("/v1/model/StreamModel", request)
        try:
            async for raw in source:
                yield frames.accept(raw)
                if frames.terminal:
                    return
            raise APIError(0, "UNKNOWN", "stream ended before terminal; resume same key/cursor", outcome_unknown=True)
        finally:
            await source.aclose()
