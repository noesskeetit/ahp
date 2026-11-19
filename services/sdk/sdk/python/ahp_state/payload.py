"""Opaque checkpoint payload spill through the Artifacts owner HTTP API.

Bytes include the original serializer type and payload. Hydration never caches
access decisions and pins immutable scope, version, digest and size. An explicit
persisted codec is shared with the Go State SDK; legacy payloads stay opaque.
"""
from __future__ import annotations

import base64
import hashlib
import json
from urllib.parse import urlsplit

import httpx

INLINE_BYTES = 256 << 10
BATCH_BYTES = 512 << 10
MAX_BYTES = 64 << 20
MAGIC = b"\x00AHP-PAYLOAD-v1\n"
METADATA_KEY = "$ahp_checkpoint_payload_v1"  # legacy user key, never a discriminator
ENCODING = "ahp-artifact-v1"
_IDENTITY = ("id", "version", "sha256", "size", "tenant", "deployment",
             "subject", "context_id", "task_id", "labels")


def _json(value) -> bytes:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()


def _decode(value) -> bytes:
    if isinstance(value, bytes):
        return value
    return base64.b64decode(value, validate=True) if value else b""


def _encode(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


class Payloads:
    """State client's automatic serializer-independent payload adapter."""

    def __init__(self, owner, base_url: str | None = None):
        self.owner = owner
        self.base = (base_url or owner.base_url).rstrip("/")

    def error(self, message):
        from ahp_state.client import StateValidationError
        return StateValidationError("INVALID_ARGUMENT", message)

    async def request(self, op: str, body: dict, *, binary=False):
        from ahp_state.client import StateForbiddenError, StateAPIError, _raise_typed
        parsed = urlsplit(self.base)
        if parsed.scheme != "https" or not parsed.netloc or parsed.path or parsed.query or parsed.fragment or parsed.username:
            raise self.error("Artifacts owner must be a bare HTTPS origin")
        token = await self.owner.authorization_token()
        if not token:
            raise StateForbiddenError("FORBIDDEN", "artifact task token required")
        try:
            async with self.owner.http_client.stream(
                "POST", f"{self.base}/v1/artifacts/{op}", json=body,
                headers={"Authorization": f"Bearer {token}"},
                follow_redirects=False,
            ) as response:
                limit = MAX_BYTES if binary else 1 << 20
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(data) + len(chunk) > limit:
                        raise self.error("artifact response exceeds size limit")
                    data.extend(chunk)
                if response.status_code != 200:
                    try:
                        error = json.loads(data)
                    except ValueError:
                        error = {"code": "INVALID_ARGUMENT", "message": "Artifacts owner refused request"}
                    raise _raise_typed(response.status_code, error)
                return bytes(data) if binary else json.loads(data)
        except httpx.HTTPError:
            raise StateAPIError("TRANSPORT", "artifact request failed") from None

    async def spill(self, run_id: str, data: bytes, force=False) -> bytes:
        if len(data) <= INLINE_BYTES and not force:
            return data
        if not data or len(data) > MAX_BYTES:
            raise self.error("checkpoint payload exceeds artifact size limit")
        digest = hashlib.sha256(data).hexdigest()
        key = "sha256:" + hashlib.sha256(_json(["checkpoint-v1", run_id, digest])).hexdigest()
        upload = await self.request("BeginUpload", dict(key=key, name="checkpoint.bin",
                                     mime="application/octet-stream", sha256=digest, size=len(data), labels={}))
        if upload.get("sha256") != digest or upload.get("size") != len(data):
            raise self.error("artifact upload declaration mismatch")
        target = urlsplit(upload.get("presigned_url", ""))
        if target.scheme not in ("https", "http") or not target.netloc or target.username:
            raise self.error("invalid artifact upload URL")
        headers = upload.get("headers") or {}
        for name, value in headers.items():
            if name.lower() in ("authorization", "proxy-authorization", "cookie", "host"):
                raise self.error("unsafe artifact upload header")
            if name.lower() == "content-length" and str(value) != str(len(data)):
                raise self.error("artifact upload size mismatch")
        # The blob endpoint never receives workload credentials or task tokens.
        async with httpx.AsyncClient(timeout=60, follow_redirects=False, trust_env=False) as upload_client:
            try:
                async with upload_client.stream("PUT", upload["presigned_url"], content=data, headers=headers) as result:
                    # PUT acknowledgements need no response body. Streaming and
                    # closing here bounds memory for both success and refusal.
                    if not 200 <= result.status_code < 300:
                        raise self.error("artifact upload refused")
            except httpx.HTTPError:
                raise self.error("artifact upload unconfirmed") from None
        ref = await self.request("Seal", {"id": upload["id"]})
        if ref.get("id") != upload["id"] or not ref.get("version") or ref.get("sha256") != digest or ref.get("size") != len(data) or not ref.get("context_id") or not ref.get("task_id"):
            raise self.error("sealed checkpoint artifact integrity or binding mismatch")
        return _json({key: ref.get(key) for key in _IDENTITY})

    async def hydrate(self, data: bytes, encoding="") -> bytes:
        if not encoding:
            return data
        if encoding != ENCODING:
            raise self.error("unsupported checkpoint payload encoding")
        try:
            ref = json.loads(data)
            if not all(ref.get(k) for k in ("id", "version", "context_id", "task_id")) or not 0 < ref["size"] <= MAX_BYTES:
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise self.error("invalid checkpoint artifact reference") from None
        actual = await self.request("Get", {"id": ref["id"]})
        if any(actual.get(key) != ref.get(key) for key in _IDENTITY):
            raise self.error("checkpoint artifact descriptor mismatch")
        data = await self.request("Download", {"id": ref["id"]}, binary=True)
        if len(data) != ref["size"] or hashlib.sha256(data).hexdigest() != ref["sha256"]:
            raise self.error("checkpoint artifact integrity mismatch")
        return data

    async def checkpoint(self, run_id: str, checkpoint: dict, budget=BATCH_BYTES) -> dict:
        cp = dict(checkpoint)
        metadata = cp.get("metadata")
        raw_md = _json(metadata) if metadata is not None else b""
        force = len(raw_md) + sum(len(_decode(p.get("data"))) for p in cp.get("parts", [])) > budget
        parts = []
        for part in cp.get("parts", []):
            p = dict(part)
            if p.get("data") is not None:
                data = _decode(p["data"])
                p["data"] = _encode(await self.spill(run_id, data, force and len(data) > 1024))
                p.pop("encoding", None)
                if len(data) > INLINE_BYTES or (force and len(data) > 1024):
                    p["encoding"] = ENCODING
            parts.append(p)
        if "parts" in cp:
            cp["parts"] = parts
        cp.pop("metadata_encoding", None)
        if len(raw_md) > INLINE_BYTES or (force and len(raw_md) > 1024):
            cp["metadata"] = json.loads(await self.spill(run_id, raw_md, True))
            cp["metadata_encoding"] = ENCODING
        return cp

    async def hydrate_checkpoint(self, checkpoint: dict) -> dict:
        cp = dict(checkpoint)
        if cp.get("metadata_encoding"):
            cp["metadata"] = json.loads(await self.hydrate(_json(cp.get("metadata")), cp["metadata_encoding"]))
        cp.pop("metadata_encoding", None)
        if cp.get("parts"):
            cp["parts"] = [await self.hydrate_row(p) for p in cp["parts"]]
        return cp

    async def hydrate_row(self, row: dict) -> dict:
        row = dict(row)
        if row.get("data") is not None:
            row["data"] = _encode(await self.hydrate(_decode(row["data"]), row.get("encoding", "")))
        row.pop("encoding", None)
        return row

    async def writes(self, run_id: str, writes: list[dict], budget=BATCH_BYTES) -> list[dict]:
        total = sum(len(_decode(w.get("data"))) for w in writes)
        rows = []
        for row in writes:
            row = dict(row)
            data = _decode(row.get("data"))
            row["data"] = _encode(await self.spill(run_id, data, total > budget and len(data) > 1024))
            row.pop("encoding", None)
            if len(data) > INLINE_BYTES or (total > budget and len(data) > 1024):
                row["encoding"] = ENCODING
            rows.append(row)
        return rows
