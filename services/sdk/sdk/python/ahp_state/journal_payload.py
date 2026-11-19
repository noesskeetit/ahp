"""Immutable payloads for the compact State API."""

from __future__ import annotations

import base64
from hashlib import sha256

from ahp_state.client import StateValidationError

INLINE_LIMIT = 256 << 10


async def encode_payload(client, session_path, lease_epoch, value: bytes, *, allow_spill=True):
    """Serialize bytes inline or stage them under the current session lease."""
    data = base64.b64encode(value).decode("ascii")
    if len(value) <= INLINE_LIMIT:
        return {"inline": data}
    if not allow_spill:
        raise StateValidationError("INVALID_ARGUMENT", "shared state requires an inline payload")
    ref = await client.request_state("POST", session_path + "/blobs:stage", body={
        "lease_epoch": lease_epoch, "data": data,
    })
    return {"blob": ref}


async def decode_payload(client, session_path, payload) -> bytes:
    """Hydrate a stored payload, verifying the returned content."""
    if payload.get("inline") is not None:
        return base64.b64decode(payload["inline"], validate=True)
    ref = payload.get("blob")
    if ref is None:
        raise StateValidationError("INVALID_ARGUMENT", "missing journal payload")
    reply = await client.request_state("POST", session_path + "/blobs:read", body=ref)
    data = base64.b64decode(reply["data"], validate=True)
    if len(data) != ref["size_bytes"] or sha256(data).hexdigest() != ref["sha256"]:
        raise StateValidationError("INVALID_ARGUMENT", "journal blob does not match its descriptor")
    return data
