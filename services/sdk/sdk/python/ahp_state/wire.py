"""Wire encoding for checkpoints and pending writes (block 1, PDF rev 3.2 §4.2).

Three jobs, all of them compatibility-critical:

1. **Identifiers.** The State contract accepts only ``[A-Za-z0-9._\\-:@/]`` up to
   512 bytes (``validIdentifier`` in ``internal/state/store.go``) and rejects
   anything else with INVALID_ARGUMENT. LangGraph channel/task/checkpoint ids are
   arbitrary Python strings, so every identifier that reaches the wire goes
   through :func:`encode_id`, which is reversible (:func:`decode_id`) because the
   escape marker ``x~`` contains ``~`` - a character the safe charset excludes, so
   a legitimately safe id can never be mistaken for an escaped one.

2. **Opaque blobs.** A channel value or a pending write is serialized with the
   LangGraph ``serde`` into its ``(type, bytes)`` tuple and packed as
   ``base64(json({"t": type, "d": base64(bytes)}))``. The server treats the outer
   field as an opaque ``[]byte`` and never parses the payload, while the SDK can
   still round-trip messages, pydantic models, dataclasses, exceptions and
   interrupts exactly like LangGraph's own savers. A ``("empty", b"")`` tuple is
   the LangGraph marker for "channel exists but holds no value"; readers skip it.

3. **The checkpoint envelope.** ``metadata`` is the only field the server keeps
   as JSON, so the checkpoint body (everything except ``channel_values``) and the
   serialized metadata ride there as a versioned envelope. Unknown/future
   checkpoint keys are carried verbatim: a LangGraph checkpoint format change
   (§9.8 "Смена формата снимка LangGraph") must not lose state, because the body
   is never re-derived field by field.
"""

from __future__ import annotations

import base64
import binascii
import json
import re
from typing import Any

ENVELOPE_VERSION = 1
_SAFE_ID = re.compile(r"^[A-Za-z0-9._\-:@/]+$")
# The escaped form is ``_u_<hex>`` - only characters the State charset allows, and
# the pattern is unambiguous because :func:`encode_id` escapes any id that already
# looks like it (its payload is always hex, so decoding returns the original).
_ESCAPE_PREFIX = "_u_"
_ESCAPED = re.compile(r"^_u_([0-9a-f]*)$")
_MAX_ID_BYTES = 512

# LangGraph's own marker for "this channel has no value in this checkpoint".
EMPTY_TYPE = "empty"


class WireError(Exception):
    """Raised when a payload cannot be represented on the State wire."""


# ---------------------------------------------------------------------------
# identifiers
# ---------------------------------------------------------------------------


def _needs_escape(value: str) -> bool:
    return bool(not _SAFE_ID.match(value) or _ESCAPED.match(value))


def encode_id(value: str) -> str:
    """Map an arbitrary string onto a State-legal identifier, reversibly."""
    if not isinstance(value, str):
        raise WireError(f"identifier must be str, got {type(value).__name__}")
    if value and not _needs_escape(value):
        return value
    escaped = _ESCAPE_PREFIX + value.encode("utf-8").hex()
    if len(escaped.encode("utf-8")) > _MAX_ID_BYTES:
        # A digest would not be reversible, and part/pending-write lookups must
        # decode back to the original channel and task names. Fail explicitly
        # instead of writing state that can never be read back correctly.
        raise WireError(f"identifier of {len(value)} chars cannot be encoded within "
                        f"{_MAX_ID_BYTES} bytes")
    return escaped


def decode_id(value: str) -> str:
    """Inverse of :func:`encode_id`."""
    if not isinstance(value, str):
        return value
    match = _ESCAPED.match(value)
    if not match:
        return value
    try:
        return bytes.fromhex(match.group(1)).decode("utf-8")
    except (ValueError, UnicodeDecodeError) as exc:
        raise WireError(f"cannot decode identifier {value!r}: {exc}") from exc


def pack_blob(serde_type: str, payload: bytes) -> str:
    """Pack one serde ``(type, bytes)`` tuple into a wire ``data`` field."""
    return base64.b64encode(
        json.dumps({"t": serde_type, "d": base64.b64encode(payload).decode("ascii")},
                   separators=(",", ":")).encode("utf-8")
    ).decode("ascii")


def unpack_blob(data: str | bytes) -> tuple[str, bytes]:
    """Inverse of :func:`pack_blob`."""
    raw = base64.b64decode(data) if isinstance(data, str) else data
    try:
        doc = json.loads(raw.decode("utf-8"))
        return doc["t"], base64.b64decode(doc["d"])
    except (KeyError, ValueError, UnicodeDecodeError) as exc:
        raise WireError(f"malformed checkpoint blob: {exc}") from exc


def dump_value(serde, value: Any) -> str:
    return pack_blob(*serde.dumps_typed(value))


def load_value(serde, data: str | bytes) -> Any:
    serde_type, payload = unpack_blob(data)
    if serde_type == EMPTY_TYPE:
        return None
    return serde.loads_typed((serde_type, payload))


def empty_blob() -> str:
    """The LangGraph "channel has no value" marker, wire-packed."""
    return pack_blob(EMPTY_TYPE, b"")


def is_empty_blob(data: str | bytes) -> bool:
    try:
        return unpack_blob(data)[0] == EMPTY_TYPE
    except WireError:
        return False


# ---------------------------------------------------------------------------
# checkpoint envelope
# ---------------------------------------------------------------------------


def pack_envelope(serde, body: dict, metadata: dict, parent_id: str | None) -> dict:
    """The JSON document stored in the checkpoint ``metadata`` column."""
    body_type, body_bytes = serde.dumps_typed(body)
    md_type, md_bytes = serde.dumps_typed(metadata)
    return {
        "sdk": ENVELOPE_VERSION,
        "cp": {"t": body_type, "d": base64.b64encode(body_bytes).decode("ascii")},
        "md": {"t": md_type, "d": base64.b64encode(md_bytes).decode("ascii")},
        "parent": encode_id(parent_id) if parent_id else None,
    }


def unpack_envelope(serde, envelope: Any) -> tuple[dict, dict, str | None]:
    """Read :func:`pack_envelope`; a foreign/unsupported envelope is explicit."""
    if not isinstance(envelope, dict):
        raise WireError(f"checkpoint envelope must be an object, got {type(envelope).__name__}")
    version = envelope.get("sdk")
    if version != ENVELOPE_VERSION:
        raise WireError(f"unsupported checkpoint envelope version {version!r}")
    try:
        body = serde.loads_typed((envelope["cp"]["t"], base64.b64decode(envelope["cp"]["d"])))
        metadata = serde.loads_typed((envelope["md"]["t"], base64.b64decode(envelope["md"]["d"])))
    except (KeyError, ValueError, TypeError) as exc:
        raise WireError(f"corrupt checkpoint envelope: {exc}") from exc
    parent = envelope.get("parent")
    return body, metadata, (decode_id(parent) if parent else None)
