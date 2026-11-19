"""AHP Tool Gateway receipts and separate human approval client."""
from ._transport import Transport, AsyncTransport, JSON, required


class ToolsClient:
    """Call/Get/Fence return journal receipts; UNKNOWN never triggers retry."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = Transport(base_url, token, http_client=http_client)

    def call(self, key: str, tool: str, arguments: JSON) -> JSON:
        """Dispatch one canonical tool action with the caller's durable key."""
        return self._wire.call("/v1/tools/Call", {"key": required(key), "tool": required(tool, "tool"), "arguments": arguments}, mutation=True)

    def get(self, key: str) -> JSON:
        """Reconcile by the original key, including SIDE_EFFECT_UNKNOWN."""
        return self._wire.call("/v1/tools/Get", {"key": required(key)})

    def fence(self, key: str) -> JSON:
        """Fence an unresolved effect without redispatching it."""
        return self._wire.call("/v1/tools/Fence", {"key": required(key)}, mutation=True)

    def connection_state(self) -> JSON:
        """Read configured connection identifiers, never credentials."""
        return self._wire.call("/v1/tools/ConnectionState", {})

    def tool_result(self, key: str) -> JSON:
        """Read the stored result of one SUCCEEDED operation, never re-dispatch it.

        Only a successful, provenance-bearing row yields a result. The owner
        answers every other state with an error and this client passes that
        classification through unchanged: NOT_FOUND when the key is absent or
        belongs to another tenant, deployment or subject; FORBIDDEN without a
        verified context, on foreign or missing provenance, on another context of
        the same subject, on a revoked personal grant, on a non-task bearer, and
        on a reconciled row whose provenance nobody may invent; CONFLICT for
        pending, uncertain, failed or stale rows, which means poll Get and never
        re-send the effect. ``result_storage`` is "inline" (raw MCP
        CallToolResult in ``payload``) or "artifact" (immutable ``artifact_ref``,
        bytes via ``fetch_result_bytes``).
        """
        result = self._wire.call("/v1/tools/GetResult", {"key": required(key)})
        _validate_result(result)
        return result

class ApprovalClient:
    """Explicit human-only client; construction does not confer consent authority."""
    def __init__(self, base_url: str, human_token, *, http_client):
        self._wire = Transport(base_url, human_token, http_client=http_client)

    def approve(self, request_id: str, digest: str, tool: str) -> JSON:
        """Approve precisely the reviewed canonical request digest."""
        return self._wire.call("/v1/tools/Approve", {"request_id": required(request_id), "digest": required(digest), "tool": required(tool)}, mutation=True)

    def approvals(self, limit: int = 100) -> JSON:
        """Read the human's pending worklist."""
        return self._wire.call("/v1/tools/Approvals", {"limit": limit})


class AsyncToolsClient:
    """Call/Get/Fence return journal receipts; UNKNOWN never triggers retry."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = AsyncTransport(base_url, token, http_client=http_client)

    async def call(self, key: str, tool: str, arguments: JSON) -> JSON:
        """Dispatch one canonical tool action with the caller's durable key."""
        return await self._wire.call("/v1/tools/Call", {"key": required(key), "tool": required(tool, "tool"), "arguments": arguments}, mutation=True)

    async def get(self, key: str) -> JSON:
        """Reconcile by the original key, including SIDE_EFFECT_UNKNOWN."""
        return await self._wire.call("/v1/tools/Get", {"key": required(key)})

    async def fence(self, key: str) -> JSON:
        """Fence an unresolved effect without redispatching it."""
        return await self._wire.call("/v1/tools/Fence", {"key": required(key)}, mutation=True)

    async def connection_state(self) -> JSON:
        """Read configured connection identifiers, never credentials."""
        return await self._wire.call("/v1/tools/ConnectionState", {})

    async def tool_result(self, key: str) -> JSON:
        """Read the stored result of one SUCCEEDED operation, never re-dispatch it."""
        result = await self._wire.call("/v1/tools/GetResult", {"key": required(key)})
        _validate_result(result)
        return result

class AsyncApprovalClient:
    """Explicit human-only client; construction does not confer consent authority."""
    def __init__(self, base_url: str, human_token, *, http_client):
        self._wire = AsyncTransport(base_url, human_token, http_client=http_client)

    async def approve(self, request_id: str, digest: str, tool: str) -> JSON:
        """Approve precisely the reviewed canonical request digest."""
        return await self._wire.call("/v1/tools/Approve", {"request_id": required(request_id), "digest": required(digest), "tool": required(tool)}, mutation=True)

    async def approvals(self, limit: int = 100) -> JSON:
        """Read the human's pending worklist."""
        return await self._wire.call("/v1/tools/Approvals", {"limit": limit})


class ResultShapeError(RuntimeError):
    """The owner reported a result that contradicts its own declared shape.

    This is not a caller mistake and not a retry hint: acting on an inline body
    that is missing, or on an artifact row that also carries bytes inline, would
    make a decision on the wrong payload.
    """


def _validate_result(result: JSON) -> None:
    storage = result.get("result_storage")
    payload = result.get("payload")
    ref = result.get("artifact_ref")
    if storage == "inline":
        if payload in (None, "", {}):
            raise ResultShapeError("tools sdk: owner reported an inline result with no payload")
        if ref is not None:
            raise ResultShapeError("tools sdk: inline result also carries an artifact reference")
    elif storage == "artifact":
        if not isinstance(ref, dict) or not ref.get("artifact_id") or not ref.get("digest"):
            raise ResultShapeError("tools sdk: artifact result carries no verifiable reference")
        if payload not in (None, "", {}):
            raise ResultShapeError("tools sdk: artifact result also carries an inline payload")
    else:
        raise ResultShapeError("tools sdk: unknown result_storage %r" % (storage,))


def fetch_result_bytes(artifacts, result: JSON) -> bytes:
    """Fetch an artifact-stored result and verify its digest before returning it.

    ``artifacts`` is any Artifacts client exposing ``download(artifact_id)``.
    Bytes are fetched under the caller's CURRENT authority: reading a result
    grants no extra storage capability, so a revoke between the two calls fails
    closed at the Artifacts owner.
    """
    if result.get("result_storage") != "artifact":
        raise ValueError("tools sdk: result is not artifact-stored; use payload")
    ref = result["artifact_ref"]
    body = artifacts.download(ref["artifact_id"])
    if result.get("digest") and result["digest"] != ref["digest"]:
        raise ResultShapeError("tools sdk: result digest and artifact digest disagree")
    _require_digest(ref["digest"], body)
    if int(ref.get("size") or 0) and len(body) != int(ref["size"]):
        raise ResultShapeError("tools sdk: stored bytes do not match the declared size")
    return body


async def fetch_result_bytes_async(artifacts, result: JSON) -> bytes:
    """Async counterpart of :func:`fetch_result_bytes`."""
    if result.get("result_storage") != "artifact":
        raise ValueError("tools sdk: result is not artifact-stored; use payload")
    ref = result["artifact_ref"]
    body = await artifacts.download(ref["artifact_id"])
    if result.get("digest") and result["digest"] != ref["digest"]:
        raise ResultShapeError("tools sdk: result digest and artifact digest disagree")
    _require_digest(ref["digest"], body)
    if int(ref.get("size") or 0) and len(body) != int(ref["size"]):
        raise ResultShapeError("tools sdk: stored bytes do not match the declared size")
    return body


def _require_digest(digest: str, body: bytes) -> None:
    import hashlib

    want = digest.split(":", 1)[-1]
    try:
        want = bytes.fromhex(want)
    except ValueError as exc:
        raise ResultShapeError("tools sdk: declared digest is not hex") from exc
    if hashlib.sha256(body).digest() != want:
        raise ResultShapeError("tools sdk: stored bytes do not match the declared digest")

