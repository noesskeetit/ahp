"""Explicit shared Memory records, independent of native Hindsight clients."""
from ._transport import APIError, Transport, AsyncTransport, required

PREFIX = "/v1/memory/shared/"


def _mutation(key, request):
    key = required(key)
    if "op_key" in request and request["op_key"] != key:
        raise ValueError("request op_key differs from the stable operation key")
    return dict(request, op_key=key)


class SharedMemoryClient:
    """Task-token + caller-owned mTLS; single attempt, no redirect or fallback."""

    def __init__(self, base_url, token, *, http_client):
        self._wire = Transport(base_url, token, http_client=http_client)

    def propose(self, key, request):
        """Persist a proposed fact; an ACK is not approval."""
        return self._wire.call(PREFIX + "Propose", _mutation(key, request), mutation=True)

    def search(self, request):
        return self._wire.call(PREFIX + "Search", request)

    def read(self, record_id, *, workspace=None):
        request = {"id": required(record_id, "record id")}
        if workspace is not None:
            request["workspace"] = required(workspace, "workspace")
        return self._wire.call(PREFIX + "Read", request)

    def get_operation(self, key):
        """Reconcile an operation key; record content is obtained by read()."""
        return self._wire.call(PREFIX + "Get", {"op_key": required(key)})

    def fence(self, key):
        """Close an uncommitted key; a committed result stays SUCCEEDED."""
        return self._wire.call(PREFIX + "Fence", {"op_key": required(key)}, mutation=True)

    def review(self, key, record_id, decision):
        """Server-side memory:review authority remains mandatory."""
        return self._wire.call(PREFIX + "Review", _mutation(key, {"id": required(record_id), "decision": required(decision)}), mutation=True)

    def add_labels(self, key, record_id, labels):
        return self._wire.call(PREFIX + "AddLabels", _mutation(key, {"id": required(record_id), "labels": labels}), mutation=True)

    def delete(self, key, record_id):
        result = self._wire.call(PREFIX + "Delete", _mutation(key, {"id": required(record_id)}), mutation=True)
        if result.get("ok") is not True:
            raise APIError(200, "INVALID_RESPONSE", "memory deletion was not acknowledged", outcome_unknown=True)
        return result

    def reindex(self, key, limit=20):
        return self._wire.call(PREFIX + "Reindex", _mutation(key, {"limit": limit}), mutation=True)

    def delete_subject(self, key, subject):
        """Privileged erasure; this is not a per-operation fence."""
        return self._wire.call(PREFIX + "DeleteSubject", _mutation(key, {"subject": required(subject)}), mutation=True)


class AsyncSharedMemoryClient:
    """Async counterpart; supplied HTTP clients remain caller-owned."""

    def __init__(self, base_url, token, *, http_client):
        self._wire = AsyncTransport(base_url, token, http_client=http_client)

    async def propose(self, key, request):
        return await self._wire.call(PREFIX + "Propose", _mutation(key, request), mutation=True)

    async def search(self, request):
        return await self._wire.call(PREFIX + "Search", request)

    async def read(self, record_id, *, workspace=None):
        request = {"id": required(record_id, "record id")}
        if workspace is not None:
            request["workspace"] = required(workspace, "workspace")
        return await self._wire.call(PREFIX + "Read", request)

    async def get_operation(self, key):
        return await self._wire.call(PREFIX + "Get", {"op_key": required(key)})

    async def fence(self, key):
        return await self._wire.call(PREFIX + "Fence", {"op_key": required(key)}, mutation=True)

    async def review(self, key, record_id, decision):
        return await self._wire.call(PREFIX + "Review", _mutation(key, {"id": required(record_id), "decision": required(decision)}), mutation=True)

    async def add_labels(self, key, record_id, labels):
        return await self._wire.call(PREFIX + "AddLabels", _mutation(key, {"id": required(record_id), "labels": labels}), mutation=True)

    async def delete(self, key, record_id):
        result = await self._wire.call(PREFIX + "Delete", _mutation(key, {"id": required(record_id)}), mutation=True)
        if result.get("ok") is not True:
            raise APIError(200, "INVALID_RESPONSE", "memory deletion was not acknowledged", outcome_unknown=True)
        return result

    async def reindex(self, key, limit=20):
        return await self._wire.call(PREFIX + "Reindex", _mutation(key, {"limit": limit}), mutation=True)

    async def delete_subject(self, key, subject):
        return await self._wire.call(PREFIX + "DeleteSubject", _mutation(key, {"subject": required(subject)}), mutation=True)
