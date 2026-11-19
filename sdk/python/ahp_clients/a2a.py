"""AHP A2A 1.0 machine face; stable message IDs, server-owned delegation grants."""
from copy import deepcopy
from itertools import count
from ._transport import Transport, AsyncTransport, APIError, JSON, decode, required, MAX_BYTES


class RPCError(Exception):
    def __init__(self, code: int, message: str, data=None):
        self.code, self.message, self.data = code, message, data
        super().__init__(f"A2A {code}: {message}")


def _result(response: JSON, request_id: int) -> JSON:
    if response.get("jsonrpc") != "2.0" or response.get("id") != request_id:
        raise APIError(200, "INVALID_RESPONSE", "invalid JSON-RPC envelope", outcome_unknown=True)
    if "error" in response:
        e = response["error"]
        if not isinstance(e, dict) or type(e.get("code")) is not int or not isinstance(e.get("message", ""), str):
            raise APIError(200, "INVALID_RESPONSE", "invalid JSON-RPC error", outcome_unknown=True)
        raise RPCError(e["code"], e.get("message", "request rejected"), e.get("data"))
    if not isinstance(response.get("result"), dict):
        raise APIError(200, "INVALID_RESPONSE", "invalid A2A result", outcome_unknown=True)
    return response["result"]


def _send(request: JSON) -> JSON:
    required(request.get("message", {}).get("messageId"), "stable messageId")
    return request


class A2AClient:
    """Task mTLS client. /a2a/public is human ingress and is not used here."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = Transport(base_url, token, http_client=http_client)
        self._ids = count(1)

    def _call(self, method: str, params: JSON, mutation: bool) -> JSON:
        request_id = next(self._ids)
        response = self._wire.call("/a2a/internal", {"jsonrpc": "2.0", "id": request_id,
                                    "method": method, "params": params}, mutation=mutation)
        return _result(response, request_id)

    def send(self, request: JSON) -> JSON:
        """Send one stable messageId; return task/message union envelope."""
        return self._call("SendMessage", _send(request), True)

    def delegate(self, parent_task: str, max_steps: int, request: JSON) -> JSON:
        """Request a child; the server validates/narrows the presented parent grant."""
        required(parent_task, "parent_task")
        if max_steps <= 0:
            raise ValueError("positive delegated step reserve required")
        request = deepcopy(request)
        request.setdefault("metadata", {}).update({"ahp_parent_task": parent_task, "max_steps": max_steps})
        return self.send(request)

    def get(self, task_id: str) -> JSON:
        """Read durable task status, artifacts and history."""
        return self._call("GetTask", {"id": required(task_id, "task_id")}, False)

    def cancel(self, task_id: str) -> JSON:
        """Request cooperative durable task cancellation."""
        return self._call("CancelTask", {"id": required(task_id, "task_id")}, True)

    def resubscribe(self, task_id: str):
        """Replay the durable snapshot/history then live events; there is no cursor."""
        source = self._stream("SubscribeToTask", {"id": required(task_id, "task_id")})
        try:
            for event in source:
                yield event
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        finally:
            source.close()

    def send_stream(self, request: JSON):
        """Send a stable messageId and yield bounded SSE event envelopes."""
        source = self._stream("SendStreamingMessage", _send(request))
        try:
            for event in source:
                yield event
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        finally:
            source.close()

    def _stream(self, method: str, params: JSON):
        request_id = next(self._ids)
        source = self._wire.lines("/a2a/internal", {"jsonrpc": "2.0", "id": request_id,
                                  "method": method, "params": params}, sse=True)
        data = bytearray()
        terminal = False
        try:
            for line in source:
                if line.startswith(b"data:"):
                    data.extend(line[5:].removeprefix(b" ") + b"\n")
                    if len(data) > MAX_BYTES:
                        raise APIError(200, "INVALID_RESPONSE", "SSE record exceeds 16 MiB")
                elif not line and data:
                    event = _result(decode(bytes(data)), request_id)
                    terminal = _terminal(event)
                    yield event
                    if terminal:
                        return
                    data.clear()
            if data:
                event = _result(decode(bytes(data)), request_id)
                terminal = _terminal(event)
                yield event
            if not terminal:
                raise APIError(0, "UNKNOWN", "A2A stream ended before interaction boundary; resubscribe task", outcome_unknown=True)
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        finally:
            source.close()


class AsyncA2AClient:
    """Task mTLS client. /a2a/public is human ingress and is not used here."""
    def __init__(self, base_url: str, token, *, http_client):
        self._wire = AsyncTransport(base_url, token, http_client=http_client)
        self._ids = count(1)

    async def _call(self, method: str, params: JSON, mutation: bool) -> JSON:
        request_id = next(self._ids)
        response = await self._wire.call("/a2a/internal", {"jsonrpc": "2.0", "id": request_id,
                                    "method": method, "params": params}, mutation=mutation)
        return _result(response, request_id)

    async def send(self, request: JSON) -> JSON:
        """Send one stable messageId; return task/message union envelope."""
        return await self._call("SendMessage", _send(request), True)

    async def delegate(self, parent_task: str, max_steps: int, request: JSON) -> JSON:
        """Request a child; the server validates/narrows the presented parent grant."""
        required(parent_task, "parent_task")
        if max_steps <= 0:
            raise ValueError("positive delegated step reserve required")
        request = deepcopy(request)
        request.setdefault("metadata", {}).update({"ahp_parent_task": parent_task, "max_steps": max_steps})
        return await self.send(request)

    async def get(self, task_id: str) -> JSON:
        """Read durable task status, artifacts and history."""
        return await self._call("GetTask", {"id": required(task_id, "task_id")}, False)

    async def cancel(self, task_id: str) -> JSON:
        """Request cooperative durable task cancellation."""
        return await self._call("CancelTask", {"id": required(task_id, "task_id")}, True)

    async def resubscribe(self, task_id: str):
        """Replay the durable snapshot/history then live events; there is no cursor."""
        source = self._stream("SubscribeToTask", {"id": required(task_id, "task_id")})
        try:
            async for event in source:
                yield event
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        finally:
            await source.aclose()

    async def send_stream(self, request: JSON):
        """Send a stable messageId and yield bounded SSE event envelopes."""
        source = self._stream("SendStreamingMessage", _send(request))
        try:
            async for event in source:
                yield event
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        finally:
            await source.aclose()

    async def _stream(self, method: str, params: JSON):
        request_id = next(self._ids)
        source = self._wire.lines("/a2a/internal", {"jsonrpc": "2.0", "id": request_id,
                                  "method": method, "params": params}, sse=True)
        data = bytearray()
        terminal = False
        try:
            async for line in source:
                if line.startswith(b"data:"):
                    data.extend(line[5:].removeprefix(b" ") + b"\n")
                    if len(data) > MAX_BYTES:
                        raise APIError(200, "INVALID_RESPONSE", "SSE record exceeds 16 MiB")
                elif not line and data:
                    event = _result(decode(bytes(data)), request_id)
                    terminal = _terminal(event)
                    yield event
                    if terminal:
                        return
                    data.clear()
            if data:
                event = _result(decode(bytes(data)), request_id)
                terminal = _terminal(event)
                yield event
            if not terminal:
                raise APIError(0, "UNKNOWN", "A2A stream ended before interaction boundary; resubscribe task", outcome_unknown=True)
        except APIError as error:
            if error.status == 0 or error.status == 200 or error.status >= 500:
                error.outcome_unknown = True
            raise
        finally:
            await source.aclose()


def _terminal(event: JSON) -> bool:
    if "message" in event:
        if not isinstance(event["message"], dict):
            raise APIError(200, "INVALID_RESPONSE", "invalid A2A message", outcome_unknown=True)
        return True
    task = event.get("task", event.get("statusUpdate", {}))
    if not isinstance(task, dict) or not isinstance(task.get("status", {}), dict):
        raise APIError(200, "INVALID_RESPONSE", "invalid A2A task status", outcome_unknown=True)
    state = task.get("status", {}).get("state")
    if state is not None and not isinstance(state, str):
        raise APIError(200, "INVALID_RESPONSE", "invalid A2A state", outcome_unknown=True)
    return state in {
        "TASK_STATE_COMPLETED", "TASK_STATE_CANCELED", "TASK_STATE_FAILED", "TASK_STATE_REJECTED",
        "TASK_STATE_INPUT_REQUIRED"}
