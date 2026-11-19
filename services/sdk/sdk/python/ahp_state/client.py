"""Async HTTP client for the AHP State API.

All calls go to POST {base}/v1/state/{Operation} with a strict JSON body.
Scope is derived server-side from mTLS + task token; never from request bodies.

Fence-scoped operations use nested fence: {"fence": {"run_id":..., "worker":..., "epoch":...}}
per the State HTTP contract (DisallowUnknownFields on server).
Flat run_id/worker/epoch at root ONLY for Heartbeat, Release, Suspend.
"""

from __future__ import annotations

import json
import inspect
from typing import Any, Callable

import httpx


class StateAPIError(Exception):
    """Base exception for AHP State API errors.

    The ``code`` field carries the server's stable machine-readable error
    code (e.g. "CONFLICT", "NOT_FOUND", "STALE_EPOCH").

    ``code`` is the DOMAIN code, not merely the HTTP class. Live State flattens
    some domain codes onto a coarser class - ``KEY_REUSE_MISMATCH`` rides on
    HTTP 400 as ``INVALID_ARGUMENT`` with the domain code in a sibling
    ``reason`` field (``pkg/transport/json.go`` ``WriteError``) - so a client
    that reads only ``code`` cannot tell "your key was already used for a
    different request" from "your version is negative", and the idempotency
    guards that depend on that difference silently never fire. ``http_class``
    keeps the flattened class for whoever needs it.
    """

    def __init__(self, code: str, message: str, *, http_class: str | None = None):
        self.code = code
        self.http_class = http_class or code
        self.message = message
        super().__init__(f"{code}: {message}" if message else code)


class StateConflictError(StateAPIError):
    """HTTP 409: CAS version conflict, STALE_EPOCH, or LEASE_LOST."""


class StateNotFoundError(StateAPIError):
    """HTTP 404: resource not found (absent KV item, unknown checkpoint, etc.)."""


class StateForbiddenError(StateAPIError):
    """HTTP 403: forbidden (bad token, scope mismatch, or revocation)."""


class StateValidationError(StateAPIError):
    """HTTP 400: invalid argument or KEY_REUSE_MISMATCH."""


class StateServerError(StateAPIError):
    """HTTP 5xx: internal server error."""


class StateTimeoutError(StateAPIError):
    """Network-level timeout (no response from server)."""


def _raise_typed(status_code: int, err: dict) -> StateAPIError:
    """Map an HTTP error status + JSON body to a typed StateAPIError."""
    surfaced = err.get("code", "UNKNOWN")
    code = err.get("reason") or surfaced
    message = err.get("message", "")
    # ``WriteError`` marshals ``Message: err.Error()``, and ``contracts.Error``
    # already renders "CODE: text" - so the wire message arrives pre-prefixed.
    # Strip one prefix, or every SDK error reads "CODE: CODE: text".
    for prefix in (f"{code}: ", f"{surfaced}: "):
        if message.startswith(prefix):
            message = message[len(prefix):]
            break
    if code != surfaced:
        return _typed(status_code, code, message, http_class=surfaced)
    return _typed(status_code, code, message)


def _typed(status_code: int, code: str, message: str,
           *, http_class: str | None = None) -> StateAPIError:
    if status_code == 404:
        return StateNotFoundError(code, message, http_class=http_class)
    elif status_code == 403:
        return StateForbiddenError(code, message, http_class=http_class)
    elif status_code == 400:
        return StateValidationError(code, message, http_class=http_class)
    elif status_code == 409:
        return StateConflictError(code, message, http_class=http_class)
    elif status_code >= 500:
        return StateServerError(code or "INTERNAL", message or "internal server error",
                                http_class=http_class)
    return StateAPIError(code, message, http_class=http_class)


def _fence(fence: dict) -> dict:
    """Build the nested fence object required by the State HTTP contract."""
    return {
        "run_id": fence["run_id"],
        "worker": fence["worker"],
        "epoch": fence["epoch"],
    }


def _flat_fence(fence: dict) -> dict:
    """Build flat fence fields for Heartbeat/Release/Suspend (root-level)."""
    return {
        "run_id": fence["run_id"],
        "worker": fence["worker"],
        "epoch": fence["epoch"],
    }


from ahp_state.common import CommonStateMethods


class StateAPIClient(CommonStateMethods):
    """Thin async client for the AHP State API.

    Parameters
    ----------
    base_url : str
        Bare https origin (no path).
    token : str | None
        Task token for task-scoped calls (Bearer).
    http_client : httpx.AsyncClient | None
        Pre-configured HTTP client (for mTLS etc). If None, uses the factory or default.
    http_client_factory : Callable[[], httpx.AsyncClient] | None
        Required for sync adapters with an injected client. Each call must return
        a new pool preserving mTLS, CA, timeout, proxy policy and connection limits.
        Async calls use the original pool; sync calls own and close their fresh pool.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        token_source: Callable[[], Any] | None = None,
        http_client: httpx.AsyncClient | None = None,
        owns_http_client: bool = False,
        artifacts_base_url: str | None = None,
        http_client_factory: Callable[[], httpx.AsyncClient] | None = None,
    ):
        self._base = base_url.rstrip("/")
        if token is not None and token_source is not None:
            raise ValueError("choose a fixed token or token_source")
        self._token = token
        self._token_source = token_source
        self._client = http_client if http_client is not None else (
            http_client_factory() if http_client_factory else httpx.AsyncClient(timeout=30.0))
        self._http_client_factory = http_client_factory
        if http_client is None and http_client_factory is None:
            self._http_client_factory = lambda: httpx.AsyncClient(timeout=30.0)
        # Ownership, not merely "was one handed in": mtls_client builds its own
        # TLS-bearing pool and hands it over, and a pool nobody owns is a pool
        # that leaks on every container restart.
        self._owns_client = http_client is None or owns_http_client
        from ahp_state.payload import Payloads
        self._payloads = Payloads(self, artifacts_base_url)

    def with_token(self, token: str | None) -> "StateAPIClient":
        """A task-bound view sharing the async pool and explicit transport factory."""
        return StateAPIClient(self._base, token=token, http_client=self._client,
                              http_client_factory=self._http_client_factory,
                              artifacts_base_url=self._payloads.base)

    def fork_for_sync(self) -> "StateAPIClient":
        """Create an owned, independent pool for one synchronous adapter call.

        A supplied HTTP client needs an explicit factory preserving its mTLS,
        timeout and limits. Private httpx transport fields are never copied.
        """
        if self._http_client_factory is None:
            raise RuntimeError("sync AHP adapters require http_client_factory for an injected HTTP client")
        fresh = self._http_client_factory()
        if fresh is self._client:
            raise RuntimeError("http_client_factory must return a fresh independent HTTP client")
        return StateAPIClient(self._base, token=self._token, token_source=self._token_source, http_client=fresh,
                              owns_http_client=True, http_client_factory=self._http_client_factory,
                              artifacts_base_url=self._payloads.base)

    def with_token_source(self, source: Callable[[], Any]) -> "StateAPIClient":
        """An independent credential view; the transport pool stays shared."""
        return StateAPIClient(self._base, token_source=source, http_client=self._client,
                              http_client_factory=self._http_client_factory,
                              artifacts_base_url=self._payloads.base)

    async def authorization_token(self) -> str | None:
        if self._token_source is None:
            return self._token
        token = self._token_source()
        if inspect.isawaitable(token):
            token = await token
        if not isinstance(token, str) or not token.strip():
            raise StateForbiddenError("FORBIDDEN", "token source returned an empty credential")
        return token

    async def __aenter__(self) -> "StateAPIClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    @property
    def base_url(self) -> str:
        """The State API origin this client talks to (no trailing slash)."""
        return self._base

    @property
    def http_client(self) -> httpx.AsyncClient:
        """The pool this client speaks through.

        Exposed so a service can build a per-run, token-bound client over the
        SAME pool - the alternative is a new TLS handshake per turn, or quietly
        reusing the client that built it, which carries that client's
        credential. Ownership stays with whoever constructed it: reading the
        pool is not owning it, so ``close()`` is still not implied here.
        """
        return self._client

    async def close(self) -> None:
        """Close the underlying HTTP client if we own it."""
        if self._owns_client:
            await self._client.aclose()

    async def call(self, operation: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
        """Execute one State API operation."""
        if operation in ("PutCheckpoint", "PutWrites", "PutBatch"):
            encoded = json.dumps(body or {}, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
            if len(encoded) > 1 << 20:
                raise StateValidationError("INVALID_ARGUMENT", "checkpoint request exceeds 1MiB after payload spill")
        token = await self.authorization_token() if operation != "ClaimRun" else self._token
        return await self._request("POST", f"/v1/state/{operation}", token, body or {})

    async def request_state(
        self, method: str, path: str, *, body: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a relative State API route using the verified workload transport."""
        if not path.startswith("/v1/state/"):
            raise StateValidationError("INVALID_ARGUMENT", "expected a relative State API path")
        return await self._request(method, path, await self.authorization_token(), body, params)

    async def request_tool(self, operation: str, body: dict[str, Any]) -> dict[str, Any]:
        """Use the authenticated gateway exposed by the workload transport."""
        if operation not in {"Call", "Get", "GetResult"}:
            raise StateValidationError("INVALID_ARGUMENT", "unsupported Tool Gateway operation")
        return await self._request("POST", f"/v1/tools/{operation}", await self.authorization_token(), body)

    async def _request(
        self, method: str, path: str, token: str | None, body: dict[str, Any] | None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers: dict[str, str] = {"Content-Type": "application/json", "Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            resp = await self._client.request(method, self._base + path, json=body, params=params,
                                              headers=headers, follow_redirects=False)
        except httpx.TimeoutException as e:
            raise StateTimeoutError("TIMEOUT", f"{type(e).__name__}: {e}") from e
        except httpx.HTTPError as e:
            raise StateAPIError("TRANSPORT", f"{type(e).__name__}: {e}") from e
        if 300 <= resp.status_code < 400:
            raise StateForbiddenError("FORBIDDEN", "State redirect refused")
        if resp.status_code >= 400:
            try:
                err = resp.json()
                raise _raise_typed(resp.status_code, err)
            except (json.JSONDecodeError, KeyError):
                raise StateAPIError("UNKNOWN", f"HTTP {resp.status_code}") from None
        return resp.json()

    # --- Thread lifecycle ---

    async def create_thread(self, context_id: str, key: str) -> dict:
        return await self.call("CreateThread", {"context_id": context_id, "key": key})

    async def create_run(self, thread_id: str, key: str, payload: dict, task_id: str = "") -> dict:
        body: dict[str, Any] = {"thread_id": thread_id, "key": key, "payload": payload}
        if task_id:
            body["task_id"] = task_id
        return await self.call("CreateRun", body)

    async def claim_run(self, worker: str, ttl_ms: int = 120000,
                        run_id: str | None = None, *,
                        payload_type: str | None = None) -> dict:
        """Claim an eligible FIFO head using workload mTLS.

        Optional selectors match the run id and ``payload.type``. Neither permits
        overtaking an earlier unfinished run in the same thread or claiming a
        different workload scope. Omitted selectors leave the queue unfiltered.
        """
        if payload_type is not None:
            try:
                encoded = payload_type.encode('utf-8')
            except UnicodeEncodeError:
                raise StateValidationError("INVALID_ARGUMENT", "payload_type must be valid UTF-8") from None
            if len(encoded) > 128:
                raise StateValidationError("INVALID_ARGUMENT", "payload_type must be at most 128 bytes")
        body: dict[str, Any] = {"worker": worker, "ttl_ms": ttl_ms}
        if run_id:
            body["run_id"] = run_id
        if payload_type:
            body["payload_type"] = payload_type
        return await self.call("ClaimRun", body)

    async def claim_run_for(self, worker: str, run_id: str,
                            ttl_ms: int = 120000, *,
                            payload_type: str | None = None) -> dict:
        """Claim exactly ``run_id``, and only when it is its thread's head.

        A named run that is not the head answers with no lease rather than with
        somebody else's work, so a busy worker leaves the queue alone and retries.
        """
        if not run_id:
            raise StateValidationError("INVALID_ARGUMENT", "claim_run_for requires the run_id to execute")
        return await self.claim_run(worker, ttl_ms=ttl_ms, run_id=run_id, payload_type=payload_type)

    async def release(self, fence: dict, status: str = "done",
                      close_reason: str | None = None,
                      failure_cause: str | None = None) -> dict:
        """Release a lease. Uses FLAT fence (run_id/worker/epoch at root).

        ``close_reason`` names why the run ended, so the run's terminal row carries
        the same terminal fact the execution plane journals for its task.
        ``failure_cause`` is this plane's own sentence about the same end, stored
        verbatim beside the dictionary word. Since DEF-RELEASE-REASON-1 a run ending
        ``failed`` or ``canceled`` owes State at least one of the two: the door refuses
        an unexplained terminal the same way it refuses an unexplained batch status
        (PDF rev3.2 §2.5). Each half is optional on the wire and empty means "no key
        sent", so a caller that states nothing sends byte-identical historical bytes.
        """
        body: dict[str, Any] = {**_flat_fence(fence), "status": status}
        if close_reason:
            body["close_reason"] = close_reason
        if failure_cause:
            body["failure_cause"] = failure_cause
        return await self.call("Release", body)

    async def heartbeat(self, fence: dict, ttl_ms: int = 60000) -> dict:
        """Send heartbeat. Uses FLAT fence."""
        body: dict[str, Any] = {**_flat_fence(fence), "ttl_ms": ttl_ms}
        return await self.call("Heartbeat", body)

    async def suspend(self, fence: dict, wait_for: str = "", deadline: str = "") -> dict:
        """Suspend a run. Uses FLAT fence."""
        body: dict[str, Any] = {**_flat_fence(fence), "wait_for": wait_for}
        if deadline:
            body["deadline"] = deadline
        return await self.call("Suspend", body)

    async def delete_thread(self, thread_id: str) -> None:
        """Delete a thread and all its sub-resources (cascade)."""
        await self.call("DeleteThread", {"thread_id": thread_id})

    # --- Checkpoint operations ---

    async def put_checkpoint(self, fence: dict, checkpoint: dict, key: str) -> dict:
        checkpoint = await self._payloads.checkpoint(fence["run_id"], checkpoint)
        result = await self.call("PutCheckpoint", {
            "fence": _fence(fence),
            "checkpoint": checkpoint,
            "key": key,
        })
        return await self._payloads.hydrate_checkpoint(result)

    async def get_checkpoint(self, thread_id: str, namespace: str, cp_id: str = "") -> dict:
        """Read one checkpoint (or the newest of a namespace when ``cp_id`` is empty).

        ``GetCheckpoint`` answers with the checkpoint object itself - the handler
        writes ``state.Checkpoint`` directly (``internal/stateapi``). A response
        still wrapped in the historical ``{"checkpoint": ...}`` envelope is
        normalized here, in one place, so no caller has to guess the shape.
        """
        body: dict[str, Any] = {"thread_id": thread_id, "namespace": namespace}
        if cp_id:
            body["id"] = cp_id
        result = await self.call("GetCheckpoint", body)
        if isinstance(result, dict) and "id" not in result:
            inner = result.get("checkpoint")
            if isinstance(inner, dict):
                return await self._payloads.hydrate_checkpoint(inner)
        return await self._payloads.hydrate_checkpoint(result)

    async def list_checkpoints(self, thread_id: str, namespace: str, limit: int = 1000) -> list[dict]:
        result = await self.call("ListCheckpoints", {
            "thread_id": thread_id, "namespace": namespace, "limit": limit,
        })
        return [await self._payloads.hydrate_checkpoint(cp) for cp in result.get("checkpoints", [])]

    async def put_writes(self, fence: dict, namespace: str, checkpoint_id: str, writes: list[dict]) -> None:
        writes = await self._payloads.writes(fence["run_id"], writes)
        await self.call("PutWrites", {
            "fence": _fence(fence),
            "namespace": namespace,
            "checkpoint_id": checkpoint_id,
            "writes": writes,
        })

    async def get_writes(self, thread_id: str, namespace: str, checkpoint_id: str) -> list[dict]:
        result = await self.call("GetWrites", {
            "thread_id": thread_id, "namespace": namespace, "checkpoint_id": checkpoint_id,
        })
        return [await self._payloads.hydrate_row(w) for w in result.get("writes", [])]

    async def get_part(self, thread_id: str, namespace: str, version: str, channel: str) -> dict:
        result = await self.call("GetPart", {
            "thread_id": thread_id, "namespace": namespace,
            "version": version, "channel": channel,
        })
        return await self._payloads.hydrate_row(result)

    # --- KV operations ---

    async def get_kv(self, namespace: list[str], key: str) -> dict:
        return await self.call("GetKV", {"namespace": namespace, "key": key})

    async def put_kv(self, fence: dict, namespace: list[str], key: str, operation_key: str,
                     expected_version: int, value: Any) -> dict:
        return await self.call("PutKV", {
            "fence": _fence(fence),
            "namespace": namespace,
            "key": key,
            "operation_key": operation_key,
            "expected_version": expected_version,
            "value": value,
        })

    async def delete_kv(self, fence: dict, namespace: list[str], key: str, operation_key: str,
                        expected_version: int) -> None:
        await self.call("DeleteKV", {
            "fence": _fence(fence),
            "namespace": namespace,
            "key": key,
            "operation_key": operation_key,
            "expected_version": expected_version,
        })

    # --- Search ---

    async def search_kv(self, namespace_prefix: list[str], key_prefix: str = "",
                        filter: dict | None = None, limit: int = 100) -> dict:
        body: dict[str, Any] = {"namespace_prefix": namespace_prefix, "limit": limit}
        if key_prefix:
            body["key_prefix"] = key_prefix
        if filter:
            body["filter"] = filter
        return await self.call("SearchKV", body)

    # --- Thread listing ---

    async def list_threads(self, limit: int = 100, cursor: str = "") -> dict:
        body: dict[str, Any] = {"limit": limit}
        if cursor:
            body["cursor"] = cursor
        return await self.call("ListThreads", body)

    # --- Intent (nested fence) ---

    async def get_intent(self, run_id: str, node: str, call: str) -> dict:
        return await self.call("GetIntent", {
            "run_id": run_id, "node": node, "call": call,
        })

    async def record_intent(self, fence: dict, node: str, call: str,
                            kind: str, target: str, args: Any = None) -> dict:
        body: dict[str, Any] = {
            "fence": _fence(fence),
            "node": node,
            "call": call,
            "kind": kind,
            "target": target,
        }
        if args is not None:
            body["args"] = args
        return await self.call("RecordIntent", body)

    async def update_intent(self, fence: dict, node: str, call: str,
                            status: str, result: Any = None) -> dict:
        body: dict[str, Any] = {
            "fence": _fence(fence),
            "node": node,
            "call": call,
            "status": status,
        }
        if result is not None:
            body["result"] = result
        return await self.call("UpdateIntent", body)

    async def reconcile_intent(self, fence: dict, node: str, call: str,
                               status: str = "", digest: str = "",
                               result: Any = None) -> dict:
        body: dict[str, Any] = {
            "fence": _fence(fence),
            "node": node,
            "call": call,
            "status": status,
            "digest": digest,
        }
        if result is not None:
            body["result"] = result
        return await self.call("ReconcileIntent", body)

    # --- Inbox ---

    async def ack_inbox(self, fence: dict, ids: list[str]) -> dict:
        return await self.call("AckInbox", {
            "fence": _fence(fence),
            "ids": ids,
        })

    # --- Stream (nested fence) ---

    async def append_stream(self, fence: dict, key: str, kind: str,
                            payload: Any = None) -> dict:
        body: dict[str, Any] = {
            "fence": _fence(fence),
            "key": key,
            "kind": kind,
            "payload": payload if payload is not None else {},
        }
        return await self.call("AppendStream", body)

    async def read_stream(self, thread_id: str, after: int = 0, limit: int = 100) -> dict:
        return await self.call("ReadStream", {
            "thread_id": thread_id, "after": after, "limit": limit,
        })

    # --- Batch (nested fence, struct form) ---

    async def put_batch(self, fence: dict, batch: dict) -> dict:
        """Submit a batch operation.

        ``batch`` is a BatchRequest struct (dict) with fields:
        - key: str (required)
        - checkpoint: Checkpoint (optional)
        - stream: StreamOp (optional)
        - run_status: RunStatusOp (optional)
        - writes: []PendingWrite (optional)
        - writes_namespace: str (optional)
        """
        batch = dict(batch)
        if batch.get("checkpoint"):
            batch["checkpoint"] = await self._payloads.checkpoint(fence["run_id"], batch["checkpoint"], 256 << 10)
        if batch.get("writes"):
            batch["writes"] = await self._payloads.writes(fence["run_id"], batch["writes"], 256 << 10)
        result = await self.call("PutBatch", {
            "fence": _fence(fence),
            "batch": batch,
        })
        if result.get("checkpoint"):
            result["checkpoint"] = await self._payloads.hydrate_checkpoint(result["checkpoint"])
        return result


    # --- Thread/Run queries ---

    async def get_thread(self, thread_id: str) -> dict:
        return await self.call("GetThread", {"thread_id": thread_id})

    async def get_thread_by_context(self, context_id: str) -> dict:
        return await self.call("GetThreadByContext", {"context_id": context_id})

    async def get_run(self, run_id: str) -> dict:
        return await self.call("GetRun", {"run_id": run_id})

    async def list_runs(self, thread_id: str) -> dict:
        return await self.call("ListRuns", {"thread_id": thread_id})

    # --- Inbox ---

    async def enqueue(self, thread_id: str, key: str, kind: str, payload: Any = None) -> dict:
        body: dict[str, Any] = {"thread_id": thread_id, "key": key, "kind": kind}
        if payload is not None:
            body["payload"] = payload
        return await self.call("Enqueue", body)

    async def read_inbox(self, thread_id: str, after: int = 0, limit: int = 100) -> dict:
        return await self.call("ReadInbox", {
            "thread_id": thread_id, "after": after, "limit": limit,
        })

    # --- Admin / Control ---

    async def delete_subject(self) -> dict:
        return await self.call("DeleteSubject", {})

    async def set_legalhold(self, thread_id: str, reason: str = "") -> dict:
        body: dict[str, Any] = {"thread_id": thread_id}
        if reason:
            body["reason"] = reason
        return await self.call("SetLegalhold", body)

    async def remove_legalhold(self, thread_id: str) -> dict:
        return await self.call("RemoveLegalhold", {"thread_id": thread_id})

    async def export_thread(self, thread_id: str) -> dict:
        """Legacy task route (forbidden by server); use recovery.StateAdminClient."""
        return await self.call("ExportThread", {"thread_id": thread_id})

    async def import_thread(self, export: dict, context_id: str = "") -> dict:
        body: dict[str, Any] = {"export": export}
        if context_id:
            body["context_id"] = context_id
        return await self.call("ImportThread", body)
