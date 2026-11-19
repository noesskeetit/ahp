"""AHPStore: LangGraph BaseStore backed by AHP State API KV operations.

Replaces LangGraph's built-in store (in-memory, Postgres) with the AHP State
API KV primitive. All mutations are fenced by the active lease epoch.

Versioning / compare-and-set (SDK-R6-1)
---------------------------------------
The authoritative State contract (``internal/state/kv.go``) makes
``expected_version`` a real compare-and-set token:

  * ``expected_version == 0`` is create-only (CONFLICT on a live item);
  * any other value must equal the stored version (else CONFLICT);
  * ``DeleteKV`` requires ``expected_version >= 1``; absent delete is NOT_FOUND.

A successful write sets ``version = baseline + 1`` and a tombstone keeps its
version, so a delete -> recreate continues from ``baseline + 1``. ``GetKV``
returns NOT_FOUND for absent OR deleted, so the read path cannot see tombstone
generations.

Crash-safe durable operation identity (SDK-R6-2, Root 20261030T2103Z)
---------------------------------------------------------------------
The server deduplicates fenced mutations by ``operation_key``: it stores a
receipt keyed on ``(scope, operation_key)`` whose ``request_digest`` is
``Digest(op, key, namespace, expected_version, valueDigest)``. Replaying the
identical request returns the stored receipt (idempotent); a reused key with a
*different* digest fails KEY_REUSE_MISMATCH. The replay check runs BEFORE the
version CAS, so a retried request that already committed replays regardless of
how far the live version has since advanced.

Therefore, to survive "PutKV committed, response lost, process restarted with
the same node/call", the SDK must re-send the ORIGINAL ``(operation_key,
expected_version, payload)`` — never re-derive them from a freshly-read version.
When the adapter supplies a durable operation identity
(``with_identity(node, call)`` — the task/RecordIntent keys, task-local via a
contextvar), each write is journalled as a durable per-sub-operation record.

Per-sub-operation journal (SDK-R7, fixes P1/P2/P3)
--------------------------------------------------
A single ``(run, node, call)`` identity can span MANY logical writes (a
multi-op ``abatch`` under one call identity, or a put -> delete -> recreate
sequence). Treating that identity as a one-shot resource silently drops or
hijacks later writes, so the journal is a MAP keyed by the stable sub-operation
``operation_key`` — a pure function of ``(run, node, call, kind, namespace,
key, value)`` that does NOT fold in the live ``expected_version``. On re-entry
each sub-operation replays ITS OWN original ``(operation_key, expected_version,
value)`` (a genuine retry), and a sub-operation identity reused with a
*different* payload raises an explicit error instead of silently skipping (P1)
or returning a stale result (P3). The intent is kept in ``dispatched`` for the
call's lifetime so later sub-operations can still journal; each sub-operation's
completion is tracked inside ``result.subops`` (a backward-compatible shim also
reads a legacy flat single-sub-operation journal).

Unsupported / best-effort path (documented, NOT crash-idempotent)
------------------------------------------------------------------
A raw LangGraph ``BaseStore`` ``abatch`` call carries no task/call identity.
Without ``with_identity`` the store falls back to a read-modify-write CAS
upsert/delete. That is correct for sequential use but is **not** crash-idempotent
(a committed-but-lost-ack write can be re-issued against the new version), so it
is best-effort only and should not be claimed correct across a process restart;
the durable path requires an adapter-supplied identity. No silent error masking:
transport/server errors propagate (only NOT_FOUND reads map to None).

Usage:
    store = AHPStore(client, fence=fence)
    with store.with_identity(node=stable_task_id, call=persisted_tool_call_id):
        await store.abatch([PutOp(namespace=("ns",), key="k", value={...})])

The ``call`` identity is a tool_call_id from checkpointed model output or a
stable call position, never an ID re-minted inside the retrying mutation node.
The ``node`` identity is the task instance, not its reusable logical name.
"""

from __future__ import annotations

import contextlib
import contextvars
import hashlib
import json
from typing import Any, Iterable, Optional

from langgraph.store.base import BaseStore, GetOp, Item, ListNamespacesOp, PutOp, SearchOp

from ahp_state.client import (
    StateAPIClient,
    StateAPIError,
    StateConflictError,
    StateNotFoundError,
    StateForbiddenError,
    StateValidationError,
    StateServerError,
    StateTimeoutError,
)
from ahp_state.intents import FENCED, IntentRecorder

# Task-local operation identity (node, call). A ContextVar is per-asyncio-task,
# so two concurrent calls under different identities never clobber each other.
from ahp_state.sync import invoke_sync


_identity_var: contextvars.ContextVar[Optional[dict]] = contextvars.ContextVar(
    "ahp_operation_identity", default=None
)


class AHPStore(BaseStore):
    """State API KV store for LangGraph.

    Parameters
    ----------
    client : StateAPIClient
        Async State API client.
    fence : dict
        Lease fence for write operations.
    max_retries : int
        Bounded compare-and-set retry count on CONFLICT (best-effort path).
    """

    def __init__(self, client: StateAPIClient, fence: dict[str, Any], *, max_retries: int = 8):
        self._client = client
        self._fence = fence
        self._max_retries = max(0, max_retries)

    # --- operation identity (task-local) ---

    @contextlib.contextmanager
    def with_identity(self, *, node: str, call: str):
        """Scope a durable operation identity to writes made in this block.

        Backed by a contextvar so concurrent tasks each see their own identity.
        ``node`` must be the stable task instance ID, not a reusable logical
        node name. ``call`` is a tool_call_id from checkpointed model output, or
        an explicit call index if no tool_call_id exists. Model and mutation
        belong in separate nodes; re-minting an ID inside a retry is unsafe.
        """
        token = _identity_var.set({"node": node, "call": call})
        try:
            yield self
        finally:
            _identity_var.reset(token)

    @staticmethod
    def current_identity() -> Optional[dict]:
        """The active operation identity for this task, or None."""
        return _identity_var.get()

    # --- operation key (durable; NOT a function of the live version) ---

    @staticmethod
    def _encode_ns(namespace: Iterable[str]) -> str:
        return "".join(f"{len(seg)}:{seg}" for seg in namespace)

    def _op_key(self, namespace: Iterable[str], key: str, kind: str, value: Any = None) -> str:
        """Stable logical-operation identity, independent of expected_version.

        Folds run_id + (node,call when identity is active) + kind + length-
        prefixed namespace + key + value. It deliberately does NOT include the
        live version, so the SAME logical write always maps to the SAME key and
        the server can replay its receipt. The (expected_version, payload) pair
        is journaled alongside it (see ``_durable_write``) so a retry re-sends
        the ORIGINAL request and replays rather than double-writing.

        ``value`` must be JSON-serializable: it is folded into the key in
        canonical form via ``json.dumps(value, sort_keys=True,
        separators=(",", ":"))`` so key order / whitespace never change the
        identity. A non-JSON-serializable value (e.g. a ``set``, ``bytes``, or a
        custom object) raises ``TypeError`` here -- fail fast, before any
        HTTP / journal round-trip.
        """
        value_repr = json.dumps(value, sort_keys=True, separators=(",", ":"))
        ident = _identity_var.get()
        parts = [self._fence["run_id"], kind, self._encode_ns(namespace), key, value_repr]
        if ident is not None:
            parts[1:1] = [ident["node"], ident["call"]]
        digest = hashlib.sha256("\x00".join(parts).encode()).hexdigest()[:32]
        return f"{self._fence['run_id']}:kv:{digest}"

    def _recorder(self, node: str) -> IntentRecorder:
        return IntentRecorder(self._client, self._fence, node=node)

    # --- BaseStore batch entrypoint ---

    def batch(self, ops: Iterable) -> list:
        return invoke_sync(self, lambda view: view.abatch(list(ops)))

    async def abatch(self, ops: Iterable) -> list:
        results = []
        ident = _identity_var.get()
        for op in ops:
            if isinstance(op, GetOp):
                results.append(await self._aget(op.namespace, op.key))
            elif isinstance(op, PutOp):
                if op.value is not None:
                    if ident is not None:
                        await self._durable_write(ident["node"], ident["call"], op.namespace, op.key, "put", op.value)
                    else:
                        await self._put_cas(op.namespace, op.key, op.value)
                else:
                    if ident is not None:
                        await self._durable_write(ident["node"], ident["call"], op.namespace, op.key, "delete", None)
                    else:
                        await self._delete_cas(op.namespace, op.key)
                results.append(None)
            elif isinstance(op, SearchOp):
                results.append(await self._asearch(op.namespace_prefix, op.filter, op.limit))
            elif isinstance(op, ListNamespacesOp):
                results.append(await self._alist_namespaces(op.match_conditions, op.max_depth, op.limit, op.offset))
            else:
                results.append(None)
        return results

    # --- read (NOT_FOUND only -> None; everything else propagates) ---

    async def _read(self, namespace: tuple[str, ...], key: str) -> Optional[dict]:
        try:
            result = await self._client.get_kv(list(namespace), key)
        except StateNotFoundError:
            return None
        except StateAPIError:
            raise
        return result.get("item", result)

    @staticmethod
    def _ts(value: str) -> str:
        """Normalize ISO timestamp for Python 3.9 datetime.fromisoformat().

        Go's RFC3339Nano trims trailing zeros from the fractional part,
        producing 1-9 digits. Python 3.9 fromisoformat only accepts 3 or 6.
        We normalize to exactly 6 (microseconds): pad with zeros or truncate.
        Also handles the 'Z' suffix (not valid in Python 3.9).

        An absent timestamp becomes the epoch, never ``""``: LangGraph's ``Item``
        runs ``datetime.fromisoformat`` on this value with no guard, so an empty
        string raised ``ValueError`` from inside a plain *read* of agent memory.
        ``internal/state/kv.go`` declares ``updated_at NOT NULL`` and always
        marshals it, so an empty value means a stand or an older server that did
        not send one - a gap to survive, not one to fold on.
        """
        if not value:
            return "1970-01-01T00:00:00+00:00"
        s = value
        # Handle Z suffix
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        # Normalize fractional seconds to exactly 6 digits
        # Pattern: ...T HH:MM:SS.fff...+00:00 or ...T HH:MM:SS.fff...Z (already handled)
        import re
        m = re.search(r'(\d{2}:\d{2}:\d{2})\.(\d+)([+-]\d{2}:\d{2})?$', s)
        if m:
            frac = m.group(2)
            # Pad or truncate to 6 digits
            frac = (frac + "000000")[:6]
            s = s[:m.start(2)] + frac + s[m.end(2):]
        return s

    async def _aget(self, namespace: tuple[str, ...], key: str) -> Optional[Item]:
        item_data = await self._read(namespace, key)
        if item_data is None:
            return None
        ts = self._ts(item_data.get("updated_at", ""))
        return Item(
            value=item_data.get("value"),
            key=item_data.get("key", key),
            namespace=list(namespace),
            created_at=ts,
            updated_at=ts,
        )

    # --- durable (crash-safe) write: journalled (op_key, expected_version, value) ---

    async def _durable_write(self, node: str, call: str, namespace: tuple[str, ...],
                             key: str, kind: str, value: Any) -> dict:
        """Idempotent across retry/restart, with PER-SUB-OPERATION identity.

        One tool call (a single ``(node, call)`` identity) may perform several KV
        mutations (an ``abatch`` of puts, a put->delete->recreate sequence, ...).
        The journal is therefore a MAP keyed by the stable sub-operation identity
        (``op_key`` = f(run, node, call, kind, ns, key, value) — a pure function
        of the logical request, so a repeated same-key write maps to its OWN slot
        and a restart re-derives the same key). Each slot stores the ORIGINAL
        ``(op_key, expected_version, value)`` so a committed-but-lost-ack write
        REPLAYS via the server receipt instead of double-writing, and a NEW
        sub-operation (different key / value / kind) gets its own slot instead of
        being silently skipped (P1) or hijacking a stale op_key (P3).

        The intent is kept in ``dispatched`` for the call's lifetime (it never
        takes a terminal status here) so later sub-operations can still be
        journaled; each sub-operation's completion is tracked inside
        ``result.subops``. The journal is written BEFORE every mutation so it
        survives a crash between the write and the next step.
        """
        rec = self._recorder(node)
        current = await rec.get(call)
        if current is not None and current.get("status") == FENCED:
            raise StateAPIError("STALE_EPOCH", f"intent {call!r} is fenced")

        op_key = self._op_key(namespace, key, kind, value)

        # Load the per-sub-operation journal: the new ``subops`` map, or the
        # legacy single-slot flat result written by earlier SDK revisions.
        journal: dict = {}
        if current is not None and isinstance(current.get("result"), dict):
            res = current["result"]
            if isinstance(res.get("subops"), dict):
                journal = dict(res["subops"])
            elif res.get("op_key"):  # legacy flat single sub-operation
                journal[res["op_key"]] = {
                    "op_key": res["op_key"],
                    "expected_version": int(res.get("expected_version", 0)),
                    "value": res.get("value"),
                }  # no kind/namespace/key -> legacy; replayed by op_key only

        if op_key in journal:
            entry = journal[op_key]
            # Fingerprint validation (P5-class): a non-legacy slot records
            # (kind, namespace, key); a mismatch under the same identity is an
            # explicit error, never a silent skip or a stale result.
            if entry.get("kind") is not None and (
                entry.get("kind") != kind
                or tuple(entry.get("namespace", []) or []) != tuple(namespace)
                or entry.get("key") != key
            ):
                raise StateAPIError(
                    "INTENT_ARG_MISMATCH",
                    f"sub-operation identity {op_key} reused with a different "
                    f"(kind, namespace, key)",
                )
            expected = int(entry["expected_version"])
            val = entry.get("value") if kind == "put" else None
            issue = True
        else:
            cur = await self._read(namespace, key)
            if kind == "delete" and cur is None:
                # Idempotent no-op: deleting an item that was never created.
                return {"op_key": op_key, "key": key, "namespace": list(namespace)}
            expected = 0 if cur is None else int(cur.get("version", 0))
            val = value
            journal[op_key] = {
                "op_key": op_key, "expected_version": expected, "value": value,
                "kind": kind, "namespace": list(namespace), "key": key,
                "status": "pending",
            }
            if current is None:
                await rec.record(call, kind="kv",
                                 target="/".join(namespace) + "/" + key, args={})
            # Persist the journal BEFORE the mutation (crash-safe).
            await rec.update(call, "dispatched", result={"subops": journal})
            issue = True

        # Issue the fenced mutation with bounded timeout retry. A timeout means
        # the request may or may not have committed; re-sending the same
        # (op_key, expected_version, payload) is safe because the server's
        # receipt replay short-circuits to idempotent success.
        for attempt in range(self._max_retries + 1):
            try:
                if kind == "put":
                    await self._client.put_kv(self._fence, list(namespace), key, op_key, expected, val)
                else:
                    await self._client.delete_kv(self._fence, list(namespace), key, op_key, expected)
                break
            except StateTimeoutError:
                if attempt < self._max_retries:
                    continue
                raise
            except StateConflictError:
                # A CONFLICT on the durable path means the version moved under
                # us (another writer in the same run). This is a programming
                # error in the agent logic, not a transient condition — propagate.
                raise

        journal[op_key]["status"] = "succeeded"
        await rec.update(call, "dispatched", result={"subops": journal})
        return {"op_key": op_key, "key": key, "namespace": list(namespace)}

    # --- best-effort CAS path (no identity; NOT crash-idempotent; documented) ---

    async def _put_cas(self, namespace: tuple[str, ...], key: str, value: Any) -> None:
        for attempt in range(self._max_retries + 1):
            current = await self._read(namespace, key)
            expected = 0 if current is None else int(current.get("version", 0))
            op_key = self._op_key(namespace, key, "put", value)
            try:
                await self._client.put_kv(self._fence, list(namespace), key, op_key, expected, value)
                return
            except (StateConflictError, StateTimeoutError):
                if attempt < self._max_retries:
                    continue
                raise
            except StateAPIError:
                raise

    async def _delete_cas(self, namespace: tuple[str, ...], key: str) -> None:
        for attempt in range(self._max_retries + 1):
            current = await self._read(namespace, key)
            if current is None:
                return  # absent delete is an idempotent no-op
            expected = int(current.get("version", 0))
            op_key = self._op_key(namespace, key, "delete", None)
            try:
                await self._client.delete_kv(self._fence, list(namespace), key, op_key, expected)
                return
            except (StateConflictError, StateTimeoutError):
                if attempt < self._max_retries:
                    continue
                raise
            except StateAPIError:
                raise

    # --- search / namespaces (errors propagate) ---

    async def _asearch(self, namespace_prefix, filter, limit) -> list[Item]:
        result = await self._client.search_kv(
            namespace_prefix=list(namespace_prefix) if namespace_prefix else [],
            filter=filter or {}, limit=limit,
        )
        items_data = result.get("items", [])
        return [
            Item(value=item.get("value"), key=item.get("key", ""), namespace=item.get("namespace", []),
                 created_at=self._ts(item.get("updated_at", "")), updated_at=self._ts(item.get("updated_at", "")))
            for item in items_data
        ]

    async def _alist_namespaces(self, match_conditions, max_depth, limit, offset) -> list[tuple[str, ...]]:
        # Extract namespace prefix from match_conditions (prefix type only)
        ns_prefix: list[str] = []
        if match_conditions:
            for cond in match_conditions:
                if getattr(cond, "match_type", "prefix") == "prefix":
                    ns_prefix = list(getattr(cond, "path", ()))
                    break
        result = await self._client.search_kv(
            namespace_prefix=ns_prefix,
            limit=(limit + offset) if limit else 1000,
        )
        items_data = result.get("items", [])
        seen: set[tuple[str, ...]] = set()
        namespaces: list[tuple[str, ...]] = []
        for item in items_data:
            ns = tuple(item.get("namespace", []))
            if max_depth is not None and len(ns) > max_depth:
                continue
            if ns not in seen:
                seen.add(ns)
                namespaces.append(ns)
        return namespaces[offset: offset + limit] if limit else namespaces[offset:]
