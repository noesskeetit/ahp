"""``AHPCheckpointSaver`` - the LangGraph checkpointer that keeps durable state in AHP.

Block 1 of PDF rev 3.2 (§4.2, coverage row "AHPCheckpointSaver + durability=sync,
pending writes"). One LangGraph ``thread`` lives in one AHP State thread under one
lease: every write carries the fence ``(run_id, worker, epoch)``, so a zombie pod
whose epoch went stale is rejected by the server instead of corrupting state.

Mapping onto the State contract
-------------------------------
``put``            -> ``PutCheckpoint`` (header + part refs + blobs, one atomic
                      durable commit; idempotent on the request key)
``put_writes``     -> ``PutWrites`` (pending writes, unique on
                      ``(checkpoint, task, index)``, replay is a no-op)
``get_tuple``      -> ``GetCheckpoint`` + one ``GetPart`` per channel version +
                      ``GetWrites``
``list``           -> ``ListCheckpoints`` (newest first) + the same reassembly
``delete_thread``  -> ``DeleteThread``

Durability is synchronous by construction: every method awaits its own fenced
HTTP call and nothing is buffered or batched, so when ``put``/``put_writes``
return the state is already durable and visible to another process. The
execution side must also drive LangGraph with ``durability="sync"`` - see
:mod:`ahp_state.agent`, which pins it.

Synchronous surface
-------------------
LangGraph calls the sync API from sync code and the async API from async code.
The client is an ``httpx.AsyncClient``, so the sync methods run their coroutines
with an independent per-call transport from the explicit client factory. No
connection pool crosses event loops, and concurrent async calls keep their own
client. mTLS and transport policy come from that factory, never private fields.
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Iterator, Optional, Sequence

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    PendingWrite,
    get_checkpoint_id,
    get_checkpoint_metadata,
)

from ahp_state.client import StateAPIClient, StateAPIError
from ahp_state import wire
from ahp_state.wire import WireError
from ahp_state.sync import invoke_sync

NAMESPACE = "graph"

#: Separator between this SDK's checkpoint-namespace prefix and a nested
#: LangGraph namespace. ``/`` is inside the State ``validIdentifier`` charset;
#: LangGraph's own separator ``|`` is not, which is exactly why the nested part is
#: escaped before it goes on the wire.
AHP_NS_SEP = "/"


class AHPCheckpointSaver(BaseCheckpointSaver[str]):
    """Durable, lease-fenced LangGraph checkpointer over the AHP State API.

    Parameters
    ----------
    client:
        Async State API client already bound to a task token.
    thread_id:
        The AHP State thread this lease owns (never a LangGraph thread id - the
        server derives the thread from the lease, and so do we).
    fence:
        ``{"run_id", "worker", "epoch"}`` of the active lease.
    namespace:
        The AHP checkpoint namespace this saver owns for the root graph. A
        LangGraph subgraph namespace is folded under it (reversibly), so nested
        checkpoints of ``ask:sub`` and the root never share an AHP namespace.
    """

    def __init__(
        self,
        client: StateAPIClient,
        thread_id: str,
        fence: dict[str, Any],
        *,
        namespace: str = NAMESPACE,
    ):
        super().__init__()
        self._client = client
        self._thread_id = thread_id
        self._fence = fence
        self._namespace = namespace

    # -- helpers ---------------------------------------------------------

    def _ns_of(self, config: Optional[RunnableConfig]) -> tuple[str, str]:
        """``(langgraph_ns, ahp_ns)`` for a runnable config.

        The two must never be conflated. LangGraph folds ``checkpoint_ns`` into
        every task id (``prepare_next_tasks``) and reads it as the *name of the
        current subgraph*, so every config and checkpoint tuple we hand back
        carries LangGraph's own namespace - empty for the root graph. The AHP
        namespace is a wire detail: this SDK's prefix, with a nested LangGraph
        namespace folded into it reversibly so a subgraph's checkpoints cannot
        collide with the root graph's.
        """
        cfg = (config or {}).get("configurable", {}) or {}
        lg = cfg.get("checkpoint_ns") or ""
        return lg, self._ahp_ns(lg)

    def _ahp_ns(self, langgraph_ns: str) -> str:
        if not langgraph_ns:
            return self._namespace
        return f"{self._namespace}{AHP_NS_SEP}{wire.encode_id(langgraph_ns)}"

    def _langgraph_ns(self, ahp_ns: str) -> str:
        """Inverse of :meth:`_ahp_ns`: what LangGraph must be given back."""
        if ahp_ns == self._namespace:
            return ""
        prefix = f"{self._namespace}{AHP_NS_SEP}"
        if ahp_ns.startswith(prefix):
            return wire.decode_id(ahp_ns[len(prefix):])
        return ahp_ns

    def _config_for(self, langgraph_ns: str, checkpoint_id: str) -> RunnableConfig:
        return {"configurable": {"thread_id": self._thread_id,
                                 "checkpoint_ns": langgraph_ns,
                                 "checkpoint_id": checkpoint_id}}

    # ------------------------------------------------------------------
    # async surface
    # ------------------------------------------------------------------

    async def aget_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        _, namespace = self._ns_of(config)
        cp_id = get_checkpoint_id(config) or ""
        try:
            header = await self._client.get_checkpoint(self._thread_id, namespace, cp_id)
        except StateAPIError as exc:
            if exc.code == "NOT_FOUND":
                return None
            raise
        if not header or not header.get("id"):
            return None
        return await self._build_tuple(namespace, header)

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        langgraph_ns, namespace = self._ns_of(config)
        cp_id = checkpoint["id"]
        body = {k: v for k, v in checkpoint.items() if k != "channel_values"}
        values = checkpoint.get("channel_values", {}) or {}

        envelope = wire.pack_envelope(self.serde, body,
                                      get_checkpoint_metadata(config, metadata),
                                      _parent_id(config))
        parts = []
        for channel, version in new_versions.items():
            if channel in values:
                parts.append({"channel": wire.encode_id(channel),
                              "version": str(version),
                              "data": wire.dump_value(self.serde, values[channel])})
            else:
                # LangGraph marks "channel exists, holds nothing" with ("empty", b"").
                parts.append({"channel": wire.encode_id(channel),
                              "version": str(version),
                              "data": wire.empty_blob()})

        payload = {
            "id": wire.encode_id(cp_id),
            "thread_id": self._thread_id,
            "namespace": namespace,
            "version": 1,
            "metadata": envelope,
            "parts": parts,
        }
        parent = _parent_id(config)
        if parent:
            payload["parent"] = wire.encode_id(parent)

        try:
            await self._client.put_checkpoint(self._fence, payload,
                                              "cp:" + wire.encode_id(cp_id))
        except StateAPIError as exc:
            if exc.code == "KEY_REUSE_MISMATCH" and await self._already_stored(namespace, payload):
                # The identical commit is durable (our own earlier write whose
                # response was lost): this is a replay, not a conflict.
                return self._config_for(langgraph_ns, cp_id)
            raise
        return self._config_for(langgraph_ns, cp_id)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        langgraph_ns, namespace = self._ns_of(config)
        cp_id = get_checkpoint_id(config)
        if not cp_id:
            raise ValueError("put_writes requires a checkpoint_id in config")

        rows = []
        for index, (channel, value) in enumerate(writes):
            rows.append({
                "task": wire.encode_id(task_id),
                "index": WRITES_IDX_MAP.get(channel, index),
                "channel": wire.encode_id(channel),
                "data": wire.dump_value(self.serde, value),
            })

        try:
            await self._client.put_writes(self._fence, namespace,
                                          wire.encode_id(cp_id), rows)
        except StateAPIError as exc:
            if exc.code == "KEY_REUSE_MISMATCH" and \
                    await self._pending_identical(namespace, cp_id, rows):
                return  # already durably recorded: a replay of our own write
            raise

    async def alist(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> AsyncIterator[CheckpointTuple]:
        if limit is not None and limit <= 0:
            return
        _, namespace = self._ns_of(config)
        requested_id = get_checkpoint_id(config) if config else None
        try:
            if requested_id:
                headers = [await self._client.get_checkpoint(
                    self._thread_id, namespace, wire.encode_id(requested_id))]
            else:
                headers = await self._client.list_checkpoints(
                    self._thread_id, namespace, limit=1000)
        except StateAPIError as exc:
            if exc.code == "NOT_FOUND":
                return
            raise
        before_id = get_checkpoint_id(before) if before else None
        emitted = 0
        # LangGraph orders public checkpoint IDs (UUIDv6), whereas owner rows
        # are returned in insertion order. Decode before comparing escaped IDs.
        for header in sorted(headers, key=lambda h: wire.decode_id(h["id"]), reverse=True):
            public_id = wire.decode_id(header["id"])
            if before_id is not None and public_id >= before_id:
                continue
            if filter:
                try:
                    _, metadata, _ = self._envelope(header)
                except WireError:
                    metadata = {}
                if not all(metadata.get(k) == v for k, v in filter.items()):
                    continue
            yield await self._build_tuple(namespace, header)
            emitted += 1
            if limit is not None and emitted >= limit:
                return

    async def adelete_thread(self, thread_id: str) -> None:
        await self._client.delete_thread(thread_id)

    # ------------------------------------------------------------------
    # sync surface (PDF §3/§4.2: the sync interface must be functional)
    # ------------------------------------------------------------------

    def get_tuple(self, config: RunnableConfig) -> Optional[CheckpointTuple]:
        return invoke_sync(self, lambda view: view.aget_tuple(config))

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return invoke_sync(self, lambda view: view.aput(config, checkpoint, metadata, new_versions))

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        invoke_sync(self, lambda view: view.aput_writes(config, writes, task_id, task_path))

    def list(
        self,
        config: Optional[RunnableConfig],
        *,
        filter: Optional[dict[str, Any]] = None,
        before: Optional[RunnableConfig] = None,
        limit: Optional[int] = None,
    ) -> Iterator[CheckpointTuple]:
        async def _collect(view):
            return [t async for t in view.alist(config, filter=filter,
                                                before=before, limit=limit)]
        return iter(invoke_sync(self, _collect))

    def delete_thread(self, thread_id: str) -> None:
        invoke_sync(self, lambda view: view.adelete_thread(thread_id))

    # ------------------------------------------------------------------
    # reassembly
    # ------------------------------------------------------------------

    def _envelope(self, header: dict) -> tuple[dict, dict, Optional[str]]:
        return wire.unpack_envelope(self.serde, header.get("metadata"))

    async def _build_tuple(self, namespace: str, header: dict) -> CheckpointTuple:
        body, metadata, parent_id = self._envelope(header)
        channel_values = await self._load_channels(namespace, header, body)
        checkpoint = dict(body)
        checkpoint["channel_values"] = channel_values

        public_id = header["id"]
        pending = await self._load_pending(namespace, public_id)
        return CheckpointTuple(
            config=self._config_for(self._langgraph_ns(namespace),
                                    wire.decode_id(public_id)),
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=(self._config_for(self._langgraph_ns(namespace), parent_id)
                           if parent_id else None),
            pending_writes=pending,
        )

    async def _load_channels(self, namespace: str, header: dict,
                             body: dict) -> dict[str, Any]:
        """Re-read every channel blob the checkpoint's versions point at.

        Reads never return bytes (the contract returns part *references*), and a
        version may have been written by an older checkpoint, so the lookup is by
        ``(channel, version)`` and not by "the parts of this checkpoint".
        """
        values: dict[str, Any] = {}
        inline = {p.get("channel"): p.get("data")
                  for p in header.get("parts", []) if p.get("data")}
        for channel, version in (body.get("channel_versions") or {}).items():
            encoded = wire.encode_id(channel)
            data = inline.get(encoded)
            if data is None:
                try:
                    part = await self._client.get_part(self._thread_id, namespace,
                                                       str(version), encoded)
                except StateAPIError as exc:
                    if exc.code == "NOT_FOUND":
                        continue
                    raise
                data = part.get("data") if isinstance(part, dict) else None
                if data is None:
                    continue
            if wire.is_empty_blob(data):
                continue
            try:
                values[channel] = wire.load_value(self.serde, data)
            except WireError as exc:
                raise WireError(f"channel {channel!r} at version {version!r}: {exc}") from exc
        return values

    async def _load_pending(self, namespace: str, cp_id: str) -> list[PendingWrite]:
        rows = await self._client.get_writes(self._thread_id, namespace, cp_id)
        pending: list[PendingWrite] = []
        for row in rows:
            data = row.get("data")
            if not data:
                continue
            value = wire.load_value(self.serde, data)
            pending.append((wire.decode_id(row.get("task", "")),
                            wire.decode_id(row.get("channel", "")),
                            value))
        return pending

    # ------------------------------------------------------------------
    # replay detection (durable-but-lost-response window)
    # ------------------------------------------------------------------

    async def _already_stored(self, namespace: str, payload: dict) -> bool:
        """True when the durable header holds *exactly* this request body.

        Only reached after the server answered KEY_REUSE_MISMATCH, i.e. after it
        refused a replay. "Identical" must mean the whole body - header envelope,
        part references **and** blob bytes - because two checkpoints sharing a
        public id can differ only inside a channel blob. Anything weaker would
        turn a genuine id-reuse conflict into a silent success.
        """
        try:
            header = await self._client.get_checkpoint(
                self._thread_id, namespace, payload["id"])
        except StateAPIError:
            return False
        if not header or not header.get("id"):
            return False
        if header.get("metadata") != payload.get("metadata"):
            return False
        if header.get("parent", "") != payload.get("parent", ""):
            return False

        stored_refs = sorted((p["channel"], str(p["version"]))
                             for p in header.get("parts", []))
        wanted_refs = sorted((p["channel"], str(p["version"]))
                             for p in payload.get("parts", []))
        if stored_refs != wanted_refs:
            return False

        for channel, version in wanted_refs:
            try:
                part = await self._client.get_part(self._thread_id, namespace,
                                                   version, channel)
            except StateAPIError:
                return False
            wanted = next(p["data"] for p in payload.get("parts", [])
                          if p["channel"] == channel and str(p["version"]) == version)
            if (part or {}).get("data") != wanted:
                return False
        return True

    async def _pending_identical(self, namespace: str, cp_id: str,
                                 rows: list[dict]) -> bool:
        """True when every rejected row is already stored byte-identically."""
        try:
            stored = await self._client.get_writes(self._thread_id, namespace,
                                                   wire.encode_id(cp_id))
        except StateAPIError:
            return False
        by_key = {(w.get("task"), w.get("index")): w.get("data") for w in stored}
        return all(by_key.get((row["task"], row["index"])) == row["data"] for row in rows)


def _parent_id(config: Optional[RunnableConfig]) -> Optional[str]:
    """The checkpoint this one was created from.

    LangGraph hands the parent in ``config["configurable"]["checkpoint_id"]`` on
    the write path; it is absent for the first checkpoint of a thread.
    """
    cfg = (config or {}).get("configurable", {}) or {}
    return cfg.get("checkpoint_id") or None
