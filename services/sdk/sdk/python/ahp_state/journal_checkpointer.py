"""Async and sync LangGraph checkpointing over the compact State journal."""
from __future__ import annotations

import base64
from hashlib import sha256
import json
from sys import getsizeof
from urllib.parse import quote

from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)

from ahp_state import wire
from ahp_state.checkpoint_saver import AHPCheckpointSaver, NAMESPACE
from ahp_state.client import StateAPIError, StateValidationError
from ahp_state.journal_client import AHPJournalClient
from ahp_state.journal_payload import decode_payload, encode_payload


_CHAT_CACHE_BYTES = 64 * 1024


class AHPJournalCheckpointSaver(AHPCheckpointSaver):
    """Persist complete typed snapshots and independent pending node writes."""

    def __init__(self, client, thread_id, fence, *, namespace=NAMESPACE, chat_channel=None):
        self._journal = fence if isinstance(fence, AHPJournalClient) else None
        if self._journal is not None and self._journal.session_id != thread_id:
            raise StateValidationError("INVALID_ARGUMENT", "journal lease belongs to another session")
        super().__init__(client, thread_id, fence, namespace=namespace)
        self._chat_channel = chat_channel
        # invoke_sync uses a shallow view with a fresh transport. Share only the
        # bounded projection slot so sequential sync writes retain the same hint.
        self._chat_slot = [None]
        if self._journal is not None:
            self._journal.on_lease_end(self.clear_chat_cache)

    def clear_chat_cache(self):
        self._chat_slot[0] = None

    def _epoch(self):
        return self._journal.epoch if self._journal is not None else self._fence["epoch"]

    def _lease_error(self, exc):
        self.clear_chat_cache()
        if self._journal is not None:
            self._journal._note_error(exc)

    async def _write(self, path, body):
        try:
            return await self._client.request_state("POST", path, body=body)
        except StateAPIError as exc:
            self._lease_error(exc)
            raise

    def _session_path(self):
        return "/v1/state/sessions/" + quote(self._thread_id, safe="")

    async def _encode(self, value):
        data = base64.b64decode(wire.dump_value(self.serde, value), validate=True)
        try:
            return await encode_payload(self._client, self._session_path(), self._epoch(), data)
        except StateAPIError as exc:
            self._lease_error(exc)
            raise

    async def _decode(self, payload):
        data = await decode_payload(self._client, self._session_path(), payload)
        return wire.load_value(self.serde, base64.b64encode(data).decode("ascii"))

    def _check_session(self, config):
        cfg = (config or {}).get("configurable", {})
        if cfg.get("thread_id") != self._thread_id:
            raise StateValidationError("INVALID_ARGUMENT", "a checkpoint saver owns exactly one session")

    async def _checkpoint_record(self, config):
        self._check_session(config)
        _, namespace = self._ns_of(config)
        params = {"checkpoint_ns": namespace}
        if cp_id := get_checkpoint_id(config):
            params["checkpoint_id"] = wire.encode_id(cp_id)
        try:
            return await self._client.request_state(
                "GET", self._session_path() + "/checkpoints", params=params)
        except StateAPIError as exc:
            if exc.code == "NOT_FOUND":
                return None
            raise

    async def aget_tuple(self, config):
        record = await self._checkpoint_record(config)
        if record is None:
            return None
        return await self._tuple(record)

    async def _tuple(self, record):
        checkpoint = record["checkpoint"]
        lg = self._langgraph_ns(checkpoint["namespace"])
        parent = checkpoint.get("parent_id")
        native = await self._decode(checkpoint["payload"])
        return CheckpointTuple(
            config=self._config_for(lg, wire.decode_id(checkpoint["id"])),
            checkpoint=native["checkpoint"],
            metadata=native["metadata"],
            parent_config=self._config_for(lg, wire.decode_id(parent)) if parent else None,
            pending_writes=[
                (wire.decode_id(row["task_id"]), wire.decode_id(row["channel"]),
                 await self._decode(row["payload"]))
                for row in record.get("writes") or ()
            ],
        )

    async def aput(self, config, checkpoint, metadata, new_versions):
        self._check_session(config)
        lg, namespace = self._ns_of(config)
        cp_id = checkpoint["id"]
        encoded_id = wire.encode_id(cp_id)
        epoch = self._epoch()
        native_metadata = get_checkpoint_metadata(config, metadata)
        doc = {
            "namespace": namespace, "id": encoded_id, "snapshot_seq": 0,
            "codec": "langgraph-jsonplus-v1", "metadata": _indexable_metadata(native_metadata),
            "channel_versions": checkpoint["channel_versions"],
            "payload": await self._encode({"checkpoint": checkpoint, "metadata": native_metadata}),
        }
        parent = get_checkpoint_id(config)
        if parent:
            doc["parent_id"] = wire.encode_id(parent)
        body = {"lease_epoch": epoch, "key": f"cp:{encoded_id}", "checkpoint": doc}
        slot = None
        cache_enabled = getattr(self._client, "has_fixed_scope", True)
        if self._chat_channel is not None and not lg:
            scope = (self._thread_id, namespace, epoch)
            previous = ()
            if parent:
                cached = self._chat_slot[0] if cache_enabled else None
                if cached is not None and cached[:2] == (scope, parent):
                    previous = cached[2]
                else:
                    self.clear_chat_cache()
                    record = await self._checkpoint_record(config)
                    if record is not None and record["checkpoint"].get("chat_event_id"):
                        snapshot = await self._decode(record["checkpoint"]["payload"])
                        previous = _chat_fingerprints(self._chat_messages(snapshot["checkpoint"]))
            messages = self._chat_messages(checkpoint)
            current = _chat_fingerprints(messages)
            body["chat"] = _chat_delta(previous, current, messages)
            if cache_enabled:
                candidate = (scope, cp_id, current)
                if _projection_size(candidate) <= _CHAT_CACHE_BYTES:
                    slot = candidate
        path = self._session_path() + "/checkpoints"
        try:
            await self._write(path, body)
        except StateAPIError as exc:
            # A lost ACK across an SDK upgrade can replay a row whose metadata
            # was stored only inside the typed payload. The server still checks
            # every other byte against the original operation.
            if exc.code != "KEY_REUSE_MISMATCH" or not doc["metadata"]:
                raise
            await self._write(path, {**body, "checkpoint": {**doc, "metadata": {}}})
        if self._chat_channel is not None and not lg:
            try:
                if epoch == self._epoch():
                    self._chat_slot[0] = slot
            except StateAPIError:
                self.clear_chat_cache()
        return self._config_for(lg, cp_id)

    def _chat_messages(self, checkpoint):
        messages = checkpoint.get("channel_values", {}).get(self._chat_channel, [])
        if not isinstance(messages, list):
            raise StateValidationError("INVALID_ARGUMENT", "chat channel requires a materialized message list")
        roles = {"human": "user", "ai": "assistant"}
        result = []
        for message in messages:
            if not getattr(message, "id", None) or message.type == "remove":
                raise StateValidationError("INVALID_ARGUMENT", "chat channel requires materialized messages with IDs")
            role = getattr(message, "role", None) or roles.get(message.type, message.type)
            result.append({"id": message.id, "role": role, "content": message.content})
        return list({message["id"]: message for message in result}.values())

    async def aput_writes(self, config, writes, task_id, task_path=""):
        self._check_session(config)
        _, namespace = self._ns_of(config)
        cp_id = get_checkpoint_id(config)
        if not cp_id:
            raise StateValidationError("INVALID_ARGUMENT", "pending writes require a checkpoint ID")
        rows = [
            {"task_id": wire.encode_id(task_id), "task_path": task_path,
             "index": WRITES_IDX_MAP.get(channel, index), "channel": wire.encode_id(channel),
             "codec": "langgraph-jsonplus-v1", "payload": await self._encode(value)}
            for index, (channel, value) in enumerate(writes)
        ]
        path = f"{self._session_path()}/checkpoints/{quote(wire.encode_id(cp_id), safe='')}/writes"
        await self._write(path, {
            "lease_epoch": self._epoch(), "namespace": namespace, "writes": rows,
        })

    async def alist(self, config, *, filter=None, before=None, limit=None):
        self._check_session(config)
        _, namespace = self._ns_of(config)
        cursor = get_checkpoint_id(before) if before else None
        remaining = limit
        journal = AHPJournalClient(self._client, self._thread_id, harness_type="langgraph")
        metadata = _indexable_metadata(filter or {})
        while remaining is None or remaining > 0:
            size = min(remaining, 100) if remaining is not None else 100
            try:
                page = await journal.list_checkpoints(
                    namespace=namespace, before=wire.encode_id(cursor) if cursor else "",
                    limit=size, include_tuples=True, metadata=metadata or None,
                    include_unindexed=bool(metadata),
                )
            except StateAPIError as exc:
                if exc.code == "NOT_FOUND":
                    return
                raise
            records = page["checkpoint_tuples"] or []
            for record in records:
                cursor = wire.decode_id(record["checkpoint"]["id"])
                item = await self._tuple(record)
                if any(item.metadata.get(key) != value for key, value in (filter or {}).items()):
                    continue
                yield item
                if remaining is not None:
                    remaining -= 1
                    if remaining == 0:
                        return
            if len(records) < size:
                return

    async def adelete_thread(self, thread_id):
        self._check_session({"configurable": {"thread_id": thread_id}})
        try:
            await self._client.request_state("DELETE", self._session_path())
        finally:
            self.clear_chat_cache()


def _indexable_metadata(metadata):
    indexed = {}
    for key, value in metadata.items():
        try:
            encoded = json.dumps(value, ensure_ascii=True, allow_nan=False)
            # JSONB rejects embedded NUL. Keep such values in the native
            # payload and apply their filter in Python.
            if "\x00" not in key and "\\u0000" not in encoded:
                indexed[key] = json.loads(encoded)
        except (TypeError, ValueError):
            continue
    return indexed


def _chat_fingerprints(messages):
    return tuple((message["id"], sha256(json.dumps(
        [message["role"], message["content"]], ensure_ascii=True, sort_keys=True, allow_nan=False,
        separators=(",", ":"),
    ).encode()).digest()) for message in messages)


def _projection_size(value):
    # Count Python objects retained by the single slot, including container
    # and string overhead, not just the logical wire bytes.
    return getsizeof(value) + (sum(_projection_size(item) for item in value) if isinstance(value, tuple) else 0)


def _chat_delta(previous, current, messages):
    before = dict(previous)
    after = dict(current)
    delta = {
        "remove": [key for key in before if key not in after],
        "upserts": [message for message in messages if before.get(message["id"]) != after[message["id"]]],
    }
    natural = [key for key in before if key in after] + [key for key in after if key not in before]
    if natural != list(after):
        delta["order"] = list(after)
    return delta
