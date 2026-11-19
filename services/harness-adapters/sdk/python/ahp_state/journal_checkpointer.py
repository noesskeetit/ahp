"""Async and sync LangGraph checkpointing over the compact State journal."""
from __future__ import annotations

import base64
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
from ahp_state.journal_payload import decode_payload, encode_payload


class AHPJournalCheckpointSaver(AHPCheckpointSaver):
    """Persist complete typed snapshots and independent pending node writes."""

    def __init__(self, client, thread_id, fence, *, namespace=NAMESPACE, chat_channel=None):
        super().__init__(client, thread_id, fence, namespace=namespace)
        self._chat_channel = chat_channel

    def _session_path(self):
        return "/v1/state/sessions/" + quote(self._thread_id, safe="")

    async def _encode(self, value):
        data = base64.b64decode(wire.dump_value(self.serde, value), validate=True)
        return await encode_payload(self._client, self._session_path(), self._fence["epoch"], data)

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
                for row in record["writes"]
            ],
        )

    async def aput(self, config, checkpoint, metadata, new_versions):
        self._check_session(config)
        lg, namespace = self._ns_of(config)
        cp_id = checkpoint["id"]
        encoded_id = wire.encode_id(cp_id)
        doc = {
            "namespace": namespace, "id": encoded_id, "snapshot_seq": 0,
            "codec": "langgraph-jsonplus-v1", "metadata": {},
            "channel_versions": checkpoint["channel_versions"],
            "payload": await self._encode({
                "checkpoint": checkpoint,
                "metadata": get_checkpoint_metadata(config, metadata),
            }),
        }
        if parent := get_checkpoint_id(config):
            doc["parent_id"] = wire.encode_id(parent)
        body = {
            "lease_epoch": self._fence["epoch"], "key": f"cp:{encoded_id}",
            "checkpoint": doc,
        }
        if self._chat_channel is not None and not lg:
            previous = []
            if parent:
                record = await self._checkpoint_record(config)
                if record is not None and record["checkpoint"].get("chat_event_id"):
                    snapshot = await self._decode(record["checkpoint"]["payload"])
                    previous = self._chat_messages(snapshot["checkpoint"])
            body["chat"] = _chat_delta(previous, self._chat_messages(checkpoint))
        await self._client.request_state("POST", self._session_path() + "/checkpoints", body=body)
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
        return result

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
        await self._client.request_state("POST", path, body={
            "lease_epoch": self._fence["epoch"], "namespace": namespace, "writes": rows,
        })

    async def alist(self, config, *, filter=None, before=None, limit=None):
        self._check_session(config)
        lg, namespace = self._ns_of(config)
        cursor = get_checkpoint_id(before) if before else None
        remaining = limit
        while remaining is None or remaining > 0:
            params = {"checkpoint_ns": namespace, "list": "true",
                      "limit": min(remaining, 100) if remaining is not None else 100}
            if cursor:
                params["before"] = wire.encode_id(cursor)
            ids = (await self._client.request_state(
                "GET", self._session_path() + "/checkpoints", params=params))["checkpoint_ids"] or []
            if not ids:
                return
            for checkpoint_id in ids:
                cursor = wire.decode_id(checkpoint_id)
                item = await self.aget_tuple(self._config_for(lg, cursor))
                if item is None or any(item.metadata.get(k) != v for k, v in (filter or {}).items()):
                    continue
                yield item
                if remaining is not None:
                    remaining -= 1
                    if remaining == 0:
                        return

    async def adelete_thread(self, thread_id):
        self._check_session({"configurable": {"thread_id": thread_id}})
        await self._client.request_state("DELETE", self._session_path())


def _chat_delta(previous, current):
    before = {message["id"]: message for message in previous}
    after = {message["id"]: message for message in current}
    delta = {
        "remove": [key for key in before if key not in after],
        "upserts": [message for key, message in after.items() if before.get(key) != message],
    }
    natural = [key for key in before if key in after] + [key for key in after if key not in before]
    if natural != list(after):
        delta["order"] = list(after)
    return delta
