"""Framework-neutral, versioned common State methods; no native interpretation.

Methods send one request. For an unknown outcome the host repeats the identical
key and body; the SDK never generates a new operation or retries implicitly.
"""
from __future__ import annotations
import json


def _invalid(message):
    from ahp_state.client import StateValidationError
    raise StateValidationError("INVALID_ARGUMENT", message)


def _revision(value):
    if type(value) is not int or value < 0:
        _invalid("revision must be a nonnegative integer")


class CommonStateMethods:
    async def commit_records(self, fence: dict, namespace: str, key: str,
                             expected_revision: int, *, document=None, delta=None,
                             kv=None, artifact_references=None, ack_inbox=None,
                             stream=None, run_status=None) -> dict:
        from ahp_state.client import _fence
        _revision(expected_revision)
        if not key or (document is None) == (delta is None):
            _invalid("publication requires key and exactly one document or delta")
        body = {"fence": _fence(fence), "namespace": namespace, "key": key,
                "expected_revision": expected_revision}
        body["delta" if delta is not None else "document"] = delta if delta is not None else document
        for name, value in (("kv", kv), ("artifact_references", artifact_references),
                            ("ack_inbox", ack_inbox), ("stream", stream), ("run_status", run_status)):
            if value is not None:
                body[name] = value
        try:
            size = len(json.dumps(body, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode())
        except (ValueError, TypeError):
            _invalid("common publication is not valid finite JSON")
        if size > 9 << 20:
            _invalid("common publication exceeds request limit")
        return await self.call("CommitRecords", body)

    async def read_records(self, thread_id: str, namespace: str, revision: int = 0) -> dict:
        _revision(revision)
        if not thread_id:
            _invalid("thread is required")
        return await self.call("ReadRecords", {"thread_id": thread_id, "namespace": namespace, "revision": revision})

    async def read_records_bundle(self, thread_id: str, namespace: str, keys: list[dict], known_revision: int = 0) -> dict:
        _revision(known_revision)
        if not thread_id or not 1 <= len(keys) <= 8:
            _invalid("thread and 1..8 owner keys required")
        return await self.call("ReadRecordsBundle", {"thread_id": thread_id, "namespace": namespace,
                                                     "keys": keys, "known_revision": known_revision})

    async def get_kv_batch(self, keys: list[dict]) -> dict:
        if not 1 <= len(keys) <= 8:
            _invalid("KV batch requires 1..8 keys")
        return await self.call("GetKVBatch", {"keys": keys})

    async def put_kv_batch(self, fence: dict, operation_key: str, items: list[dict]) -> dict:
        from ahp_state.client import _fence
        if not operation_key or not 1 <= len(items) <= 8:
            _invalid("KV batch requires operation key and 1..8 items")
        return await self.call("PutKVBatch", {"fence": _fence(fence), "batch": {"operation_key": operation_key, "items": items}})

    async def put_common_root(self, fence: dict, namespace: str, key: str,
                              operation_key: str, expected_version: int, revision=None) -> dict:
        from ahp_state.client import _fence
        _revision(expected_version)
        if revision is not None:
            _revision(revision)
            if revision == 0:
                _invalid("root revision must be positive")
        if not key or not operation_key:
            _invalid("root and operation keys required")
        return await self.call("PutCommonRoot", {"fence": _fence(fence), "root": {
            "namespace": namespace, "key": key, "operation_key": operation_key,
            "expected_version": expected_version, "revision": revision}})
