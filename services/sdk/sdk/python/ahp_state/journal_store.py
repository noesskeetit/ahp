"""LangGraph BaseStore backed by the compact PostgreSQL State API."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from itertools import groupby
import json
from urllib.parse import quote
from uuid import uuid4

from langgraph.store.base import BaseStore, GetOp, Item, ListNamespacesOp, PutOp, SearchItem, SearchOp

from ahp_state import wire
from ahp_state.client import StateAPIError, StateValidationError
from ahp_state.journal_payload import decode_payload, encode_payload
from ahp_state.sync import invoke_sync

_PREFIX = "langs:"
_API = "/v1/state"
_PAGE_SIZE = 128
_MAX_MUTATIONS = 256
_MAX_INLINE_BYTES = 2 << 20
_MAX_BODY_BYTES = (3 << 20) - 2048


def _namespace(parts):
    return _PREFIX + "".join(part.encode("utf-8").hex() + "/" for part in parts)


def _native_namespace(value):
    return tuple(bytes.fromhex(part).decode("utf-8") for part in value[len(_PREFIX):].split("/")[:-1])


def _matches(namespace, condition):
    size = len(condition.path)
    if len(namespace) < size:
        return False
    actual = namespace[-size:] if size and condition.match_type == "suffix" else namespace[:size]
    return all(expected == "*" or expected == found for expected, found in zip(condition.path, actual))


class AHPJournalStore(BaseStore):
    """Isolated per user and deployment, with versioned mutations and prefix reads."""

    def __init__(self, client, session_id, lease_epoch):
        self._client = client
        self._session_id = session_id
        self._epoch = lease_epoch
        self._session = _API + "/sessions/" + quote(session_id, safe="")

    def batch(self, ops):
        return invoke_sync(self, lambda view: view.abatch(list(ops)))

    async def abatch(self, ops):
        results = []
        for kind, group in groupby(ops, type):
            if issubclass(kind, PutOp):
                puts = list(group)
                await self._put_many(puts)
                results.extend([None] * len(puts))
                continue
            for op in group:
                if isinstance(op, GetOp):
                    row = await self._row(op.namespace, op.key)
                    result = None if row is None or row.get("deleted") else await self._item(row)
                elif isinstance(op, SearchOp):
                    result = await self._search(op)
                elif isinstance(op, ListNamespacesOp):
                    result = await self._list_namespaces(op)
                else:
                    raise TypeError(f"unsupported LangGraph store operation {type(op).__name__}")
                results.append(result)
        return results

    async def _list(self, parts, *, prefix=False, offset=0, keys=(), limit=_PAGE_SIZE):
        params = {"scope_type": "store", "namespace": _namespace(parts),
                  "prefix": str(prefix).lower(), "offset": offset, "limit": limit}
        if keys:
            params["key"] = [wire.encode_id(key) for key in keys]
        return await self._client.request_state("GET", _API + "/kv", params=params) or []

    async def _row(self, parts, key):
        return next(iter(await self._list(parts, keys=(key,), limit=1)), None)

    async def _rows(self, parts):
        for offset in range(0, 10001, _PAGE_SIZE):
            page = await self._list(parts, prefix=True, offset=offset)
            for row in page:
                if not row.get("deleted"):
                    yield row
            if len(page) < _PAGE_SIZE:
                return
        raise StateValidationError("INVALID_ARGUMENT", "scan exceeds the compact store pagination limit")

    async def _item(self, row, cls=Item):
        value = json.loads(await decode_payload(self._client, self._session, row["payload"]))
        timestamp = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
        return cls(value=value, key=wire.decode_id(row["key"]), namespace=_native_namespace(row["namespace"]),
                   created_at=timestamp, updated_at=timestamp)

    async def _versions(self, ops):
        keys = defaultdict(set)
        for op in ops:
            if getattr(op, "ttl", None) is not None or isinstance(op.index, list):
                raise StateValidationError("INVALID_ARGUMENT", "this store has no TTL or vector index configured")
            keys[op.namespace].add(op.key)
        rows = {}
        for namespace, wanted in keys.items():
            if len(wanted) == 1:
                key = next(iter(wanted))
                row = await self._row(namespace, key)
                if row is not None:
                    rows[namespace, key] = (row["version"], bool(row.get("deleted")))
                continue
            ordered = sorted(wanted)
            for start in range(0, len(ordered), _MAX_MUTATIONS):
                batch = ordered[start:start + _MAX_MUTATIONS]
                for row in await self._list(namespace, keys=batch, limit=len(batch)):
                    key = wire.decode_id(row["key"])
                    rows[namespace, key] = (row["version"], bool(row.get("deleted")))
        return rows

    async def _put_many(self, ops):
        versions = await self._versions(ops)
        changes = []
        inline_size = body_size = 0
        for op in ops:
            identity = (op.namespace, op.key)
            version, deleted = versions.get(identity, (0, True))
            if op.value is None and deleted:
                continue
            change = {"scope_type": "store", "namespace": _namespace(op.namespace),
                      "key": wire.encode_id(op.key), "expected_version": version}
            data_size = 0
            if op.value is None:
                change["delete"] = True
            else:
                data = json.dumps(op.value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
                change["payload"] = await encode_payload(
                    self._client, self._session, self._epoch, data, allow_spill=False)
                data_size = len(data)
            change_size = len(json.dumps(change).encode()) + 1
            if (len(changes) == _MAX_MUTATIONS or inline_size + data_size > _MAX_INLINE_BYTES
                    or body_size + change_size > _MAX_BODY_BYTES):
                await self._mutate(changes)
                changes = []
                inline_size = body_size = 0
            changes.append(change)
            inline_size += data_size
            body_size += change_size
            versions[identity] = (version + 1, op.value is None)
        if changes:
            await self._mutate(changes)

    async def _mutate(self, changes):
        request = {"session_id": self._session_id, "lease_epoch": self._epoch,
                   "key": "langstore:" + uuid4().hex, "kv": changes}
        try:
            await self._client.request_state("POST", _API + "/kv:mutate", body=request)
        except StateAPIError as error:
            if error.code not in {"TRANSPORT", "TIMEOUT"}:
                raise
            # Retry the exact request, including its original compare-and-set versions.
            await self._client.request_state("POST", _API + "/kv:mutate", body=request)

    async def _search(self, op):
        if getattr(op, "query", None) is not None:
            raise StateValidationError("INVALID_ARGUMENT", "this store has no vector index configured")
        answer = []
        skipped = 0
        async for row in self._rows(op.namespace_prefix):
            item = await self._item(row, SearchItem)
            if any(key not in item.value or item.value[key] != value for key, value in (op.filter or {}).items()):
                continue
            if skipped < op.offset:
                skipped += 1
            elif len(answer) < op.limit:
                answer.append(item)
            else:
                break
        return answer

    async def _list_namespaces(self, op):
        conditions = op.match_conditions or ()
        prefix = next((c.path for c in conditions if c.match_type == "prefix" and "*" not in c.path), ())
        answer = set()
        async for row in self._rows(prefix):
            namespace = _native_namespace(row["namespace"])
            if all(_matches(namespace, condition) for condition in conditions):
                answer.add(namespace[:op.max_depth] if op.max_depth is not None else namespace)
        return sorted(answer)[op.offset:op.offset + op.limit]
