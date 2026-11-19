"""Bounded shared-owner values: immutable chunks, atomically published manifest.

Chunks and manifest use the SAME reviewed subject/deployment namespace. Staging
is not publication. Include the returned KV put in CommitRecords to publish
state and native event together. Unpublished chunks are retained conservatively;
there is deliberately no age-based garbage collector.
"""
from __future__ import annotations
import base64
import copy
import hashlib
import json

from ahp_state.client import StateValidationError, StateConflictError

FORMAT = "ahp-shared-values/1"
CHUNK_FORMAT = "ahp-shared-chunk/1"
CHUNK_BYTES = 192 << 10
MAX_VALUE_BYTES = 16 << 20
MAX_TOTAL_BYTES = 32 << 20
MAX_KEYS = 256
_UNSET = object()


def _error(message):
    raise StateValidationError("INVALID_ARGUMENT", message)


def _bytes(value):
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    except (ValueError, TypeError, UnicodeError):
        _error("shared value must be finite UTF-8 JSON")


def _hash(raw):
    return hashlib.sha256(raw).hexdigest()


def _decode(value):
    try:
        return base64.b64decode(value, validate=True)
    except (ValueError, TypeError):
        _error("invalid shared base64")


class SharedValues:
    def __init__(self, client, namespace, owner, key, *, binding=()):
        if owner not in ("subject", "deployment") or not namespace or not key:
            _error("shared data requires an explicit subject/deployment namespace")
        self.client, self.namespace, self.owner, self.key = client, list(namespace), owner, key
        self.binding = list(binding)
        if len(_bytes(self.binding)) > 4096:
            _error('shared binding byte bound')

    def _key(self, key):
        return {"namespace": self.namespace, "key": key, "expected_owner": self.owner}

    async def _item(self):
        return (await self.client.get_kv_batch([self._key(self.key)]))["items"][0]

    def _manifest(self, item):
        if item is None:
            return {"format": FORMAT, "binding": self.binding, "values": []}
        m = item.get("value")
        if not isinstance(m, dict) or set(m) != {"format", "binding", "values"} or m["format"] != FORMAT or m["binding"] != self.binding:
            _error("incompatible shared manifest: explicit profile migration required")
        if not isinstance(m["values"], list) or len(m["values"]) > MAX_KEYS:
            _error("shared manifest key bound")
        seen, total = set(), 0
        for v in m["values"]:
            if not isinstance(v, dict) or set(v) != {"key", "version", "sha256", "size", "chunks"}:
                _error("invalid shared value descriptor")
            key = _decode(v["key"])
            if len(key) > 1024:
                _error("shared native key byte bound")
            try:
                key.decode()
            except UnicodeError:
                _error("invalid shared key")
            if key in seen or type(v["version"]) is not int or v["version"] < 1 or type(v["size"]) is not int or not 0 < v["size"] <= MAX_VALUE_BYTES:
                _error("invalid shared key/version/size")
            seen.add(key)
            if not isinstance(v["chunks"], list) or len(v["chunks"]) != (v["size"] + CHUNK_BYTES - 1) // CHUNK_BYTES:
                _error("invalid shared chunk count")
            for h in [v["sha256"], *v["chunks"]]:
                if not isinstance(h, str) or len(h) != 64 or any(c not in '0123456789abcdef' for c in h):
                    _error("invalid shared content hash")
            total += v["size"]
        if total > MAX_TOTAL_BYTES:
            _error("shared owner byte bound")
        if len(_bytes(m)) > 512 << 10:
            _error('shared manifest byte bound')
        return copy.deepcopy(m)

    def _chunk(self, item, digest):
        if item is None or not isinstance(item.get('value'), dict) or item['value'].get('format') != CHUNK_FORMAT:
            _error("missing shared immutable chunk")
        value = item["value"]
        if set(value) != {"format", "bytes_base64"}:
            _error("invalid shared chunk envelope")
        raw = _decode(value["bytes_base64"])
        if not 0 < len(raw) <= CHUNK_BYTES or _hash(raw) != digest:
            _error("shared chunk integrity mismatch")
        return raw

    async def read(self, item=_UNSET):
        if item is _UNSET:
            item = await self._item()
        manifest = self._manifest(item)
        hashes = sorted({h for v in manifest["values"] for h in v["chunks"]})
        chunks = {}
        for start in range(0, len(hashes), 8):
            group = hashes[start:start + 8]
            rows = (await self.client.get_kv_batch([self._key('blob-' + h) for h in group]))['items']
            for h, row in zip(group, rows, strict=True):
                chunks[h] = self._chunk(row, h)
        values = {}
        for v in manifest["values"]:
            raw = b''.join(chunks[h] for h in v["chunks"])
            if len(raw) != v["size"] or _hash(raw) != v["sha256"]:
                _error("shared value integrity mismatch")
            try:
                values[_decode(v['key']).decode()] = json.loads(raw, parse_constant=lambda _: _error('shared JSON contains nonfinite number'))
            except (ValueError, UnicodeError):
                _error("shared value is not JSON")
        return values, item

    async def prepare(self, fence, changes, *, current=_UNSET):
        if not isinstance(changes, dict) or any(not isinstance(k, str) for k in changes):
            _error("shared changes must be a string-keyed object")
        item = await self._item() if current is _UNSET else current
        manifest = self._manifest(item)
        entries = {v['key']: v for v in manifest['values']}
        chunks = {}
        for key, value in changes.items():
            raw = _bytes(value)
            if len(raw) > MAX_VALUE_BYTES:
                _error("shared value exceeds native byte bound")
            encoded_key = base64.b64encode(key.encode()).decode()
            hashes = []
            for start in range(0, len(raw), CHUNK_BYTES):
                chunk = raw[start:start + CHUNK_BYTES]
                h = _hash(chunk)
                chunks[h] = chunk
                hashes.append(h)
            entries[encoded_key] = {'key': encoded_key, 'version': entries.get(encoded_key, {}).get('version', 0) + 1,
                                    'sha256': _hash(raw), 'size': len(raw), 'chunks': hashes}
        manifest['values'] = [entries[k] for k in sorted(entries)]
        self._manifest({'value': manifest})  # Validate all bounds BEFORE staging.
        hashes = sorted(chunks)
        for start in range(0, len(hashes), 8):
            group = hashes[start:start + 8]
            rows = (await self.client.get_kv_batch([self._key('blob-' + h) for h in group]))['items']
            missing = []
            for h, row in zip(group, rows, strict=True):
                if row is not None:
                    self._chunk(row, h)
                else:
                    missing.append({**self._key('blob-' + h), 'expected_version': 0,
                                    'value': {'format': CHUNK_FORMAT, 'bytes_base64': base64.b64encode(chunks[h]).decode()}})
            if not missing:
                continue
            # Same body/key after an unknown outcome; no transparent retry.
            operation = 'stage-' + _hash(_bytes([fence['run_id'], missing]))
            try:
                await self.client.put_kv_batch(fence, operation, missing)
            except StateConflictError as exc:
                if exc.code != 'CONFLICT':
                    raise
                rows = (await self.client.get_kv_batch([self._key('blob-' + h) for h in group]))['items']
                for h, row in zip(group, rows, strict=True):
                    self._chunk(row, h)
        return {**self._key(self.key), 'expected_version': item['version'] if item else 0, 'value': manifest}
