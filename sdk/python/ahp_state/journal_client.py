"""Compact State API for custom Python agents."""

from __future__ import annotations

from urllib.parse import quote

from ahp_state.client import StateAPIError, StateValidationError
from ahp_state.journal_payload import decode_payload, encode_payload
from ahp_state.journal_tools import SessionFencedError, StandaloneToolGuard


class AHPJournalClient:
    """Obtain a lease and use it with one running agent instance."""

    def __init__(self, client, session_id, *, harness_type, format_version=1):
        self._client = client
        self.session_id = session_id
        self.harness_type = harness_type
        self.format_version = format_version
        self.path = "/v1/state/sessions/" + quote(session_id, safe="")
        self._lease = None
        self.fenced = False

    @property
    def epoch(self):
        if self.fenced:
            raise SessionFencedError("STALE_EPOCH", "this agent lost its write lease")
        if self._lease is None:
            raise StateValidationError("INVALID_ARGUMENT", "open the journal with a write lease first")
        return self._lease["epoch"]

    async def _mutate(self, suffix, body):
        try:
            return await self._client.request_state(
                "POST", self.path + suffix, body={**body, "lease_epoch": self.epoch},
            )
        except StateAPIError as error:
            if error.code == "STALE_EPOCH":
                self.fenced = True
                raise SessionFencedError(error.code, error.message) from error
            raise

    async def acquire(self, ttl_ms=30000):
        if self._lease is not None or self.fenced:
            raise StateValidationError("INVALID_ARGUMENT", "create a new agent instance for each lease")
        self._lease = await self._client.request_state("POST", "/v1/state/sessions:acquire", body={
            "session_id": self.session_id, "harness_type": self.harness_type,
            "format_version": self.format_version, "ttl_ms": ttl_ms,
        })
        return self._lease.copy()

    async def inspect(self):
        return await self._client.request_state("GET", self.path, params={"view": "resume"})

    async def heartbeat(self, ttl_ms=30000):
        self._lease = await self._mutate(":heartbeat", {"ttl_ms": ttl_ms})
        return self._lease.copy()

    async def release(self, *, status="IDLE"):
        await self._mutate(":heartbeat", {"release": True, "status": status})
        self._lease = None

    async def append(self, operation_key, expected_seq, events, **changes):
        return await self._mutate("/events:append", {
            **changes, "key": operation_key, "expected_seq": expected_seq, "events": events,
        })

    async def events(self, *, after_seq=0, limit=100, **filters):
        return await self._client.request_state("GET", self.path + "/events", params={
            **filters, "after_seq": after_seq, "limit": limit,
        })

    async def put_checkpoint(self, operation_key, checkpoint, **changes):
        return await self._mutate("/checkpoints", {**changes, "key": operation_key, "checkpoint": checkpoint})

    async def checkpoint(self, *, namespace="", checkpoint_id=""):
        return await self._client.request_state(
            "GET", self.path + "/checkpoints", params={"checkpoint_ns": namespace, "checkpoint_id": checkpoint_id},
        )

    async def encode(self, data):
        return await encode_payload(self._client, self.path, self.epoch, data)

    async def decode(self, payload):
        return await decode_payload(self._client, self.path, payload)

    def standalone_tools(self):
        return StandaloneToolGuard(self._client, self.path, self.epoch)
