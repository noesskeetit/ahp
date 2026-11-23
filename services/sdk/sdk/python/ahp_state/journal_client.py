"""Compact State API for custom Python agents."""

from __future__ import annotations

import asyncio
import base64
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from functools import wraps
import json
from urllib.parse import quote
from weakref import WeakMethod

from ahp_state.client import StateAPIError, StateValidationError
from ahp_state.journal_payload import decode_payload, encode_payload
from ahp_state.journal_tools import SessionFencedError, StandaloneToolGuard
from ahp_state.sync import invoke_sync

_API = "/v1/state"


def _receipt_for_turn(receipt, turn_seq):
    control = (receipt or {}).get("control")
    if control is None or control.get("turn_seq", turn_seq) == turn_seq:
        return receipt
    return {key: value for key, value in receipt.items() if key != "control"}


@dataclass
class _LeaseState:
    value: dict | None = None
    turn_seq: int | None = None
    guard: StandaloneToolGuard | None = None
    fenced: bool = False
    on_end: set = field(default_factory=set)
    heartbeat_task: asyncio.Task | None = None


class AHPJournalClient:
    """Compact State operations for one session and its current write lease."""

    def __init__(self, client, session_id, *, harness_type, format_version=1):
        self._client = client
        self.session_id = session_id
        self.harness_type = harness_type
        self.format_version = format_version
        self.path = _API + "/sessions/" + quote(session_id, safe="")
        self._lease = _LeaseState()

    @property
    def fenced(self):
        return self._lease.fenced or (self._lease.guard is not None and self._lease.guard.fenced)

    @property
    def epoch(self):
        if self.fenced:
            raise SessionFencedError("STALE_EPOCH", "this agent lost its write lease")
        if self._lease.value is None:
            raise StateValidationError("INVALID_ARGUMENT", "open the journal with a write lease first")
        return self._lease.value["epoch"]

    @property
    def turn_seq(self):
        """The current VM-managed turn, or None for an unbound or pull lease."""
        return self._lease.turn_seq

    def _check_can_acquire(self):
        if self._lease.value is not None or self.fenced:
            raise StateValidationError("INVALID_ARGUMENT", "this client already has or has lost its write lease")

    def _note_error(self, error):
        if error.code == "STALE_EPOCH":
            self._lose_lease()
            raise SessionFencedError(error.code, error.message) from error

    def on_lease_end(self, callback):
        """Register a bound method that discards local lease-scoped state."""
        self._lease.on_end = {ref for ref in self._lease.on_end if ref() is not None}
        self._lease.on_end.add(WeakMethod(callback))

    def _lose_lease(self):
        self._lease.fenced = True
        self._clear_lease()

    def _clear_lease(self):
        renewal = self._lease.heartbeat_task
        self._lease.heartbeat_task = None
        if renewal is not None and renewal is not asyncio.current_task():
            renewal.cancel()
        if self._lease.guard is not None:
            self._lease.guard.fenced = True
        self._lease.guard = self._lease.value = None
        self._lease.turn_seq = None
        for ref in self._lease.on_end:
            if (callback := ref()) is not None:
                callback()

    def _set_lease(self, value):
        self._lease.value = value
        return value.copy()

    async def _mutate(self, suffix, body):
        try:
            return await self._client.request_state(
                "POST", self.path + suffix, body={**body, "lease_epoch": self.epoch},
            )
        except StateAPIError as error:
            self._note_error(error)
            raise

    async def acquire(self, ttl_ms=30000):
        self._check_can_acquire()
        return self._set_lease(await self._client.request_state("POST", _API + "/sessions:acquire", body={
            "session_id": self.session_id, "harness_type": self.harness_type,
            "format_version": self.format_version, "ttl_ms": ttl_ms,
        }))

    async def enqueue_turn(self, operation_key, input_payload, *, execution_mode=None,
                           delivery_mode=None, target_turn_seq=None):
        body = {
            "session_id": self.session_id, "harness_type": self.harness_type,
            "format_version": self.format_version, "key": operation_key, "input": input_payload,
        }
        if execution_mode is not None:
            body["execution_mode"] = execution_mode
        if delivery_mode is not None:
            body["delivery_mode"] = delivery_mode
        if target_turn_seq is not None:
            body["target_turn_seq"] = target_turn_seq
        return await self._client.request_state("POST", _API + "/sessions:enqueue", body=body)

    async def claim_turn(self, ttl_ms=30000):
        self._check_can_acquire()
        return self._set_lease(await self._client.request_state("POST", _API + "/sessions:claim-turn", body={
            "session_id": self.session_id, "ttl_ms": ttl_ms,
        }))

    async def claim_vm_turn(self, *, turn_seq, generation, execution_id, ttl_ms=30000):
        self._check_can_acquire()
        value = await self._client.request_state("POST", self.path + "/vm:claim", body={
            "turn_seq": turn_seq, "generation": generation, "execution_id": execution_id, "ttl_ms": ttl_ms,
        })
        self._lease.turn_seq = turn_seq
        return self._set_lease(value)

    async def cancel_turn(self, *, turn_seq):
        return await self._client.request_state("POST", self.path + "/vm:cancel", body={"turn_seq": turn_seq})

    async def turn_status(self):
        return await self._client.request_state("GET", self.path)

    async def inspect(self):
        return await self._client.request_state("GET", self.path, params={"view": "resume"})

    async def list_sessions(self, *, view=None, harness=None, limit=100, **filters):
        params = {"harness": harness if harness is not None else self.harness_type, "limit": limit, **filters}
        if view is not None:
            params["view"] = view
        return await self._client.request_state("GET", _API + "/sessions", params=params)

    async def delete_session(self):
        result = await self._client.request_state("DELETE", self.path)
        self._clear_lease()
        return result

    async def set_legal_hold(self, held):
        return await self._client.request_state("PATCH", self.path + "/legal-hold", body={"held": held})

    async def heartbeat(self, ttl_ms=30000):
        current = self._lease.value
        updated = await self._mutate(":heartbeat", {"ttl_ms": ttl_ms})
        if self._lease.value is not current or self.fenced:
            raise SessionFencedError("STALE_EPOCH", "lease changed while heartbeat was in flight")
        return self._set_lease(_receipt_for_turn(updated, self.turn_seq))

    async def release(self, *, status="IDLE"):
        await self._mutate(":heartbeat", {"release": True, "status": status})
        self._clear_lease()

    @asynccontextmanager
    async def lease(self, ttl_ms=30000, *, heartbeat_interval=None, release_status="IDLE"):
        """Maintain a write lease while the calling task executes.

        A failed renewal fences the wrapper and cancels the calling task. The
        heartbeat error is raised at the context boundary so the host can stop.
        """
        interval = ttl_ms / 3000 if heartbeat_interval is None else heartbeat_interval
        if not 0 < interval < ttl_ms / 1000:
            raise ValueError("heartbeat interval must be positive and shorter than the lease TTL")
        epoch = (await self.acquire(ttl_ms))["epoch"]
        owner = asyncio.current_task()
        failure = None

        async def renew():
            nonlocal failure
            try:
                while self._lease.heartbeat_task is asyncio.current_task():
                    await asyncio.sleep(interval)
                    if self._lease.heartbeat_task is asyncio.current_task():
                        await self.heartbeat(ttl_ms)
            except Exception as error:
                failure = error
                self._lose_lease()
                owner.cancel()

        task = asyncio.create_task(renew())
        self._lease.heartbeat_task = task
        try:
            yield self
            if failure is not None:
                raise failure
        except asyncio.CancelledError:
            if failure is not None:
                owner.uncancel()
                raise failure from None
            raise
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            if self._lease.heartbeat_task is task:
                self._lease.heartbeat_task = None
                if self._lease.value is not None and not self.fenced and self.epoch == epoch:
                    await self.release(status=release_status)

    def _turn_fields(self, turn_seq):
        turn_seq = self.turn_seq if turn_seq is None else turn_seq
        return {"turn_seq": turn_seq} if turn_seq is not None else {}

    async def append(self, operation_key, expected_seq, events, *, turn_seq=None, steer_ack=None, **changes):
        """Commit events and, if supplied, ACK the steer included in that commit."""
        fields = self._turn_fields(turn_seq)
        epoch = self.epoch
        if steer_ack is not None:
            fields["steer_ack"] = steer_ack
        result = await self._mutate("/events:append", {
            **changes, **fields, "key": operation_key, "expected_seq": expected_seq, "events": events,
        })
        receipt_turn = self.turn_seq
        if (changes.get("finish_turn") and self._lease.value is not None
                and self._lease.value["epoch"] == epoch and receipt_turn == fields.get("turn_seq")):
            control = result.get("control") or {}
            next_turn = control.get("next_turn")
            if next_turn is None:
                self._clear_lease()
            else:
                receipt_turn = next_turn["input_seq"]
                if control.get("turn_seq", receipt_turn) != receipt_turn:
                    raise StateValidationError("INVALID_ARGUMENT", "followup receipt identifies conflicting turns")
                if self._lease.guard is not None:
                    self._lease.guard.fenced = True
                    self._lease.guard = None
                self._lease.turn_seq = receipt_turn
        return _receipt_for_turn(result, receipt_turn)

    async def events(self, *, after_seq=0, limit=100, **filters):
        return await self._client.request_state("GET", self.path + "/events", params={
            **filters, "after_seq": after_seq, "limit": limit,
        })

    async def put_checkpoint(self, operation_key, checkpoint, *, turn_seq=None, steer_ack=None, **changes):
        """Commit a snapshot and, if supplied, ACK the steer included in it."""
        body = {**changes, **self._turn_fields(turn_seq), "key": operation_key, "checkpoint": checkpoint}
        if steer_ack is not None:
            body["steer_ack"] = steer_ack
        result = await self._mutate("/checkpoints", body)
        return _receipt_for_turn(result, self.turn_seq)

    async def checkpoint(self, *, namespace="", checkpoint_id=""):
        return await self._client.request_state(
            "GET", self.path + "/checkpoints", params={"checkpoint_ns": namespace, "checkpoint_id": checkpoint_id},
        )

    async def list_checkpoints(self, *, namespace="", before="", limit=100,
                               include_tuples=False, metadata=None, include_unindexed=False):
        params = {"checkpoint_ns": namespace, "list": "true", "before": before, "limit": limit}
        if include_tuples:
            params["include_tuples"] = "true"
        if metadata is not None:
            params["metadata"] = json.dumps(metadata, ensure_ascii=True, allow_nan=False, separators=(",", ":"))
        if include_unindexed:
            params["include_unindexed"] = "true"
        return await self._client.request_state("GET", self.path + "/checkpoints", params=params)

    async def put_writes(self, checkpoint_id, writes, *, namespace="", turn_seq=None):
        suffix = f"/checkpoints/{quote(checkpoint_id, safe='')}/writes"
        return await self._mutate(suffix, {**self._turn_fields(turn_seq), "namespace": namespace, "writes": writes})

    async def get_kv(self, namespace, *, scope_type="session", key="", prefix=False, offset=0, limit=100):
        params = {"session_id": self.session_id, "scope_type": scope_type, "namespace": namespace,
                  "prefix": str(prefix).lower(), "offset": offset, "limit": limit}
        if key:
            params["key"] = key
        return await self._client.request_state("GET", _API + "/kv", params=params)

    async def mutate_kv(self, operation_key, changes, *, expected_seq=None, catalog=None, turn_seq=None):
        body = {**self._turn_fields(turn_seq), "session_id": self.session_id, "key": operation_key, "kv": changes}
        if expected_seq is not None:
            body["expected_seq"] = expected_seq
        if catalog is not None:
            body["catalog"] = catalog
        try:
            return await self._client.request_state(
                "POST", _API + "/kv:mutate", body={**body, "lease_epoch": self.epoch},
            )
        except StateAPIError as error:
            self._note_error(error)
            raise

    async def chat_history(self, *, namespace="", checkpoint_id=""):
        return await self._client.request_state(
            "GET", self.path + "/chat-history", params={"checkpoint_ns": namespace, "checkpoint_id": checkpoint_id},
        )

    async def stage_blob(self, data):
        return await self._mutate("/blobs:stage", {"data": base64.b64encode(data).decode("ascii")})

    async def read_blob(self, ref):
        return await decode_payload(self._client, self.path, {"blob": ref})

    async def encode(self, data):
        try:
            return await encode_payload(self._client, self.path, self.epoch, data)
        except StateAPIError as error:
            self._note_error(error)
            raise

    async def decode(self, payload):
        return await decode_payload(self._client, self.path, payload)

    def standalone_tools(self):
        epoch = self.epoch
        # The guard shares the same fencing signal as heartbeat and mutations.
        if self._lease.guard is None:
            self._lease.guard = StandaloneToolGuard(self._client, self.path, epoch, turn_seq=self.turn_seq)
        return self._lease.guard

    async def begin_standalone_tool(self, call_id, name, args_digest, *, turn_seq=None):
        # Use the current transport, including the invocation's isolated sync
        # transport when this is called via SyncAHPJournalClient.
        guard = StandaloneToolGuard(self._client, self.path, self.epoch, **self._turn_fields(turn_seq))
        try:
            return await guard.begin(call_id, name, args_digest)
        except StateAPIError as error:
            self._note_error(error)
            raise

    async def get_tool_intent(self, call_id):
        return await self._client.request_state("GET", _API + "/aux:mutate", params={
            "session_id": self.session_id, "call_id": call_id,
        })

    async def get_user_state(self, namespace):
        return await self._client.request_state("GET", _API + "/aux:mutate", params={
            "state_namespace": namespace,
        })

    async def mutate_tool_intent(self, transition):
        try:
            return await self._client.request_state("POST", _API + "/intents:mutate", body={
                "session_id": self.session_id, "lease_epoch": self.epoch,
                "transition": {**transition, **self._turn_fields(transition.get("turn_seq"))},
            })
        except StateAPIError as error:
            self._note_error(error)
            raise


def _sync(method):
    @wraps(method)
    def run(self, *args, **kwargs):
        return invoke_sync(self._journal, lambda view: method(view, *args, **kwargs))
    return run


class SyncAHPJournalClient:
    """Blocking State facade with a fresh transport for each call.

    Drive lease renewal explicitly in synchronous code, or use the async
    client with its background-heartbeat context.
    """

    def __init__(self, client, session_id, *, harness_type, format_version=1):
        self._journal = AHPJournalClient(client, session_id, harness_type=harness_type, format_version=format_version)

    @property
    def session_id(self):
        return self._journal.session_id

    @property
    def fenced(self):
        return self._journal.fenced

    @property
    def epoch(self):
        return self._journal.epoch

    @property
    def turn_seq(self):
        return self._journal.turn_seq

    acquire = _sync(AHPJournalClient.acquire)
    enqueue_turn = _sync(AHPJournalClient.enqueue_turn)
    claim_turn = _sync(AHPJournalClient.claim_turn)
    claim_vm_turn = _sync(AHPJournalClient.claim_vm_turn)
    cancel_turn = _sync(AHPJournalClient.cancel_turn)
    turn_status = _sync(AHPJournalClient.turn_status)
    inspect = _sync(AHPJournalClient.inspect)
    list_sessions = _sync(AHPJournalClient.list_sessions)
    delete_session = _sync(AHPJournalClient.delete_session)
    set_legal_hold = _sync(AHPJournalClient.set_legal_hold)
    heartbeat = _sync(AHPJournalClient.heartbeat)
    release = _sync(AHPJournalClient.release)
    append = _sync(AHPJournalClient.append)
    events = _sync(AHPJournalClient.events)
    put_checkpoint = _sync(AHPJournalClient.put_checkpoint)
    checkpoint = _sync(AHPJournalClient.checkpoint)
    list_checkpoints = _sync(AHPJournalClient.list_checkpoints)
    put_writes = _sync(AHPJournalClient.put_writes)
    get_kv = _sync(AHPJournalClient.get_kv)
    mutate_kv = _sync(AHPJournalClient.mutate_kv)
    chat_history = _sync(AHPJournalClient.chat_history)
    stage_blob = _sync(AHPJournalClient.stage_blob)
    read_blob = _sync(AHPJournalClient.read_blob)
    encode = _sync(AHPJournalClient.encode)
    decode = _sync(AHPJournalClient.decode)
    begin_standalone_tool = _sync(AHPJournalClient.begin_standalone_tool)
    get_tool_intent = _sync(AHPJournalClient.get_tool_intent)
    mutate_tool_intent = _sync(AHPJournalClient.mutate_tool_intent)
    get_user_state = _sync(AHPJournalClient.get_user_state)
