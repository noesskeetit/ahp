"""ADK native session service backed by the compact AHP event journal."""
from __future__ import annotations

import asyncio
import base64
from copy import deepcopy
from dataclasses import dataclass, field
from hashlib import sha256
import hmac
import json
import time
from datetime import datetime
from urllib.parse import quote
from uuid import uuid4

from google.adk.errors import StaleSessionError
from google.adk.events import Event
from google.adk.sessions import BaseSessionService, Session
from google.adk.sessions.base_session_service import ListSessionsResponse
from google.adk.tools.function_tool import FunctionTool
from google.genai import types

from ahp_state import wire
from ahp_state.client import StateAPIError, StateConflictError, StateNotFoundError, StateValidationError
from ahp_state.journal_client import AHPJournalClient, _receipt_for_turn
from ahp_state.journal_payload import decode_payload, encode_payload
from ahp_state.journal_tools import StandaloneToolGuard, admission_may_have_committed

API = "/v1/state"
NAMESPACE = "adk"
INIT = "adk:session:init"
EVENT_TYPE = "adk.event"
TURN_INPUT_CONTROL = {"turn.steer", "turn.followup"}


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False,
                      sort_keys=True, separators=(",", ":")).encode()


def _ephemeral(key):
    return key.startswith(("temp:", "secret:"))


def _scope_key(key):
    prefix, delimiter, name = key.partition(":")
    if delimiter and prefix in ("app", "user"):
        return prefix, wire.encode_id(name)
    return "session", wire.encode_id(key)


def _native_key(item):
    key = wire.decode_id(item["key"])
    return key if item["scope_type"] == "session" else f"{item['scope_type']}:{key}"


@dataclass
class _Projection:
    head: int = 0
    revision: int = 0
    created_at: float = 0
    updated_at: float = 0
    state: dict = field(default_factory=dict)
    versions: dict = field(default_factory=dict)
    events: dict = field(default_factory=dict)

    def marker(self):
        return {"head": self.head, "revision": self.revision, "versions": self.versions}

    def remember(self, event):
        self.events[event.id] = event
        self.updated_at = max(self.updated_at, event.timestamp)


class ADKToolOutcomeUnknown(RuntimeError):
    """The host must stop this invocation until external reconciliation."""


class ADKToolPending(RuntimeError):
    """The host can check again while holding the lease."""


class AHPJournalSessionService(BaseSessionService):
    """ADK sessions in the scope of one authenticated AHP deployment and user.

    Hosts pass the same ordinary FunctionTool instances to this adapter and
    Runner. Install `before_tool_callback` and call `recover_tool_calls` before
    starting a replacement Runner invocation.
    """

    def __init__(self, client, *, app_name, user_id, session_id=None, lease_epoch=0,
                 turn_seq=None, effect_tools=(), tool_gateway=None):
        self.client = client
        self.app_name = app_name
        self.user_id = user_id
        self.session_id = session_id
        self.lease_epoch = 0 if lease_epoch is None else lease_epoch
        self.turn_seq = turn_seq
        self.control = None
        self.path = f"{API}/sessions/{quote(session_id, safe='')}" if session_id is not None else ""
        self.lock = asyncio.Lock()
        self._bindings = {}
        self._projection = None
        self._needs_refresh = False
        self._fenced = False
        self.effect_tools = {}
        for tool in effect_tools:
            self._validate_tool_registration(tool)
            if tool.name in self.effect_tools:
                raise StateValidationError("INVALID_ARGUMENT", "duplicate managed ADK tool")
            self.effect_tools[tool.name] = tool
        self.tool_gateway = tool_gateway
        self.tool_guard = StandaloneToolGuard(client, self.path, self.lease_epoch, turn_seq=turn_seq)
        self._receipts = {}

    @staticmethod
    def _validate_tool_registration(tool):
        # ADK runs before_tool_callback earlier than its own confirmation gate.
        # Only an ordinary FunctionTool can use this dispatch path.
        if type(tool) is not FunctionTool or tool._require_confirmation is not False or tool.is_long_running:
            raise StateValidationError("INVALID_ARGUMENT",
                                       "managed ADK tool must be an ordinary FunctionTool without native confirmation")

    def _verify(self, app, user):
        if (app, user) != (self.app_name, self.user_id):
            raise StateValidationError("INVALID_ARGUMENT", "ADK identity differs from the trusted host binding")

    def _target(self, session_id):
        if session_id is None:
            raise StateValidationError("INVALID_ARGUMENT", "ADK operation requires a session ID")
        bound = self._bindings.get(session_id)
        if bound is not None:
            return bound
        if session_id == self.session_id:
            return self
        return self._new_binding(session_id, 0)

    def _new_binding(self, session_id, lease_epoch, turn_seq=None):
        return AHPJournalSessionService(
            self.client, app_name=self.app_name, user_id=self.user_id, session_id=session_id,
            lease_epoch=lease_epoch, turn_seq=turn_seq, effect_tools=self.effect_tools.values(),
            tool_gateway=self.tool_gateway,
        )

    def bind_session(self, session_id, lease_epoch, *, turn_seq=None):
        """Bind the host's lease and, for VM execution, its exact turn."""
        if lease_epoch <= 0:
            raise StateValidationError("INVALID_ARGUMENT", "ADK requires a positive write lease epoch")
        previous = self._bindings.get(session_id)
        if previous is None and session_id == self.session_id:
            previous = self
        if previous is not None:
            if (previous.lease_epoch == lease_epoch and previous.turn_seq == turn_seq
                    and not previous._fenced):
                return previous
            previous._invalidate()
        binding = self._new_binding(session_id, lease_epoch, turn_seq)
        self._bindings[session_id] = binding
        return binding

    def _invalidate(self, *, preserve_control=False):
        self._projection = None
        self._fenced = True
        self.tool_guard.fenced = True
        self._receipts.clear()
        if not preserve_control:
            self.control = None

    def _owns_lease(self):
        return self.lease_epoch > 0 and not self._fenced and not self.tool_guard.fenced

    def _marker(self, projection):
        return {**projection.marker(), "turn_seq": self.turn_seq}

    def _session_marker(self, session):
        if not session._storage_update_marker:
            raise StaleSessionError("ADK session has no journal revision")
        marker = json.loads(session._storage_update_marker)
        if marker.get("turn_seq") != self.turn_seq:
            raise StaleSessionError("ADK session belongs to another turn")
        return marker

    async def _payload(self, value, *, allow_spill=True):
        return await encode_payload(
            self.client, self.path, self.lease_epoch, _json(value), allow_spill=allow_spill,
        )

    async def _value(self, payload):
        return json.loads(await decode_payload(self.client, self.path, payload))

    async def _page(self, after_seq, *, include_state=True):
        params = {"after_seq": after_seq, "limit": 100}
        if include_state:
            params["state_namespace"] = NAMESPACE
        return await self.client.request_state("GET", self.path + "/events", params=params)

    def _retains_projection(self):
        return self._owns_lease() and getattr(self.client, "has_fixed_scope", True)

    def _cached(self):
        if self._retains_projection() and not self._needs_refresh:
            return self._projection
        return None

    async def _read(self):
        projection = (self._projection if self._retains_projection() else None) or _Projection()
        try:
            page = await self._page(projection.head)
            epoch = page.get("lease_epoch")
            if epoch is not None and self._owns_lease() and epoch != self.lease_epoch:
                self._invalidate()
                if projection.head:
                    projection = _Projection()
                    page = await self._page(0)
            if page["head_seq"] < projection.head:
                raise StaleSessionError("ADK journal head moved backwards")
            head = page["head_seq"]
            entries = page.get("state") or []
            state = {_native_key(item): await self._value(item["payload"])
                     for item in entries if not item.get("deleted")}
            versions = {_native_key(item): item["version"] for item in entries}
            after = projection.head
            while after < head:
                for row in page.get("events") or []:
                    if row["seq"] > head:
                        break
                    if row["seq"] != after + 1:
                        raise StateValidationError("INVALID_ARGUMENT", "ADK journal history is incomplete")
                    after = row["seq"]
                    if self.turn_seq is None or row["type"] not in TURN_INPUT_CONTROL:
                        projection.revision = after
                    if row["id"] == INIT:
                        initialized = await self._value(row["payload"])
                        projection.created_at = initialized["created_at"]
                        projection.updated_at = max(projection.updated_at, projection.created_at)
                    elif row["type"] == EVENT_TYPE:
                        if not projection.created_at:
                            raise StateValidationError("INVALID_ARGUMENT", "ADK event appears before initialization")
                        event = Event.model_validate(await self._value(row["payload"]))
                        projection.remember(event)
                    elif not projection.created_at and not (
                        row["type"] in TURN_INPUT_CONTROL or row["type"] == "turn.completed"
                        or (row["type"] == "turn.input" and row["role"] == "user" and row["codec"] == "json")
                    ):
                        raise StateValidationError("INVALID_ARGUMENT", "ADK session has unexpected records before initialization")
                if after < head:
                    if not page.get("events"):
                        raise StateValidationError("INVALID_ARGUMENT", "ADK journal history is incomplete")
                    page = await self._page(after, include_state=False)
            projection.head, projection.state, projection.versions = head, state, versions
            return projection
        except StateAPIError as exc:
            self._projection = None
            if exc.code == "NOT_FOUND":
                return None
            if exc.code == "STALE_EPOCH":
                self._invalidate()
            raise
        except Exception:
            self._projection = None
            raise

    def _remember(self, projection):
        if self._retains_projection():
            self._projection = projection
            self._needs_refresh = False

    def _session(self, projection, *, config=None, transient=None):
        events = list(projection.events.values())
        if config is not None:
            if config.after_timestamp is not None:
                events = [event for event in events if event.timestamp >= config.after_timestamp]
            if config.num_recent_events is not None:
                limit = config.num_recent_events
                events = events[-limit:] if limit else []
        session = Session(
            app_name=self.app_name, user_id=self.user_id, id=self.session_id,
            state={**deepcopy(projection.state), **(transient or {})},
            events=[event.model_copy(deep=True) for event in events], last_update_time=projection.updated_at,
        )
        session._storage_update_marker = _json(self._marker(projection)).decode()
        return session

    def _catalog(self, *, event_count, updated_at):
        return {
            "agent_id": wire.encode_id(self.app_name),
            "native_key": wire.encode_id(self.session_id),
            "summary_metadata": {"event_count": event_count, "last_update_time": updated_at},
        }

    async def _append(self, *, event_id, kind, role, document, delta, marker,
                      catalog, intents=(), outcomes=(), finish_turn=None, steer_ack=None):
        changes = []
        for key, value in delta.items():
            scope, native_key = _scope_key(key)
            mutation = {"scope_type": scope, "namespace": NAMESPACE, "key": native_key,
                        "expected_version": marker["versions"].get(key, 0)}
            if value is None:
                mutation["delete"] = True
            else:
                mutation["payload"] = await self._payload(value, allow_spill=scope == "session")
            changes.append(mutation)
        body = {
            "lease_epoch": self.lease_epoch, "key": event_id, "expected_seq": marker["head"],
            "events": [{"id": event_id, "type": kind, "role": role, "codec": "json-adk-2.14.0",
                        "payload": await self._payload(document)}],
            "kv": changes,
            "catalog": catalog,
        }
        if intents:
            body["intents"] = intents
        if outcomes:
            body["tool_outcomes"] = outcomes
        if finish_turn is not None:
            body["finish_turn"] = finish_turn
        if self.turn_seq is not None:
            body["turn_seq"] = self.turn_seq
        if steer_ack is not None:
            body["steer_ack"] = steer_ack
        path = self.path + "/events:append"

        async def commit(request):
            try:
                return await self.client.request_state("POST", path, body=request)
            except (StateAPIError, json.JSONDecodeError) as exc:
                if (self.turn_seq is None or (steer_ack is None and not finish_turn)
                        or not admission_may_have_committed(exc)):
                    raise
                # Replay the exact operation to recover its atomic ACK or
                # followup receipt after an ambiguous response.
                return await self.client.request_state("POST", path, body=request)

        try:
            receipt = await commit(body)
        except StateAPIError as exc:
            if self.turn_seq is None or exc.code != "CONFLICT" or exc.message != "session head has changed":
                raise
            # The coordinator may have appended steer or followup while ADK
            # was building the event. Rebase only over those control records.
            latest = await self._read()
            if not self._owns_lease() or latest is None or latest.revision != marker["revision"]:
                raise
            receipt = await commit({**body, "expected_seq": latest.head})
        control = receipt.get("control") or {}
        next_turn = control.get("next_turn") if finish_turn else None
        receipt_turn = next_turn["input_seq"] if next_turn is not None else self.turn_seq
        self.control = _receipt_for_turn(receipt, receipt_turn).get("control") if self._owns_lease() else None
        return receipt

    def _tool_key(self, invocation, call_id):
        if not invocation or not call_id:
            raise StateValidationError("INVALID_ARGUMENT", "managed ADK call requires native identity")
        return "adk:tool:" + hmac.digest(self.session_id.encode(), _json([invocation, call_id]), "sha256").hex()

    def _managed_calls(self, event):
        for call in event.get_function_calls():
            if call.name in self.effect_tools:
                yield self._tool_key(event.invocation_id, call.id), call

    async def _intent(self, key):
        return await self.client.request_state("GET", API + "/aux:mutate", params={
            "session_id": self.session_id, "call_id": key,
        })

    async def _mark_dispatched(self, key):
        transition = {"call_id": key, "gateway_operation_id": key, "status": "DISPATCHED"}
        if self.turn_seq is not None:
            transition["turn_seq"] = self.turn_seq
        await self.client.request_state("POST", API + "/aux:mutate", body={
            "session_id": self.session_id, "lease_epoch": self.lease_epoch,
            "transition": transition,
        })

    @staticmethod
    def _args_digest(args):
        return sha256(_json(args or {})).digest()

    def _validate_intent(self, key, tool, args, intent):
        if (intent["tool_name"] != tool or base64.b64decode(intent["args_digest"]) != self._args_digest(args)
                or (intent["status"] != "PREPARED" and intent["gateway_operation_id"] != key)):
            raise StateValidationError("INVALID_ARGUMENT", "managed ADK call differs from its durable intent")

    async def _get_tool_receipt(self, key, tool, *, allow_missing=False):
        try:
            result = await self.tool_gateway("GetResult", {"key": key})
        except StateNotFoundError as exc:
            if allow_missing and exc.code == "NOT_FOUND":
                return "missing", None
            raise
        except StateConflictError:
            operation = await self.tool_gateway("Get", {"key": key})
            if operation["key"] != key or operation["target"] != tool:
                raise StateValidationError("INVALID_ARGUMENT", "Tool Gateway returned another ADK operation")
            if operation["status"] == "SIDE_EFFECT_UNKNOWN":
                return "unknown", {"code": "TOOL_OUTCOME_UNKNOWN"}
            if operation["status"] in {"ACCEPTED", "DISPATCHED", "SUCCEEDED"}:
                raise ADKToolPending(key)
            raise StateValidationError("INVALID_ARGUMENT", "ADK tool has no supported Gateway result")
        if (result["key"] != key or result["target"] != tool or result["status"] != "SUCCEEDED"
                or result["result_storage"] != "inline" or not isinstance(result.get("payload"), dict)):
            raise StateValidationError("INVALID_ARGUMENT", "unsupported ADK Tool Gateway receipt")
        return "known", result["payload"]

    async def _call_tool(self, key, tool, args):
        await self._mark_dispatched(key)
        try:
            await self.tool_gateway("Call", {"key": key, "tool": tool, "arguments": args or {}})
        except StateAPIError as exc:
            if exc.code not in {"TRANSPORT", "TIMEOUT"}:
                raise
        return await self._get_tool_receipt(key, tool)

    async def _publish_receipt(self, session, event, call_id, tool, key, receipt):
        self._receipts[key] = (tool, receipt, key)
        await self.append_event(session, Event(
            id=key + ":result", invocation_id=event.invocation_id, author=event.author, branch=event.branch,
            content=types.Content(role="user", parts=[
                types.Part(function_response=types.FunctionResponse(id=call_id, name=tool, response=receipt[1])),
            ]),
        ))

    async def before_tool_callback(self, tool, args, tool_context):
        """ADK's native callback may dispatch only after the journal recorded the model call."""
        session = tool_context.session
        self._verify(session.app_name, session.user_id)
        return await self._target(session.id)._before_tool_callback(tool, args, tool_context)

    async def _before_tool_callback(self, tool, args, tool_context):
        if tool.name not in self.effect_tools:
            return None
        if tool is not self.effect_tools[tool.name]:
            raise StateValidationError("INVALID_ARGUMENT", "ADK managed tool differs from the registered instance")
        self._validate_tool_registration(tool)
        if not self._owns_lease():
            raise StaleSessionError("ADK managed tool requires a current write lease")
        if self.turn_seq is not None:
            self._session_marker(session=tool_context.session)
        key = self._tool_key(tool_context.invocation_id, tool_context.function_call_id)
        if self.tool_gateway is None:
            decision = await self.tool_guard.begin(key, tool.name, self._args_digest(args))
            if not decision["can_execute"]:
                if decision["status"] == "COMPLETED":
                    prior = await self._value(decision["result"])
                    self._receipts[key] = (tool.name, ("known", prior), decision["gateway_operation_id"])
                    return deepcopy(prior)
                raise ADKToolOutcomeUnknown(key)
            result = await tool.run_async(args=args, tool_context=tool_context)
            if not isinstance(result, dict):
                result = {"result": result}
            # Runner persists the native response and outcome in one append.
            self._receipts[key] = (tool.name, ("known", result), decision["gateway_operation_id"])
            return deepcopy(result)
        intent = await self._intent(key)
        self._validate_intent(key, tool.name, args, intent)
        if intent["status"] != "PREPARED":
            raise ADKToolOutcomeUnknown(key)
        receipt = await self._call_tool(key, tool.name, args)
        if receipt[0] == "unknown":
            event = next(event for event in tool_context.session.events
                         if any(found == key for found, _ in self._managed_calls(event)))
            await self._publish_receipt(tool_context.session, event, tool_context.function_call_id,
                                        tool.name, key, receipt)
            raise ADKToolOutcomeUnknown(key)
        self._receipts[key] = (tool.name, receipt, key)
        return deepcopy(receipt[1])

    async def recover_tool_calls(self, invocation_id, *, session_id=None):
        """Reconcile before invoking Runner for a cold resumption."""
        return await self._target(session_id or self.session_id)._recover_tool_calls(invocation_id)

    async def _recover_tool_calls(self, invocation_id):
        if not self.effect_tools:
            return
        if not self._owns_lease():
            raise StateValidationError("INVALID_ARGUMENT", "ADK recovery requires a write lease")
        for tool in self.effect_tools.values():
            self._validate_tool_registration(tool)
        projection = await self._read()
        if projection is None or not projection.created_at:
            raise StaleSessionError("ADK journal session does not exist")
        self._remember(projection)
        session = self._session(projection)
        for event in list(session.events):
            for key, call in self._managed_calls(event):
                intent = await self._intent(key)
                if intent["status"] == "COMPLETED":
                    continue
                if self.tool_gateway is None:
                    await self.tool_guard.begin(key, call.name, self._args_digest(call.args))
                    raise ADKToolOutcomeUnknown(key)
                self._validate_intent(key, call.name, call.args, intent)
                if event.invocation_id != invocation_id or intent["status"] not in {"PREPARED", "DISPATCHED"}:
                    raise ADKToolOutcomeUnknown(key)
                receipt = await self._get_tool_receipt(key, call.name, allow_missing=True)
                if receipt[0] == "missing":
                    receipt = await self._call_tool(key, call.name, call.args)
                elif intent["status"] == "PREPARED":
                    await self._mark_dispatched(key)
                await self._publish_receipt(session, event, call.id, call.name, key, receipt)
                if receipt[0] == "unknown":
                    raise ADKToolOutcomeUnknown(key)

    async def _tool_changes(self, event):
        intents = [{"call_id": key, "tool_name": call.name, "idempotency_key": key,
                    "args_digest": list(self._args_digest(call.args))}
                   for key, call in self._managed_calls(event)]
        outcomes = []
        for response in event.get_function_responses():
            if response.name not in self.effect_tools:
                continue
            key = self._tool_key(event.invocation_id, response.id)
            prepared = self._receipts.get(key)
            if prepared is None:
                raise StateValidationError("INVALID_ARGUMENT", "ADK tool response lacks its authoritative receipt")
            tool_name, (kind, payload), operation_id = prepared
            if response.name != tool_name or response.response != payload:
                raise StateValidationError("INVALID_ARGUMENT", "ADK tool response differs from its Gateway receipt")
            outcome = {"call_id": key, "gateway_operation_id": operation_id,
                       "status": "COMPLETED" if kind == "known" else "OUTCOME_UNKNOWN"}
            if kind == "known":
                outcome["result"] = await self._payload(payload)
            outcomes.append(outcome)
        return intents, outcomes

    async def create_session(self, *, app_name, user_id, state=None, session_id=None):
        self._verify(app_name, user_id)
        session_id = session_id or self.session_id or uuid4().hex
        target = self._target(session_id)
        acquired = None
        if not target._owns_lease():
            acquired = AHPJournalClient(self.client, session_id, harness_type="adk")
            lease = await acquired.acquire()
            target = self.bind_session(session_id, lease["epoch"])
        try:
            return await target._create(state or {})
        except BaseException:
            if acquired is not None:
                try:
                    await acquired.release()
                except StateAPIError:
                    pass
                target._invalidate()
                self._bindings.pop(session_id, None)
            raise

    async def _create(self, values):
        async with self.lock:
            projection = await self._read()
            if projection is None or projection.created_at:
                raise StateValidationError("INVALID_ARGUMENT", "ADK session already exists or could not be acquired")
            created_at = time.time()
            delta = {key: value for key, value in values.items() if not _ephemeral(key)}
            try:
                receipt = await self._append(
                    event_id=INIT, kind="adk.init", role="system", document={"created_at": created_at},
                    delta=delta, marker=projection.marker(), catalog=self._catalog(event_count=0, updated_at=created_at),
                )
            except StateAPIError as exc:
                if exc.code == "STALE_EPOCH":
                    self._invalidate()
                raise
            projection.created_at = projection.updated_at = created_at
            self._apply(projection, delta, receipt["head_seq"])
            return self._session(projection, transient={key: v for key, v in values.items() if _ephemeral(key)})

    async def get_session(self, *, app_name, user_id, session_id, config=None):
        self._verify(app_name, user_id)
        target = self._target(session_id)
        async with target.lock:
            projection = await target._read()
            if projection is None or not projection.created_at:
                return None
            target._remember(projection)
            return target._session(projection, config=config)

    def _apply(self, projection, delta, head):
        for key, value in delta.items():
            if value is None:
                projection.state.pop(key, None)
            else:
                projection.state[key] = deepcopy(value)
            projection.versions[key] = projection.versions.get(key, 0) + 1
        projection.head = projection.revision = head
        self._remember(projection)

    def _refresh(self, target, projection, transient):
        local = {key: value for key, value in target.state.items() if _ephemeral(key)}
        target.state = {**deepcopy(projection.state), **local, **transient}
        target.last_update_time = projection.updated_at
        target._storage_update_marker = _json(self._marker(projection)).decode()

    async def append_event(self, session, event, *, finish_turn=None, steer_ack=None):
        """Supply steer_ack with the durable native user event containing the applied input."""
        self._verify(session.app_name, session.user_id)
        return await self._target(session.id)._append_event(session, event, finish_turn=finish_turn, steer_ack=steer_ack)

    async def _append_event(self, session, event, *, finish_turn, steer_ack):
        if steer_ack is not None and (event.partial or event.author != "user" or not (
            event.content and event.content.parts
        )):
            raise StateValidationError("INVALID_ARGUMENT", "steer ACK requires a durable native user input event")
        if event.partial:
            return event
        if not event.id:
            raise StateValidationError("INVALID_ARGUMENT", "ADK event requires a stable identity")
        if not self._owns_lease():
            raise StaleSessionError("ADK session does not have a current write lease")
        before = self._session_marker(session) if self.turn_seq is not None else None
        stored = event.model_copy(deep=True)
        transient = {key: value for key, value in stored.actions.state_delta.items() if _ephemeral(key)}
        delta = {key: value for key, value in stored.actions.state_delta.items() if not _ephemeral(key)}
        stored.actions.state_delta = delta
        document = stored.model_dump(mode="json")

        async with self.lock:
            projection = self._cached() or await self._read()
            if projection is None or not projection.created_at:
                raise StaleSessionError("ADK journal session does not exist")
            if not self._owns_lease():
                raise StaleSessionError("ADK write lease is no longer current")
            self._remember(projection)
            prior = projection.events.get(event.id)
            if prior is not None:
                if prior.model_dump(mode="json") != document:
                    raise StateValidationError("KEY_REUSE_MISMATCH", "ADK event ID already has different data")
                session.events = [entry.model_copy(deep=True) for entry in projection.events.values()]
                self._refresh(session, projection, transient)
                return prior.model_copy(deep=True)
            if before is None:
                before = self._session_marker(session)
            position = "revision" if self.turn_seq is not None else "head"
            if before[position] != getattr(projection, position) or any(
                before["versions"].get(key, 0) != projection.versions.get(key, 0) for key in delta
            ):
                raise StaleSessionError("ADK event was built against stale state")
            try:
                intents, outcomes = await self._tool_changes(stored) if self.effect_tools else ([], [])
                role = "tool_call" if intents else "tool_result" if outcomes else (
                    "user" if event.author == "user" else "assistant")
                receipt = await self._append(
                    event_id="event:" + wire.encode_id(event.id), kind=EVENT_TYPE, role=role, document=document,
                    delta=delta, marker=projection.marker(), intents=intents, outcomes=outcomes,
                    catalog=self._catalog(event_count=len(projection.events) + 1,
                                          updated_at=max(projection.updated_at, event.timestamp)),
                    finish_turn=finish_turn, steer_ack=steer_ack,
                )
            except BaseException as exc:
                # A lost ACK or cancellation may have committed the event.
                # Force a tail read on the next operation before using CAS.
                self._needs_refresh = True
                if isinstance(exc, StateAPIError) and exc.code == "CONFLICT":
                    raise StaleSessionError("ADK journal changed during event commit") from exc
                if isinstance(exc, StateAPIError) and exc.code == "STALE_EPOCH":
                    self._invalidate()
                raise
            for outcome in outcomes:
                self._receipts.pop(outcome["call_id"], None)
            projection.remember(stored)
            self._apply(projection, delta, receipt["head_seq"])
            session.events.append(stored.model_copy(deep=True))
            self._refresh(session, projection, transient)
            if finish_turn:
                self._invalidate(preserve_control=True)
            return stored.model_copy(deep=True)

    async def get_user_state(self, *, app_name, user_id):
        self._verify(app_name, user_id)
        entries = await self.client.request_state("GET", API + "/aux:mutate", params={"state_namespace": NAMESPACE})
        # Shared payloads are always inline, including when no session is bound.
        return {wire.decode_id(item["key"]): await self._value(item["payload"])
                for item in entries or [] if not item.get("deleted")}

    async def list_sessions(self, *, app_name, user_id=None):
        self._verify(app_name, self.user_id if user_id is None else user_id)
        journal = AHPJournalClient(self.client, self.session_id or "", harness_type="adk")
        sessions = []
        cursor = ""
        agent = wire.encode_id(self.app_name)
        while True:
            page = await journal.list_sessions(view="catalog", cursor=cursor)
            for row in page["sessions"]:
                if row.get("agent_id") not in (None, "", agent):
                    continue
                updated = (row.get("summary_metadata") or {}).get("last_update_time")
                if updated is None:
                    updated = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00")).timestamp()
                sessions.append(Session(app_name=self.app_name, user_id=self.user_id,
                                        id=row["session_id"], last_update_time=updated))
            cursor = page.get("next_cursor")
            if not cursor:
                return ListSessionsResponse(sessions=sessions)

    async def pause_for_input(self, *, session_id=None, invocation_id=None):
        """Release a directly acquired lease after Runner drains a native HITL pause."""
        target = self._target(session_id or self.session_id)
        async with target.lock:
            projection = target._cached() or await target._read()
            pending = set()
            if projection is not None:
                for event in projection.events.values():
                    if invocation_id is not None and event.invocation_id != invocation_id:
                        continue
                    pending.update(event.long_running_tool_ids or ())
                    if event.author == "user":
                        pending.difference_update(response.id for response in event.get_function_responses())
            if not pending:
                raise StateValidationError("INVALID_ARGUMENT", "ADK invocation has no unanswered native input")
            await target._release("WAITING_INPUT")
        self._bindings.pop(target.session_id, None)

    async def release_session(self, *, session_id=None):
        """Release a directly acquired session lease after a completed invocation."""
        target = self._target(session_id or self.session_id)
        async with target.lock:
            await target._release("IDLE")
        self._bindings.pop(target.session_id, None)

    async def _release(self, status):
        if not self._owns_lease():
            raise StaleSessionError("ADK session does not have a current write lease")
        try:
            await self.client.request_state("POST", self.path + ":heartbeat", body={
                "lease_epoch": self.lease_epoch, "release": True, "status": status,
            })
        except StateAPIError as exc:
            if exc.code == "STALE_EPOCH":
                self._invalidate()
            raise
        self._invalidate()

    async def delete_session(self, *, app_name, user_id, session_id):
        self._verify(app_name, user_id)
        target = self._target(session_id)
        async with target.lock:
            journal = AHPJournalClient(self.client, session_id, harness_type="adk")
            try:
                metadata = await journal.inspect()
            except StateAPIError as exc:
                if exc.code == "NOT_FOUND":
                    return
                raise
            if metadata["harness_type"] != "adk":
                raise StateValidationError("INVALID_ARGUMENT", "session belongs to another harness")
            await journal.delete_session()
            target._invalidate()
            self._bindings.pop(session_id, None)
