"""ADK native session service backed by the compact AHP event journal."""
from __future__ import annotations

import asyncio
import base64
from copy import deepcopy
from hashlib import sha256
import hmac
import json
import time
from importlib.metadata import version
from urllib.parse import quote

from google.adk.errors import StaleSessionError
from google.adk.events import Event
from google.adk.sessions import BaseSessionService, Session
from google.adk.sessions.base_session_service import ListSessionsResponse
from google.adk.tools.function_tool import FunctionTool
from google.genai import types

from ahp_state import wire
from ahp_state.client import StateAPIError, StateConflictError, StateNotFoundError, StateValidationError
from ahp_state.journal_payload import decode_payload, encode_payload
from ahp_state.journal_tools import StandaloneToolGuard

API = "/v1/state"
NAMESPACE = "adk"
INIT = "adk:session:init"
EVENT_TYPE = "adk.event"


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


def _marker(page):
    return {"head": page["head_seq"],
            "versions": {_native_key(item): item["version"] for item in page.get("state") or []}}


class ADKToolOutcomeUnknown(RuntimeError):
    """The host must stop this invocation until external reconciliation."""


class ADKToolPending(RuntimeError):
    """The host can check again while holding the lease."""


class AHPJournalSessionService(BaseSessionService):
    """One ADK session bound by the host to an authenticated AHP session lease.

    Hosts pass the same ordinary FunctionTool instances to this adapter and
    Runner. Install `before_tool_callback` and call `recover_tool_calls` before
    starting a replacement Runner invocation.
    """

    def __init__(self, client, *, app_name, user_id, session_id, lease_epoch, effect_tools=(), tool_gateway=None):
        if version("google-adk") != "2.14.0":
            raise StateValidationError("INVALID_ARGUMENT", "ADK journal adapter supports version 2.14.0")
        self.client = client
        self.app_name = app_name
        self.user_id = user_id
        self.session_id = session_id
        self.lease_epoch = lease_epoch
        self.path = f"{API}/sessions/{quote(session_id, safe='')}"
        self.lock = asyncio.Lock()
        self.effect_tools = {}
        for tool in effect_tools:
            self._validate_tool_registration(tool)
            if tool.name in self.effect_tools:
                raise StateValidationError("INVALID_ARGUMENT", "duplicate managed ADK tool")
            self.effect_tools[tool.name] = tool
        self.tool_gateway = tool_gateway
        self.tool_guard = StandaloneToolGuard(client, self.path, lease_epoch)
        self._receipts = {}

    @staticmethod
    def _validate_tool_registration(tool):
        # ADK runs before_tool_callback earlier than its own confirmation gate.
        # Only the pinned ordinary FunctionTool can use this dispatch path.
        if type(tool) is not FunctionTool or tool._require_confirmation is not False or tool.is_long_running:
            raise StateValidationError("INVALID_ARGUMENT",
                                       "managed ADK tool must be an ordinary FunctionTool without native confirmation")

    def _verify(self, app, user, session=None):
        if (app, user) != (self.app_name, self.user_id) or (
            session is not None and session != self.session_id
        ):
            raise StateValidationError("INVALID_ARGUMENT", "ADK identity differs from the trusted host binding")

    async def _payload(self, value, *, allow_spill=True):
        return await encode_payload(
            self.client, self.path, self.lease_epoch, _json(value), allow_spill=allow_spill,
        )

    async def _value(self, payload):
        return json.loads(await decode_payload(self.client, self.path, payload))

    async def _page(self):
        return await self.client.request_state("GET", self.path + "/events",
                                               params={"state_namespace": NAMESPACE, "limit": 100})

    async def _read(self):
        try:
            page = await self._page()
        except StateAPIError as exc:
            if exc.code == "NOT_FOUND":
                return None
            raise
        if not page["head_seq"]:
            return None
        head = page["head_seq"]
        events = list(page["events"])
        while events[-1]["seq"] < head:
            more = await self.client.request_state("GET", self.path + "/events", params={
                "after_seq": events[-1]["seq"], "limit": 100,
            })
            if not more["events"]:
                raise StateValidationError("INVALID_ARGUMENT", "ADK journal history is incomplete")
            events.extend(item for item in more["events"] if item["seq"] <= head)
        if events[0]["id"] != INIT:
            raise StateValidationError("INVALID_ARGUMENT", "session does not contain an ADK journal")

        initialized = await self._value(events[0]["payload"])
        native_events = [Event.model_validate(await self._value(item["payload"]))
                         for item in events[1:] if item["type"] == EVENT_TYPE]
        entries = page.get("state") or []
        session = Session(app_name=self.app_name, user_id=self.user_id, id=self.session_id,
                          state={_native_key(item): await self._value(item["payload"])
                                 for item in entries if not item.get("deleted")},
                          events=native_events,
                          last_update_time=max([initialized["created_at"], *(event.timestamp for event in native_events)]))
        session._storage_update_marker = _json(_marker(page)).decode()
        return session

    async def _append(self, *, event_id, kind, role, document, delta, marker, intents=(), outcomes=()):
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
        }
        if intents:
            body["intents"] = intents
        if outcomes:
            body["tool_outcomes"] = outcomes
        return await self.client.request_state("POST", self.path + "/events:append", body=body)

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
        await self.client.request_state("POST", API + "/aux:mutate", body={
            "session_id": self.session_id, "lease_epoch": self.lease_epoch,
            "transition": {"call_id": key, "gateway_operation_id": key, "status": "DISPATCHED"},
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
        if tool.name not in self.effect_tools:
            return None
        if tool is not self.effect_tools[tool.name]:
            raise StateValidationError("INVALID_ARGUMENT", "ADK managed tool differs from the registered instance")
        self._validate_tool_registration(tool)
        self._verify(tool_context.session.app_name, tool_context.session.user_id, tool_context.session.id)
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

    async def recover_tool_calls(self, invocation_id):
        """Reconcile before invoking Runner for a cold resumption."""
        if not self.effect_tools:
            return
        if self.lease_epoch <= 0:
            raise StateValidationError("INVALID_ARGUMENT", "ADK recovery requires a write lease")
        for tool in self.effect_tools.values():
            self._validate_tool_registration(tool)
        session = await self._read()
        if session is None:
            raise StaleSessionError("ADK journal session does not exist")
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
        self._verify(app_name, user_id, session_id)
        async with self.lock:
            before = _marker(await self._page())
            if before["head"]:
                raise StateValidationError("INVALID_ARGUMENT", "ADK session already exists")
            values = state or {}
            await self._append(event_id=INIT, kind="adk.init", role="system",
                               document={"created_at": time.time()},
                               delta={key: value for key, value in values.items() if not _ephemeral(key)},
                               marker=before)
            session = await self._read()
            session.state.update({key: value for key, value in values.items() if _ephemeral(key)})
            return session

    async def get_session(self, *, app_name, user_id, session_id, config=None):
        self._verify(app_name, user_id, session_id)
        session = await self._read()
        if session is not None and config is not None:
            if config.after_timestamp is not None:
                session.events = [event for event in session.events if event.timestamp >= config.after_timestamp]
            if config.num_recent_events is not None:
                limit = config.num_recent_events
                session.events = session.events[-limit:] if limit else []
        return session

    @staticmethod
    def _refresh(target, journal, transient):
        local = {key: value for key, value in target.state.items() if _ephemeral(key)}
        target.state = {**journal.state, **local, **transient}
        target.events = journal.events
        target.last_update_time = journal.last_update_time
        target._storage_update_marker = journal._storage_update_marker

    async def append_event(self, session, event):
        self._verify(session.app_name, session.user_id, session.id)
        if event.partial:
            return event
        if not event.id:
            raise StateValidationError("INVALID_ARGUMENT", "ADK event requires a stable identity")
        stored = event.model_copy(deep=True)
        transient = {key: value for key, value in stored.actions.state_delta.items() if _ephemeral(key)}
        stored.actions.state_delta = {
            key: value for key, value in stored.actions.state_delta.items() if not _ephemeral(key)
        }
        document = stored.model_dump(mode="json")

        async with self.lock:
            latest = await self._read()
            if latest is None:
                raise StaleSessionError("ADK journal session does not exist")
            prior = next((candidate for candidate in latest.events if candidate.id == event.id), None)
            if prior is not None:
                if prior.model_dump(mode="json") != document:
                    raise StateValidationError("KEY_REUSE_MISMATCH", "ADK event ID already has different data")
                self._refresh(session, latest, transient)
                return prior
            if not session._storage_update_marker:
                raise StaleSessionError("ADK session has no journal revision")
            before = json.loads(session._storage_update_marker)
            current = json.loads(latest._storage_update_marker)
            if before["head"] != current["head"] or any(
                before["versions"].get(key, 0) != current["versions"].get(key, 0)
                for key in stored.actions.state_delta
            ):
                raise StaleSessionError("ADK event was built against stale state")
            try:
                intents, outcomes = await self._tool_changes(stored) if self.effect_tools else ([], [])
                role = "tool_call" if intents else "tool_result" if outcomes else (
                    "user" if event.author == "user" else "assistant")
                await self._append(event_id="event:" + wire.encode_id(event.id), kind=EVENT_TYPE,
                                   role=role, document=document, delta=stored.actions.state_delta, marker=before,
                                   intents=intents, outcomes=outcomes)
            except StateAPIError as exc:
                if exc.code == "CONFLICT":
                    raise StaleSessionError("ADK journal changed during event commit") from exc
                raise
            for outcome in outcomes:
                self._receipts.pop(outcome["call_id"], None)
            updated = await self._read()
            if updated is None:
                raise StaleSessionError("ADK session was deleted during event commit")
            self._refresh(session, updated, transient)
            return stored

    async def get_user_state(self, *, app_name, user_id):
        self._verify(app_name, user_id)
        entries = await self.client.request_state("GET", API + "/aux:mutate",
                                                  params={"state_namespace": NAMESPACE})
        return {wire.decode_id(item["key"]): await self._value(item["payload"]) for item in entries or [] if not item.get("deleted")}

    async def list_sessions(self, *, app_name, user_id=None):
        self._verify(app_name, self.user_id if user_id is None else user_id)
        session = await self._read()
        return ListSessionsResponse(sessions=[] if session is None else [
            session.model_copy(update={"events": [], "state": {}}),
        ])

    async def delete_session(self, *, app_name, user_id, session_id):
        self._verify(app_name, user_id, session_id)
        await self.client.request_state("DELETE", self.path)
