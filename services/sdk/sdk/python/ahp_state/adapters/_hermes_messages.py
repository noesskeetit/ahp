"""Native Hermes message repository backed by the compact State API."""

from base64 import b64decode, b64encode
from copy import deepcopy
from dataclasses import asdict, dataclass, fields, replace
from hashlib import sha256
import json
import re
from threading import Lock, RLock
import time
from urllib.parse import quote
from uuid import uuid4

from persistence import RowVisibility
from persistence.conversation.messages import (
    BatchAppendResult,
    CompactionResult,
    ConversationMessage,
    ConversationRequest,
    NewMessage,
    RowDisposition,
    RowOutcome,
    RowStamp,
    TranscriptRead,
    TranscriptRow,
    TranscriptView,
)
from persistence.core.errors import CompressionInProgress, HeldRowRewritten
from session_rules.turns import (
    MODEL_ONLY_DISPLAY_METADATA_KEY,
    coalesce_tool_call_id,
    index_tool_call_uids,
    resolve_tool_call_uid,
    sanitize_context,
    split_user_originated_turn,
)

from ahp_state.client import StateAPIError, StateConflictError, StateNotFoundError
from ahp_state.journal_tools import admission_may_have_committed

EVENT_TYPE = "hermes.message"
CODEC = "hermes-newmessage-json-v1"
COMPACTION_TYPE = "hermes.compaction"
COMPACTION_CODEC = "hermes-compaction-json-v1"
_LEGACY_KV_EVENT = "ahp.state.mutation"
REPLAY_FIELDS = {field.name for field in fields(ConversationMessage) if not field.name.startswith("_")}
TRANSCRIPT_FIELDS = {field.name for field in fields(TranscriptRow)} - {"id", "session_id", "visibility"}
TRANSCRIPT_JSON_FIELDS = ("reasoning_details", "codex_reasoning_items", "codex_message_items",
                          "absorbed_message_uids", "tool_call_uids")
_PROJECTION_ROWS = 1024
_PROJECTION_WIRE_BYTES = 4 << 20
_SESSION_NAMESPACE = "hermes:session"


def encode(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False,
                      sort_keys=True, separators=(",", ":")).encode("ascii")


def session_path(session_id):
    return f"/v1/state/sessions/{quote(session_id, safe='')}"


def session_lineage(request, session_id):
    path = []
    seen = set()
    while session_id:
        if session_id in seen:
            raise ValueError("Hermes session lineage contains a cycle")
        try:
            record = request("GET", session_path(session_id), params={"view": "resume"})
        except StateNotFoundError as error:
            if error.code != "NOT_FOUND":
                raise
            break
        if record.get("harness_type", "hermes") != "hermes":
            raise ValueError("Hermes lineage points to a different harness")
        path.append(session_id)
        seen.add(session_id)
        session_id = record.get("parent_session_id")
    return tuple(reversed(path))


@dataclass(frozen=True)
class _RecordedEffect:
    native_id: str
    tool_name: str
    arguments: dict
    key: str
    args_digest: bytes
    native_uid: str | None

    def intent(self):
        return {"call_id": self.key, "tool_name": self.tool_name, "idempotency_key": self.key,
                "args_digest": list(self.args_digest)}


class JournalMessages:
    """Subset of native MessageRepository bound to one AHP session."""

    def __init__(self, request, session_id, *, lease_epoch=None, holder=None, effect_tools=(),
                 artifacts=None, tool_lease_proof=None, gateway_mode=False):
        self._transport = request
        self.session_id = session_id
        self.lease_epoch = lease_epoch
        self.holder = holder
        self.artifacts = artifacts
        self.tool_lease_proof = tool_lease_proof
        self.gateway_mode = gateway_mode or tool_lease_proof is not None
        self.uncertain = False
        self.effect_tools = frozenset(effect_tools)
        self.tool_calls = None
        self.receipts = {}
        self.dispatch_lock = Lock()
        self._projection_lock = RLock()
        self._projection = None
        self._cache_epoch = lease_epoch
        self._read_epoch = None
        self._lost_lease = False
        self.blocked_call = None
        self.held_after_commit = False
        self.session_route = session_path(session_id)
        self.route = self.session_route + "/events"

    def request(self, method, path, **kwargs):
        try:
            return self._transport(method, path, **kwargs)
        except StateAPIError as error:
            if error.code == "STALE_EPOCH":
                self._lost_lease = True
                self.discard_projection()
            raise

    def discard_projection(self):
        with self._projection_lock:
            self._projection = None
            self.tool_calls = None
            self.receipts.clear()

    def _bound(self, session_id):
        if session_id != self.session_id:
            raise ValueError("Hermes session differs from the AHP host binding")

    def _event_page(self, offset):
        return self.request("GET", self.route, params={"limit": 100, "after_seq": offset})

    def _records(self, offset=0, page=None):
        if page is None:
            page = self._event_page(offset)
        head = page["head_seq"]
        while offset < head:
            if not page["events"]:
                raise ValueError("Hermes journal history is incomplete")
            for row in page["events"]:
                if row["seq"] > head:
                    break
                if row["seq"] != offset + 1:
                    raise ValueError("Hermes journal contains a gap")
                offset = row["seq"]
                if (row["codec"] == "json" and (row["type"], row["role"]) in
                        {("turn.input", "user"), ("turn.completed", "assistant")}):
                    # The dispatcher owns these rows. They count towards the
                    # physical frontier, but do not change the native conversation.
                    yield row["type"], offset, None, "", 0
                    continue
                data = self._load_payload(row["payload"])
                decoded = json.loads(data)
                if row["type"] == EVENT_TYPE and row["codec"] == CODEC:
                    uid = decoded["message_uid"]
                    if row["id"] not in {f"hermes:message:{uid}", f"hermes:compacted:{offset}:{uid}"}:
                        raise ValueError("Hermes journal message identity mismatch")
                elif row["type"] == COMPACTION_TYPE and row["codec"] == COMPACTION_CODEC:
                    if row["id"] != f"hermes:compaction:{offset}" or decoded["base_seq"] != offset - 1:
                        raise ValueError("Hermes journal compaction identity mismatch")
                elif (row["type"] == _LEGACY_KV_EVENT and row["codec"] == "json"
                      and row["role"] == "system" and decoded == {}):
                    pass
                else:
                    raise ValueError("Hermes journal contains an unsupported event")
                yield row["type"], offset, decoded, sha256(data).hexdigest(), len(data)
            if offset < head:
                page = self._event_page(offset)

    def _rows(self):
        for kind, seq, data, digest, _ in self._records():
            if kind == EVENT_TYPE:
                yield seq, data, digest

    def _active_rows(self, retired_rows=None):
        with self._projection_lock:
            if self._cache_epoch != self.lease_epoch:
                self.discard_projection()
                self._cache_epoch = self.lease_epoch
                self._lost_lease = False
            head, active, size = self._projection or (0, {}, 0)
            offset = head if retired_rows is None else 0
            page = self._event_page(offset)
            read_epoch = page.get("lease_epoch")
            if (self._read_epoch is not None and read_epoch != self._read_epoch) or page["head_seq"] < offset:
                self.discard_projection()
                if offset:
                    offset = 0
                    page = self._event_page(0)
            self._read_epoch = page.get("lease_epoch")
            if self.lease_epoch is not None and self._read_epoch not in {None, self.lease_epoch}:
                self._lost_lease = True
            if not offset:
                active, size = {}, 0
            records = self._records(offset, page)
            return self._project(offset, active.copy(), size, records, retired_rows)

    def _project(self, head, active, size, records, retired_rows=None):
        for kind, head, data, digest, length in records:
            if kind == EVENT_TYPE:
                active[head] = (data, digest)
                size += length
            elif kind == COMPACTION_TYPE:
                retired = data["archived"] + data["rewound"]
                if sorted(retired) != list(active):
                    raise ValueError("Hermes compaction does not identify the prior generation")
                if retired_rows is not None:
                    for seq in data["archived"]:
                        retired_rows[seq] = (active[seq][0], RowVisibility.COMPACTION_ARCHIVED)
                    for seq in data["rewound"]:
                        retired_rows[seq] = (active[seq][0], RowVisibility.REWOUND)
                active.clear()
                size = 0
        self._projection = (head, active, size) if (
            not self._lost_lease and len(active) <= _PROJECTION_ROWS and size <= _PROJECTION_WIRE_BYTES
        ) else None
        return head, active

    def _remember_append(self, head, records):
        with self._projection_lock:
            if self._projection is not None and self._projection[0] == head:
                _, active, size = self._projection
                self._project(head, active.copy(), size, records)

    def _prepared_calls(self, data):
        if data["role"] != "assistant":
            return
        uid_index = {}
        index_tool_call_uids(uid_index, {
            "tool_calls": data["tool_calls"], "_tool_call_uids": data["tool_call_uids"],
        })
        for call in data["tool_calls"]:
            function = call.get("function", {})
            name = function.get("name")
            if name not in self.effect_tools:
                continue
            call_id = native_call_id(call["id"])
            args = json.loads(function["arguments"])
            if not isinstance(args, dict):
                raise ValueError("Hermes effect arguments must be a JSON object")
            key = "hermes:tool:" + sha256(encode([data["message_uid"], call_id])).hexdigest()
            uid = resolve_tool_call_uid(uid_index, call["id"])
            yield call_id, _RecordedEffect(call["id"], name, args, key, sha256(encode(args)).digest(), uid)

    def _calls(self):
        if self.tool_calls is None:
            calls = dict(
                pair for _, data, _ in self._rows() for pair in self._prepared_calls(data)
            ) if self.effect_tools else {}
            self._remember_calls(calls)
            return calls
        return self.tool_calls

    def _remember_calls(self, calls):
        self.tool_calls = calls if (not self._lost_lease and len(calls) <= _PROJECTION_ROWS and
            sum(len(encode(call.arguments)) for call in calls.values()) <= _PROJECTION_WIRE_BYTES
        ) else None

    def _read_intent(self, key):
        return self.request("GET", "/v1/state/aux:mutate", params={
            "session_id": self.session_id, "call_id": key,
        })

    def _transition(self, key, status):
        self.request("POST", "/v1/state/aux:mutate", body={
            "session_id": self.session_id, "lease_epoch": self.lease_epoch,
            "transition": {"call_id": key, "gateway_operation_id": key, "status": status},
        })

    def _begin_standalone(self, call):
        path = self.session_route + "/tools:begin-standalone"
        body = {
            "lease_epoch": self.lease_epoch, "call_id": call.key, "tool_name": call.tool_name,
            "args_digest": list(call.args_digest),
        }
        try:
            return self.request("POST", path, body=body)
        except (StateAPIError, json.JSONDecodeError) as error:
            if not admission_may_have_committed(error):
                raise
            return self.request("POST", path, body={**body, "ack_lost": True})

    def dispatch_effect(self, call_id, name, args, execute):
        """execute receives the durable operation key and returns its authoritative receipt."""
        call = self._calls().get(native_call_id(call_id))
        if not call or call.tool_name != name or call.args_digest != sha256(encode(args)).digest():
            raise ValueError("Hermes effect differs from its durable intent")
        key = call.key
        with self.dispatch_lock:
            if self.blocked_call is not None:
                raise HermesReconciliationRequired(self.blocked_call)
            if self.gateway_mode:
                if self._read_intent(key)["status"] != "PREPARED":
                    raise HermesReconciliationRequired(call_id)
                self._transition(key, "DISPATCHED")
            elif not self._begin_standalone(call)["can_execute"]:
                raise HermesReconciliationRequired(call_id)
        try:
            receipt = execute(key)
        except Exception:
            with self.dispatch_lock:
                self.blocked_call = native_call_id(call_id)
            raise
        self.receipts[key] = (receipt, None)
        return receipt

    def assert_resumable(self):
        """Check authoritative tool outcomes before allowing a restored turn into the model."""
        for call_id, call in self._calls().items():
            key = call.key
            status = self._read_intent(key)["status"]
            if status == "COMPLETED":
                continue
            if not self.gateway_mode:
                self._begin_standalone(call)
            elif status == "DISPATCHED":
                self._transition(key, "OUTCOME_UNKNOWN")
            raise HermesReconciliationRequired(call_id)

    def recover_gateway_effects(self, gateway):
        """Settle durable tool messages before entering the native model loop.

        The host passes an authenticated Tool Gateway request function with
        the restored context. It retries HermesGatewayPending while it owns the
        lease. The fenced path waits for explicit SAFE_TO_RETRY before sending.
        """
        if self.lease_epoch is None:
            raise ValueError("Hermes recovery requires the AHP host's write lease")
        for call_id, effect in self._calls().items():
            intent = self._read_intent(effect.key)
            status = intent["status"]
            if status == "COMPLETED":
                continue
            if status not in {"PREPARED", "DISPATCHED"}:
                raise HermesReconciliationRequired(call_id)
            if status == "DISPATCHED" and intent["gateway_operation_id"] != effect.key:
                raise ValueError("Hermes journal and Gateway operation identity differ")
            result = self._gateway_result(gateway, effect, status, allow_missing=True)
            if result is None:
                self._transition(effect.key, "DISPATCHED")
                status = "DISPATCHED"
                try:
                    gateway("Call", {"key": effect.key, "tool": effect.tool_name, "arguments": effect.arguments,
                                     **self.tool_authority()})
                except StateAPIError as error:
                    if error.code not in {"TIMEOUT", "TRANSPORT"}:
                        raise
                result = self._gateway_result(gateway, effect, status, allow_missing=False)
            if status == "PREPARED":
                self._transition(effect.key, "DISPATCHED")
            self.receipts[effect.key] = (result["payload"], result.get("journal_blob"))
            self._append_gateway_tool(effect, result["payload"])

    def _gateway_result(self, gateway, effect, status, *, allow_missing):
        if self.tool_lease_proof is not None:
            result = self._fenced_receipt(gateway, effect, status, allow_missing)
        else:
            result = self._legacy_receipt(gateway, effect, status, allow_missing)
        if result is None:
            return None
        if (result["key"] != effect.key or result["target"] != effect.tool_name
                or result["status"] != "SUCCEEDED"):
            raise ValueError("Tool Gateway returned another Hermes receipt")
        if result["result_storage"] == "inline" and "payload" in result:
            return result
        if result["result_storage"] == "artifact" and self.artifacts is not None:
            ref = result["artifact_ref"]
            blob = {"artifact_id": ref["artifact_id"], "version": ref["version"],
                    "sha256": ref["digest"], "size_bytes": ref["size"]}
            return {**result, "payload": json.loads(self._load_payload({"blob": blob})), "journal_blob": blob}
        raise ValueError("Tool Gateway returned an unsupported Hermes receipt")

    def tool_authority(self):
        if self.tool_lease_proof is None:
            return {}
        return {"session_id": self.session_id, "lease_epoch": self.lease_epoch,
                "tool_lease_proof": self.tool_lease_proof}

    def _fenced_receipt(self, gateway, effect, status, allow_missing):
        call_id = native_call_id(effect.native_id)
        try:
            result = gateway("FenceAndLookup", {"key": effect.key, **self.tool_authority()})
        except StateNotFoundError as error:
            if allow_missing and error.code == "SAFE_TO_RETRY":
                return None
            raise
        except StateConflictError as error:
            if error.code != "OUTCOME_UNKNOWN":
                raise
            self._record_unknown(effect, status)
            raise HermesReconciliationRequired(call_id) from error
        if result["status"] == "IN_FLIGHT":
            raise HermesGatewayPending(call_id)
        if result["status"] != "COMPLETED":
            raise ValueError("unexpected Tool Gateway lookup status")
        return result["receipt"]

    def _legacy_receipt(self, gateway, effect, status, allow_missing):
        try:
            result = gateway("GetResult", {"key": effect.key})
        except StateNotFoundError as error:
            if allow_missing and error.code == "NOT_FOUND":
                return None
            raise
        except StateConflictError:
            operation = gateway("Get", {"key": effect.key})
            if operation["key"] != effect.key or operation["target"] != effect.tool_name:
                raise ValueError("Tool Gateway returned another operation")
            actual = operation["status"]
            call_id = native_call_id(effect.native_id)
            if actual in {"ACCEPTED", "DISPATCHED", "SUCCEEDED"}:
                raise HermesGatewayPending(call_id)
            if actual == "SIDE_EFFECT_UNKNOWN":
                self._record_unknown(effect, status)
            raise HermesReconciliationRequired(call_id)
        return result

    def _record_unknown(self, effect, status):
        if status == "PREPARED":
            self._transition(effect.key, "DISPATCHED")
        self._append_gateway_tool(effect, {"code": "TOOL_OUTCOME_UNKNOWN"}, unknown=True)

    def _append_gateway_tool(self, effect, payload, *, unknown=False):
        message = NewMessage(role="tool", content=encode(payload).decode("ascii"),
                             tool_name=effect.tool_name, tool_call_id=effect.native_id,
                             tool_call_uid=effect.native_uid, effect_disposition="unknown" if unknown else None)
        self.append_batch(self.session_id, [message], turn_lease_holder=self.holder)

    def append_batch(self, session_id, messages, *, turn_lease_holder=None, chunk_rows=None,
                     turn_lease_ttl_seconds=300.0):
        self._bound(session_id)
        if self.lease_epoch is None or turn_lease_holder != self.holder:
            raise ValueError("Hermes append requires the AHP host's write lease")
        if self.uncertain:
            raise ValueError("Hermes append acknowledgement is unresolved; reopen from durable state")
        if self.held_after_commit:
            raise HermesReconciliationRequired(self.blocked_call)
        if chunk_rows is not None:
            raise NotImplementedError("Hermes chunk progress is outside this append-only slice")
        if not messages:
            return BatchAppendResult(())
        if any(message.repair_target or message.db_row_snapshot or message.restored_from_store
               for message in messages):
            raise NotImplementedError("Hermes row mutation requires explicit journal operations")

        head, _ = self._active_rows()
        events = []
        records = []
        outcomes = []
        previous_calls = self._calls()
        calls = previous_calls.copy()
        intents = []
        tool_outcomes = []
        hold_after_commit = None
        for index, message in enumerate(messages, start=1):
            data = asdict(message)
            data["message_uid"] = data["message_uid"] or uuid4().hex
            if data["timestamp"] is None:
                data["timestamp"] = time.time()
            wire = encode(data)
            digest = sha256(wire).hexdigest()
            records.append((EVENT_TYPE, head + index, json.loads(wire), digest, len(wire)))
            events.append(self._event(f"hermes:message:{data['message_uid']}",
                                             EVENT_TYPE, data["role"], CODEC, wire, data.get("content")))
            for call_id, intent in self._prepared_calls(data):
                if call_id in calls:
                    raise ValueError("Hermes reused a recorded effect call ID")
                calls[call_id] = intent
                intents.append(intent.intent())
            if message.role == "tool" and message.tool_call_id:
                call = previous_calls.get(native_call_id(message.tool_call_id))
                if call and call.tool_name == message.tool_name:
                    key = call.key
                    recorded = self._read_intent(key)
                    if recorded["status"] == "DISPATCHED":
                        outcome = {"call_id": key, "gateway_operation_id": recorded["gateway_operation_id"],
                                   "status": "OUTCOME_UNKNOWN"}
                        if key in self.receipts and message.effect_disposition != "unknown":
                            outcome["status"] = "COMPLETED"
                            receipt, artifact = self.receipts[key]
                            outcome["result"] = ({"blob": artifact} if artifact is not None
                                                 else self._store_payload(encode(receipt)))
                        else:
                            hold_after_commit = message.tool_call_id
                        tool_outcomes.append(outcome)
            outcomes.append(RowOutcome(
                id=head + index, timestamp=data["timestamp"], db_row_snapshot=sha256(wire).hexdigest(),
                disposition=RowDisposition.APPENDED, message_uid=data["message_uid"],
                tool_call_uids=message.tool_call_uids, tool_call_uid=message.tool_call_uid,
            ))
        body = {"lease_epoch": self.lease_epoch, "expected_seq": head, "events": events,
                "key": "hermes:batch:" + sha256(encode(events)).hexdigest()}
        if intents:
            body["intents"] = intents
        if tool_outcomes:
            body["tool_outcomes"] = tool_outcomes
        self._append_events(body)
        self._remember_append(head, records)
        self._remember_calls(calls)
        for outcome in tool_outcomes:
            self.receipts.pop(outcome["call_id"], None)
        if hold_after_commit is not None:
            self.blocked_call = hold_after_commit
            self.held_after_commit = True
            # Hermes stops the native turn when its incremental persistence
            # barrier raises. The intent and tool row are already durable.
            raise HermesReconciliationRequired(hold_after_commit)
        return BatchAppendResult(tuple(outcomes))

    def _append_events(self, body):
        try:
            answer = self.request("POST", self.route + ":append", body=body)
        except StateAPIError as error:
            if error.code not in {"TIMEOUT", "TRANSPORT"}:
                raise
            self.uncertain = True
            answer = self.request("POST", self.route + ":append", body=body)
        head = body["expected_seq"]
        if (answer["first_seq"], answer["head_seq"]) != (head + 1, head + len(body["events"])):
            self.uncertain = True
            raise ValueError("Hermes journal append receipt does not cover the batch")
        self.uncertain = False

    def compact(self, session_id, replacement, *, watermark=None, carried_tail_count=0,
                lock_holder=None, model_config_patch=None, carried=(), coverage=None, held_for_cas=None):
        """Atomically archive an explicit generation and preserve concurrent appends."""
        self._bound(session_id)
        if self.lease_epoch is None or (lock_holder is not None and lock_holder != self.holder):
            raise CompressionInProgress(session_id)
        if self.uncertain:
            raise ValueError("Hermes append acknowledgement is unresolved; reopen from durable state")
        head, active = self._active_rows()
        self.tool_calls = None
        effects = self._calls()
        for call_id, call in effects.items():
            if self._read_intent(call.key)["status"] != "COMPLETED":
                raise HermesReconciliationRequired(call_id)
        by_uid = {data["message_uid"]: seq for seq, (data, _) in active.items()}

        def held_id(message, row_id, *, allow_rewrite=False):
            seq = row_id if row_id is not None else by_uid.get(message.message_uid)
            stored = active.get(seq)
            if stored is None:
                if row_id is None:
                    return None
                raise HeldRowRewritten(session_id)
            data, digest = stored
            if ((message.message_uid and message.message_uid != data["message_uid"])
                    or (message.db_row_snapshot and message.db_row_snapshot != digest)):
                raise HeldRowRewritten(session_id)
            if (message.role != data["role"] or message.tool_call_id != data["tool_call_id"]
                    or list(message.tool_calls) != data["tool_calls"]
                    or (message.content != data["content"] and not (allow_rewrite and message.db_row_snapshot == digest))):
                raise HeldRowRewritten(session_id)
            return seq

        held_versions = {}
        for message in held_for_cas or ():
            seq = held_id(message, message.row_id)
            if seq is None:
                raise HeldRowRewritten(session_id)
            held_versions[seq] = message.db_row_snapshot
        if coverage is None:
            boundary = head if watermark is None else int(watermark.last_active or 0)
            covered = {seq for seq in active if seq <= boundary}
        else:
            covered = set(coverage.covered_ids)
            for message in coverage.unresolved:
                seq = held_id(message, message.row_id)
                if seq is None:
                    raise HeldRowRewritten(session_id)
                covered.add(seq)
            if not covered <= active.keys():
                raise HeldRowRewritten(session_id)
        rewound = set(active) - covered
        for message in carried:
            seq = held_id(message, message.row_id)
            if seq is None:
                raise HeldRowRewritten(session_id)
            rewound.add(seq)

        replacement_rows = []
        claimed = set()
        rewritten_unseen = set()
        merged_sources = set()
        for message in replacement:
            seq = held_id(message, message.repair_target, allow_rewrite=True)
            if message.absorbed_row_ids:
                sources = (seq, *message.absorbed_row_ids)
                if seq is None or claimed.intersection(sources):
                    raise HeldRowRewritten(session_id)
                data = self._merged_text_carry(session_id, message, sources, active, covered, held_versions)
                claimed.update(sources)
                merged_sources.update(sources)
                replacement_rows.append((seq, data))
                continue
            if seq is not None:
                if seq in claimed:
                    raise ValueError("Hermes compaction carries one native row twice")
                claimed.add(seq)
                data, digest = active[seq]
                if message.content != data["content"]:
                    data = self._rewritten_text_carry(session_id, message, data, digest)
                    if seq not in covered:
                        rewritten_unseen.add(seq)
                replacement_rows.append((seq, data))
            else:
                data = asdict(message)
                if list(self._prepared_calls(data)):
                    raise ValueError("Hermes compaction cannot introduce a new external action")
                if (message.role == "tool" and message.tool_call_id
                        and native_call_id(message.tool_call_id) in effects):
                    raise ValueError("Hermes compaction cannot manufacture an effect result")
                data["message_uid"] = data["message_uid"] or uuid4().hex
                data["timestamp"] = time.time() if data["timestamp"] is None else data["timestamp"]
                replacement_rows.append((None, data))
        if carried_tail_count < 0 or carried_tail_count > len(replacement_rows):
            raise ValueError("invalid Hermes carried tail length")
        if carried_tail_count and any(seq is None for seq, _ in replacement_rows[-carried_tail_count:]):
            raise NotImplementedError("Hermes compaction needs durable identities for carried messages")

        # Archive the former generation, then publish the replacement and
        # any input that arrived while the compressor was running.
        concurrent = [data for seq, (data, _) in active.items() if seq not in covered and seq not in claimed]
        rewound.update(claimed)
        rewound.difference_update(rewritten_unseen)
        if merged_sources:
            # The native compactor classifies the covered originals using the
            # slots in the retained tail. The merged row still occupies one slot.
            tail = replacement_rows[-carried_tail_count:] if carried_tail_count else ()
            tail_below = carried_tail_count - sum(seq not in covered for seq, _ in tail)
            eligible = [seq for seq in active if seq in covered]
            tail_originals = set(eligible[-tail_below:]) if tail_below else set()
            rewound.difference_update(merged_sources - tail_originals)
        marker = {"base_seq": head, "archived": sorted(set(active) - rewound), "rewound": sorted(rewound)}
        wire = encode(marker)
        events = [self._event(f"hermes:compaction:{head + 1}", COMPACTION_TYPE,
                                     "compaction", COMPACTION_CODEC, wire)]
        records = [(COMPACTION_TYPE, head + 1, marker, sha256(wire).hexdigest(), len(wire))]
        stamps = []
        rows = [data for _, data in replacement_rows] + concurrent
        for index, data in enumerate(rows, start=2):
            seq = head + index
            wire = encode(data)
            uid = data["message_uid"]
            digest = sha256(wire).hexdigest()
            records.append((EVENT_TYPE, seq, json.loads(wire), digest, len(wire)))
            events.append(self._event(f"hermes:compacted:{seq}:{uid}", EVENT_TYPE,
                                      data["role"], CODEC, wire, data.get("content")))
            if index - 2 < len(replacement_rows):
                stamps.append(RowStamp(id=seq, timestamp=data["timestamp"], db_row_snapshot=digest,
                                       message_uid=uid, tool_call_uids=data["tool_call_uids"],
                                       tool_call_uid=data["tool_call_uid"]))
        patches = [{"seq": seq, "is_active": False, "is_compacted": seq not in rewound}
                   for seq in active]
        body = {"lease_epoch": self.lease_epoch, "expected_seq": head, "events": events,
                "event_patches": patches}
        if model_config_patch:
            body["kv"] = [self._model_config_change(model_config_patch)]
        body["key"] = "hermes:compact:" + sha256(encode(body)).hexdigest()
        self._append_events(body)
        self._remember_append(head, records)
        return CompactionResult(active_count=len(rows), rows=tuple(stamps))

    def _read_session_value(self, key):
        rows = self.request("GET", "/v1/state/kv", params={
            "session_id": self.session_id, "scope_type": "session", "namespace": _SESSION_NAMESPACE,
            "key": key, "limit": 1,
        })
        if not rows:
            return 0, {}
        row = rows[0]
        value = {} if row.get("deleted") else json.loads(self._load_payload(row["payload"]))
        if not isinstance(value, dict):
            raise ValueError(f"Hermes session {key} is not a JSON object")
        return row["version"], value

    def _session_value_change(self, key, version, value):
        return {"scope_type": "session", "namespace": _SESSION_NAMESPACE, "key": key,
                "expected_version": version, "payload": self._store_payload(encode(value))}

    def _model_config_change(self, patch):
        version, config = self._read_session_value("model_config")
        for key, value in patch.items():
            if value is None:
                config.pop(key, None)
            else:
                config[key] = value
        return self._session_value_change("model_config", version, config)

    def _session_mutation(self, *, changes=(), catalog=None):
        if self.lease_epoch is None:
            raise ValueError("Hermes session writes require the AHP host's write lease")
        body = {"session_id": self.session_id, "lease_epoch": self.lease_epoch, "kv": list(changes)}
        if catalog:
            body["catalog"] = catalog
        body["key"] = "hermes:session:" + sha256(encode(body)).hexdigest()
        try:
            self.request("POST", "/v1/state/kv:mutate", body=body)
        except StateAPIError as error:
            if not admission_may_have_committed(error):
                raise
            self.request("POST", "/v1/state/kv:mutate", body=body)

    @staticmethod
    def _rewritten_text_carry(session_id, message, stored, digest):
        # A current stamp and the complete original text prove this is the
        # compressor's wrapper around a particular row, even when text repeats.
        original = stored["content"]
        if (message.message_uid != stored["message_uid"] or message.db_row_snapshot != digest
                or not isinstance(original, str) or not original.strip() or not isinstance(message.content, str)
                or message.role not in {"user", "assistant"} or message.tool_calls or message.tool_call_id
                or message.absorbed_message_uids != tuple(stored["absorbed_message_uids"])
                or message.api_content not in (None, stored["api_content"])):
            raise HeldRowRewritten(session_id)
        block = r"(?m)^[^\S\n]*" + re.escape(original.strip()) + r"[^\S\n]*$"
        if not re.search(block, message.content):
            raise HeldRowRewritten(session_id)
        return {**stored, "content": message.content, "compressed_summary": message.compressed_summary,
                "api_content": message.api_content}

    @staticmethod
    def _merged_text_carry(session_id, message, sources, active, covered, held_versions):
        first = sources[0]
        ordered = list(active)
        start = ordered.index(first)
        if (list(sources) != ordered[start:start + len(sources)] or not set(sources) <= covered
                or any(held_versions.get(seq) != active[seq][1] for seq in sources)):
            raise HeldRowRewritten(session_id)
        originals = [active[seq][0] for seq in sources]
        stored = originals[0]
        if (message.role != "user" or message.message_uid != stored["message_uid"]
                or message.db_row_snapshot != active[first][1] or message.api_content is not None
                or any(row["role"] != "user" or not isinstance(row["content"], str)
                       or row["tool_calls"] or row["tool_call_id"] or row["display_kind"]
                       or row["observed"] != stored["observed"] for row in originals)):
            raise HeldRowRewritten(session_id)
        text = "\n\n".join(row["content"] for row in originals if row["content"])
        uids = list(stored["absorbed_message_uids"])
        for absorbed in originals[1:]:
            uids.extend([absorbed["message_uid"], *absorbed["absorbed_message_uids"]])
        uids = tuple(uid for uid in dict.fromkeys(uids) if uid != stored["message_uid"])
        if message.content != text or message.absorbed_message_uids != uids:
            raise HeldRowRewritten(session_id)
        return {**stored, "content": text, "compressed_summary": message.compressed_summary,
                "api_content": None, "absorbed_message_uids": uids}

    def _store_payload(self, wire):
        if len(wire) <= 256 << 10:
            return {"inline": b64encode(wire).decode("ascii")}
        if self.artifacts is None:
            return {"blob": self.request("POST", self.session_route + "/blobs:stage", body={
                "lease_epoch": self.lease_epoch, "data": b64encode(wire).decode("ascii"),
            })}
        digest = sha256(wire).hexdigest()
        key = "hermes:" + sha256(encode([self.session_id, self.lease_epoch, digest])).hexdigest()
        ref = self.artifacts.put(key, "hermes-state.bin", "application/octet-stream", wire, journal_stage=True)
        if (ref["context_id"] != self.session_id or ref["sha256"] != digest or
                ref["size"] != len(wire) or not ref["version"]):
            raise ValueError("Hermes artifact publication detached from its session or bytes")
        return {"blob": {"artifact_id": ref["id"], "version": ref["version"],
                         "sha256": digest, "size_bytes": len(wire)}}

    def _load_payload(self, payload):
        if payload.get("inline") is not None:
            return b64decode(payload["inline"], validate=True)
        blob = payload.get("blob")
        if blob is None:
            raise ValueError("Hermes journal blob is missing")
        if self.artifacts is None:
            reply = self.request("POST", self.session_route + "/blobs:read", body=blob)
            raw = b64decode(reply["data"], validate=True)
            if len(raw) != blob["size_bytes"] or sha256(raw).hexdigest() != blob["sha256"]:
                raise ValueError("Hermes journal blob does not match its descriptor")
            return raw
        artifact_id = blob["artifact_id"]
        metadata = self.artifacts.get(artifact_id)
        if (metadata["id"] != artifact_id or metadata["context_id"] != self.session_id or
                metadata["version"] != blob["version"] or metadata["size"] != blob["size_bytes"] or
                metadata["sha256"] != blob["sha256"]):
            raise ValueError("Hermes artifact no longer matches its committed version")
        raw = self.artifacts.download(artifact_id)
        if len(raw) != blob["size_bytes"] or sha256(raw).hexdigest() != blob["sha256"]:
            raise ValueError("Hermes artifact bytes do not match its committed digest")
        return raw

    def _event(self, event_id, kind, role, codec, wire, content=None):
        event = {"id": event_id, "type": kind, "role": role, "codec": codec,
                 "payload": self._store_payload(wire)}
        if isinstance(content, str) and role in {"user", "assistant", "compaction"}:
            event["content_text"] = content.replace("\0", "").encode()[:32768].decode(errors="ignore")
        return event

    def append(self, session_id, message, *, turn_lease_holder=None, turn_lease_ttl_seconds=300.0):
        outcome = self.append_batch(session_id, [message], turn_lease_holder=turn_lease_holder,
                                    turn_lease_ttl_seconds=turn_lease_ttl_seconds)
        return outcome.outcomes[0].id

    def read_transcript(self, session_id, request=TranscriptRead()):
        self._bound(session_id)
        if request.include_lineage:
            unpaged = TranscriptRead(view=request.view)
            transcript = [row for repository in self._lineage_repositories()
                          for row in repository.read_transcript(repository.session_id, unpaged)]
            if request.view is TranscriptView.DISPLAY:
                transcript = self._display_rows(transcript)
            return self._transcript_page(transcript, request)
        retired = None if request.view is TranscriptView.ACTIVE else {}
        try:
            _, active = self._active_rows(retired)
        except StateNotFoundError as error:
            if error.code != "NOT_FOUND":
                raise
            return ()
        rows = {seq: (data, RowVisibility.ACTIVE) for seq, (data, _) in active.items()}
        if retired is not None:
            rows.update(retired)
        transcript = [self._transcript_row(seq, rows[seq]) for seq in sorted(rows)
                      if request.after_id is None or seq > request.after_id]
        if request.view is TranscriptView.DISPLAY:
            transcript = self._display_rows(transcript)
        return self._transcript_page(transcript, request)

    @staticmethod
    def _transcript_page(transcript, request):
        limit = request.limit if request.limit is None or request.limit >= 0 else None
        offset = max(0, request.offset)
        if request.latest:
            return tuple(transcript[::-1][offset:][:limit][::-1])
        return tuple(transcript[offset:][:limit])

    def display_count(self, session_id):
        return len(self.read_transcript(session_id, TranscriptRead(view=TranscriptView.DISPLAY)))

    def _transcript_row(self, seq, value):
        data, visibility = value
        native = {key: data[key] for key in TRANSCRIPT_FIELDS if key in data}
        native["tool_calls"] = data["tool_calls"] or None
        for key in TRANSCRIPT_JSON_FIELDS:
            native[key] = json.dumps(data[key]) if data.get(key) else None
        return TranscriptRow(id=seq, session_id=self.session_id, visibility=visibility, **deepcopy(native))

    @staticmethod
    def _display_key(row):
        if row.role == "assistant" and row.tool_calls:
            call_ids = tuple(coalesce_tool_call_id(call) for call in row.tool_calls)
            if all(call_ids):
                return (row.role, None, row.timestamp, row.tool_call_id, call_ids, row.tool_name)
        content = row.content
        if row.role == "user":
            handoff, live = split_user_originated_turn({
                "role": row.role, "content": content, "display_kind": row.display_kind,
                "display_metadata": row.display_metadata,
            })
            if handoff is not None and live is not None:
                content = live.get("content")
        return (row.role, encode(content), row.timestamp, row.tool_call_id, encode(row.tool_calls), row.tool_name)

    @classmethod
    def _display_rows(cls, transcript):
        first_by_uid = {}
        order = {}
        placed = []
        archived_tools = {}
        for row in transcript:
            if row.visibility is RowVisibility.REWOUND:
                continue
            key = cls._display_key(row)
            if row.message_uid:
                if row.compressed_summary:
                    key = first_by_uid.get(row.message_uid, key)
                else:
                    first_by_uid.setdefault(row.message_uid, key)
            slot = order.setdefault(key, row.id)
            if (row.display_metadata or {}).get(MODEL_ONLY_DISPLAY_METADATA_KEY):
                continue
            placed.append((key, row))
            if row.role == "tool" and row.message_uid and row.visibility is RowVisibility.COMPACTION_ARCHIVED:
                identity = (row.message_uid, row.timestamp)
                archived_tools[identity] = min(archived_tools.get(identity, slot), slot)

        representatives = {}
        for key, row in placed:
            slot = order[key]
            if row.role == "tool" and archived_tools.get((row.message_uid, row.timestamp), slot) < slot:
                continue
            previous = representatives.get(key)
            if (previous is None or row.visibility is RowVisibility.ACTIVE
                    or previous.visibility is not RowVisibility.ACTIVE):
                representatives[key] = row
        return [representatives[key] for key in order if key in representatives]

    def read_conversation(self, session_id, request=ConversationRequest()):
        self._bound(session_id)
        if request.repair_alternation:
            raise NotImplementedError("Hermes alternation repair requires native row mutation")
        if request.view is TranscriptView.DISPLAY:
            displayed = self.read_transcript(session_id, TranscriptRead(
                view=TranscriptView.DISPLAY, include_lineage=request.include_lineage,
            ))
            return self._display_conversation(displayed, include_row_ids=request.include_row_ids)
        if request.include_lineage:
            local = replace(request, include_lineage=False)
            return tuple(row for repository in self._lineage_repositories()
                         for row in repository.read_conversation(repository.session_id, local))
        result = []
        rows = self._rows() if request.view is TranscriptView.ALL else (
            (seq, data, digest) for seq, (data, digest) in self._active_rows()[1].items()
        )
        for seq, data, digest in rows:
            native = {key: data[key] for key in REPLAY_FIELDS if key in data}
            native["message_id"] = data["platform_message_id"]
            native["tool_calls"] = tuple(data["tool_calls"]) if data["tool_calls"] else None
            native["absorbed_message_uids"] = tuple(data["absorbed_message_uids"])
            native["data_use"] = tuple(data["data_use"])
            native["db_row_snapshot"] = digest
            native["row_id"] = seq if request.include_row_ids else None
            result.append(ConversationMessage(**deepcopy(native)))
        return tuple(result)

    def _lineage_repositories(self):
        for session_id in session_lineage(self.request, self.session_id):
            yield self if session_id == self.session_id else JournalMessages(
                self.request, session_id, artifacts=self.artifacts,
            )

    @staticmethod
    def _display_conversation(rows, *, include_row_ids):
        messages = []
        tool_call_uids = {}
        for row in rows:
            native = {key: getattr(row, key) for key in REPLAY_FIELDS if hasattr(row, key)}
            for key in TRANSCRIPT_JSON_FIELDS:
                encoded = getattr(row, key)
                native[key] = json.loads(encoded) if encoded else None
            native["absorbed_message_uids"] = tuple(native["absorbed_message_uids"] or ())
            native["message_id"] = row.platform_message_id
            native["row_id"] = row.id if include_row_ids else None
            native["compressed_summary"] = False
            native["tool_calls"] = tuple(row.tool_calls) if row.tool_calls else None
            native["_tool_call_uid_stored"] = bool(row.tool_call_uid)
            native["data_use"] = tuple((row.display_metadata or {}).get("data_use") or ())
            if row.role in {"user", "assistant"} and isinstance(row.content, str):
                native["content"] = sanitize_context(row.content).strip()
            if row.role == "user":
                tool_call_uids.clear()
            elif row.role == "assistant" and row.tool_calls:
                index_tool_call_uids(tool_call_uids, {
                    "tool_calls": row.tool_calls, "_tool_call_uids": native["tool_call_uids"],
                })
            elif row.role == "tool" and row.tool_call_id and not row.tool_call_uid:
                native["tool_call_uid"] = resolve_tool_call_uid(tool_call_uids, row.tool_call_id)
            messages.append(ConversationMessage(**native))
        return tuple(messages)


def native_call_id(call_id):
    return call_id.split("|", 1)[0].strip()


class HermesReconciliationRequired(RuntimeError):
    def __init__(self, call_id):
        self.call_id = call_id
        super().__init__(f"Hermes effect {call_id} requires reconciliation")


class HermesGatewayPending(RuntimeError):
    def __init__(self, call_id):
        self.call_id = call_id
        super().__init__(f"Hermes effect {call_id} is still pending at Tool Gateway")
