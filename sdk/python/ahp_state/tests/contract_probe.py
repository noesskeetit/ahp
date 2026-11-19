"""One probe matrix, two transports: the double and the live State server.

The double ``fake_state.py`` proved block 1 and was caught wrong twice, so
"the double says X" stopped being evidence. This module asks the *same*
questions of both - identical JSON bodies over the same wire - and records
what each one actually answered: HTTP-level outcome, error code, and the
response envelope. The difference is the divergence table the report needs,
and ``test_double_matches_live_baseline`` pins the live answers so the double
cannot drift again without a red test.

A probe never asserts. It observes; the tests compare.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable


class ProbeError(Exception):
    """Normalized transport error: the wire code and message, nothing else.

    ``code`` is the DOMAIN code the SDK would report - ``reason`` when the
    platform flattened one onto a coarser HTTP class, exactly as
    ``ahp_state.client._raise_typed`` resolves it. Both harnesses apply that
    rule, or the table would compare the double's raw wire against the live
    stand through the SDK and call the difference a divergence.
    """

    def __init__(self, code: str, message: str = "", http_class: str = ""):
        self.code = code
        self.message = message
        self.http_class = http_class or code
        super().__init__(f"{code}: {message}")


Call = Callable[[str, dict, str | None], dict]


@dataclass
class Harness:
    """A transport plus the fixture ids one probe needs."""

    name: str
    call: Call
    probes: dict[str, Any] = field(default_factory=dict)

    # -- fixture construction, in wire calls only ------------------------

    def thread(self, tag: str) -> str:
        reply = self.call("CreateThread",
                          {"context_id": tag, "key": f"thread:{tag}"}, None)
        return _id(reply)

    def bootstrap(self, tag: str) -> dict:
        """Fresh thread + run + claimed lease, all through the wire."""
        tid = self.thread(tag)
        self.call("CreateRun", {"thread_id": tid, "key": f"run:{tag}",
                                "payload": {"context_id": tag}}, None)
        claim = self.call("ClaimRun", {"worker": f"w-{tag}", "ttl_ms": 120000}, None)
        lease = claim.get("lease") or {}
        if not lease:
            raise ProbeError("PROBE", f"{self.name}: ClaimRun returned no lease for {tag}")
        return {"thread_id": tid, "run_id": _id(lease["run"]),
                "fence": {"run_id": _id(lease["run"]), "worker": lease["worker"],
                          "epoch": lease["epoch"]},
                "token": claim.get("task_token") or ""}


def _id(reply: dict) -> str:
    """Thread/run identity, FLAT or wrapped - the probe must survive both."""
    if "id" in reply:
        return reply["id"]
    for wrapper in ("thread", "run", "intent", "item", "lease"):
        inner = reply.get(wrapper)
        if isinstance(inner, dict) and "id" in inner:
            return inner["id"]
    raise ProbeError("PROBE", f"no id in {sorted(reply)}")


def _ident(reply: dict) -> str:
    """Identity of an intent row: the two sides disagree on whether one exists.

    Live State answers ``RecordIntent`` with an ``id``; the double answers with
    the same row minus it. The probe compares what BOTH can show - the
    ``(run, node, call)`` tuple the journal is keyed on - and the digest, which
    is where a replay is actually proven.
    """
    if "id" in reply:
        return f"id={reply['id']}"
    return "no-id " + json.dumps([reply.get("run_id"), reply.get("node"),
                                  reply.get("call")])


def _keys(reply: dict) -> str:
    """Envelope signature: top-level keys, plus the wrapped body's keys."""
    for wrapper in ("thread", "run"):
        inner = reply.get(wrapper)
        if isinstance(inner, dict):
            return f"{{{wrapper}}}+{sorted(inner)}"
    return f"FLAT+{sorted(reply)}"


def _item(reply: dict) -> dict:
    """Unwrap ``{"item": {...}}`` if a server wraps it; live State does not."""
    inner = reply.get("item")
    return inner if isinstance(inner, dict) else reply


def _observe(call: Call, op: str, body: dict, token: str | None = None) -> str:
    try:
        reply = call(op, body, token)
    except ProbeError as exc:
        return f"ERR {exc.code}"
    except Exception as exc:  # transport-level truth is part of the contract
        return f"ERR {type(exc).__name__}"
    return _keys(reply)


def _ask(call: Call, op: str, body: dict, token: str | None = None):
    """The reply itself, or the error - for probes that must read fields."""
    try:
        return call(op, body, token), None
    except ProbeError as exc:
        return None, exc
    except Exception as exc:  # noqa: BLE001 - normalized, never propagated
        return None, ProbeError(type(exc).__name__, str(exc))


# --- probes -----------------------------------------------------------------
# Each probe returns {label: observation}. Labels are compared verbatim.

def probe_envelopes(h: Harness) -> dict[str, str]:
    tag = f"env-{uuid.uuid4().hex[:10]}"
    thread = h.call("CreateThread", {"context_id": tag, "key": f"thread:{tag}"}, None)
    tid = _id(thread)
    run = h.call("CreateRun", {"thread_id": tid, "key": f"run:{tag}",
                               "payload": {"context_id": tag}}, None)
    claim = h.call("ClaimRun", {"worker": f"w-{tag}", "ttl_ms": 120000}, None)
    lease = claim.get("lease") or {}
    return {
        "CreateThread envelope": _keys(thread),
        "CreateRun envelope": _keys(run),
        "ClaimRun lease keys": str(sorted(lease)) if lease else "no lease",
        "ClaimRun run keys": str(sorted(lease["run"])) if lease else "no lease",
    }


def probe_write_authorization(h: Harness) -> dict[str, str]:
    tag = f"auth-{uuid.uuid4().hex[:10]}"
    b = h.bootstrap(tag)
    f = b["fence"]
    ns = ["probe", tag]
    return {
        "write by wrong worker": _observe(h.call, "PutKV", {
            "fence": {**f, "worker": "impostor"}, "namespace": ns, "key": "k",
            "operation_key": "op-1", "expected_version": 0, "value": {"a": 1}}, None),
        "write at stale epoch": _observe(h.call, "PutKV", {
            "fence": {**f, "epoch": f["epoch"] + 5}, "namespace": ns, "key": "k",
            "operation_key": "op-2", "expected_version": 0, "value": {"a": 1}}, None),
        "write with unknown run": _observe(h.call, "PutKV", {
            "fence": {**f, "run_id": "run-does-not-exist"}, "namespace": ns, "key": "k",
            "operation_key": "op-3", "expected_version": 0, "value": {"a": 1}}, None),
    }


def probe_released_lease(h: Harness) -> dict[str, str]:
    tag = f"rel-{uuid.uuid4().hex[:10]}"
    b = h.bootstrap(tag)
    f = b["fence"]
    ns = ["probe", tag]
    release = _observe(h.call, "Release",
                       {"run_id": f["run_id"], "worker": f["worker"],
                        "epoch": f["epoch"], "status": "done"}, None)
    return {
        "Release": release,
        "Release with an unknown status": _observe(h.call, "Release", {
            "run_id": b["run_id"], "worker": f["worker"], "epoch": f["epoch"] + 1,
            "status": "banana"}, None),
        "heartbeat after release": _observe(h.call, "Heartbeat", {
            "run_id": f["run_id"], "worker": f["worker"], "epoch": f["epoch"],
            "ttl_ms": 30000}, None),
        "write after release": _observe(h.call, "PutKV", {
            "fence": f, "namespace": ns, "key": "k",
            "operation_key": "op-4", "expected_version": 0, "value": {"a": 2}}, None),
    }


def probe_absent_reads(h: Harness) -> dict[str, str]:
    tag = f"abs-{uuid.uuid4().hex[:10]}"
    h.bootstrap(tag)
    return {
        "GetKV absent key": _observe(h.call, "GetKV",
                                     {"namespace": ["probe", tag], "key": "nope"}, None),
        "GetIntent absent": _observe(h.call, "GetIntent", {
            "run_id": "run-does-not-exist", "node": "n", "call": "c"}, None),
    }


def probe_kv_idempotency(h: Harness) -> dict[str, str]:
    tag = f"idem-{uuid.uuid4().hex[:10]}"
    b = h.bootstrap(tag)
    f, ns = b["fence"], ["probe", tag]

    # operation keys are prefixed with the run, the way ``AHPStore._op_key``
    # does. The platform spends an operation key for the whole SUBJECT
    # (``internal/state/kv.go``: tenant, deployment, subject, operation_key),
    # so a bare "op-A" is a ticking bomb - see the last observation below.
    def key(suffix: str) -> str:
        return f"{b['run_id']}:kv:{suffix}"

    def put(op_key: str, expected: int, value: dict):
        return _ask(h.call, "PutKV", {"fence": f, "namespace": ns, "key": "k",
                                      "operation_key": op_key,
                                      "expected_version": expected,
                                      "value": value}, None)

    out: dict[str, str] = {}
    first, err = put(key("op-A"), 0, {"v": 1})
    out["PutKV first"] = f"version={_item(first).get('version')}" if first else f"ERR {err.code}"
    replay, err = put(key("op-A"), 0, {"v": 1})
    out["replay same operation_key"] = (
        f"version={_item(replay).get('version')}" if replay else f"ERR {err.code}")
    _, err = put(key("op-B"), 0, {"v": 9})
    out["new op key, stale expected_version"] = f"ERR {err.code}" if err else "accepted"
    _, err = put(key("op-C"), 1, {"v": 2})
    out["new op key, correct version"] = "accepted" if not err else f"ERR {err.code}"
    _, err = put(key("op-A"), 0, {"v": 999})
    out["replay same op key, changed body"] = f"ERR {err.code}" if err else "accepted"
    read, err = _ask(h.call, "GetKV", {"namespace": ns, "key": "k"}, None)
    out["value after all writes"] = (json.dumps(_item(read).get("value"), sort_keys=True)
                                     if read else f"ERR {err.code}")
    # the scoping rule itself: a DIFFERENT run of the same subject reusing this
    # run's operation key must be refused, permanently. This is why AHPStore
    # folds run_id into every key it sends.
    other = h.bootstrap(f"{tag}-2")
    _, err = _ask(h.call, "PutKV", {
        "fence": other["fence"], "namespace": ["probe", f"{tag}-2"], "key": "k2",
        "operation_key": key("op-A"), "expected_version": 0,
        "value": {"v": 1}}, None)
    out["another run reuses the operation key"] = f"ERR {err.code}" if err else "accepted"
    return out


def probe_intent_machine(h: Harness) -> dict[str, str]:
    tag = f"int-{uuid.uuid4().hex[:10]}"
    b = h.bootstrap(tag)
    fence = {"fence": b["fence"]}
    run_id = b["run_id"]
    base = {"node": "charge", "call": "call-1", "kind": "tool", "target": "tool",
            "args": {"amount": 42}}

    def status_of(call_id: str) -> str:
        got, err = _ask(h.call, "GetIntent",
                        {"run_id": run_id, "node": "charge", "call": call_id}, None)
        return _item(got).get("status", "?") if got else f"ERR {err.code}"

    out: dict[str, str] = {}
    first, err = _ask(h.call, "RecordIntent", {**fence, **base}, None)
    if first is None:
        return {"RecordIntent": f"ERR {err.code}"}
    out["RecordIntent status"] = _item(first).get("status", "?")
    out["RecordIntent row has id"] = str("id" in _item(first))
    again, _ = _ask(h.call, "RecordIntent", {**fence, **base}, None)
    out["replay same identity"] = str(_ident(first) == _ident(again or {}))
    out["replay same digest"] = str(_item(first).get("digest")
                                    == _item(again or {}).get("digest"))
    got, err = _ask(h.call, "UpdateIntent", {**fence, "node": "charge",
                                             "call": "call-2",
                                             "status": "succeeded"}, None)
    out["recorded -> succeeded"] = f"ERR {err.code}" if err else "accepted"
    got, err = _ask(h.call, "UpdateIntent", {**fence, "node": "charge",
                                             "call": "call-1",
                                             "status": "dispatched"}, None)
    out["recorded -> dispatched"] = f"ERR {err.code}" if err else "accepted"
    out["GetIntent after dispatch"] = status_of("call-1")
    replay, _ = _ask(h.call, "RecordIntent", {**fence, **base}, None)
    out["replay reports current status"] = (_item(replay).get("status", "?")
                                            if replay else "unavailable")
    got, err = _ask(h.call, "UpdateIntent", {**fence, "node": "charge",
                                             "call": "call-1", "status": "succeeded",
                                             "result": {"receipt": "r-1"}}, None)
    out["dispatched -> succeeded"] = f"ERR {err.code}" if err else "accepted"
    out["GetIntent after success"] = status_of("call-1")
    got, err = _ask(h.call, "UpdateIntent", {**fence, "node": "charge",
                                             "call": "call-1", "status": "succeeded",
                                             "result": {"receipt": "other"}}, None)
    out["succeeded -> different result"] = f"ERR {err.code}" if err else "accepted"
    got, err = _ask(h.call, "RecordIntent", {**fence, **base,
                                             "args": {"amount": 99}}, None)
    out["record with new fingerprint"] = f"ERR {err.code}" if err else "accepted"
    got, err = _ask(h.call, "UpdateIntent", {**fence, "node": "charge",
                                             "call": "call-1",
                                             "status": "dispatched"}, None)
    out["succeeded -> dispatched (rewind)"] = f"ERR {err.code}" if err else "accepted"
    # strict request decoding: live decodes into typed fields and refuses a body
    # that puts an object where target is a string. A double that stores it hides
    # a call the SDK can never make against the platform.
    _, err = _ask(h.call, "RecordIntent", {**fence, "node": "charge", "call": "call-9",
                                           "kind": "tool", "target": {"amount": 1}},
                  None)
    out["record with a non-string target"] = f"ERR {err.code}" if err else "accepted"
    return out


def probe_thread_identity(h: Harness) -> dict[str, str]:
    tag = f"thr-{uuid.uuid4().hex[:10]}"
    first = h.call("CreateThread", {"context_id": tag, "key": f"thread:{tag}"}, None)
    same_ctx = h.call("CreateThread", {"context_id": tag,
                                       "key": f"thread:{tag}:other"}, None)
    return {
        "re-CreateThread same context_id": str(_id(first) == _id(same_ctx)),
        "same context_id new key": str(_id(first) == _id(same_ctx)),
    }


def probe_empty_claim(h: Harness) -> dict[str, str]:
    reply = h.call("ClaimRun", {"worker": f"empty-{uuid.uuid4().hex[:8]}",
                                "ttl_ms": 60000}, None)
    return {"ClaimRun with nothing queued": "lease null" if reply.get("lease") is None
            else f"lease for {_id(reply['lease']['run'])}"}


def probe_credentials(h: Harness) -> dict[str, str]:
    return {
        "read with forged token": _observe(h.call, "ListThreads", {"limit": 1},
                                           "forged-token"),
        "fenced write with forged token": _observe(h.call, "GetKV", {
            "namespace": ["probe", "x"], "key": "k"}, "forged-token"),
    }


PROBES: list[tuple[str, Callable[[Harness], dict[str, str]]]] = [
    ("empty claim", probe_empty_claim),
    ("envelope shapes", probe_envelopes),
    ("write authorization", probe_write_authorization),
    ("released lease", probe_released_lease),
    ("absent reads", probe_absent_reads),
    ("KV idempotency", probe_kv_idempotency),
    ("intent machine", probe_intent_machine),
    ("thread identity", probe_thread_identity),
    ("credentials", probe_credentials),
]


def run_matrix(h: Harness) -> dict[str, dict[str, str]]:
    """Every probe against one transport. Never raises on a bad answer."""
    results: dict[str, dict[str, str]] = {}
    for name, probe in PROBES:
        try:
            results[name] = probe(h)
        except Exception as exc:  # a transport that cannot even run a probe IS a result
            results[name] = {"__probe aborted__": f"{type(exc).__name__}: {exc}"}
    return results


def divergence_table(double: dict, live: dict) -> list[tuple[str, str, str, str]]:
    """(section, observation, double said, live said) for every mismatch."""
    rows: list[tuple[str, str, str, str]] = []
    for section in double:
        for label, mine in double[section].items():
            theirs = live.get(section, {}).get(label, "<not probed>")
            if mine != theirs:
                rows.append((section, label, mine, theirs))
    return rows


def render(double: dict, live: dict) -> str:
    lines = ["| раздел | наблюдение | ДВОЙНИК | ЖИВОЙ STATE | совпало |",
             "|---|---|---|---|---|"]
    for section in double:
        for label, mine in double[section].items():
            theirs = live.get(section, {}).get(label, "<not probed>")
            lines.append(f"| {section} | {label} | `{mine}` | `{theirs}` | "
                         f"{'да' if mine == theirs else '**НЕТ**'} |")
    return "\n".join(lines)
