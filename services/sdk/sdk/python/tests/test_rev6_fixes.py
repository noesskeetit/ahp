"""Regression tests for the authoritative rev6/rev7 findings
(reports/sdk/root/rev6-source-findings-20261030.json + Root 20261030T2103Z).

One faithful State-API fake (FaithfulHandler) reproduces the REAL internal/state
semantics — not the version-ignoring "TrackingHandler" the audit rejected:

  * KV (internal/state/kv.go):    expected_version CAS — create-only (0) conflicts
    on a live item, exact-version CAS otherwise, delete needs version >= 1,
    tombstones keep the version, and operation_key receipts replay an identical
    request (matched on op,key,namespace,expected_version,value) but fail
    KEY_REUSE for a reused key with a different request.
  * intent (internal/state/intents.go): statuses recorded/dispatched/succeeded/
    failed/unknown/fenced; RecordIntent replay returns "recorded" (NOT the current
    status); UpdateIntent is monotonic and persists `result`; an unknown intent
    resolves only via the privileged ReconcileIntent with the exact digest. The
    real Intent struct has NO `args` field, so the SDK journals the operation
    mapping in `result`.

Each test asserts behaviour that FAILS on the rev5/rev6 SDK and PASSES on rev7.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import pytest
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any

from langgraph.store.base import GetOp, PutOp
from ahp_state import (
    AHPStore, StateAPIClient, StateAPIError, AHPCheckpointSaver, IntentRecorder, SideEffectUnknown,
)


FENCE = {"run_id": "run-abc", "worker": "w-1", "epoch": 7}

_kv: dict[tuple, dict] = {}
_receipts: dict[str, dict] = {}
_intents: dict[tuple, dict] = {}
_requests: list[dict] = []
_lock = threading.Lock()
_conflict_once = {"count": 0}

VALID_STATUS = {"recorded", "dispatched", "succeeded", "failed", "unknown", "fenced"}
TERMINAL = {"succeeded", "failed"}


def _allow(frm, to):
    if frm == to:
        return True
    if frm == "recorded":
        return to in ("dispatched", "fenced")
    if frm == "dispatched":
        return to in TERMINAL or to in ("unknown", "fenced")
    if frm == "unknown":
        return to == "fenced"
    return False


def _digest(*parts) -> str:
    return hashlib.sha256("\x00".join(parts).encode()).hexdigest()


def _stored_version(ns_t, key) -> int:
    row = _kv.get((ns_t, key))
    return row["version"] if row else 0


class FaithfulHandler(BaseHTTPRequestHandler):
    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}
        path = self.path.replace("/v1/state/", "")
        _requests.append({"op": path, "body": body})
        with _lock:
            handler = getattr(self, f"_op_{path}", None)
            handler(body) if handler else self._json({"ok": True})

    def _op_PutKV(self, body):
        ns_t = tuple(body.get("namespace", [])); key = body.get("key")
        exp = int(body.get("expected_version", 0)); op_key = body.get("operation_key", "")
        value = body.get("value")
        rd = _digest("put", key, json.dumps(ns_t), str(exp), json.dumps(value, sort_keys=True))
        r = _receipts.get(op_key)
        if r is not None:
            if r["rd"] != rd:
                return self._json({"code": "KEY_REUSE", "message": "op key reused with different request"}, 409)
            if r["kind"] != "put" or r.get("item") is None:
                return self._json({"code": "KEY_REUSE", "message": "receipt kind mismatch"}, 409)
            return self._json({"item": r["item"]})            # receipt replay (no version bump)
        if _conflict_once["count"] > 0:
            _conflict_once["count"] -= 1
            return self._json({"code": "CONFLICT", "message": "kv version conflict"}, 409)
        baseline = _stored_version(ns_t, key); cur = _kv.get((ns_t, key))
        live = cur if (cur and not cur["deleted"]) else None
        if live is not None:
            if exp == 0:
                return self._json({"code": "CONFLICT", "message": "kv item already exists"}, 409)
            if live["version"] != exp:
                return self._json({"code": "CONFLICT", "message": "kv version conflict"}, 409)
        else:
            if exp != 0 and exp != baseline:
                return self._json({"code": "NOT_FOUND", "message": "kv item"}, 404)
        item = {"key": key, "namespace": list(ns_t), "value": value,
                "version": baseline + 1, "updated_at": "2026-01-01T00:00:00Z"}
        _kv[(ns_t, key)] = {"value": value, "version": baseline + 1, "deleted": False}
        _receipts[op_key] = {"rd": rd, "kind": "put", "item": item}
        self._json({"item": item})

    def _op_GetKV(self, body):
        ns_t = tuple(body.get("namespace", [])); key = body.get("key"); cur = _kv.get((ns_t, key))
        if cur is None or cur["deleted"]:
            return self._json({"code": "NOT_FOUND", "message": "kv item not found"}, 404)
        self._json({"item": {"key": key, "namespace": list(ns_t), "value": cur["value"],
                             "version": cur["version"], "updated_at": "2026-01-01T00:00:00Z"}})

    def _op_DeleteKV(self, body):
        ns_t = tuple(body.get("namespace", [])); key = body.get("key")
        exp = int(body.get("expected_version", 0)); op_key = body.get("operation_key", "")
        if exp < 1:
            return self._json({"code": "INVALID", "message": "expected version must be positive for delete"}, 400)
        rd = _digest("delete", key, json.dumps(ns_t), str(exp), "")
        r = _receipts.get(op_key)
        if r is not None:
            if r["rd"] != rd or r["kind"] != "delete":
                return self._json({"code": "KEY_REUSE", "message": "op key reused"}, 409)
            return self._json({"ok": True})
        cur = _kv.get((ns_t, key))
        if cur is None or cur["deleted"]:
            return self._json({"code": "NOT_FOUND", "message": "kv item"}, 404)
        if cur["version"] != exp:
            return self._json({"code": "CONFLICT", "message": "kv version conflict"}, 409)
        cur["deleted"] = True; cur["value"] = None
        _receipts[op_key] = {"rd": rd, "kind": "delete", "deleted": True}
        self._json({"ok": True})

    def _op_SearchKV(self, body):
        pref = tuple(body.get("namespace_prefix", [])); items = []
        for (ns_t, key), row in _kv.items():
            if row["deleted"]:
                continue
            if pref and ns_t[:len(pref)] != pref:
                continue
            items.append({"key": key, "namespace": list(ns_t), "value": row["value"],
                          "version": row["version"], "updated_at": "2026-01-01T00:00:00Z"})
        self._json({"items": items})

    def _op_RecordIntent(self, body):
        run = body["fence"]["run_id"]; node, call = body["node"], body["call"]
        fp = _digest(body.get("kind", ""), body.get("target", ""), json.dumps(body.get("args", {}), sort_keys=True))
        k = (run, node, call); cur = _intents.get(k)
        if cur is not None:
            if cur["digest"] != fp:
                return self._json({"code": "KEY_REUSE", "message": "intent key reused with different fingerprint"}, 409)
            return self._json({"intent": {"node": node, "call": call, "kind": body.get("kind"),
                                          "status": "recorded", "digest": cur["digest"]}})  # FAITHFUL replay
        _intents[k] = {"status": "recorded", "digest": fp, "kind": body.get("kind"),
                       "target": body.get("target", ""), "result": None}
        self._json({"intent": {"node": node, "call": call, "kind": body.get("kind"),
                               "status": "recorded", "digest": fp}})

    def _op_UpdateIntent(self, body):
        run = body["fence"]["run_id"]; node, call, status = body["node"], body["call"], body.get("status")
        k = (run, node, call)
        if status not in VALID_STATUS:
            return self._json({"code": "INVALID", "message": f"unknown intent status {status}"}, 400)
        cur = _intents.get(k)
        if cur is None:
            return self._json({"code": "NOT_FOUND", "message": "intent not found"}, 404)
        if not _allow(cur["status"], status):
            if cur["status"] == "unknown":
                return self._json({"code": "CONFLICT", "message": "unknown intents reconcile only via the privileged API"}, 409)
            return self._json({"code": "CONFLICT", "message": f"illegal transition {cur['status']}->{status}"}, 409)
        if cur["status"] in TERMINAL:
            if json.dumps(cur["result"], sort_keys=True) != json.dumps(body.get("result"), sort_keys=True):
                return self._json({"code": "CONFLICT", "message": "already completed with different result"}, 409)
            return self._json({"intent": {"node": node, "call": call, "status": cur["status"], "result": cur["result"]}})
        cur["status"] = status
        if body.get("result") is not None:
            cur["result"] = body["result"]          # persists the journalled mapping
        self._json({"intent": {"node": node, "call": call, "status": status, "result": cur["result"]}})

    def _op_GetIntent(self, body):
        k = (body["run_id"], body["node"], body["call"]); cur = _intents.get(k)
        if cur is None:
            return self._json({"code": "NOT_FOUND", "message": "intent not found"}, 404)
        self._json({"intent": {"node": body["node"], "call": body["call"], "status": cur["status"],
                               "digest": cur["digest"], "result": cur["result"]}})

    def _op_ReconcileIntent(self, body):
        run = body["fence"]["run_id"]; node, call, status = body["node"], body["call"], body.get("status")
        if status not in TERMINAL:
            return self._json({"code": "INVALID", "message": "reconcile target must be succeeded or failed"}, 400)
        k = (run, node, call); cur = _intents.get(k)
        if cur is None:
            return self._json({"code": "NOT_FOUND", "message": "intent not found"}, 404)
        if cur["status"] != "unknown":
            return self._json({"code": "CONFLICT", "message": "only unknown intents can be reconciled"}, 409)
        if cur["digest"] != body.get("digest", ""):
            return self._json({"code": "KEY_REUSE", "message": "reconciliation fingerprint mismatch"}, 409)
        cur["status"] = status
        if body.get("result") is not None:
            cur["result"] = body["result"]
        self._json({"intent": {"node": node, "call": call, "status": status, "result": cur["result"]}})

    def _json(self, data, status=200):
        self.send_response(status); self.send_header("Content-Type", "application/json")
        self.end_headers(); self.wfile.write(json.dumps(data).encode())

    def log_message(self, *args):
        pass


class _StatusHandler(BaseHTTPRequestHandler):
    def __init__(self, code, payload, *a, **kw):
        self._code, self._payload = code, payload
        super().__init__(*a, **kw)

    def do_POST(self):  # noqa: N802
        data = json.dumps(self._payload).encode()
        self.send_response(self._code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def log_message(self, *args):
        pass


def _make_status_server(code, payload):
    handler = lambda *a, **kw: _StatusHandler(code, payload, *a, **kw)  # noqa: E731
    srv = HTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


@pytest.fixture
def state_server():
    for d in (_kv, _receipts, _intents):
        d.clear()
    _requests.clear(); _conflict_once["count"] = 0
    srv = HTTPServer(("127.0.0.1", 0), FaithfulHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()


def _client(url):
    return StateAPIClient(url, token="t")


async def _read_item(client, ns, key):
    return (await client.get_kv(list(ns), key))["item"]


# ---------------- SDK-R6-1: version-aware CAS ----------------

class TestR61CompareAndSet:
    @pytest.mark.asyncio
    async def test_update_existing_key_bumps_version(self, state_server):
        client = _client(state_server); store = AHPStore(client, fence=FENCE)
        await store.abatch([PutOp(namespace=("ns",), key="k", value="v1")])
        assert (await _read_item(client, ("ns",), "k"))["version"] == 1
        await store.abatch([PutOp(namespace=("ns",), key="k", value="v2")])
        item = await _read_item(client, ("ns",), "k")
        assert item["value"] == "v2" and item["version"] == 2

    @pytest.mark.asyncio
    async def test_concurrent_conflict_retries_and_converges(self, state_server):
        _conflict_once["count"] = 1
        client = _client(state_server); store = AHPStore(client, fence=FENCE)
        await store.abatch([PutOp(namespace=("ns",), key="k", value="v")])
        assert len([r for r in _requests if r["op"] == "PutKV"]) >= 2
        assert (await _read_item(client, ("ns",), "k"))["value"] == "v"

    @pytest.mark.asyncio
    async def test_delete_requires_positive_version_and_is_idempotent(self, state_server):
        client = _client(state_server); store = AHPStore(client, fence=FENCE)
        await store.abatch([PutOp(namespace=("ns",), key="k", value="v")])
        await store.abatch([PutOp(namespace=("ns",), key="k", value=None)])
        dels = [r for r in _requests if r["op"] == "DeleteKV"]
        assert len(dels) == 1 and int(dels[0]["body"]["expected_version"]) >= 1
        with pytest.raises(StateAPIError) as ei:
            await client.get_kv(["ns"], "k")
        assert ei.value.code == "NOT_FOUND"
        await store.abatch([PutOp(namespace=("ns",), key="k", value=None)])  # idempotent no-op
        assert len([r for r in _requests if r["op"] == "DeleteKV"]) == 1


# ---------------- SDK-R6-2: durable logical-operation identity ----------------

class TestR62OperationIdentity:
    @pytest.mark.asyncio
    async def test_two_writes_same_key_distinct_values_no_keyreuse(self, state_server):
        client = _client(state_server); store = AHPStore(client, fence=FENCE)
        await store.abatch([PutOp(namespace=("ns",), key="k", value="a")])
        await store.abatch([PutOp(namespace=("ns",), key="k", value="b")])
        assert (await _read_item(client, ("ns",), "k"))["value"] == "b"

    @pytest.mark.asyncio
    async def test_put_delete_recreate_does_not_replay_stale_receipt(self, state_server):
        client = _client(state_server); store = AHPStore(client, fence=FENCE)
        await store.abatch([PutOp(namespace=("ns",), key="k", value="orig")])
        await store.abatch([PutOp(namespace=("ns",), key="k", value=None)])
        await store.abatch([PutOp(namespace=("ns",), key="k", value="recreated")])
        item = await _read_item(client, ("ns",), "k")
        assert item["value"] == "recreated" and item["version"] == 2

    @pytest.mark.asyncio
    async def test_namespace_tuple_boundary_is_unambiguous(self, state_server):
        client = _client(state_server); store = AHPStore(client, fence=FENCE)
        await store.abatch([PutOp(namespace=("a:b", "c"), key="k", value="first")])
        await store.abatch([PutOp(namespace=("a", "b:c"), key="k", value="second")])
        assert (await _read_item(client, ("a:b", "c"), "k"))["value"] == "first"
        assert (await _read_item(client, ("a", "b:c"), "k"))["value"] == "second"

    def test_op_key_is_version_independent_and_not_a_counter(self):
        """The durable op key must NOT fold the live version, so the same logical
        write maps to the same key; and it is not a sequential counter."""
        store = AHPStore(_client("http://127.0.0.1:1"), fence=FENCE)
        with store.with_identity(node="n", call="c"):
            k_v0 = store._op_key(("ns",), "k", "put", "v")
            k_v9 = store._op_key(("ns",), "k", "put", "v")   # version is not an argument at all
            assert k_v0 == k_v9
        keys = {store._op_key(("ns",), "k", "put", "v"),
                store._op_key(("ns",), "k2", "put", "v"),
                store._op_key(("ns",), "k3", "delete", None)}
        assert len(keys) == 3 and not any(x.endswith(("-1", "-2", "-3")) for x in keys)

    @pytest.mark.asyncio
    async def test_lost_ack_replays_instead_of_double_write(self, state_server):
        """Root's scenario: PutKV committed, response lost, process restart with
        the SAME node/call -> the durable journal must REPLAY the original
        (op_key, expected, value), NOT re-derive the version and double-write."""
        client_a = _client(state_server); store_a = AHPStore(client_a, fence=FENCE)
        rec = IntentRecorder(client_a, fence=FENCE, node="n")
        with store_a.with_identity(node="n", call="c2"):
            K = store_a._op_key(("ns",), "k", "put", "v1")
        # Journal the original mapping and commit the write, then "crash" (no succeed).
        await rec.record("c2", kind="kv", target="ns/k")
        await rec.update("c2", "dispatched", result={"op_key": K, "expected_version": 0, "value": "v1"})
        await client_a.put_kv(FENCE, ["ns"], "k", K, 0, "v1")      # committed; ack lost
        assert (await _read_item(client_a, ("ns",), "k"))["version"] == 1

        # RESTART: a fresh store + client, same node/call, same logical value.
        client_b = _client(state_server); store_b = AHPStore(client_b, fence=FENCE)
        with store_b.with_identity(node="n", call="c2"):
            await store_b.abatch([PutOp(namespace=("ns",), key="k", value="v1")])
        item = await _read_item(client_b, ("ns",), "k")
        assert item["version"] == 1, f"expected receipt replay (v1), got version {item['version']} (double-write)"
        assert item["value"] == "v1"
        # The KV intent stays non-terminal (dispatched) so further sub-operations
        # under the same identity can still be journaled; the sub-operation itself
        # is marked succeeded inside the journal (result.subops).
        intent = await rec.get("c2")
        assert intent["status"] == "dispatched"
        subops = intent["result"]["subops"]
        assert subops[K]["status"] == "succeeded"

    @pytest.mark.asyncio
    async def test_identity_is_task_local_under_interleaving(self, state_server):
        """_identity is a contextvar: two concurrent tasks under different
        identities must not observe each other's identity (no interleaving bug)."""
        store = AHPStore(_client(state_server), fence=FENCE)
        seen = {}

        async def worker(node, call):
            with store.with_identity(node=node, call=call):
                await asyncio.sleep(0.02)             # force an interleave
                seen[call] = (store.current_identity(), store._op_key(("ns",), "k", "put", "v"))

        await asyncio.gather(worker("nA", "cA"), worker("nB", "cB"))
        assert seen["cA"][0] == {"node": "nA", "call": "cA"}
        assert seen["cB"][0] == {"node": "nB", "call": "cB"}
        assert seen["cA"][1] != seen["cB"][1]


# ---------------- SDK-R6-3: intent status machine + fail-closed ----------------

class TestR63IntentMachine:
    @pytest.mark.asyncio
    async def test_record_replay_returns_recorded_not_current(self, state_server):
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        await rec.record("tc-1"); await rec.dispatch("tc-1"); await rec.succeed("tc-1", {"x": 1})
        assert (await rec.record("tc-1"))["status"] == "recorded"           # replay, not current
        assert (await rec.get("tc-1"))["status"] == "succeeded"             # GetIntent is truth

    @pytest.mark.asyncio
    async def test_guarded_skips_already_succeeded_effect(self, state_server):
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent"); calls = {"n": 0}

        async def effect():
            calls["n"] += 1; return "R"
        assert await rec.guarded("tc-1", effect) == "R" and calls["n"] == 1

        async def other():
            calls["n"] += 100; return "BAD"
        assert await rec.guarded("tc-1", other) == "R" and calls["n"] == 1  # skipped

    @pytest.mark.asyncio
    async def test_guarded_crash_window_surfaces_unknown(self, state_server):
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        await rec.record("tc-1"); await rec.dispatch("tc-1")               # effect fired, crash before completion
        ran = {"n": 0}

        async def effect():
            ran["n"] += 1; return "again"
        with pytest.raises(SideEffectUnknown):
            await rec.guarded("tc-1", effect)
        assert ran["n"] == 0
        assert (await rec.get("tc-1"))["status"] == "unknown"

    @pytest.mark.asyncio
    async def test_guarded_marks_unknown_not_failed_on_effect_exception(self, state_server):
        """Root 20261030T2103Z: an effect that raises may have run before the ack
        was lost -> fail-closed UNKNOWN, never a confirmed FAILED."""
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")

        async def boom():
            raise RuntimeError("effect ran but ack lost")
        with pytest.raises(RuntimeError):
            await rec.guarded("tc-x", boom)
        assert (await rec.get("tc-x"))["status"] == "unknown"

    @pytest.mark.asyncio
    async def test_reconcile_unknown_to_succeeded(self, state_server):
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        await rec.record("tc-1"); await rec.dispatch("tc-1"); await rec.mark_unknown("tc-1")
        out = await rec.reconcile("tc-1", "succeeded", result={"confirmed": True})
        assert out["status"] == "succeeded" and (await rec.get("tc-1"))["result"] == {"confirmed": True}

    @pytest.mark.asyncio
    async def test_illegal_status_and_illegal_transition_rejected(self, state_server):
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        with pytest.raises(StateAPIError):
            await rec.update("tc-1", "completed")
        await rec.record("tc-2")
        with pytest.raises(StateAPIError) as ei:
            await rec.update("tc-2", "succeeded")   # recorded->succeeded illegal
        assert ei.value.code == "CONFLICT"


# ---------------- broad-except fix ----------------

class TestErrorsPropagate:
    @pytest.mark.asyncio
    async def test_store_get_propagates_500(self):
        srv, url = _make_status_server(500, {"code": "INTERNAL", "message": "boom"})
        try:
            with pytest.raises(StateAPIError):
                await AHPStore(_client(url), fence=FENCE).abatch([GetOp(namespace=("ns",), key="k")])
        finally:
            srv.shutdown()

    @pytest.mark.asyncio
    async def test_aget_tuple_propagates_500_not_empty(self):
        srv, url = _make_status_server(500, {"code": "INTERNAL", "message": "boom"})
        try:
            saver = AHPCheckpointSaver(_client(url), thread_id="th-1", fence=FENCE)
            with pytest.raises(StateAPIError):
                await saver.aget_tuple({"configurable": {"thread_id": "th-1"}})
        finally:
            srv.shutdown()

    @pytest.mark.asyncio
    async def test_aget_tuple_not_found_returns_none(self, state_server):
        saver = AHPCheckpointSaver(_client(state_server), thread_id="ghost", fence=FENCE)
        assert await saver.aget_tuple({"configurable": {"thread_id": "ghost"}}) is None
