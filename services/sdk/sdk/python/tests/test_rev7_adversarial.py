"""Regression tests for the rev7 adversarial findings (P1-P5).

forum/posts/20261030T212531Z-sdk-rev7-adversarial-probes-findings.md

Each test reproduces one confirmed defect and asserts the CORRECT behaviour.
Every test here FAILS on the pre-fix (rev7) SDK and PASSES on the fixed SDK.
They run against the same faithful State-API handler (FaithfulHandler) that
reproduces the real internal/state semantics (KV receipt replay + CAS, and the
intent status machine with from==to transitions), so no narrowing to mocks.

  P1  multi-op abatch under one (run,node,call): every write persists.
  P2  put -> delete -> recreate under one identity: all apply (final v2).
  P3  crash re-entry with a NEW key: no stale op_key hijack / KEY_REUSE.
  P4  concurrent guarded() with one call_id: the effect fires exactly once.
  P5  changed (kind,target,args) under a terminal intent: explicit error,
      never a silent stale result.
"""

from __future__ import annotations

import asyncio

import pytest
from langgraph.store.base import PutOp

from ahp_state import AHPStore, StateAPIError, IntentRecorder, SideEffectUnknown

# Reuse the faithful handler + fixtures (real internal/state semantics).
from test_rev6_fixes import (  # noqa: F401  (state_server is a fixture)
    state_server, FENCE, _client, _read_item, _requests, _kv, _intents,
)


# ---------------- P1: multi-op abatch under one identity ----------------

class TestP1MultiOp:
    @pytest.mark.asyncio
    async def test_two_puts_one_identity_both_persist(self, state_server):
        """abatch([Put(k1,v1), Put(k2,v2)]) under ONE identity: the pre-fix SDK
        marked the intent succeeded after k1 and silently SKIPPED k2 (data loss)."""
        client = _client(state_server)
        store = AHPStore(client, fence=FENCE)
        with store.with_identity(node="n", call="tool1"):
            await store.abatch([
                PutOp(namespace=("ns",), key="k1", value="v1"),
                PutOp(namespace=("ns",), key="k2", value="v2"),
            ])
        puts = [r for r in _requests if r["op"] == "PutKV"]
        assert len(puts) == 2, f"expected 2 PutKV calls, got {len(puts)}"
        assert (await _read_item(client, ("ns",), "k1"))["value"] == "v1"
        assert (await _read_item(client, ("ns",), "k2"))["value"] == "v2"

    @pytest.mark.asyncio
    async def test_multi_op_same_key_distinct_values_all_apply(self, state_server):
        """Repeated same-key writes inside one tool are DISTINCT sub-operations:
        they must not collapse to one (only hash(kind,ns,key))."""
        client = _client(state_server)
        store = AHPStore(client, fence=FENCE)
        with store.with_identity(node="n", call="tool1b"):
            await store.abatch([
                PutOp(namespace=("ns",), key="k", value="a"),
                PutOp(namespace=("ns",), key="k", value="b"),
            ])
        item = await _read_item(client, ("ns",), "k")
        assert item["value"] == "b" and item["version"] == 2


# ---------------- P2: put -> delete -> recreate under one identity ----------------

class TestP2PutDeleteRecreate:
    @pytest.mark.asyncio
    async def test_put_delete_recreate_all_apply(self, state_server):
        """put -> delete -> recreate, sequential, ONE identity: the pre-fix SDK
        skipped the delete and the recreate after the first put succeeded."""
        client = _client(state_server)
        store = AHPStore(client, fence=FENCE)
        with store.with_identity(node="n", call="tool2"):
            await store.abatch([PutOp(namespace=("ns",), key="k", value="v1")])
            await store.abatch([PutOp(namespace=("ns",), key="k", value=None)])  # delete
            await store.abatch([PutOp(namespace=("ns",), key="k", value="v2")])  # recreate
        item = await _read_item(client, ("ns",), "k")
        assert item["value"] == "v2", f"recreate was skipped, got {item['value']!r}"
        assert item["version"] == 2, f"expected version 2, got {item['version']}"


# ---------------- P3: crash re-entry with a NEW key ----------------

class TestP3CrashReentryNewKey:
    @pytest.mark.asyncio
    async def test_reentry_new_key_no_opkey_hijack(self, state_server):
        """Crash after journal+commit of k1 (status=dispatched, no succeed); the
        re-entry arrives with a NEW key k2. The pre-fix SDK replayed k1's op_key
        against k2 -> server digest mismatch -> KEY_REUSE, k2 lost, intent stuck.
        The journal here is the legacy flat single-slot mapping (the crash state
        the finding describes); the fixed SDK must not hijack it for k2."""
        client_a = _client(state_server)
        store_a = AHPStore(client_a, fence=FENCE)
        rec = IntentRecorder(client_a, fence=FENCE, node="n")
        with store_a.with_identity(node="n", call="tool3"):
            K1 = store_a._op_key(("ns",), "k1", "put", "a")
        # First attempt journaled + committed k1, then crashed before succeed().
        await rec.record("tool3", kind="kv", target="ns/k1")
        await rec.update("tool3", "dispatched",
                         result={"op_key": K1, "expected_version": 0, "value": "a"})
        await client_a.put_kv(FENCE, ["ns"], "k1", K1, 0, "a")  # committed
        assert (await _read_item(client_a, ("ns",), "k1"))["value"] == "a"

        # RESTART: re-entry with a FRESH key k2 under the SAME identity.
        client_b = _client(state_server)
        store_b = AHPStore(client_b, fence=FENCE)
        with store_b.with_identity(node="n", call="tool3"):
            await store_b.abatch([PutOp(namespace=("ns",), key="k2", value="b")])
        # k2 must be committed (no KEY_REUSE); k1 must be untouched.
        assert (await _read_item(client_b, ("ns",), "k2"))["value"] == "b"
        assert (await _read_item(client_b, ("ns",), "k1"))["value"] == "a"


# ---------------- P4: concurrent guarded() with one call_id ----------------

class TestP4ConcurrentDispatch:
    @pytest.mark.asyncio
    async def test_concurrent_guarded_fires_effect_once(self, state_server):
        """Two concurrent guarded() under one call_id: the pre-fix SDK had no
        claim/serialization (State allows from==to), so BOTH crossed
        recorded->dispatched and fired the effect twice. The fixed SDK serializes
        same-call dispatch -> exactly one external effect."""
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        calls = {"n": 0}

        async def effect():
            calls["n"] += 1
            await asyncio.sleep(0.02)  # hold the effect open
            return "R"

        r1, r2 = await asyncio.gather(
            rec.guarded("tc-p4", effect, kind="tool", target="t", args={"i": 1}),
            rec.guarded("tc-p4", effect, kind="tool", target="t", args={"i": 1}),
        )
        assert calls["n"] == 1, f"effect fired {calls['n']} times, expected exactly 1"
        assert r1 == "R" and r2 == "R"

    @pytest.mark.asyncio
    async def test_sequential_guarded_same_call_is_idempotent(self, state_server):
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        calls = {"n": 0}

        async def effect():
            calls["n"] += 1
            return "R"
        assert await rec.guarded("tc-p4b", effect) == "R" and calls["n"] == 1
        assert await rec.guarded("tc-p4b", effect) == "R" and calls["n"] == 1


# ---------------- P5: changed args under a terminal intent ----------------

class TestP5FingerprintMismatch:
    @pytest.mark.asyncio
    async def test_changed_target_under_succeeded_raises_not_stale(self, state_server):
        """guarded succeeded, then a NEW call reuses the call_id with a DIFFERENT
        target/args: the pre-fix SDK returned the stale stored result silently.
        The fixed SDK validates the fingerprint and raises INTENT_ARG_MISMATCH."""
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        calls = {"n": 0}

        async def effect():
            calls["n"] += 1
            return "r1"
        assert await rec.guarded("tc-p5", effect, kind="tool", target="t1",
                                 args={"x": 1}) == "r1"
        assert calls["n"] == 1

        async def effect2():
            calls["n"] += 100
            return "r2"
        with pytest.raises(StateAPIError) as ei:
            await rec.guarded("tc-p5", effect2, kind="tool", target="t2",
                              args={"x": 2})
        assert ei.value.code == "INTENT_ARG_MISMATCH"
        assert calls["n"] == 1, "the changed effect must NOT run"

    @pytest.mark.asyncio
    async def test_same_fingerprint_under_succeeded_returns_stored(self, state_server):
        """A genuine retry (IDENTICAL kind/target/args) still skips and returns
        the stored result — the fingerprint gate must not reject true replays."""
        rec = IntentRecorder(_client(state_server), fence=FENCE, node="agent")
        calls = {"n": 0}

        async def effect():
            calls["n"] += 1
            return "r1"
        assert await rec.guarded("tc-p5b", effect, kind="tool", target="t1",
                                 args={"x": 1}) == "r1"
        async def effect_retry():  # different closure, SAME fingerprint
            calls["n"] += 100
            return "SHOULD_NOT_RUN"
        assert await rec.guarded("tc-p5b", effect_retry, kind="tool", target="t1",
                                 args={"x": 1}) == "r1"
        assert calls["n"] == 1
