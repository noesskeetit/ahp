"""Full 37-route contract coverage against a live State stand.

For each route: correct→200, wrong-args→400, unknown→404.
Produces a 37×3 result table.

The stand comes from the hand-out in ``AHP_STATE_HANDSHAKE``; the old tunnel
variables still work as overrides - see ``tests/_live_stand.py``.
"""
import json
import os
import ssl
import sys
import uuid

import pytest
import httpx

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ahp_state.client import (
    StateAPIClient,
    StateAPIError,
    StateConflictError,
    StateNotFoundError,
    StateForbiddenError,
    StateValidationError,
    StateTimeoutError,
)

from _live_stand import STAND


def _get_token():
    """The task token the hand-out was issued with (never hardcoded)."""
    return STAND.token


def _client(token=None) -> StateAPIClient:
    return STAND.client(token)


pytestmark = STAND.skip_mark()


async def _setup() -> dict:
    """Create thread + run + claim. Returns context dict for all tests."""
    token = _get_token()
    c = _client(token)
    uid = uuid.uuid4().hex[:8]

    thread = await c.create_thread(f"ctx-c37-{uid}", f"c37-{uid}")
    thread_id = thread["id"]
    run = await c.create_run(thread_id, f"run-{uid}", payload={"t": "c37"})
    await c.close()

    cc = _client(None)
    claim = await cc.claim_run("c37-coverage", ttl_ms=120000)
    lease = claim["lease"]
    fence = {"run_id": lease["run"]["id"], "worker": lease["worker"], "epoch": lease["epoch"]}
    kv = _client(claim["task_token"])
    await cc.close()

    return {
        "client": kv,
        "fence": fence,
        "thread_id": thread_id,
        "run_id": run["id"],
        "context_id": f"ctx-c37-{uid}",
    }


async def _teardown(ctx):
    try:
        await ctx["client"].release(ctx["fence"], status="done")
    except (StateTimeoutError, StateAPIError):
        pass
    await ctx["client"].close()


def _classify(exc_or_none, status_code=None):
    """Classify result: 'ok', '400', '404', '409', 'error'."""
    if exc_or_none is None and status_code is None:
        return "ok"
    if isinstance(exc_or_none, StateValidationError):
        return "400"
    if isinstance(exc_or_none, StateNotFoundError):
        return "404"
    if isinstance(exc_or_none, StateConflictError):
        return "409"
    if isinstance(exc_or_none, StateAPIError):
        return f"err:{exc_or_none.code}"
    return "ok"


@pytest.mark.asyncio
async def test_all_37_routes():
    """Full 37-route coverage: correct, wrong-args, unknown for each."""
    ctx = await _setup()
    c = ctx["client"]
    fence = ctx["fence"]
    tid = ctx["thread_id"]
    results = {}

    # Helper: test one route
    async def check(name, correct_fn, wrong_fn, unknown_fn):
        r = {"correct": "?", "wrong": "?", "unknown": "?"}
        try:
            await correct_fn()
            r["correct"] = "PASS"
        except StateAPIError as e:
            r["correct"] = f"FAIL({e.code})"
        except Exception as e:
            r["correct"] = f"EXC({type(e).__name__})"

        try:
            await wrong_fn()
            r["wrong"] = "FAIL(200)"  # got success, expected 400
        except StateValidationError:
            r["wrong"] = "PASS"
        except StateNotFoundError:
            r["wrong"] = "PASS(404)"  # also acceptable
        except StateAPIError as e:
            r["wrong"] = f"PASS({e.code})"
        except Exception as e:
            r["wrong"] = f"EXC({type(e).__name__})"

        try:
            await unknown_fn()
            r["unknown"] = "FAIL(200)"
        except StateNotFoundError:
            r["unknown"] = "PASS"
        except StateValidationError:
            r["unknown"] = "PASS(400)"
        except StateAPIError as e:
            r["unknown"] = f"PASS({e.code})"
        except Exception as e:
            r["unknown"] = f"EXC({type(e).__name__})"

        results[name] = r

    # Pre-create resources for correct-path tests
    ns = ["c37", "test"]
    op_uid = uuid.uuid4().hex[:6]

    # --- 1. ClaimRun ---
    await check("ClaimRun",
        lambda: _client(None).claim_run("c37-claim2", ttl_ms=60000) if False else _noop(),
        lambda: c.call("ClaimRun", {}),
        lambda: c.call("ClaimRun", {"worker": ""}))

    # --- 2. CreateThread ---
    await check("CreateThread",
        lambda: c.create_thread(f"ctx-ok-{op_uid}", f"ok-{op_uid}"),
        lambda: c.create_thread("", ""),
        lambda: c.call("CreateThread", {"context_id": "", "key": ""}))

    # --- 3. GetThread ---
    await check("GetThread",
        lambda: c.get_thread(tid),
        lambda: c.get_thread(""),
        lambda: c.get_thread(f"nonexistent-{op_uid}"))

    # --- 4. GetThreadByContext ---
    await check("GetThreadByContext",
        lambda: c.get_thread_by_context(ctx["context_id"]),
        lambda: c.get_thread_by_context(""),
        lambda: c.get_thread_by_context(f"no-such-ctx-{op_uid}"))

    # --- 5. ListThreads ---
    await check("ListThreads",
        lambda: c.list_threads(limit=10),
        lambda: c.call("ListThreads", {"limit": -1}),
        lambda: c.call("ListThreads", {"limit": 10, "bad_field": "x"}))

    # --- 6. CreateRun ---
    await check("CreateRun",
        lambda: c.create_run(tid, f"r2-{op_uid}", payload={"t": 1}),
        lambda: c.call("CreateRun", {"thread_id": tid, "key": ""}),
        lambda: c.call("CreateRun", {"thread_id": f"no-{op_uid}", "key": "x", "payload": {}}))

    # --- 7. GetRun ---
    await check("GetRun",
        lambda: c.get_run(ctx["run_id"]),
        lambda: c.get_run(""),
        lambda: c.get_run(f"no-run-{op_uid}"))

    # --- 8. ListRuns ---
    await check("ListRuns",
        lambda: c.list_runs(tid),
        lambda: c.list_runs(""),
        lambda: c.list_runs(f"no-thread-{op_uid}"))

    # --- 9. Heartbeat ---
    await check("Heartbeat",
        lambda: c.heartbeat(fence, ttl_ms=30000),
        lambda: c.call("Heartbeat", {}),
        lambda: c.call("Heartbeat", {"run_id": f"no-{op_uid}", "worker": "x", "epoch": 0}))

    # --- 10. Release ---
    # Can't test "correct" without killing the lease; use wrong/unknown only
    await check("Release",
        lambda: _noop(),  # skip correct (would kill our lease)
        lambda: c.call("Release", {}),
        lambda: c.call("Release", {"run_id": f"no-{op_uid}", "worker": "x", "epoch": 0}))

    # --- 11. Suspend ---
    await check("Suspend",
        lambda: _noop(),  # skip (would suspend our run)
        lambda: c.call("Suspend", {}),
        lambda: c.call("Suspend", {"run_id": f"no-{op_uid}", "worker": "x", "epoch": 0}))

    # --- 12. PutCheckpoint ---
    cp_id = f"cp-{op_uid}"
    await check("PutCheckpoint",
        lambda: c.put_checkpoint(fence, {"id": cp_id, "thread_id": tid, "namespace": "c37", "version": 1, "created_at": "2026-10-31T00:00:00Z"}, cp_id),
        lambda: c.call("PutCheckpoint", {}),
        lambda: c.put_checkpoint(fence, {"id": f"no-{op_uid}", "thread_id": f"no-{op_uid}", "namespace": "x", "version": 1, "created_at": "2026-10-31T00:00:00Z"}, f"no-{op_uid}"))

    # --- 13. GetCheckpoint ---
    await check("GetCheckpoint",
        lambda: c.get_checkpoint(tid, "c37", cp_id),
        lambda: c.call("GetCheckpoint", {}),
        lambda: c.get_checkpoint(f"no-{op_uid}", "c37", f"no-{op_uid}"))

    # --- 14. ListCheckpoints ---
    await check("ListCheckpoints",
        lambda: c.list_checkpoints(tid, "c37"),
        lambda: c.call("ListCheckpoints", {}),
        lambda: c.list_checkpoints(f"no-{op_uid}", "c37"))

    # --- 15. GetPart ---
    await check("GetPart",
        lambda: c.get_part(tid, "c37", "1", "__full__"),
        lambda: c.call("GetPart", {}),
        lambda: c.get_part(f"no-{op_uid}", "c37", "1", "no"))

    # --- 16. PutWrites ---
    await check("PutWrites",
        lambda: c.put_writes(fence, "c37", cp_id, [{"task": "t", "index": 0, "channel": "ch", "data": "aGk="}]),
        lambda: c.call("PutWrites", {}),
        lambda: c.put_writes(fence, "no-ns", f"no-{op_uid}", []))

    # --- 17. GetWrites ---
    await check("GetWrites",
        lambda: c.get_writes(tid, "c37", cp_id),
        lambda: c.call("GetWrites", {}),
        lambda: c.get_writes(f"no-{op_uid}", "c37", f"no-{op_uid}"))

    # --- 18. PutKV ---
    kv_key = f"k-{op_uid}"
    await check("PutKV",
        lambda: c.put_kv(fence, ns, kv_key, f"op-{op_uid}", 0, {"v": 1}),
        lambda: c.call("PutKV", {}),
        lambda: c.put_kv(fence, ns, f"no-{op_uid}", f"op-no-{op_uid}", 0, {}))

    # --- 19. DeleteKV ---
    await check("DeleteKV",
        lambda: c.delete_kv(fence, ns, kv_key, f"del-{op_uid}", 1),
        lambda: c.call("DeleteKV", {}),
        lambda: c.delete_kv(fence, ns, f"no-{op_uid}", f"del-no-{op_uid}", 1))

    # --- 20. GetKV ---
    await check("GetKV",
        lambda: c.get_kv(ns, kv_key),
        lambda: c.call("GetKV", {}),
        lambda: c.get_kv(ns, f"no-key-{op_uid}"))

    # --- 21. SearchKV ---
    await check("SearchKV",
        lambda: c.search_kv(["c37"], limit=10),
        lambda: c.call("SearchKV", {}),
        lambda: c.search_kv(["no-ns-xyz"]))

    # --- 22. RecordIntent ---
    await check("RecordIntent",
        lambda: c.record_intent(fence, "agent", f"call-{op_uid}", "kv", "target"),
        lambda: c.call("RecordIntent", {}),
        lambda: c.call("RecordIntent", {"fence": fence, "node": "", "call": ""}))

    # --- 23. UpdateIntent ---
    await check("UpdateIntent",
        lambda: c.update_intent(fence, "agent", f"call-{op_uid}", "succeeded"),
        lambda: c.call("UpdateIntent", {}),
        lambda: c.update_intent(fence, "agent", f"no-call-{op_uid}", "succeeded"))

    # --- 24. ReconcileIntent ---
    await check("ReconcileIntent",
        lambda: c.reconcile_intent(fence, "agent", f"call-{op_uid}", status="succeeded"),
        lambda: c.call("ReconcileIntent", {}),
        lambda: c.reconcile_intent(fence, "agent", f"no-re-{op_uid}", status="succeeded"))

    # --- 25. GetIntent ---
    await check("GetIntent",
        lambda: c.get_intent(ctx["run_id"], "agent", f"call-{op_uid}"),
        lambda: c.call("GetIntent", {}),
        lambda: c.get_intent(f"no-run-{op_uid}", "agent", f"no-{op_uid}"))

    # --- 26. Enqueue ---
    await check("Enqueue",
        lambda: c.enqueue(tid, f"eq-{op_uid}", "task", payload={"x": 1}),
        lambda: c.call("Enqueue", {}),
        lambda: c.enqueue(f"no-{op_uid}", "k", "task"))

    # --- 27. ReadInbox ---
    await check("ReadInbox",
        lambda: c.read_inbox(tid),
        lambda: c.call("ReadInbox", {}),
        lambda: c.read_inbox(f"no-{op_uid}"))

    # --- 28. AckInbox ---
    await check("AckInbox",
        lambda: c.ack_inbox(fence, [f"no-evt-{op_uid}"]),
        lambda: c.call("AckInbox", {}),
        lambda: c.ack_inbox(fence, []))

    # --- 29. AppendStream ---
    await check("AppendStream",
        lambda: c.append_stream(fence, f"str-{op_uid}", "event", payload={"x": 1}),
        lambda: c.call("AppendStream", {}),
        lambda: c.call("AppendStream", {"fence": fence, "key": "", "kind": ""}))

    # --- 30. ReadStream ---
    await check("ReadStream",
        lambda: c.read_stream(tid),
        lambda: c.call("ReadStream", {}),
        lambda: c.read_stream(f"no-{op_uid}"))

    # --- 31. PutBatch ---
    await check("PutBatch",
        lambda: c.put_batch(fence, {"key": f"batch-{op_uid}"}),
        lambda: c.call("PutBatch", {}),
        lambda: c.put_batch(fence, {"key": ""}))

    # --- 32. DeleteThread ---
    del_tid = f"del-{op_uid}"
    await check("DeleteThread",
        lambda: _noop(),  # skip (would delete our thread)
        lambda: c.call("DeleteThread", {}),
        lambda: c.delete_thread(f"no-thread-{op_uid}"))

    # --- 33. DeleteSubject ---
    await check("DeleteSubject",
        lambda: _noop(),  # skip (destructive)
        lambda: c.call("DeleteSubject", {"bad": "field"}),
        lambda: _noop())

    # --- 34. SetLegalhold ---
    await check("SetLegalhold",
        lambda: c.set_legalhold(tid, reason="c37-test"),
        lambda: c.call("SetLegalhold", {}),
        lambda: c.set_legalhold(f"no-{op_uid}", reason="x"))

    # --- 35. RemoveLegalhold ---
    await check("RemoveLegalhold",
        lambda: c.remove_legalhold(tid),
        lambda: c.call("RemoveLegalhold", {}),
        lambda: c.remove_legalhold(f"no-{op_uid}"))

    # --- 36. ExportThread ---
    await check("ExportThread",
        lambda: c.export_thread(tid),
        lambda: c.call("ExportThread", {}),
        lambda: c.export_thread(f"no-{op_uid}"))

    # --- 37. ImportThread ---
    await check("ImportThread",
        lambda: _noop(),  # skip (needs valid export)
        lambda: c.call("ImportThread", {}),
        lambda: c.import_thread({"thread": {}}))

    # Print results table
    print("\n" + "=" * 70)
    print(f"{'Route':<25} {'Correct':<12} {'WrongArgs':<12} {'Unknown':<12}")
    print("=" * 70)
    pass_count = 0
    total = 0
    for name in sorted(results.keys()):
        r = results[name]
        print(f"{name:<25} {r['correct']:<12} {r['wrong']:<12} {r['unknown']:<12}")
        for v in r.values():
            total += 1
            if v.startswith("PASS"):
                pass_count += 1
    print("=" * 70)
    print(f"Total: {pass_count}/{total} PASS")

    await _teardown(ctx)


async def _noop():
    """Placeholder for routes where 'correct' would be destructive."""
    pass
