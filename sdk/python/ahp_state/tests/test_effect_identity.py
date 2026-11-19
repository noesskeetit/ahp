"""Additional external-stand process restart probes.

PDF rev 3.2 keys intents by deterministic LangGraph task_id and a persisted
model tool_call_id, or by the call index when no tool_call_id exists. Positional
indices are safe only within one task invocation. Re-minting a model call ID in
the mutation node is intentionally an invalid graph shape: model and mutation
must be separate durable nodes. The mandatory isolated real-owner loop/restart
matrix is TestSDKEffectsRealOwners; these older probes need an external hand-out.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
import time
import uuid

import pytest

from ahp_state.agent import mtls_client
from ahp_state.intents import IntentRecorder

HANDSHAKE = os.environ.get("AHP_STATE_HANDSHAKE", "")
SRC = str(pathlib.Path(__file__).resolve().parents[2])

pytestmark = pytest.mark.skipif(
    not (HANDSHAKE and pathlib.Path(HANDSHAKE).is_file()),
    reason="no live State stand (AHP_STATE_HANDSHAKE unset): the duplicate this "
           "module measures only exists across two real processes",
)


def _env(hand: dict, fence: dict, token: str, thread_id: str,
         effects: pathlib.Path, identity: str, crash: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC
    env["AHP_STATE_HANDSHAKE"] = HANDSHAKE
    env["AHP_STATE_URL"] = hand["base_url"]
    env["AHP_FENCE"] = json.dumps(fence)
    env["AHP_TOKEN"] = token
    env["AHP_THREAD_ID"] = thread_id
    env["AHP_EFFECTS_FILE"] = str(effects)
    env["AHP_READY_FILE"] = ""
    env["AHP_WORKER"] = fence["worker"]
    env["AHP_EFFECT_IDENTITY"] = identity
    env["AHP_PAYLOAD"] = json.dumps({"input": {"messages": ["charge 42"]}})
    if crash:
        env["AHP_CRASH"] = crash
    else:
        env.pop("AHP_CRASH", None)
    return env


def _run_to_completion(env: dict, log: pathlib.Path, timeout: float) -> tuple[int, dict]:
    """Run one pod to its own end. Files, never pipes: a pipe lets the child
    block on a full buffer while the parent waits for an exit code."""
    with log.open("ab") as handle:
        proc = subprocess.Popen([sys.executable, "-m", "ahp_state.tests.restart_child"],
                                cwd=SRC, env=env, stdout=handle,
                                stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        deadline = time.monotonic() + timeout
        rc = None
        while time.monotonic() < deadline:
            rc = proc.poll()
            if rc is not None:
                break
            time.sleep(0.05)
        if rc is None:
            proc.kill()
            proc.wait(timeout=30)
            raise AssertionError(f"pod did not finish in {timeout}s, see {log}")
    outcome = {}
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("AHP_OUTCOME "):
            outcome = json.loads(line[len("AHP_OUTCOME "):])
    return rc, outcome


def _lines(path: pathlib.Path) -> list[str]:
    if not path.is_file():
        return []
    return [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


async def _claimed_run(hand: dict, worker: str) -> tuple:
    """A thread and a run of our own, with the lease claimed by us.

    ClaimRun is FIFO for the whole scope, so foreign stragglers are drained and
    released first - otherwise the claim hands back somebody else's run.
    """
    from ahp_state import StateAPIClient  # noqa: F401  (documents the wire user)

    context = f"effid-{uuid.uuid4().hex[:10]}"
    client = mtls_client(hand)
    while True:
        claim = await client.claim_run(worker=f"{worker}-drain", ttl_ms=60000)
        lease = claim.get("lease")
        if not lease:
            break
        await client.release({"run_id": lease["run"]["id"], "worker": lease["worker"],
                              "epoch": lease["epoch"]}, "failed")
    thread = await client.create_thread(context, f"thread:{context}")
    run = await client.create_run(thread["id"], f"run:{context}",
                                 {"input": {"messages": ["charge 42"]}})
    claim = await client.claim_run(worker=f"{worker}-{context}", ttl_ms=300_000)
    lease = claim["lease"]
    fence = {"run_id": lease["run"]["id"], "worker": lease["worker"],
             "epoch": lease["epoch"]}
    assert fence["run_id"] == run["id"], "the stand handed back another run"
    return client, thread, fence, claim["task_token"]


def _call_ids(log: pathlib.Path) -> list[str]:
    """The identities this pod journaled under, as it reported them.

    There is no ListIntents on this wire (measured: the state store exposes
    RecordIntent/UpdateIntent/ReconcileIntent/GetIntent only,
    ``internal/state/intents.go``), so the reproducibility of the key is read
    from what each pod says it used, and the row itself is read back by
    ``GetIntent`` on that id.
    """
    ids = []
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("AHP_CALL_ID "):
            ids.append(line[len("AHP_CALL_ID "):].strip())
    return ids


async def _effect_identity_leg(identity: str, tmp_path: pathlib.Path) -> dict:
    """One pod dies after a durable effect; a second adopts the same run.

    Returns the effect log lines, the identity each pod journaled under, and the
    fence, so each test asserts on the numbers rather than on this plumbing.
    """
    hand = json.loads(pathlib.Path(HANDSHAKE).read_text(encoding="utf-8"))
    effects = tmp_path / "effects.jsonl"
    client, thread, fence, token = await _claimed_run(hand, f"effid-{identity}")
    try:
        rc, _ = _run_to_completion(
            _env(hand, fence, token, thread["id"], effects, identity, "after-effect"),
            tmp_path / "pod-1.log", 240.0)
        assert rc == 7, f"pod 1 must die after the effect, rc={rc} (see pod-1.log)"
        after_pod1 = _lines(effects)
        assert len(after_pod1) == 1, f"pod 1 effect count {len(after_pod1)}: {after_pod1}"

        rc2, outcome = _run_to_completion(
            _env(hand, fence, token, thread["id"], effects, identity, ""),
            tmp_path / "pod-2.log", 240.0)
        assert rc2 == 0, f"pod 2 exited {rc2}, see {tmp_path / 'pod-2.log'}"
        assert outcome.get("run_id") == fence["run_id"], (
            f"pod 2 ran a DIFFERENT run: {outcome}")
        assert outcome.get("status") in ("completed", "done"), outcome
        return {
            "lines": _lines(effects),
            "pod1_node": next(line.removeprefix("AHP_INTENT_NODE ") for line in (tmp_path / "pod-1.log").read_text().splitlines() if line.startswith("AHP_INTENT_NODE ")),
            "pod1_ids": _call_ids(tmp_path / "pod-1.log"),
            "pod2_ids": _call_ids(tmp_path / "pod-2.log"),
            "fence": fence,
            "client": client,
        }
    except BaseException:
        await client.close()
        raise
    # the caller owns the client from here: it still has to read the row back


async def test_positional_identity_replays_instead_of_charging_twice(tmp_path):
    """The SDK default: a re-executed node counts from zero, meets its own
    journaled row, and replays the stored result. One effect, one row."""
    info = await _effect_identity_leg("positional", tmp_path)
    try:
        assert info["pod1_ids"] and info["pod1_ids"] == info["pod2_ids"], (
            "the identity did not survive the restart: pod 1 journaled under "
            f"{info['pod1_ids']}, pod 2 under {info['pod2_ids']}")
        assert len(info["lines"]) == 1, (
            "the external effect fired twice across a restart under the SDK's "
            f"positional identity: {info['lines']}")
        row = await IntentRecorder(info["client"], info["fence"],
                                   node=info["pod1_node"]).get(info["pod1_ids"][0])
        assert row is not None and row.get("status") == "succeeded", row
    finally:
        await info["client"].close()


async def test_reminting_call_id_in_mutation_node_duplicates_the_effect(tmp_path):
    """The trap the contract names, measured rather than quoted.

    This test asserts that keying an effect by a per-attempt id DOES duplicate
    the world touch. It is red-by-construction evidence, not a wish: if a future
    platform stops duplicating here, this test goes red and the rule in
    ``Effects.call`` gets a documented exception.
    """
    info = await _effect_identity_leg("tool-call-id", tmp_path)
    try:
        assert info["pod1_ids"] and info["pod2_ids"], "a pod reported no identity"
        assert set(info["pod1_ids"]).isdisjoint(info["pod2_ids"]), (
            "expected a per-attempt id to move across the restart and prove the "
            f"contract rule; both pods used {info['pod1_ids']}")
        assert len(info["lines"]) == 2, (
            "expected the per-attempt identity to duplicate the effect and prove "
            f"the contract rule; got {len(info['lines'])} line(s): {info['lines']}")
    finally:
        await info["client"].close()


async def test_a_reused_position_with_a_different_request_fails_loud(tmp_path):
    """Positional keys are safe because a collision is an error, not a replay of
    the wrong answer: same (run, node, call) with a different logical request is
    refused as INTENT_ARG_MISMATCH and the effect never runs a second time."""
    from ahp_state import Effects, StateAPIError

    hand = json.loads(pathlib.Path(HANDSHAKE).read_text(encoding="utf-8"))
    effects = tmp_path / "effects.jsonl"
    client, _thread, fence, _token = await _claimed_run(hand, "effid-collide")
    try:
        calls = {"n": 0}

        async def transport(tool: str, args: dict) -> dict:
            calls["n"] += 1
            with effects.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps({"tool": tool, "args": args}) + "\n")
            return {"charged": True}

        effects_view = Effects(client, fence, transport, node="charge")
        first = await effects_view.call("billing.charge", {"amount": 42})
        assert first == {"charged": True}
        assert effects_view.next_call_id() == "effect-1", "position must advance"

        # the same POSITION, reached with a different logical request
        second_view = Effects(client, fence, transport, node="charge")
        recorder = IntentRecorder(client, fence, node="charge")
        with pytest.raises(StateAPIError) as exc:
            await second_view.call("billing.charge", {"amount": 99})
        assert exc.value.code == "INTENT_ARG_MISMATCH", exc.value.code
        assert calls["n"] == 1, "the refused call still touched the world"
        assert len(_lines(effects)) == 1
        assert await recorder.get("effect-0") is not None
    finally:
        await client.close()
