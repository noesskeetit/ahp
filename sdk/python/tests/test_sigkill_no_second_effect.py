"""A run whose worker stops existing mid-effect must not touch the world twice.

The other restart legs here close a client and build a new one, which proves the
*journal* survives a new adapter but not that a life can end without any
goodbye. A container killed by the platform gets exactly that: no `close()`, no
lease release, an effect that fired and was never acknowledged. This leg kills
its own worker with `SIGKILL` at that instant and asks the two questions the
acceptance asks of a real stand:

* did the world get touched exactly once - read out of the world file, not out
  of a process's memory, after the kill;
* does the next life refuse the effect instead of guessing, journal the
  ambiguity, and then - once reconciled - replay the recorded result without a
  second touch?

`SideEffectUnknown` is the contract's answer to the second question
(`ahp_state/intents.py`: dispatched/unknown is ambiguous, "never a silent
re-run, never assume done"). A pass on the first question but a re-firing on the
second is the defect this file exists to catch, so both are asserted.

Needs a live stand through the hand-out in ``AHP_STATE_HANDSHAKE``; without one
the leg skips with the missing input named, never passes.
"""
from __future__ import annotations

import asyncio
import json
import os
import select
import subprocess
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from _live_stand import STAND, claim_own_run        # noqa: E402
from ahp_state.agent import Effects                 # noqa: E402
from ahp_state.intents import IntentRecorder, SideEffectUnknown   # noqa: E402

pytestmark = STAND.skip_mark()

CHILD = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_sigkill_child.py")


def _read_marker(pipe, marker: str, timeout: float) -> str:
    """The child's line beginning with ``marker``, with a deadline.

    The child imports the SDK, so its first lines are library warnings, not
    protocol. Skipping to the marker is what lets the leg say "never claimed"
    and "never fired" as two different facts instead of one vague timeout; the
    deadline keeps a silent child from hanging the leg.
    """
    import time

    deadline = time.monotonic() + timeout
    skipped = []
    while time.monotonic() < deadline:
        ready, _, _ = select.select([pipe], [], [], max(0.1, deadline - time.monotonic()))
        if not ready:
            break
        line = pipe.readline()
        if not line:
            break
        line = line.strip()
        if line.startswith(marker):
            return line
        if line:
            skipped.append(line)
    raise AssertionError(f"child never said {marker!r} within {timeout}s; "
                         f"its other output: {skipped[-3:]}")


def _world_lines(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path) as fh:
        return [json.loads(line) for line in fh if line.strip()]


async def _queue_a_run() -> str:
    client = STAND.client()
    uid = uuid.uuid4().hex[:8]
    thread = await client.create_thread(f"ctx-sigkill-{uid}", f"sigkill-{uid}")
    run = await client.create_run(thread["id"], f"run-{uid}",
                                  payload={"task": "sigkill-no-second-effect"})
    await client.close()
    return run["id"]


def test_a_killed_worker_does_not_charge_twice(tmp_path):
    asyncio.run(_leg(tmp_path))


async def _leg(tmp_path):
    world = str(tmp_path / "world.jsonl")
    run_id = await _queue_a_run()

    child = subprocess.Popen([sys.executable, CHILD, run_id, f"killer-{uuid.uuid4().hex[:6]}", world],
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                             env={**os.environ, "PYTHONPATH": os.path.dirname(CHILD)})
    try:
        claimed = _read_marker(child.stdout, "CLAIMED", 90.0)
        assert claimed.startswith("CLAIMED"), f"child never claimed the run: {claimed!r}"
        fired = _read_marker(child.stdout, "FIRED", 60.0)
        assert fired == "FIRED", f"child never reached the effect: {fired!r}"
        child.kill()                       # SIGKILL: no close, no release, no finally
        child.wait(timeout=30)
    finally:
        if child.poll() is None:
            child.kill()

    touched = _world_lines(world)
    assert len(touched) == 1, f"the world was touched {len(touched)} times by one life: {touched}"

    # A second life: the dead worker's lease expires, the claim lands on a new epoch.
    client = STAND.client()
    lease = await claim_own_run(client, run_id, "after-the-kill", deadline_s=90.0)
    fence = {"run_id": lease["run"]["id"], "worker": lease["worker"], "epoch": lease["epoch"]}
    assert fence["epoch"] > int(claimed.split()[1]), \
        f"the new life did not take a higher epoch: {fence['epoch']}"

    calls = []

    async def transport(tool: str, args: dict) -> dict:
        calls.append((tool, args))
        return {"charged": args.get("amount")}

    effects = Effects(client, fence, transport)
    with pytest.raises(SideEffectUnknown) as caught:
        await effects.call("billing.charge", {"amount": 100})
    assert calls == [], f"the next life re-fired an effect that may already have landed: {calls}"
    journal = caught.value.intent if isinstance(getattr(caught.value, "intent", None), dict) else {}
    assert journal.get("status") in (None, "unknown"), \
        f"ambiguity was journaled as {journal.get('status')!r}, not 'unknown'"

    status = await IntentRecorder(client, fence).get("effect-0")
    assert status and status.get("status") == "unknown", \
        f"the journaled status is {(status or {}).get('status')!r}"

    # First, the identity rule this whole guarantee rests on, measured rather than
    # assumed: positions belong to ONE node execution. Reaching the same effect a
    # second time from the same view is a DIFFERENT logical effect (`effect-1`),
    # which is why the platform binds a view at the top of a node body - and why
    # a leg must not reuse a view across executions and call the result a replay.
    assert effects.last_call_id == "effect-0", f"first execution used {effects.last_call_id!r}"

    # Recovery has to finish, not just refuse: reconcile once, then let the NEXT
    # execution ask for the same logical effect. It must replay the recorded
    # result instead of reaching the world again.
    await IntentRecorder(client, fence).reconcile("effect-0", "succeeded",
                                                  result={"charged": 100, "reconciled": True})
    row = await IntentRecorder(client, fence).get("effect-0")
    assert row and row.get("status") == "succeeded", f"reconcile did not close the row: {row}"
    assert row.get("result") == {"charged": 100, "reconciled": True}, \
        f"ReconcileIntent kept {row.get('result')!r} instead of the outcome it was given"

    before = len(_world_lines(world))
    replayed = await Effects(client, fence, transport).call("billing.charge", {"amount": 100})
    # Measured before the payload is compared, because the two failure modes look
    # identical from here - a replay that returns the wrong value, and a replay
    # that charged again - and only the world and the call log tell them apart.
    assert calls == [], f"the next execution RE-FIRED an effect that already landed: {calls}"
    assert len(_world_lines(world)) == before == 1, \
        f"the world went from {before} to {len(_world_lines(world))} lines on a replay"
    assert replayed == {"charged": 100, "reconciled": True}, \
        f"a terminal intent replayed {replayed!r} instead of its recorded result"

    await client.release(fence, status="done")
    await client.close()
