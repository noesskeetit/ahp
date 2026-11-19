"""Restart must not repeat an external effect (PDF rev 3.2 §4.2, §9.2 "под дольше аренды").

Every case here crosses a real process boundary: the pod is a **child process**
that performs its external effect by appending one fsynced line to a file and then
kills itself with ``os._exit`` - no ``except``, no ``finally``, no atexit, nothing
buffered. The platform then hands the *same* run to a new pod under a new lease
epoch, which is what ``ClaimRun`` after a dead worker looks like.

Proved:

* an effect whose journal entry is durable is **replayed, never re-executed** -
  the file still holds one line after two pods;
* an effect that died in the ambiguous window (fired, journal not advanced) does
  **not** fire again: the run suspends for reconciliation, and only after an
  operator reconciles the intent does the turn finish - still one line;
* the turn is *continued* from the durable checkpoint: a restart never re-applies
  the input, so no node output is counted twice.

This is a local proof against the State contract double, not platform readiness.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from ahp_state import StateAPIClient
from ahp_state.intents import IntentRecorder
from ahp_state.tests.fake_state import FakeState, FakeStateServer, StateDoc

SDK_ROOT = Path(__file__).resolve().parents[2]      # .../sdk/python
CHILD = "ahp_state.tests.restart_child"


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


def run_child(server: FakeStateServer, effects: Path, *, worker: str = "pod-1",
              crash: str = "", run_id: str = "", hand: dict | None = None,
              ) -> subprocess.CompletedProcess:
    """Start one pod as its own process and wait for it, crash included."""
    """Start one pod as its own process and wait for it (crash included)."""
    env = dict(os.environ)
    env["PYTHONPATH"] = str(SDK_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["AHP_STATE_URL"] = server.url
    env["AHP_EFFECTS_FILE"] = str(effects)
    env["AHP_WORKER"] = worker
    if crash:
        env["AHP_CRASH"] = crash
    if hand is not None:      # an already-held lease: adopt it, do not claim
        env["AHP_FENCE"] = json.dumps(hand["fence"])
        env["AHP_TOKEN"] = hand["token"]
        env["AHP_THREAD_ID"] = hand["thread_id"]
        if hand.get("payload") is not None:
            env["AHP_PAYLOAD"] = json.dumps(hand["payload"])
    elif run_id:
        env["AHP_RUN_ID"] = run_id
    return subprocess.run([sys.executable, "-m", CHILD], env=env, cwd=str(SDK_ROOT),
                          capture_output=True, text=True, timeout=150)


def outcome_of(proc: subprocess.CompletedProcess) -> dict:
    lines = [line for line in proc.stdout.splitlines() if line.startswith("AHP_OUTCOME ")]
    assert lines, f"the pod reported no outcome (rc={proc.returncode}):\n" \
                  f"{proc.stdout}\n{proc.stderr}"
    return json.loads(lines[-1][len("AHP_OUTCOME "):])


def effects_of(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in
            path.read_text(encoding="utf-8").splitlines() if line.strip()]


def hand_over(server: FakeStateServer, run_id: str, thread_id: str,
              worker: str) -> dict:
    """What the reaper does with a dead pod's run: same run, new epoch, new token."""
    hand = server.re_claim(run_id, worker=worker)
    hand["thread_id"] = thread_id
    return hand


async def test_a_durable_effect_is_replayed_not_repeated(server, tmp_path):
    """Die after the effect is journaled, before the state write: one effect, two pods."""
    effects = tmp_path / "effects.jsonl"
    thread = server.ensure_thread(f"restart-{uuid.uuid4().hex[:8]}")
    run_id = server.queue_turn(thread, input={"messages": []})

    first = run_child(server, effects, worker="pod-1", crash="after-effect",
                      run_id=run_id)
    assert first.returncode == 7, first.stderr[-2000:]
    assert len(effects_of(effects)) == 1, "the first pod fired exactly one effect"
    assert server.state.doc.d["runs"][run_id]["status"] == "running", \
        "a dead pod must not have released its run"

    hand = hand_over(server, run_id, thread, "pod-2")
    second = outcome_of(run_child(server, effects, worker="pod-2", hand=hand))

    assert second["status"] == "done", second
    assert second["run_id"] == run_id, "a restart must continue the same run"
    assert len(effects_of(effects)) == 1, \
        "the restarted pod re-executed an effect that was already durable"
    assert second["result"]["receipt"] == {"charged": True, "reference": "bill-1"}
    assert second["result"]["messages"] == ["charged", "done"], \
        "the turn was replayed from the start instead of continued from the checkpoint"
    assert server.state.doc.d["runs"][run_id]["status"] == "done"


async def test_an_ambiguous_effect_suspends_instead_of_firing_again(server, tmp_path):
    """Die between the effect and its journal write: never guess, always reconcile."""
    effects = tmp_path / "effects.jsonl"
    thread = server.ensure_thread(f"restart-{uuid.uuid4().hex[:8]}")
    run_id = server.queue_turn(thread, input={"messages": []})

    first = run_child(server, effects, worker="pod-1", crash="in-effect", run_id=run_id)
    assert first.returncode == 8, first.stderr[-2000:]
    assert len(effects_of(effects)) == 1

    hand = hand_over(server, run_id, thread, "pod-2")
    second = outcome_of(run_child(server, effects, worker="pod-2", hand=hand))

    assert second["status"] == "awaiting_reconciliation", second
    assert second["wait_for"] == "reconcile:charge-1", second
    assert len(effects_of(effects)) == 1, \
        "an effect in the ambiguous window was fired a second time"
    assert server.state.doc.d["runs"][run_id]["status"] == "waiting"

    # the waiting run is released with its identity intact: same run id, new epoch
    released = server.release_wait(run_id, worker="pod-3")
    assert released["fence"]["run_id"] == run_id, \
        "a resumed run keeps its id - that is what keeps the intent identity stable"

    # an operator resolves what actually happened outside the process, under the
    # lease the run now has (ReconcileIntent is fenced: unknown -> terminal only)
    client = StateAPIClient(server.url, token=released["token"])
    intent_node = next(line.removeprefix("AHP_INTENT_NODE ") for line in first.stdout.splitlines()
                       if line.startswith("AHP_INTENT_NODE "))
    recorder = IntentRecorder(client, released["fence"], node=intent_node)
    assert (await recorder.get("charge-1"))["status"] == "unknown"
    await recorder.reconcile("charge-1", "succeeded",
                             result={"charged": True, "reference": "bill-1"})

    third = outcome_of(run_child(server, effects, worker="pod-3", hand=released))

    assert third["status"] == "done", third
    assert len(effects_of(effects)) == 1, "reconciliation caused a second execution"
    assert third["result"]["messages"] == ["charged", "done"], third["result"]
    assert third["result"]["receipt"] == {"charged": True, "reference": "bill-1"}


async def test_a_death_before_any_checkpoint_reapplies_the_input(server, tmp_path):
    """The other half of the rule: nothing checkpointed yet, so the input *is* the restart.

    The turn journal says the turn started, but LangGraph never got as far as its
    first checkpoint. Continuing from "nothing" would be a no-op turn; the only
    correct restart is to apply the run's input once, which is what the journal's
    ``has_input`` marker exists for.
    """
    effects = tmp_path / "effects.jsonl"
    thread = server.ensure_thread(f"restart-{uuid.uuid4().hex[:8]}")
    run_id = server.queue_turn(thread, input={"messages": []})

    first = run_child(server, effects, worker="pod-1", crash="after-journal",
                      run_id=run_id)
    assert first.returncode == 9, first.stderr[-2000:]
    assert effects_of(effects) == []
    assert server.state.doc.d["checkpoints"] == {}, "the pod checkpointed something"

    hand = hand_over(server, run_id, thread, "pod-2")
    hand["payload"] = {"input": {"messages": []}}
    second = outcome_of(run_child(server, effects, worker="pod-2", hand=hand))

    assert second["status"] == "done", second
    assert len(effects_of(effects)) == 1, "the effect fired once per pod, not once per turn"
    assert second["result"]["messages"] == ["charged", "done"], second["result"]


async def test_an_adopted_lease_without_input_or_checkpoint_fails_loudly(server, tmp_path):
    """No durable state and no input to apply is a visible failure, not a silent done."""
    effects = tmp_path / "effects.jsonl"
    thread = server.ensure_thread(f"restart-{uuid.uuid4().hex[:8]}")
    run_id = server.queue_turn(thread, input={"messages": []})

    first = run_child(server, effects, worker="pod-1", crash="after-journal",
                      run_id=run_id)
    assert first.returncode == 9, first.stderr[-2000:]

    hand = hand_over(server, run_id, thread, "pod-2")
    second = outcome_of(run_child(server, effects, worker="pod-2", hand=hand))

    assert second["status"] == "failed", second
    assert "no input" in second["error"], second
    assert effects_of(effects) == []
    assert server.state.doc.d["runs"][run_id]["status"] == "failed"
