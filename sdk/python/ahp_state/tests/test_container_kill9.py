"""A real process, really killed with SIGKILL, against a live State stand.

The contract asked for a container. This host has the Docker CLI but no
accessible daemon - ``docker info`` fails on ``/var/run/docker.sock`` - so no
image is built here and the report says so. What follows is the closest real
thing: a **separate OS process**, started with its own pid, holding a lease,
performing one durable external effect, and killed by the **parent** with
``os.kill(pid, SIGKILL)``. Not ``os._exit``, not a raised exception, not a
mocked clock: the kernel discards it, no ``finally``, no ``atexit``, nothing
buffered survives. Everything the restart guarantee has to survive a killed pod
is present; only the namespace isolation is not, and that is not what is under
test.

The scenario, in the order the platform performs it:

1. the parent creates the thread and the run over the wire and **claims the
   lease itself**, so the hand-out it passes down is a real ``ClaimRun`` answer;
2. child one adopts that lease (``run_leased`` - the restarted-pod path), fires
   the effect, makes it durable, and stays alive;
3. the parent waits for proof that the effect landed, then ``kill -9``;
4. child two adopts the SAME run and drives the turn to completion;
5. the effect file still holds exactly one line, the intent journal still holds
   one row keyed ``(run_id, node, call)``, and the run is terminal.

Requires a live stand; there is no fake here on purpose::

    AHP_STATE_HANDSHAKE=/path/handshake.json
"""

from __future__ import annotations

import json
import os
import pathlib
import signal
import subprocess
import sys
import time
import uuid

import pytest

from ahp_state import AHPStore, Effects
from ahp_state.agent import mtls_client

HANDSHAKE = os.environ.get("AHP_STATE_HANDSHAKE", "")
SRC = str(pathlib.Path(__file__).resolve().parents[2])

pytestmark = pytest.mark.skipif(
    not (HANDSHAKE and pathlib.Path(HANDSHAKE).is_file()),
    reason="no live State stand (AHP_STATE_HANDSHAKE unset): a fake stand "
           "cannot be killed, which is the whole point of this module",
)

CALL_ID = "charge-1"


def _child_env(hand: dict, fence: dict, token: str, thread_id: str,
               effects: pathlib.Path, ready: pathlib.Path,
               crash: str) -> dict:
    env = dict(os.environ)
    env["PYTHONPATH"] = SRC
    env["AHP_STATE_HANDSHAKE"] = HANDSHAKE
    env["AHP_STATE_URL"] = hand["base_url"]
    env["AHP_FENCE"] = json.dumps(fence)
    env["AHP_TOKEN"] = token
    env["AHP_THREAD_ID"] = thread_id
    env["AHP_EFFECTS_FILE"] = str(effects)
    env["AHP_READY_FILE"] = str(ready)
    env["AHP_WORKER"] = fence["worker"]
    # the run's own payload, exactly as a supervisor would hand it back to a
    # restarted pod; ``_graph_input`` decides whether it is applied or continued
    env["AHP_PAYLOAD"] = json.dumps({"input": {"messages": ["charge 42"]}})
    if crash:
        env["AHP_CRASH"] = crash
    else:
        env.pop("AHP_CRASH", None)
    return env


def _spawn(log: pathlib.Path, env: dict):
    """Start the pod. stdout/stderr go to a FILE: a pipe would let the child
    block on a full buffer while the parent waits on the ready file."""
    handle = log.open("ab")
    proc = subprocess.Popen([sys.executable, "-m", "ahp_state.tests.restart_child"],
                            cwd=SRC, env=env, stdout=handle, stderr=subprocess.STDOUT,
                            stdin=subprocess.DEVNULL)
    return proc, handle


def _wait_for(predicate, timeout: float, what: str):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError(f"timed out after {timeout}s waiting for {what}")


def _effect_lines(path: pathlib.Path) -> list[str]:
    if not path.is_file():
        return []
    return [ln for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]


def _outcome_of(log: pathlib.Path) -> dict:
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.startswith("AHP_OUTCOME "):
            return json.loads(line[len("AHP_OUTCOME "):])
    return {}


def test_a_sigkilled_pod_restarts_without_repeating_the_effect(tmp_path):
    hand = json.loads(pathlib.Path(HANDSHAKE).read_text(encoding="utf-8"))
    effects = tmp_path / "effects.jsonl"
    ready = tmp_path / "ready"
    first_log = tmp_path / "pod-1.log"
    second_log = tmp_path / "pod-2.log"
    context = f"kill9-{uuid.uuid4().hex[:10]}"

    # hand-off file, mode 0600: a hand-out is a credential, not a fixture
    handout = tmp_path / "handout.json"
    handout.write_text(json.dumps(hand), encoding="utf-8")
    handout.chmod(0o600)
    assert handout.stat().st_mode & 0o077 == 0o000

    async def scenario():
        from ahp_state import StateAPIClient

        async with mtls_client(hand) as parent:
            # drain stragglers: ClaimRun is FIFO for the whole scope
            while True:
                claim = await parent.claim_run(worker="kill9-drain", ttl_ms=60000)
                lease = claim.get("lease")
                if not lease:
                    break
                await parent.release({"run_id": lease["run"]["id"],
                                      "worker": lease["worker"],
                                      "epoch": lease["epoch"]}, "failed")

            thread = await parent.create_thread(context, f"thread:{context}")
            run = await parent.create_run(thread["id"], f"run:{context}",
                                          {"input": {"messages": ["charge 42"]}})
            claim = await parent.claim_run(worker=f"kill9-{context}", ttl_ms=300_000)
            lease = claim["lease"]
            fence = {"run_id": lease["run"]["id"], "worker": lease["worker"],
                     "epoch": lease["epoch"]}
            assert fence["run_id"] == run["id"], "the stand handed back another run"
            token = claim["task_token"]
            assert token, "ClaimRun must hand out a task token"

            # --- pod 1: effect fires, then the kernel takes it ---------------
            env = _child_env(hand, fence, token, thread["id"], effects, ready,
                             "wait-kill")
            pod, log_handle = _spawn(first_log, env)
            try:
                _wait_for(lambda: ready.is_file(), 180.0,
                          f"pod 1 to journal its effect (see {first_log})")
                lines = _wait_for(lambda: _effect_lines(effects), 30.0,
                                  "the effect file to gain a line")
                assert len(lines) == 1, lines

                os.kill(pod.pid, signal.SIGKILL)
                # poll() is None while alive and the negative signal number
                # afterwards, so the wait predicate is the rc itself
                killed = _wait_for(lambda: pod.poll(), 30.0,
                                   "the killed pod to be reaped")
                assert killed == -signal.SIGKILL, (
                    f"the pod did not die of SIGKILL but of {killed}; this is "
                    f"not evidence of an external kill")
            finally:
                if pod.poll() is None:
                    pod.kill()
                    pod.wait(timeout=30)
                log_handle.close()

            assert len(_effect_lines(effects)) == 1, "the effect count moved on kill"

            # The real graph task_id, observed from the child after dispatch.
            intent_node = json.loads(ready.read_text(encoding="utf-8"))["intent_node"]
            # the journal holds one row, under the durable identity
            intent = await Effects(parent, fence, None, task_id=intent_node).status(CALL_ID)
            assert intent is not None, "the effect was never journaled"
            assert intent["status"] == "succeeded", intent
            intent_id = intent.get("id")

            # --- pod 2: same run, adopted, driven to a terminal status -------
            env = _child_env(hand, fence, token, thread["id"], effects, ready, "")
            pod2, log2 = _spawn(second_log, env)
            try:
                outcome = _wait_for(lambda: _outcome_of(second_log), 240.0,
                                    f"pod 2 to report an outcome (see {second_log})")
            finally:
                if pod2.poll() is None:
                    pod2.kill()
                    pod2.wait(timeout=30)
                log2.close()

            assert outcome.get("run_id") == fence["run_id"], (
                f"the restarted pod ran a DIFFERENT run: {outcome}")
            assert outcome.get("status") in ("completed", "done"), outcome

            assert _effect_lines(effects) == lines, (
                "the external effect fired twice across a real SIGKILL: "
                f"{lines} -> {_effect_lines(effects)}")

            again = await Effects(parent, fence, None, task_id=intent_node).status(CALL_ID)
            assert again.get("id") == intent_id, (
                "the restart journaled a second intent instead of replaying "
                f"{intent_id}")
            assert again.get("status") == "succeeded", again

            # terminal: the run is finished and cannot be claimed again
            status_doc = await parent.call("GetRun", {"run_id": fence["run_id"]})
            status = status_doc.get("run", status_doc).get("status")
            assert status in ("done", "succeeded", "failed", "canceled"), (
                f"the run is not terminal after the restart: {status!r}")
            # and it never comes back. Other runs may surface in this scope at
            # any moment (an expired lease from an earlier session returns to
            # the queue asynchronously), so what is asserted is exactly the
            # claim: THIS run is not re-offered. Measured semantics of the
            # platform: Release done/failed/canceled is terminal, and only
            # Release(status="pending") re-queues a run under the same id.
            for attempt in range(8):
                idle = await parent.claim_run(worker=f"kill9-after-{attempt}",
                                              ttl_ms=60000)
                lease = idle.get("lease")
                if lease is None:
                    break
                other = lease["run"]
                assert other["id"] != fence["run_id"], (
                    "a terminal run went back into the queue for another worker")
                await parent.release({"run_id": other["id"], "worker": lease["worker"],
                                      "epoch": lease["epoch"]}, "canceled")

            # the receipt the graph produced is durable, read by a third client
            store = AHPStore(parent, fence)
            row = await store.aget(("ahp", "turns"), fence["run_id"])
            assert row is not None, "the turn journal row is gone"

    import asyncio
    asyncio.run(scenario())
