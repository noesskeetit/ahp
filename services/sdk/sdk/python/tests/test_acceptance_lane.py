"""The acceptance legs of a user image's turn, on a live stand.

Root's TASK 2 asks for one thing: a user image's turns, driven through the
standard entry point (``python -m ahp_state.container`` - not this file's own
``AgentService``), on a scope whose claims the stand's built-in model worker
cannot take, must show (a) a durable checkpoint, (b) no foreign executor in the
thread's own stream, (c) an interrupt that a documented answer route wakes, and
(d) no second external effect after a hard kill. Every leg prints its own verdict
line and fails or blocks; none of them can pass on a fake state, because none of
them import one.

Three legs refine those four rather than add to them: (d2) enters the effect's
ambiguity window on purpose and is killed inside it, (e) asks whether a reader can
read a gate back as a gate, and (e1) measures the durable rows that such a read
would be rebuilt from on the bytes that do not have the read yet. (f) answers a
gate the instant it appears, which is the ordering a watcher on the thread's own
stream falls into by accident.

Configuration is the platform's own hand-out (see ``tests/_live_stand.py``):
``AHP_STATE_HANDSHAKE`` naming ``base_url``, ``ca_file``, ``cert_file``,
``key_file``, ``task_token``. Two more inputs belong to these legs alone,
because they are the parts of the lane a State hand-out cannot name:

``AHP_SPACE_EDGE_URL``   the human/space edge, e.g. ``http://127.0.0.1:18381``,
                         the only place ``POST /v1/space/RespondHITL`` exists
``AHP_HUMAN_TOKEN_FILE`` a 0600 file holding one human bearer token (never
                         printed)

Only legs (c) and (e) consume those two: they are the two predicates about the
human-facing route itself. The rest reach the same states through State's own API,
because a durability claim that waits on a bearer token is a claim unmeasured.

The user image is the example image the container ships:
``AHP_AGENT_MODULE`` (default ``agent``, resolved with ``examples/
billing_agent`` on ``PYTHONPATH``, which is what the image's own ``/app`` does).

A missing input is reported as the named missing input - never as a pass, and
never as a leg quietly not run.
"""
from __future__ import annotations

import hashlib
import json
import asyncio
import os
import signal
import subprocess
import sys
import time
import uuid

import httpx
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_HERE = os.path.dirname(os.path.abspath(__file__))
_PKG = os.path.dirname(_HERE)

from _live_stand import STAND                                   # noqa: E402
from ahp_state.agent import AgentService                         # noqa: E402

pytestmark = STAND.skip_mark()

#: The step kinds the stand's built-in model worker writes. A thread whose stream
#: carries them was executed by that worker, whatever its scope said - which is
#: exactly the claim leg (b) is here to falsify.
FOREIGN_STEP_KINDS = frozenset({"model", "tool", "compaction"})

EDGE_URL = (os.environ.get("AHP_SPACE_EDGE_URL") or "").rstrip("/")
HUMAN_TOKEN_FILE = os.environ.get("AHP_HUMAN_TOKEN_FILE") or ""
AGENT_MODULE = os.environ.get("AHP_AGENT_MODULE") or "agent"


def _require_edge() -> tuple[str, str]:
    """The edge route + a human bearer, or a skip that names what is absent."""
    if not EDGE_URL:
        pytest.skip("leg needs AHP_SPACE_EDGE_URL (the human edge that serves "
                    "POST /v1/space/RespondHITL); the State hand-out does not name it")
    if not HUMAN_TOKEN_FILE or not os.path.exists(HUMAN_TOKEN_FILE):
        pytest.skip(f"leg needs AHP_SPACE_TOKEN_FILE/AHP_HUMAN_TOKEN_FILE to name an "
                    f"existing 0600 file with a human bearer ({HUMAN_TOKEN_FILE or 'unset'!r})")
    with open(HUMAN_TOKEN_FILE, encoding="utf-8") as handle:
        return EDGE_URL, handle.read().strip()


def _handshake_file(tmp_path) -> str:
    """Re-mount the resolved hand-out for a child process, at 0600, unprinted."""
    path = tmp_path / "handshake.json"
    config = dict(STAND.config)
    payload = {key: config[key] for key in
               ("base_url", "ca_file", "cert_file", "key_file", "task_token")}
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(path, 0o600)
    return str(path)


def _entry_env(handshake_path: str, worker: str, extra: dict | None = None) -> dict:
    """The environment the shipped container image sets, minus the image."""
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [_PKG, os.path.join(_PKG, "examples", "billing_agent"), env.get("PYTHONPATH", "")])
    env.update({
        "AHP_AGENT_MODULE": AGENT_MODULE,
        "AHP_STATE_HANDSHAKE": handshake_path,
        "AHP_WORKER_ID": worker,
        # Serve until stopped, exactly as a pod does. A leg does not own the
        # queue: an entry limited to one turn spends that turn on whatever the
        # stand queued first (a run a previous crashed entry left ``pending``)
        # and exits before the leg's own run is ever claimed - which reads as
        # "my run is stuck pending" and is nothing of the kind.
        "AHP_MAX_TURNS": "0",
        "AHP_IDLE_WAIT_S": "0.2",
    })
    # One world per leg, always. The image's own default is a path relative to its
    # workdir, so legs that leave it alone all charge into the same directory - and
    # then a receipt counted by one leg can have been written by another, which is
    # how one run's restart and a different run's ordinary turn looked like a double
    # charge. A receipt names the tool and its args and no run id, so the directory
    # IS the attribution: sharing it forfeits the claim.
    env.setdefault("AHP_EFFECT_WORLD_DIR",
                   os.path.join(os.path.dirname(handshake_path), "effect-world"))
    env.update(extra or {})
    return env


def spawn_entry(handshake_path: str, worker: str, log_path,
                extra: dict | None = None) -> subprocess.Popen:
    """Start the standard entry point. This is the leg's subject, not its harness.

    ``extra`` adds environment the way an operator would (a world directory, a
    transport), never a different code path: the command stays the shipped
    ``python -m ahp_state.container``.
    """
    handle = open(log_path, "wb")
    return subprocess.Popen([sys.executable, "-m", "ahp_state.container"],
                            cwd=_PKG, env=_entry_env(handshake_path, worker, extra),
                            stdout=handle, stderr=subprocess.STDOUT)


def stop_entry(proc: subprocess.Popen, timeout: float = 60.0) -> int:
    """Ask the platform's own entry point to stop, and require that it did.

    ``SIGTERM`` is the stop the shipped image is asked for, and the entry point
    promises a clean exit on it - so a leg that stops its worker measures the
    same shutdown a pod gets instead of a ``kill()`` no orchestrator ever sends.
    A non-zero exit here is the leg failing, not the harness tidying up.
    """
    if proc.poll() is not None:
        return proc.returncode
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        raise AssertionError(
            "the standard entry point did not stop on SIGTERM; it is still a "
            "writer somewhere, which is the failure this leg exists to catch"
        ) from None
    assert proc.returncode == 0, (
        f"the entry point exited {proc.returncode} after a stop signal - the "
        f"contract says a stopped container exits 0")
    return proc.returncode


async def queue_turn(payload: dict) -> tuple[str, str]:
    """Queue one turn of the user image through the plane. -> (thread_id, run_id)."""
    client = STAND.client()
    uid = uuid.uuid4().hex[:10]
    thread = await client.create_thread(f"acc-{uid}", f"acc-{uid}")
    run = await client.create_run(thread["id"], f"acc-run-{uid}", payload=payload)
    await client.close()
    return thread["id"], run["id"]


def sha_of(values) -> str:
    """A stable digest of a set of ids, so a leg's output can be quoted in a post."""
    digest = hashlib.sha256()
    for value in sorted(str(v) for v in values):
        digest.update(value.encode())
        digest.update(b"\n")
    return digest.hexdigest()[:16]


async def stream_events(thread_id: str) -> list[dict]:
    """Every event this thread's stream holds, drained (bounded, like the plane's)."""
    client = STAND.client()
    events: list[dict] = []
    after, pages = 0, 0
    try:
        while pages < 200:
            page = await client.read_stream(thread_id, after=after, limit=200)
            batch = page.get("events") or []
            events.extend(batch)
            if not batch:
                break
            after = max([ev.get("sequence") or 0 for ev in batch] + [after])
            pages += 1
    finally:
        await client.close()
    return events


async def inbox_of(client, thread_id: str) -> list[dict]:
    """This thread's UNACKED inbox events, drained (the plane pages by ``sequence``).

    The inbox is not the stream: ``ReadInbox`` is where a gate's question and a
    human's answer are actually written, and it answers with unacked events only,
    so "still in the box" here means "nobody has consumed it yet".
    """
    events: list[dict] = []
    after, pages = 0, 0
    while pages < 200:
        page = await client.read_inbox(thread_id, after=after, limit=200)
        batch = page.get("events") or []
        events.extend(batch)
        if not batch:
            break
        after = max([ev.get("sequence") or 0 for ev in batch] + [after])
        pages += 1
    return events


async def wait_for_run(run_id: str, statuses: set, timeout_s: float = 90.0) -> dict:
    """Poll the plane until this run reaches one of ``statuses``."""
    client = STAND.client()
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            run = await client.get_run(run_id)
            if run.get("status") in statuses:
                return run
            await asyncio.sleep(0.25)
        raise AssertionError(f"run {run_id} never reached {sorted(statuses)} "
                             f"in {timeout_s:.0f}s (last: {run.get('status')})")
    finally:
        await client.close()


async def checkpoints(thread_id: str, namespace: str = "graph") -> list[dict]:
    client = STAND.client()
    try:
        return await client.list_checkpoints(thread_id, namespace, 1000)
    finally:
        await client.close()


async def effect_rows(run_id: str, node: str, call: str) -> dict:
    """One effect's journal row: the plane's ``(run, node, call)`` identity.

    ``GetIntent`` answers with ``state.Intent`` directly (``handlers.go``), so a
    missing row is a ``NOT_FOUND`` error rather than an empty object - which is the
    honest answer to "did this turn touch the world", and it is raised, not hidden.
    """
    client = STAND.client()
    try:
        return await client.get_intent(run_id, node, call)
    finally:
        await client.close()


async def test_a_completed_turn_leaves_a_durable_checkpoint_a_stranger_reads(tmp_path):
    """Leg (a): the turn's durability is a row on the server, not a process memory.

    The container entry point runs as its own process and the reader is a second,
    unrelated mTLS client, so a checkpoint only shows up here if it is durable.
    """
    handshake = _handshake_file(tmp_path)
    thread_id, run_id = await queue_turn({"input": {"request": {"amount": 10}}})

    proc = spawn_entry(handshake, "acc-leg-a", tmp_path / "entry-a.log")
    try:
        run = await wait_for_run(run_id, {"waiting", "done", "failed", "canceled"})
        stop_entry(proc)
    finally:
        if proc.poll() is None:
            proc.kill()

    heads = await checkpoints(thread_id)
    ids = [cp["id"] for cp in heads]        # state.Checkpoint names it "id"
    print(f"\nleg-a run={run_id} status={run.get('status')} "
          f"checkpoints={len(ids)} sha={sha_of(ids)}")
    assert ids, ("the standard entry finished a turn and left no checkpoint in "
                 "namespace 'graph' - a `done` without a checkpoint is not a passed leg")
    assert run.get("status") != "failed", run


async def test_the_thread_stream_names_no_built_in_executor(tmp_path):
    """Leg (b): my thread's stream is mine.

    Claim separation is (tenant, deployment) and nothing else, so a scope whose
    claims the built-in model worker cannot take has to show it in the one place
    the platform cannot hide it: the thread's own event stream.
    """
    handshake = _handshake_file(tmp_path)
    thread_id, run_id = await queue_turn({"input": {"request": {"amount": 10}}})
    proc = spawn_entry(handshake, "acc-leg-b", tmp_path / "entry-b.log")
    try:
        await wait_for_run(run_id, {"waiting", "done", "failed", "canceled"})
        stop_entry(proc)
    finally:
        if proc.poll() is None:
            proc.kill()

    events = await stream_events(thread_id)
    kinds = sorted({ev.get("kind", "") for ev in events})
    foreign = sorted(k for k in kinds if k in FOREIGN_STEP_KINDS)
    print(f"\nleg-b thread={thread_id} events={len(events)} kinds={kinds} "
          f"foreign={foreign} sha={sha_of(kinds)}")
    assert events, "the thread has no stream at all: nothing was measured, so nothing passed"
    assert not foreign, (
        f"{foreign} in this thread's stream means the stand's built-in executor ran "
        f"this scope - the hand-out's deployment is not a scope the worker cannot claim")


async def test_respond_hitl_on_the_documented_route_wakes_the_same_run(tmp_path):
    """Leg (c): a user answers the gate over the route the platform publishes.

    The gate the container parks on is ``hitl:<run_id>:<interrupt_id>``; the answer
    route is ``POST /v1/space/RespondHITL``. The claim being tested is narrow and
    falsifiable: the run that comes back is the SAME run id, and the answer is what
    the graph's resume value becomes.
    """
    edge, human_token = _require_edge()
    handshake = _handshake_file(tmp_path)
    thread_id, run_id = await queue_turn({"input": {"request": {"amount": 500}}})

    proc = spawn_entry(handshake, "acc-leg-c", tmp_path / "entry-c.log")
    try:
        parked = await wait_for_run(run_id, {"waiting"})
        assert parked.get("wait_for") == "hitl_response", parked

        gates = [ev for ev in await stream_events(thread_id)
                 if ev.get("kind") == "agent_input_required"]
        assert gates, "the gate was never published to the thread's stream"
        interrupts = (gates[-1].get("payload") or {}).get("interrupts") or []
        assert interrupts, gates[-1]
        hitl_id = f"{run_id}:{interrupts[-1]['id']}"

        async with httpx.AsyncClient(timeout=20.0) as edge_client:
            answer = await edge_client.post(
                f"{edge}/v1/space/RespondHITL",
                headers={"Authorization": f"Bearer {human_token}",
                         "Content-Type": "application/json",
                         "X-AHP-Session": "1"},
                json={"thread_id": thread_id, "hitl_id": hitl_id,
                      "approved": True, "response": "yes"})
        print(f"\nleg-c POST {edge}/v1/space/RespondHITL -> {answer.status_code} "
              f"hitl_id={hitl_id}")
        assert answer.status_code == 200, (
            f"the published answer route refused the gate this run asked: "
            f"{answer.status_code} {answer.text[:200]}")

        woke = await wait_for_run(run_id, {"pending", "running", "done", "failed"})
        assert woke["id"] == run_id, "the answer produced a different run"
        stop_entry(proc, 90)
        events = await stream_events(thread_id)
        done = [ev for ev in events if ev.get("kind") == "agent_completed"]
        assert done, f"the resumed run ended without a terminal frame: {[e.get('kind') for e in events]}"
    finally:
        if proc.poll() is None:
            proc.kill()


async def test_the_gate_this_container_asks_is_readable_as_a_gate(tmp_path):
    """Leg (e): a reader can ASK about my gate - the payload shape is the contract.

    ``GetHITL`` (added by vm2-state-runner in ``1799775``, ``internal/space/
    hitl_gate.go``) reconstructs a gate from its inbox event, parsing the payload
    as ``space.HITLRequest`` and taking the id from the KEY. So this leg is the
    direct test of the shape this container writes: pending before the answer,
    then the recorded decision - and it is the only leg that proves the gate is
    readable by someone other than the executor that asked it.

    ``1799775`` is not in the accepted bytes yet, so a missing route is reported
    as the measured code. That is a BLOCKED leg, printed as such - not a pass.
    """
    edge, human_token = _require_edge()
    handshake = _handshake_file(tmp_path)
    thread_id, run_id = await queue_turn({"input": {"request": {"amount": 500}}})
    proc = spawn_entry(handshake, "acc-leg-e", tmp_path / "entry-e.log")
    try:
        await wait_for_run(run_id, {"waiting"})
        events = await stream_events(thread_id)
        gates = [ev for ev in events if ev.get("kind") == "agent_input_required"]
        assert gates, "the gate was never published to the thread's stream"
        interrupts = (gates[-1].get("payload") or {}).get("interrupts") or []
        hitl_id = f"{run_id}:{interrupts[-1]['id']}"

        headers = {"Authorization": f"Bearer {human_token}",
                   "Content-Type": "application/json", "X-AHP-Session": "1"}
        async with httpx.AsyncClient(timeout=20.0) as edge_client:
            before = await edge_client.post(f"{edge}/v1/space/GetHITL", headers=headers,
                                            json={"thread_id": thread_id,
                                                  "hitl_id": hitl_id})
            print(f"\nleg-e GET-gate before the answer -> {before.status_code}")
            if before.status_code in (404, 405):
                pytest.skip(f"BLOCKED: the stand cannot read a gate yet "
                            f"(POST /v1/space/GetHITL -> {before.status_code}); "
                            f"1799775 is not in the bytes this lane runs - measured "
                            f"on 90cd4e5: no internal/space/hitl_gate.go at all, "
                            f"SubmitHITL mints its own id in coordinator.go:1211")
            assert before.status_code == 200, f"{before.status_code} {before.text[:200]}"
            gate = before.json()
            assert gate.get("status") == "pending", gate
            assert gate.get("id") == hitl_id, gate
            assert gate.get("prompt"), f"the reader sees no question: {gate}"

            await _answer_gate(thread_id, run_id)
            after = await edge_client.post(f"{edge}/v1/space/GetHITL", headers=headers,
                                           json={"thread_id": thread_id,
                                                 "hitl_id": hitl_id})
        assert after.status_code == 200, f"{after.status_code} {after.text[:200]}"
        decided = after.json()
        print(f"leg-e gate after the answer -> status={decided.get('status')} "
              f"response_present={bool(decided.get('response'))}")
        assert decided.get("status") in ("approved", "rejected"), decided
    finally:
        if proc.poll() is None:
            proc.kill()


async def test_a_gate_is_addressable_by_a_reader_that_never_asked(tmp_path):
    """Leg (e1): the half of ``GetHITL`` that exists in these bytes, measured directly.

    Leg (e) wants ``POST /v1/space/GetHITL``, and no lane runs bytes that have it -
    measured now on the human edge this lane can reach: ``NOT_FOUND: space: unknown
    operation GetHITL``. What that route does is reconstruct a gate out of durable
    rows, and every one of those rows is reachable through State's own API. So this
    leg measures the reconstruction itself, from the position the missing route would
    be in: a second client that asked nothing, parked nothing and knows only the
    thread and the run must be able to find the open question and the id to answer.

    * the run's own asked-journal names the gate (``HITL_ASKED_NAMESPACE``), and the
      id a reader must answer is ``<run_id>:<interrupt_id>`` - the same id the stream
      frame implies, so the two views of the gate cannot disagree;
    * the question sits in the thread's INBOX, unacked, under exactly the key
      ``RespondHITL`` answers to. A gate only on the stream would be readable and
      still unanswerable, which is the failure this leg exists to catch.

    This is NOT leg (e): no human-facing route is exercised and this leg does not
    claim one. It is the strongest statement the gate contract allows until
    ``1799775`` is in the candidate.
    """
    handshake = _handshake_file(tmp_path)
    thread_id, run_id = await queue_turn({"input": {"request": {"amount": 500}}})
    proc = spawn_entry(handshake, "acc-leg-e1", tmp_path / "entry-e1.log")
    try:
        await wait_for_run(run_id, {"waiting"})
        interrupt_id = await _wait_for_gate_frame(thread_id)
        hitl_id = f"{run_id}:{interrupt_id}"

        reader = STAND.client()
        try:
            asked = (await reader.get_kv(AgentService.HITL_ASKED_NAMESPACE, run_id)) or {}
            journal = asked.get("value") or {}
            assert hitl_id in (journal.get("asked") or []), (
                f"the asked-journal of run {run_id} does not name the gate its own "
                f"stream published: journal={journal} gate={hitl_id}")
            assert journal.get("wait_for") == AgentService.HITL_WAIT_FOR, (
                f"a gate journaled to wake on {journal.get('wait_for')!r} is parked "
                f"forever, because State wakes on {AgentService.HITL_WAIT_FOR!r}: {journal}")

            key = f"{AgentService.HITL_GATE_KEY_PREFIX}{hitl_id}"
            gates = [ev for ev in await inbox_of(reader, thread_id) if ev.get("key") == key]
            assert gates, (
                f"no inbox event under key {key!r}: a reader other than the executor "
                f"cannot address this gate, so GetHITL would reconstruct nothing and "
                f"RespondHITL would answer nothing - the gate exists only in this "
                f"container's memory")
            gate = gates[-1]
            assert not gate.get("acked"), (
                f"the open question was consumed before anyone answered it: {gate}")
            assert gate.get("kind") == AgentService.HITL_GATE_KIND, gate
            payload = gate.get("payload") or {}
            assert payload.get("status") == "pending", payload
            assert payload.get("prompt"), f"a gate a reader cannot read the question of: {payload}"
            assert payload.get("run_id") == run_id, payload
        finally:
            await reader.close()

        print(f"\nleg-e1 run={run_id} gate={hitl_id} journaled=True wait_for="
              f"{AgentService.HITL_WAIT_FOR} inbox_key={key} unacked=True "
              f"prompt=True - State-side reconstruction of the gate succeeds; the "
              f"human read itself stays blocked on GetHITL (leg e)")
    finally:
        if proc.poll() is None:
            proc.kill()


ATTEMPTS = int(os.environ.get("AHP_HITL_RACE_TRIES", "3"))


async def _wait_for_gate_frame(thread_id: str, timeout_s: float = 60.0) -> str:
    """Wait until this run's open question is visible on the thread's stream."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        frames = [ev for ev in await stream_events(thread_id)
                  if ev.get("kind") == "agent_input_required"]
        if frames:
            interrupts = (frames[-1].get("payload") or {}).get("interrupts") or []
            if interrupts:
                return str(interrupts[-1]["id"])
        await asyncio.sleep(0.1)
    raise AssertionError(f"no gate frame appeared on thread {thread_id} in {timeout_s:.0f}s")


async def _wait_for_frame(thread_id: str, kind: str, timeout_s: float = 90.0):
    """The first event of ``kind`` on this thread's stream, or ``None``."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for event in await stream_events(thread_id):
            if event.get("kind") == kind:
                return event
        await asyncio.sleep(0.25)
    return None


async def test_an_answer_written_the_moment_the_gate_appears_still_resumes(tmp_path):
    """Leg (f): an approver faster than the park must not strand the run forever.

    The window is the one ``577e2a9`` names for Space's own gate: State wakes a
    parked run only in the moment an inbox event is INSERTED, so a decision that
    lands after the question is published and before ``Suspend`` arrives wakes
    nothing, and the park that follows waits for a signal that has already passed -
    with no error on either side, and the answer sitting unacked in the same inbox.
    A reader that approves by watching the thread's stream walks into this on the
    ordinary path, which is exactly what this leg is: it answers as soon as the open
    question is visible, without waiting for the run to park.

    Space repairs the window for the gates IT submits, and this container's gate
    never goes through ``SubmitHITL`` - the repair there is the container's own
    (``AgentService._hitl_rearm``). Whether the window was actually hit is measured,
    never assumed: an attempt counts only when the run was not yet parked at the
    moment the answer was accepted. If no attempt hit it, this leg says so as a
    BLOCKED skip rather than crediting a repair it never observed.

    The answer is written with State's own primitive, not over the human edge, and
    that is not a shortcut: the window belongs to State's insert-vs-park ordering
    and to this container's repair, neither of which the edge is part of.
    ``Coordinator.RespondHITL`` writes exactly one thing (``coordinator.go:438-447``:
    ``Enqueue(key="hitl_response:"+id, kind="hitl_response", {hitl_id, approved,
    response})``), and ``_answer_gate_via_state`` writes that same event with this
    run's own hand-out, so the wake this leg races is the same wake the route
    causes. The route itself - auth, the space coordinator, the human's own read -
    is leg (c)'s to prove and stays blocked there; nothing here credits it.
    """
    handshake = _handshake_file(tmp_path)
    raced: list[str] = []
    resumed: list[str] = []
    runs: list[str] = []
    for attempt in range(ATTEMPTS):
        thread_id, run_id = await queue_turn({"input": {"request": {"amount": 500}}})
        runs.append(run_id)
        proc = spawn_entry(handshake, f"acc-leg-f-{attempt}",
                           tmp_path / f"entry-f{attempt}.log")
        try:
            await _wait_for_gate_frame(thread_id)
            reader = STAND.client()
            try:
                before = await reader.get_run(run_id)
            finally:
                await reader.close()
            # the same event RespondHITL writes, written directly: see the docstring
            hitl_id = await _answer_gate_via_state(thread_id, run_id)
            if before.get("status") != "waiting":
                raced.append(run_id)
                print(f"\nleg-f attempt {attempt} beat the park: run was "
                      f"{before.get('status')!r} when the answer was accepted "
                      f"(hitl_id={hitl_id})")
            # Await the TURN, not the run's status, and await it while the entry is
            # still serving. A run brought back to ``pending`` by the repair is not
            # yet a resumed turn: a worker has to claim it, and a leg that stops its
            # worker first and then looks for the terminal frame is measuring its own
            # impatience - it would report the lost wake in bytes that never had one.
            closed = await _wait_for_frame(thread_id, "agent_completed", timeout_s=90.0)
            back = await wait_for_run(run_id, {"done", "failed"}, timeout_s=30.0)
            stop_entry(proc, 90)
            assert closed is not None, (
                f"attempt {attempt}: the answer is in this thread and the run is "
                f"still parked: run={back} hitl_id={hitl_id} - the lost wake is "
                f"alive in these bytes (see entry-f{attempt}.log for what the "
                f"serving entry did with the answer)")
            assert back["id"] == run_id, back
            assert back["status"] == "done", (
                f"attempt {attempt}: the turn closed but the run says "
                f"{back['status']!r}: {back}")
            resumed.append(run_id)
        finally:
            if proc.poll() is None:
                proc.kill()

    print(f"\nleg-f attempts={ATTEMPTS} answers_that_beat_the_park={len(raced)} "
          f"turns_resumed={len(resumed)} runs_sha={sha_of(runs)}")
    assert len(resumed) == ATTEMPTS, (
        f"{ATTEMPTS - len(resumed)} of {ATTEMPTS} attempts left a run parked with its "
        f"own answer in the thread: resumed={resumed}")
    if not raced:
        pytest.skip("BLOCKED: leg ran but never hit the window - every answer landed "
                    "after the park, so the repair was never exercised; raise "
                    "AHP_HITL_RACE_TRIES or answer from a faster reader")


async def _answer_gate(thread_id: str, run_id: str) -> str:
    """Answer this run's open gate over the published route. -> the hitl_id used."""
    edge, human_token = _require_edge()
    gates = [ev for ev in await stream_events(thread_id)
             if ev.get("kind") == "agent_input_required"]
    assert gates, "the gate was never published to the thread's stream"
    interrupts = (gates[-1].get("payload") or {}).get("interrupts") or []
    assert interrupts, gates[-1]
    hitl_id = f"{run_id}:{interrupts[-1]['id']}"
    async with httpx.AsyncClient(timeout=20.0) as edge_client:
        answer = await edge_client.post(
            f"{edge}/v1/space/RespondHITL",
            headers={"Authorization": f"Bearer {human_token}",
                     "Content-Type": "application/json", "X-AHP-Session": "1"},
            json={"thread_id": thread_id, "hitl_id": hitl_id,
                  "approved": True, "response": "yes"})
    assert answer.status_code == 200, (
        f"the published answer route refused the gate this run asked: "
        f"{answer.status_code} {answer.text[:200]}")
    return hitl_id


async def test_a_hard_kill_after_the_effect_charges_exactly_once(tmp_path):
    """Leg (d): kill -9 between the effect and the release does not charge twice.

    The kill is not simulated - the entry point is a real process and it is sent
    SIGKILL once the gate is answered, which is the window the effect journal
    exists for. What is then measured is the durable record of the turn, from a
    second process that knows nothing about the first:

    * the gate was asked ONCE (``agent_input_required``), because a restart that
      re-entered the node would ask again;
    * the turn closed ONCE (``agent_completed``), i.e. no second life completed
      the same run and appended its own terminal frame;
    * the effect's intent row is in a terminal state under one (run, node, call)
      identity, and a second life that could not decide says so on the stream
      (``agent_awaiting_reconciliation``) instead of dispatching again.

    What this leg cannot see is the tool's own ledger behind the gateway - the
    journal is the platform's half of at-most-once, and it asserts that half.

    The gate is answered with State's own primitive rather than over the human
    edge, for the same reason leg (f) is: nothing about a restart re-charging or
    not re-charging depends on who wrote the answer, and ``RespondHITL`` writes
    exactly this event (``coordinator.go:438-447``). Waiting for a human bearer
    here would only leave the at-most-once predicate unmeasured; the route is
    leg (c)'s, and this leg does not claim it.
    """
    # The leg names the identity it later reads. Effects are journaled under
    # (run, node, call) and the entry's node comes from AHP_EFFECT_NODE, whose
    # default is "agent" (container/__main__.py:173); querying "charge" without
    # setting it asks for a row this turn never wrote and reads as NOT_FOUND.
    # The world is this leg's own directory for the same reason the node is: the
    # image's default is a path relative to its workdir, and a leg that counts
    # someone else's receipts is not counting anything.
    world = tmp_path / "world"
    world.mkdir()
    effects_env = {"AHP_EFFECT_WORLD_DIR": str(world), "AHP_EFFECT_NODE": "charge"}
    handshake = _handshake_file(tmp_path)
    thread_id, run_id = await queue_turn({"input": {"request": {"amount": 500}}})

    first = spawn_entry(handshake, "acc-leg-d-1", tmp_path / "entry-d1.log", effects_env)
    try:
        await wait_for_run(run_id, {"waiting"})
        hitl_id = await _answer_gate_via_state(thread_id, run_id)
        first.send_signal(signal.SIGKILL)
        first.wait(timeout=10)
    finally:
        if first.poll() is None:
            first.kill()

    killed = await stream_events(thread_id)
    for _ in range(3):
        second = spawn_entry(handshake, "acc-leg-d-2", tmp_path / "entry-d2.log", effects_env)
        try:
            run = await wait_for_run(run_id, {"done", "failed"}, timeout_s=60.0)
        except AssertionError:
            stop_entry(second, 30)
            continue
        stop_entry(second)
        break
    else:
        pytest.fail(f"run {run_id} never closed after the kill (gate {hitl_id}) - "
                    f"the delivery after SIGKILL did not resume the checkpoint")

    events = await stream_events(thread_id)
    kinds = [ev.get("kind") for ev in events]
    asks = kinds.count("agent_input_required")
    closes = kinds.count("agent_completed")
    effects = await effect_rows(run_id, "charge", "effect-0")
    touches = await _world_touches(world)
    print(f"\nleg-d run={run_id} status={run.get('status')} gate_asks={asks} terminal_frames={closes} "
          f"intent={effects.get('status')} target={effects.get('target')} world_touches={len(touches)} "
          f"awaiting_reconciliation={'agent_awaiting_reconciliation' in kinds}")
    assert len(touches) == 1, (
        f"{len(touches)} receipts in the world for this one run (run={run.get('status')}, "
        f"intent={effects.get('status')}): the journal is not the only witness, and the "
        f"user was charged {len(touches)} times through the standard entry")
    assert asks == 1, f"the question was asked {asks} times - the restart re-entered the node"
    assert closes == 1, (
        f"{closes} terminal frames for one run - a second life completed this turn "
        f"separately, which is what a duplicated effect looks like from here")
    assert effects.get("status") in ("succeeded", "failed", "unknown"), (
        f"the effect journal has no terminal row for this effect: {effects}")


async def _answer_gate_via_state(thread_id: str, run_id: str) -> str:
    """Answer this run's gate with the State primitive ``RespondHITL`` writes.

    NOT a substitute for leg (c), which demands the published human route. This
    exists so the durability leg can reach the effect at all: the answer becomes
    an inbox event keyed ``hitl_response:<hitl_id>`` under kind ``hitl_response``,
    which is precisely what the SDK reads back, written here with this run's own
    hand-out against this run's own thread.

    It is the same event the route writes, field for field - measured against
    ``Coordinator.RespondHITL`` in ``internal/space/coordinator.go:438-447``, which
    is a key, a kind and ``{hitl_id, approved, response}`` and nothing else. The
    ``response`` is a string for the same reason: it becomes the graph's resume
    value (``AgentService._hitl_answer`` takes ``payload["response"]``), so a human
    typing an answer and this helper put the same shape in front of the graph.
    """
    gates = [ev for ev in await stream_events(thread_id)
             if ev.get("kind") == "agent_input_required"]
    assert gates, "the gate was never published to the thread's stream"
    interrupts = (gates[-1].get("payload") or {}).get("interrupts") or []
    assert interrupts, gates[-1]
    hitl_id = f"{run_id}:{interrupts[-1]['id']}"
    client = STAND.client()
    try:
        await client.enqueue(thread_id, key=f"hitl_response:{hitl_id}",
                             kind="hitl_response",
                             payload={"hitl_id": hitl_id, "approved": True,
                                      "response": "yes"})
    finally:
        await client.close()
    return hitl_id


async def _world_touches(world) -> list:
    return sorted(world.glob("*.json"))


async def test_a_kill_inside_the_effect_window_through_the_shipped_entry_charges_once(
        tmp_path):
    """Leg (d2): the ambiguity window, entered on purpose, through the real entry.

    Everything else about this turn is the platform's own path: the shipped
    ``python -m ahp_state.container``, the user image the Dockerfile ships, a real
    gate answered and resumed. Only the world is rigged - it lands the charge and
    then hangs, so the ``SIGKILL`` below arrives after the money moved and before
    the journal learned about it. That is the one moment at-most-once is worth
    anything, and a second file in ``world`` is a real second charge, not a
    bookkeeping smell.
    """
    if not STAND.config:
        pytest.skip("leg needs AHP_STATE_HANDSHAKE naming a live stand")
    world = tmp_path / "world"
    world.mkdir()
    handshake = _handshake_file(tmp_path)
    rigged = {
        "AHP_EFFECT_WORLD_DIR": str(world),
        "AHP_EFFECT_TRANSPORT": "tests._blocking_world:charge",
        "AHP_EFFECT_NODE": "charge",
        # A killed worker keeps its lease until the TTL expires, and the default
        # is two minutes: a leg that waits less than that is measuring its own
        # patience, not the platform. (Same lesson as test_sigkill_no_second_effect.)
        "AHP_LEASE_TTL_MS": "15000",
    }
    thread_id, run_id = await queue_turn({"input": {"request": {"amount": 500}}})

    first = spawn_entry(handshake, "acc-leg-d2-1", tmp_path / "entry-d2a.log", rigged)
    try:
        await wait_for_run(run_id, {"waiting"})
        hitl_id = await _answer_gate_via_state(thread_id, run_id)
        deadline = time.monotonic() + 90.0
        while time.monotonic() < deadline and not await _world_touches(world):
            await asyncio.sleep(0.1)
        touches = await _world_touches(world)
        assert touches, (
            f"the charge never reached the world through the shipped entry "
            f"(gate {hitl_id}); the entry never got that far - see entry-d2a.log")
        first.send_signal(signal.SIGKILL)
        first.wait(timeout=10)
    finally:
        if first.poll() is None:
            first.kill()

    after_kill = await _world_touches(world)
    assert len(after_kill) == 1, f"the world was touched {len(after_kill)} times before the restart"

    # a new epoch now owns a run whose effect landed unrecorded. It must refuse to
    # guess, not re-fire: the row goes unknown and the run parks for reconciliation.
    second = spawn_entry(handshake, "acc-leg-d2-2", tmp_path / "entry-d2b.log", rigged)
    statuses: list[str] = []
    try:
        deadline = time.monotonic() + 150.0
        while time.monotonic() < deadline:
            statuses = [ev.get("kind") for ev in await stream_events(thread_id)]
            if "agent_awaiting_reconciliation" in statuses:
                break
            if len(await _world_touches(world)) > 1:
                break
            await asyncio.sleep(0.5)
    finally:
        stop_entry(second, 60)

    touches = await _world_touches(world)
    kinds = [ev.get("kind") for ev in await stream_events(thread_id)]
    row = await effect_rows(run_id, "charge", "effect-0")
    print(f"\nleg-d2 run={run_id} world_touches={len(touches)} gate={hitl_id} "
          f"intent={row.get('status')} awaiting_reconciliation="
          f"{'agent_awaiting_reconciliation' in kinds} kinds={sorted(set(kinds))}")

    assert len(touches) == 1, (
        f"{len(touches)} receipts after a restart of a run whose effect had already "
        f"landed - the user image charged twice, through the standard entry")
    assert row.get("status") in ("unknown", "dispatched"), (
        f"the journal answered {row.get('status')!r} for an effect that landed while "
        f"its worker was killed - it may not claim a result it never recorded")
    assert "agent_awaiting_reconciliation" in kinds, (
        f"the run neither re-charged nor said so after the restart (kinds={sorted(set(kinds))}, "
        f"intent={row.get('status')}); a silent stall is the failure mode this leg exists "
        f"to catch, and reconciliation is the honest answer - read entry-d2b.log before "
        f"blaming the platform, the second worker may never have been able to claim")
