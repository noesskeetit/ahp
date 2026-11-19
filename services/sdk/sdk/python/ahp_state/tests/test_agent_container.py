"""Block 1 / PDF §2.3 + §4.2 - the user's container runs a *custom* LangGraph graph.

These tests drive a real ``StateGraph`` through LangGraph's own Pregel executor
with :class:`ahp_state.agent.AgentService` as the only platform glue, against the
State contract double. They are local proofs (no live stand) of:

* the user's nodes execute in this process - the container is not a thin client
  over a platform-side runner;
* the turn runs with ``durability="sync"``;
* ``interrupt()`` suspends the run and a later ``resume`` continues it;
* steering applied between super-steps is visible to later nodes;
* a lost lease stops the container instead of letting it write on as a zombie.
"""

from __future__ import annotations

import json
import operator
import uuid

import pytest
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from typing_extensions import Annotated, TypedDict

from ahp_state import StateAPIClient
from ahp_state.agent import AgentService
from ahp_state.tests.fake_state import FakeState, FakeStateServer, StateDoc


class State(TypedDict, total=False):
    messages: list[str]
    counter: int
    # an additive channel: folding the same message twice is visible, which is
    # exactly what these tests need to rule out
    steering: Annotated[list[str], operator.add]


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


def service(server, builder, **kw) -> AgentService:
    return AgentService(graph=builder, base_url=server.url, worker="w1", **kw)


def enqueue_steering(server, thread_id: str, text: str, key: str) -> None:
    """What the coordinator does when a user steers a running turn."""
    server.post("Enqueue", {"thread_id": thread_id, "key": key, "kind": "steering",
                            "payload": {"state": {"steering": [text]}}})


def build_two_step(seen: list[str]):
    def build(ctx):
        g = StateGraph(State)

        def first(state):
            seen.append("first")
            return {"messages": ["first"], "counter": state.get("counter", 0) + 1}

        def second(state):
            seen.append("second")
            return {"messages": state["messages"] + ["second"], "counter": state["counter"] + 1}

        g.add_node("first", first)
        g.add_node("second", second)
        g.add_edge(START, "first")
        g.add_edge("first", "second")
        g.add_edge("second", END)
        return g.compile(checkpointer=ctx.saver, store=ctx.store)

    return build


async def test_custom_nodes_run_in_this_process(server):
    seen: list[str] = []
    thread = server.ensure_thread("ctx-basic")
    server.queue_turn(thread, input={"counter": 0})

    outcome = await service(server, build_two_step(seen)).run_once()
    assert outcome.status == "done", outcome
    assert seen == ["first", "second"]
    assert outcome.result["counter"] == 2
    assert outcome.result["messages"] == ["first", "second"]


async def test_turn_is_executed_with_sync_durability(server):
    """LangGraph defaults to durability="async"; the container must pin "sync"."""
    modes: list[object] = []

    class Spy:
        def __init__(self, inner):
            self._inner = inner
            self.checkpointer = inner.checkpointer
            self.store = inner.store

        def astream(self, inp, config, **kw):
            modes.append(kw.get("durability"))
            return self._inner.astream(inp, config, **kw)

        async def aget_state(self, config):
            return await self._inner.aget_state(config)

        async def aupdate_state(self, config, values, **kw):
            return await self._inner.aupdate_state(config, values, **kw)

    def build(ctx):
        g = StateGraph(State)
        g.add_node("noop", lambda s: {"counter": 1})
        g.add_edge(START, "noop")
        g.add_edge("noop", END)
        return Spy(g.compile(checkpointer=ctx.saver))

    thread = server.ensure_thread("ctx-durability")
    server.queue_turn(thread, input={"counter": 0})
    await service(server, build).run_once()
    assert modes == ["sync"], modes


async def test_the_completed_turn_is_durable_in_state(server):
    def build(ctx):
        g = StateGraph(State)
        g.add_node("inc", lambda s: {"counter": s.get("counter", 0) + 1})
        g.add_edge(START, "inc")
        g.add_edge("inc", END)
        return g.compile(checkpointer=ctx.saver)

    thread = server.ensure_thread("ctx-durable")
    server.queue_turn(thread, input={"counter": 1})
    outcome = await service(server, build).run_once()

    reader = StateAPIClient(server.url)
    headers = await reader.list_checkpoints(outcome.thread_id, "graph", 10)
    assert headers, "the completed turn produced no durable checkpoint"
    runs = server.state.doc.d["runs"]
    assert runs[outcome.run_id]["status"] == "done", "the lease was never released"


def _asking_graph(ctx):
    """The smallest graph that asks a human a question and stops until answered."""
    g = StateGraph(State)

    def ask(state):
        answer = interrupt({"question": "approve?"})
        return {"messages": ["asked", answer]}

    g.add_node("ask", ask)
    g.add_edge(START, "ask")
    g.add_edge("ask", END)
    return g.compile(checkpointer=ctx.saver)


def _asking_service(server):
    return service(server, _asking_graph)


def _approver_beats_the_park(server, thread_id: str) -> AgentService:
    """A service whose approver answers while its own run is still parking.

    The answer is inserted after the gate event is durable and before ``Suspend``
    returns - the exact interleaving that makes State's wake (which fires only at
    insert time, against a run that is still executing) fire at nothing. It is
    written as an override of the park, not as a sleep, so the window is a fact of
    the test rather than a race somebody has to lose.
    """
    class _AnswerBeatsThePark(AgentService):
        async def _suspend(self, client, fence, wait_for):
            for gate in [m for m in server.state.doc.d["inbox"]
                         if m["thread_id"] == thread_id
                         and m["kind"] == self.HITL_GATE_KIND]:
                hitl_id = gate["key"][len(self.HITL_GATE_KEY_PREFIX):]
                server.post("Enqueue", {
                    "thread_id": thread_id,
                    "key": f"{self.HITL_ANSWER_KEY_PREFIX}{hitl_id}",
                    "kind": self.HITL_WAIT_FOR,
                    "payload": {"hitl_id": hitl_id, "approved": True,
                                "response": "yes"}})
            return await super()._suspend(client, fence, wait_for)

    return _AnswerBeatsThePark(graph=_asking_graph, base_url=server.url, worker="w1")


def answer_events(server, thread: str) -> list[dict]:
    return [m for m in server.state.doc.d["inbox"]
            if m["thread_id"] == thread and m["kind"] == "hitl_response"]


async def test_interrupt_parks_on_the_name_the_platform_answers(server):
    """The interrupt contract, as the platform actually implements it.

    ``Coordinator.RespondHITL`` wakes a parked run by enqueueing an inbox event of
    kind ``hitl_response``, and State wakes a waiting head when the event's kind
    equals its ``wait_for``. So a container that parks on a private name (this one
    used to write ``input:<interrupt id>``) is parked forever and nobody can tell:
    the run looks healthy and simply is never claimed again. This test is the
    reason the name is a constant and not a choice.
    """
    thread = server.ensure_thread("ctx-interrupt-park")
    server.queue_turn(thread, input={})
    first = await _asking_service(server).run_once()

    assert first.status == "interrupted", first
    assert first.interrupts[0]["value"] == {"question": "approve?"}
    assert first.wait_for == "hitl_response", first.wait_for
    run = server.state.doc.d["runs"][first.run_id]
    assert run["status"] == "waiting", run["status"]
    assert run["wait_for"] == "hitl_response"

    # the question is durable in the thread inbox, keyed by this run and interrupt,
    # so it outlives the executor that asked it and a reader can answer it
    gates = [m for m in server.state.doc.d["inbox"]
             if m["thread_id"] == thread and m["kind"] == "hitl_request"]
    assert len(gates) == 1, gates
    assert gates[0]["key"] == f"hitl:{first.run_id}:{first.interrupts[0]['id']}"
    # the payload IS space.HITLRequest, so GET GetHITL can answer it: the question
    # is `prompt`, the gate names itself by `id`, and Space derives that id from the
    # key alone (hitl_gate.go) - which is why the key above is the contract.
    assert gates[0]["payload"]["id"] == f"{first.run_id}:{first.interrupts[0]['id']}"
    assert gates[0]["payload"]["prompt"] == {"question": "approve?"}
    assert gates[0]["payload"]["status"] == "pending"
    assert gates[0]["payload"]["turn_id"] == first.run_id


async def test_an_event_of_another_kind_does_not_wake_a_parked_turn(server):
    thread = server.ensure_thread("ctx-interrupt-foreign")
    server.queue_turn(thread, input={})
    first = await _asking_service(server).run_once()
    assert first.status == "interrupted", first

    server.post("Enqueue", {"thread_id": thread, "key": "unrelated",
                            "kind": "something_else", "payload": {"x": 1}})
    assert server.state.doc.d["runs"][first.run_id]["status"] == "waiting", \
        "a foreign event kind must not wake a HITL park"
    idle = await _asking_service(server).run_once()
    assert idle.status == "noop", \
        f"a foreign event kind made the parked run claimable: {idle.status}"


async def test_hitl_answer_wakes_the_same_run_and_is_consumed_once(server):
    """Answer over the documented contract, then prove it was taken exactly once.

    The answer arrives the way ``POST /v1/space/RespondHITL`` puts it in the wire:
    an inbox event of kind ``hitl_response`` whose body carries ``response``. State
    returns THIS run to pending - same id, same FIFO position - the container reads
    the answer out of the inbox, journals it under the lease before acking, and
    resumes the open checkpoint.
    """
    thread = server.ensure_thread("ctx-interrupt-answer")
    run_id = server.queue_turn(thread, input={})
    first = await _asking_service(server).run_once()
    assert first.status == "interrupted", first
    assert first.run_id == run_id

    server.post("Enqueue", {
        "thread_id": thread,
        # exactly the key Coordinator.RespondHITL writes: "hitl_response:" + hitl_id,
        # and the hitl_id of the gate this run actually asked (run id + interrupt id)
        "key": f"hitl_response:{run_id}:{first.interrupts[0]['id']}",
        "kind": "hitl_response",
        "payload": {"hitl_id": f"{run_id}:{first.interrupts[0]['id']}",
                    "approved": True, "response": "yes"}})

    woke = server.state.doc.d["runs"][run_id]
    assert woke["status"] == "pending", \
        f"the answer did not wake its own run: {woke['status']}"
    assert woke["wait_for"] == ""

    second = await _asking_service(server).run_once()
    assert second.status == "done", second
    assert second.run_id == run_id, "the answer must resume the same run, not a new one"
    assert second.result["messages"] == ["asked", "yes"], second.result

    answers = answer_events(server, thread)
    assert len(answers) == 1 and answers[0]["acked"], "the answer was not consumed once"
    journaled = await StateAPIClient(server.url).get_kv(
        AgentService.HITL_ANSWER_NAMESPACE, run_id)
    assert journaled["value"]["answer"] == "yes"

    # nothing is owed any more: a further claim finds no work, so the ack and the
    # journal agree instead of one of them stranding the thread
    idle_again = await _asking_service(server).run_once()
    assert idle_again.status == "noop", \
        f"the thread still owes work after the answer was consumed: {idle_again.status}"


def steering_rows(server) -> list[dict]:
    return [m for m in server.state.doc.d["inbox"] if m["kind"] == "steering"]


async def test_another_gates_answer_does_not_answer_this_run(server):
    """A HITL answer belongs to the gate named in its KEY, not to whoever wakes.

    State's wake rule fires on the event's KIND, so an answer addressed to a
    different gate still returns this run to pending - the platform cannot tell
    them apart. The container therefore has to read by key, or it resumes an
    approval question on some other card's verdict and calls that an approved
    turn. A foreign answer leaves the run parked and its event unread.
    """
    thread = server.ensure_thread("ctx-interrupt-foreign-answer")
    run_id = server.queue_turn(thread, input={})
    first = await _asking_service(server).run_once()
    assert first.status == "interrupted", first
    own_gate = f"{run_id}:{first.interrupts[0]['id']}"

    server.post("Enqueue", {
        "thread_id": thread, "key": "hitl_response:run-other:int-9",
        "kind": "hitl_response",
        "payload": {"hitl_id": "run-other:int-9", "approved": True,
                    "response": "NOT THIS GATE'S ANSWER"}})

    woke = server.state.doc.d["runs"][run_id]
    assert woke["status"] == "pending", "the platform's wake rule changed"

    re_parked = await _asking_service(server).run_once()
    assert re_parked.status == "interrupted", \
        f"a foreign gate's answer resumed this run: {re_parked.status}"
    assert re_parked.result is None, re_parked.result
    foreign = [m for m in answer_events(server, thread) if m["key"] == "hitl_response:run-other:int-9"]
    assert len(foreign) == 1 and not foreign[0]["acked"], \
        "the container consumed an answer that was not addressed to it"
    assert server.state.doc.d["runs"][run_id]["status"] == "waiting"

    # the real answer still works after the wrong one was correctly left alone
    server.post("Enqueue", {
        "thread_id": thread, "key": f"hitl_response:{own_gate}", "kind": "hitl_response",
        "payload": {"hitl_id": own_gate, "approved": True, "response": "yes"}})
    done = await _asking_service(server).run_once()
    assert done.status == "done", done
    assert done.result["messages"] == ["asked", "yes"], done.result


async def test_an_answer_written_before_the_park_still_wakes_the_run(server):
    """An answer that beat the park woke nothing - until the container re-armed it.

    State fires its wake in the moment an event is INSERTED, and at that moment this
    run was still executing, so the park that followed waits for a signal that has
    already passed: the turn stops forever and neither side is told. Space repairs
    that window for the gates IT submits (577e2a9), and a container's gate never goes
    through ``SubmitHITL`` - so the container rings the same doorbell, under the
    platform author's own key and kind. Without that repair this test is RED: the run
    stays ``waiting`` while its own answer sits unread in the same inbox.
    """
    thread = server.ensure_thread("ctx-interrupt-lost-wake")
    run_id = server.queue_turn(thread, input={})
    first = await _approver_beats_the_park(server, thread).run_once()
    assert first.status == "interrupted", first
    gate = f"{run_id}:{first.interrupts[0]['id']}"

    verdicts = [m for m in answer_events(server, thread)
                if m["key"] == f"hitl_response:{gate}"]
    assert len(verdicts) == 1 and not verdicts[0]["acked"], \
        f"the answer vanished instead of beating the park: {verdicts}"

    bell_key = f"hitl_response:{gate}{AgentService.HITL_WAKE_KEY_SUFFIX}"
    bells = [m for m in answer_events(server, thread) if m["key"] == bell_key]
    assert len(bells) == 1, (
        "an answer that beat the park left the run parked forever: "
        f"{server.state.doc.d['runs'][run_id]}")
    assert bells[0]["kind"] == "hitl_response", \
        "the repair must use the name State's wake rule reacts to"
    assert bells[0]["payload"]["resignal_of"] == f"hitl_response:{gate}", \
        "a doorbell has to name the decision it re-signals"
    assert server.state.doc.d["runs"][run_id]["status"] == "pending", \
        "the doorbell did not return the parked run to the queue"

    resumed = await _asking_service(server).run_once()
    assert resumed.status == "done", resumed
    assert resumed.run_id == run_id, "the repair resumed another run"
    assert resumed.result["messages"] == ["asked", "yes"], resumed.result

    # a doorbell is traffic, not a verdict: the gate keeps exactly one decision and
    # it is the human's, and both events end up acked so the thread owes nothing
    verdicts = [m for m in answer_events(server, thread)
                if m["key"] == f"hitl_response:{gate}"]
    assert len(verdicts) == 1 and verdicts[0]["acked"], verdicts
    assert [m for m in answer_events(server, thread) if m["key"] == bell_key][0]["acked"], \
        "the container left its own doorbell as permanent traffic"
    assert server.state.doc.d["runs"][run_id]["status"] == "done"
    idle = await _asking_service(server).run_once()
    assert idle.status == "noop", f"the thread still owes work: {idle.status}"


async def test_a_parked_gate_with_no_answer_produces_no_wake_traffic(server):
    """The repair follows positive evidence, so an unanswered park stays silent.

    Inventing a wake for a gate nobody answered would release runs on a signal the
    thread never asked for - the same line Space holds itself to: nothing is
    inferred from an inbox that says nothing.
    """
    thread = server.ensure_thread("ctx-interrupt-no-answer")
    run_id = server.queue_turn(thread, input={})
    first = await _asking_service(server).run_once()
    assert first.status == "interrupted", first

    assert answer_events(server, thread) == [], \
        "a park with no answer invented wake traffic"
    assert server.state.doc.d["runs"][run_id]["status"] == "waiting"


async def test_the_rearm_rings_one_doorbell_per_gate(server):
    """The repair is idempotent, because a second doorbell is a second wake.

    The row is put back into the parked shape to ask one narrow question of a second
    pass, with the human's answer still in the box: does it ring again? It must not.
    """
    thread = server.ensure_thread("ctx-interrupt-one-doorbell")
    run_id = server.queue_turn(thread, input={})
    first = await _approver_beats_the_park(server, thread).run_once()
    gate = f"{run_id}:{first.interrupts[0]['id']}"
    bell_key = f"hitl_response:{gate}{AgentService.HITL_WAKE_KEY_SUFFIX}"
    assert len([m for m in answer_events(server, thread) if m["key"] == bell_key]) == 1

    row = server.state.doc.d["runs"][run_id]
    row["status"], row["wait_for"] = "waiting", "hitl_response"
    again = await _asking_service(server)._hitl_rearm(
        StateAPIClient(server.url), {"run_id": run_id}, thread)

    assert again == [], f"a second repair pass rang a second doorbell: {again}"
    assert len([m for m in answer_events(server, thread) if m["key"] == bell_key]) == 1, \
        "the doorbell key is not deduplicated"


async def test_steering_between_supersteps_reaches_later_nodes(server):
    """A steer that lands before the second superstep is applied *and* seen.

    The container parks the executor at the boundary, folds the message as a
    durable checkpoint of its own and resumes, so no second writer ever races the
    running superstep for the same channel versions.
    """
    def build(ctx):
        g = StateGraph(State)
        g.add_node("first", lambda s: {"messages": ["first"]})
        # reads first's durable output back, so the assertion also proves that
        # parking the executor at the boundary lost nothing
        g.add_node("second",
                   lambda s: {"messages": [*s["messages"], "second",
                                           *s.get("steering", [])]})
        g.add_edge(START, "first")
        g.add_edge("first", "second")
        g.add_edge("second", END)
        return g.compile(checkpointer=ctx.saver)

    thread = server.ensure_thread("ctx-steering")
    enqueue_steering(server, thread, "keep it to one line", "steer-1")
    server.queue_turn(thread, input={})

    outcome = await service(server, build).run_once()
    assert outcome.status == "done", outcome
    assert outcome.result.get("steering") == ["keep it to one line"], outcome.result
    assert outcome.result["messages"] == ["first", "second", "keep it to one line"], \
        "the later node did not see the folded steering"
    rows = steering_rows(server)
    assert [m["acked"] for m in rows] == [True], "steering must be acked exactly once"
    assert outcome.folded_steering == [rows[0]["id"]]


async def test_steering_is_folded_once_across_turns(server):
    """An acked message is history, not work: the next turn must not fold it again."""
    def build(ctx):
        g = StateGraph(State)
        g.add_node("only", lambda s: {"counter": s.get("counter", 0) + 1})
        g.add_edge(START, "only")
        g.add_edge("only", END)
        return g.compile(checkpointer=ctx.saver)

    thread = server.ensure_thread("ctx-steering-once")
    enqueue_steering(server, thread, "once", "steer-once")
    server.queue_turn(thread, input={})
    svc = service(server, build)
    first = await svc.run_once()
    assert first.status == "done", first
    assert first.result.get("steering") == ["once"], first.result

    server.queue_turn(thread, input={"counter": 9})
    again = await svc.run_once()
    assert again.status == "done", again
    assert again.result.get("steering") == ["once"], \
        "an acked steering message was folded a second time"
    assert again.folded_steering == [], again.folded_steering


async def test_steering_is_deferred_while_an_interrupt_is_pending(server):
    """Folding onto a pause would make the open question invisible.

    LangGraph attaches the ``__interrupt__`` write to the newest checkpoint and
    derives the owning task id from that checkpoint's id, so a side update
    supersedes the checkpoint the interrupt rides on. The rows stay owed instead,
    and are folded once the turn resumes.
    """
    def build(ctx):
        g = StateGraph(State)

        def ask(state):
            answer = interrupt({"question": "approve?"})
            return {"messages": ["asked", answer]}

        g.add_node("ask", ask)
        g.add_edge(START, "ask")
        g.add_edge("ask", END)
        return g.compile(checkpointer=ctx.saver)

    thread = server.ensure_thread("ctx-steering-pause")
    enqueue_steering(server, thread, "ask for a smaller amount", "steer-pause")
    server.queue_turn(thread, input={})
    svc = service(server, build)

    first = await svc.run_once()
    assert first.status == "interrupted", first
    assert first.interrupts[0]["value"] == {"question": "approve?"}
    assert first.folded_steering == [], "steering must not be folded onto a pause"
    assert [m["acked"] for m in steering_rows(server)] == [False], \
        "the row stays owed until the resume"

    server.queue_turn(thread, resume="yes")
    second = await svc.run_once()
    assert second.status == "done", second
    assert second.result.get("steering") == ["ask for a smaller amount"], second.result
    assert second.result["messages"] == ["asked", "yes"], second.result
    assert [m["acked"] for m in steering_rows(server)] == [True]


async def test_a_node_may_take_ownership_of_steering(server):
    """Whatever a node consumes itself, the container must not apply a second time."""
    def build(ctx):
        g = StateGraph(State)

        async def planner(state):
            rows = await ctx.take_steering()
            return {"steering": [r["payload"]["state"]["steering"][0] for r in rows]}

        g.add_node("planner", planner)
        g.add_edge(START, "planner")
        g.add_edge("planner", END)
        return g.compile(checkpointer=ctx.saver)

    thread = server.ensure_thread("ctx-steering-take")
    enqueue_steering(server, thread, "hand me the wheel", "steer-take")
    server.queue_turn(thread, input={})

    outcome = await service(server, build).run_once()
    assert outcome.status == "done", outcome
    assert outcome.result.get("steering") == ["hand me the wheel"], \
        "steering was applied twice: once by the node, once by the container"
    assert [m["acked"] for m in steering_rows(server)] == [True]


async def test_lost_lease_stops_the_container_from_writing(server):
    """A zombie worker must not keep writing after another worker takes over."""
    wrote_after_loss: list[str] = []

    def build(ctx):
        g = StateGraph(State)

        async def slow(state):
            import asyncio
            await asyncio.sleep(0.3)
            wrote_after_loss.append("node finished")
            return {"counter": 1}

        g.add_node("slow", slow)
        g.add_edge(START, "slow")
        g.add_edge("slow", END)
        return g.compile(checkpointer=ctx.saver)

    thread = server.ensure_thread("ctx-lease")
    run_id = server.queue_turn(thread, input={})

    class TakingOver(AgentService):
        """Simulates the takeover landing while the turn is executing."""

        async def _heartbeat(self, fence, stop):
            import asyncio

            await asyncio.sleep(0.05)
            server.re_claim(fence["run_id"], worker="takeover")
            return await super()._heartbeat(fence, stop)

    svc = TakingOver(graph=build, base_url=server.url, worker="w1",
                     heartbeat_ms=20, lease_ttl_ms=50)
    outcome = await svc.run_once(run_id=run_id)
    assert outcome.status == "lease_lost", outcome
    header = server.state.doc.d["checkpoints"].get(
        __import__("json").dumps([thread, "graph", run_id]))
    # nothing new may be durable under the dead epoch
    stale = [k for k, v in server.state.doc.d["writes"].items()
             if json.loads(k)[2] and v.get("task") == "zombie"]
    assert stale == []


async def test_run_without_input_stays_a_noop_when_queue_is_empty(server):
    seen: list[str] = []
    outcome = await service(server, build_two_step(seen)).run_once()
    assert outcome.status == "noop"
    assert seen == []


# ---------------------------------------------------------------------------
# where a failed turn leaves its reason
#
# Space answers "why did this turn fail" from the durable thread stream: the frame
# of kind "error" whose KEY is space.StepKey(turn, n) == "<turn_id>:<n>", where a
# Space turn id is the State run id (internal/space/coordinator.go mints the turn
# as &Turn{ID: run.ID}). Agreed with vm2-state-runner in 20261101T135717Z §4. The
# three legs below are that contract, from the writer's side.
# ---------------------------------------------------------------------------


def _failed_turn(server):
    """A run that cannot start: journaled as failed, with the reason in the stream."""
    thread = server.ensure_thread("ctx-cause")
    run_id = server.queue_turn(thread)          # no input, nothing to continue
    return thread, run_id


async def test_failed_turn_writes_the_platform_cause_frame(server):
    thread, run_id = _failed_turn(server)
    outcome = await service(server, build_two_step([])).run_once(run_id=run_id)
    assert outcome.status == "failed", outcome

    frames = [event for event in server.state.doc.d["stream"]
              if event["kind"] == "error"]
    assert len(frames) == 1, frames
    assert frames[0]["key"] == f"{run_id}:0", \
        "the cause must carry the turn's own step key, or no reader can attribute it"
    assert frames[0]["payload"]["error"] == outcome.error
    assert frames[0]["payload"]["agent_event"] == "agent_failed", \
        "the framework's own name belongs inside the payload, not in the kind"


async def test_cause_key_never_reuses_a_durable_step_index(server):
    """A re-delivered run must not stamp a key another writer already used.

    State answers a key reuse with different bytes as KEY_REUSE_MISMATCH and drops
    the append - so picking an index by counting this process's own frames would
    lose the reason exactly on the restart where someone is asking what happened.
    """
    thread, run_id = _failed_turn(server)
    for index, status in enumerate(("model", "tool")):
        server.state.doc.d["stream"].append({
            "seq": server.state.doc.next_seq(), "thread_id": thread,
            "key": f"{run_id}:{index}", "kind": status, "payload": {"n": index}})

    outcome = await service(server, build_two_step([])).run_once(run_id=run_id)
    assert outcome.status == "failed", outcome

    frames = [event for event in server.state.doc.d["stream"]
              if event["kind"] == "error"]
    assert [event["key"] for event in frames] == [f"{run_id}:2"], frames


async def test_an_unreadable_stream_writes_an_unattributable_key(server, monkeypatch):
    """Fail closed on attribution, never on the reason.

    Without a high-water the SDK cannot pick a turn-attributable index safely, and a
    guessed one can collide and lose the frame. So the frame keeps a key this run
    alone owns: still durable, still visible to every reader of the stream, just not
    claimable as one turn's cause.
    """
    from ahp_state import client as client_module

    async def unreadable(self, thread_id, after=0, limit=100):
        raise client_module.StateAPIError("FORBIDDEN", "stream not readable")

    monkeypatch.setattr(client_module.StateAPIClient, "read_stream", unreadable)
    thread, run_id = _failed_turn(server)
    outcome = await service(server, build_two_step([])).run_once(run_id=run_id)
    assert outcome.status == "failed", outcome

    frames = [event for event in server.state.doc.d["stream"]
              if event["kind"] == "error"]
    assert [event["key"] for event in frames] == [f"agent:{run_id}:agent_failed"], frames
    assert frames[0]["payload"]["error"] == outcome.error
