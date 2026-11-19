"""The pod of ``test_restart_no_repeat`` - a real process that can really die.

Run with ``python -m ahp_state.tests.restart_child``. It builds an ordinary custom
LangGraph graph whose ``charge`` node performs exactly one external effect (one
appended, fsynced line in ``$AHP_EFFECTS_FILE``) and then kills itself on request
with ``os._exit`` - no exception handling, no ``atexit``, no buffered write
surviving: the same thing a killed container pod looks like from the inside.

Configuration comes from the environment (never argv, so no token in ``ps``):

``AHP_STATE_URL``    State API base URL (required)
``AHP_EFFECTS_FILE`` file the external effect appends to (required)
``AHP_WORKER``       worker id for ``ClaimRun``
``AHP_RUN_ID``       run to claim, or ``AHP_FENCE`` + ``AHP_TOKEN`` +
``AHP_THREAD_ID``    an already-held lease to adopt (the takeover path), with
``AHP_PAYLOAD``      the run payload that hand-out would have carried
``AHP_CRASH``        ``in-effect``  : die after the effect fired, before its
                     journal write (the ambiguous window)
                     ``after-effect``: die after the effect is journaled durable,
                     before any state write
                     ``before-effect``: die inside the first node, with only the
                     graph input durable
                     ``after-journal``: die after the turn journal, before
                     LangGraph ever ran (no checkpoint at all)
``AHP_CRASH``        ``in-effect``  : die after the effect fired, before its
                     journal write (the ambiguous window)
                     ``after-effect``: die after the effect is journaled durable,
                     before any state write
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import operator
import os
import sys
import uuid

from langgraph.graph import END, START, StateGraph
from typing_extensions import Annotated, TypedDict

from ahp_state import StateAPIClient
from ahp_state.agent import AgentService, mtls_client


class State(TypedDict, total=False):
    messages: Annotated[list[str], operator.add]
    receipt: dict


def _die(code: int) -> None:
    os._exit(code)          # a pod kill: nothing else runs


def fire_effect() -> dict:
    """The external effect itself: append one durable line, then maybe die."""
    path = os.environ["AHP_EFFECTS_FILE"]
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({"tool": "billing.charge", "amount": 42}) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    if os.environ.get("AHP_CRASH") == "in-effect":
        _die(8)
    return {"charged": True, "reference": "bill-1"}


async def transport(tool: str, args: dict) -> dict:
    return fire_effect()


def build(ctx):
    graph = StateGraph(State)

    async def charge(state):
        if os.environ.get("AHP_CRASH") == "before-effect":
            _die(9)             # dies with only the input checkpoint durable
        # AHP_EFFECT_IDENTITY picks where the effect identity comes from, which
        # is the whole question when a process dies between the effect and the
        # checkpoint:
        #   "" (default)     - a fixed id, what the SIGKILL module proves
        #   "positional"     - what the SDK does by default and the seam
        #                      contract publishes: position in this node's
        #                      effect sequence, reproducible on re-execution
        #   "tool-call-id"   - what the docs used to suggest: an id the model
        #                      mints per attempt, so a re-executed node calls a
        #                      *different* logical effect
        effects = ctx.effects.bind("charge")
        from langgraph.config import get_config
        print(f"AHP_INTENT_NODE {get_config()['configurable']['__pregel_task_id']}", flush=True)
        mode = os.environ.get("AHP_EFFECT_IDENTITY", "")
        if mode == "positional":
            receipt = await effects.call("billing.charge", {"amount": 42})
        elif mode == "tool-call-id":
            receipt = await effects.call("billing.charge", {"amount": 42},
                                             call_id=f"call-{uuid.uuid4().hex}")
        else:
            receipt = await effects.call("billing.charge", {"amount": 42},
                                             call_id="charge-1")
        # report the identity the effect actually went under, so a harness can
        # measure whether the key survived the restart instead of reading it
        # out of this file
        print(f"AHP_CALL_ID {effects.last_call_id}", flush=True)
        if os.environ.get("AHP_CRASH") == "after-effect":
            _die(7)             # the effect is journaled; the state write never happened
        if os.environ.get("AHP_CRASH") == "wait-kill":
            # The effect is journaled and durable. Announce it and stay alive so
            # the parent can send a real SIGKILL from outside: a self-inflicted
            # os._exit proves nothing about an external kill. No credential is
            # written here - the parent already holds the lease it handed down.
            with open(os.environ["AHP_READY_FILE"], "w", encoding="utf-8") as handle:
                handle.write(json.dumps({"intent_node": effects.last_intent_node, "call_id": effects.last_call_id}) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            print("AHP_READY", flush=True)
            await asyncio.sleep(3600)
        return {"messages": ["charged"], "receipt": receipt}

    graph.add_node("charge", charge)
    graph.add_node("finish", lambda state: {"messages": ["done"]})
    graph.add_edge(START, "charge")
    graph.add_edge("charge", "finish")
    graph.add_edge("finish", END)
    return graph.compile(checkpointer=ctx.saver, store=ctx.store)


async def journal_the_turn_then_die() -> None:
    """Die the way a pod does after journaling the turn but before LangGraph ran.

    Nothing durable exists except the "this turn has started" record, which is
    exactly the state a restarted worker has to reason about.
    """
    client = StateAPIClient(os.environ["AHP_STATE_URL"])
    claim = await client.claim_run(os.environ.get("AHP_WORKER", "child"), ttl_ms=120000)
    lease = claim["lease"]
    fence = {"run_id": lease["run"]["id"], "worker": lease["worker"], "epoch": lease["epoch"]}
    await client.put_kv(fence, AgentService.TURN_NAMESPACE, fence["run_id"],
                        f"turn:{fence['run_id']}", 0,
                        {"run_id": fence["run_id"], "has_input": True, "resumes": False})
    _die(9)


async def main() -> int:
    if os.environ.get("AHP_CRASH") == "after-journal":
        await journal_the_turn_then_die()
    # The URL says WHICH stand; the handshake says HOW to speak to it (client
    # certificates, task token). Preferring the file whenever it exists made
    # every double-based restart test follow a live hand-out out of the process
    # environment and talk to the wrong server - so the file is honored only for
    # the origin it was issued for.
    url = os.environ.get("AHP_STATE_URL", "")
    handshake = os.environ.get("AHP_STATE_HANDSHAKE", "")
    hand = None
    if handshake:
        with open(handshake, encoding="utf-8") as handle:
            hand = json.load(handle)
    if hand is not None and (not url or url == hand.get("base_url")):
        client = mtls_client(hand)
        service = AgentService(graph=build, client=client,
                               worker=os.environ.get("AHP_WORKER", "child"),
                               effects=transport)
    else:
        service = AgentService(graph=build, base_url=url,
                               worker=os.environ.get("AHP_WORKER", "child"),
                               effects=transport)
    fence = os.environ.get("AHP_FENCE")
    if fence:
        raw = os.environ.get("AHP_PAYLOAD")
        outcome = await service.run_leased(
            json.loads(fence), os.environ.get("AHP_TOKEN"),
            thread_id=os.environ.get("AHP_THREAD_ID", ""),
            payload=json.loads(raw) if raw else None)
    else:
        run_id = os.environ.get("AHP_RUN_ID") or None
        outcome = await service.run_once(run_id=run_id)
    print("AHP_OUTCOME " + json.dumps(dataclasses.asdict(outcome)), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
