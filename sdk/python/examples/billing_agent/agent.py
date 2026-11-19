"""A custom agent the platform can run as it is: two nodes, one effect, one question.

This is what ``AHP_AGENT_MODULE`` points at. The image owns the graph - the
platform never executes a turn for you - and :mod:`ahp_state.agent` owns the
durable half: lease-fenced checkpoints, ``durability="sync"``, heartbeats,
interrupt/resume, steering, and an effect journal that keeps a restart from
charging anyone twice.

Contract for the builder:

``build(ctx) -> CompiledStateGraph``
    compile with ``ctx.saver`` and ``ctx.store`` - those are the platform-bound
    components for the lease this run holds. Nothing else may hold durable state.
``ctx.effects.call(tool, args)``
    the only way to touch the outside world. The effect is journaled under its
    POSITION in this node's effect sequence, which a re-executed node reproduces
    exactly - that is what the journal replays after a restart. Pass ``call_id``
    only when you can name the logical call durably; never pass a model
    ``tool_call_id``, it is re-minted per attempt and would double the effect.
``ctx.take_steering()``
    steering this node consumes itself. Leave it alone otherwise and the container
    folds it into the state at the next superstep boundary.

Check the image before shipping it: ``ahp-sdk-check examples/billing_agent``.
"""

from __future__ import annotations

import operator
import json
import os
import pathlib
import uuid

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt
from typing_extensions import Annotated, TypedDict


class State(TypedDict, total=False):
    request: dict
    messages: Annotated[list[str], operator.add]
    receipt: dict
    approved: bool
    within_limit: bool


def build(ctx):
    """Compile the agent's graph against the platform components of this lease."""

    async def plan(state: State) -> dict:
        rows = await ctx.take_steering()
        notes = [str((row.get("payload") or {}).get("state", {}).get("note", ""))
                 for row in rows]
        policy = await ctx.store.aget(("billing",), "policy")
        limit = (policy.value if policy else None) or {"max_amount": 1000}
        amount = (state.get("request") or {}).get("amount", 0)
        if amount > limit["max_amount"]:
            return {"messages": [f"{amount} exceeds the policy limit; not charging"],
                    "within_limit": False}
        return {"messages": [f"planned with {len(notes)} steering note(s)"],
                "within_limit": True}

    async def charge(state: State) -> dict:
        request = state.get("request") or {}
        approved = interrupt({"question": f"charge {request.get('amount')}?"})
        if not approved:
            return {"messages": ["cancelled"], "approved": False}
        # asked first, charged second: the interrupt is durable, so a restart
        # resumes with the answer instead of asking or charging again
        receipt = await ctx.effects.bind("charge").call("billing.charge", request)
        return {"messages": ["charged"], "receipt": receipt, "approved": True}

    graph = StateGraph(State)
    graph.add_node("plan", plan)
    graph.add_node("charge", charge)
    graph.add_edge(START, "plan")
    graph.add_conditional_edges("plan",
                                lambda state: "charge" if state.get("within_limit", True)
                                else END,
                                {"charge": "charge", END: END})
    graph.add_edge("charge", END)
    return graph.compile(checkpointer=ctx.saver, store=ctx.store)


async def effect_transport(tool: str, args: dict) -> dict:
    """Touch the outside world once, durably, where someone can count the touch.

    This is the half the platform cannot write for an image: only the agent knows
    what "charge" actually does. ``python -m ahp_state.container`` picks this up as
    the module attribute ``effect_transport``; a harness can swap in its own world
    with ``AHP_EFFECT_TRANSPORT=other.module:callable`` and rebuild nothing.

    The receipt file is named with a random id on purpose. Naming it after the
    request would let an illegal second charge overwrite the first receipt and
    hide the exact defect the durability legs exist to catch - so the world itself
    is the counter: one charge, one file.
    """
    world = pathlib.Path(os.environ.get("AHP_EFFECT_WORLD_DIR", "effect-world"))
    world.mkdir(parents=True, exist_ok=True)
    name = f"{uuid.uuid4().hex}.json"
    with (world / name).open("wb") as handle:
        handle.write(json.dumps({"tool": tool, "args": args}, sort_keys=True).encode())
        handle.flush()
        os.fsync(handle.fileno())
    directory = os.open(str(world), os.O_RDONLY)      # the directory entry is the fact
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return {"tool": tool, "receipt": name, "world": str(world / name)}
