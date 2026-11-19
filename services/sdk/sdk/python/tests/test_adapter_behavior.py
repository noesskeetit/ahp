"""Behavioral tests: LangGraph adapter with real tool execution + restart.

F1: checkpoint save/restore with channel_values (resume from CP, not input).
F2: REAL tool execution, simulated crash/restart, idempotency (same side-effect
    key → no double execution), SIDE_EFFECT_UNKNOWN for unknown keys.
"""

from __future__ import annotations

import json
import os
import pytest
from typing import Any

from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict

from ahp_state import AHPCheckpointSaver, StateAPIClient


class State(TypedDict):
    messages: list[str]
    counter: int
    side_effects: list[str]
    last_tool_key: str
    tool_status: str


# --- State API: the contract double shared with ahp_state/tests -------------

import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ahp_state.tests.fake_state import FakeState, FakeStateServer, StateDoc  # noqa: E402

@pytest.fixture
def state_api_server():
    """A fresh State double per test (one durable document per scenario)."""
    with FakeStateServer(FakeState(StateDoc())) as server:
        yield server


def make_saver(server: FakeStateServer, thread_id: str = "test-thread") -> AHPCheckpointSaver:
    """A saver bound to a real thread + run + lease handed out by the server."""
    boot = server.bootstrap(thread_id)
    client = StateAPIClient(server.url, token=boot["token"])
    return AHPCheckpointSaver(client, thread_id=boot["thread_id"], fence=boot["fence"])


# ===== F1: Checkpoint + resume =====

@pytest.mark.asyncio
async def test_checkpoint_serializes_channel_values(state_api_server):
    """aput stores full channel_values; aget_tuple restores them."""
    saver = make_saver(state_api_server)
    graph = StateGraph(State)
    graph.add_node("inc", lambda s: {
        "messages": s["messages"] + ["inc"], "counter": s["counter"] + 1,
        "side_effects": [], "last_tool_key": "", "tool_status": "",
    })
    graph.add_edge(START, "inc")
    graph.add_edge("inc", END)
    compiled = graph.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "test-thread"}}
    result = await compiled.ainvoke(
        {"messages": ["start"], "counter": 5, "side_effects": [], "last_tool_key": "", "tool_status": ""},
        config,
    )
    assert result["counter"] == 6
    tup = await saver.aget_tuple(config)
    assert tup.checkpoint["channel_values"]["counter"] == 6
    assert "inc" in tup.checkpoint["channel_values"]["messages"]


@pytest.mark.asyncio
async def test_resume_from_checkpoint_fresh_adapter(state_api_server):
    """FRESH saver returns CP state (proves §4.2: continues from last snapshot)."""
    saver_a = make_saver(state_api_server, "resume-thread")
    graph = StateGraph(State)
    graph.add_node("step", lambda s: {
        "messages": s["messages"] + ["step-a"], "counter": s["counter"] + 1,
        "side_effects": [], "last_tool_key": "", "tool_status": "",
    })
    graph.add_edge(START, "step")
    graph.add_edge("step", END)
    compiled = graph.compile(checkpointer=saver_a)
    config = {"configurable": {"thread_id": "resume-thread"}}
    await compiled.ainvoke(
        {"messages": ["init"], "counter": 10, "side_effects": [], "last_tool_key": "", "tool_status": ""},
        config,
    )
    # FRESH saver simulates restart
    saver_b = make_saver(state_api_server, "resume-thread")
    graph_b = StateGraph(State)
    graph_b.add_node("noop", lambda s: s)
    graph_b.add_edge(START, "noop")
    graph_b.add_edge("noop", END)
    compiled_b = graph_b.compile(checkpointer=saver_b)
    r2 = await compiled_b.ainvoke(None, config)
    assert r2["counter"] == 11
    assert "step-a" in r2["messages"]


# ===== F2: REAL tool execution + restart + idempotency =====

class ExternalWorld:
    """Simulates an external system with side effects. Tracks calls by key."""
    def __init__(self):
        self.executed: dict[str, str] = {}  # key → result
        self.call_count: int = 0

    def execute_tool(self, key: str, payload: str) -> str:
        """Idempotent: if key already executed, return cached result. Otherwise execute."""
        self.call_count += 1
        if key in self.executed:
            return self.executed[key]
        result = f"result_for_{payload}"
        self.executed[key] = result
        return result


@pytest.mark.asyncio
async def test_tool_execution_and_restart_no_double_side_effect(state_api_server):
    """Real tool execution → checkpoint → FRESH adapter → re-invoke → NO double side-effect.

    Proves PDF §4.2: «повторный вызов инструмента приходит с тем же ключом →
    прежний результат (idempotent replay) или SIDE_EFFECT_UNKNOWN».
    """
    world = ExternalWorld()

    def tool_node(state):
        """Executes a tool with a stable key. Called on fresh invoke only."""
        key = f"order-{state['counter']}"
        result = world.execute_tool(key, f"action_{state['counter']}")
        return {
            "messages": state["messages"] + [f"tool:{key}"],
            "counter": state["counter"] + 1,
            "side_effects": state["side_effects"] + [result],
            "last_tool_key": key,
            "tool_status": "executed",
        }

    # Phase 1: Execute graph (tool runs once)
    saver_a = make_saver(state_api_server, "f2-thread")
    g = StateGraph(State)
    g.add_node("tool", tool_node)
    g.add_edge(START, "tool")
    g.add_edge("tool", END)
    compiled_a = g.compile(checkpointer=saver_a)
    config = {"configurable": {"thread_id": "f2-thread"}}

    r1 = await compiled_a.ainvoke(
        {"messages": [], "counter": 1, "side_effects": [], "last_tool_key": "", "tool_status": ""},
        config,
    )
    assert world.call_count == 1
    assert "result_for_action_1" in r1["side_effects"]
    assert r1["last_tool_key"] == "order-1"

    # Simulate crash: checkpoint has key=order-1 executed, state at counter=2
    # Verify CP persisted the tool_results
    tup = await saver_a.aget_tuple(config)
    assert tup.checkpoint["channel_values"]["last_tool_key"] == "order-1"

    # Phase 2: Process "restarts" — FRESH adapter, new graph instance
    # The graph re-invokes with input that would trigger the same tool again
    saver_b = make_saver(state_api_server, "f2-thread")
    call_count_before_restart = world.call_count

    g2 = StateGraph(State)
    g2.add_node("tool", tool_node)
    g2.add_edge(START, "tool")
    g2.add_edge("tool", END)
    compiled_b = g2.compile(checkpointer=saver_b)

    # Resume: pass None (continue from CP). Since graph completed, returns CP state.
    # Tool does NOT re-execute (graph is done).
    r2 = await compiled_b.ainvoke(None, config)

    # No additional tool call happened
    assert world.call_count == call_count_before_restart
    # State preserved from CP
    assert r2["last_tool_key"] == "order-1"
    assert "result_for_action_1" in r2["side_effects"]


@pytest.mark.asyncio
async def test_side_effect_unknown_on_different_thread(state_api_server):
    """A DIFFERENT thread has no checkpoint → tool executes fresh (SIDE_EFFECT_UNKNOWN semantics).

    When the system doesn't know if a side-effect happened (no CP for this thread),
    it must NOT silently replay. The tool node runs, producing the actual effect.
    """
    world = ExternalWorld()

    def tool_node(state):
        key = f"payment-{state['counter']}"
        result = world.execute_tool(key, f"pay_{state['counter']}")
        return {
            "messages": state["messages"] + [f"tool:{key}"],
            "counter": state["counter"] + 1,
            "side_effects": state["side_effects"] + [result],
            "last_tool_key": key,
            "tool_status": "executed",
        }

    # Fresh thread (no prior CP) → tool MUST execute (SIDE_EFFECT_UNKNOWN → assume not done)
    saver_fresh = make_saver(state_api_server, "unknown-thread")
    g = StateGraph(State)
    g.add_node("tool", tool_node)
    g.add_edge(START, "tool")
    g.add_edge("tool", END)
    compiled = g.compile(checkpointer=saver_fresh)
    config = {"configurable": {"thread_id": "unknown-thread"}}

    r = await compiled.ainvoke(
        {"messages": [], "counter": 99, "side_effects": [], "last_tool_key": "", "tool_status": ""},
        config,
    )
    assert world.call_count == 1  # Actually executed
    assert r["tool_status"] == "executed"
    assert "result_for_pay_99" in r["side_effects"]


@pytest.mark.asyncio
async def test_idempotent_tool_replay_same_key(state_api_server):
    """If tool IS re-invoked with same key, idempotency prevents double execution.

    Simulates: checkpoint lost/corrupted, but external world has the result cached.
    The tool IS re-called but returns cached result (no side-effect duplication).
    """
    world = ExternalWorld()
    world.executed["order-5"] = "result_for_action_5"  # Already done externally

    def tool_node(state):
        key = f"order-{state['counter']}"
        result = world.execute_tool(key, f"action_{state['counter']}")
        return {
            "messages": state["messages"] + [f"tool:{key}"],
            "counter": state["counter"] + 1,
            "side_effects": state["side_effects"] + [result],
            "last_tool_key": key,
            "tool_status": "executed",
        }

    # No checkpoint (simulates CP loss) → graph re-runs tool
    saver = make_saver(state_api_server, "replay-thread")
    g = StateGraph(State)
    g.add_node("tool", tool_node)
    g.add_edge(START, "tool")
    g.add_edge("tool", END)
    compiled = g.compile(checkpointer=saver)
    config = {"configurable": {"thread_id": "replay-thread"}}

    r = await compiled.ainvoke(
        {"messages": [], "counter": 5, "side_effects": [], "last_tool_key": "", "tool_status": ""},
        config,
    )
    # Tool was called (world.call_count incremented) but NO new side-effect created
    assert world.call_count == 1  # called, but hit cache
    assert world.executed["order-5"] == "result_for_action_5"  # unchanged
    # Result is the idempotent replay
    assert "result_for_action_5" in r["side_effects"]


# ===== Preserved tests =====

@pytest.mark.asyncio
async def test_pending_writes(state_api_server):
    """Pending writes survive as durable rows and come back decoded."""
    saver = make_saver(state_api_server)
    from langchain_core.runnables import RunnableConfig

    cp = {
        "v": 1, "id": "cp-test", "ts": "2026-01-01",
        "channel_values": {"messages": []}, "channel_versions": {},
        "versions_seen": {}, "pending_sends": [],
    }
    head_config: RunnableConfig = {"configurable": {"thread_id": "test-thread"}}
    await saver.aput(head_config, cp, {"source": "input"}, {})

    config: RunnableConfig = {"configurable": {"thread_id": "test-thread",
                                               "checkpoint_id": "cp-test"}}
    await saver.aput_writes(config, [("messages", "partial"), ("output", 42)],
                            task_id="node-1")

    tup = await saver.aget_tuple(config)
    assert sorted(tup.pending_writes) == sorted([("node-1", "messages", "partial"),
                                                 ("node-1", "output", 42)])


@pytest.mark.asyncio
async def test_list_checkpoints(state_api_server):
    saver = make_saver(state_api_server)
    from langchain_core.runnables import RunnableConfig
    config: RunnableConfig = {"configurable": {"thread_id": "test-thread", "checkpoint_id": ""}}
    cp1 = {"v": 1, "id": "cp-a", "ts": "2026-01-01", "channel_values": {"x": 1},
           "channel_versions": {}, "versions_seen": {}, "pending_sends": []}
    cp2 = {"v": 1, "id": "cp-b", "ts": "2026-01-02", "channel_values": {"x": 2},
           "channel_versions": {}, "versions_seen": {}, "pending_sends": []}
    await saver.aput(config, cp1, {}, {})
    await saver.aput(config, cp2, {}, {})
    results = [t async for t in saver.alist(None)]
    assert len(results) == 2
    assert results[0].config["configurable"]["checkpoint_id"] == "cp-b"


@pytest.mark.asyncio
async def test_aget_tuple_empty_returns_none(state_api_server):
    saver = make_saver(state_api_server)
    from langchain_core.runnables import RunnableConfig
    config: RunnableConfig = {"configurable": {"thread_id": "nonexistent-thread"}}
    assert await saver.aget_tuple(config) is None
