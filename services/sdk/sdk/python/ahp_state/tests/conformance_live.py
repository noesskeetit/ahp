"""Actual LangGraph, real owner HTTP/mTLS/PG; no model and no local state DB."""
import asyncio
import faulthandler
import json
import sys
from ahp_state import AHPCheckpointSaver, AHPStore, StateAPIError, SideEffectUnknown
from ahp_state.agent import mtls_client, Effects
from ahp_state.intents import IntentRecorder
from langgraph.graph import StateGraph, START, END
from langgraph.types import interrupt, Command
from typing_extensions import TypedDict

faulthandler.dump_traceback_later(25, exit=True)


class Values(TypedDict):
    count: int
    answer: str
    blob: bytes


with open(sys.argv[1], encoding="utf-8") as stream:
    hand = json.load(stream)
phase = sys.argv[2]
client = mtls_client(hand)
saver = AHPCheckpointSaver(client, hand["thread_id"], hand["fence"])
store = AHPStore(client, hand["fence"])
config = {"configurable": {"thread_id": hand["thread_id"]},
          "metadata": {"fixture": "кириллица"}}


def graph():
    g = StateGraph(Values)
    g.add_node("first", lambda state: {"count": state["count"] + 1})
    g.add_node("second", lambda state: {"count": state["count"] + 1})
    g.add_edge(START, "first"); g.add_edge("first", "second"); g.add_edge("second", END)
    return g.compile(checkpointer=saver, store=store)


def sync_saver():
    g = graph()
    result = g.invoke({"count": 0, "blob": b"\x00\xff"}, config, durability="sync")
    assert result["count"] == 2
    for _ in range(3):
        assert saver.get_tuple(config).checkpoint["channel_values"]["count"] == 2
    checkpoints = list(saver.list(config))
    assert len(checkpoints) >= 3
    older = list(saver.list(config, before=checkpoints[1].config))
    assert [cp.checkpoint["id"] for cp in older] == [cp.checkpoint["id"] for cp in checkpoints[2:]], "before returned newer checkpoints"
    assert list(saver.list(config, limit=0)) == []
    assert list(saver.list(config, filter={"source": "input"}))[0].metadata["source"] == "input"
    cp = saver.get_tuple(config)
    assert cp.checkpoint["channel_values"]["blob"] == b"\x00\xff"
    assert cp.metadata["fixture"] == "кириллица"
    assert cp.parent_config and cp.metadata["source"] == "loop"
    saver.put_writes(cp.config, [("answer", b"\x00\xff")], "pending-task")
    assert ("pending-task", "answer", b"\x00\xff") in saver.get_tuple(config).pending_writes
    extra = {"source": "update", "fixture": {"nested": [1, "кириллица"], "raw": b"\x00\xff"}}
    saved = saver.put(cp.config, dict(cp.checkpoint, id="serialized-metadata"), extra, {})
    assert saver.get_tuple(saved).metadata["fixture"] == extra["fixture"]
    assert len(list(saver.list(saved))) == 1


async def mixed_store():
    original = store._client
    def sync_write(index):
        with store.with_identity(node="sync-worker", call=f"call-{index}"):
            store.put(("conformance",), f"key-{index}", {"index": index})
        assert store.get(("conformance",), f"key-{index}").value == {"index": index}
    async def async_write(index):
        with store.with_identity(node="async-worker", call=f"call-{index}"):
            await store.aput(("conformance",), f"key-{index}", {"index": index})
        assert (await store.aget(("conformance",), f"key-{index}")).value == {"index": index}
    await asyncio.gather(*(asyncio.to_thread(sync_write, i) for i in range(4)),
                         *(async_write(i) for i in range(4, 8)))
    assert store._client is original, "sync calls changed shared client"
    for i in range(8):
        assert (await store.aget(("conformance",), f"key-{i}")).value == {"index": i}
    assert len(await store.asearch(("conformance",))) == 8


async def async_saver():
    result = await graph().ainvoke({"count": 0, "blob": b"\x00\xff"}, config, durability="sync")
    assert result["count"] == 2
    checkpoints = [cp async for cp in saver.alist(config)]
    assert len(checkpoints) >= 3
    older = [cp async for cp in saver.alist(config, before=checkpoints[1].config)]
    assert [cp.checkpoint["id"] for cp in older] == [cp.checkpoint["id"] for cp in checkpoints[2:]], "before returned newer checkpoints"
    assert [cp async for cp in saver.alist(config, limit=0)] == []
    cp = await saver.aget_tuple(config)
    assert cp.checkpoint["channel_values"]["blob"] == b"\x00\xff"
    assert cp.metadata["fixture"] == "кириллица"
    assert cp.parent_config and cp.metadata["source"] == "loop"
    await saver.aput_writes(cp.config, [("answer", b"\x00\xff")], "pending-task")
    assert ("pending-task", "answer", b"\x00\xff") in (await saver.aget_tuple(config)).pending_writes
    extra = {"source": "update", "fixture": {"nested": [1, "кириллица"], "raw": b"\x00\xff"}}
    saved = await saver.aput(cp.config, dict(cp.checkpoint, id="serialized-metadata"), extra, {})
    assert (await saver.aget_tuple(saved)).metadata["fixture"] == extra["fixture"]
    assert len([cp async for cp in saver.alist(saved)]) == 1


async def hitl():
    async def first(state):
        old = await store.aget(("hitl",), "executions")
        await store.aput(("hitl",), "executions", {"n": (old.value["n"] if old else 0) + 1})
        return {"count": state["count"] + 1}
    def ask(state):
        return {"answer": interrupt("continue?")}
    g = StateGraph(Values);g.add_node("first", first);g.add_node("ask", ask)
    g.add_edge(START, "first");g.add_edge("first", "ask");g.add_edge("ask", END)
    compiled = g.compile(checkpointer=saver, store=store)
    if phase.endswith("write"):
        result = await compiled.ainvoke({"count": 0}, config, durability="sync")
        assert result["__interrupt__"]
    else:
        result = await compiled.ainvoke(Command(resume="yes"), config, durability="sync")
        assert result["answer"] == "yes" and result["count"] == 1
        assert (await store.aget(("hitl",), "executions")).value["n"] == 1


async def pending_restart():
    async def good(state):
        old = await store.aget(("pending",), "good")
        await store.aput(("pending",), "good", {"n": (old.value["n"] if old else 0) + 1})
        return {"count": 1}
    async def bad(state):
        if phase.endswith("write"):
            await asyncio.sleep(.1)
            raise ValueError("injected sibling failure")
        return {"answer": "recovered"}
    g=StateGraph(Values);g.add_node("good",good);g.add_node("bad",bad)
    g.add_edge(START,"good");g.add_edge(START,"bad");g.add_edge("good",END);g.add_edge("bad",END)
    compiled=g.compile(checkpointer=saver,store=store)
    if phase.endswith("write"):
        try: await compiled.ainvoke({"count":0},config,durability="sync")
        except ValueError as error: assert str(error)=="injected sibling failure"
        else: raise AssertionError("failure did not fire")
    else:
        result=await compiled.ainvoke(None,config,durability="sync")
        assert result["count"]==1 and result["answer"]=="recovered"
        assert (await store.aget(("pending",),"good")).value["n"]==1


async def failure():
    seen=[]
    def first(state): seen.append("first"); return {"count":1}
    def second(state): seen.append("second"); return {"count":2}
    g=StateGraph(Values);g.add_node("first",first);g.add_node("second",second)
    g.add_edge(START,"first");g.add_edge("first","second");g.add_edge("second",END)
    try: await g.compile(checkpointer=saver,store=store).ainvoke({"count":0},config,durability="sync")
    except StateAPIError: pass
    else: raise AssertionError("checkpoint failure did not stop graph")
    assert seen==["first"], seen


async def effects_loop():
    from langgraph.config import get_config
    tasks_seen = []
    positional = phase.startswith("effects-positional-")
    unknown = phase.startswith("effects-unknown-")
    async def transport(tool, args):
        response = await client.http_client.post(hand["base_url"] + "/fixture/effect", json=args)
        response.raise_for_status()
        if unknown and phase.endswith("write"):
            raise RuntimeError("injected tool response loss after physical effect")
        return response.json()
    effects = Effects(client, hand["fence"], transport)
    def model_node(state):
        # Synthetic persisted model output, same tool_call_id across iterations.
        return {"count": state["count"] + 1, "answer": "call_0"}
    async def mutate(state):
        tasks_seen.append(get_config()["configurable"]["__pregel_task_id"])
        # Invalid explicit identity and unbound positional calls must stop before IO.
        try:
            await Effects(client, hand["fence"], transport, task_id="wrong-task").call("synthetic", {}, call_id="no-effect")
        except ValueError:
            pass
        else:
            raise AssertionError("explicit foreign task_id accepted")
        try:
            await effects.call("synthetic", {})
        except ValueError:
            pass
        else:
            raise AssertionError("unbound graph position accepted")
        view = effects.bind("mutate")
        call_id = None if positional else state["answer"]
        value = await view.call("synthetic", {"n": state["count"]}, call_id=call_id)
        assert value == {"n": state["count"]}
        try:
            await view.call("synthetic", {"n": 99}, call_id=view.last_call_id)
        except StateAPIError as error:
            assert error.code == "INTENT_ARG_MISMATCH"
        else:
            raise AssertionError("changed arguments under same task/call accepted")
        if state["count"] == 1:
            interrupt("resume after completed effect, before node checkpoint")
        return {}
    g = StateGraph(Values);g.add_node("model",model_node);g.add_node("mutate",mutate)
    g.add_edge(START,"model");g.add_edge("model","mutate")
    g.add_conditional_edges("mutate", lambda state: "model" if state["count"] < 2 else END)
    compiled = g.compile(checkpointer=saver,store=store)
    if phase.startswith("effects-legacy-"):
        legacy = IntentRecorder(client,hand["fence"],node="mutate")
        await legacy.record("call_0",kind="tool",target="synthetic",args={"n":1})
        await legacy.dispatch("call_0")
        result = await transport("synthetic",{"n":1})
        expected = "dispatched"
        if phase.endswith("completed"):
            await legacy.succeed("call_0",result); expected="succeeded"
        elif phase.endswith("unknown"):
            await legacy.mark_unknown("call_0"); expected="unknown"
        try:
            await compiled.ainvoke({"count":0},config,durability="sync")
        except StateAPIError as error:
            assert error.code=="INTENT_IDENTITY_MIGRATION_REQUIRED"
        else:
            raise AssertionError("legacy intent silently mapped into new task identity")
        assert (await legacy.get("call_0"))["status"]==expected
        assert await IntentRecorder(client,hand["fence"],node=tasks_seen[0]).get("call_0") is None
        return
    if unknown:
        try:
            await compiled.ainvoke({"count":0} if phase.endswith("write") else None,config,durability="sync")
        except (RuntimeError, SideEffectUnknown) as error:
            if phase.endswith("write"):
                assert str(error)=="injected tool response loss after physical effect"
                await store.aput(("effect-proof",),"first-task",{"task":tasks_seen[0]})
            else:
                assert isinstance(error, SideEffectUnknown)
                assert (await store.aget(("effect-proof",),"first-task")).value["task"]==tasks_seen[0]
            intent = await client.get_intent(hand["fence"]["run_id"],tasks_seen[0],"call_0")
            assert intent["status"]=="unknown"
        else:
            raise AssertionError("UNKNOWN effect was silently retried/completed")
        return
    if phase.endswith("write"):
        result = await compiled.ainvoke({"count":0},config,durability="sync")
        assert result["__interrupt__"] and len(tasks_seen)==1
        await store.aput(("effect-proof",),"first-task",{"task":tasks_seen[0]})
    else:
        result = await compiled.ainvoke(Command(resume="continue"),config,durability="sync")
        assert result["count"]==2 and len(tasks_seen)==2 and tasks_seen[0]!=tasks_seen[1]
        assert (await store.aget(("effect-proof",),"first-task")).value["task"]==tasks_seen[0]
        for task_id in tasks_seen:
            intent = await client.get_intent(hand["fence"]["run_id"],task_id,"effect-0" if positional else "call_0")
            assert intent["status"]=="succeeded"


async def main():
    try:
        if phase=="mixed-store": await mixed_store()
        elif phase=="async-saver": await async_saver()
        elif phase.startswith("hitl-"): await hitl()
        elif phase.startswith("pending-restart-"): await pending_restart()
        elif phase=="failure": await failure()
        elif phase.startswith("effects-"): await effects_loop()
        else: raise AssertionError(phase)
    finally:
        await client.close()


if phase=="sync-saver":
    try:
        sync_saver()
    finally:
        asyncio.run(client.close())
else:
    asyncio.run(main())
faulthandler.cancel_dump_traceback_later()
print(phase+" real PG/mTLS/LangGraph PASS")
