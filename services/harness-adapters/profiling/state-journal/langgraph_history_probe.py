"""Compare a native LangGraph branch with the durable State API chat view."""
import asyncio
import base64
from importlib.metadata import version
import json
import os
from pathlib import Path
from typing import Annotated, TypedDict
from uuid import uuid4

from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from ahp_state import wire
from ahp_state.agent import mtls_client
from ahp_state.journal_checkpointer import AHPJournalCheckpointSaver


class Chat(TypedDict):
    messages: Annotated[list, add_messages]


def build_graph(client, session, epoch):
    workflow = StateGraph(Chat)
    workflow.add_node("reply", lambda _: {})
    workflow.add_edge(START, "reply")
    workflow.add_edge("reply", END)
    return workflow.compile(checkpointer=AHPJournalCheckpointSaver(
        client, session, {"epoch": epoch}, chat_channel="messages",
    ))


class ReplayOnce:
    def __init__(self, client):
        self.client = client
        self.checkpoint_id = None

    async def request_state(self, method, path, **kwargs):
        result = await self.client.request_state(method, path, **kwargs)
        body = kwargs.get("body", {})
        if method == "POST" and path.endswith("/checkpoints") and self.checkpoint_id is None:
            if body.get("chat", {}).get("upserts"):
                replay = await self.client.request_state(method, path, **kwargs)
                assert replay == result
                self.checkpoint_id = result["checkpoint_id"]
        return result


async def main():
    session = "langgraph-chat-" + uuid4().hex
    base = "/v1/state/sessions/" + session
    config = {"configurable": {"thread_id": session}}
    handshake = Path(os.environ["AHP_STATE_HANDSHAKE"])
    doc = json.loads(handshake.read_text())

    async def acquire(client):
        lease = await client.request_state("POST", "/v1/state/sessions:acquire", body={
            "session_id": session, "harness_type": "langgraph", "format_version": 1, "ttl_ms": 30000,
        })
        return lease["epoch"]

    async with mtls_client(doc) as client:
        epoch = await acquire(client)
        replayed = ReplayOnce(client)
        graph = build_graph(replayed, session, epoch)
        await graph.ainvoke({"messages": [
            HumanMessage(id="question", content="Проверь отчёт"),
            AIMessage(id="answer", content="Старая версия"),
        ]}, config, durability="sync")
        original = (await graph.aget_state(config)).config
        replace = await graph.aupdate_state(original, {
            "messages": [AIMessage(id="answer", content="Новая версия")],
        }, as_node="reply")
        remove = await graph.aupdate_state(replace, {
            "messages": [RemoveMessage(id="question")],
        }, as_node="reply")
        fork = await graph.aupdate_state(original, {
            "messages": [AIMessage(id="fork", content="Ответ в соседней ветке")],
        }, as_node="reply")
        await client.request_state("POST", base + ":heartbeat", body={"lease_epoch": epoch, "release": True})

    async with mtls_client(doc) as client:
        epoch = await acquire(client)
        assert epoch == 2
        graph = build_graph(client, session, epoch)

        async def compare(label, checkpoint, expected):
            config_id = wire.encode_id(checkpoint["configurable"]["checkpoint_id"])
            native = (await graph.aget_state(checkpoint)).values["messages"]
            page = await client.request_state("GET", base + "/chat-history", params={
                "checkpoint_ns": "graph", "checkpoint_id": config_id,
            })
            assert page["checkpoint_id"] == config_id, page
            app = [(item["id"], item["content"]) for item in page["messages"]]
            assert app == [(message.id, message.content) for message in native] == expected, page
            return {"branch": label, "checkpoint": config_id, "messages": page["messages"]}

        records = [
            await compare("original", original, [("question", "Проверь отчёт"), ("answer", "Старая версия")]),
            await compare("replace", replace, [("question", "Проверь отчёт"), ("answer", "Новая версия")]),
            await compare("remove", remove, [("answer", "Новая версия")]),
            await compare("fork", fork, [
                ("question", "Проверь отчёт"), ("answer", "Старая версия"), ("fork", "Ответ в соседней ветке"),
            ]),
        ]
        latest = await client.request_state("GET", base + "/chat-history", params={"checkpoint_ns": "graph"})
        assert latest["checkpoint_id"] == records[-1]["checkpoint"]
        events = await client.request_state("GET", base + "/events")
        patches = [item for item in events["events"] if item["type"] == "langgraph.chat.patch"]
        assert len(patches) == 6, events
        assert replayed.checkpoint_id is not None
        by_id = {item["id"]: item for item in patches}
        for record in records:
            cp = await client.request_state("GET", base + "/checkpoints", params={
                "checkpoint_ns": "graph", "checkpoint_id": record["checkpoint"],
            })
            event = by_id[cp["checkpoint"]["chat_event_id"]]
            record["event_id"] = event["id"]
            record["parent_event_id"] = event.get("parent_id")
            record["delta"] = json.loads(base64.b64decode(event["payload"]["inline"]))
        assert records[1]["delta"] == {
            "upserts": [{"id": "answer", "role": "assistant", "content": "Новая версия"}],
        }
        assert records[2]["delta"] == {"remove": ["question"]}
        assert records[3]["parent_event_id"] == records[0]["event_id"]
        assert [item["id"] for item in records[3]["delta"]["upserts"]] == ["fork"]
        print(json.dumps({"langgraph": version("langgraph"), "langchain_core": version("langchain-core"),
                          "epoch": epoch, "patches": len(patches), "replayed": replayed.checkpoint_id,
                          "branches": records}, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
