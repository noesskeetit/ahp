"""Run the baseline's real LangGraph fan-out against the compact journal stand."""
import argparse
import asyncio
import base64
from collections import Counter
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

from langgraph.config import get_config

from ahp_state.agent import mtls_client
from ahp_state.client import StateAPIError
from ahp_state.journal_checkpointer import AHPJournalCheckpointSaver
from langgraph_probe import graph_builder

API = "/v1/state"


def encode(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def event_payload(value):
    return {"inline": base64.b64encode(encode(value)).decode()}


class UnknownEffect(Exception):
    pass


class ToolRecorder:
    """Exercise journal mutation boundaries with a fsynced synthetic tool."""

    def __init__(self, client, session, epoch, directory, mode):
        self.client = client
        self.session = session
        self.epoch = epoch
        self.path = f"{API}/sessions/{session}"
        self.directory = directory
        self.mode = mode

    def bind(self, _node):
        return self

    async def transition(self, call_id, status):
        await self.client.request_state("POST", API + "/aux:mutate", body={
            "session_id": self.session, "lease_epoch": self.epoch,
            "transition": {"call_id": call_id, "gateway_operation_id": call_id,
                           "status": status},
        })

    async def append(self, key, kind, role, value, **mutations):
        page = await self.client.request_state("GET", self.path + "/events", params={"limit": 1})
        return await self.client.request_state("POST", self.path + "/events:append", body={
            "lease_epoch": self.epoch, "key": key, "expected_seq": page["head_seq"],
            "events": [{"id": key, "type": kind, "role": role, "codec": "json",
                        "payload": event_payload(value)}],
            **mutations,
        })

    async def call(self, name, arguments, call_id):
        native_task = get_config()["configurable"]["__pregel_task_id"]
        call_id = f"{native_task}:{call_id}"
        digest = hashlib.sha256(encode(arguments)).digest()
        try:
            row = await self.client.request_state("GET", API + "/aux:mutate", params={
                "session_id": self.session, "call_id": call_id,
            })
        except StateAPIError as exc:
            if exc.code != "NOT_FOUND":
                raise
        else:
            if row["tool_name"] != name or base64.b64decode(row["args_digest"]) != digest:
                raise ValueError("tool call identity was reused for a different action")
            if row["status"] == "COMPLETED":
                return self.project_result(json.loads(base64.b64decode(row["result"]["inline"])))
            if row["status"] == "DISPATCHED":
                receipt = await self.recover_result(name, call_id)
                if receipt is not None:
                    return await self.complete(call_id, receipt)
                await self.transition(call_id, "OUTCOME_UNKNOWN")
            raise UnknownEffect(call_id)

        if self.mode == "resume":
            raise AssertionError("a cold replacement attempted a new tool action")
        await self.append(f"prepare:{call_id}", "tool.call", "tool_call", arguments, intents=[{
            "call_id": call_id, "tool_name": name, "args_digest": list(digest),
            "idempotency_key": call_id,
        }])
        await self.transition(call_id, "DISPATCHED")
        result = await self.invoke_tool(name, arguments, call_id)
        return await self.complete(call_id, result)

    async def recover_result(self, _name, _call_id):
        return None

    def project_result(self, receipt):
        return receipt

    async def invoke_tool(self, _name, _arguments, call_id):
        with (self.directory / "effects.jsonl").open("a") as stream:
            stream.write(json.dumps({"pid": os.getpid(), "call_id": call_id,
                                     "epoch": self.epoch, "time": time.time_ns()}) + "\n")
            stream.flush()
            if sys.platform == "darwin":
                fcntl.fcntl(stream.fileno(), fcntl.F_FULLFSYNC)
            else:
                os.fsync(stream.fileno())
        if self.mode == "unknown":
            (self.directory / "effect-before-ack").touch()
            await asyncio.Event().wait()
        return {"receipt": "confirmed"}

    async def complete(self, call_id, receipt):
        await self.append(f"result:{call_id}", "tool.result", "tool_result", receipt, tool_outcomes=[{
            "call_id": call_id, "gateway_operation_id": call_id, "status": "COMPLETED",
            "result": event_payload(receipt),
        }])
        return self.project_result(receipt)


async def acquire(client, session, ttl_ms):
    return await client.request_state("POST", API + "/sessions:acquire", body={
        "session_id": session, "harness_type": "langgraph", "format_version": 1, "ttl_ms": ttl_ms,
    })


async def drive(client, directory, session, epoch, mode):
    saver = AHPJournalCheckpointSaver(client, session, {"epoch": epoch})
    effects = ToolRecorder(client, session, epoch, directory, mode)
    graph = graph_builder(directory, mode)(SimpleNamespace(saver=saver, effects=effects, store=None))
    config = {"configurable": {"thread_id": session}}
    input_value = None if mode == "resume" else {"steps": ["input"]}
    return await graph.ainvoke(input_value, config=config, durability="sync")


async def child(client, directory, session, mode):
    lease = await acquire(client, session, 6000)
    result = await drive(client, directory, session, lease["epoch"], mode)
    raise AssertionError(f"crash worker unexpectedly completed: {result!r}")


async def trial(client, directory, mode):
    directory.mkdir()
    session = f"compact-{mode}-{uuid4().hex}"
    with (directory / "child.log").open("wt") as log:
        proc = subprocess.Popen([sys.executable, __file__, str(directory), "--child", mode,
                                 "--session", session], stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 15
            marker = "a-durable-pending" if mode == "pending" else "effect-before-ack"
            while not ((directory / marker).is_file() and (directory / f"b-{mode}").is_file()):
                if proc.poll() is not None:
                    raise AssertionError((directory / "child.log").read_text())
                if time.monotonic() > deadline:
                    raise TimeoutError("child never reached the crash point")
                await asyncio.sleep(0.03)
            proc.send_signal(signal.SIGKILL)
            status = proc.wait(timeout=5)
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=5)
    assert status == -signal.SIGKILL, status

    deadline = time.monotonic() + 15
    while True:
        try:
            recovered = await acquire(client, session, 30000)
            break
        except StateAPIError as exc:
            if exc.code != "CONFLICT" or time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.1)
    assert recovered["epoch"] == 2, recovered
    heartbeat = f"{API}/sessions/{session}:heartbeat"
    try:
        await client.request_state("POST", heartbeat, body={"lease_epoch": 1, "ttl_ms": 1000})
    except StateAPIError as exc:
        assert exc.code == "STALE_EPOCH", exc
    else:
        raise AssertionError("heartbeat succeeded with the expired lease")

    held = False
    result = None
    try:
        result = await drive(client, directory, session, recovered["epoch"], "resume")
    except UnknownEffect as exc:
        held = True
        row = await client.request_state("GET", API + "/aux:mutate", params={
            "session_id": session, "call_id": str(exc),
        })
        assert row["status"] == "OUTCOME_UNKNOWN", row
    executed = Counter(json.loads(line)["branch"]
                       for line in (directory / "node-entries.jsonl").read_text().splitlines())
    if mode == "pending":
        assert not held and result == {"steps": ["input", "A", "B", "joined"]}, result
        assert executed == {"a": 1, "b": 2}, executed
    else:
        assert held and result is None, result
    effects = (directory / "effects.jsonl").read_text().splitlines()
    assert len(effects) == 1, effects
    await client.request_state("POST", heartbeat, body={"lease_epoch": recovered["epoch"], "release": True})
    return {"mode": mode, "session": session, "sigkill": status, "epoch": recovered["epoch"],
            "old_epoch_rejected": True, "effects": len(effects), "node_executions": dict(executed),
            "reconcile_required": held, "result": result}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--child", choices=("pending", "unknown"))
    parser.add_argument("--session")
    args = parser.parse_args()
    hand = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    async with mtls_client(hand) as client:
        if args.child:
            await child(client, args.directory, args.session, args.child)
        else:
            for mode in ("pending", "unknown"):
                print(json.dumps(await trial(client, args.directory / mode, mode)), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
