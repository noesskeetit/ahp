"""Native LangGraph crash recovery with real State and Tool Gateway routes."""

import argparse
import asyncio
from collections import Counter
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

import httpx

from ahp_state.agent import mtls_client
from ahp_state.client import StateAPIError
from ahp_state.journal_checkpointer import AHPJournalCheckpointSaver
from compact_faults import API, ToolRecorder, UnknownEffect, acquire
from langgraph_probe import graph_builder


class GatewayToolRecorder(ToolRecorder):
    def __init__(self, client, session, epoch, directory, mode, scenario):
        super().__init__(client, session, epoch, directory, mode)
        self.scenario = scenario

    async def gateway(self, op, body, *, drop=False):
        headers = {"Authorization": "Bearer " + await self.client.authorization_token()}
        if drop:
            headers["X-Test-Lose-Reply"] = "yes"
        return await self.client._client.post(self.client._base + f"/v1/tools/{op}", json=body, headers=headers)

    async def invoke_tool(self, name, arguments, call_id):
        assert name == "test.mutate"  # Test graph alias for the loopback MCP tool.
        try:
            response = await self.gateway("Call", {"key": call_id, "tool": "write", "arguments": arguments},
                                          drop=self.scenario == "known")
        except (httpx.RemoteProtocolError, httpx.ReadError):
            assert self.scenario == "known"
        else:
            response.raise_for_status()
            assert self.scenario == "unknown" and response.json()["status"] == "SIDE_EFFECT_UNKNOWN"
        ready = self.directory / "gateway-before-state.tmp"
        ready.write_text(call_id)
        ready.replace(self.directory / "gateway-before-state")
        await asyncio.Event().wait()

    async def recover_result(self, name, call_id):
        assert name == "test.mutate"
        response = await self.gateway("GetResult", {"key": call_id})
        if response.status_code == 409:
            status = await self.gateway("Get", {"key": call_id})
            status.raise_for_status()
            assert status.json()["status"] == "SIDE_EFFECT_UNKNOWN"
            return None
        response.raise_for_status()
        result = response.json()
        assert result["key"] == call_id and result["status"] == "SUCCEEDED" and result["result_storage"] == "inline"
        assert result["provenance"]["grant_task"] == "native-original"
        assert result["provenance"]["grant_context"] == "native-journal"
        return result["payload"]

    def project_result(self, receipt):
        assert receipt["content"] == [{"type": "text", "text": "written"}]
        return {"receipt": "confirmed"}


async def invoke_graph(client, directory, session, epoch, mode, scenario):
    saver = AHPJournalCheckpointSaver(client, session, {"epoch": epoch})
    effects = GatewayToolRecorder(client, session, epoch, directory, mode, scenario)
    graph = graph_builder(directory, mode)(SimpleNamespace(saver=saver, effects=effects, store=None))
    config = {"configurable": {"thread_id": session}}
    return await graph.ainvoke(None if mode == "resume" else {"steps": ["input"]},
                               config=config, durability="sync")


async def source(handshake, directory, session, scenario):
    async with mtls_client(handshake) as client:
        lease = await acquire(client, session, 5000)
        assert lease["epoch"] == 1 and lease["worker"] == "native-original"
        await invoke_graph(client, directory, session, lease["epoch"], "unknown", scenario)
    raise AssertionError("the original graph unexpectedly returned")


async def run(handshake, directory, scenario):
    session = f"gateway-{scenario}-{uuid4().hex}"
    marker = directory / "gateway-before-state"
    with (directory / "source.log").open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, __file__, str(directory), scenario, "--source-session", session],
            stdout=log, stderr=log,
        )
        try:
            deadline = time.monotonic() + 20
            while not (marker.is_file() and (directory / "b-unknown").is_file()):
                if process.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError((directory / "source.log").read_text() or "Gateway failpoint timed out")
                await asyncio.sleep(.05)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
    assert process.returncode == -signal.SIGKILL
    call_id = marker.read_text()

    async with mtls_client({**handshake, "task_token": handshake["replacement_token"]}) as replacement:
        deadline = time.monotonic() + 10
        while True:
            try:
                lease = await acquire(replacement, session, 30000)
                break
            except StateAPIError as error:
                if error.code != "CONFLICT" or time.monotonic() > deadline:
                    raise
                await asyncio.sleep(.1)
        assert lease["epoch"] == 2 and lease["worker"] == "native-replacement"
        heartbeat = f"{API}/sessions/{session}:heartbeat"
        async with mtls_client(handshake) as original:
            try:
                await original.request_state("POST", heartbeat, body={"lease_epoch": 1, "ttl_ms": 1000})
            except StateAPIError as error:
                assert error.code == "STALE_EPOCH"
            else:
                raise AssertionError("the first worker still held the lease")

        intent_params = {"session_id": session, "call_id": call_id}
        initial = await replacement.request_state("GET", API + "/aux:mutate", params=intent_params)
        assert initial["status"] == "DISPATCHED" and initial["gateway_operation_id"] == call_id
        try:
            result = await invoke_graph(replacement, directory, session, lease["epoch"], "resume", scenario)
        except UnknownEffect as error:
            assert str(error) == call_id
            result = None
        intent = await replacement.request_state("GET", API + "/aux:mutate", params=intent_params)
        journal = await replacement.request_state("GET", f"{API}/sessions/{session}/events")
        if scenario == "known":
            assert result == {"steps": ["input", "A", "B", "joined"]}
            assert intent["status"] == "COMPLETED" and journal["head_seq"] == 2
            assert journal["events"][1]["payload"] == intent["result"]
        else:
            assert result is None and intent["status"] == "OUTCOME_UNKNOWN" and journal["head_seq"] == 1
        await replacement.request_state("POST", heartbeat, body={"lease_epoch": 2, "release": True})

    entries = Counter(row["branch"] for row in
                      map(json.loads, (directory / "node-entries.jsonl").read_text().splitlines()))
    assert entries == {"a": 2, "b": 2}
    return {"scenario": scenario, "source_signal": "SIGKILL", "epoch": lease["epoch"],
            "stale_epoch_rejected": True, "status": intent["status"], "event_count": journal["head_seq"],
            "node_executions": entries, "graph_result": result}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("scenario", choices=("known", "unknown"))
    parser.add_argument("--source-session")
    args = parser.parse_args()
    handshake = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    if args.source_session:
        await source(handshake, args.directory, args.source_session, args.scenario)
    else:
        print(json.dumps(await run(handshake, args.directory, args.scenario)), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
