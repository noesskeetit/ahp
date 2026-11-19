"""Matched admission-to-terminal benchmark using the same run and tool layers."""

import argparse
import asyncio
import base64
from collections import defaultdict
import json
import os
from pathlib import Path
import time
from uuid import uuid4

from langgraph.config import get_config
from langgraph.graph import END, START, StateGraph

from ahp_state.agent import mtls_client
from ahp_state.checkpoint_saver import AHPCheckpointSaver
from ahp_state.client import StateAPIClient
from ahp_state.intents import IntentRecorder
from ahp_state.journal_checkpointer import AHPJournalCheckpointSaver
from checkpoint_pair import seed_empty, sql_delta, sql_snapshot, stats
from compact_faults import ToolRecorder, acquire, event_payload
from langgraph_probe import GraphState, read_run


def compact_view(client, token):
    return StateAPIClient(client._base + "/compact", token=token, http_client=client._client)


async def gateway_tool(client, token, key, name, arguments, tool_lease=None):
    authority = tool_lease or {}
    called = await client._request("POST", "/v1/tools/Call", token, {
        "key": key, "tool": name, "arguments": arguments, **authority,
    })
    assert called["status"] == "SUCCEEDED"
    if tool_lease is None:
        result = await client._request("POST", "/v1/tools/GetResult", token, {"key": key})
    else:
        resolved = await client._request("POST", "/v1/tools/FenceAndLookup", token, {"key": key, **authority})
        assert resolved["status"] == "COMPLETED"
        result = resolved["receipt"]
    assert result["status"] == "SUCCEEDED" and result["key"] == key
    assert result["payload"]["content"] == [{"type": "text", "text": "written"}]
    return result["payload"]


class BenchmarkToolRecorder(ToolRecorder):
    def __init__(self, journal, thread_id, epoch, worker, token, tool_lease=None):
        super().__init__(journal, thread_id, epoch, None, "benchmark")
        self.worker = worker
        self.token = token
        self.tool_lease = tool_lease

    async def invoke_tool(self, name, arguments, call_id):
        return await gateway_tool(self.worker, self.token, call_id, name, arguments, self.tool_lease)


def graph_for(saver, effect):
    async def left(_state):
        await effect()
        return {"steps": ["A"]}

    graph = StateGraph(GraphState)
    graph.add_node("a", left)
    graph.add_node("b", lambda _state: {"steps": ["B"]})
    graph.add_node("join", lambda _state: {"steps": ["joined"]})
    graph.add_edge(START, "a")
    graph.add_edge(START, "b")
    graph.add_edge(["a", "b"], "join")
    graph.add_edge("join", END)
    return graph.compile(checkpointer=saver)


async def execute_dispatched_turn(client, session, worker_token):
    started = time.perf_counter_ns()
    turn_key = "turn-" + uuid4().hex
    input_value = {"steps": ["input"]}
    queued = await client.request_state("POST", "/v1/state/sessions:enqueue", body={
        "session_id": session, "harness_type": "langgraph", "format_version": 1,
        "key": turn_key, "input": event_payload(input_value),
    })
    admitted = time.perf_counter_ns()
    worker = client.with_token(worker_token)
    turn = await worker.request_state("POST", "/v1/state/sessions:claim-turn", body={
        "session_id": session, "ttl_ms": 120000,
    })
    ready = time.perf_counter_ns()
    assert turn["session_id"] == session and turn["input_seq"] == queued["head_seq"]
    assert json.loads(base64.b64decode(turn["input"]["inline"])) == input_value
    epoch = turn["epoch"]
    saver = AHPJournalCheckpointSaver(worker, session, {"epoch": epoch})
    recorder = BenchmarkToolRecorder(worker, session, epoch, worker, worker_token, {
        "session_id": session, "lease_epoch": epoch, "tool_lease_proof": turn["tool_lease_proof"],
    })

    async def effect():
        return await recorder.call("write", {"value": turn_key}, "call-a")

    result = await graph_for(saver, effect).ainvoke(input_value, {"configurable": {"thread_id": session}}, durability="sync")
    assert result["steps"][-4:] == ["input", "A", "B", "joined"], result
    done_graph = time.perf_counter_ns()
    final = await recorder.append(turn_key + ":completed", "turn.completed", "assistant", result, finish_turn="IDLE")
    released = time.perf_counter_ns()
    status = await client.request_state("GET", "/v1/state/sessions/" + session)
    terminal = time.perf_counter_ns()
    assert status["status"] == "IDLE" and status["head_seq"] == final["head_seq"]
    return turn_times(started, admitted, ready, done_graph, terminal), (ready, released)


async def execute_turn(client, thread_id, worker_name, mode, worker_token=None):
    if mode == "dispatch":
        return await execute_dispatched_turn(client, thread_id, worker_token)
    started = time.perf_counter_ns()
    run = await client.create_run(thread_id, "bench-" + uuid4().hex, {"input": {"steps": ["input"]}})
    admitted = time.perf_counter_ns()
    assert run["status"] == "pending"
    claimed = await client.claim_run_for(worker_name, run["id"], ttl_ms=120000)
    ready = time.perf_counter_ns()
    lease = claimed["lease"]
    assert lease["worker"] == worker_name and lease["run"]["id"] == run["id"]
    payload = lease["run"]["payload"]
    assert payload == {"input": {"steps": ["input"]}}
    token = claimed["task_token"]
    worker = client.with_token(token)
    fence = {"run_id": run["id"], "epoch": lease["epoch"], "worker": worker_name}
    tool_args = {"value": run["id"]}
    journal_lease = None
    if mode == "legacy":
        saver = AHPCheckpointSaver(worker, thread_id, fence)

        async def effect():
            node = get_config()["configurable"]["__pregel_task_id"]
            return await IntentRecorder(worker, fence, node=node).guarded(
                "call-a", lambda: gateway_tool(worker, token, f"{node}:call-a", "write", tool_args),
                target="write", args=tool_args,
            )
    else:
        journal = compact_view(client, token)
        journal_lease = await acquire(journal, thread_id, 120000)
        saver = AHPJournalCheckpointSaver(journal, thread_id, {"epoch": journal_lease["epoch"]})
        recorder = BenchmarkToolRecorder(journal, thread_id, journal_lease["epoch"], worker, token)

        async def effect():
            return await recorder.call("write", tool_args, "call-a")
    graph = graph_for(saver, effect)
    value = await graph.ainvoke(payload["input"], {"configurable": {"thread_id": thread_id}}, durability="sync")
    assert value["steps"][-4:] == ["input", "A", "B", "joined"], value
    done_graph = time.perf_counter_ns()
    if journal_lease is not None:
        await journal.request_state("POST", f"/v1/state/sessions/{thread_id}:heartbeat",
                                    body={"lease_epoch": journal_lease["epoch"], "release": True})
    await worker.release(fence, "done")
    released = time.perf_counter_ns()
    assert (await read_run(client, run["id"]))["status"] == "done"
    terminal = time.perf_counter_ns()
    return turn_times(started, admitted, ready, done_graph, terminal), (ready, released)


def turn_times(started, admitted, ready, done_graph, terminal):
    return {
        "admission": (admitted - started) / 1e6,
        "claim": (ready - admitted) / 1e6,
        "graph_and_journal_claim": (done_graph - ready) / 1e6,
        "terminal": (terminal - done_graph) / 1e6,
        "turn": (terminal - started) / 1e6,
    }


async def run(client, args):
    if args.mode == "dispatch":
        await seed_empty(client, "compact", args.seed)
    else:
        await seed_empty(client, "legacy", args.seed)
        if args.mode == "compact":
            await seed_empty(compact_view(client, await client.authorization_token()), "compact", args.seed)
    start = asyncio.Event()
    warmed = [asyncio.Event() for _ in range(args.concurrency)]
    samples = defaultdict(list)
    rpc = defaultdict(list)
    peak = active = 0
    before = after = None
    measured_started = measured_done = None
    lease_intervals = []
    original = StateAPIClient._request

    async def timed(self, method, path, token, body, params=None):
        sent = time.perf_counter_ns()
        try:
            return await original(self, method, path, token, body, params)
        finally:
            if start.is_set():
                gateway = "tools" if path.startswith("/v1/tools/") else (
                    "compact" if args.mode == "dispatch" or self._base.endswith("/compact") else "legacy")
                rpc[f"{gateway} {method} {path.rsplit('/', 1)[-1]}"].append((time.perf_counter_ns() - sent) / 1e6)

    async def start_measurement():
        nonlocal before, measured_started
        await asyncio.gather(*(event.wait() for event in warmed))
        if args.sql_target_db:
            before = await sql_snapshot(args)
        measured_started = time.perf_counter_ns()
        start.set()

    async def load(index):
        nonlocal peak, active
        ident = uuid4().hex
        worker_name = "worker-" + ident
        thread_id = "thread-" + ident
        worker_token = None
        if args.mode == "dispatch":
            worker_token = args.worker_tokens[index]
        else:
            thread = await client.create_thread("bench-" + ident, thread_id)
            thread_id = thread["id"]
        for _ in range(args.history):
            await execute_turn(client, thread_id, worker_name, args.mode, worker_token)
        warmed[index].set()
        await start.wait()
        for _ in range(100 // args.concurrency):
            active += 1
            peak = max(peak, active)
            try:
                stage_times, interval = await execute_turn(client, thread_id, worker_name, args.mode, worker_token)
            finally:
                active -= 1
            for stage, duration in stage_times.items():
                samples[stage].append(duration)
            lease_intervals.append(interval)

    StateAPIClient._request = timed
    try:
        async with asyncio.TaskGroup() as workers:
            workers.create_task(start_measurement())
            for index in range(args.concurrency):
                workers.create_task(load(index))
        measured_done = time.perf_counter_ns()
        if args.sql_target_db:
            after = await sql_snapshot(args)
    finally:
        StateAPIClient._request = original
    count = len(samples["turn"])
    assert count == 100 and peak == args.concurrency
    receipt_route = "FenceAndLookup" if args.mode == "dispatch" else "GetResult"
    assert len(rpc["tools POST Call"]) == len(rpc["tools POST " + receipt_route]) == count
    peak_claimed = claimed = 0
    edges = sorted(event for claim, release in lease_intervals for event in ((claim, 1), (release, -1)))
    for _time, delta in edges:
        claimed += delta
        peak_claimed = max(peak_claimed, claimed)
    elapsed = (measured_done - measured_started) / 1e9
    report = {
        "mode": args.mode, "seed": args.seed, "history": args.history, "concurrency": args.concurrency, "peak": peak,
        "peak_claimed": peak_claimed, "elapsed_s": elapsed, "turns_per_second": count / elapsed,
        "turns": count, "samples_ms": samples, "stages": {name: stats(values) for name, values in samples.items()},
        "rpc_raw_ms": rpc, "rpc_per_turn": sum(map(len, rpc.values())) / count,
        "fraction_le_300ms": sum(value <= 300 for value in samples["turn"]) / count,
    }
    if before is not None:
        report["postgres"] = sql_delta(before, after, count)
    print(json.dumps(report), flush=True)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("legacy", "compact", "dispatch"))
    parser.add_argument("concurrency", type=int, choices=(1, 10))
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--history", type=int, required=True)
    parser.add_argument("--sql-target-db")
    parser.add_argument("--sql-monitor-db", default="postgres")
    parser.add_argument("--psql", default="psql")
    args = parser.parse_args()
    hand = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    args.worker_tokens = hand.get("worker_tokens", [])
    async with mtls_client(hand) as client:
        await run(client, args)


if __name__ == "__main__":
    asyncio.run(main())
