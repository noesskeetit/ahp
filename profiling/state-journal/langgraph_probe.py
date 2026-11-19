"""Real LangGraph + AHP Python SDK + mTLS State API + isolated PostgreSQL."""
import asyncio
import argparse
from collections import Counter, defaultdict
from dataclasses import asdict
import fcntl
import importlib.metadata
import json
import operator
import os
from pathlib import Path
from statistics import quantiles
import subprocess
import sys
import time
from typing import Annotated, TypedDict
from uuid import uuid4

from langgraph.graph import START, END, StateGraph
from langgraph.config import get_config

from ahp_state import StateAPIClient
from ahp_state.agent import AgentService, mtls_client
from ahp_state.client import StateAPIError
from ahp_state.intents import IntentRecorder

HAND = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
RPC = defaultdict(list)
CLAIMED = set()
PEAK = 0
original_call = StateAPIClient.call


async def measured(self, operation, body=None):
    global PEAK
    started = time.perf_counter_ns()
    try:
        result = await original_call(self, operation, body)
        if operation == "ClaimRun" and (lease := result.get("lease")):
            CLAIMED.add(lease["run"]["id"])
            PEAK = max(PEAK, len(CLAIMED))
        if operation == "Release":
            CLAIMED.discard(body["run_id"])
        return result
    finally:
        RPC[operation].append((time.perf_counter_ns() - started) / 1e6)


class GraphState(TypedDict, total=False):
    steps: Annotated[list[str], operator.add]


def graph_builder(directory, mode):
    def enter(branch):
        if mode in ("pending", "unknown", "resume"):
            with (directory / "node-entries.jsonl").open("a") as stream:
                stream.write(json.dumps({"branch": branch, "mode": mode,
                                         "pid": os.getpid()}) + "\n")
            (directory / f"{branch}-{mode}").touch()

    def build(ctx):
        graph = StateGraph(GraphState)
        put_writes = ctx.saver.aput_writes
        task_ids = {}

        async def journal_pending(config, writes, task_id, task_path=""):
            await put_writes(config, writes, task_id, task_path)
            if mode != "pending" or task_ids.get("left") != task_id:
                return
            if ("steps", ["A"]) not in writes:
                return
            (directory / "a-durable-pending").touch()

        if mode == "pending":
            ctx.saver.aput_writes = journal_pending

        async def left(_):
            enter("a")
            task_ids["left"] = get_config()["configurable"]["__pregel_task_id"]
            (directory / "a-task-id").write_text(task_ids["left"])
            result = await ctx.effects.bind("left").call("test.mutate", {"value": 1}, call_id="call-a")
            assert result == {"receipt": "confirmed"}
            return {"steps": ["A"]}

        async def right(_):
            enter("b")
            if mode in ("pending", "unknown"):
                await asyncio.Event().wait()
            return {"steps": ["B"]}

        graph.add_node("a", left)
        graph.add_node("b", right)
        graph.add_node("join", lambda state: {"steps": ["joined"]})
        graph.add_edge(START, "a")
        graph.add_edge(START, "b")
        graph.add_edge(["a", "b"], "join")
        graph.add_edge("join", END)
        return graph.compile(checkpointer=ctx.saver, store=ctx.store)
    return build


async def drive(client, directory, mode, run_id, worker, ttl=120000):
    async def effect(_tool, _arguments):
        with (directory / "effects.jsonl").open("a") as out:
            out.write(json.dumps({"call_id": "call-a", "pid": os.getpid(),
                                  "unix_ns": time.time_ns(), "worker": worker,
                                  "run_id": run_id}) + "\n")
            out.flush()
            if sys.platform == "darwin":
                fcntl.fcntl(out.fileno(), fcntl.F_FULLFSYNC)
            else:
                os.fsync(out.fileno())
        if mode == "unknown":
            (directory / "effect-before-ack").touch()
            await asyncio.Event().wait()
        return {"receipt": "confirmed"}

    service = AgentService(client=client, graph=graph_builder(directory, mode), effects=effect,
                           worker=worker, lease_ttl_ms=ttl, heartbeat_ms=60000)
    return asdict(await service.run_once(run_id=run_id))


async def enqueue(client, thread_id=None):
    key = uuid4().hex
    if thread_id is None:
        thread_id = (await client.create_thread("probe-" + key, "thread-" + key))["id"]
    run = await client.create_run(thread_id, "run-" + key, {"input": {"steps": ["input"]}})
    return run["id"]


async def wait_for(action, timeout=20):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        result = await action()
        if result:
            return result
        await asyncio.sleep(0.03)
    raise TimeoutError("test condition never became true")


async def read_run(client, run_id):
    doc = await client.call("GetRun", {"run_id": run_id})
    return doc.get("run", doc)


async def crash_trial(client, directory, mode):
    directory.mkdir()
    run_id = await enqueue(client)
    assert (await read_run(client, run_id))["status"] == "pending"
    # Claim is made by the crashed process via normal worker entry point.
    with (directory / "worker.log").open("w") as log:
        child = subprocess.Popen([sys.executable, __file__, "child", str(directory), mode, run_id],
                                 stdout=log, stderr=subprocess.STDOUT)
        async def ready():
            assert child.poll() is None, (directory / "worker.log").read_text()
            marker = "a-durable-pending" if mode == "pending" else "effect-before-ack"
            return (directory / marker).exists() and (directory / ("b-" + mode)).exists()

        try:
            await wait_for(ready)
            assert (await read_run(client, run_id))["status"] == "running"
        finally:
            if child.poll() is None:
                child.kill()
            returncode = child.wait(timeout=5)
    assert returncode == -9, returncode
    task_id = (directory / "a-task-id").read_text()
    effects = (directory / "effects.jsonl").read_text().splitlines()
    assert len(effects) == 1, effects

    # Observe the actual second ClaimRun. This is a new grant, not adopting the
    # crashed process's credentials or modifying lease expiry directly in SQL.
    async def get_claim():
        result = await client.claim_run("replacement", ttl_ms=120000, run_id=run_id)
        return result if result.get("lease") else None

    claim = await wait_for(get_claim, timeout=15)
    lease = claim["lease"]
    fence = {"run_id": run_id, "epoch": lease["epoch"], "worker": lease["worker"]}
    assert fence["epoch"] == 2, fence
    stale = {"run_id": run_id, "epoch": 1, "worker": "crashed"}
    try:
        await client.heartbeat(stale, ttl_ms=1000)
    except StateAPIError as exc:
        assert exc.code == "STALE_EPOCH", exc
    else:
        raise AssertionError("stale worker heartbeat accepted")
    recorder = IntentRecorder(client.with_token(claim["task_token"]), fence, node=task_id)
    before = await recorder.get("call-a")
    assert before["status"] == ("succeeded" if mode == "pending" else "dispatched"), before

    async def should_not_dispatch(*_):
        with (directory / "effects.jsonl").open("a") as out:
            out.write(json.dumps({"call_id": "call-a", "epoch": fence["epoch"],
                                  "pid": os.getpid(), "unix_ns": time.time_ns()}) + "\n")
            out.flush()
            os.fsync(out.fileno())
        raise AssertionError("external action dispatched a second time")
    service = AgentService(client=client, graph=graph_builder(directory, "resume"),
                           effects=should_not_dispatch, worker="replacement")
    outcome = asdict(await service.run_leased(fence, claim["task_token"],
                                            thread_id=lease["run"]["thread_id"],
                                            payload={"input": {"steps": ["input"]}}))
    observed = await read_run(client, run_id)
    executions = Counter(json.loads(line)["branch"]
                         for line in (directory / "node-entries.jsonl").read_text().splitlines())
    if mode == "pending":
        assert outcome["status"] == "done", outcome
        assert outcome["result"]["steps"] == ["input", "A", "B", "joined"], outcome
        assert (directory / "b-resume").is_file()
        assert not (directory / "a-resume").exists()
        assert executions == {"a": 1, "b": 2}, executions
        assert observed["status"] == "done", observed
    else:
        assert outcome["status"] == "awaiting_reconciliation", outcome
        assert outcome["wait_for"] == "reconcile:call-a", outcome
        assert observed["status"] == "waiting", observed
    assert (directory / "effects.jsonl").read_text().splitlines() == effects
    return {"run_id": run_id, "signal": returncode, "new_epoch": fence["epoch"],
            "old_epoch_rejected": True, "effect_count": len(effects),
            "intent_before_resume": before["status"], "run_status": observed["status"],
            "outcome": outcome["status"], "wait_for": outcome["wait_for"],
            "a_reran": (directory / "a-resume").exists(),
            "b_reran": (directory / "b-resume").exists(),
            "node_executions": dict(executions),
            "result": outcome["result"]}


def stats(values):
    p50, p95, p99 = [quantiles(values, n=100, method="inclusive")[i] for i in (49, 94, 98)] if len(values) > 1 else [values[0]] * 3
    return {"n": len(values), "p50_ms": round(p50, 3), "p95_ms": round(p95, 3), "p99_ms": round(p99, 3)}


async def main():
    async with mtls_client(HAND) as client:
        if sys.argv[1] == "child":
            outcome = await drive(client, Path(sys.argv[2]), sys.argv[3], sys.argv[4], "crashed", ttl=4000)
            print(json.dumps(outcome))
            return
        directory = Path(sys.argv[2])
        runs = {}
        if sys.argv[1] == "faults":
            for mode in ("pending", "unknown"):
                runs[mode] = await crash_trial(client, directory / mode, mode)
                print(json.dumps({mode: runs[mode]}), flush=True)
            return
        parser = argparse.ArgumentParser()
        parser.add_argument("mode", choices=["bench"])
        parser.add_argument("directory", type=Path)
        parser.add_argument("concurrency", type=int, choices=[1, 10])
        parser.add_argument("--seed-threads", type=int, default=0)
        parser.add_argument("--prior-turns", type=int, default=0)
        args = parser.parse_args()
        await benchmark(client, args.directory, args.concurrency,
                        args.seed_threads, args.prior_turns)


async def seed(client, directory, concurrency, seed_threads, prior_turns):
    if seed_threads:
        pending = iter(range(seed_threads))
        prefix = uuid4().hex

        async def admit_threads():
            for item in pending:
                key = f"seed-{prefix}-{item}"
                await client.create_thread(key, key)

        await asyncio.gather(*(admit_threads() for _ in range(10)))
    if not prior_turns:
        return [None] * concurrency

    async def new_history(worker):
        key = uuid4().hex
        thread_id = (await client.create_thread(f"history-{key}", f"history-{key}"))["id"]
        for _ in range(prior_turns):
            run_id = await enqueue(client, thread_id)
            outcome = await drive(client, directory, "seed", run_id, f"seed-{worker}")
            assert outcome["status"] == "done", outcome
        return thread_id

    return await asyncio.gather(*(new_history(i) for i in range(concurrency)))


async def benchmark(client, directory, concurrency, seed_threads, prior_turns):
    global PEAK
    threads = await seed(client, directory, concurrency, seed_threads, prior_turns)
    StateAPIClient.call = measured
    for i in range(5):
        run_id = await enqueue(client)
        assert (await drive(client, directory, "warmup", run_id, "warmup"))["status"] == "done"
    assert not CLAIMED
    RPC.clear()
    PEAK = 0
    turn_ms = []
    admission_ms = []
    accepted = 0
    statuses = defaultdict(int)

    async def worker(worker_index):
        nonlocal accepted
        for i in range(100 // concurrency):
            started = time.perf_counter_ns()
            run_id = await enqueue(client, threads[worker_index])
            admission_ms.append((time.perf_counter_ns() - started) / 1e6)
            accepted += 1
            outcome = await drive(client, directory, "benchmark", run_id,
                                  f"benchmark-{worker_index}")
            turn_ms.append((time.perf_counter_ns() - started) / 1e6)
            statuses[outcome["status"]] += 1

    def transactions():
        psql = os.environ["BENCH_PSQL"]
        query = ("select xact_commit, xact_rollback from pg_stat_database "
                 "where datname = :'target_database';")
        output = subprocess.check_output([psql, "-h", os.environ["BENCH_PGSOCKET"],
                                         "-p", os.environ["BENCH_PGPORT"],
                                         "-U", "postgres", "-d", "postgres", "-At",
                                         "-v", "target_database=" + os.environ["BENCH_PGDATABASE"]],
                                         input=query, text=True)
        return list(map(int, output.strip().split("|")))

    # Let PostgreSQL publish accumulated counters. Query a separate database so
    # observer transactions are excluded from the measured database.
    await asyncio.sleep(1.2)
    before = transactions()
    wall_start = time.perf_counter()
    await asyncio.gather(*(worker(i) for i in range(concurrency)))
    elapsed = time.perf_counter() - wall_start
    await asyncio.sleep(1.2)
    after = transactions()
    assert not CLAIMED, "unfinished leased runs at benchmark end"
    print(json.dumps({
        "langgraph": importlib.metadata.version("langgraph"),
        "concurrency": concurrency, "accepted": accepted, "max_claimed": PEAK,
        "seed_threads": seed_threads, "prior_turns_per_thread": prior_turns,
        "status_counts": statuses, "elapsed_s": round(elapsed, 3),
        "http_calls_per_turn": round(sum(map(len, RPC.values())) / accepted, 3),
        "pg_commits_observed": after[0] - before[0],
        "pg_rollbacks_observed": after[1] - before[1],
        "completed_per_s": round(statuses["done"] / elapsed, 3),
        "turn": stats(turn_ms), "admission": stats(admission_ms),
        "turn_raw_ms": turn_ms, "admission_raw_ms": admission_ms,
        "state_api_and_pg": {op: stats(samples) for op, samples in sorted(RPC.items())},
        "rpc_raw_ms": RPC}, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
