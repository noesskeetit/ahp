"""Paired native LangGraph checkpoint measurement over the same mTLS client."""
import argparse
import asyncio
from collections import Counter, defaultdict
import json
import os
from pathlib import Path
from statistics import quantiles
import time
from uuid import uuid4

from langgraph.graph import START, END, StateGraph

from ahp_state.agent import mtls_client
from ahp_state.checkpoint_saver import AHPCheckpointSaver
from ahp_state.client import StateAPIClient
from ahp_state.journal_checkpointer import AHPJournalCheckpointSaver
from langgraph_probe import GraphState

API = "/v1/state"
SQL_COUNTERS = ("calls", "rows", "wal_bytes", "shared_blks_hit", "shared_blks_read", "shared_blks_dirtied")
TRANSACTION_SQL = {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "ABORT", "SAVEPOINT", "RELEASE"}
SQL_SNAPSHOT = """
SELECT json_build_object(
    'dealloc', info.dealloc, 'stats_reset', info.stats_reset,
    'statements', (
        SELECT coalesce(json_agg(stat), '[]') FROM (
            SELECT userid, queryid::text, query, calls, rows, wal_bytes,
                   shared_blks_hit, shared_blks_read, shared_blks_dirtied
            FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = :'target_db') AND toplevel
        ) stat
    )
)
FROM pg_stat_statements_info info
WHERE EXISTS (SELECT 1 FROM pg_database WHERE datname = :'target_db');
"""


async def sql_snapshot(args):
    process = await asyncio.create_subprocess_exec(
        args.psql, "-XwAt", "-d", args.sql_monitor_db, "-v", "ON_ERROR_STOP=1",
        "-v", f"target_db={args.sql_target_db}",
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(SQL_SNAPSHOT.encode()), timeout=15)
    except BaseException:
        if process.returncode is None:
            process.kill()
            await process.wait()
        raise
    if process.returncode or not stdout.strip():
        raise RuntimeError("PostgreSQL counter read failed: " + stderr.decode().strip())
    return json.loads(stdout)


def sql_delta(before, after, graph_count):
    if any(before[key] != after[key] for key in ("dealloc", "stats_reset")):
        raise ValueError("pg_stat_statements evicted or reset counters during the measurement")
    fields = SQL_COUNTERS
    prior = {(item["userid"], item["queryid"]): item for item in before["statements"]}
    operations = Counter()
    totals = Counter()
    changes = []
    for item in after["statements"]:
        old = prior.pop((item["userid"], item["queryid"]), {})
        delta = {field: item[field] - old.get(field, 0) for field in fields}
        if any(value < 0 for value in delta.values()):
            raise ValueError("PostgreSQL statement counters went backwards")
        if not delta["calls"]:
            continue
        operation = item["query"].lstrip().split(maxsplit=1)[0].rstrip(";").upper()
        operations[operation] += delta["calls"]
        totals.update(delta)
        changes.append({"query": item["query"], "queryid": item["queryid"], **delta})
    if prior or not totals["calls"]:
        raise ValueError("PostgreSQL statement snapshot is incomplete")
    changes.sort(key=lambda item: (-item["calls"], item["queryid"]))
    transaction_commands = sum(operations[key] for key in TRANSACTION_SQL)
    return {
        "source": "pg_stat_statements, completed top-level statements, target database",
        "operation_counts": dict(sorted(operations.items())), "totals": totals,
        "transaction_commands": transaction_commands,
        "sql_statements_per_graph": totals["calls"] / graph_count,
        "non_transaction_sql_per_graph": (totals["calls"] - transaction_commands) / graph_count,
        "statement_wal_bytes_per_graph": totals["wal_bytes"] / graph_count,
        "statements": changes,
    }


async def acquire(client, mode):
    unique = uuid4().hex
    if mode == "legacy":
        thread = await client.create_thread(f"measure-{unique}", f"thread-{unique}")
        run = await client.create_run(thread["id"], f"run-{unique}", {"input": {}})
        result = await client.claim_run(f"worker-{unique}", run_id=run["id"], ttl_ms=120000)
        lease = result["lease"]
        fence = {"run_id": run["id"], "epoch": lease["epoch"], "worker": lease["worker"]}
        scoped_client = client.with_token(result["task_token"])

        async def release():
            await scoped_client.release(fence, "done")
        return AHPCheckpointSaver(scoped_client, thread["id"], fence), thread["id"], release

    result = await client.request_state("POST", API + "/sessions:acquire", body={
        "session_id": unique, "harness_type": "langgraph", "format_version": 1, "ttl_ms": 120000,
    })
    fence = {"epoch": result["epoch"]}

    async def release():
        await client.request_state("POST", f"{API}/sessions/{unique}:heartbeat",
                                   body={"lease_epoch": fence["epoch"], "release": True})

    return AHPJournalCheckpointSaver(client, unique, fence), unique, release


async def seed_empty(client, mode, count):
    ids = iter(range(count))
    seed = uuid4().hex

    async def worker():
        for index in ids:
            key = f"{seed}-{index}"
            if mode == "legacy":
                await client.create_thread(key, key)
            else:
                await client.request_state("POST", API + "/sessions:acquire", body={
                    "session_id": key, "harness_type": "langgraph", "format_version": 1,
                })

    await asyncio.gather(*(worker() for _ in range(10)))


def graph_for(saver):
    graph = StateGraph(GraphState)
    graph.add_node("a", lambda state: {"steps": ["A"]})
    graph.add_node("b", lambda state: {"steps": ["B"]})
    graph.add_node("join", lambda state: {"steps": ["joined"]})
    graph.add_edge(START, "a")
    graph.add_edge(START, "b")
    graph.add_edge(["a", "b"], "join")
    graph.add_edge("join", END)
    return graph.compile(checkpointer=saver)


def stats(values):
    p = quantiles(values, n=100, method="inclusive") if len(values) > 1 else values * 99
    return {"n": len(values), "p50_ms": p[49], "p95_ms": p[94], "p99_ms": p[98]}


async def run(client, args):
    await seed_empty(client, args.mode, args.seed)
    rpc = defaultdict(list)
    total = []
    go = asyncio.Event()
    active = peak = 0
    before = after = None
    release_callbacks = []
    ready = [asyncio.Event() for _ in range(args.concurrency)]
    original = StateAPIClient._request

    async def timed(self, method, path, token, body, params=None):
        start = time.perf_counter_ns()
        try:
            return await original(self, method, path, token, body, params)
        finally:
            if go.is_set():
                operation = path.rsplit("/", 1)[-1]
                rpc[f"{method} {operation}"].append((time.perf_counter_ns() - start) / 1e6)
    StateAPIClient._request = timed

    async def worker(index):
        nonlocal active, peak
        saver, session, release = await acquire(client, args.mode)
        release_callbacks.append(release)
        graph = graph_for(saver)
        config = {"configurable": {"thread_id": session}}

        async def turn():
            value = await graph.ainvoke({"steps": ["input"]}, config, durability="sync")
            assert value["steps"][-4:] == ["input", "A", "B", "joined"], value

        for _ in range(args.history):
            await turn()
        ready[index].set()
        await go.wait()
        for _ in range(100 // args.concurrency):
            started = time.perf_counter_ns()
            active += 1
            peak = max(peak, active)
            try:
                await turn()
            finally:
                active -= 1
            total.append((time.perf_counter_ns() - started) / 1e6)

    async def start_after_history():
        nonlocal before
        await asyncio.gather(*(handle.wait() for handle in ready))
        if args.sql_target_db:
            before = await sql_snapshot(args)
        go.set()

    try:
        async with asyncio.TaskGroup() as workers:
            workers.create_task(start_after_history())
            for i in range(args.concurrency):
                workers.create_task(worker(i))
        if args.sql_target_db:
            after = await sql_snapshot(args)
    finally:
        StateAPIClient._request = original
        await asyncio.gather(*(release() for release in release_callbacks))
    assert len(total) == 100 and peak == args.concurrency
    result = {
        "mode": args.mode, "concurrency": args.concurrency, "peak_graph_calls": peak,
        "seed": args.seed, "history_turns_per_worker": args.history,
        "graph": stats(total), "graph_raw_ms": total, "rpc_raw_ms": rpc,
        "rpc_per_graph": sum(map(len, rpc.values())) / len(total),
        "rpc": {name: stats(values) for name, values in sorted(rpc.items())},
    }
    if before is not None:
        result["postgres"] = sql_delta(before, after, len(total))
    print(json.dumps(result, indent=2))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("legacy", "compact"))
    parser.add_argument("concurrency", type=int, choices=(1, 10))
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--history", type=int, default=25)
    parser.add_argument("--sql-target-db", help="Read pg_stat_statements for this dedicated benchmark database")
    parser.add_argument("--sql-monitor-db", default="postgres", help="Database with the pg_stat_statements view installed")
    parser.add_argument("--psql", default="psql", help="psql executable, connection via standard PG environment")
    args = parser.parse_args()
    if args.sql_target_db == args.sql_monitor_db:
        parser.error("SQL monitoring database must differ from the benchmark database")
    hand = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    async with mtls_client(hand) as client:
        await run(client, args)


if __name__ == "__main__":
    asyncio.run(main())
