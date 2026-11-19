from collections import defaultdict
from pathlib import Path
from statistics import quantiles
import json
import sys

root = Path(sys.argv[1])
groups = defaultdict(list)
for file in sorted((root / "bench").glob("*.json")):
    run = json.loads(file.read_text())
    groups[run["concurrency"]].append(run)
assert len(groups) == 2 and all(len(runs) == 3 for runs in groups.values())

def stats(samples):
    result = quantiles(samples, n=100, method="inclusive")
    return {"n": len(samples), "p50": round(result[49], 3),
            "p95": round(result[94], 3), "p99": round(result[98], 3)}

summary = {}
for concurrency, runs in sorted(groups.items()):
    all_turns = [t for run in runs for t in run["turn_raw_ms"]]
    all_admissions = [t for run in runs for t in run["admission_raw_ms"]]
    rpc = defaultdict(list)
    for run in runs:
        for operation, samples in run["rpc_raw_ms"].items():
            rpc[operation].extend(samples)
    accepted = sum(r["accepted"] for r in runs)
    completed = sum(r["status_counts"].get("done", 0) for r in runs)
    assert accepted == completed == 300
    peak = max(r["max_claimed"] for r in runs)
    assert peak == concurrency
    summary[concurrency] = {
        "accepted": accepted, "completed": completed, "peak_claimed": peak,
        "under_300ms": sum(t <= 300 for t in all_turns),
        "admission": stats(all_admissions), "whole_turn": stats(all_turns),
        "turns_per_s": round(completed / sum(r["elapsed_s"] for r in runs), 2),
        "http_calls_per_turn": round(sum(map(len, rpc.values())) / accepted, 3),
        "pg_commits_per_turn_observed": round(sum(r["pg_commits_observed"] for r in runs) / accepted, 2),
        "pg_rollbacks_per_turn_observed": round(sum(r["pg_rollbacks_observed"] for r in runs) / accepted, 2),
        "rpc": {operation: stats(samples) for operation, samples in rpc.items()},
    }
print(json.dumps(summary, indent=2))
