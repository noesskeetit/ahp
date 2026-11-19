#!/usr/bin/env python3
# Consolidated human-readable summary over the newest evidence file per phase.
# Reads only the JSON reports in the evidence directory (default ../evidence
# relative to the worktree). No secrets are present in these files; the script
# prints aggregates only.
import json, glob, os, sys

EV = sys.argv[1] if len(sys.argv) > 1 else "../evidence"
MB = 1024 * 1024

def newest(kind):
    fs = glob.glob(os.path.join(EV, f"profiling-{kind}-*.json"))
    return max(fs, key=os.path.getmtime) if fs else None

def load(kind):
    f = newest(kind)
    return (f, json.load(open(f))) if f else (None, None)

def f_or(v, w, prec):
    # None means never measured: print the marker, never a -1 or 0 that reads
    # as a number (DEF-PROF-1 follow-up).
    return f"{v:>{w}.{prec}f}" if v is not None else f"{'n/a':>{w}}"

def f1(v):
    return f"{v:.1f}" if v is not None else "n/a"

def head(d):
    m, v = d["machine"], d["version"]
    print(f"machine: {m['hostname']} {m['os_version']} ({m['arch']}) "
          f"{m['physical_cores']}/{m['logical_cores']} cores, "
          f"{round(m['memory_bytes']/1024**3,1)} GB RAM")
    print(f"binary : rev {v['source_revision'][:8]} dirty={v['source_dirty']} sha256 {v['binary_sha256'][:12]}")

print("=== OVERHEAD (deterministic platform overhead) ===")
f, d = load("overhead")
if d:
    print("file:", os.path.basename(f)); head(d)
    for ph in d["phases"]:
        print(f"\n[{ph['name']}]")
        print(f"  {'conc':>4} {'rps':>9} {'p50ms':>8} {'p95ms':>9} {'p99ms':>9} {'err%':>5} {'cpuS':>7} {'gorut':>6} {'rssMB':>7}")
        for s in ph["steps"]:
            sm = s.get("samplers") or {}
            cpu = sm.get("proc_cpu_sec_delta"); gor = sm.get("goroutines_end"); rss = sm.get("rss_end_bytes")
            rss_mb = rss / MB if rss is not None else None
            print(f"  {s['concurrency']:>4} {s['throughput_rps']:>9.1f} {f_or(s.get('p50_ms'), 8, 1)} {f_or(s.get('p95_ms'), 9, 1)} {f_or(s.get('p99_ms'), 9, 1)} {s['error_rate_pct']:>5.2f} "
                  f"{f_or(cpu, 7, 1)} {f_or(gor, 6, 0)} {f_or(rss_mb, 7, 0)}")
    print("\nboundaries:")
    for n in d.get("notes", []):
        if "boundary" in n or "degradation" in n: print("  -", n)

print("\n=== RECOVERY (same instance, no restart) ===")
f, d = load("recovery")
if d:
    print("file:", os.path.basename(f))
    for ph in d.get("phases", []):
        for s in ph["steps"]:
            print(f"  {ph['name']:<10} conc={s['concurrency']:<3} rps={s['throughput_rps']:.1f} p95={f1(s.get('p95_ms'))}ms err%={s['error_rate_pct']:.2f}")
    for n in d.get("notes", []): print("  -", n)

print("\n=== SOAK (leak analysis) ===")
f, d = load("soak")
if d:
    print("file:", os.path.basename(f))
    s = d.get("summary") or {}
    for k in ("rss_slope_bytes_per_min","goroutines_slope_per_min","rss_start_bytes","rss_end_bytes","rss_max_bytes","goroutines_start","goroutines_end","goroutines_max","minio_health_ok_all","pg_deadlocks_delta"):
        if k in s: print(f"  {k} = {'insufficient samples' if s[k] is None else s[k]}")
    for n in d.get("notes", []): print("  -", n)

print("\n=== DEP-FAIL (dependency loss) ===")
f, d = load("depfail")
if d:
    print("file:", os.path.basename(f))
    for ph in d.get("phases", []):
        for s in ph["steps"]:
            print(f"  {ph['name']:<14} conc={s['concurrency']:<3} ok={s['requests']-s['errors']:<6} err%={s['error_rate_pct']:.2f} p95={f1(s.get('p95_ms'))}ms")
    for n in d.get("notes", []): print("  -", n)

print("\n=== LLM (REAL Qwen E2E, tokens only) ===")
f, d = load("llm")
if d:
    print("file:", os.path.basename(f)); head(d)
    for p in d.get("model_usage", {}).get("phases", []):
        u = p.get("usage_tokens_totals") or {}
        # DEF-PROF-1: transport and terminal planes print apart; ttfb None is
        # "not measured", never a 0-ms reading. Old pre-fix reports (single
        # `errors` counter) stay readable.
        t = p.get("ttfb_p95_ms")
        ttxt = "not measured" if t is None else t
        u2 = p.get("usage_tokens_totals")
        utok = "not collected" if not u2 else f"{u2.get('prompt_tokens',0)}/{u2.get('completion_tokens',0)}/{u2.get('total_tokens',0)}"
        mtxt = p.get("model") if p.get("model") is not None else "not collected"
        if "transport_errors" in p:
            counts = (f"transport_err={p.get('transport_errors',0)} done={p.get('terminal_completed')} "
                      f"failed={p.get('terminal_failed')} blocked={p.get('terminal_blocked')} "
                      f"unknown={p.get('terminal_unknown')} logical_err={p.get('logical_errors')} verdict={p.get('verdict')}")
        else:
            counts = f"err={p.get('errors','?')} (pre-DEF-PROF-1 schema)"
        print(f"  {p['phase']:<12} req={p['requests']:<3} {counts} "
              f"tokens(p/c/t)={utok} "
              f"status={p.get('generation_status_counts')} model={mtxt} p95={f1(p.get('p95_ms'))} ttfb_p95={ttxt}")
    acct = d.get("model_usage", {}).get("accounting") or {}
    if acct:
        reasons = ", ".join(acct.get("verdict_reasons", [])) or "none"
        print(f"  RUN VERDICT: {acct.get('verdict', d.get('verdict','?'))} (reasons: {reasons})")
        if acct.get("logical_breakdown"): print(f"  logical_breakdown: {acct['logical_breakdown']}")
    print("  -", d.get("model_usage",{}).get("note","")[:160])
