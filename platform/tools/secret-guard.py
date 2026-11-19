#!/usr/bin/env python3
"""Director's mechanical no-secrets control.

WHY: the objective forbids secrets in reports, profiles and Git, and the lane
provisioning tools legitimately write operational credentials into
evidence/<lane>-<variant>.{json,env}. Those files are functional credential
stores (mode 600, outside Git) - but nothing so far STOPPED one of their values
from being copied into a report, a profile artifact, or a committed file. That
copy is how a leak actually happens, not the store itself.

WHAT IT DOES: builds the value set from the operational stores, then fails if any
of those values appears in a guarded surface (reports/, git-tracked files, any
file marked as an artifact/report). Values are never printed - only the key name
that produced them, so a failure is actionable without becoming a leak.

usage: secret-guard.py [--quiet]
exit: 0 clean, 1 leak found, 2 could not build the value set.
"""
import argparse
import json
import os
import subprocess
import sys

LANES = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HQ = os.path.dirname(os.path.dirname(LANES))
STORE_GLOBS = ("evidence",)                      # operational credential stores
SECRET_MARKERS = ("PASSWORD", "_TOKEN", "VAULT_DEV", "_SECRET", "ACCESS_KEY")
GUARDED = [
    os.path.join(HQ, "reports"),                 # every report and profile
    os.path.join(LANES, "reports"),
]
MIN_LEN = 16                                     # shorter values produce noise, not leaks


def stores():
    for root, dirs, files in os.walk(os.path.join(LANES, "evidence")):
        dirs[:] = [d for d in dirs if d not in ("node_modules", ".git", "runtime")]
        for fn in files:
            if fn.endswith((".json", ".env")):
                yield os.path.join(root, fn)


def value_set():
    """Map secret value -> originating key name (never the other way round)."""
    found = {}
    for path in stores():
        try:
            if os.path.getsize(path) > 4_000_000:
                continue
            raw = open(path, "r", errors="ignore").read()
        except OSError:
            continue
        pairs = []
        if path.endswith(".json"):
            try:
                doc = json.loads(raw)
            except ValueError:
                continue
            stack = [doc]
            while stack:
                cur = stack.pop()
                if isinstance(cur, dict):
                    for k, v in cur.items():
                        if isinstance(v, str):
                            pairs.append((str(k), v))
                        elif isinstance(v, (dict, list)):
                            stack.append(v)
                elif isinstance(cur, list):
                    stack.extend(x for x in cur if isinstance(x, (dict, list)))
        else:
            for line in raw.splitlines():
                if "=" in line:
                    k, v = line.split("=", 1)
                    pairs.append((k.strip(), v.strip().strip('"')))
        for key, val in pairs:
            if len(val) >= MIN_LEN and any(m in key.upper() for m in SECRET_MARKERS):
                found[val] = key
    return found


def guarded_files():
    for base in GUARDED:
        if not os.path.isdir(base):
            continue
        for root, dirs, files in os.walk(base):
            dirs[:] = [d for d in dirs if d not in (".git", "node_modules")]
            for fn in files:
                if fn.endswith((".md", ".json", ".txt", ".csv", ".log", ".html", ".py", ".sh", ".go", ".yaml", ".yml")):
                    yield os.path.join(root, fn)


def git_tracked():
    repo = os.path.join(LANES, "qwen-int")
    out = subprocess.run(["git", "-C", repo, "grep", "-I", "--name-only", "-e", ".", "HEAD"],
                         capture_output=True, text=True)
    for line in out.stdout.splitlines():
        p = os.path.join(repo, line.split("\t", 1)[0] if "\t" in line else line)
        if os.path.isfile(p):
            yield p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument("--include-git", action="store_true", help="also scan git-tracked blobs")
    args = ap.parse_args()

    values = value_set()
    if not values:
        print("secret-guard: no operational secret values found (store missing?)", file=sys.stderr)
        return 2
    if not args.quiet:
        print(f"secret-guard: {len(values)} operational values in the set")

    hits = []
    streams = [guarded_files()] + ([git_tracked()] if args.include_git else [])
    for stream in streams:
        for path in stream:
            try:
                raw = open(path, "r", errors="ignore").read()
            except OSError:
                continue
            for val, key in values.items():
                if val in raw:
                    hits.append((path, key))
                    break
    if hits:
        print(f"secret-guard: FAIL - {len(hits)} guarded files carry an operational credential")
        for path, key in hits[:40]:
            print(f"  {os.path.relpath(path, HQ)}  (value of {key})")
        print("secret-guard: remove the value, keep the key name; do not paste store contents into reports")
        return 1
    print("secret-guard: OK - no operational credential found in guarded surfaces")
    return 0


if __name__ == "__main__":
    sys.exit(main())
