#!/usr/bin/env python3
"""Restart exactly one recorded role of the lane stand.

Used by the recovery scenario: the process dies while a run is in flight and
comes back against the same database, bucket and certificates.
"""
import argparse
import hashlib
import json
import os
import shlex
import signal
import subprocess
import time
from pathlib import Path

os.umask(0o077)
parser = argparse.ArgumentParser()
parser.add_argument("--instance", required=True, type=Path)
parser.add_argument("--role", required=True, choices=("agent", "backend", "notes", "runner"))
parser.add_argument("--bin-dir", required=True, type=Path)
args = parser.parse_args()
instance = args.instance.resolve()
bin_dir = args.bin_dir.resolve()
record = instance / "processes.json"
processes = json.loads(record.read_text())
entry = processes[args.role]
binary = {"backend": "ahp", "agent": "ahp-agent", "notes": "ahp-note-mcp", "runner": "as-runner"}[args.role]
candidate = bin_dir / binary
# Validate everything required for the SAME build before stopping a process.
# An invalid path used to kill the working role and only then fail Popen.
build = json.loads((bin_dir / "build.json").read_text())
digest = hashlib.sha256(candidate.read_bytes()).hexdigest()
if entry["binary"] != binary or digest != entry["sha256"] or digest != build["binaries"][binary]:
    raise SystemExit("restart requires the recorded binary digest")
if build["vcs"][binary]["modified"] != "false":
    raise SystemExit("restart requires a clean build")
values = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL")}
values["NO_PROXY"] = "127.0.0.1,localhost,::1"
for line in (instance / (args.role + ".env")).read_text().splitlines():
    if line and not line.startswith("#"):
        key, raw = line.split("=", 1)
        fields = shlex.split(raw)
        values[key] = fields[0] if fields else ""
try:
    os.kill(entry["pid"], 0)
except ProcessLookupError:
    pass
else:
    if Path("/proc").is_dir():
        actual = Path("/proc") / str(entry["pid"]) / "exe"
        if not actual.exists() or actual.resolve() != candidate.resolve():
            raise SystemExit("refusing an unrecognized or reused process id")
    else:
        name = subprocess.check_output(["ps", "-p", str(entry["pid"]), "-o", "comm="], text=True).strip()
        if Path(name).resolve() != candidate.resolve():
            raise SystemExit("refusing an unrecognized or reused process id")
    os.kill(entry["pid"], signal.SIGKILL)
    time.sleep(.2)
with (instance / "logs" / (args.role + ".log")).open("ab") as log:
    process = subprocess.Popen([str(candidate)], cwd=instance, env=values, stdin=subprocess.DEVNULL,
                               stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
entry["pid"] = process.pid
entry["restarted_at"] = time.time()
record.write_text(json.dumps(processes, indent=2) + "\n")
print(json.dumps({"role": args.role, "pid": process.pid, "sha256": digest}))
