#!/usr/bin/env python3
"""Stop only the processes this lane recorded, by PID, without deleting data."""
import argparse
import json
import os
import signal
import time
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--instance", required=True, type=Path)
args = parser.parse_args()
record = args.instance.resolve() / "processes.json"
if not record.exists():
    raise SystemExit("no recorded processes for this instance")
processes = json.loads(record.read_text())
for role in ("runner", "agent", "backend", "notes"):
    entry = processes.get(role)
    if not entry:
        continue
    try:
        os.kill(entry["pid"], signal.SIGTERM)
    except ProcessLookupError:
        continue
def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


for _ in range(40):
    if not any(alive(entry["pid"]) for entry in processes.values()):
        break
    time.sleep(0.25)
print(json.dumps({"stopped": {role: processes[role]["pid"] for role in processes}}))
