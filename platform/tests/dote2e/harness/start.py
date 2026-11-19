#!/usr/bin/env python3
"""Start exactly the prepared dot-e2e instance and wait for readiness.

Only the binaries built from this lane's revision are launched, on this lane's
own ports, with this lane's own database, bucket and certificates.
"""
import argparse
import datetime
import hashlib
import json
import os
import shlex
import socket
import ssl
import subprocess
import time
import urllib.request
from pathlib import Path

os.umask(0o077)
parser = argparse.ArgumentParser()
parser.add_argument("--instance", required=True, type=Path)
parser.add_argument("--bin-dir", required=True, type=Path)
args = parser.parse_args()
args.bin_dir = args.bin_dir.resolve()
instance = args.instance.resolve()
run_tag = datetime.datetime.now(datetime.timezone.utc).strftime("%H%M%S")
scope = json.loads((instance / "scope.json").read_text())
build = json.loads((args.bin_dir.resolve() / "build.json").read_text())
def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


if (instance / "processes.json").exists():
    recorded = json.loads((instance / "processes.json").read_text())
    if any(alive(entry["pid"]) for entry in recorded.values()):
        raise SystemExit("instance still running; stop it first (recorded PIDs preserved)")
    (instance / "processes.json").rename(instance / ("processes.stale-" + run_tag + ".json"))


def env_for(role):
    values = {k: v for k, v in os.environ.items() if k in ("PATH", "HOME", "TMPDIR", "LANG", "LC_ALL")}
    values["NO_PROXY"] = "127.0.0.1,localhost,::1"
    for line in (instance / (role + ".env")).read_text().splitlines():
        if line and not line.startswith("#"):
            key, raw = line.split("=", 1)
            fields = shlex.split(raw)
            values[key] = fields[0] if fields else ""
    return values


# Human operator and the foreign stranger are provisioned before the plane starts.
bootstrap_env = env_for("backend")
bootstrap_env["AHP_PROVISION_USERS_JSON"] = (instance / "users.json").read_text()
with (instance / "logs/bootstrap.log").open("ab") as log:
    result = subprocess.run([str(args.bin_dir / "ahp"), "--role", "provision-browser-users"],
                            env=bootstrap_env, stdout=log, stderr=subprocess.STDOUT)
if result.returncode:
    raise SystemExit("operator provisioning failed; private bootstrap log retained")

ports = {"notes": 29741, "backend": 29720, "agent": 29731, "runner": 29750}
binaries = {"notes": "ahp-note-mcp", "backend": "ahp", "agent": "ahp-agent", "runner": "as-runner"}
processes = {}
# The Space runner is started last: it probes the planes it depends on at startup.
for role in ("notes", "backend", "agent", "runner"):
    listen = (env_for(role).get("AHP_LISTEN") or env_for(role).get("AS_LISTEN")
              or env_for(role).get("AHP_NOTE_MCP_LISTEN") or "127.0.0.1:%d" % ports[role])
    port = int(listen.split(":")[1])
    ports[role] = port
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            raise SystemExit("port %d occupied; refusing duplicate launcher" % port)
    digest = hashlib.sha256((args.bin_dir / binaries[role]).read_bytes()).hexdigest()
    if digest != build["binaries"][binaries[role]]:
        raise SystemExit("binary digest mismatch for " + binaries[role])
    with (instance / "logs" / (role + ".log")).open("ab") as log:
        process = subprocess.Popen([str(args.bin_dir / binaries[role])], cwd=instance, env=env_for(role),
                                   stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)
    if role == "agent":
        # The native agent identifies itself by its own immutable binary digest,
        # which the platform then pins to the task (PDF rev3.2 L946).
        values = env_for(role)
        values["AHP_AGENT_DIGEST"] = "sha256:" + digest
        values["AHP_AGENT_IMAGE"] = "native:sha256:" + digest
        (instance / "agent.env").write_text("".join("%s=%s\n" % (k, shlex.quote(str(v)))
                                                    for k, v in values.items()))
        (instance / "agent.env").chmod(0o600)
        process.terminate()
        process.wait(timeout=20)
        with (instance / "logs" / "agent.log").open("ab") as log:
            process = subprocess.Popen([str(args.bin_dir / binaries[role])], cwd=instance, env=env_for(role),
                                       stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                                       start_new_session=True)
    processes[role] = {"pid": process.pid, "binary": binaries[role], "sha256": digest, "port": port}
    (instance / "processes.json").write_text(json.dumps(processes, indent=2) + "\n")
    context = ssl.create_default_context(cafile=instance / "certs/ca.crt")
    context.load_cert_chain(instance / "certs/operator.crt", instance / "certs/operator.key")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=context))
    deadline = time.monotonic() + 90
    while True:
        if process.poll() is not None:
            raise SystemExit(role + " exited; inspect private logs (started processes preserved)")
        try:
            scheme = "http" if role in ("notes", "runner") else "https"
            path = "/readyz" if role == "agent" else "/healthz"
            with opener.open("%s://127.0.0.1:%d%s" % (scheme, port, path), timeout=2) as response:
                if response.status != 200:
                    raise RuntimeError("not ready")
                break
        except SystemExit:
            raise
        except Exception:
            if time.monotonic() >= deadline:
                raise SystemExit(role + " did not become ready; inspect private logs")
            time.sleep(0.5)
scope["status"] = "STARTED_NOT_ACCEPTED"
(instance / "scope.json").write_text(json.dumps(scope, indent=2) + "\n")
print(json.dumps({"instance": str(instance), "origin": scope["origin"], "status": "STARTED_NOT_ACCEPTED",
                  "pids": {role: processes[role]["pid"] for role in processes}}))
