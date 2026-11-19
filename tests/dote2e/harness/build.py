#!/usr/bin/env python3
"""Build the dot-e2e lane binaries from this worktree and record their digests.

Refuses a dirty tree so every evidence JSON names an exact source revision.

Provenance follows docs/acceptance-run.md §0: the revision of record is the
binary's own vcs.revision read back with `go version -m` (an ldflags stamp is
NOT provenance), and vcs.modified must be false. The per-run bin directory is
frozen (a-w) after build: mutable bin/ is not admissible as evidence.
"""
import argparse
import datetime
import hashlib
import json
import os
import subprocess
from pathlib import Path

os.umask(0o077)
parser = argparse.ArgumentParser()
parser.add_argument("--source", required=True, type=Path)
parser.add_argument("--bin-dir", required=True, type=Path)
args = parser.parse_args()
source = args.source.resolve()
revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
if subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True).strip():
    raise SystemExit("source is dirty; commit lane tests before building")
bin_dir = args.bin_dir.resolve()
if (bin_dir / "build.json").exists():
    raise SystemExit("bin dir already carries a build manifest; per-run dirs are immutable, use a fresh bin-<stamp>")
bin_dir.mkdir(parents=True, exist_ok=True)
when = subprocess.check_output(["git", "show", "-s", "--format=%cI", revision], cwd=source, text=True).strip()
flags = " ".join("-X github.com/ahp-platform/ahp/internal/buildinfo." + k + "=" + v for k, v in
                 [("Revision", revision), ("CommitTime", when), ("Modified", "false")])
build_env = dict(os.environ, CGO_ENABLED="0")
digests = {}
vcs = {}
with (bin_dir / "build.log").open("a") as log:
    for name in ("ahp", "ahp-agent", "ahp-note-mcp", "as-runner"):
        subprocess.run(["go", "build", "-tags", "ahp_exec,ahp_data", "-ldflags", flags,
                        "-o", str(bin_dir / name), "./cmd/" + name], cwd=source, stdout=log,
                       stderr=subprocess.STDOUT, check=True, env=build_env)
        binary = bin_dir / name
        digests[name] = hashlib.sha256(binary.read_bytes()).hexdigest()
        # §0: provenance is what the BINARY itself reports, not what the linker
        # was told. Read the stamped settings back with `go version -m`.
        report = subprocess.check_output(["go", "version", "-m", str(binary)], text=True)
        compiler = report.splitlines()[0].split()[-1]
        if not compiler.startswith("go1."):
            raise SystemExit(name + ": missing compiler version in binary build info")
        stamped = {}
        for line in report.splitlines():
            fields = line.split()
            if len(fields) == 2 and fields[0] == "build" and (fields[1].startswith("vcs.") or fields[1].startswith("CGO_ENABLED=")):
                stamped[fields[1].split("=", 1)[0]] = fields[1].split("=", 1)[1]
        got_rev, got_mod = stamped.get("vcs.revision", ""), stamped.get("vcs.modified", "")
        if not got_rev:
            raise SystemExit(name + ": no vcs.revision stamped (go build -buildvcs did not run)")
        if got_rev != revision:
            raise SystemExit(name + ": vcs.revision " + got_rev + " != source HEAD " + revision)
        if got_mod != "false":
            raise SystemExit(name + ": vcs.modified=" + got_mod + "; §0 accepts only clean trees")
        if stamped.get("CGO_ENABLED") != "0":
            raise SystemExit(name + ": portable workload requires CGO_ENABLED=0")
        vcs[name] = {"revision": got_rev, "modified": got_mod, "cgo_enabled": "0", "go_version": compiler}
(bin_dir / "build.json").write_text(json.dumps({
    "revision": revision,
    "source_tree": subprocess.check_output(["git", "rev-parse", revision + "^{tree}"], cwd=source, text=True).strip(),
    "source_worktree": str(source),
    "binary_dir": str(bin_dir),
    "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "tags": ["ahp_exec", "ahp_data"], "binaries": digests,
    "vcs": vcs, "vcs_modified": all(entry["modified"] == "false" for entry in vcs.values())},
    indent=2) + "\n")
# Freeze the per-run directory: binaries execute (a-x kept for group/other off,
# owner r-x), metadata reads, nothing writes. A rerun must choose a new stamp.
for name in digests:
    (bin_dir / name).chmod(0o555)
for meta in ("build.json", "build.log"):
    if (bin_dir / meta).exists():
        (bin_dir / meta).chmod(0o444)
bin_dir.chmod(0o555)
print(json.dumps({"revision": revision, "vcs_modified": True,
                  "binaries": {k: v[:12] for k, v in digests.items()}}))
