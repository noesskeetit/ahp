#!/usr/bin/env python3
"""Create command-specific overlays against the pinned profiling baseline."""

import json
import sys
from pathlib import Path


def rewrite(source, target, replacements):
    text = source.read_text()
    for old, new in replacements:
        count = text.count(old)
        if count != 1:
            raise ValueError(f"{source}: expected one occurrence of {old!r}, found {count}")
        text = text.replace(old, new)
    target.write_text(text)
    return str(target)


def pool_observer(role, name, indentation="\t"):
    old = f"{indentation}defer {name}.Close()"
    return old, f'{indentation}profilePool("{role}", {name})\n{old}'


def prepare(assembled, template, output):
    command_text, separator, runtime_text = template.read_text().partition("// --- VMRUNTIME TEMPLATE ---")
    if not separator or not command_text.strip() or not runtime_text.strip():
        raise ValueError(f"{template}: missing command or vmruntime probe template")
    edits = {
        "state-api": {
            "cmd/state-api/main.go": [
                pool_observer("identity", "identityPool", "\t\t"),
                pool_observer("state", "pool"),
            ],
            "internal/statejournal/events.go": [
                (
                    "\tif err := tx.Commit(ctx); err != nil {\n\t\treturn AppendReceipt{}, err\n\t}\n\treturn receipt, nil",
                    "\tif err := tx.Commit(ctx); err != nil {\n\t\treturn AppendReceipt{}, err\n\t}\n"
                    "\tif request.FinishTurn != \"\" && turn.Mode == journal.ExecutionVMManaged {\n"
                    "\t\tprofileFinishCommitted(session, turn.Seq, receipt.Control)\n\t}\n\treturn receipt, nil",
                ),
            ],
        },
        "run-coordinator": {
            "cmd/run-coordinator/main.go": [pool_observer("notify", "pool")]
        },
        "ahp-vm-runtime": {
            "cmd/ahp-vm-runtime/main.go": [
                pool_observer("runtime", "pool"),
                pool_observer("identity", "identityPool"),
                (
                    "\thandler := vmruntime.Handler",
                    "\tprofileSetBootstrapLookup(vmruntime.ProfileBootstrapNS)\n\thandler := vmruntime.Handler",
                ),
            ],
            "cmd/ahp-vm-runtime/injector_config.go": [
                (
                    "Transport: &http.Transport{TLSClientConfig: config, ForceAttemptHTTP2: true,",
                    "Transport: profileTraceTransport(&http.Transport{TLSClientConfig: config, ForceAttemptHTTP2: true,",
                ),
                (
                    "IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second},",
                    "IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second}),",
                ),
            ],
            "internal/vmruntime/manager.go": [
                (
                    "func (m Manager) EnsureRunning(ctx context.Context, exec Execution) error {",
                    "func (m Manager) EnsureRunning(ctx context.Context, exec Execution) error {\n\tprofileBootStart(exec.ID)",
                ),
            ],
            "internal/vmruntime/injector.go": [
                (
                    'if r.Method == http.MethodGet && path == "/v1/runtime/bootstrap" && len(query) == 0 && r.Body == http.NoBody {',
                    'if r.Method == http.MethodGet && path == "/v1/runtime/bootstrap" && len(query) == 0 && r.Body == http.NoBody {'
                    "\n\t\tprofileBootArrive(exec.ID)",
                ),
            ],
        },
    }
    command_probe = output / "command-profile-init.go"
    command_probe.write_text(command_text)
    runtime_probe = output / "runtime-profile-init.go"
    runtime_probe.write_text(runtime_text.lstrip())

    for role, paths in edits.items():
        mapping = {
            str(assembled / "cmd" / role / "ahp_profile_overlay.go"): str(command_probe),
        }
        if role == "ahp-vm-runtime":
            mapping[str(assembled / "internal" / "vmruntime" / "ahp_profile_probe.go")] = str(runtime_probe)
        if role == "state-api":
            finish = template.with_name("profile-finish.go.in")
            finish_probe = output / "state-profile-finish.go"
            finish_probe.write_text(finish.read_text())
            mapping[str(assembled / "internal" / "statejournal" / "ahp_profile_finish.go")] = str(finish_probe)
        for name, replacements in paths.items():
            target = output / name.replace("/", "_")
            mapping[str(assembled / name)] = rewrite(assembled / name, target, replacements)
        (output / f"{role}.json").write_text(json.dumps({"Replace": mapping}))


if __name__ == "__main__":
    try:
        prepare(*(Path(arg) for arg in sys.argv[1:]))
    except (OSError, ValueError) as error:
        sys.exit(f"Cannot create profiling overlays: {error}")
