"""F2 crash-window agent: a real AHP LangGraph adapter process driven by the
Go test (tests/sdk/f2_crash_test.go) over real HTTP against a real PostgreSQL.

It is invoked twice, as two separate OS processes, under the SAME (run, node,
call):

  mode=crash    -> run the guarded side effect, which appends to an EXTERNAL
                   witness file and then hard-kills the process (os._exit)
                   BEFORE the intent can be marked succeeded. This leaves the
                   authoritative intent in real PG as "dispatched".
  mode=recover  -> a FRESH process re-enters the SAME call. The adapter must NOT
                   re-run the effect and must surface SideEffectUnknown (exit 2).

The external witness file is the exactly-once proof: the Go test asserts it
contains exactly one effect line across both processes. This is deliberately NOT
an in-memory call counter or a new-thread shortcut.

All configuration is via environment (see the Go test). No credentials/DSN are
read or printed here.
"""

from __future__ import annotations

import asyncio
import os
import sys

# Make the SDK importable when the Go test runs us from an arbitrary CWD.
_sdk_root = os.environ.get("AHP_F2_SDK_ROOT")
if _sdk_root:
    sys.path.insert(0, _sdk_root)

from ahp_state.client import StateAPIClient  # noqa: E402
from ahp_state.intents import IntentRecorder, SideEffectUnknown  # noqa: E402


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _witness() -> str:
    return _env("AHP_F2_WITNESS", "f2_witness.txt")


def _call_id() -> str:
    return _env("AHP_F2_CALL_ID", "f2-call")


async def _guarded_effect(client: StateAPIClient, fence: dict) -> str:
    """Run the guarded side effect: an EXTERNAL file append, then (in crash mode)
    a hard process exit before the intent can be marked succeeded."""
    rec = IntentRecorder(client, fence, node="agent")

    async def effect() -> str:
        witness = _witness()
        with open(witness, "a", encoding="utf-8") as fh:
            fh.write("f2-effect-" + _call_id() + "\n")
        # Crash mode: the external effect has FIRED and been durably appended to
        # an external file. Now hard-kill the process, exactly like a kill -9 /
        # power loss, so the intent is left in real PG as "dispatched" (the
        # succeed write below never runs).
        if os.environ.get("AHP_F2_MODE") == "crash":
            os._exit(137)
        return "effect-fired"

    return await rec.guarded(_call_id(), effect, kind="tool",
                             target="external:witness", args={"witness": _witness()})


async def _amain() -> int:
    base = _env("AHP_F2_BASE")
    if not base:
        print("AHP_F2_BASE not set", file=sys.stderr)
        return 3
    fence = {
        "run_id": _env("AHP_F2_RUN_ID"),
        "worker": _env("AHP_F2_WORKER"),
        "epoch": int(_env("AHP_F2_EPOCH", "0") or 0),
    }
    client = StateAPIClient(base)
    try:
        outcome = await _guarded_effect(client, fence)
    except SideEffectUnknown:
        # The expected recover outcome: the prior process's effect may or may not
        # have landed, so the adapter surfaces ambiguity instead of re-running.
        print("F2-RECOVER side-effect-unknown (no re-run)")
        return 2
    except SystemExit:
        raise
    # Normal completion (never reached in crash mode, which os._exit's first).
    print("F2-NORMAL outcome=" + repr(outcome))
    return 0


def main() -> None:
    try:
        code = asyncio.run(_amain())
    except SideEffectUnknown:
        code = 2
    sys.exit(code)


if __name__ == "__main__":
    main()
