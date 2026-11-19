"""Run the native ADK tool barrier with no Tool Gateway process."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import sys
import time
from uuid import uuid4

from google.adk.tools.function_tool import FunctionTool
from google.genai import types

from ahp_state.journal_adk import ADKToolOutcomeUnknown, AHPJournalSessionService
from ahp_state.transport import mtls_client
from adk_gateway_probe import APP, USER, FixtureModel, acquire, current_intent, key_for, runner_for

LEASE_MS = 5000


async def source(handshake, session, directory):
    async with mtls_client(handshake) as client:
        lease = await acquire(client, session, LEASE_MS)

        async def write(item: int) -> dict:
            """Apply an effect to the local test ledger."""
            with (directory / "effect.jsonl").open("a") as ledger:
                print(json.dumps({"item": item, "pid": os.getpid()}), file=ledger, flush=True)
                os.fsync(ledger.fileno())
            (directory / "effect-durable").touch()
            await asyncio.Event().wait()
            raise AssertionError("the controller should have killed the worker")

        tool = FunctionTool(write)
        journal = AHPJournalSessionService(client, app_name=APP, user_id=USER, session_id=session,
                                           lease_epoch=lease["epoch"], effect_tools=(tool,))
        await journal.create_session(app_name=APP, user_id=USER)
        async with runner_for(journal, FixtureModel(), journal.before_tool_callback, tools=[tool]) as runner:
            async for _ in runner.run_async(
                user_id=USER, session_id=session,
                new_message=types.Content(role="user", parts=[types.Part(text="apply the fixture effect")]),
            ):
                pass
        raise AssertionError("native ADK turn ended before the crash boundary")


async def resume(handshake, session, directory):
    async with mtls_client(handshake) as client:
        lease = await acquire(client, session, LEASE_MS)
        assert lease["epoch"] == 2
        document = lease["unresolved_intents"]
        assert len(document) == 1
        assert document[0]["status"] == "DISPATCHED"

        def write(item: int) -> dict:
            """Reject all accidental repeat effects."""
            raise AssertionError("the replacement attempted the external effect again")

        tool = FunctionTool(write)
        journal = AHPJournalSessionService(client, app_name=APP, user_id=USER, session_id=session,
                                           lease_epoch=lease["epoch"], effect_tools=(tool,))
        restored = await journal.get_session(app_name=APP, user_id=USER, session_id=session)
        event = next(event for event in restored.events if event.get_function_calls())
        call, = event.get_function_calls()
        key = key_for(session, event.invocation_id, call.id)
        assert document[0]["call_id"] == key
        try:
            await journal.recover_tool_calls(event.invocation_id)
        except ADKToolOutcomeUnknown:
            pass
        else:
            raise AssertionError("the replacement proceeded with an unknown outcome")
        result = await current_intent(client, session, key)
        assert result["status"] == "OUTCOME_UNKNOWN"
        effects = (directory / "effect.jsonl").read_text().splitlines()
        assert len(effects) == 1 and json.loads(effects[0])["pid"] != os.getpid()
        print(json.dumps({"epoch": 2, "status": "OUTCOME_UNKNOWN", "effects": 1, "held_before_model": True}))


async def controller(directory, session):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / "source.log").open("wb") as log:
        child = await asyncio.create_subprocess_exec(sys.executable, __file__, str(directory), "source", session,
                                                     stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 25
            while not (directory / "effect-durable").exists():
                if child.returncode is not None or time.monotonic() >= deadline:
                    raise RuntimeError((directory / "source.log").read_text())
                await asyncio.sleep(0.05)
        finally:
            if child.returncode is None:
                child.kill()
            await child.wait()
    assert child.returncode == -signal.SIGKILL
    await asyncio.sleep(LEASE_MS / 1000 + 0.1)
    other = await asyncio.create_subprocess_exec(sys.executable, __file__, str(directory), "resume", session)
    assert await asyncio.wait_for(other.wait(), timeout=30) == 0
    print(json.dumps({"source_signal": "SIGKILL", "replacement_process": True}))


async def main():
    directory = Path(sys.argv[1])
    if len(sys.argv) == 2:
        await controller(directory, "adk-standalone-" + uuid4().hex)
        return
    handshake = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    mode, session = sys.argv[2:]
    await (source if mode == "source" else resume)(handshake, session, directory)


if __name__ == "__main__":
    asyncio.run(main())
