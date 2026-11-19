"""Pinned ADK 2.14.0, live compact journal and PostgreSQL."""
import argparse
import asyncio
import json
from importlib.metadata import version
import os
from pathlib import Path
import subprocess
import sys
from uuid import uuid4

from google.adk.agents import BaseAgent
from google.adk.errors import StaleSessionError
from google.adk.events import Event, EventActions
from google.adk.runners import Runner
from google.genai import types

from ahp_state.client import StateValidationError
from ahp_state.journal_adk import AHPJournalSessionService
from ahp_state.transport import mtls_client

APP = "bench-adk"
USER = "bench-user"
API = "/v1/state"


class ProbeAgent(BaseAgent):
    async def _run_async_impl(self, ctx):
        assert ctx.session.state["user:count"] == 1
        yield Event(
            id="runner-final", invocation_id=ctx.invocation_id, author=self.name,
            content=types.Content(role="model", parts=[types.Part(text="done")]),
            actions=EventActions(state_delta={
                "runner_output": "done",
                "user:count": 2,
                "app:last": "runner",
                "temp:probe": "TEMP_ONLY_FIXTURE",
                "secret:probe": "SECRET_ONLY_FIXTURE",
            }),
        )


async def service(client, session, acquire=True):
    epoch = 0
    if acquire:
        response = await client.request_state("POST", API + "/sessions:acquire", body={
            "session_id": session, "harness_type": "adk", "format_version": 1, "ttl_ms": 120000,
        })
        epoch = response["epoch"]
    return AHPJournalSessionService(client, app_name=APP, user_id=USER,
                                    session_id=session, lease_epoch=epoch)


async def load(adapter):
    return await adapter.get_session(app_name=APP, user_id=USER, session_id=adapter.session_id)


async def inspect_cold(client, session):
    adapter = await service(client, session, acquire=False)
    restored = await load(adapter)
    assert restored.state["user:count"] == 2
    assert restored.state["app:last"] == "runner"
    assert restored.state["runner_output"] == "done"
    assert "temp:probe" not in restored.state and "secret:probe" not in restored.state
    assert "partial_fixture" not in [event.id for event in restored.events]
    assert [event.id for event in restored.events].count("runner-final") == 1
    for event in restored.events:
        assert all(not key.startswith(("temp:", "secret:")) for key in event.actions.state_delta)
    return {"events": len(restored.events), "state": restored.state, "pid": os.getpid()}


async def probe(client):
    suffix = uuid4().hex
    adapters = [await service(client, "adk-" + label + "-" + suffix) for label in ("a", "b")]
    first = await adapters[0].create_session(app_name=APP, user_id=USER, state={
        "local": "a", "user:count": 0, "app:last": "initial",
    })
    second = await adapters[1].create_session(app_name=APP, user_id=USER, state={"local": "b"})
    assert second.state["user:count"] == 0 and second.state["app:last"] == "initial"
    competing = [
        Event(id=f"compete-{label}", author="probe", actions=EventActions(state_delta={
            "user:count": 1, "app:last": label, f"winner-{label}": True,
        }))
        for label in ("a", "b")
    ]
    outcomes = await asyncio.gather(
        *(adapter.append_event(session, event)
          for adapter, session, event in zip(adapters, (first, second), competing)),
        return_exceptions=True,
    )
    assert sum(isinstance(item, StaleSessionError) for item in outcomes) == 1, outcomes
    assert sum(isinstance(item, Event) for item in outcomes) == 1, outcomes
    winner = next(index for index, outcome in enumerate(outcomes) if isinstance(outcome, Event))
    loser = 1 - winner
    persisted = [await load(adapter) for adapter in adapters]
    assert all(session.state["user:count"] == 1 for session in persisted)
    assert len(persisted[winner].events) == 1 and not persisted[loser].events
    loser_label = ("a", "b")[loser]
    assert f"winner-{loser_label}" not in persisted[loser].state

    # ADK retries and SDK retries with the same event never duplicate it.
    replayed = await adapters[winner].append_event(persisted[winner], competing[winner])
    assert replayed.id == competing[winner].id
    changed = competing[winner].model_copy(deep=True)
    changed.actions.state_delta["user:count"] = 100
    try:
        await adapters[winner].append_event(persisted[winner], changed)
    except StateValidationError as exc:
        assert exc.code == "KEY_REUSE_MISMATCH"
    else:
        raise AssertionError("ADK event reuse with changed data was accepted")

    adapter = adapters[loser]
    session = persisted[loser]
    partial = Event(id="partial_fixture", author="probe", partial=True,
                    actions=EventActions(state_delta={"user:count": 200}))
    await adapter.append_event(session, partial)
    assert (await load(adapter)).state["user:count"] == 1

    runner = Runner(app_name=APP, agent=ProbeAgent(name="probe_agent"), session_service=adapter)
    async with runner:
        delivered = [item async for item in runner.run_async(
            user_id=USER, session_id=adapter.session_id,
            new_message=types.Content(role="user", parts=[types.Part(text="go")]),
        )]
    assert [event.id for event in delivered].count("runner-final") == 1
    user = await adapter.get_user_state(app_name=APP, user_id=USER)
    assert user == {"count": 2}, user
    assert (await load(adapters[winner])).state["app:last"] == "runner"

    await adapters[winner].delete_session(app_name=APP, user_id=USER,
                                          session_id=adapters[winner].session_id)
    assert await load(adapters[winner]) is None
    assert await adapters[winner].get_user_state(app_name=APP, user_id=USER) == {"count": 2}

    proc = await asyncio.create_subprocess_exec(
        sys.executable, __file__, "--inspect", adapter.session_id,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    result, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
    if proc.returncode != 0:
        raise RuntimeError(stderr.decode(errors="replace"))
    cold = json.loads(result)
    assert cold["pid"] != os.getpid()
    return {"adk": version("google-adk"), "same_scope_sessions": 2,
            "concurrent_events_committed": 1, "concurrent_stale_events": 1,
            "same_event_replay": True, "partial_filtered": True,
            "runner": "native Runner.run_async", "deleted_session_preserved_shared_state": True,
            "cold_process": cold}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inspect")
    args = parser.parse_args()
    hand = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    async with mtls_client(hand) as client:
        result = await inspect_cold(client, args.inspect) if args.inspect else await probe(client)
    print(json.dumps(result), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
