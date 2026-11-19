"""ADK human approval and the native pre-dispatch confirmation boundary."""

import asyncio
from base64 import b64decode
import json
import os
from pathlib import Path
import signal
import sys
import time
from uuid import uuid4

from google.adk.events import Event
from google.adk.models.llm_response import LlmResponse
from google.adk.tools.function_tool import FunctionTool
from google.adk.tools.long_running_tool import LongRunningFunctionTool
from google.genai import types

from adk_gateway_probe import API, APP, ITEM, USER, FixtureModel, acquire, current_intent, key_for, runner_for, write
from ahp_state.client import StateAPIError, StateValidationError
from ahp_state.journal_adk import AHPJournalSessionService
from ahp_state.transport import mtls_client


async def confirmation_boundary(handshake):
    async with mtls_client(handshake) as client:
        session = "adk-confirmation-" + uuid4().hex
        lease = await acquire(client, session, 30000)
        async def gateway(operation, body):
            raise AssertionError("confirmation-required tool attempted Gateway dispatch before approval")

        for invalid in ("write", FunctionTool(write, require_confirmation=True), LongRunningFunctionTool(write)):
            try:
                AHPJournalSessionService(client, app_name=APP, user_id=USER, session_id=session,
                                         lease_epoch=lease["epoch"], effect_tools=(invalid,), tool_gateway=gateway)
            except StateValidationError as error:
                assert error.code == "INVALID_ARGUMENT"
            else:
                raise AssertionError("confirmation or deferred tool registration was accepted")
        page = await client.request_state("GET", f"{API}/sessions/{session}/events")
        assert page["head_seq"] == 0

        ordinary = FunctionTool(write)
        journal = AHPJournalSessionService(client, app_name=APP, user_id=USER, session_id=session,
                                           lease_epoch=lease["epoch"], effect_tools=(ordinary,), tool_gateway=gateway)
        await journal.create_session(app_name=APP, user_id=USER)
        try:
            await invoke(journal, FixtureModel(), [FunctionTool(write, require_confirmation=True)],
                         new_message=types.Content(role="user", parts=[types.Part(text="try managed tool")]))
        except StateValidationError as error:
            assert error.code == "INVALID_ARGUMENT" and "registered instance" in str(error)
        else:
            raise AssertionError("Runner used another managed tool instance")
        return {"scenario": "confirmation", "rejected_before_dispatch": True, "rejected_registrations": 3,
                "same_name_substitution_rejected": True}


class ApprovalModel(FixtureModel):
    phase: str

    async def generate_content_async(self, llm_request, stream=False):
        self.model_calls += 1
        if self.phase == "pause":
            assert self.model_calls == 1
            part = types.Part(function_call=types.FunctionCall(
                id="approval-1", name="request_approval", args={"item": ITEM},
            ))
        else:
            assert self.phase == "answer" and self.model_calls <= 2
            receipts = {part.function_response.name: part.function_response for entry in llm_request.contents
                        for part in entry.parts if part.function_response is not None}
            approval = receipts["request_approval"]
            assert approval.id == "approval-1" and approval.response == {"approved": True}
            if self.model_calls == 1:
                part = types.Part(function_call=types.FunctionCall(id="write-1", name="write", args={"item": ITEM}))
            else:
                effect = receipts["write"]
                assert effect.response["content"] == [{"type": "text", "text": "written"}]
                part = types.Part(text="DONE")
        yield LlmResponse(content=types.Content(role="model", parts=[part]))


def approval_tool(on_wait):
    def request_approval(item: int) -> None:
        """Wait for a human decision about the item."""
        assert item == ITEM
        on_wait()

    return LongRunningFunctionTool(request_approval)


def build_journal(client, session, lease, gateway):
    tool = FunctionTool(write)
    return AHPJournalSessionService(client, app_name=APP, user_id=USER, session_id=session,
                                    lease_epoch=lease["epoch"], effect_tools=(tool,), tool_gateway=gateway), tool


async def invoke(journal, model, tools, **turn):
    async with runner_for(journal, model, journal.before_tool_callback, tools=tools) as runner:
        return [event async for event in runner.run_async(user_id=USER, session_id=journal.session_id, **turn)]


def approval_answer():
    return types.Content(role="user", parts=[types.Part(function_response=types.FunctionResponse(
        id="approval-1", name="request_approval", response={"approved": True},
    ))])


async def reacquire(client, session, ttl_ms):
    deadline = time.monotonic() + 12
    while True:
        try:
            return await acquire(client, session, ttl_ms)
        except StateAPIError as exc:
            if exc.code != "CONFLICT" or time.monotonic() >= deadline:
                raise
            await asyncio.sleep(.1)


async def forbid_gateway(operation, body):
    raise AssertionError("unexpected Tool Gateway attempt at human answer boundary")


def no_second_wait():
    raise AssertionError("replacement ran LongRunningFunctionTool again")


async def pause_source(handshake, directory, session):
    async with mtls_client(handshake) as client:
        lease = await acquire(client, session, 5000)

        journal, tool = build_journal(client, session, lease, forbid_gateway)
        await journal.create_session(app_name=APP, user_id=USER)
        waits = []
        model = ApprovalModel(phase="pause")
        await invoke(journal, model, [tool, approval_tool(lambda: waits.append("waiting"))],
                     new_message=types.Content(role="user", parts=[types.Part(text="request approval")]))
        assert waits == ["waiting"] and model.model_calls == 1
        stored = await journal.get_session(app_name=APP, user_id=USER, session_id=session)
        waiting = [event for event in stored.events if "approval-1" in (event.long_running_tool_ids or ())]
        assert len(waiting) == 1 and waiting[0].get_function_calls()[0].name == "request_approval"
        assert not any(event.get_function_responses() for event in stored.events)
        marker = directory / "waiting.tmp"
        marker.write_text(json.dumps([waiting[0].invocation_id, waiting[0].id]))
        marker.replace(directory / "waiting")
        await asyncio.Event().wait()


async def answer_source(handshake, directory, session, invocation):
    async with mtls_client({**handshake, "task_token": handshake["replacement_token"]}) as client:
        lease = await reacquire(client, session, 5000)
        assert lease["epoch"] == 2
        request_state = client.request_state

        async def park_after_commit(method, path, **kwargs):
            receipt = await request_state(method, path, **kwargs)
            if method == "POST" and path.endswith("/events:append"):
                for row in kwargs["body"]["events"]:
                    event = Event.model_validate_json(b64decode(row["payload"]["inline"]))
                    if event.author == "user" and any(fr.id == "approval-1" for fr in event.get_function_responses()):
                        assert event.invocation_id == invocation
                        marker = directory / "answer-committed.tmp"
                        marker.write_text(event.id)
                        marker.replace(directory / "answer-committed")
                        await asyncio.Event().wait()
            return receipt

        client.request_state = park_after_commit
        journal, tool = build_journal(client, session, lease, forbid_gateway)
        await invoke(journal, ApprovalModel(phase="wait"), [tool, approval_tool(no_second_wait)],
                     invocation_id=invocation, new_message=approval_answer())
    raise AssertionError("ADK moved past the human answer commit boundary")


async def kill_at(directory, marker_name, *args):
    marker = directory / marker_name
    log_path = directory / (marker_name + ".log")
    with log_path.open("wb") as log:
        child = await asyncio.create_subprocess_exec(sys.executable, __file__, str(directory), *args,
                                                     stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 35
            while not marker.is_file():
                if child.returncode is not None or time.monotonic() >= deadline:
                    raise RuntimeError(log_path.read_text() or "ADK wait boundary timed out")
                await asyncio.sleep(.05)
        finally:
            if child.returncode is None:
                child.kill()
            await child.wait()
    assert child.returncode == -signal.SIGKILL
    return marker.read_text()


async def hitl_after_restart(handshake, directory, *, answer_ack_lost):
    session = "adk-hitl-" + uuid4().hex
    invocation_id, wait_event = json.loads(await kill_at(directory, "waiting", "pause-source", session))
    answer_event = None
    if answer_ack_lost:
        answer_event = await kill_at(directory, "answer-committed", "answer-source", session, invocation_id)
    target_epoch = 3 if answer_ack_lost else 2
    async with mtls_client({**handshake, "task_token": handshake["replacement_token"]}) as client:
        lease = await reacquire(client, session, 30000)
        assert lease["epoch"] == target_epoch and lease["worker"] == "adk-replacement"
        old_handshake = ({**handshake, "task_token": handshake["replacement_token"]} if answer_ack_lost else handshake)
        async with mtls_client(old_handshake) as old:
            try:
                await old.request_state("POST", f"{API}/sessions/{session}:heartbeat",
                                        body={"lease_epoch": target_epoch - 1, "ttl_ms": 1000})
            except StateAPIError as exc:
                assert exc.code == "STALE_EPOCH"
            else:
                raise AssertionError("previous worker still owned the ADK session")

        gateway_calls = []

        async def gateway(operation, body):
            gateway_calls.append(operation)
            return await client.request_tool(operation, body)

        journal, tool = build_journal(client, session, lease, gateway)
        tools = [tool, approval_tool(no_second_wait)]
        await journal.recover_tool_calls(invocation_id)
        if answer_ack_lost:
            before = await journal.get_session(app_name=APP, user_id=USER, session_id=session)
            committed = [event for event in before.events if event.id == answer_event]
            assert len(committed) == 1 and committed[0].get_function_responses()[0].response == {"approved": True}
            assert not any(call.name == "write" for event in before.events for call in event.get_function_calls())
        else:
            pending = ApprovalModel(phase="wait")
            await invoke(journal, pending, tools, invocation_id=invocation_id)
            assert pending.model_calls == 0 and gateway_calls == []

        resumed = ApprovalModel(phase="answer")
        delivered = await invoke(journal, resumed, tools, invocation_id=invocation_id,
                                 new_message=None if answer_ack_lost else approval_answer())
        assert resumed.model_calls == 2 and gateway_calls == ["Call", "GetResult"]
        assert any(part.text == "DONE" for event in delivered if event.content for part in event.content.parts)
        stored = await journal.get_session(app_name=APP, user_id=USER, session_id=session)
        record = [event for event in stored.events if event.id == wait_event]
        assert len(record) == 1 and record[0].invocation_id == invocation_id
        assert record[0].long_running_tool_ids == {"approval-1"}
        responses = [(event.invocation_id, item.id, item.name) for event in stored.events
                     for item in event.get_function_responses()]
        assert responses == [(invocation_id, "approval-1", "request_approval"), (invocation_id, "write-1", "write")]
        status = await current_intent(client, session, key_for(session, invocation_id, "write-1"))
        assert status["status"] == "COMPLETED"
        return {"scenario": "hitl-answer-lost" if answer_ack_lost else "hitl", "source_signal": "SIGKILL",
                "killed_processes": 2 if answer_ack_lost else 1, "epoch": lease["epoch"], "stale_epoch_rejected": True,
                "checked_pause_without_answer": not answer_ack_lost, "model_calls_after_answer": resumed.model_calls,
                "same_invocation": True, "wait_tool_replayed": False, "answer_events": 1,
                "answer_ack_lost": answer_ack_lost, "gateway_calls": gateway_calls}


async def main():
    directory, scenario, *source_session = sys.argv[1:]
    handshake = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    if scenario == "pause-source":
        await pause_source(handshake, Path(directory), source_session[0])
    elif scenario == "answer-source":
        await answer_source(handshake, Path(directory), *source_session)
    elif scenario == "confirmation":
        print(json.dumps(await confirmation_boundary(handshake)))
    else:
        print(json.dumps(await hitl_after_restart(handshake, Path(directory), answer_ack_lost=scenario == "hitl-answer-lost")))


if __name__ == "__main__":
    asyncio.run(main())
