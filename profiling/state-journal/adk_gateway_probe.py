"""A native ADK invocation crossing the durable tool dispatch barrier."""

import asyncio
from base64 import b64decode
import hmac
import json
import os
from pathlib import Path
import signal
import sys
import time
from uuid import uuid4

from google.adk.agents import LlmAgent
from google.adk.apps.app import App, ResumabilityConfig
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.adk.runners import Runner
from google.adk.tools.function_tool import FunctionTool
from google.genai import types

from ahp_state.client import StateAPIError
from ahp_state.journal_adk import ADKToolOutcomeUnknown, AHPJournalSessionService
from ahp_state.transport import mtls_client

APP = "test_adk_gateway"
USER = "fixture-user"
API = "/v1/state"
ITEM = int(os.environ.get("AHP_JOURNAL_ADK_FIXTURE_ITEM", "17"))


def key_for(session, invocation, call_id):
    data = json.dumps([invocation, call_id], separators=(",", ":"), ensure_ascii=False)
    return "adk:tool:" + hmac.digest(session.encode(), data.encode(), "sha256").hex()


class FixtureModel(BaseLlm):
    model: str = "gemini-2.5-flash"
    resumed: bool = False
    model_calls: int = 0

    async def generate_content_async(self, llm_request, stream=False):
        self.model_calls += 1
        assert self.model_calls <= (1 if self.resumed else 2)
        if self.resumed or self.model_calls == 2:
            responses = [part.function_response for content in llm_request.contents
                         for part in content.parts if part.function_response is not None]
            assert len(responses) == 1 and responses[0].name == "write"
            assert responses[0].response["content"] == [{"type": "text", "text": "written"}]
            part = types.Part(text="DONE")
        else:
            part = types.Part(function_call=types.FunctionCall(id="write-1", name="write", args={"item": ITEM}))
        yield LlmResponse(content=types.Content(role="model", parts=[part]))


def write(item: int) -> dict:
    """Write the provided fixture item."""
    raise AssertionError("ADK bypassed the Tool Gateway callback")


def runner_for(journal, model, callback, *, tools=None):
    agent = LlmAgent(name="fixture_agent", model=model, tools=tools if tools is not None else [write],
                     before_tool_callback=callback)
    return Runner(app=App(name=APP, root_agent=agent, resumability_config=ResumabilityConfig(is_resumable=True)),
                  session_service=journal)


async def acquire(client, session, ttl_ms):
    return await client.request_state("POST", API + "/sessions:acquire", body={
        "session_id": session, "ttl_ms": ttl_ms, "harness_type": "adk", "format_version": 1,
    })


async def current_intent(client, session, key):
    return await client.request_state("GET", API + "/aux:mutate", params={
        "session_id": session, "call_id": key,
    })


async def source(handshake, directory, scenario, session):
    async with mtls_client(handshake) as client:
        lease = await acquire(client, session, 5000)

        async def park(invocation, key):
            marker = directory / "before-native-result.tmp"
            marker.write_text(json.dumps([invocation, key]))
            marker.replace(directory / "before-native-result")
            await asyncio.Event().wait()

        active_invocation = None
        gateway_calls = []

        async def drop_before_native_result(operation, body):
            gateway_calls.append(operation)
            if scenario in {"live", "live-unknown"}:
                return await client.request_tool(operation, body)
            assert operation == "Call"
            if scenario == "unsent":
                await park(active_invocation, body["key"])
            try:
                result = await client.request_tool(operation, body)
            except StateAPIError as error:
                assert scenario == "known" and error.code == "TRANSPORT"
            else:
                assert scenario == "unknown" and result["status"] == "SIDE_EFFECT_UNKNOWN"
            await park(active_invocation, body["key"])

        managed_tool = FunctionTool(write)
        journal = AHPJournalSessionService(client, app_name=APP, user_id=USER, session_id=session,
                                           lease_epoch=lease["epoch"], effect_tools=(managed_tool,),
                                           tool_gateway=drop_before_native_result)
        await journal.create_session(app_name=APP, user_id=USER, state={"user:count": 1})

        async def verify_barrier(tool, args, tool_context):
            nonlocal active_invocation
            snapshot = await journal.get_session(app_name=APP, user_id=USER, session_id=session)
            event = next(e for e in snapshot.events if e.get_function_calls())
            assert event.invocation_id == tool_context.invocation_id
            call = event.get_function_calls()[0]
            assert (call.id, call.name, call.args) == (tool_context.function_call_id, tool.name, args)
            key = key_for(session, event.invocation_id, call.id)
            intent = await current_intent(client, session, key)
            assert intent["status"] == "PREPARED"
            active_invocation = event.invocation_id
            if scenario == "prepared":
                await park(active_invocation, key)
            return await journal.before_tool_callback(tool, args, tool_context)

        model = FixtureModel()
        try:
            async with runner_for(journal, model, verify_barrier, tools=[managed_tool]) as runner:
                events = [event async for event in runner.run_async(user_id=USER, session_id=session,
                    new_message=types.Content(role="user", parts=[types.Part(text="write item")]))]
        except ADKToolOutcomeUnknown:
            assert scenario == "live-unknown"
        else:
            assert scenario == "live"
            assert any(part.text == "DONE" for event in events if event.content for part in event.content.parts)
        if scenario in {"live", "live-unknown"}:
            snapshot = await journal.get_session(app_name=APP, user_id=USER, session_id=session)
            responses = [response for event in snapshot.events if event.invocation_id == active_invocation
                         for response in event.get_function_responses() if response.name == "write"]
            key = key_for(session, active_invocation, "write-1")
            intent = await current_intent(client, session, key)
            assert len(responses) == 1 and responses[0].id == "write-1"
            if scenario == "live":
                assert model.model_calls == 2 and gateway_calls == ["Call", "GetResult"]
                assert intent["status"] == "COMPLETED"
                assert responses[0].response == json.loads(b64decode(intent["result"]["inline"]))
            else:
                assert model.model_calls == 1 and gateway_calls == ["Call", "GetResult", "Get"]
                assert intent["status"] == "OUTCOME_UNKNOWN" and "result" not in intent
                assert responses[0].response == {"code": "TOOL_OUTCOME_UNKNOWN"}
            return {"scenario": scenario, "epoch": 1, "gateway_calls": gateway_calls,
                    "result_events": len(responses), "model_requests": model.model_calls, "status": intent["status"]}
    raise AssertionError("the original invocation unexpectedly returned")


async def run(handshake, directory, scenario):
    session = "adk-gateway-" + uuid4().hex
    if scenario in {"live", "live-unknown"}:
        return await source(handshake, directory, scenario, session)
    marker = directory / "before-native-result"
    with (directory / "adk-source.log").open("wb") as log:
        process = await asyncio.create_subprocess_exec(sys.executable, __file__, str(directory), scenario, session,
                                                       stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 40
            while not marker.is_file():
                if process.returncode is not None or time.monotonic() >= deadline:
                    raise RuntimeError((directory / "adk-source.log").read_text() or "ADK failpoint timed out")
                await asyncio.sleep(.05)
        finally:
            if process.returncode is None:
                process.kill()
            await process.wait()
    assert process.returncode == -signal.SIGKILL
    invocation, key = json.loads(marker.read_text())
    async with mtls_client({**handshake, "task_token": handshake["replacement_token"]}) as client:
        deadline = time.monotonic() + 10
        while True:
            try:
                lease = await acquire(client, session, 30000)
                break
            except StateAPIError as error:
                if error.code != "CONFLICT" or time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(.1)
        assert lease["epoch"] == 2 and lease["worker"] == "adk-replacement"
        async with mtls_client(handshake) as original:
            try:
                await original.request_state("POST", f"{API}/sessions/{session}:heartbeat",
                                             body={"lease_epoch": 1, "ttl_ms": 1000})
            except StateAPIError as error:
                assert error.code == "STALE_EPOCH"
            else:
                raise AssertionError("ADK initial lease is still active")

        gateway_calls = []

        async def gateway(operation, body):
            assert body["key"] == key
            gateway_calls.append(operation)
            return await client.request_tool(operation, body)

        managed_tool = FunctionTool(write)
        journal = AHPJournalSessionService(client, app_name=APP, user_id=USER, session_id=session,
                                           lease_epoch=lease["epoch"], effect_tools=(managed_tool,), tool_gateway=gateway)
        before = await current_intent(client, session, key)
        assert before["status"] == ("PREPARED" if scenario == "prepared" else "DISPATCHED")
        model = FixtureModel(resumed=True)
        try:
            await journal.recover_tool_calls(invocation)
        except ADKToolOutcomeUnknown:
            assert scenario == "unknown"
        else:
            assert scenario != "unknown"
            async with runner_for(journal, model, journal.before_tool_callback, tools=[managed_tool]) as runner:
                events = [event async for event in runner.run_async(user_id=USER, session_id=session,
                                                                     invocation_id=invocation)]
            assert model.model_calls == 1
            assert any(part.text == "DONE" for event in events if event.content for part in event.content.parts)

        snapshot = await journal.get_session(app_name=APP, user_id=USER, session_id=session)
        assert snapshot.state["user:count"] == 1
        results = [part for event in snapshot.events if event.invocation_id == invocation
                   for part in event.get_function_responses() if part.name == "write"]
        assert len(results) == 1 and results[0].id == "write-1"
        intent = await current_intent(client, session, key)
        expected_operations = {"prepared": ["GetResult", "Call", "GetResult"],
                               "unsent": ["GetResult", "Call", "GetResult"], "known": ["GetResult"],
                               "unknown": ["GetResult", "Get"]}[scenario]
        assert gateway_calls == expected_operations
        if scenario == "unknown":
            assert model.model_calls == 0 and intent["status"] == "OUTCOME_UNKNOWN"
            assert "result" not in intent and results[0].response == {"code": "TOOL_OUTCOME_UNKNOWN"}
        else:
            assert intent["status"] == "COMPLETED"
            receipt = await client.request_tool("GetResult", {"key": key})
            assert results[0].response == json.loads(b64decode(intent["result"]["inline"])) == receipt["payload"]
        try:
            await journal.recover_tool_calls(invocation)
        except ADKToolOutcomeUnknown:
            assert scenario == "unknown"
        else:
            assert scenario != "unknown"
        assert gateway_calls == expected_operations
        return {"scenario": scenario, "source_signal": "SIGKILL", "epoch": lease["epoch"],
                "stale_epoch_rejected": True, "gateway_calls": gateway_calls, "result_events": len(results),
                "model_requests": model.model_calls, "status": intent["status"], "repeat_recovery_no_calls": True}


async def main():
    directory, scenario, *source_session = sys.argv[1:]
    handshake = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    if source_session:
        await source(handshake, Path(directory), scenario, source_session[0])
    else:
        print(json.dumps(await run(handshake, Path(directory), scenario)))


if __name__ == "__main__":
    asyncio.run(main())
