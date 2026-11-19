"""Exercise the ADK journal against the compact HTTP request contract."""

from copy import deepcopy

import pytest

pytest.importorskip("google.adk")

from google.adk.events import Event, EventActions  # noqa: E402
from google.genai import types  # noqa: E402

from ahp_state import AHPJournalSessionService, StateNotFoundError, StateTimeoutError, StateValidationError  # noqa: E402


class ADKTransport:
    def __init__(self):
        self.sessions = {}
        self.calls = []
        self.lose_ack = False

    async def request_state(self, method, path, *, params=None, body=None):
        self.calls.append((method, path, params, body))
        if path == "/v1/state/sessions:acquire":
            values = self.sessions.setdefault(body["session_id"], {
                "epoch": 0, "rows": [], "kv": {}, "catalog": {}, "status": "IDLE",
            })
            values["epoch"] += 1
            values["status"] = "ACTIVE"
            return {"epoch": values["epoch"]}
        if path == "/v1/state/sessions":
            assert params["harness"] == "adk" and params["view"] == "catalog"
            return {"sessions": [{"session_id": key, **values["catalog"]}
                                 for key, values in self.sessions.items()]}
        session_id = path.split("/")[4].removesuffix(":heartbeat")
        if session_id not in self.sessions:
            raise StateNotFoundError("NOT_FOUND", "unknown session")
        values = self.sessions[session_id]
        rows = values["rows"]
        if path.endswith("/events"):
            start = params["after_seq"]
            reply = {"head_seq": len(rows), "lease_epoch": values["epoch"],
                     "events": rows[start:start + params["limit"]]}
            if params.get("state_namespace") == "adk":
                reply["state"] = list(values["kv"].values())
            return deepcopy(reply)
        if path.endswith("/events:append"):
            assert body["lease_epoch"] == values["epoch"]
            assert body["expected_seq"] == len(rows)
            for delta in body["kv"]:
                scope, key = delta["scope_type"], delta["key"]
                version = values["kv"].get((scope, key), {}).get("version", 0)
                assert version == delta["expected_version"]
                values["kv"][scope, key] = {
                    **delta, "version": version + 1, "deleted": delta.get("delete", False),
                }
            row, = body["events"]
            rows.append({**row, "seq": len(rows) + 1})
            values["catalog"] = body["catalog"]
            if self.lose_ack:
                self.lose_ack = False
                raise StateTimeoutError("TIMEOUT", "lost append acknowledgement")
            return {"first_seq": len(rows), "head_seq": len(rows)}
        if path.endswith(":heartbeat") and body.get("release"):
            values["status"] = body["status"]
            return {}
        raise AssertionError((method, path))


async def test_adk_has_one_post_per_append_and_one_catalog_get_for_ten_sessions():
    transport = ADKTransport()
    identity = {"app_name": "my-app", "user_id": "user"}
    journal = AHPJournalSessionService(transport, **identity)
    sessions = [await journal.create_session(**identity, session_id=f"session-{i}") for i in range(10)]
    session = sessions[0]

    for i in range(110):
        transport.calls.clear()
        await journal.append_event(session, Event(
            id=f"message-{i}", author="model", actions=EventActions(state_delta={"count": i, "temp:native": i}),
        ))
        assert [(method, path) for method, path, *_ in transport.calls] == [
            ("POST", "/v1/state/sessions/session-0/events:append"),
        ]
    assert session.state == {"count": 109, "temp:native": 109}
    assert len(session.events) == 110

    transport.calls.clear()
    listed = await journal.list_sessions(**identity)
    assert {item.id for item in listed.sessions} == {item.id for item in sessions}
    assert all(not item.events and not item.state for item in listed.sessions)
    assert [(method, path) for method, path, *_ in transport.calls] == [("GET", "/v1/state/sessions")]

    transport.calls.clear()
    restored = await journal.get_session(**identity, session_id=session.id)
    assert len(restored.events) == 110
    assert restored.state == {"count": 109}
    assert [call[2]["after_seq"] for call in transport.calls] == [111]

    event = Event(id="lost-ack", author="model", actions=EventActions(state_delta={"count": 110}))
    transport.lose_ack = True
    with pytest.raises(StateTimeoutError):
        await journal.append_event(restored, event)
    transport.calls.clear()
    await journal.append_event(restored, event)
    assert len(restored.events) == 111 and restored.state == {"count": 110}
    assert [call[2]["after_seq"] for call in transport.calls] == [111]


async def test_adk_durable_native_pause_then_resumes_under_new_epoch():
    transport = ADKTransport()
    identity = {"app_name": "my-app", "user_id": "user"}
    journal = AHPJournalSessionService(transport, **identity)
    session = await journal.create_session(**identity, session_id="hitl")
    await journal.append_event(session, Event(
        id="ask", invocation_id="turn", author="model", long_running_tool_ids={"approval"},
        content=types.Content(role="model", parts=[types.Part(
            function_call=types.FunctionCall(id="approval", name="approval"),
        )]),
    ))
    await journal.pause_for_input(session_id=session.id, invocation_id="turn")
    assert transport.sessions[session.id]["status"] == "WAITING_INPUT"
    await transport.request_state("POST", "/v1/state/sessions:acquire", body={"session_id": session.id})
    journal.bind_session(session.id, 2)
    restored = await journal.get_session(**identity, session_id=session.id)
    assert restored.events[0].long_running_tool_ids == {"approval"}
    await journal.append_event(restored, Event(
        id="answer", invocation_id="turn", author="user",
        content=types.Content(role="user", parts=[types.Part(
            function_response=types.FunctionResponse(id="approval", name="approval", response={"ok": True}),
        )]),
    ))
    with pytest.raises(StateValidationError, match="no unanswered native input"):
        await journal.pause_for_input(session_id=session.id, invocation_id="turn")
