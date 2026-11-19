"""Exercise the pinned Hermes native persistence seam on a temporary State stand."""

import asyncio
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

from agent.context_compressor import _DB_PERSISTED_MARKER
from persistence.conversation.messages import ConversationRequest, NewMessage
from run_agent import AIAgent

from adapters.hermes.journal_messages import JournalMessages
from ahp_state.client import StateAPIError
from ahp_state.transport import mtls_client


def connection(runner):
    handshake = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    client = mtls_client(handshake)
    return client, lambda *args, **kwargs: runner.run(client.request_state(*args, **kwargs))


def acquire(request, session, ttl_ms=5000):
    return request("POST", "/v1/state/sessions:acquire", body={
        "session_id": session, "harness_type": "hermes", "format_version": 1, "ttl_ms": ttl_ms,
    })["epoch"]


def initial_messages():
    return [
        {"role": "user", "content": "Reserve item", "timestamp": 1001.123456789,
         "api_content": "Reserve item [exact provider text]", "message_uid": "fixture-user"},
        {"role": "assistant", "content": None, "timestamp": 1002.0,
         "tool_calls": [{"id": "call-1", "type": "function",
                         "function": {"name": "reserve", "arguments": '{"item":1}'}}],
         "_tool_call_uids": {"call-1": "fixed-tool-uid"}, "message_uid": "fixture-assistant"},
        {"role": "tool", "content": "reservation-42", "timestamp": 1003.0,
         "tool_call_id": "call-1", "_tool_call_uid": "fixed-tool-uid", "message_uid": "fixture-tool"},
    ]


def source(request, session, marker):
    epoch = acquire(request, session)
    repo = JournalMessages(request, session, lease_epoch=epoch)
    handle = SimpleNamespace(read_only=False, messages=repo)
    agent = AIAgent(api_key="local-fixture", base_url="http://127.0.0.1:9/v1",
                   model="fixture/model", quiet_mode=True, session_id=session,
                   persistence=handle, skip_context_files=True, skip_memory=True,
                   skip_background_review=True, load_soul_identity=False)
    assert agent.persistence is handle and agent._session_db is handle
    assert not (Path(os.environ["HERMES_HOME"]) / "state.db").exists()
    # The stand already created this lease-bound AHP session. This exercise
    # covers the native messages repository; the sessions repository is separate.
    agent._session_db_created = True
    messages = initial_messages()
    assert agent._flush_messages_to_session_db(messages)
    assert [message["_row_id"] for message in messages] == [1, 2, 3]
    assert all(message[_DB_PERSISTED_MARKER] for message in messages)
    assert agent._flush_messages_to_session_db(messages)
    ready = marker.with_suffix(".tmp")
    ready.write_text(json.dumps({"epoch": epoch, "pid": os.getpid()}))
    ready.replace(marker)
    signal.pause()


def resume(request, session):
    epoch = acquire(request, session)
    writes = 0

    def lose_ack(method, path, **kwargs):
        nonlocal writes
        result = request(method, path, **kwargs)
        if path.endswith("/events:append"):
            writes += 1
            if writes == 1:
                raise StateAPIError("TRANSPORT", "Fixture dropped committed response")
        return result

    repo = JournalMessages(lose_ack, session, lease_epoch=epoch)
    rows = repo.read_conversation(session, ConversationRequest(include_row_ids=True))
    actual = [row.as_openai_message() for row in rows]
    for expected, recorded in zip(initial_messages(), actual, strict=True):
        for key, value in expected.items():
            assert value == recorded[key], (key, value, recorded.get(key))
    assert [row.row_id for row in rows] == [1, 2, 3]
    assert repo.append(session, NewMessage(role="assistant", content="done", timestamp=1004.0,
                                          message_uid="fixture-finish")) == 4
    assert writes == 2
    assert [row.content for row in repo.read_conversation(session)] == [
        "Reserve item", None, "reservation-42", "done",
    ]
    print(json.dumps({"pid": os.getpid(), "epoch": epoch, "restored_rows": 3, "seq": 4,
                      "lost_reply_replayed": True, "writes_attempted": writes, "native_tool_uid": rows[-1].tool_call_uid}))


def probe(request, directory):
    session = "hermes-" + uuid4().hex
    marker = directory / "hermes-ready.json"
    args = [sys.executable, __file__]
    with (directory / "hermes-source.log").open("wb") as log:
        process = subprocess.Popen([*args, "source", session, str(marker)], stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 20
            while not marker.exists():
                if process.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError("Hermes native writer did not reach its durable boundary")
                time.sleep(.05)
            owner = json.loads(marker.read_text())
            assert owner["epoch"] == 1 and owner["pid"] == process.pid
        finally:
            if process.poll() is None:
                process.kill()
            process.wait()
    assert process.returncode == -signal.SIGKILL
    time.sleep(5.1)
    resumed = json.loads(subprocess.check_output([*args, "resume", session], timeout=25))
    assert resumed["epoch"] == 2 and resumed["pid"] != owner["pid"]
    try:
        JournalMessages(request, session, lease_epoch=1).append(
            session, NewMessage(role="assistant", content="stale", timestamp=1005.0))
    except StateAPIError as error:
        assert error.code == "STALE_EPOCH", error
    else:
        raise AssertionError("Hermes old writer was not fenced")
    print(json.dumps({"scope": "native AIAgent message flush and a cold message repository",
                      "source_signal": "SIGKILL", "stale_epoch_rejected": True, **resumed}))


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        client, request = connection(runner)
        try:
            if sys.argv[1] == "source":
                source(request, sys.argv[2], Path(sys.argv[3]))
            elif sys.argv[1] == "resume":
                resume(request, sys.argv[2])
            else:
                probe(request, Path(sys.argv[1]))
        finally:
            runner.run(client.close())
