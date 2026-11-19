"""Run the pinned Hermes conversation loop through the compact tool barriers."""

import asyncio
from hashlib import sha256
from base64 import b64decode
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
from threading import Lock, Thread
import time
from types import SimpleNamespace
from uuid import uuid4

from agent.conversation_loop import run_conversation
from persistence.conversation.messages import ConversationRequest
from run_agent import AIAgent
from toolsets import create_custom_toolset
from tools.approval_context import _approval_tool_call_id
from tools.registry import registry

from adapters.hermes.journal_messages import HermesReconciliationRequired, JournalMessages
from ahp_state.client import StateAPIError
from hermes_compact_probe import connection, acquire

TOOL = "reserve_fixture_item"
CALL_ID = "fixture-reserve-1"
RESULT = '{"receipt":"fixture-reserved-17"}'
FINISHED = "The fixture reservation was recorded."
SYSTEM = "Follow the deterministic reservation scenario."
LEASE_MS = 15_000
HELD_RESUMES = {"prepared-resume", "unsent-resume", "unknown-resume"}


class StaticFixtureSession:
    def get(self, _session):
        return SimpleNamespace(system_prompt=SYSTEM, tool_pin=None, model_config={}, ended_at=None)

    def update_system_prompt(self, _session, _prompt):
        pass

    def get_title(self, _session):
        return None

    def prompt_refresh_epoch(self, _session):
        return 0

    def get_model_config_value(self, _session, _key, default=None):
        return default

    def touch_activity(self, _session, **_kwargs):
        pass


def adapter(request, session, **kwargs):
    return JournalMessages(request, session, effect_tools=(TOOL,), **kwargs)


def worker(request, phase, session, directory, model_url):
    epoch = acquire(request, session, LEASE_MS)
    repo = adapter(request, session, lease_epoch=epoch)
    try:
        repo.assert_resumable()
    except HermesReconciliationRequired as exc:
        assert phase in HELD_RESUMES and exc.call_id == CALL_ID
        print(json.dumps({"held": True, "epoch": epoch, "pid": os.getpid()}), flush=True)
        return
    assert phase not in HELD_RESUMES

    def park_at_fault(key):
        (directory / "effect-ready.tmp").write_text(key)
        (directory / "effect-ready.tmp").replace(directory / "effect-ready")
        signal.pause()
        raise AssertionError("worker should have been killed at the failpoint")

    if phase in {"prepared-source", "unsent-source"}:
        original_begin = repo._begin_standalone

        def stop_before_effect(call):
            if phase == "unsent-source":
                assert original_begin(call)["can_execute"]
            park_at_fault(call.key)

        repo._begin_standalone = stop_before_effect

    def effect(key):
        with (directory / "effects.jsonl").open("a") as stream:
            print(json.dumps({"key": key, "pid": os.getpid(), "item": 17}), file=stream)
            stream.flush()
            os.fsync(stream.fileno())
        if phase == "unknown-source":
            park_at_fault(key)
        if phase == "unknown-live":
            raise ConnectionError("Test connector lost the result after the effect")
        return RESULT

    def tool(args, **_kwargs):
        return repo.dispatch_effect(_approval_tool_call_id.get(), TOOL, args, effect)

    history = [row.as_openai_message() for row in
               repo.read_conversation(session, ConversationRequest(include_row_ids=True))]
    recovering = phase.endswith("-resume")
    if recovering:
        assert [row["role"] for row in history if row["role"] != "system"] == ["user", "assistant", "tool"]
        assert history[-1]["content"] == RESULT
    result = native_turn(repo, directory, model_url, history, tool, TOOL, recovering)
    if phase == "unknown-live":
        assert not result.get("completed") and repo.held_after_commit, result
        print(json.dumps({"held": True, "epoch": epoch, "pid": os.getpid()}), flush=True)
        return
    assert recovering, result
    assert result["completed"] and result["final_response"] == FINISHED, result
    final_messages = repo.read_conversation(session)
    assert final_messages[-1].content == FINISHED
    assert [row.content for row in final_messages if row.role == "user"] == ["Reserve test item 17.", None]
    assert not (Path(os.environ["HERMES_HOME"]) / "state.db").exists()
    print(json.dumps({"epoch": epoch, "pid": os.getpid(), "completed": True,
                      "extra_user_input": False, "native_null_user_row": True}), flush=True)


def native_turn(repo, directory, model_url, history, tool, tool_name, recovering):
    registry.register(name=tool_name, toolset="ahp_fixture", schema={
        "name": tool_name, "description": "Reserve a test item by writing to a fixture ledger.",
        "parameters": {"type": "object", "properties": {"item": {"type": "integer"}}, "required": ["item"]},
    }, handler=tool, override=True)
    create_custom_toolset("ahp_fixture", "AHP local tool fixture", [tool_name])
    usage = SimpleNamespace(flush_usage=lambda: None, queue_usage=lambda *_args: None,
                            set_billing_route=lambda *_args, **_kwargs: None)
    handle = SimpleNamespace(read_only=False, messages=repo, sessions=StaticFixtureSession(), usage=usage)
    agent = AIAgent(
        base_url=model_url, api_key="fixture-only", provider="custom", api_mode="chat_completions",
        model="fixture/model", max_iterations=4, max_tokens=1024, enabled_toolsets=["ahp_fixture"],
        quiet_mode=True, save_trajectories=False, session_id=repo.session_id, persistence=handle,
        skip_context_files=True, skip_memory=True, skip_background_review=True, load_soul_identity=False,
        cwd=str(directory),
    )
    assert agent.persistence is handle
    assert tool_name in agent.valid_tool_names, agent.valid_tool_names
    assert not (Path(os.environ["HERMES_HOME"]) / "state.db").exists()
    # The AHP host owns session creation and the current lease for this embedded
    # loop. The fixture supplies fixed prompt metadata and no usage accounting.
    agent._session_db_created = True
    return run_conversation(agent, None if recovering else "Reserve test item 17.",
                            system_message=SYSTEM, conversation_history=history)


def model_fixture(tool_name=TOOL, receipt_marker="fixture-reserved-17"):
    observed = SimpleNamespace(phase=None, requests=[], error=None)

    class ModelHandler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass

        def do_POST(self):
            try:
                assert self.path == "/v1/chat/completions"
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                has_receipt = any(m.get("role") == "tool" and receipt_marker in m.get("content", "")
                                  for m in body["messages"])
                observed.requests.append((observed.phase, has_receipt))
                if observed.phase == "known-source" and has_receipt:
                    # Keep the original model request unfinished while the
                    # controller SIGKILLs the native worker holding the socket.
                    self.connection.settimeout(10)
                    self.connection.recv(1)
                    return
                if observed.phase.endswith("-resume"):
                    assert has_receipt
                    assert [m["content"] for m in body["messages"]
                            if m.get("role") == "user" and m.get("content")] == ["Reserve test item 17."]
                    delta = {"role": "assistant", "content": FINISHED}
                    reason = "stop"
                else:
                    assert observed.phase in {
                        "known-source", "unknown-source", "unknown-live", "prepared-source", "unsent-source",
                        "large-inline-source", "large-artifact-source",
                    } and not has_receipt
                    delta = {"role": "assistant", "content": None, "tool_calls": [{
                        "index": 0, "id": CALL_ID, "type": "function",
                        "function": {"name": tool_name, "arguments": '{"item":17}'},
                    }]}
                    reason = "tool_calls"
                base = {"id": "hermes-loop-fixture", "created": int(time.time()), "model": "fixture/model"}
                if body.get("stream"):
                    chunks = [
                        {**base, "object": "chat.completion.chunk", "choices": [{"index": 0, "delta": delta}]},
                        {**base, "object": "chat.completion.chunk",
                         "choices": [{"index": 0, "delta": {}, "finish_reason": reason}]},
                    ]
                    response = ("".join("data: " + json.dumps(chunk) + "\n\n" for chunk in chunks)
                                + "data: [DONE]\n\n").encode()
                    content_type = "text/event-stream"
                else:
                    for call in delta.get("tool_calls", ()):
                        del call["index"]
                    response = json.dumps({
                        **base, "object": "chat.completion",
                        "choices": [{"index": 0, "message": delta, "finish_reason": reason}],
                        "usage": {"prompt_tokens": 40, "completion_tokens": 10, "total_tokens": 50},
                    }).encode()
                    content_type = "application/json"
                self.send_response(200)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(response)))
                self.end_headers()
                self.wfile.write(response)
            except Exception as exc:
                observed.error = repr(exc)
                self.send_error(500)

    server = ThreadingHTTPServer(("127.0.0.1", 0), ModelHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return observed, server, f"http://127.0.0.1:{server.server_port}/v1"


def controller(request, directory):
    observed, server, url = model_fixture()
    try:
        (Path(os.environ["HERMES_HOME"]) / "config.yaml").write_text(
            'tools:\n  tool_search:\n    enabled: "off"\nmodel:\n  default: fixture/model\n'
            f'  provider: custom\n  base_url: {url}\n  context_length: 131072\n'
        )
        for case, status_before, status_after, count in (
            ("known", "COMPLETED", "COMPLETED", 1),
            ("unknown", "DISPATCHED", "OUTCOME_UNKNOWN", 1),
            ("prepared", "PREPARED", "OUTCOME_UNKNOWN", 0),
            ("unsent", "DISPATCHED", "OUTCOME_UNKNOWN", 0),
        ):
            case_dir = directory / case
            case_dir.mkdir()
            session = "hermes-tool-" + uuid4().hex
            observed.phase = case + "-source"
            argv = [sys.executable, __file__]
            with (case_dir / "source.log").open("wb") as log:
                process = subprocess.Popen([*argv, observed.phase, session, str(case_dir), url],
                                           stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 25
                    while not ((case == "known" and (observed.phase, True) in observed.requests)
                               or (case_dir / "effect-ready").exists()):
                        if process.poll() is not None or time.monotonic() > deadline or observed.error:
                            raise RuntimeError((case_dir / "source.log").read_text() or str(observed.error))
                        time.sleep(.05)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait()
            assert process.returncode == -signal.SIGKILL
            effects_file = case_dir / "effects.jsonl"
            if count:
                effect, = map(json.loads, effects_file.read_text().splitlines())
                key = effect["key"]
                assert effect["pid"] == process.pid
            else:
                assert not effects_file.exists()
                key = (case_dir / "effect-ready").read_text()
            stored = adapter(request, session)
            intent = stored._read_intent(key)
            assert intent["status"] == status_before, intent
            operation_id = "" if status_before == "PREPARED" else "standalone-" + sha256(key.encode()).hexdigest()
            assert intent["gateway_operation_id"] == operation_id
            assert stored._calls()[CALL_ID].key == key
            if case == "known":
                assert json.loads(b64decode(intent["result"]["inline"])) == RESULT
            time.sleep(LEASE_MS / 1000 + .1)
            observed.phase = case + "-resume"
            with (case_dir / "resume.log").open("wb") as log:
                process_result = subprocess.run([*argv, observed.phase, session, str(case_dir), url],
                                                stdout=log, stderr=log, timeout=35)
            lines = (case_dir / "resume.log").read_text().splitlines()
            assert process_result.returncode == 0, lines
            resumed = json.loads(lines[-1])
            assert resumed["epoch"] == 2 and resumed["pid"] != process.pid
            assert len(effects_file.read_text().splitlines() if effects_file.exists() else []) == count
            assert [receipt for phase, receipt in observed.requests if phase == observed.phase] == (
                [True] if case == "known" else [])
            assert stored._read_intent(key)["status"] == status_after
            if case != "known":
                assert resumed["held"]
            else:
                assert resumed["completed"] and not resumed["extra_user_input"]
            stale = adapter(request, session, lease_epoch=1)
            try:
                stale._transition(key, "DISPATCHED")
            except StateAPIError as error:
                assert error.code == "STALE_EPOCH", error
            else:
                raise AssertionError("Hermes previous epoch could mutate a tool intent")
            print(json.dumps({"case": case, "effect_count": count, "signal": "SIGKILL",
                              "status_before": status_before, "status_after": status_after,
                              "stale_epoch_rejected": True,
                              "model_requests": [item for item in observed.requests if item[0].startswith(case + "-")],
                              **resumed}), flush=True)
        live = directory / "live"
        live.mkdir()
        observed.phase = "unknown-live"
        session = "hermes-tool-" + uuid4().hex
        with (live / "source.log").open("wb") as log:
            result = subprocess.run([sys.executable, __file__, observed.phase, session, str(live), url],
                                    stdout=log, stderr=log, timeout=35)
        messages = (live / "source.log").read_text().splitlines()
        assert result.returncode == 0, messages
        held = json.loads(messages[-1])
        assert held["held"]
        effect, = map(json.loads, (live / "effects.jsonl").read_text().splitlines())
        intent = adapter(request, session)._read_intent(effect["key"])
        assert intent["status"] == "OUTCOME_UNKNOWN", intent
        assert [receipt for phase, receipt in observed.requests if phase == observed.phase] == [False]
        print(json.dumps({"case": "live-unknown", "single_effect": True, "model_requests": 1, **held}), flush=True)
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    with asyncio.Runner() as runner:
        client, state_request = connection(runner)
        request_lock = Lock()

        def locked_request(*args, **kwargs):
            with request_lock:
                return state_request(*args, **kwargs)

        try:
            if len(sys.argv) == 2:
                controller(locked_request, Path(sys.argv[1]))
            else:
                worker(locked_request, sys.argv[1], sys.argv[2], Path(sys.argv[3]), sys.argv[4])
        finally:
            runner.run(client.close())
