"""Recover a native Hermes turn across the actual State API and Tool Gateway."""

import argparse
import asyncio
from base64 import b64decode
import json
import os
from pathlib import Path
from types import SimpleNamespace
import signal
import subprocess
import sys
from threading import Lock
import time
from uuid import uuid4

import httpx
from persistence.conversation.messages import ConversationRequest
from tools.approval_context import _approval_tool_call_id

from adapters.hermes.journal_messages import HermesReconciliationRequired, JournalMessages
from ahp_clients.artifacts import AsyncArtifactsClient
from ahp_state.client import StateAPIError
from ahp_state.transport import mtls_client
from hermes_tool_probe import CALL_ID, FINISHED, LEASE_MS, model_fixture, native_turn

TOOL = "write"


def acquire_worker(request, session, ttl_ms):
    return request("POST", "/v1/state/sessions:acquire", body={
        "session_id": session, "harness_type": "hermes", "format_version": 1, "ttl_ms": ttl_ms,
    })


def artifact_reader(runner, handshake, client, lock):
    owner = AsyncArtifactsClient(handshake["base_url"], lambda: handshake["task_token"], http_client=client._client)

    def call(method, *args, **kwargs):
        with lock:
            return runner.run(method(*args, **kwargs))

    return SimpleNamespace(get=lambda artifact_id: call(owner.get, artifact_id),
                           download=lambda artifact_id: call(owner.download, artifact_id),
                           put=lambda *args, **kwargs: call(owner.put, *args, **kwargs))


def run_probe(runner, handshake, request, gateway, args, model_url):
    session = args.source_session
    lease = acquire_worker(request, session, LEASE_MS)
    assert lease["epoch"] == 1
    repo = JournalMessages(request, session, lease_epoch=1, effect_tools=(TOOL,),
                           tool_lease_proof=lease.get("tool_lease_proof"))
    marker = args.directory / "before-recovery"

    def park(key):
        marker.with_suffix(".tmp").write_text(key)
        marker.with_suffix(".tmp").replace(marker)
        signal.pause()
        raise AssertionError("expected SIGKILL")

    if args.scenario in {"prepared", "unsent"}:
        transition = repo._transition

        def dispatch_barrier(key, status):
            assert status == "DISPATCHED"
            if args.scenario == "unsent":
                transition(key, status)
            park(key)

        repo._transition = dispatch_barrier

    def tool(arguments, **_kwargs):
        def execute(key):
            body = {"key": key, "tool": TOOL, "arguments": arguments, **repo.tool_authority()}
            if args.scenario in {"known", "large-inline", "large-artifact"}:
                async def lost_reply():
                    async with mtls_client(handshake) as client:
                        await client._client.post(handshake["base_url"] + "/v1/tools/Call", json=body, headers={
                            "Authorization": "Bearer " + handshake["task_token"],
                            "X-Test-Lose-Reply": "yes",
                        })

                try:
                    runner.run(lost_reply())
                except (httpx.ReadError, httpx.RemoteProtocolError):
                    park(key)
                raise AssertionError("the Gateway reply was not lost")
            response = gateway("Call", body)
            assert args.scenario == "unknown" and response["status"] == "SIDE_EFFECT_UNKNOWN", response
            park(key)

        return repo.dispatch_effect(_approval_tool_call_id.get(), TOOL, arguments, execute)

    native_turn(repo, args.directory, model_url, [], tool, TOOL, False)
    raise AssertionError("the original Hermes process unexpectedly finished")


def controller(runner, handshake, request, args):
    observed, server, model_url = model_fixture(TOOL, receipt_marker="written")
    directory = args.directory
    session = args.session_id or "hermes-gateway-" + uuid4().hex
    marker = directory / "before-recovery"
    try:
        (Path(os.environ["HERMES_HOME"]) / "config.yaml").write_text(
            'tools:\n  tool_search:\n    enabled: "off"\nmodel:\n  default: fixture/model\n'
            f'  provider: custom\n  base_url: {model_url}\n  context_length: 131072\n'
        )
        observed.phase = args.scenario + "-source"
        with (directory / "source.log").open("wb") as log:
            process = subprocess.Popen([sys.executable, __file__, str(directory), args.scenario,
                                        "--source-session", session, "--model-url", model_url],
                                       stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 25
                while not marker.is_file():
                    if process.poll() is not None or time.monotonic() > deadline or observed.error:
                        raise RuntimeError((directory / "source.log").read_text() or str(observed.error))
                    time.sleep(.05)
            finally:
                if process.poll() is None:
                    process.kill()
                process.wait(timeout=5)
        assert process.returncode == -signal.SIGKILL
        key = marker.read_text()
        replacement = mtls_client({**handshake, "task_token": handshake["replacement_token"]})
        lock = Lock()

        def read_state(*pos, **kw):
            with lock:
                return runner.run(replacement.request_state(*pos, **kw))

        gateway_calls = []

        def gateway(operation, body):
            gateway_calls.append(operation)
            return runner.run(replacement._request("POST", "/v1/tools/" + operation,
                                                  handshake["replacement_token"], body))

        try:
            deadline = time.monotonic() + LEASE_MS / 1000 + 3
            while True:
                try:
                    lease = acquire_worker(read_state, session, 30000)
                    break
                except StateAPIError as error:
                    if error.code != "CONFLICT" or time.monotonic() >= deadline:
                        raise
                    time.sleep(.1)
            epoch = lease["epoch"]
            assert epoch == 2
            artifacts = None
            if args.scenario == "large-artifact":
                artifacts = artifact_reader(runner, {**handshake, "task_token": handshake["replacement_token"]},
                                            replacement, lock)
            repo = JournalMessages(read_state, session, lease_epoch=epoch, effect_tools=(TOOL,),
                                   artifacts=artifacts, tool_lease_proof=lease.get("tool_lease_proof"))
            before = repo._read_intent(key)["status"]
            assert before == ("PREPARED" if args.scenario == "prepared" else "DISPATCHED"), before
            original = JournalMessages(request, session, lease_epoch=1, effect_tools=(TOOL,))
            try:
                original._transition(key, "DISPATCHED")
            except StateAPIError as error:
                assert error.code == "STALE_EPOCH", error
            else:
                raise AssertionError("previous worker can still mutate the intent")
            observed.phase = args.scenario + "-resume"
            try:
                repo.recover_gateway_effects(gateway)
            except ValueError as error:
                assert args.scenario == "large-inline" and str(error) == "Hermes message requires artifact spill"
                assert gateway_calls == ["GetResult"]
                assert repo._read_intent(key)["status"] == "DISPATCHED"
                assert not any(phase.endswith("-resume") for phase, _ in observed.requests)
                assert not any(row.role == "tool" for row in repo.read_conversation(session))
                receipt = gateway("GetResult", {"key": key})
                size = len(json.dumps(receipt["payload"], separators=(",", ":")).encode())
                assert receipt["result_storage"] == "inline" and size > 256 << 10
                return {"scenario": args.scenario, "before": before, "after": "DISPATCHED",
                        "epoch": epoch, "gateway_result_bytes": size, "continued": False,
                        "state_error": str(error), "receipt_rows": 0}
            except HermesReconciliationRequired as error:
                assert args.scenario == "unknown" and error.call_id == CALL_ID
                assert repo._read_intent(key)["status"] == "OUTCOME_UNKNOWN"
                assert gateway_calls == ["GetResult", "Get"]
                assert not any(phase.endswith("-resume") for phase, _ in observed.requests)
                unknown_rows = [row for row in repo.read_conversation(session) if row.role == "tool"]
                assert len(unknown_rows) == 1
                assert json.loads(unknown_rows[0].content) == {"code": "TOOL_OUTCOME_UNKNOWN"}
                return {"scenario":args.scenario, "before":before, "after":"OUTCOME_UNKNOWN",
                        "epoch":epoch, "gateway_calls":gateway_calls, "continued":False, "unknown_rows":1}

            assert args.scenario != "unknown"
            recovery_calls = list(gateway_calls)
            expected = (["FenceAndLookup"] if args.scenario == "large-artifact"
                        else ["GetResult"] if args.scenario == "known"
                        else ["GetResult", "Call", "GetResult"])
            assert recovery_calls == expected
            result_row = repo._read_intent(key)
            assert result_row["status"] == "COMPLETED"
            rows = repo.read_conversation(session, ConversationRequest(include_row_ids=True))
            tool_rows = [row for row in rows if row.role == "tool"]
            assert len(tool_rows) == 1 and tool_rows[0].tool_call_id == CALL_ID
            assert tool_rows[0].tool_call_uid == repo._calls()[CALL_ID].native_uid
            receipt_size = None
            if args.scenario == "large-artifact":
                blob = result_row["result"]["blob"]
                receipt = gateway("FenceAndLookup", {"key": key, **repo.tool_authority()})["receipt"]
                ref = receipt["artifact_ref"]
                assert receipt["result_storage"] == "artifact"
                assert (blob["artifact_id"], blob["version"], blob["sha256"], blob["size_bytes"]) == (
                    ref["artifact_id"], ref["version"], ref["digest"], ref["size"])
                receipt_size = blob["size_bytes"]
                assert receipt_size == 307246
                assert json.loads(repo._load_payload(result_row["result"])) == json.loads(tool_rows[0].content)
                event_page = read_state("GET", repo.route, params={"limit": 100})
                event_refs = [row["payload"]["blob"] for row in event_page["events"]
                              if row["role"] == "tool" and row["payload"].get("blob")]
                assert len(event_refs) == 1 and event_refs[0]["size_bytes"] > 256 << 10
            else:
                receipt = gateway("GetResult", {"key": key})
                assert json.loads(tool_rows[0].content) == receipt["payload"]
                assert json.loads(b64decode(result_row["result"]["inline"])) == receipt["payload"]
            before_retry = list(gateway_calls)
            repo.recover_gateway_effects(gateway)
            assert gateway_calls == before_retry

            def unexpected_tool(*_args, **_kwargs):
                raise AssertionError("the resumed model should not invoke another tool")

            result = native_turn(repo, directory, model_url, [row.as_openai_message() for row in rows],
                                 unexpected_tool, TOOL, True)
            assert result["completed"] and result["final_response"] == FINISHED, result
            assert [seen for phase, seen in observed.requests if phase.endswith("-resume")] == [True]
            assert len([row for row in repo.read_conversation(session) if row.role == "tool"]) == 1
            assert not (Path(os.environ["HERMES_HOME"]) / "state.db").exists()
            verdict = {"scenario": args.scenario, "before": before, "after": "COMPLETED",
                       "epoch": epoch, "gateway_calls": recovery_calls, "continued": True, "receipt_rows": 1}
            if receipt_size is not None:
                verdict["gateway_result_bytes"] = receipt_size
                verdict["storage"] = "artifact"
            return verdict
        finally:
            runner.run(replacement.close())
    finally:
        server.shutdown()
        server.server_close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("scenario", choices=("prepared", "unsent", "known", "large-inline", "large-artifact", "unknown"))
    parser.add_argument("--session-id")
    parser.add_argument("--source-session")
    parser.add_argument("--model-url")
    args = parser.parse_args()
    handshake = json.loads(Path(os.environ["AHP_STATE_HANDSHAKE"]).read_text())
    with asyncio.Runner() as runner:
        client = mtls_client(handshake)
        lock = Lock()

        def request(*pos, **kw):
            with lock:
                return runner.run(client.request_state(*pos, **kw))

        def gateway(operation, body):
            with lock:
                return runner.run(client._request("POST", "/v1/tools/" + operation, handshake["task_token"], body))

        try:
            if args.source_session:
                run_probe(runner, handshake, request, gateway, args, args.model_url)
            else:
                print(json.dumps(controller(runner, handshake, request, args)), flush=True)
        finally:
            runner.run(client.close())


if __name__ == "__main__":
    main()
