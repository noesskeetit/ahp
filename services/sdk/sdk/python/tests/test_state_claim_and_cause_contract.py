"""Claim binding and the terminal cause: two places where the Python State client
was quieter than the Go SDK, and where quietness costs money.

An unbound ClaimRun answers with the FIFO head of ANY thread in the deployment, so
an executor that runs "what it got" runs another task's payload under that run's
fence and budget (architecture rev 3.2 §4.2 makes the run of a task the unit of
lease, budget and audit). The Go SDK removed its unbound claim path for exactly
this reason; this client now offers the named form.

And a run closed without its cause cannot be reconciled with the execution plane's
own terminal record, so Release must be able to carry one.

Self-contained on purpose: only httpx, no LangGraph import, so the contract is
measurable on a machine that has not installed the graph extras.
"""

from __future__ import annotations

import json

import pytest

import httpx

from ahp_state.client import StateAPIClient, StateValidationError

FENCE = {"run_id": "run-claim-1", "worker": "pod-claim", "epoch": 7}
BASE = "https://state-stand.internal"


class RecordingState:
    """Answers with canned bodies and records what the client actually sent."""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self.responses: dict[str, tuple[int, dict]] = {}

    def reply(self, operation: str, status: int, body: dict) -> None:
        self.responses[operation] = (status, body)

    def _handle(self, request: httpx.Request) -> httpx.Response:
        operation = request.url.path.rsplit("/", 1)[-1]
        body = json.loads(request.content.decode() or "{}")
        self.requests.append({"path": request.url.path, "body": body})
        status, payload = self.responses.get(operation, (200, {"ok": True}))
        return httpx.Response(status, json=payload)

    def client(self) -> StateAPIClient:
        http = httpx.AsyncClient(transport=httpx.MockTransport(self._handle), base_url=BASE)
        return StateAPIClient(BASE, http_client=http)


async def test_claim_run_for_names_the_run_it_executes():
    state = RecordingState()
    state.reply("ClaimRun", 200, {"lease": None, "task_token": ""})
    client = state.client()

    await client.claim_run_for("pod-claim", "run-77")

    body = state.requests[-1]["body"]
    assert body == {"worker": "pod-claim", "ttl_ms": 120000, "run_id": "run-77"}


async def test_unbound_claim_does_not_smuggle_an_empty_run_id():
    """An empty run_id reads as a malformed bound ask; absence must stay absence."""
    state = RecordingState()
    state.reply("ClaimRun", 200, {"lease": None, "task_token": ""})
    client = state.client()

    await client.claim_run("pod-claim", ttl_ms=5000)

    assert state.requests[-1]["body"] == {"worker": "pod-claim", "ttl_ms": 5000}


async def test_claim_run_for_refuses_an_empty_id_locally():
    """Refusing here beats asking the server to guess which run was meant."""
    state = RecordingState()
    client = state.client()

    with pytest.raises(StateValidationError):
        await client.claim_run_for("pod-claim", "")
    assert state.requests == []


async def test_release_carries_the_terminal_cause():
    state = RecordingState()
    state.reply("Release", 200, {"ok": True})
    client = state.client()

    await client.release(FENCE, "failed", close_reason="model_finished")

    body = state.requests[-1]["body"]
    assert body["run_id"] == FENCE["run_id"]
    assert body["worker"] == FENCE["worker"]
    assert body["epoch"] == FENCE["epoch"]
    assert body["status"] == "failed"
    assert body["close_reason"] == "model_finished"


async def test_release_omits_an_absent_cause():
    """Empty means "nothing to add", not "the cause is the empty string"."""
    state = RecordingState()
    client = state.client()

    await client.release(FENCE, "done")
    assert "close_reason" not in state.requests[-1]["body"]

    await client.release(FENCE, "done", close_reason="")
    assert "close_reason" not in state.requests[-1]["body"]


async def test_release_carries_both_halves_of_the_reason():
    """DEF-RELEASE-REASON-1: a failed run owes State the dictionary word AND this
    runtime's own sentence, so the run row and the transcript cannot disagree."""
    state = RecordingState()
    client = state.client()

    await client.release(FENCE, "failed", close_reason="error",
                         failure_cause="STATE_TIMEOUT: the checkpoint write timed out")

    body = state.requests[-1]["body"]
    assert body["close_reason"] == "error"
    assert body["failure_cause"] == "STATE_TIMEOUT: the checkpoint write timed out"


async def test_release_without_a_reason_is_the_historical_bytes():
    """A caller with nothing to say must not move a byte: the added keys are absent,
    not empty, so an old sender's request is exactly what it was before this field."""
    state = RecordingState()
    client = state.client()

    await client.release(FENCE, "done")

    assert state.requests[-1]["body"] == {"run_id": FENCE["run_id"], "worker": FENCE["worker"],
                                          "epoch": FENCE["epoch"], "status": "done"}

    await client.release(FENCE, "done", close_reason="", failure_cause="")
    body = state.requests[-1]["body"]
    assert "close_reason" not in body and "failure_cause" not in body
