"""The shipped example agent must really run (PDF rev 3.2 §2.3).

Documentation that no test executes is documentation that has rotted, so the
example in ``examples/billing_agent`` is driven through the same container path a
pod uses: ``AgentService`` + the fake State server, with a custom graph, an
``interrupt()``, one journaled external effect and steering taken by a node.

If the documented user-facing surface (``ctx.saver``, ``ctx.store``,
``ctx.effects``, ``ctx.take_steering()``) drifts, this test fails here rather than
in someone's image.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

from ahp_state.agent import AgentService
from ahp_state.tests.fake_state import FakeState, FakeStateServer, StateDoc

EXAMPLES = Path(__file__).resolve().parents[2] / "examples"


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


@pytest.fixture
def build():
    if str(EXAMPLES) not in sys.path:
        sys.path.insert(0, str(EXAMPLES))
    return importlib.import_module("billing_agent.agent").build


def service(server, build, calls):
    async def transport(tool: str, args: dict) -> dict:
        calls.append((tool, args))
        return {"charge_id": "ch-1", "amount": args.get("amount")}

    return AgentService(graph=build, base_url=server.url, worker="example-pod",
                        effects=transport)


async def test_the_example_asks_then_charges_exactly_once(server, build):
    calls: list[tuple[str, dict]] = []
    thread = server.ensure_thread("example-charge")
    server.queue_turn(thread, input={"request": {"amount": 500}})
    svc = service(server, build, calls)

    first = await svc.run_once()
    assert first.status == "interrupted", first
    assert first.interrupts[0]["value"] == {"question": "charge 500?"}
    assert calls == [], "the example charged before anyone approved"

    server.queue_turn(thread, resume="yes")
    second = await svc.run_once()
    assert second.status == "done", second
    assert calls == [("billing.charge", {"amount": 500})]
    assert second.result["receipt"] == {"charge_id": "ch-1", "amount": 500}
    assert second.result["messages"] == ["planned with 0 steering note(s)", "charged"]


async def test_the_example_refuses_over_the_policy_limit_without_asking(server, build):
    calls: list[tuple[str, dict]] = []
    thread = server.ensure_thread("example-limit")
    server.queue_turn(thread, input={"request": {"amount": 5000}})

    outcome = await service(server, build, calls).run_once()
    assert outcome.status == "done", outcome
    assert outcome.interrupts == [], "an over-limit request still asked for approval"
    assert calls == [], "an over-limit request charged anyway"
    assert "exceeds the policy limit" in outcome.result["messages"][0]


async def test_the_example_consumes_steering_it_takes_ownership_of(server, build):
    calls: list[tuple[str, dict]] = []
    thread = server.ensure_thread("example-steering")
    server.post("Enqueue", {"thread_id": thread, "key": "note-1", "kind": "steering",
                            "payload": {"state": {"note": "use the cheaper tariff"}}})
    server.queue_turn(thread, input={"request": {"amount": 100}})
    svc = service(server, build, calls)

    first = await svc.run_once()
    assert first.status == "interrupted", first
    assert first.folded_steering == [], "the container folded a row the node had taken"
    rows = [m for m in server.state.doc.d["inbox"] if m["kind"] == "steering"]
    assert [m["acked"] for m in rows] == [True]

    server.queue_turn(thread, resume=False)   # a falsy answer declines
    second = await svc.run_once()
    assert second.status == "done", second
    assert second.result["messages"] == ["planned with 1 steering note(s)", "cancelled"]
    assert second.result["approved"] is False
