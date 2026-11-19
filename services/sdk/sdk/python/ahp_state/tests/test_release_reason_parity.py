"""The Python double may not be more permissive than the door it models.

DEF-RELEASE-REASON-1 put one rule on both terminal doors of internal/state: a run that
reaches `failed` or `canceled` owes a stated reason (close_reason and/or failure_cause),
and a stated reason belongs to an ended run only. A double that accepts what the real door
refuses makes every leg written against it blind rather than green - the same failure
Russell named for the Go fakes - so the rule is pinned here, offline, against the double.

The live half of this parity (re-measuring the matrix in `test_double_matches_live.py`
with the release-reason probe added) needs a stand and is not claimed here.
"""

from __future__ import annotations

import uuid

import pytest

from ahp_state import StateAPIClient, StateAPIError

from .fake_state import FakeState, FakeStateServer, StateDoc


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


async def test_the_double_refuses_an_unexplained_failed_end(server):
    boot = server.bootstrap(f"ctx-{uuid.uuid4().hex[:8]}")
    client = StateAPIClient(server.url, token=boot["token"])

    with pytest.raises(StateAPIError) as refused:
        await client.release(boot["fence"], "failed")
    assert "stated reason" in refused.value.message
    assert (await client.get_run(boot["run_id"]))["status"] == "running"

    await client.release(boot["fence"], "failed", close_reason="error",
                         failure_cause="the pinned config refused to load")
    run = await client.get_run(boot["run_id"])
    assert run["status"] == "failed"
    payload = run.get("payload") or {}
    assert payload.get("close_reason") == "error"
    assert payload.get("failure_cause") == "the pinned config refused to load"


async def test_the_double_still_lands_a_done_run_that_states_nothing(server):
    """The contract's deliberate exception: `done` carries its verdict in the transcript."""
    boot = server.bootstrap(f"ctx-{uuid.uuid4().hex[:8]}")
    client = StateAPIClient(server.url, token=boot["token"])

    await client.release(boot["fence"], "done")

    run = await client.get_run(boot["run_id"])
    assert run["status"] == "done"
    assert "close_reason" not in (run.get("payload") or {})


async def test_the_double_refuses_a_reason_on_a_run_that_is_still_running(server):
    """Recording why a run is still running would stamp a verdict it has not reached."""
    boot = server.bootstrap(f"ctx-{uuid.uuid4().hex[:8]}")
    client = StateAPIClient(server.url, token=boot["token"])

    with pytest.raises(StateAPIError) as refused:
        await client.release(boot["fence"], "pending", close_reason="error")
    assert "terminal" in refused.value.message
