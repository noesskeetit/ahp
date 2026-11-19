"""Steering fold semantics (PDF rev 3.2 §2.3): owed, folded once, acked once.

The container-level tests show steering arriving in a running graph; these pin the
rules the fold itself must never break, independent of the executor:

* an acked row is never work again (``ReadInbox`` redelivers it - the durable
  ``acked`` flag decides, not a cursor we would have to keep);
* a row whose fold is already journaled is acked, not applied twice - that is the
  restart window between the state commit and the ack;
* a row that cannot be applied stays **owed** and is reported, never swallowed;
* ``as_node`` is mandatory: without a node that already completed there is no
  safe point, so the fold must refuse rather than guess.
"""

from __future__ import annotations

import operator
import uuid

import pytest
from langgraph.graph import END, START, StateGraph
from typing_extensions import Annotated, TypedDict

from ahp_state import StateAPIClient
from ahp_state.steering import (
    FOLD_NAMESPACE,
    SteeringFold,
    completed_node,
    fold_values,
    is_checkpoint_pause,
)
from ahp_state.tests.fake_state import FakeState, FakeStateServer, StateDoc


class State(TypedDict, total=False):
    messages: Annotated[list[str], operator.add]
    steering: Annotated[list[str], operator.add]


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


class Harness:
    """A graph + a fold bound to one lease, the way ``AgentService`` builds them."""

    def __init__(self, server: FakeStateServer, context: str):
        self.server = server
        self.boot = server.bootstrap(context)
        self.client = StateAPIClient(server.url, token=self.boot["token"])
        from ahp_state.checkpoint_saver import AHPCheckpointSaver

        saver = AHPCheckpointSaver(self.client, self.boot["thread_id"], self.boot["fence"])
        graph = StateGraph(State)
        graph.add_node("first", lambda s: {"messages": ["first"]})
        graph.add_node("second", lambda s: {"messages": ["second"]})
        graph.add_edge(START, "first")
        graph.add_edge("first", "second")
        graph.add_edge("second", END)
        self.graph = graph.compile(checkpointer=saver)
        self.config = {"configurable": {"thread_id": self.boot["thread_id"]}}
        self.fold = SteeringFold(self.client, self.boot["fence"],
                                 thread_id=self.boot["thread_id"],
                                 run_id=self.boot["fence"]["run_id"])

    def steer(self, payload, key: str = "") -> str:
        row = self.server.post("Enqueue", {
            "thread_id": self.boot["thread_id"], "key": key or f"s-{uuid.uuid4().hex[:6]}",
            "kind": "steering", "payload": payload})["id"]
        return row

    def row(self, row_id: str) -> dict:
        return next(m for m in self.server.state.doc.d["inbox"] if m["id"] == row_id)

    async def values(self) -> dict:
        return (await self.graph.aget_state(self.config)).values

    async def start(self) -> None:
        async for _ in self.graph.astream({"messages": []}, self.config,
                                          stream_mode="updates", durability="sync"):
            pass


async def test_pending_only_owes_unacked_steering_rows(server):
    h = Harness(server, "fold-pending")
    await h.start()
    mine = h.steer({"state": {"steering": ["a"]}})
    h.server.post("Enqueue", {"thread_id": h.boot["thread_id"], "key": "noise",
                              "kind": "note", "payload": {"state": {"steering": ["x"]}}})

    assert [m["id"] for m in await h.fold.pending()] == [mine]

    h.server.post("AckInbox", {"fence": h.boot["fence"], "ids": [mine]})
    assert await h.fold.pending() == [], "an acked row was offered as work again"


async def test_fold_applies_acks_and_is_then_a_noop(server):
    h = Harness(server, "fold-basic")
    await h.start()
    row = h.steer({"state": {"steering": ["shorter please"]}})

    report = await h.fold.fold(h.graph, h.config, as_node="first")
    assert report.folded == [row] and report.changed
    assert (await h.values())["steering"] == ["shorter please"]
    assert h.row(row)["acked"]

    again = await h.fold.fold(h.graph, h.config, as_node="first")
    assert not again.changed and (await h.values())["steering"] == ["shorter please"]


async def test_fold_without_a_completed_node_refuses(server):
    """No ``as_node`` means no safe point: the row must stay owed, untouched."""
    h = Harness(server, "fold-no-node")
    await h.start()
    row = h.steer({"state": {"steering": ["later"]}})

    report = await h.fold.fold(h.graph, h.config, as_node="")
    assert not report.changed
    assert h.row(row)["acked"] is False
    assert (await h.values()).get("steering", []) == []


async def test_a_journaled_fold_is_acked_but_never_applied_twice(server):
    """The restart window: the state commit is durable, its ack was lost."""
    h = Harness(server, "fold-replay")
    await h.start()
    row = h.steer({"state": {"steering": ["only once"]}})
    key = f"{h.boot['fence']['run_id']}:{row}"
    await h.client.put_kv(h.boot["fence"], FOLD_NAMESPACE, key, f"steer:{key}", 0,
                          {"run_id": h.boot["fence"]["run_id"], "state": "applied"})

    report = await h.fold.fold(h.graph, h.config, as_node="first")
    assert report.replayed == [row] and not report.folded
    assert (await h.values()).get("steering", []) == [], "the fold was applied twice"
    assert h.row(row)["acked"], "a completed fold must still reach its ack"


async def test_an_unfoldable_row_stays_owed_and_is_reported(server):
    h = Harness(server, "fold-undelivered")
    await h.start()
    row = h.steer({"state": {"steering": ["nowhere"]}, "as_node": "ghost-node"})

    report = await h.fold.fold(h.graph, h.config, as_node="first")
    assert report.undelivered == [row] and not report.changed
    assert h.row(row)["acked"] is False, "a row nobody applied must not be swallowed"
    assert h.fold.undelivered == [row]


async def test_plain_text_steering_goes_to_the_messages_channel(server):
    h = Harness(server, "fold-text")
    await h.start()
    row = h.steer("answer in Russian")

    report = await h.fold.fold(h.graph, h.config, as_node="first")
    assert report.folded == [row]
    assert "answer in Russian" in (await h.values())["messages"]


async def test_taken_rows_are_acked_only_once_durable(server):
    h = Harness(server, "fold-take")
    await h.start()
    row = h.steer({"state": {"steering": ["mine"]}})

    taken = await h.fold.take()
    assert [m["id"] for m in taken] == [row]
    assert await h.fold.pending() == [], "a taken row was offered for folding again"
    assert h.row(row)["acked"] is False, "ack must wait for the node's durable write"

    assert await h.fold.ack_consumed() == [row]
    assert h.row(row)["acked"]
    assert await h.fold.ack_consumed() == []


def test_fold_values_shapes():
    assert fold_values({"payload": {"state": {"a": 1}, "as_node": "n"}}) == ({"a": 1}, "n")
    assert fold_values({"payload": {"steering": ["x"]}}) == ({"steering": ["x"]}, None)
    assert fold_values({"payload": "text"}) == ({"messages": ["text"]}, None)
    assert fold_values({"payload": None}) == ({}, None)


def test_event_helpers():
    assert is_checkpoint_pause({"__interrupt__": [1]})
    assert not is_checkpoint_pause({"ask": {"messages": []}})
    # __start__ is the graph input and __interrupt__ a pause: neither may own an
    # update, so the last real node is kept
    assert completed_node({"__start__": {}}, seen="") == ""
    assert completed_node({"first": {}}, seen="") == "first"
    assert completed_node({"__interrupt__": []}, seen="first") == "first"


# ----------------------------------------------------------------------
# the one admitted window: fold committed, journal not yet written
# ----------------------------------------------------------------------

async def test_the_journal_window_redelivers_once_and_that_is_measured(server):
    """RED: kill the worker between the state commit and the journal write.

    Not a prose claim about at-least-once - the duplicate is counted. The fold
    is durable, the journal is absent and the row is still owed, so a resume
    legitimately applies it again. Delivering a user instruction twice beats
    dropping it; what matters is that the excess is exactly one delivery, which
    the next test pins.
    """
    h = Harness(server, "fold-window-red")
    await h.start()
    row = h.steer({"state": {"steering": ["hold the discount"]}})
    working_journal = h.fold._journal

    async def killed_after_the_commit(message):
        raise RuntimeError("worker died after the checkpoint, before the journal")

    h.fold._journal = killed_after_the_commit
    with pytest.raises(RuntimeError):
        await h.fold.fold(h.graph, h.config, as_node="first")

    assert (await h.values())["steering"] == ["hold the discount"], \
        "the fold was not durable when the worker died - this is a different bug"
    assert h.row(row)["acked"] is False, "the row was acked without its journal row"
    assert [m["id"] for m in await h.fold.pending()] == [row]

    h.fold._journal = working_journal
    report = await h.fold.fold(h.graph, h.config, as_node="first")
    assert report.folded == [row], "a resume refused to re-fold: not at-least-once"
    assert (await h.values())["steering"] == ["hold the discount", "hold the discount"]


async def test_the_redelivery_is_bounded_and_the_ack_stays_exactly_once(server):
    """GREEN: the excess is one delivery, never a growing one.

    After the window the journal exists, so every later resume is a no-op and
    the row is acked exactly once. Bounded duplication plus exactly-once ack is
    the property the container actually relies on.
    """
    h = Harness(server, "fold-window-green")
    await h.start()
    row = h.steer({"state": {"steering": ["cap it at 250"]}})
    working_journal = h.fold._journal

    async def killed_after_the_commit(message):
        raise RuntimeError("crash inside the window")

    h.fold._journal = killed_after_the_commit
    with pytest.raises(RuntimeError):
        await h.fold.fold(h.graph, h.config, as_node="first")
    h.fold._journal = working_journal

    await h.fold.fold(h.graph, h.config, as_node="first")          # the one redelivery
    third = await h.fold.fold(h.graph, h.config, as_node="first")  # a later resume
    fourth = await h.fold.fold(h.graph, h.config, as_node="first")

    assert not third.changed and not fourth.changed, "the redelivery is not bounded"
    assert (await h.values())["steering"] == ["cap it at 250", "cap it at 250"]
    assert h.row(row)["acked"] is True

    key = f"{h.boot['fence']['run_id']}:{row}"
    journaled = await h.client.get_kv(FOLD_NAMESPACE, key)
    assert journaled.get("item", journaled)["value"]["state"] == "applied"

    # and the checkpoint chain is still readable by a stranger: no orphaned branch
    from ahp_state.checkpoint_saver import AHPCheckpointSaver

    stranger = AHPCheckpointSaver(StateAPIClient(h.server.url, token=h.boot["token"]),
                                  h.boot["thread_id"], h.boot["fence"])
    tup = await stranger.aget_tuple(h.config)
    assert tup is not None
    assert tup.checkpoint["channel_values"]["steering"] == ["cap it at 250", "cap it at 250"]
