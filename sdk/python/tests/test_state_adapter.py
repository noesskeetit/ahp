"""The adapters end to end over real mTLS against a live State stand.

This replaces the module-level skip this file carried since the client surface
changed: it used to drive ``Client(ca_file=..., cert_file=...)`` / ``Session``,
neither of which the package hands out any more, and to spawn its own stand from
``AHP_STATE_SERVE_BIN``. Both halves are gone. It now speaks through
:func:`ahp_state.agent.mtls_client` and the same
:class:`~ahp_state.AHPCheckpointSaver` / :class:`~ahp_state.AHPStore` the
container uses, against a stand the caller already has, and the four properties
it always set out to prove are proved again - on real HTTPS with real client
certificates and real PostgreSQL, not against a double:

1. a genuine ``interrupt()`` / ``Command(resume=...)`` cycle survives to a brand
   new client, i.e. as if the process had restarted;
2. checkpoint history is ordered and individually addressable;
3. the store is compare-and-swap, and a stale writer is refused rather than
   silently winning;
4. a second tenant, with its own certificate and its own token, reads nothing of
   the first one's - isolation is the platform's, and this test only checks that
   the SDK never leaks a way around it.

Run with the hand-out a running stand wrote::

    AHP_STATE_HANDSHAKE=/path/handshake.json
"""

from __future__ import annotations

import json
import os
import pathlib
import pytest
import uuid
from typing import Annotated, TypedDict

import operator

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from ahp_state import AHPCheckpointSaver, AHPStore, StateAPIError, StateAPIClient
from ahp_state.agent import mtls_client
from ahp_state.tests.test_live_state_properties import Live

HANDSHAKE = os.environ.get("AHP_STATE_HANDSHAKE", "")

pytestmark = pytest.mark.skipif(
    not (HANDSHAKE and pathlib.Path(HANDSHAKE).is_file()),
    reason="no live State stand (AHP_STATE_HANDSHAKE unset): these are the "
           "live-stand properties, so there is no offline substitute",
)


def _hand() -> dict:
    return json.loads(pathlib.Path(HANDSHAKE).read_text(encoding="utf-8"))


@pytest.fixture
async def lease():
    scene = await Live(_hand()).open()
    yield scene
    await scene.client.close()


class Chat(TypedDict, total=False):
    messages: Annotated[list[str], operator.add]
    answer: str


def _graph_with_interrupt(saver: AHPCheckpointSaver, store=None):
    """A real compiled LangGraph that stops for a human and waits.

    Compiled the way ``ahp-sdk check`` requires a container to compile it -
    ``checkpointer=`` and ``store=`` both bound - so this exercises the same
    shape a pod runs, not a half-configured one.
    """
    graph = StateGraph(Chat)

    async def ask(state):
        picked = interrupt({"question": "approve the charge?"})
        return {"messages": [f"approved:{picked}"]}

    graph.add_node("ask", ask)
    graph.add_node("finish", lambda state: {"answer": "done"})
    graph.add_edge(START, "ask")
    graph.add_edge("ask", "finish")
    graph.add_edge("finish", END)
    return graph.compile(checkpointer=saver, store=store)


async def test_an_interrupt_resumes_in_a_client_that_never_saw_it_break(lease):
    """A real ``interrupt()`` in a real ``StateGraph``, resumed over a NEW TLS
    session by a client that shares no memory with the one that stopped.

    Nothing here is stubbed: LangGraph raises its own ``GraphInterrupt``, the
    saver journals it as a pending ``__interrupt__`` write on the server, and
    the second process reads that row back and continues the same thread.
    """
    cfg = {"configurable": {"thread_id": lease.thread_id}}
    first = AHPCheckpointSaver(lease.client, lease.thread_id, lease.fence,
                               namespace="adapter")
    graph = _graph_with_interrupt(first, AHPStore(lease.client, lease.fence))

    stopped = await graph.ainvoke({"messages": ["start"]}, cfg)
    assert stopped.get("__interrupt__"), (
        f"the graph did not stop for a human, it returned {stopped!r}")

    async with mtls_client(lease.hand) as later:
        revived = _graph_with_interrupt(
            AHPCheckpointSaver(later, lease.thread_id, lease.fence,
                               namespace="adapter"),
            AHPStore(later, lease.fence))
        final = await revived.ainvoke(Command(resume="yes"), cfg)

    assert "approved:yes" in final.get("messages", []), (
        f"the resumed run did not carry the answer through the server: {final}")
    assert final.get("answer") == "done"


async def test_the_checkpoint_history_is_ordered_and_addressable(lease):
    saver = AHPCheckpointSaver(lease.client, lease.thread_id, lease.fence,
                               namespace="history")
    cfg = {"configurable": {"thread_id": lease.thread_id, "checkpoint_ns": ""}}
    ids = []
    parent = ""
    for _ in range(3):
        cp = {"v": 1, "id": f"cp-{uuid.uuid4().hex[:8]}", "ts": "2026-11-01T00:00:00Z",
              "channel_values": {}, "channel_versions": {}, "versions_seen": {},
              "parent": parent, "pending_sends": []}
        # aput returns the config for the next step (BaseCheckpointSaver), so the
        # identity to read back is the one that went in
        await saver.aput(cfg, cp, {"source": "loop"}, {})
        ids.append(cp["id"])
        parent = cp["id"]

    listed = [t async for t in saver.alist(cfg)]      # alist is a generator
    seen = [t.checkpoint["id"] for t in listed]
    assert set(ids) <= set(seen), f"the three checkpoints are not all in the history: {seen}"
    positions = [seen.index(i) for i in ids]
    assert positions == sorted(positions, reverse=True), (
        f"newest-first history is not newest-first: {seen}")

    fetched = await lease.client.get_checkpoint(lease.thread_id, "history", ids[1])
    assert fetched.get("id") == ids[1], f"GetCheckpoint by id returned {fetched!r}"


async def test_the_store_is_compare_and_swap_under_a_live_lease(lease):
    ns = ("adapter", lease.context)
    store = AHPStore(lease.client, lease.fence)
    await store.aput(ns, "policy", {"limit": 10})
    row = await store.aget(ns, "policy")
    assert row.value == {"limit": 10}
    assert (await lease.raw_kv(list(ns), "policy"))["version"] == 1

    # compare-and-swap itself: a writer that still believes version 1 is current
    # must be refused. ``AHPStore.aput`` re-reads first (that is what makes it
    # forgiving), so the CAS is proved on the wire it is actually implemented on.
    async with mtls_client(lease.hand) as other:
        rival = AHPStore(other, lease.fence)
        await rival.aput(ns, "policy", {"limit": 20})
        with pytest.raises(StateAPIError) as raised:
            await other.put_kv(lease.fence, list(ns), "policy",
                               f"stale-{uuid.uuid4().hex[:8]}", 1, {"limit": 99})
        assert raised.value.code == "CONFLICT", str(raised.value)

    assert (await store.aget(ns, "policy")).value == {"limit": 20}, (
        "the refused write changed the durable value")

    found = await store.asearch(("adapter",), filter={"limit": 20})
    assert any(item.key == "policy" for item in found), (
        f"SearchKV with a JSON containment filter missed the row: {found}")


async def test_a_second_tenant_reads_nothing_of_the_first(lease):
    """Negative isolation, with the stand's own second certificate."""
    hand = lease.hand
    absent = [name for name in ("foreign_cert_file", "foreign_key_file", "foreign_task_token")
              if not hand.get(name)]
    if absent:
        # Not a pass and not a code problem: the credentials of a second tenant are the stand
        # owner's to hand over. A lane cannot mint them, because Identity lets only an allowlisted
        # exchanger turn a human session into a task token (measured on the live aggregate:
        # 403 azp_mismatch for any other deployment, 403 for a subject without space:access).
        pytest.skip("hand-out has no second-tenant credentials: " + ", ".join(absent)
                    + "; a lane cannot mint a foreign-tenant task token itself")
    foreign = {**hand,
               "cert_file": hand["foreign_cert_file"],
               "key_file": hand["foreign_key_file"],
               "task_token": hand["foreign_task_token"]}
    assert hand["tenant"] != foreign.get("tenant") or hand.get("foreign_tenant"), \
        "the hand-out does not describe a second tenant"

    ns = ["adapter", lease.context]
    await lease.store().aput(ns, "secret", {"value": "for-tenant-a-only"})

    async with mtls_client(foreign) as intruder:
        assert isinstance(intruder, StateAPIClient)
        with pytest.raises(StateAPIError) as read:
            await intruder.get_kv(ns, "secret")
        assert read.value.code in ("NOT_FOUND", "FORBIDDEN"), str(read.value)

        threads = await intruder.list_threads(limit=100)
        ids = [t.get("id") for t in threads.get("threads", [])]
        assert lease.thread_id not in ids, (
            "tenant B listed tenant A's thread - the SDK or the stand leaked scope")

        with pytest.raises(StateAPIError) as write:
            await intruder.put_kv(lease.fence, ns, "secret",
                                  f"forged-{uuid.uuid4().hex[:8]}", 1,
                                  {"value": "overwritten by tenant B"})
        assert write.value.code in ("FORBIDDEN", "STALE_EPOCH", "NOT_FOUND"), \
            str(write.value)

    assert (await lease.store().aget(tuple(ns), "secret")).value == \
        {"value": "for-tenant-a-only"}, "the intruder changed the value anyway"
