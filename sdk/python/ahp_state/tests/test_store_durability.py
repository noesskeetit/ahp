"""Block 1 / PDF rev 3.2 - ``AHPStore`` honours the same six rules as the saver.

``AHPCheckpointSaver`` and ``AHPStore`` are named together in the requirement, so
the store gets the same proof on the same contract double, in this directory:
durable when the write returns, fenced against a writer that lost the lease,
replay-safe under a durable operation identity, readable by a *fresh* instance
from the bytes a different process wrote, and JSON on the wire rather than
pickle. Pre-existing store coverage lives in ``sdk/python/tests``; it never
asserted the durability-before-return or the stale-writer half.
"""

from __future__ import annotations

import json
import uuid

import pytest

from ahp_state import AHPStore, StateAPIClient, StateAPIError

from .fake_state import FakeState, FakeStateServer, StateDoc

NS = ("billing", "tenants")


@pytest.fixture
def server():
    with FakeStateServer(FakeState(StateDoc())) as srv:
        yield srv


class Harness:
    """One claimed run: the fence a node actually holds while it writes."""

    def __init__(self, server: FakeStateServer):
        self.server = server
        self.boot = server.bootstrap(f"store-{uuid.uuid4().hex[:8]}")
        self.client = StateAPIClient(server.url, token=self.boot["token"])
        self.store = AHPStore(self.client, self.boot["fence"])

    def durable(self, key: str) -> dict:
        """The stored row as the *server* holds it - not re-read through us."""
        for raw, item in self.server.state.doc.d["kv"].items():
            if key in raw:
                return item
        raise AssertionError(f"nothing durable for key {key!r}")

    def has(self, key: str) -> bool:
        return any(key in raw for raw in self.server.state.doc.d["kv"])


async def test_a_stored_value_is_durable_when_put_returns(server):
    """The write is committed before the await completes - no async tail."""
    h = Harness(server)
    await h.store.aput(NS, "policy", {"limit": 250})

    item = h.durable("policy")
    assert item["value"] == {"limit": 250}, \
        "the value was not in the durable document when put() returned"
    assert item["version"] == 1
    assert item["updated_at"], "the row carries no server timestamp"


async def test_a_deleted_value_is_durable_when_delete_returns(server):
    h = Harness(server)
    await h.store.aput(NS, "policy", {"limit": 250})
    await h.store.adelete(NS, "policy")

    assert h.durable("policy")["deleted"] is True
    assert await h.store.aget(NS, "policy") is None


async def test_a_stale_epoch_cannot_write_to_the_store(server):
    """A writer whose epoch was superseded must fail, not quietly no-op."""
    h = Harness(server)
    stale = dict(h.boot["fence"], epoch=h.boot["fence"]["epoch"] + 1)
    stale_store = AHPStore(h.client, stale)

    with pytest.raises(StateAPIError) as raised:
        await stale_store.aput(NS, "policy", {"limit": 1})

    assert "STALE_EPOCH" in str(raised.value)
    assert not h.has("policy"), "a superseded writer reached the durable document"


async def test_a_released_lease_cannot_write_to_the_store(server):
    """Releasing the lease ends write authority for that run - immediately."""
    h = Harness(server)
    server.state.doc.d["leases"][h.boot["run_id"]]["status"] = "released"

    with pytest.raises(StateAPIError) as raised:
        await h.store.aput(NS, "policy", {"limit": 1})
    # STALE_EPOCH, not LEASE_LOST: measured against a live State stand, and
    # ``internal/state/errorcodes_test.go`` says the same - a released lease is
    # simply "not yours any more". The double used to answer LEASE_LOST here,
    # a code the platform does not have; this assertion had inherited the lie.
    assert raised.value.code == "STALE_EPOCH", str(raised.value)
    assert not h.has("policy")


async def test_a_journaled_store_write_replays_instead_of_double_writing(server):
    """Under a durable (node, call) identity the same logical write is once.

    This is what makes "the pod died after the server committed but before the
    response arrived" survivable for agent memory too, not only for effects.
    """
    h = Harness(server)
    with h.store.with_identity(node="charge", call="call-1"):
        first = await h.store.aput(NS, "invoice", {"total": 42})
        replay = await h.store.aput(NS, "invoice", {"total": 42})

    version_after_replay = h.durable("invoice")["version"]
    assert version_after_replay == 1, \
        "the replay advanced the version - the write landed twice"
    assert (await h.store.aget(NS, "invoice")).value == {"total": 42}
    assert first is None and replay is None


async def test_two_writes_under_different_identities_are_two_versions(server):
    """Replay safety must not collapse genuinely distinct writes."""
    h = Harness(server)
    with h.store.with_identity(node="charge", call="call-1"):
        await h.store.aput(NS, "invoice", {"total": 42})
    with h.store.with_identity(node="charge", call="call-2"):
        await h.store.aput(NS, "invoice", {"total": 43})

    assert h.durable("invoice")["version"] == 2
    assert (await h.store.aget(NS, "invoice")).value == {"total": 43}


async def test_a_fresh_store_reads_what_another_process_wrote(server):
    """Cross-instance readability: memory is shared through State, not a cache.

    A restarted pod rebuilds the store from the handshake, so anything readable
    only through the writing instance is not durable memory at all.
    """
    h = Harness(server)
    await h.store.aput(NS, "policy", {"limit": 250, "tier": "pro"})

    other_client = StateAPIClient(server.url, token=h.boot["token"])
    other = AHPStore(other_client, h.boot["fence"])
    found = await other.aget(NS, "policy")

    assert found is not None and found.value == {"limit": 250, "tier": "pro"}


async def test_store_values_are_json_on_the_wire_not_pickle(server):
    """A pickle blob cannot be read by another image, so it must not appear."""
    h = Harness(server)
    await h.store.aput(NS, "policy", {"limit": 250, "note": "ünïcode"})

    item = h.durable("policy")
    payload = item["value"]
    assert isinstance(payload, dict) and payload["note"] == "ünïcode"
    assert isinstance(item, dict), "the stored row is not a JSON document"
    assert "gASV" not in json.dumps(item), "a pickle frame reached the durable store"


async def test_namespaces_and_search_read_from_durable_bytes(server):
    h = Harness(server)
    await h.store.aput(("billing", "acme"), "policy", {"limit": 10})
    await h.store.aput(("billing", "globex"), "policy", {"limit": 20})

    namespaces = await h.store.alist_namespaces(prefix=("billing",))
    assert sorted(namespaces) == [("billing", "acme"), ("billing", "globex")]

    found = await h.store.asearch(("billing",), filter={"limit": 20})
    assert [item.value["limit"] for item in found] == [20]


async def test_a_response_without_a_timestamp_still_reads(server):
    """A read must not depend on a bookkeeping field being present.

    ``internal/state`` always sends ``updated_at``, so this is the defensive
    path - but it is the one that used to raise ``ValueError`` from inside
    LangGraph's ``Item`` constructor, turning an odd server response into a
    crashed agent turn. Forced here by stripping the field off the wire.
    """
    h = Harness(server)
    await h.store.aput(NS, "policy", {"limit": 250})

    real_get_kv = h.client.get_kv

    async def without_timestamp(namespace, key):
        doc = await real_get_kv(namespace, key)
        doc.get("item", doc).pop("updated_at", None)
        return doc

    h.client.get_kv = without_timestamp

    found = await h.store.aget(NS, "policy")
    assert found is not None and found.value == {"limit": 250}
    assert found.updated_at is not None
