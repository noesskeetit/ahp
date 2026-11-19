"""Block 1 against a LIVE State server - the same properties the double proves.

The double ``fake_state.py`` proved block 1 and was itself wrong twice, so this
module re-proves the same properties against a real ``cmd/state-stand`` process:
real HTTPS with mandatory client certificates, real Ed25519 task tokens, real
PostgreSQL with product migrations. Nothing here is mocked.

Opt-in, and deliberately so - a suite that silently skips cannot be evidence.
Set the hand-out the stand writes and the module runs::

    AHP_STATE_HANDSHAKE=/path/handshake.json   # base_url, ca/cert/key, task_token
    AHP_SDK_PGDATA=/path/pgdata                # optional: enables the restart test
    AHP_SDK_PGCTL=/path/pg_ctl

Two properties cannot be read the way they are read on the double. There is no
``psql`` and no Python driver on this host, so "the server document" is reached
two ways instead: a raw ``GetKV`` through a brand-new client (the State
operation, not the ``AHPStore``/``AHPCheckpointSaver`` adapter), and - stronger -
a fresh OS process that shares no memory with the writer, plus one test that
restarts PostgreSQL itself. Where that is weaker than the double's raw
``doc.d["kv"]`` read, the report says so instead of claiming parity - which it
no longer has to: ``raw_pg.py`` reads the ``ahp_state.kv`` row straight out of
the server over the wire, with no driver, no ``psql`` and no store/saver in the
way, so the durability property is read where the contract asked for it.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import ssl
import subprocess
import sys
import time
import uuid

import httpx
import pytest

from ahp_state import (AHPCheckpointSaver, AHPStore, StateAPIError, StateAPIClient,
                       StateNotFoundError)
from ahp_state.agent import AgentService, mtls_client
from .raw_pg import (RawPostgresError, discover_database, kv_row, op_receipt,
                     server_version)

HANDSHAKE = os.environ.get("AHP_STATE_HANDSHAKE", "")
PGDATA = os.environ.get("AHP_SDK_PGDATA", "")
PGCTL = os.environ.get("AHP_SDK_PGCTL", "")


def _handshake() -> dict:
    if not HANDSHAKE or not pathlib.Path(HANDSHAKE).is_file():
        pytest.skip("AHP_STATE_HANDSHAKE does not name a live State stand")
    return json.loads(pathlib.Path(HANDSHAKE).read_text(encoding="utf-8"))


pytestmark = pytest.mark.skipif(
    not (HANDSHAKE and pathlib.Path(HANDSHAKE).is_file()),
    reason="no live State stand (AHP_STATE_HANDSHAKE unset)",
)


def reachable(hs: dict) -> bool:
    """A stand that answers with *any* protocol-level status is up."""
    ctx = ssl.create_default_context(cafile=hs["ca_file"])
    ctx.load_cert_chain(hs["cert_file"], hs["key_file"])
    try:
        with httpx.Client(verify=ctx, timeout=5.0) as http:
            http.post(f"{hs['base_url']}/v1/state/Heartbeat",
                      json={"run_id": "", "worker": "probe", "epoch": 0})
        return True
    except Exception:
        return False


@pytest.fixture
def hs():
    hand = _handshake()
    if not reachable(hand):
        pytest.skip(f"live State stand unreachable at {hand.get('base_url')}")
    return hand


class Live:
    """One thread, one claimed run: a real fence from a real ClaimRun."""

    def __init__(self, hand: dict):
        self.hand = hand
        self.context = f"sdk-live-{uuid.uuid4().hex[:10]}"
        self.client = mtls_client(hand)

    async def _drain(self) -> int:
        """Finish off runs left pending by an interrupted earlier session.

        ``ClaimRun`` is FIFO for the whole scope, so one straggler queued before
        this test would be handed to it instead of its own run - which the check
        below would (rightly) report as a shared stand. This stand belongs to
        this run alone, so abandoning the stragglers is safe here - but "belongs
        to this run alone" is a claim this method does not get to make about
        whoever's context is in the way. The same rule every other live module
        uses decides (``ahp_state.tests.ownership``): a run on a thread this
        checkout never creates stops the drain and names itself, because
        releasing it to ``failed`` would cancel another role's leg - possibly one
        whose external effect is already durable - to clear our queue.
        """
        drained = 0
        while drained < 100:
            claim = await self.client.claim_run(worker=f"py-drain-{drained}",
                                                ttl_ms=60000)
            lease = claim.get("lease")
            if not lease:
                return drained
            from .ownership import SHARED_QUEUE_HINT, run_is_ours
            ours, identity = await run_is_ours(self.client, lease["run"], self.hand)
            if not ours:
                raise AssertionError(
                    f"cannot drain the stand: work this session did not queue is in "
                    f"the queue ({identity}); {SHARED_QUEUE_HINT}")
            await self.client.release({"run_id": lease["run"]["id"],
                                       "worker": lease["worker"],
                                       "epoch": lease["epoch"]}, "failed")
            drained += 1
        raise AssertionError("the stand has 100 pending runs; it is not exclusive")

    async def open(self) -> "Live":
        await self._drain()
        # FLAT answers: live CreateThread/CreateRun return the object itself,
        # not {"thread": ...} / {"run": ...}. The double wrapped both, and every
        # test written against it inherited that envelope.
        thread = await self.client.create_thread(self.context, f"ctx:{self.context}")
        self.thread_id = thread["id"]
        run = await self.client.create_run(self.thread_id, f"run:{self.context}",
                                           {"context_id": self.context})
        self.run_key = f"run:{self.context}"
        # The claim is a bounded wait for OUR run, not one hopeful call. A lease
        # that nobody released comes back to the FIFO when it expires, and it can
        # come back *between* the drain and the claim: measured, a suite that
        # ended with ~25 runs still leased handed this suite a run from the
        # previous process one claim after the queue had been reported empty.
        # Whoever is ahead is abandoned with ``failed``, the only release that
        # moves the queue on (``pending`` puts it straight back at the head).
        # Draining is legitimate because this stand belongs to this checkout;
        # the deadline - not that belief - is what stops here on a shared stand.
        deadline = time.monotonic() + 60.0
        attempt = 0
        seen_lease = False
        while True:
            claim = await self.client.claim_run(worker=f"py-live-{attempt}",
                                                ttl_ms=120000)
            lease = claim.get("lease") or {}
            if lease:
                seen_lease = True
                claimed = (lease.get("run") or {}).get("id")
                if claimed == run["id"]:
                    break
                await self.client.release({"run_id": claimed,
                                           "worker": lease["worker"],
                                           "epoch": lease["epoch"]}, "failed")
            if time.monotonic() > deadline:
                if not seen_lease:
                    pytest.skip("live stand returned no lease for the run we "
                                "just queued within 60s")
                raise AssertionError(
                    f"our run {run['id']!r} was not handed out within 60s; this "
                    f"stand keeps returning earlier runs, so it is not exclusive")
            attempt += 1
            await asyncio.sleep(0.2)
        self.run_id = claimed
        self.token = claim.get("task_token") or self.hand.get("task_token")
        self.fence = {"run_id": self.run_id, "worker": lease["worker"],
                      "epoch": lease["epoch"]}
        return self

    def store(self) -> AHPStore:
        return AHPStore(self.client, self.fence)

    def saver(self, namespace: str = "graph") -> AHPCheckpointSaver:
        return AHPCheckpointSaver(self.client, self.thread_id, self.fence,
                                  namespace=namespace)

    def reader(self) -> StateAPIClient:
        """A second client over its own TLS session - no shared SDK state."""
        return mtls_client(self.hand)

    async def raw_kv(self, namespace: list[str], key: str) -> dict:
        """The stored row by raw State operation, not through AHPStore."""
        async with mtls_client(self.hand) as probe:
            row = await probe.get_kv(namespace, key)
        return row.get("item", row)

    async def assert_absent(self, namespace: list[str], key: str) -> None:
        """Nothing was written - which live State reports by NOT_FOUND.

        The double answered this with an empty item, so a test written against
        it ("value is None or {}") would pass on the stand and never notice it
        had been reading a row that does not exist.
        """
        with pytest.raises(StateNotFoundError) as raised:
            await self.raw_kv(namespace, key)
        assert raised.value.code == "NOT_FOUND", str(raised.value)


@pytest.fixture
async def live(hs):
    scene = await Live(hs).open()
    yield scene
    # End the run, or the next thing that talks to this stand inherits it. A
    # released lease answers STALE_EPOCH ("stale or expired"), which is the
    # expected path for the tests that close their own run - the released-lease
    # and SIGKILL ones - so only that code is tolerated, and anything else is
    # raised rather than swallowed: swallowing StateAPIError here is exactly
    # what hid four bogus ReleaseRun statuses in this tree.
    try:
        await scene.client.release(scene.fence, "done")
    except StateAPIError as refused:
        if refused.code != "STALE_EPOCH":
            raise
    await scene.client.close()


READER_SNIPPET = """
import asyncio, json, os, sys
sys.path.insert(0, {src!r})
from ahp_state.agent import mtls_client

async def main():
    hand = json.loads(open(os.environ["AHP_STATE_HANDSHAKE"]).read())
    async with mtls_client(hand) as client:
        row = await client.get_kv({ns!r}, {key!r})
    print(json.dumps(row))
asyncio.run(main())
"""


async def test_targeted_claim_leaves_an_older_run_pending(hs):
    async with mtls_client(hs) as client:
        key = uuid.uuid4().hex
        older_thread = await client.create_thread(f"older-{key}", f"older-thread-{key}")
        older = await client.create_run(older_thread["id"], f"older-run-{key}", {})
        target_thread = await client.create_thread(f"target-{key}", f"target-thread-{key}")
        target = await client.create_run(target_thread["id"], f"target-run-{key}", {})
        service = AgentService(client=client, graph=lambda _: None, worker=f"target-{key}")
        claim = await service._claim(target["id"])
        try:
            assert claim is not None
            assert claim[2] == target["id"]
            response = await client.call("GetRun", {"run_id": older["id"]})
            assert response["status"] == "pending"
        finally:
            if claim:
                await client.release(claim[0], "done")
            older_claim = await client.claim_run(f"cleanup-{key}", run_id=older["id"])
            if lease := older_claim.get("lease"):
                await client.release({
                    "run_id": older["id"], "worker": lease["worker"], "epoch": lease["epoch"],
                }, "done")


async def test_put_is_visible_to_a_raw_read_the_moment_put_returns(live):
    """Sync durability: no async tail behind the response."""
    ns = ["sdk-live", live.context]
    store = live.store()
    await store.aput(tuple(ns), "policy", {"limit": 250})

    item = await live.raw_kv(ns, "policy")
    assert item["value"] == {"limit": 250}
    assert item["version"] == 1


async def test_put_survives_a_process_that_never_saw_the_writer(live):
    """A second OS process reads it: the commit is in the server, not in us."""
    ns = ["sdk-live", live.context]
    await live.store().aput(tuple(ns), "invoice", {"total": 42})

    src = str(pathlib.Path(__file__).resolve().parents[2])
    proc = subprocess.run(
        [sys.executable, "-c", READER_SNIPPET.format(src=src, ns=ns, key="invoice")],
        capture_output=True, text=True, env=dict(os.environ),
    )
    assert proc.returncode == 0, proc.stderr[-400:]
    row = json.loads(proc.stdout.strip().splitlines()[-1])
    assert row.get("item", row)["value"] == {"total": 42}, (
        "a process that never saw the writer read something else back")


async def test_pending_writes_round_trip_on_the_real_server(live):
    saver = live.saver()
    cp = {"v": 1, "id": f"cp-{uuid.uuid4().hex[:8]}", "ts": "2026-11-01T00:00:00Z",
          "channel_values": {"messages": []}, "channel_versions": {},
          "versions_seen": {}, "pending_sends": []}
    cfg = {"configurable": {"thread_id": live.thread_id, "checkpoint_ns": ""}}
    await saver.aput(cfg, cp, {"source": "input"}, {})
    cfg["configurable"]["checkpoint_id"] = cp["id"]

    await saver.aput_writes(cfg, [("counter", 3)], "task-a")
    tup = await saver.aget_tuple(cfg)
    assert [w for w in tup.pending_writes if w[0] == "task-a"] == [("task-a", "counter", 3)]


async def test_a_replayed_write_is_not_recorded_twice(live):
    saver = live.saver()
    cp = {"v": 1, "id": f"cp-{uuid.uuid4().hex[:8]}", "ts": "2026-11-01T00:00:00Z",
          "channel_values": {}, "channel_versions": {}, "versions_seen": {},
          "pending_sends": []}
    cfg = {"configurable": {"thread_id": live.thread_id, "checkpoint_ns": ""}}
    await saver.aput(cfg, cp, {"source": "input"}, {})
    cfg["configurable"]["checkpoint_id"] = cp["id"]

    await saver.aput_writes(cfg, [("counter", 3)], "task-a")
    await saver.aput_writes(cfg, [("counter", 3)], "task-a")      # replay
    tup = await saver.aget_tuple(cfg)
    assert len([w for w in tup.pending_writes if w[0] == "task-a"]) == 1


async def test_store_idempotency_is_per_node_and_call(live):
    """Same (node, call) replays to one version; a different call is a new one."""
    ns = ("sdk-live", live.context)
    store = live.store()
    with store.with_identity(node="charge", call="call-1"):
        await store.aput(ns, "invoice", {"total": 42})
        await store.aput(ns, "invoice", {"total": 42})            # replay
    replayed = await live.raw_kv(list(ns), "invoice")
    assert replayed["version"] == 1, \
        "the same (node, call) wrote twice against live State"

    with store.with_identity(node="charge", call="call-2"):
        await store.aput(ns, "invoice", {"total": 43})
    row = await live.raw_kv(list(ns), "invoice")
    assert row["version"] == 2


async def test_a_stale_epoch_cannot_write(live):
    ns = ["sdk-live", live.context]
    stale = dict(live.fence, epoch=live.fence["epoch"] + 1)
    with pytest.raises(StateAPIError) as raised:
        await live.client.put_kv(stale, ns, "policy", f"op-{uuid.uuid4().hex[:8]}",
                                 0, {"limit": 1})
    assert "STALE_EPOCH" in str(raised.value)

    await live.assert_absent(ns, "policy")


async def test_a_released_lease_cannot_write(live):
    """One answer for every dead lease, and it is not the one the double gave.

    Measured: Release, then write, then heartbeat - all three come back
    STALE_EPOCH with ``lease epoch is stale or the lease has expired``. There is
    no LEASE_LOST and no PERMISSION_DENIED on this wire at all
    (``internal/state/errorcodes_test.go`` documents the same rule).
    """
    ns = ["sdk-live", live.context]
    await live.client.release(live.fence, "done")
    with pytest.raises(StateAPIError) as raised:
        await live.client.put_kv(live.fence, ns, "policy", f"op-{uuid.uuid4().hex[:8]}",
                                 0, {"limit": 1})
    assert raised.value.code == "STALE_EPOCH", str(raised.value)
    await live.assert_absent(ns, "policy")

    with pytest.raises(StateAPIError) as beat:
        await live.client.heartbeat(live.fence, 30000)
    assert beat.value.code == "STALE_EPOCH", str(beat.value)


async def test_interrupt_and_resume_survive_through_the_saver(live):
    """The pending ``__interrupt__`` write must be readable by a new saver."""
    from langgraph.checkpoint.base import WRITES_IDX_MAP

    saver = live.saver()
    cp = {"v": 1, "id": f"cp-{uuid.uuid4().hex[:8]}", "ts": "2026-11-01T00:00:00Z",
          "channel_values": {}, "channel_versions": {}, "versions_seen": {},
          "pending_sends": []}
    cfg = {"configurable": {"thread_id": live.thread_id, "checkpoint_ns": ""}}
    await saver.aput(cfg, cp, {"source": "input"}, {})
    cfg["configurable"]["checkpoint_id"] = cp["id"]

    await saver.aput_writes(cfg, [("__interrupt__", [{"value": "approve?"}])], "task-a")

    tup = await AHPCheckpointSaver(live.reader(), live.thread_id, live.fence,
                                   namespace="graph").aget_tuple(cfg)
    assert tup is not None
    special = [w for w in tup.pending_writes if w[1] == "__interrupt__"]
    assert special, "the interrupt write was not durable/decodable on live State"
    assert WRITES_IDX_MAP["__interrupt__"] < 0


async def test_an_intent_journal_replays_its_current_status(live):
    """The restart-without-repeat guarantee rests on this: a replayed record
    returns the CURRENT status instead of starting the effect again.

    Two things the double let a test get away with, both measured here:
    ``RecordIntent`` has no ``created`` flag - a replay is proven by the same
    row (id + digest) coming back - and the journal is monotonic, so
    ``recorded -> succeeded`` is refused and only ``recorded -> dispatched ->
    succeeded`` is legal (``internal/state/intents.go`` ``allowIntentTransition``).
    """
    first = await live.client.record_intent(live.fence, "charge", "call-1",
                                            "tool", "charge", {"amount": 42})
    again = await live.client.record_intent(live.fence, "charge", "call-1",
                                            "tool", "charge", {"amount": 42})
    assert first.get("id") == again.get("id"), "a replay inserted a second row"
    assert first.get("digest") == again.get("digest"), "a replay re-fingerprinted"
    assert first.get("status") == "recorded"

    with pytest.raises(StateAPIError) as refused:
        await live.client.update_intent(live.fence, "charge", "call-1", "succeeded",
                                       {"receipt": "r-1"})
    assert refused.value.code == "CONFLICT", str(refused.value)

    await live.client.update_intent(live.fence, "charge", "call-1", "dispatched")
    replayed = await live.client.record_intent(live.fence, "charge", "call-1",
                                               "tool", "charge", {"amount": 42})
    assert replayed.get("status") == "dispatched", (
        "a replayed record must report the CURRENT status, got "
        f"{replayed.get('status')!r}")

    await live.client.update_intent(live.fence, "charge", "call-1", "succeeded",
                                    {"receipt": "r-1"})
    fetched = await live.client.get_intent(live.run_id, "charge", "call-1")
    status = fetched.get("intent", fetched).get("status")
    assert status == "succeeded", f"a replay must report the CURRENT status, got {status}"

    with pytest.raises(StateAPIError) as reused:
        await live.client.record_intent(live.fence, "charge", "call-1",
                                        "tool", "charge", {"amount": 99})
    assert reused.value.code == "KEY_REUSE_MISMATCH", (
        "a different fingerprint under the same call must be a domain "
        f"KEY_REUSE_MISMATCH, got {reused.value}")


async def test_a_fresh_client_reads_what_another_wrote(live):
    ns = ("sdk-live", live.context)
    await live.store().aput(ns, "policy", {"limit": 250, "tier": "pro"})

    other = AHPStore(live.reader(), live.fence)
    found = await other.aget(ns, "policy")
    assert found is not None and found.value == {"limit": 250, "tier": "pro"}


async def test_the_stand_rejects_a_forged_task_token(hs):
    """Isolation is the platform's, not ours: a bad credential must not read."""
    ctx = ssl.create_default_context(cafile=hs["ca_file"])
    ctx.load_cert_chain(hs["cert_file"], hs["key_file"])
    async with httpx.AsyncClient(verify=ctx, timeout=10.0) as http:
        bad = StateAPIClient(hs["base_url"], token="forged-token", http_client=http)
        with pytest.raises(StateAPIError) as raised:
            await bad.list_threads(limit=1)
    # FORBIDDEN, and it says why: the platform's own words, measured here.
    assert raised.value.code == "FORBIDDEN", str(raised.value)
    assert "three segments" in str(raised.value)


@pytest.mark.skipif(not (PGDATA and PGCTL), reason="AHP_SDK_PGDATA/AHP_SDK_PGCTL unset")
async def test_the_value_survives_a_real_database_restart(live):
    """Durability against the disk, not against a process we could cheat with.

    Defined last: it bounces PostgreSQL underneath the stand. pgxpool is expected
    to recover, and the value must still be there afterwards.
    """
    ns = ["sdk-live", live.context]
    await live.store().aput(tuple(ns), "audit", {"kept": True})

    port = os.environ.get("AHP_SDK_PGPORT", "15611")
    # pg_ctl spawns the postmaster as a CHILD that inherits its stdout/stderr.
    # Capture the pipes and the postmaster holds them open forever, so
    # subprocess.run waits for an EOF that never arrives: send both commands to
    # a file and hand the server its own log with -l.
    log = pathlib.Path(PGDATA).parent / "logs" / "pg-restart.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("ab") as sink:
        stop = subprocess.run([PGCTL, "-D", PGDATA, "-m", "fast", "-w", "stop"],
                              stdout=sink, stderr=sink)
        assert stop.returncode == 0, f"pg_ctl stop failed: see {log}"
        time.sleep(1.0)
        start = subprocess.run(
            [PGCTL, "-D", PGDATA, "-l", str(log), "-o",
             f"-p {port} -c listen_addresses=127.0.0.1", "-w", "start"],
            stdout=sink, stderr=sink)
        assert start.returncode == 0, f"pg_ctl start failed: see {log}"

    item = await live.raw_kv(ns, "audit")
    assert item["value"] == {"kept": True}, (
        "the value did not survive a real PostgreSQL restart")


# ---------------------------------------------------------------------------
# the raw server document
#
# The contract asks for durability to be read *in the server document* the
# moment the write returns - not through ``AHPStore``, not through the saver, and
# not through ``GetKV`` either, because ``GetKV`` is one more server operation
# that could be telling a comfortable story. These three read the
# ``ahp_state.kv`` / ``ahp_state.kv_ops`` rows over the wire (``raw_pg.py``), so
# what is asserted is the row the platform wrote.
# ---------------------------------------------------------------------------

RAW_NS = "sdk-raw"


def _pg_params() -> dict:
    """Where the stand's own PostgreSQL is, or an honest skip."""
    port = os.environ.get("AHP_SDK_PGPORT", "")
    if not port:
        pytest.skip("AHP_SDK_PGPORT unset: the raw document lives in the test "
                    "cluster, and there is no other way to name it")
    try:
        database = discover_database(host="127.0.0.1", port=int(port))
    except (RawPostgresError, OSError) as exc:
        pytest.skip(f"no readable PostgreSQL on 127.0.0.1:{port}: {exc}")
    return {"host": "127.0.0.1", "port": int(port), "database": database}


@pytest.fixture
def pg():
    return _pg_params()


async def test_put_lands_in_the_raw_server_row_the_moment_it_returns(live, pg):
    """Property 1, read where the contract asked.

    Until this file existed the strongest honest claim was "``GetKV`` returned
    it", which is the server reading its own row back *for* us. Here the row is
    read without the State API in the loop: same value, version 1, no tombstone.
    """
    ns = (RAW_NS, live.context)
    value = {"limit": 250, "note": "read the row, not the response"}
    await live.store().aput(ns, "policy", value)

    row = kv_row(**pg, key="policy", namespace=f"{{{RAW_NS},{live.context}}}")
    assert row is not None, (
        "the State API would have returned this value, but the server holds no "
        "row for it - which is exactly the difference between durability and a "
        "helpful answer")
    assert json.loads(row["value"]) == value
    assert row["version"] == "1", f"a single put did not land as version 1: {row}"
    assert row["deleted"] == "f", row        # PostgreSQL text format for boolean


async def test_a_refused_write_leaves_the_server_row_byte_identical(live, pg):
    """``STALE_EPOCH`` must mean "nothing moved", down to the stored bytes.

    ``test_a_released_lease_cannot_write`` proves nothing is created by a dead
    lease. This is the other half, and the one the double could not fake: a row
    that already exists - including ``updated_at`` - is unchanged after the
    refused write.
    """
    ns = (RAW_NS, live.context)
    await live.store().aput(ns, "policy", {"limit": 1})
    before = kv_row(**pg, key="policy", namespace=f"{{{RAW_NS},{live.context}}}")
    assert before is not None and before["version"] == "1", before

    await live.client.release(live.fence, "done")
    with pytest.raises(StateAPIError) as raised:
        await live.client.put_kv(live.fence, list(ns), "policy",
                                 f"op-{uuid.uuid4().hex[:8]}", 1, {"limit": 999})
    assert raised.value.code == "STALE_EPOCH", str(raised.value)

    after = kv_row(**pg, key="policy", namespace=f"{{{RAW_NS},{live.context}}}")
    assert after == before, (
        "the refused write touched the server document: "
        f"{before!r} -> {after!r}")


async def test_the_idempotency_receipt_is_a_row_on_the_server(live, pg):
    """Replay safety is a server fact, not a client memory.

    Under a durable ``(node, call)`` identity the SDK sends one
    ``operation_key``; the server's ``kv_ops`` row is what makes a second
    delivery a replay instead of a second write. Two deliveries, one receipt row,
    one version in the document.

    ``_op_key`` is private on purpose in the product, and is read here precisely
    because this test must not trust the SDK's own arithmetic: it asks the SDK
    which key it *believes* it used, then checks the server agreed.
    """
    ns = (RAW_NS, live.context)
    store = live.store()
    with store.with_identity(node="agent", call="effect-0"):
        await store.aput(ns, "invoice", {"total": 42})
        # read the key while the identity is still active: _op_key folds the
        # active (node, call) in, and computing it afterwards - which is what
        # this test did first - asks the server about a key the SDK never sent
        op_key = store._op_key(ns, "invoice", "put", {"total": 42})
        await store.aput(ns, "invoice", {"total": 42})       # the lost-ACK replay
    receipt = op_receipt(**pg, operation_key=op_key)
    assert receipt is not None, (
        "no kv_ops row for the operation key the SDK used, so the replay above "
        "was not made safe by the server")
    assert receipt["request_digest"].startswith("sha256:"), receipt

    row = kv_row(**pg, key="invoice", namespace=f"{{{RAW_NS},{live.context}}}")
    assert row is not None and row["version"] == "1", (
        f"two deliveries moved the document to version "
        f"{row and row['version']}: the receipt is decorative")


async def test_an_unknown_intent_leaves_only_reconcile(live):
    """``unknown`` is a trapdoor with exactly one ladder out: ``ReconcileIntent``.

    vm2-state-runner's seam contract (post 20261101T100700Z, the intent-machine
    section) claims three things, and until now nothing in this tree had measured
    any of them: that ``unknown`` cannot be closed by the ordinary
    ``UpdateIntent`` path, that ``ReconcileIntent`` is reachable with the *task*
    token this run was handed, and that it demands the stored fingerprint. The
    SDK already had ``reconcile`` in ``ahp_state/intents.py``; having it and
    having proved it are different statements.

    Measured, in the order a crash produces the state:
    ``recorded -> dispatched -> unknown``, then the ordinary path is refused, a
    forged fingerprint is refused *and changes nothing*, and only the digest the
    journal itself reports moves the row - on the same row id.
    """
    node, call = "charge", "reconcile-me"
    recorded = await live.client.record_intent(live.fence, node, call,
                                              "tool", "charge", {"amount": 7})
    await live.client.update_intent(live.fence, node, call, "dispatched")
    await live.client.update_intent(live.fence, node, call, "unknown")

    # The ordinary path cannot close it: an effect whose outcome is unconfirmed
    # must not be able to declare itself finished.
    with pytest.raises(StateAPIError) as ordinary:
        await live.client.update_intent(live.fence, node, call, "succeeded",
                                       {"receipt": "wishful"})
    assert ordinary.value.code == "CONFLICT", (
        f"unknown was closable by UpdateIntent ({ordinary.value.code}); the "
        f"reconciliation gate is not enforced")

    digest = recorded.get("digest", "")
    assert digest, f"RecordIntent returned no digest to reconcile against: {recorded}"

    # Reaching the privileged operation with this run's own task token is part
    # of the contract; so is the exactness of the digest.
    with pytest.raises(StateAPIError) as forged:
        await live.client.reconcile_intent(live.fence, node, call, "succeeded",
                                          digest="sha256:" + "0" * 64)
    assert forged.value.code == "KEY_REUSE_MISMATCH", (
        f"a wrong digest was refused with {forged.value.code}, not the "
        f"fingerprint error the contract names")

    stuck = await live.client.get_intent(run_id=live.run_id, node=node, call=call)
    stuck = stuck.get("intent", stuck)
    assert stuck["status"] == "unknown", (
        f"a refused reconcile still moved the intent to {stuck['status']!r}")

    closed = await live.client.reconcile_intent(live.fence, node, call, "succeeded",
                                                digest=digest, result={"receipt": "r-1"})
    closed = closed.get("intent", closed)
    assert closed["status"] == "succeeded", closed
    assert closed.get("id") == recorded.get("id"), (
        "reconcile resolved a different row than the one the effect journaled")
