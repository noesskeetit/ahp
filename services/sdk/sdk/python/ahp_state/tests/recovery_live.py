"""Real-owner recovery contract, invoked by the Go mTLS fixture. No LLM."""
import asyncio
import base64
import copy
import hashlib
import json
import ssl
import sys

import httpx
from ahp_state.client import StateAPIClient, StateAPIError
from ahp_state import StateAdminClient, TransferClient, rehome_export


async def refused(awaitable, *, code=None, http_class=None):
    try:
        await awaitable
    except StateAPIError as error:
        if code is not None:
            assert error.code == code, f"expected {code}, got {error.code}"
        if http_class is not None:
            assert error.http_class == http_class, f"expected {http_class}, got {error.http_class}"
    else:
        raise AssertionError("forbidden/unknown operation succeeded")


async def main(path):
    with open(path, encoding="utf-8") as stream:
        cfg = json.load(stream)
    tls = ssl.create_default_context(cafile=cfg["ca_file"])
    tls.load_cert_chain(cfg["cert_file"], cfg["key_file"])
    async with httpx.AsyncClient(verify=tls, timeout=60, trust_env=False) as transport:
        source = StateAPIClient(cfg["base_url"], token=cfg["source_token"], http_client=transport)
        target = StateAPIClient(cfg["base_url"], token=cfg["target_token"], http_client=transport)
        admin = StateAdminClient(StateAPIClient(cfg["base_url"], token=cfg["admin_token"], http_client=transport))
        exported = await admin.export_thread(cfg["deployment"], cfg["subject"], cfg["source_thread"])
        await refused(StateAdminClient(source).export_thread(
            cfg["deployment"], cfg["subject"], cfg["source_thread"]), http_class="FORBIDDEN")
        await refused(target.import_thread(exported, cfg["target_context"]), http_class="FORBIDDEN")
        tampered = copy.deepcopy(exported)
        tampered["checkpoints"][0]["metadata"]["sha256"] = "0" * 64
        await refused(rehome_export(source, target, tampered,
            deployment=cfg["deployment"], context_id=cfg["target_context"],
            task_id=cfg["target_task"], key="python-bad-digest"), code="INVALID_ARGUMENT")
        await refused(TransferClient(target).copy_from_transfer_grant(
            cfg["publication_race_grant"], "python-revoke-after-copy"), http_class="FORBIDDEN")
        shared = StateAPIClient(cfg["base_url"], token=cfg["shared_target_token"], http_client=transport)
        shared_replay = await rehome_export(source, shared, exported,
            deployment=cfg["deployment"], context_id=cfg["shared_target_context"],
            task_id=cfg["shared_target_task"], key=cfg["shared_recovery_key"])
        for actual, expected in zip(shared_replay["checkpoints"], cfg["shared_prepared"]["checkpoints"], strict=True):
            if actual.get("metadata_encoding"):
                assert actual["metadata"]["id"] == expected["metadata"]["id"], "Go/Python stable key diverged"
            for a, e in zip(actual["parts"], expected["parts"], strict=True):
                assert json.loads(base64.b64decode(a["data"])) == json.loads(base64.b64decode(e["data"])), "Go/Python replay changed immutable descriptor"
        cross_unknown = await rehome_export(source, shared, exported,
            deployment=cfg["deployment"], context_id=cfg["shared_target_context"],
            task_id=cfg["shared_target_task"], key=cfg["shared_unknown_key"])
        assert cross_unknown["checkpoints"][0]["metadata"]["id"] == cfg["shared_unknown_id"], "Python redispatched Go UNKNOWN"
        cross_retry = await rehome_export(source, shared, exported,
            deployment=cfg["deployment"], context_id=cfg["shared_target_context"],
            task_id=cfg["shared_target_task"], key=cfg["shared_unknown_key"])
        assert cross_unknown == cross_retry
        prepared = await rehome_export(source, target, exported,
            deployment=cfg["deployment"], context_id=cfg["target_context"],
            task_id=cfg["target_task"], key="python-recovery-once")
        replay = await rehome_export(source, target, exported,
            deployment=cfg["deployment"], context_id=cfg["target_context"],
            task_id=cfg["target_task"], key="python-recovery-once")
        assert prepared == replay, "retry changed target immutable refs"
        restored = await target.import_thread(prepared, cfg["target_context"])
        fresh = StateAPIClient(cfg["base_url"], token=cfg["target_token"], http_client=transport)
        part = await fresh.get_part(restored["id"], "recover", "v1", "value")
        recovered = base64.b64decode(part["data"])
        assert recovered == bytes([0, 255, 97, 98]) * 120000
        ref = json.loads(base64.b64decode(exported["checkpoints"][0]["parts"][0]["data"]))
        assert hashlib.sha256(recovered).hexdigest() == ref["sha256"]
        checkpoint = await fresh.get_checkpoint(restored["id"], "recover", "recovery-cp")
        assert checkpoint["metadata"]["large"] == "m" * 350000
        assert checkpoint["metadata"]["serializer"] == "jsonplus"
        pending = await fresh.get_writes(restored["id"], "recover", "recovery-cp")
        assert len(pending) == 1
        assert base64.b64decode(pending[0]["data"]) == bytes([0, 255, 97, 98]) * 120000
        source_transfer, target_transfer = TransferClient(source), TransferClient(target)
        ticket = await source_transfer.transfer_grant(ref,
            deployment=cfg["deployment"], context_id=cfg["target_context"], task_id=cfg["target_task"])
        for name in ("other_token", "mallory_token"):
            wrong_target = TransferClient(StateAPIClient(cfg["base_url"], token=cfg[name], http_client=transport))
            await refused(wrong_target.copy_from_transfer_grant(ticket["grant"], "python-" + name),
                          http_class="FORBIDDEN")
        foreign_tls = ssl.create_default_context(cafile=cfg["ca_file"])
        foreign_tls.load_cert_chain(cfg["foreign_cert_file"], cfg["foreign_key_file"])
        async with httpx.AsyncClient(verify=foreign_tls, timeout=60, trust_env=False) as foreign_http:
            foreign = TransferClient(StateAPIClient(cfg["base_url"], token=cfg["foreign_token"], http_client=foreign_http))
            await refused(foreign.copy_from_transfer_grant(ticket["grant"], "python-foreign-tenant"),
                          http_class="FORBIDDEN")
        await refused(target_transfer.copy_from_transfer_grant(cfg["revoked_grant"], "python-revoked-reader"),
                      http_class="FORBIDDEN")
        resolved = await target_transfer.copy_from_transfer_grant(cfg["unknown_grant"], "python-lost-ack")
        replayed = await target_transfer.copy_from_transfer_grant(cfg["unknown_grant"], "python-lost-ack")
        assert resolved == replayed, "UNKNOWN recovery changed immutable target"
        assert resolved["sha256"] == ref["sha256"] and resolved["version"]
        for _ in range(2):
            await refused(target_transfer.copy_from_transfer_grant(cfg["missing_grant"], "python-missing-outcome"),
                          code="transfer_outcome_unknown", http_class="UNAVAILABLE")
    print("Python real PG/MinIO/Identity/mTLS PASS: admin export; task-admin/raw-import/digest/revoked-reader/target/subject/tenant denied; revoke after Copy before publication denied; mixed-label A-B-A stable replay; Go-to-Python <>&/U+2028/2029 key parity including lost-ACK UNKNOWN resume; exact metadata/parts/pending; lost-ACK reconcile and missing-outcome UNKNOWN without redispatch")


asyncio.run(main(sys.argv[1]))
