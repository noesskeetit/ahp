"""Invoked by the Go native acceptance fixture against real PG/Identity/mTLS.

The private configuration is created in the Go test's temporary directory.
Credentials are never printed; the fixture removes the directory on completion.
"""
import asyncio
import json
import ssl
import sys
import time

import httpx

from ahp_memory import AsyncMemoryClient, MemoryClient, MemoryError, MemoryManagementClient


def main(config_path):
    with open(config_path, encoding="utf-8") as source:
        config = json.load(source)

    def tls(foreign=False):
        context = ssl.create_default_context(cafile=config["ca"])
        prefix = "foreign_tenant" if foreign else "client"
        context.load_cert_chain(config[prefix + "_cert"], config[prefix + "_key"])
        return context

    def denied(call, status):
        try:
            call()
        except MemoryError as error:
            assert error.status == status
            assert error.code != "invalid_response"
        else:
            raise AssertionError("unauthorized Memory call succeeded")

    with httpx.Client(verify=tls(), trust_env=False, timeout=30) as http:
        client = MemoryClient(config["url"], lambda: config["token"], http_client=http)
        assert client.settings()["personal_enabled"]
        assert client.operation(config["operation_id"], "personal")["state"] == "succeeded"
        facts = client.search({"scope": "personal", "query": "Which language does Alice prefer?"})
        assert facts["facts"] and not facts.get("degraded")
        assert client.list("personal")["total"] > 0
        for name in ("revoked_token", "human_token", "foreign_tenant_token"):
            other = MemoryClient(config["url"], lambda name=name: config[name], http_client=http)
            denied(other.settings, 403)
        other = MemoryClient(config["url"], lambda: config["foreign_subject_token"], http_client=http)
        denied(lambda: other.operation(config["operation_id"], "personal"), 404)
        assert not other.search({"scope": "personal", "query": "Alice"})["facts"]
        denied(lambda: client.search({"scope": "project", "project_id": "foreign", "query": "Alice"}), 403)
        agent_admin = MemoryManagementClient(config["url"], lambda: config["token"], http_client=http)
        denied(lambda: agent_admin.update_settings("py-agent-settings", {"personal_enabled": False}), 403)
        denied(lambda: agent_admin.clear("py-agent-clear", {"scope": "personal"}), 403)
        admin = MemoryManagementClient(config["url"], lambda: config["authority_token"], http_client=http)
        assert admin.update_settings("py-settings", {"personal_enabled": True})["personal_enabled"]
        request = {"target": "personal", "text": "Alice prefers concise technical answers in Russian with exact test evidence.", "source_refs": [config["source_ref"]]}
        receipt = client.remember("python-native-remember", request)
        assert client.remember("python-native-remember", request)["id"] == receipt["id"]
        denied(lambda: client.remember("python-native-remember", dict(request, text="different payload")), 409)
        deadline = time.monotonic() + 120
        while receipt["state"] != "succeeded":
            assert receipt["state"] not in ("failed", "cancelled")
            assert time.monotonic() < deadline, "native Python retain deadline"
            time.sleep(0.5)
            receipt = client.operation(receipt["id"], "personal")
        assert client.search({"scope": "personal", "query": "Russian answer preferences"})["facts"]

    with httpx.Client(verify=tls(True), trust_env=False, timeout=30) as http:
        other = MemoryClient(config["url"], lambda: config["foreign_tenant_token"], http_client=http)
        denied(lambda: other.operation(config["operation_id"], "personal"), 404)
        assert not other.search({"scope": "personal", "query": "Alice"})["facts"]

    async def async_read():
        async with httpx.AsyncClient(verify=tls(), trust_env=False, timeout=30) as http:
            client = AsyncMemoryClient(config["url"], lambda: config["token"], http_client=http)
            assert (await client.search({"scope": "personal", "query": "Russian preferences"}))["facts"]
    asyncio.run(async_read())
    print("Python Memory native contract PASS: sync write/recall, async recall, mTLS, PG, Identity, tenant/subject, revoke, human, management-purpose, idempotency")


if __name__ == "__main__":
    main(sys.argv[1])
