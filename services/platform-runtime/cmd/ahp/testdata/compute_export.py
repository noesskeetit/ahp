"""Actual Go/Python SDK export interoperability against mounted AHP owners."""
import asyncio
import hashlib
import json
import os
import ssl
import sys

import httpx
from ahp_clients import (ComputeClient, AsyncComputeClient, ArtifactsClient,
                         AsyncArtifactsClient, APIError)


def main(path):
    with open(path, encoding="utf-8") as source:
        cfg = json.load(source)

    def tls():
        context = ssl.create_default_context(cafile=cfg["ca"])
        context.load_cert_chain(cfg["cert"], cfg["key"])
        return context

    def refused(fn, code, status):
        try:
            fn()
        except APIError as error:
            assert error.status == status and (error.code == code or error.reason == code)
        else:
            raise AssertionError("expected current owner refusal")

    with httpx.Client(verify=tls(), timeout=20, trust_env=False) as http:
        client = ComputeClient(cfg["base"], lambda: cfg["token"], http_client=http)
        artifacts = ArtifactsClient(cfg["base"], lambda: cfg["token"], http_client=http)
        same = client.export(cfg["computer"], "out.txt", "sdk-go-export")
        assert same["artifact_id"] == cfg["go_ref"]["artifact_id"]
        assert same["version"] == cfg["go_ref"]["version"]
        assert "sealed_at" not in same
        data = artifacts.download(same["artifact_id"])
        assert hashlib.sha256(data).hexdigest() == cfg["digest"]
        assert len(data) == cfg["size"]
        assert artifacts.read_range(same["artifact_id"], 17, 1024) == data[17:1041]
        ref = client.export(cfg["computer"], "out.txt", "sdk-python-export")
        refused(lambda: client.export(cfg["computer"], "other.txt", "sdk-go-export"),
                "KEY_REUSE_MISMATCH", 400)
        foreign = ComputeClient(cfg["base"], lambda: cfg["foreign"], http_client=http)
        refused(lambda: foreign.export(cfg["computer"], "out.txt", "foreign-sdk"), "NOT_FOUND", 404)
        revoked = ComputeClient(cfg["base"], lambda: cfg["revoked"], http_client=http)
        refused(lambda: revoked.export(cfg["computer"], "out.txt", "revoked-sdk"), "FORBIDDEN", 403)

    async def async_replay():
        async with httpx.AsyncClient(verify=tls(), timeout=20, trust_env=False) as http:
            client = AsyncComputeClient(cfg["base"], lambda: cfg["token"], http_client=http)
            artifacts = AsyncArtifactsClient(cfg["base"], lambda: cfg["token"], http_client=http)
            replay = await client.export(cfg["computer"], "out.txt", "sdk-python-export")
            assert replay["artifact_id"] == ref["artifact_id"] and replay["version"] == ref["version"]
            assert await artifacts.download(ref["artifact_id"]) == data
    asyncio.run(async_replay())
    with os.fdopen(os.open(cfg["output"], os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as target:
        json.dump(ref, target)
    print("Python Compute sync/async + Artifacts exact bytes/range + cross-language replay + current denials PASS")


if __name__ == "__main__":
    main(sys.argv[1])
