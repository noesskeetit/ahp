"""Go harness arms a real-MinIO ACK-loss proxy; the public Python client must
classify lost PUT answers as outcome-unknown and reconcile under the SAME
upload identity: confirmation through owner Seal (bytes verified by SHA), one
bounded re-send when bytes never landed, no second artifact, no silent retry.

MIME is application/octet-stream to mirror the Go legs exactly - the only
variable under test is the client implementation, not the ingestion state
machine. A supported-text artifact is (correctly) gated by the owner until
extraction is ready (internal/artifacts/ingestion.go checkContent), which
needs the sandbox parser this reconcile harness deliberately does not run;
the text/extraction live path is covered separately by the parser-configured
TestPythonArtifactsRealOwners/TestGoArtifacts suites.
"""
import hashlib
import json
import ssl
import sys
import urllib.parse
import urllib.request

import httpx
from ahp_clients import APIError, ArtifactsClient


def main(path):
    with open(path) as f:
        cfg = json.load(f)

    def tls():
        ctx = ssl.create_default_context(cafile=cfg["ca"])
        ctx.load_cert_chain(cfg["client_cert"], cfg["client_key"])
        return ctx

    def arm(mode, count):
        q = urllib.parse.urlencode({"mode": mode, "count": count, "tenant": cfg["tenant"]})
        with urllib.request.urlopen(cfg["admin"] + "/arm?" + q) as r:
            r.read()

    def disarm():
        with urllib.request.urlopen(cfg["admin"] + "/arm?" + urllib.parse.urlencode({"mode": "off", "count": 0, "tenant": cfg["tenant"]})) as r:
            r.read()

    def stats():
        with urllib.request.urlopen(cfg["admin"] + "/stats") as r:
            return json.load(r)

    token = [cfg["token"]]
    out = {}
    with httpx.Client(verify=tls(), timeout=60, trust_env=False) as http:
        client = ArtifactsClient(cfg["base"], lambda: token[0], http_client=http)
        data = ("python ack-loss " + "π" * 4000).encode()
        digest = hashlib.sha256(data).hexdigest()

        # 1) Bytes landed, ACK lost: Seal verifies the real bytes, so the
        #    reconcile must confirm without a single re-send.
        arm("land", 1)
        ref = client.put("ack-landed", "landed.bin", "application/octet-stream", data)
        st = stats()
        disarm()
        assert st["puts"] == 1 and st["killed"] == 1, f"landed ACK loss must not re-send: {st}"
        assert ref["sha256"] == digest and ref["size"] == len(data), ref
        assert client.download(ref["id"]) == data
        out["landed"] = ref["id"]

        # 2) Bytes never landed: exactly one bounded re-send under the same ID.
        arm("lost", 1)
        ref2 = client.put("ack-lost", "lost.bin", "application/octet-stream", data)
        st = stats()
        disarm()
        assert st["puts"] == 2 and st["killed"] == 1, f"want one dead attempt + one re-send: {st}"
        assert client.download(ref2["id"]) == data
        out["lost"] = ref2["id"]

        # 3) Both attempts die: typed outcome-unknown, then the same key must
        #    reconcile the SAME upload - a second upload ID would be a second
        #    effect and is forbidden by the contract.
        up = client.begin_upload("ack-bothlost", "both.bin", "application/octet-stream", digest, len(data))
        arm("lost", 2)
        try:
            client.put("ack-bothlost", "both.bin", "application/octet-stream", data)
            raise AssertionError("both PUT attempts died; put must report outcome-unknown")
        except APIError as error:
            assert error.code == "UNKNOWN" and error.outcome_unknown, (error.code, error.outcome_unknown)
        st = stats()
        disarm()
        assert st["puts"] == 2 and st["killed"] == 2, f"Put must stop after two attempts: {st}"
        replay = client.begin_upload("ack-bothlost", "both.bin", "application/octet-stream", digest, len(data))
        assert replay["id"] == up["id"], "same key must reconcile the same upload, not mint a second"
        client.upload(replay, data)
        ref3 = client.seal(up["id"])
        assert client.download(ref3["id"]) == data
        out["bothlost"] = ref3["id"]

        # 4) A repeated convenience put is an idempotent replay of the same ref.
        assert client.put("ack-bothlost", "both.bin", "application/octet-stream", data) == ref3
        # The Go harness cross-checks these ids from the OWNER side, so publish
        # the bytes Python actually wrote (Python and Go payloads differ).
        payload = {"ids": out, "sha256": digest, "size": len(data)}
        print("python ack-loss parity PASS ids=" + json.dumps(out))
        with open(cfg["output"], "w") as f:
            json.dump(payload, f)


if __name__ == "__main__":
    main(sys.argv[1])
