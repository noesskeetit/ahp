"""Go composition invokes this against real PG/MinIO/Identity/mTLS and Linux parser."""
import asyncio
import base64
import hashlib
import json
import os
import ssl
import sys

import httpx
from ahp_clients import ArtifactsClient, AsyncArtifactsClient, APIError


def main(path):
    with open(path) as f: cfg=json.load(f)
    def tls(name="client"):
        context=ssl.create_default_context(cafile=cfg["ca"])
        context.load_cert_chain(cfg[name+"_cert"],cfg[name+"_key"])
        return context
    def denied(fn, codes=None):
        try: fn()
        except APIError as error:
            if codes: assert error.code in codes, error.code
            return error
        raise AssertionError("expected owner refusal")
    token=[cfg["source"]]
    with httpx.Client(verify=tls(),timeout=30,trust_env=False) as http:
        client=ArtifactsClient(cfg["base"],lambda:token[0],http_client=http)
        # A separate HTTPS sink requests a client certificate. The upload pool
        # must present none even when the owner pool carries mTLS/auth/cookies.
        http.headers["Authorization"]="Bearer owner-default"
        http.cookies.set("owner-cookie","private")
        upload_only=ArtifactsClient(cfg["base"],lambda:token[0],http_client=http,upload_ca_file=cfg["blob_ca"])
        trap=dict(id="trap",presigned_url=cfg["blob_url"],size=4,sha256=hashlib.sha256(b"trap").hexdigest())
        upload_only.upload(trap,b"trap")
        target=ArtifactsClient(cfg["base"],lambda:cfg["target"],http_client=http)
        assert client.download(cfg["go_ref"]["id"])==base64.b64decode(cfg["go_data"])
        data="Python sync alpha searchable привет".encode()
        ref=client.put("python-sync","sync.txt","text/plain",data,labels={"subjects":[cfg["subject_id"]]})
        assert client.put("python-sync","sync.txt","text/plain",data,labels={"subjects":[cfg["subject_id"]]})==ref
        assert client.download(ref["id"])==data
        large=b"large binary payload "*(900000)
        large_ref=client.put("large-binary","large.bin","application/octet-stream",large)
        assert len(large)>16<<20
        assert client.download(large_ref["id"])==large
        assert client.read_range(large_ref["id"],len(large)-11,100)==large[-11:]
        assert client.read_range(ref["id"],7,4)==data[7:11]
        assert client.read_range(ref["id"],len(data)-2,100)==data[-2:]
        ready=client.readiness(ref["id"])
        assert ready["content_ready"] and ready["extraction_ready"] and ready["search_ready"] and ready["ready"],ready
        assert client.process(ref["id"])["ready"]
        chunk=client.read_text_chunk(ref["id"],ref["version"],0,32768,12)
        assert len(chunk["text"].encode())<=12 and data.startswith(chunk["text"].encode())
        assert client.search_text(ref["id"],ref["version"],"searchable")["matched"]
        assert any(row["artifact_id"]==ref["id"] for row in client.resolve(ref["sha256"])["versions"])
        denied(lambda:client.read_text_chunk(ref["id"],"tampered-version",0,32,32),{"CONFLICT"})
        denied(lambda:client.put("python-sync","changed.txt","text/plain",b"changed"),{"INVALID_ARGUMENT","CONFLICT"})
        # Hash/length-negative reaches real MinIO and real owner Seal; public
        # upload helper separately rejects declaration mismatches before PUT.
        up=client.begin_upload("bad-hash","bad.bin","application/octet-stream",hashlib.sha256(b"right").hexdigest(),5)
        with httpx.Client(trust_env=False,follow_redirects=False) as blob:
            result=blob.put(up["presigned_url"],content=b"wrong")
            assert result.status_code==200
        denied(lambda:client.seal(up["id"]),{"CONFLICT"})
        unsupported=client.put("opaque","opaque.bin","application/octet-stream",b"opaque bytes")
        rd=client.readiness(unsupported["id"])
        assert rd["content_ready"] and not rd["extraction_ready"] and not rd["ready"]
        malformed=client.put("bad-json","bad.json","application/json",b"{broken")
        assert client.readiness(malformed["id"])["quarantined"]
        denied(lambda:client.download(malformed["id"]),{"CONFLICT","FORBIDDEN"})
        # Same operation reconciles a lost owner ACK without a new logical upload.
        digest=hashlib.sha256(b"lost ack").hexdigest()
        http.headers["X-Test-Lose-ACK"]="1"
        e=denied(lambda:client.begin_upload("lost-ack","lost.bin","application/octet-stream",digest,8))
        assert e.outcome_unknown
        del http.headers["X-Test-Lose-ACK"]
        up=client.begin_upload("lost-ack","lost.bin","application/octet-stream",digest,8)
        client.upload(up,b"lost ack")
        http.headers["X-Test-Lose-ACK"]="1"
        assert denied(lambda:client.seal(up["id"])).outcome_unknown
        del http.headers["X-Test-Lose-ACK"]
        reconciled=client.seal(up["id"])
        assert client.seal(up["id"])==reconciled and client.download(reconciled["id"])==b"lost ack"
        # Immutable reference pinning: tampered version is refused at owner;
        # tampered digest is detected when validating the returned exact grant.
        bad=dict(ref,version="tampered")
        denied(lambda:client.transfer_grant(bad,cfg["owner_deployment"],cfg["target_context"],cfg["target_task"]),{"FORBIDDEN"})
        bad=dict(ref,sha256="0"*64)
        denied(lambda:client.transfer_grant(bad,cfg["owner_deployment"],cfg["target_context"],cfg["target_task"]),{"INVALID_RESPONSE"})
        grant=client.transfer_grant(ref,cfg["owner_deployment"],cfg["target_context"],cfg["target_task"])
        transferred=target.copy_from_transfer_grant(grant["grant"],"python-transfer")
        assert target.copy_from_transfer_grant(grant["grant"],"python-transfer")==transferred
        assert transferred["sha256"]==ref["sha256"] and transferred["version"] and transferred["context_id"]==cfg["target_context"]
        assert transferred["labels"]["subjects"]==[cfg["subject_id"]]
        assert target.download(transferred["id"])==data
        http.headers["X-Test-Lose-ACK"]="1"
        assert denied(lambda:target.copy_from_transfer_grant(grant["grant"],"lost-transfer")).outcome_unknown
        del http.headers["X-Test-Lose-ACK"]
        recovered=target.copy_from_transfer_grant(grant["grant"],"lost-transfer")
        assert target.copy_from_transfer_grant(grant["grant"],"lost-transfer")==recovered
        assert target.download(recovered["id"])==data
        denied(lambda:target.download(ref["id"]),{"FORBIDDEN","NOT_FOUND"})
        for name in ("revoked","other_context","target"):
            token[0]=cfg[name]
            denied(lambda:client.get(ref["id"]),{"FORBIDDEN","NOT_FOUND"})
        token[0]=cfg["source"]
        assert client.get(ref["id"])["id"]==ref["id"]
        # Live revoke after metadata must stop the second HTTP call's bytes.
        token[0]=cfg["race"]
        http.headers["X-Test-Revoke-After-Get"]="1"
        denied(lambda:client.download(ref["id"]),{"FORBIDDEN"})
        del http.headers["X-Test-Revoke-After-Get"]
        token[0]=cfg["source"]
        for name in ("tenant","deployment"):
            token[0]=cfg[name]
            denied(lambda:client.get(ref["id"]),{"FORBIDDEN"})
        token[0]=cfg["source"]
    for name in ("tenant","deployment","subject"):
        with httpx.Client(verify=tls(name),trust_env=False) as http:
            c=ArtifactsClient(cfg["base"],lambda:cfg[name],http_client=http)
            denied(lambda:c.download(ref["id"]),{"FORBIDDEN","NOT_FOUND"})
    async def run():
        current=[cfg["source"]]
        async def callback(): return current[0]
        async with httpx.AsyncClient(verify=tls(),trust_env=False,timeout=30) as http:
            c=AsyncArtifactsClient(cfg["base"],callback,http_client=http)
            uploader=AsyncArtifactsClient(cfg["base"],callback,http_client=http,upload_ca_file=cfg["blob_ca"])
            await uploader.upload(trap,b"trap")
            async_ref=await c.put("python-async","async.txt","text/plain",b"Python async exact bytes")
            assert await c.download(async_ref["id"])==b"Python async exact bytes"
            assert await c.read_range(async_ref["id"],0,6)==b"Python"
            assert (await c.readiness(async_ref["id"]))["ready"]
            assert (await c.process(async_ref["id"]))["ready"]
            assert (await c.read_text_chunk(async_ref["id"],async_ref["version"],0,32,32))["text"]=="Python async exact bytes"
            assert (await c.search_text(async_ref["id"],async_ref["version"],"async"))["matched"]
            assert (await c.resolve(async_ref["sha256"]))["versions"]
            ticket=await c.transfer_grant(async_ref,cfg["owner_deployment"],cfg["target_context"],cfg["target_task"])
            current[0]=cfg["target"]
            copied=await c.copy_from_transfer_grant(ticket["grant"],"async-copy")
            assert await c.download(copied["id"])==b"Python async exact bytes"
            current[0]=cfg["revoked"]
            try: await c.get(async_ref["id"])
            except APIError as e: assert e.code=="FORBIDDEN"
            else: raise AssertionError("async revoke ignored")
            return async_ref
    async_ref=asyncio.run(run())
    fd=os.open(cfg["output"],os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,"w") as f:json.dump({"sync":ref,"async":async_ref,"transfer":transferred},f)
    print("Python sync+async actual owner upload/seal/download/range/parser/transfer PASS; Go crossread source, tamper/hash/foreign/revoke denials, lost ACK same-key/ID reconciliation")

if __name__=="__main__":main(sys.argv[1])
