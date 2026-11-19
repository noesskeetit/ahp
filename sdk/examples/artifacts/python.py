"""Public Python Artifacts: current task token + caller-owned verified mTLS.

Set AHP_API_URL, AHP_TOKEN_FILE, AHP_CA_FILE, AHP_CERT_FILE, AHP_KEY_FILE.
Persist --key before dispatch. On UNKNOWN reconcile that same key/ID; do not
invent a fresh operation. No provider/storage credentials or framework required.
"""
import argparse
import asyncio
import os
from pathlib import Path
import ssl

import httpx
from ahp_clients import ArtifactsClient, AsyncArtifactsClient


def token():
    return Path(os.environ["AHP_TOKEN_FILE"]).read_text().strip()


def tls():
    ctx=ssl.create_default_context(cafile=os.environ["AHP_CA_FILE"])
    ctx.load_cert_chain(os.environ["AHP_CERT_FILE"],os.environ["AHP_KEY_FILE"])
    return ctx


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file",type=Path)
    parser.add_argument("--key",required=True,help="persisted unique operation key")
    parser.add_argument("--async-client",action="store_true")
    args=parser.parse_args()
    # Public helper accepts bounded bytes; no silently truncated file reads.
    if not 0<args.file.stat().st_size<=64<<20:
        raise ValueError("file must be 1..64 MiB")
    with args.file.open("rb") as source:
        data=source.read((64<<20)+1)
    if not 0<len(data)<=64<<20:
        raise ValueError("file changed outside the supported size bound")
    if args.async_client:
        async def run():
            async with httpx.AsyncClient(verify=tls(),trust_env=False,timeout=60) as http:
                c=AsyncArtifactsClient(os.environ["AHP_API_URL"],token,http_client=http)
                ref=await c.put(args.key,args.file.name,"application/octet-stream",data)
                assert await c.download(ref["id"])==data
                print(ref["id"],ref["version"],ref["sha256"],ref["size"])
        asyncio.run(run())
    else:
        with httpx.Client(verify=tls(),trust_env=False,timeout=60) as http:
            c=ArtifactsClient(os.environ["AHP_API_URL"],token,http_client=http)
            ref=c.put(args.key,args.file.name,"application/octet-stream",data)
            assert c.download(ref["id"])==data
            print(ref["id"],ref["version"],ref["sha256"],ref["size"])

if __name__=="__main__":main()
