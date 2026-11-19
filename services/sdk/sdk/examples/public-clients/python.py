"""Standalone owner clients using a pre-existing State lease and IntentRecorder.

AHP_NODE_TASK_ID and AHP_CALL_ID come from the durable runtime, not new UUIDs.
Task token files and mTLS files are caller supplied; no provider credentials.
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os
from pathlib import Path
import ssl

import httpx
from ahp_clients import AsyncModelClient, AsyncToolsClient, AsyncComputeClient, AsyncA2AClient
from ahp_state import StateAPIClient, IntentRecorder


async def run(service: str) -> None:
    def token() -> str:
        return Path(os.environ["AHP_TOKEN_FILE"]).read_text().strip()
    tls = ssl.create_default_context(cafile=os.environ["AHP_CA_FILE"])
    tls.load_cert_chain(os.environ["AHP_CERT_FILE"], os.environ["AHP_KEY_FILE"])
    async with httpx.AsyncClient(verify=tls, trust_env=False, timeout=120) as http:
        base = os.environ["AHP_API_URL"]
        # State's existing client is task-bound; construct this view for the current lease.
        # Applications may instead pass their runtime-supplied per-run IntentRecorder.
        state = StateAPIClient(base, token=token(), http_client=http)
        fence = {"run_id": os.environ["AHP_RUN_ID"], "worker": os.environ["AHP_WORKER"], "epoch": int(os.environ["AHP_EPOCH"])}
        recorder = IntentRecorder(state, fence, node=os.environ["AHP_NODE_TASK_ID"])
        call = os.environ["AHP_CALL_ID"]
        payload = json.loads(os.environ["AHP_REQUEST_JSON"])
        intent = await recorder.record(call, kind=service, target="public-owner", args=payload)
        current = await recorder.get(call)
        if current["status"] == "succeeded":
            print(json.dumps(current["result"]))
            return
        if current["status"] != "recorded":
            raise RuntimeError("existing intent must be reconciled; refusing redispatch")
        await recorder.dispatch(call)
        key = intent["id"]  # server-issued identity stays stable across restart
        kwargs = {"http_client": http}
        try:
            if service == "model":
                payload.pop("key", None)
                result = await AsyncModelClient(base, token, **kwargs).submit(key, **payload)
            elif service == "tools":
                result = await AsyncToolsClient(base, token, **kwargs).call(key, payload["tool"], payload["arguments"])
                if result["status"] != "SUCCEEDED":
                    raise RuntimeError("tool receipt unresolved; reconcile original operation")
            elif service == "compute":
                result = await AsyncComputeClient(base, token, **kwargs).run(payload["computer_id"], key, payload["command"], payload.get("cwd", ""))
                if result["status"] != "succeeded":
                    raise RuntimeError("command unresolved; attach/reconcile original operation")
            else:
                payload["message"]["messageId"] = key
                result = await AsyncA2AClient(base, token, **kwargs).send(payload)
        except BaseException:
            # Cancellation and lost acknowledgements never become a confirmed failure.
            await asyncio.shield(recorder.mark_unknown(call))
            raise
        await recorder.succeed(call, result)
        print(json.dumps(result))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service", choices=["model", "tools", "compute", "a2a"])
    args = parser.parse_args()
    asyncio.run(run(args.service))
