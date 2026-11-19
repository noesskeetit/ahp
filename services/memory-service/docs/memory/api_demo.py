#!/usr/bin/env python3
"""Synthetic real-embedding demo through a deployed AHP Memory mTLS API."""
import json
import os
from pathlib import Path
import ssl
import urllib.error
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def main():
    context = ssl.create_default_context(cafile=os.environ["AHP_MEMORY_DEMO_CA"])
    context.load_cert_chain(os.environ["AHP_MEMORY_DEMO_CERT"], os.environ["AHP_MEMORY_DEMO_KEY"])
    base = os.environ["AHP_MEMORY_DEMO_URL"].rstrip("/")
    opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=context))
    alice = Path(os.environ["AHP_MEMORY_DEMO_TOKEN_FILE"]).read_text().strip()
    bob = Path(os.environ["AHP_MEMORY_DEMO_OTHER_TOKEN_FILE"]).read_text().strip()
    operation = "memory-semantic-demo-" + uuid.uuid4().hex

    def call(name, body, token):
        request = urllib.request.Request(
            base + "/v1/memory/" + name,
            data=json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        )
        try:
            with opener.open(request, timeout=60) as response:
                return response.status, json.load(response)
        except urllib.error.HTTPError as error:
            return error.code, {}

    status, proposed = call("Propose", {"kind": "user", "text": "The physician prescribed a remedy for the illness.", "op_key": operation}, alice)
    if status != 200:
        raise RuntimeError(f"Propose status={status}")
    memory_id = proposed["memory"]["id"]
    try:
        status, _ = call("Review", {"id": memory_id, "decision": "approve", "op_key": operation + "-review"}, alice)
        if status != 200:
            raise RuntimeError(f"Review status={status}")
        status, result = call("Search", {"query": "doctor medication disease", "kind": "user", "limit": 1}, alice)
        if status != 200 or result.get("mode") != "hybrid" or not result.get("hits") or result["hits"][0]["memory"]["id"] != memory_id:
            raise RuntimeError(f"semantic top-1 failed, status={status}")
        other_status, _ = call("Read", {"id": memory_id}, bob)
        if other_status not in (403, 404):
            raise RuntimeError(f"cross-user read status={other_status}")
        print(json.dumps({"semantic_top_1": True, "mode": result["mode"], "cross_user_status": other_status, "memory_id": memory_id}))
    finally:
        status, _ = call("Delete", {"id": memory_id, "op_key": operation + "-delete"}, alice)
        if status != 200:
            raise RuntimeError(f"synthetic cleanup failed, status={status}, id={memory_id}")


if __name__ == "__main__":
    main()
