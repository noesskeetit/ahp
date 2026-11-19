"""Read back the stored result of ONE tool operation under the caller's authority.

A read only: it never dispatches, never fences, and never turns a refusal into
an empty result. Same environment as the Go twin (``main.go``):
AHP_API_URL, AHP_CA_FILE, AHP_CERT_FILE, AHP_KEY_FILE, AHP_TOKEN_FILE,
AHP_OPERATION_KEY, optional AHP_ARTIFACTS_API_URL for an artifact-stored result.
"""
import json
import os
import sys

import httpx

from ahp_clients import (APIError, ArtifactsClient, ToolsClient, fetch_result_bytes,
                         mtls_context)


def main():
    # The context must carry the client certificate: httpx drops a `cert=` argument
    # given together with a string `verify`, and the front then refuses the
    # handshake for want of a workload certificate.
    context = mtls_context(os.environ["AHP_CA_FILE"], os.environ["AHP_CERT_FILE"],
                           os.environ["AHP_KEY_FILE"])
    token = lambda: open(os.environ["AHP_TOKEN_FILE"], encoding="utf-8").read().strip()
    base = os.environ["AHP_API_URL"]
    with httpx.Client(verify=context, trust_env=False, timeout=60) as http:
        client = ToolsClient(base, token, http_client=http)
        try:
            result = client.tool_result(os.environ["AHP_OPERATION_KEY"])
        except APIError as refusal:
            # CONFLICT: not settled (or never will be) - poll Get.
            # NOT_FOUND: absent OR someone else's; existence is not disclosed.
            # FORBIDDEN: this reader is not the authority that paid for the effect.
            print(f"result read refused with {refusal.code}: {refusal}", file=sys.stderr)
            return 1
        if result["result_storage"] == "inline":
            print(json.dumps(result))
            return 0
        # The bytes live in the Artifacts owner and are fetched under THIS
        # authority; fetch_result_bytes verifies the declared digest first.
        store = ArtifactsClient(os.environ.get("AHP_ARTIFACTS_API_URL", base), token, http_client=http)
        body = fetch_result_bytes(store, result)
        ref = result["artifact_ref"]
        print(json.dumps({"artifact_id": ref["artifact_id"], "bytes": len(body), "digest": ref["digest"]}))
        return 0


if __name__ == "__main__":
    sys.exit(main())
