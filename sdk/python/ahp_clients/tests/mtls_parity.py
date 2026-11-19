"""One mTLS scenario, driven from Python; executed by the Go parity test.

    --mode helper      the supported way: mtls_context() -> verify=<SSLContext>
    --mode deprecated  the trap: verify=<ca path> plus cert=(crt, key)

Both modes call the SAME owner operation and print one JSON line; the Go test
decides what is acceptable, so this script cannot soften its own verdict.
"""
import argparse
import json
import sys

import httpx

from ahp_clients import ToolsClient, mtls_context


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--origin", required=True)
    parser.add_argument("--ca", required=True)
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    parser.add_argument("--mode", choices=("helper", "deprecated"), default="helper")
    args = parser.parse_args()

    if args.mode == "helper":
        client = httpx.Client(verify=mtls_context(args.ca, args.cert, args.key), trust_env=False, timeout=20)
    else:
        # Deprecated httpx form: create_ssl_context() returns before cert= is applied,
        # so the connection carries no client certificate.
        client = httpx.Client(verify=args.ca, cert=(args.cert, args.key), trust_env=False, timeout=20)

    try:
        receipt = ToolsClient(args.origin, lambda: "task-token", http_client=client).get("parity-1")
        print(json.dumps({"mode": args.mode, "status": "OK", "receipt": receipt}))
        return 0
    except Exception as error:  # noqa: BLE001 - the caller asserts on the classification
        print(json.dumps({"mode": args.mode, "status": "RAISED",
                          "error_type": type(error).__name__, "error": str(error)[:300]}))
        return 3


if __name__ == "__main__":
    sys.exit(main())
