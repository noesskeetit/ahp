# Reading a stored tool result (Go and Python, identical contract)

`main.go` and `python.py` do the same thing: read the result of ONE operation
key back from the Tool Gateway owner, under the caller's current authority.

```sh
AHP_API_URL=... AHP_CA_FILE=... AHP_CERT_FILE=... AHP_KEY_FILE=... \
AHP_TOKEN_FILE=... AHP_OPERATION_KEY=... \
go run ./sdk/examples/tool-result
PYTHONPATH=sdk/python python3 sdk/examples/tool-result/python.py
```

What the two programs refuse to do, on purpose:

- A result read never dispatches. Only a `SUCCEEDED`, provenance-bearing row
  answers; everything else stays an error the caller must act on:
  `CONFLICT` (pending, uncertain, failed, stale — poll `Get`),
  `NOT_FOUND` (absent **or** belonging to another tenant/deployment/subject),
  `FORBIDDEN` (no verified context, foreign or missing provenance, another
  context of the same subject, revoked grant, non-task bearer, or a reconciled
  row whose provenance nobody may invent).
- An artifact-stored result is fetched from the Artifacts owner under the same
  authority and its declared digest is verified before the bytes are used. The
  result read grants no extra storage capability: a revoke between the two calls
  fails closed at the Artifacts owner.

`sdk/python/tests/test_tools_result_contract.py` measures this contract against a
recording owner double (categories, no retry, digest verification); the real
owner leg over PostgreSQL, Identity and mTLS runs from
`cmd/ahp/sdk_public_clients_test.go` (`ToolsResultReadUnderOwnerAuthority` plus
`sdk/python/ahp_clients/tests/live_contract.py`).
