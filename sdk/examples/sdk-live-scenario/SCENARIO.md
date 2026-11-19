# SDK-live scenario (joint with Boyle) — one contract, two independent legs

Contract of record: `private/pdf-closure-20261102/evidence/sdk-live/scenario.json`
(`ahp-sdk-live-scenario/v1`, author Boyle, 47 steps P1–P3 / R1–R12 / T1–T12 /
M1–M5 / A1–A5 / ME1–ME5 / S1–S5). Its sha256 is recorded inside every verdict; a
leg that read different bytes is a different scenario and is rejected by
`parity_check.py`.

## What lives here

| file | role |
|---|---|
| `go/main.go` | independent Go leg — public `sdk/go/*` clients only |
| `python.py` | independent Python leg — public `ahp_clients` / `ahp_memory` / `ahp_state` only |
| `parity_check.py` | compares the two verdicts (contract sha, step agreement, run independence) |

Boyle's `evidence/sdk-live/run.sh` + `sdklive_go`/`sdklive_py` is the harness of
record for the pinned stand. These two drivers are a SECOND implementation of the
same contract, so a green combined report is not one person's code agreeing with
itself.

## Contract rules implemented verbatim

* unset `prereq_env` → `SKIP_PREREQ`, never `PASS`;
* `expect_by_env`: first matching `VAR=value` key wins, `default` last;
* expected `code` is a substring match; expected `http` accepts all three shapes
  used in the file (`[{http:[200],code:""}]`, bare `[200,201]`, single object);
* `fail_expected: true` → the step PASSES when the platform refuses (HTTP ≥ 400);
* `labels` / `note` copied verbatim into the verdict.

Status vocabulary: `PASS`, `FAIL`, `SKIP_PREREQ`, `SKIP_NOT_IMPLEMENTED`.
`SKIP_PREREQ` is also used for a driver-side prerequisite the contract does not
name (recorded with the reason, never as PASS).

## Implemented in these two legs (rest is SKIP_NOT_IMPLEMENTED)

`P1` cert → CN/OU, `P2` login → `/v1/identity/control/exchange` → scoped task
token, `A1` BeginUpload+Upload+Seal, `A2` download + sha256, `A3` range slice,
`A4` range past end refused, `A5` foreign read denied, `T7` replay same key,
`T9` `ToolResult` incl. artifact-ref bytes. Memory (ME*) and state (S1–S4) are
not implemented here and are reported as such — Boyle's legs cover them.

## Human byte-read leg (`proposed_steps`, NOT counted as contract steps)

Root asked whether a human can read the bytes the product sealed. Verdict from
code at `9d23732f`: the route EXISTS and is mounted —

1. `GET /u-ahp/v1/artifacts/Resolve?sha256=<digest>` → exact sealed version
   (`internal/space/ui_resolve.go:38`);
2. `GET /u-ahp/v1/artifacts/download?artifact_id=…&version=…` → bytes,
   `Cache-Control: no-store` (`internal/space/ui_download.go:47`);
3. `version=latest` is refused with `exact_version_required`
   (`internal/space/ui_download.go:62`).

Both routes are mounted by **as-runner** (`cmd/as-runner/main.go:517`,
`cmd/as-runner/main.go:520`) and forwarded by the single-origin edge
(`cmd/ahp/single_origin.go:206`) with path/query preserved, so a browser needs a
human session bearer only — no workload certificate. Auth: `ExchangeMiddleware`,
no human JWT is forwarded upstream.

What genuinely does NOT exist: a human/UI route to the tools result ROW —
`ToolsPlane.AllowedOps` (`internal/space/ui_planes.go`) has no `GetResult`; the
result row is task-facing by design and the PDF does not require a UI route for
it. H1–H3 are therefore proposed additions to the shared contract, emitted in a
separate `proposed_steps` section so they never inflate the 47 contract steps.
A stand without as-runner fails H1–H3 for stand reasons, not product reasons.

## Environment variables (names only; values never appear in argv, logs or reports)

Contract-owned: `AHP_SCENARIO_TOOL`, `AHP_SCENARIO_FOREIGN_TLS_CLIENT_CERT`,
`AHP_SCENARIO_FOREIGN_TLS_CLIENT_KEY`, `AHP_SCENARIO_AGENT_ID`,
`AHP_SCENARIO_AGENT_IMAGE`, `AHP_SCENARIO_AGENT_VERSION_1`,
`AHP_SCENARIO_AGENT_VERSION_2`, `AHP_SCENARIO_EVAL_PASSED`,
`AHP_SCENARIO_EVAL_PASSED_V2`, `AHP_SCENARIO_INVOKE`,
`AHP_SCENARIO_APPROVAL_TTL_SHORT`, `AHP_EXEC_MAX_TOKENS`.

Harness-owned (`run.sh`): `AHP_SCENARIO_REV`, `AHP_ORIGIN`, `AHP_TLS_CA`,
`AHP_TLS_CLIENT_CERT`, `AHP_TLS_CLIENT_KEY`, `AHP_ACCEPTANCE_USERS_FILE` (0600,
`[{username,password,tenant,subject}]`), `AHP_SCENARIO_REPO`,
`AHP_SCENARIO_VENV`, `AHP_SCENARIO_OUT_DIR`,
`AHP_SCENARIO_FOREIGN_USERS_FILE` / `AHP_SCENARIO_FOREIGN_USER_INDEX`
(the foreign task token for A5 — minted, not assumed), `AHP_SCENARIO_CAPABILITIES`,
`AHP_SCENARIO_TASK`, `AHP_SCENARIO_RUN_ID`, `AHP_TEST_USER_INDEX`.

These drivers: `AHP_SCENARIO_CONTRACT` (required), `AHP_API_URL`, `AHP_CA_FILE`,
`AHP_CERT_FILE`, `AHP_KEY_FILE`, `AHP_SDK_STAMP` (label; the run identity gets a
per-process suffix so the two legs cannot share the tools idempotency key),
`AHP_HUMAN_USER` / `AHP_HUMAN_PASSWORD` / `AHP_HUMAN_TOKEN_FILE`,
`AHP_TASK_ID` / `AHP_TASK_CONTEXT` / `AHP_TASK_CAPABILITIES` /
`AHP_TASK_TOKEN_FILE`, `AHP_ARTIFACTS_API_URL`, `AHP_TOOL_ARGUMENTS_JSON`,
`AHP_VERDICT_OUT`, `AHP_SCENARIO_FOREIGN_TASK_TOKEN_FILE`.

## Running

One pass over both SDKs (harness of record, after the director pins a stand):

```
AHP_SCENARIO_REV=<pinned-sha> bash \
  private/pdf-closure-20261102/evidence/sdk-live/run.sh          # --dry-run for a no-network check
```

These two legs, against the same stand:

```
cd sdk/examples/sdk-live-scenario/go && go build -o /tmp/sdklive-hegel . && \
  AHP_SCENARIO_CONTRACT=<scenario.json> AHP_API_URL=… AHP_CA_FILE=… AHP_CERT_FILE=… \
  AHP_KEY_FILE=… AHP_SDK_STAMP=<label> AHP_VERDICT_OUT=/tmp/go.json /tmp/sdklive-hegel
AHP_SCENARIO_CONTRACT=<scenario.json> AHP_API_URL=… AHP_CA_FILE=… AHP_SDK_STAMP=<label> \
  AHP_VERDICT_OUT=/tmp/py.json python3 python.py
python3 parity_check.py /tmp/go.json /tmp/py.json   # 0 PASS, 1 FAIL, 3 no effects yet
```

## Boundaries (state these with any green report)

* nothing here has been executed against a live stand yet — the numbers on file
  come from a no-network dry pass (47 steps: 39 `SKIP_PREREQ`, 8
  `SKIP_NOT_IMPLEMENTED`, 0 `FAIL`, identical in both languages);
* expected codes are only meaningful on the pinned stand the contract was written
  for ("live HERE ONLY");
* credentials come from a 0600 file or private env only; reports carry sha256
  fingerprints of tokens, never the tokens.
