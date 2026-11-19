# Public AHP clients: Python and Go

These clients call AHP owner APIs with a current task token and caller-supplied
mTLS transport. They do not hold provider, database, Vault or storage credentials.
Python `ahp_clients` requires only `httpx`; no agent framework is imported.
The product authority is architecture rev3.2 §4.2; the mounted Go handlers define
wire details. Agent Space is not a dependency.

Python exports sync and async variants of `ModelClient`, `ToolsClient`,
`ComputeClient`, `A2AClient`, and the separately constructed `ApprovalClient`.
Go packages are `sdk/go/model`, `sdk/go/tools`, `sdk/go/compute`, `sdk/go/a2a`.
Construct one client per HTTPS owner origin and pass its workload transport.
The supplied HTTP pool stays caller-owned. Token sources are consulted on every
request; a stale token is never cached by these clients. Callers must close
streams/generators when abandoning iteration.

## mTLS transport (Python: build the context, do not pass `cert=`)

The owner faces authenticate the workload by its client certificate, so a client
that presents none is refused inside the TLS handshake — long before any token is
read. In Python, build the context with `ahp_clients.mtls_context(ca, cert, key)`
and pass it as httpx's `verify`:

```python
import httpx
from ahp_clients import mtls_context

client = httpx.Client(verify=mtls_context("ca.pem", "client.pem", "client.key"),
                      trust_env=False)
```

The shorter-looking form is broken and fails silently:
`httpx.Client(verify="ca.pem", cert=("client.pem", "client.key"))`. In httpx 0.28
`create_ssl_context()` returns as soon as `verify` is a string, so the deprecated
`cert=` argument is never loaded; the connection then carries no client
certificate and the front answers with `TLSV13_ALERT_CERTIFICATE_REQUIRED`
(`tls: client didn't provide a certificate` in the front's log, measured 2026-11-09
against the product's own TLS configuration and lane certificate material).
Nothing about TLS 1.3, OpenSSL or the platform requires relaxing mTLS: with the
context above the same Python interpreter negotiates TLS 1.3 and is accepted with
its workload identity verified. `TestMTLSParityGoAndPythonOnOneFront`
(`tests/sdk/mtls_parity_test.go`) runs both languages against one front configured
like the product and pins the broken construction as refused.

## Operation identity, errors and recovery

Every mutation that has a key in its owner contract requires the caller's stable
key. Keys are never generated implicitly. A2A uses the message's `messageId` as
the durable send identity; its JSON-RPC `id` only correlates responses.

The executable examples under `sdk/examples/public-clients` use an existing
claimed State lease and `IntentRecorder`. They use the persisted `Intent.ID`
as the downstream key. The runtime supplies the durable node/task ID and
persisted call identity. A logical graph node name alone is insufficient for a
loop; a newly generated model tool-call ID is insufficient after restart.
A repeated intent verifies its fingerprint and reads its stored status before
dispatch. An existing `dispatched`/`unknown` intent requires reconciliation.
The examples are intentionally conservative on errors and never turn an uncertain
effect into a confirmed failure. A2A send success means durable acceptance, not
that the delegated task completed. `Call`/`Get`/`Fence` stay a content-free
receipt; the raw stored result of one successful operation is read separately
with `ToolResult` / `tool_result`, which dispatches nothing and releases content
only to the authority that paid for the effect.

No client retries a mutation automatically. A lost acknowledgement can mean the
owner committed an effect. Go `Error.OutcomeUnknown` / Python
`APIError.outcome_unknown` preserves that distinction. Use Model Submit with the
same request/key, Tools Get with the original key, Compute GetCommand/AttachCommand,
or A2A Get/Resubscribe. Do not replace an uncertain key with a new one.

Platform errors preserve HTTP status, `code`, `reason` and a bounded message.
In particular, the current owner sends `code=INVALID_ARGUMENT` and
`reason=KEY_REUSE_MISMATCH`; clients do not rewrite that server contract.
Go errors support `contracts.Code`, `contracts.ReasonOf`, and context cancellation
through `errors.Is`. Python async cancellation propagates `CancelledError`.
Server `SIDE_EFFECT_UNKNOWN` is returned verbatim. Tools Fence leaves an already
unknown effect unknown; it does not invent a successful or safely absent effect.

Responses and individual stream records are bounded to 16 MiB; redirects are
refused. Model streams are NDJSON (`accepted`, `delta`, `heartbeat`, `terminal`),
not SSE. Model stream closure does not cancel the durable generation: use Cancel.
Resume explicitly using the identical request/key and `after=last_sequence`.
Identity/cursor gaps and EOF before terminal are errors. A2A uses SSE and replays
a durable task snapshot/history on Resubscribe; its endpoint has no cursor.

## Parity and verification

The generated method-level surface comparison (Go `go doc` vs Python `def` scan,
per family, with which side lacks which method) lives in
[`parity-api.md`](parity-api.md); the table below is the acceptance/evidence view.

`TestPublicSDKOwnerAPIsRealPostgres` (`cmd/ahp/sdk_public_clients_test.go`) runs
real PostgreSQL, Identity grants, TLS client certificates and owner handlers;
its `PythonActualOwnerAPI` subtest launches Python over the same actual sockets.
Model inference is a controlled provider double, MCP uses the real protocol
with a local tool fixture, and physical Compute is explicitly mocked. Compute
lifecycle checks use real Redis and files/processes in a throwaway environment
prefix. These are not production LLM, external MCP or microVM isolation claims.

In the table, **both** means the Go method and Python sync/async surface exist;
`live` names the composition test exercising the owner path. A method marked
`not exercised` is implemented and compiles, but is not claimed accepted by this
SDK test. The same Python method name is available on both sync/async classes;
not every operation is repeated in both scheduling modes.

| Family | Go / Python public method | Owner path | Evidence / boundary |
|---|---|---|---|
| Model | Submit / submit | `/v1/model/SubmitModel` | live ModelSubmitReplayAndStreamingRecovery + Python; exact-key replay, lost response |
| Model | Get / get | GetModel | live Model + Python + authority denials |
| Model | Stream / stream | StreamModel | live Model + Python sync/async; cursor resume, no duplicate inference |
| Model | Cancel / cancel | CancelModel | live Model + Python; durable cancellation acknowledgement |
| Tools | Call / call | `/v1/tools/Call` | live ToolsJournalUnknownAndHumanDenial + Python; receipt only |
| Tools | Get / get | Get | live Tools + Python; lost-response reconciliation |
| Tools | Fence / fence | Fence | live Tools + Python; UNKNOWN preserved |
| Tools | ConnectionState / connection_state | ConnectionState | live Python |
| Tools | ToolResult / tool_result | `/v1/tools/GetResult` | live ToolsResultReadUnderOwnerAuthority + Python; successful provenance-bearing row only, artifact bytes verified by digest (real external MCP and the artifact sink are fixtures) |
| Human Tools | Approve / approve | Approve | implemented; positive human approval not exercised here |
| Human Tools | Approvals / approvals | Approvals | live Go/Python task-token denial; no implicit consent |
| A2A | Send / send | `/a2a/internal`, SendMessage | live A2AStreamingAndCooperativeCancel + Python |
| A2A | Delegate / delegate | SendMessage + parent/reserve metadata | live A2ADelegateReplayGetResubscribe + Python; server derives grant lineage |
| A2A | Get / get | GetTask | live both + authority denials |
| A2A | Cancel / cancel | CancelTask | live both; terminal-task refusal retained |
| A2A | SendStream / send_stream | SendStreamingMessage | live Go; Python decoder shares Resubscribe transport |
| A2A | Resubscribe / resubscribe | SubscribeToTask | live both including Python async |
| Compute | CreateComputer / create_computer | `/v1/compute/CreateComputer` | live ComputeLedgerFenceAndAttach + Python; replay |
| Compute | GetComputer / get_computer | GetComputer | live both + authority denials |
| Compute | AcquireIncarnation / acquire_incarnation | AcquireIncarnation | live both; physical backend mocked |
| Compute | StopIncarnation / stop_incarnation | StopIncarnation | both; not exercised |
| Compute | Destroy / destroy | Destroy | both; not exercised |
| Compute | Exec / exec | Exec | live both; durable ledger acceptance only |
| Compute | Run / run | Run | both; positive command dispatch not exercised; mock backend cannot execute |
| Compute | MarkDispatched / mark_dispatched | MarkDispatched | live both |
| Compute | Complete, CompleteWithOutput / complete | Complete | both; truncation flag preserved; not exercised |
| Compute | GetCommand / get_command | GetCommand | live both through AttachCommand |
| Compute | AttachCommand / attach_command | polling GetCommand | live both; status/output sequence preserved; **not a PTY stream** |
| Compute | Fence / fence | Fence | live both; `side_effect_unknown` retained |
| Compute | Cancel / cancel | Cancel | both; not exercised |
| Compute | Restart / restart | Restart | both; explicit only, never automatic; not exercised |
| Compute | CommitWorkspace / commit_workspace | CommitWorkspace | both; not exercised |
| Compute | WorkspaceHead / workspace_head | WorkspaceHead | both; not exercised |
| Compute | EnsureEnvironment / ensure_environment | EnsureEnvironment | live both |
| Compute | BeginInstall, BeginInstallDetails / begin_install | BeginInstall | live Go full runner response; old Go method preserves revision-only compatibility |
| Compute | FinishInstall / finish_install | FinishInstall | live Go |
| Compute | CurrentEnvironment / current_environment | CurrentEnvironment | both; not exercised |
| Compute | Snapshot / snapshot | Snapshot | both; not exercised |
| Compute | Export / export | Export | mandatory `key`; real Go/Python sync+async export, replay and exact bytes verified; quiescent source required |
| Compute | AcquireLease / acquire_lease | AcquireLease | both; not exercised |
| Compute | ReleaseLease / release_lease | ReleaseLease | both; not exercised |
| Compute | CurrentFence / current_fence | CurrentFence | both; not exercised |
| Compute | TypedPatch / typed_patch | TypedPatch | live both; real file/hash, stale hash denial |
| Compute | StateHead / state_head | StateHead | live both |
| Compute | StateRevisions / state_revisions | StateRevisions | live both |
| Compute | DeriveBranch / derive_branch | DeriveBranch | both; not exercised |
| Compute | MergeBranch / merge_branch | MergeBranch | both; not exercised |
| Compute | Sleep / sleep | Sleep | live both; Redis lease blocks sleep |
| Compute | Wake / wake | Wake | live both; physical backend mocked |
| Compute | CostReport / cost_report | CostReport | live both |
| Compute | TakeWriterLease / take_writer_lease | TakeWriterLease | both; not exercised directly; writer lease used by TypedPatch/install |
| Compute | ReleaseWriterLease / release_writer_lease | ReleaseWriterLease | both; not exercised directly |
| Compute | AcquireActivityLease / acquire_activity_lease | AcquireActivityLease | live both with Redis |
| Compute | ReleaseActivityLease / release_activity_lease | ReleaseActivityLease | live both with Redis |
| Compute | ActivityLeases / activity_leases | ActivityLeases | live both with Redis |
| Compute | RunInstall / run_install | RunInstall | live Go, actual process in temporary prefix |
| Compute | ListEnvironment / list_environment | ListEnvironment | live both, actual file digests |
| Compute | PublishVersion / publish_version | PublishVersion | both; not exercised |
| Compute | DrainVersion / drain_version | DrainVersion | both; not exercised |
| Compute | RollbackVersion / rollback_version | RollbackVersion | both; not exercised |
| Compute | GetActiveVersion / get_active_version | GetActiveVersion | both; not exercised |

## Confirmed platform gaps (open)

1. **Tool output retrieval.** `internal/tools/service.go` stores the MCP result in
   `operations.Operation.Result`. `internal/tools/toolsapi/toolsapi.go:view`
   deliberately excludes it. No result/ArtifactRef read endpoint is mounted.
   Journal scope contains tenant/deployment/subject, but no immutable source
   task/context/label snapshot (`internal/operations/operations.go:Operation`).
   OAuth marks the dispatch context private; that does not transfer taint to
   another context reading a saved result. A safe GetResult needs atomic
   completion plus immutable provenance/labels, current read/connection authority,
   and label binding before content is returned. Pending/UNKNOWN/failed must not
   become empty success. The ledger limit is 1 MiB; oversized results fail and
   no artifact spill is currently wired. No GetResult endpoint was opened here.
   `ToolsClient` is usable for receipts/reconciliation, **not yet a complete MCP
   result client**.
2. **Compute export.** Integrated with the existing Artifacts BeginUpload/PUT/Seal
   owner contract. Public signatures are `Export(ctx, computerID, path, key)` /
   `export(computer_id, path, key)`. A deployment-wide durable request binding
   prevents changing subject, computer, task or normalized path under one key.
   The approved PoC requires a quiescent source; running commands/PTYs/install
   or an uncertain execution outcome refuse export. This is a bounded private
   copy under owner lifecycle guards, not a hypervisor snapshot. The combined
   test exercises Go→Python sync→async replay without another PUT, actual
   artifact bytes/range and current cross-subject/revoked denial. The older
   fixture deliberately lacks `artifacts` capability and now asserts403
   FORBIDDEN; it is not the positive export proof. `sealed_at` is omitted when
   the owner does not attest a timestamp. See implementation-pitfalls.md §13.

3. **Browser/PTY.** `internal/compute/computeapi/api.go` mounts no browser or PTY
   session endpoint. Activity lease kind `browser` is coordination, not a browser
   client. AttachCommand is explicitly durable polling, not interactive I/O.
4. **Model additional administrative surfaces.** This block covers Submit/Get/
   Stream/Cancel. Reserve/provider-file/tree-budget administration is not exposed
   as additional public methods here. No client directly selects provider secrets.

## Running the examples and checks

Both programs require an existing active State lease and its task authority:
`AHP_API_URL`, `AHP_TOKEN_FILE`, `AHP_CA_FILE`, `AHP_CERT_FILE`, `AHP_KEY_FILE`,
`AHP_RUN_ID`, `AHP_WORKER`, `AHP_EPOCH`, `AHP_NODE_TASK_ID`, `AHP_CALL_ID`, and
`AHP_REQUEST_JSON`. Token/certificate values are read from files, never printed.
The Python State recorder is the existing per-run token-bound client; it does
not claim automatic token refresh. New owner clients read the token file afresh.

```sh
# AHP_REQUEST_JSON='{"messages":[{"role":"user","content":"Hello"}]}'
go run ./sdk/examples/public-clients/go -service model
PYTHONPATH=sdk/python python3 sdk/examples/public-clients/python.py model
# tools: {"tool":"registered-alias","arguments":{...}}
# compute: {"computer_id":"...","command":"...","cwd":""}
# a2a: {"message":{"role":"ROLE_USER","parts":[{"text":"..."}]}}
```

Private acceptance uses `../run-tests.py --lane sdk-public-clients`; private
credentials and logs remain outside Git. The evidence report records exact
candidate hashes, negative intermediate runs and final checked commands.

Review correction: a valid A2A `TASK_STATE_INPUT_REQUIRED` ends the current
interaction without making the task terminal. Clients close the subscription at
that boundary and preserve the returned task/status event; `GetTask` and a later
message continue to address the durable task. A stream ending while `WORKING`
is still UNKNOWN. Malformed successful stream replies and HTTP 5xx preserve
post-dispatch uncertainty; explicit HTTP 4xx refusals remain refusals. This also
applies to non-JSON Compute 5xx responses. No automatic replay is added.
