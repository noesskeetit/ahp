# AHP Compact State API

The service runs the compact State API on PostgreSQL 18 and a private S3-compatible bucket. It serves the same journal protocol to native adapters and custom Go and Python agents.

## Run

Provide a database, an existing bucket dedicated to the deployment, a workload CA, a TLS server key pair, and a trusted Ed25519 JWT issuer. Grant the State API identity read, write, list, and delete access to its S3 prefix including object versions if bucket versioning is enabled. Inject secrets into the service environment.

From the repository root:

```sh
go run ./cmd/state-api
```

| Environment | Purpose |
| --- | --- |
| `AHP_STATE_DSN` | PostgreSQL connection configuration |
| `AHP_STATE_ADDR` | Bind address, default `127.0.0.1:8443` |
| `AHP_STATE_TLS_CERT`, `AHP_STATE_TLS_KEY`, `AHP_STATE_CLIENT_CA` | Server TLS material and trusted workload CA |
| `AHP_STATE_TOKEN_ISSUER`, `AHP_STATE_TOKEN_KID`, `AHP_STATE_TOKEN_PUBLIC_KEY` | JWT trust settings and path to an Ed25519 public key PEM |
| `AHP_STATE_GRANTS_FILE` | Optional path to locally authorized scope and grant records |
| `AHP_STATE_S3_ENDPOINT`, `AHP_STATE_S3_BUCKET` | S3 API host with optional port and existing private bucket |
| `AHP_STATE_S3_ACCESS_KEY`, `AHP_STATE_S3_SECRET_KEY` | S3 credentials from the service's secret source |
| `AHP_STATE_S3_TLS` | S3 TLS toggle, default `true` |
| `AHP_STATE_RETENTION`, `AHP_STATE_STAGING_TTL` | Deleted-session retention and orphan-upload age, defaults 30 days and one hour |
| `AHP_STATE_TOOL_LEASE_KEY`, `AHP_STATE_TOOL_LEASE_ISSUER`, `AHP_STATE_TOOL_LEASE_KID` | Optional dedicated signing authority for fenced Tool Gateway requests |

The State API verifies the client certificate and a task JWT bound to that workload.
Tasks use the `ahp` audience. The issuer supplies the explicit signed capability
list as `"cap":["state"]`. Additional permissions require `state.shared` or
`state.retention` in that list. An unrecognized grant ID does not confer a
capability by itself.

With `AHP_STATE_GRANTS_FILE` set, the local file is the authority. Replace the
file atomically. The process watches for changes and reloads on SIGHUP. Invalid
replacements deny access until a valid policy is loaded. This fixture grants
access to a single subject.

```json
[
  {"tenant_id":"tenant-1","deployment_id":"agent-1","subject_id":"user-1",
   "grant_id":"agent-state","capabilities":["state","state.shared"]}
]
```

The compact migration is embedded in the service and serializes concurrent bootstrap attempts. It creates six logical tables in `ahp_journal`, each with four hash partitions. The service leaves the legacy State schema outside its startup path. Authenticated `GET /readyz` checks PostgreSQL and the bucket over the provisioned service connections.

## Inside a session

- `sessions` holds ownership, lease epoch, durable input position, and current checkpoint and journal pointers. Workers claim queued work through the API. The sweeper finds expired claims across tenants in PostgreSQL, returns them to the queue, and keeps their original input.
- `session_events` holds the ordered journal and stable append receipts. Adapters can include text for search and record visibility changes in the same transaction as the operation that caused them.
- `checkpoints` and `checkpoint_writes` hold typed snapshots and pending graph writes. The adapter owns the codec and reconstructs the native state. A list with `include_tuples=true` returns snapshots and their pending writes together. The `metadata` filter accepts a JSON object.
- `scoped_kv` holds session, user, app, and per-user framework store keys. A deletion keeps its CAS revision so delayed updates cannot silently recreate a key. `POST /v1/state/kv:mutate` commits a fenced CAS batch and a private session receipt without a journal event. `expected_seq` is optional. Batch reads accept repeated `key` query parameters.
- `tool_intents` records external action admission and its outcome. A prepared action can receive at most one direct-dispatch permission. A replacement worker treats unresolved work as unknown and blocks automatic duplicate execution. After an uncertain admission response, SDKs retry with `ack_lost` to record uncertainty in the same epoch. The recovery request never authorizes dispatch.

Clients preserve stable mutation keys and supply the current lease epoch. The Go SDK lives at `sdk/go/state/journal`. The Python SDK exposes the native LangGraph checkpoint saver and BaseStore and provides shared blob and standalone tool helpers. The two JavaScript-based adapters use the TypeScript-facing client in `sdk/ts/ahp-state`.

Small values stay inline up to 256 KiB. The direct S3 stage route supports values up to 4 MiB and returns a content-addressed reference scoped to the session. A short, durable staging mark protects reused S3 objects until their staging period ends. The upload runs outside the PostgreSQL transaction. A producer rechecks its lease after upload. Shared KV values stay inline. Reads verify the stored size and digest. Session deletion fences the old worker immediately. The reaper preserves held sessions and revisits incomplete cleanup. An authorized operator grant with `state.retention` can set and release a hold before cleanup begins.

## Platform origin

With `AHP_ENABLE_COMPACT_STATE=1`, the ordinary AHP process serves the native
session API at the same mTLS origin as the other platform services. The local
full-system launcher enables it. The all-in-one mode uses live Identity grants
including revocation. The standalone state role checks explicit signed task
capabilities.

The native dispatch flow is enqueue, claim, heartbeat, framework writes, then
a terminal append with `finish_turn`. Native adapters keep queue control
records out of their framework history while tracking the durable cursor.
Go callers can use `FinishClaimedTurn` with a prepared payload. A retry must
reuse that payload and the original input position.

The legacy RPC client, administration, and older deployments can still use
the State RPC routes. The native routes do not transfer existing runs or
legacy queue history.

## Verify

Use a disposable database named with the `ahp_carbon_compact` prefix. The journal integration tests also accept a local disposable MinIO instance through `AHP_JOURNAL_TEST_S3`, `AHP_JOURNAL_TEST_S3_ACCESS`, and `AHP_JOURNAL_TEST_S3_SECRET`.

```sh
go vet ./cmd/state-api ./internal/statejournal ./sdk/go/state/journal
go test -race -count=1 ./cmd/state-api ./internal/statejournal ./sdk/go/state/journal
```

Run the native retry and crash probes from `profiling/state-journal` against the service with a workload certificate and task token. The handoff check uses a confirmed tool result and a fresh receiving process with its own native checkpoint. Keep evidence for native resume and external-call reconciliation separate.
