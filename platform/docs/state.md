# Durable State API and SDK

The State block owns the platform's durable agent state: threads, runs, lease
fencing, opaque checkpoints, pending writes, intents, an ordered stream, a
transactional batch endpoint, a control plane (delete / legal-hold /
export-import), and SDKs that drive all of it over HTTP. Storage is PostgreSQL;
the public surface is the HTTP facade in `internal/stateapi`.

`ClaimRun` accepts optional `run_id` and `payload_type` selectors. The latter
matches the top-level `payload.type` string (at most 128 UTF-8 bytes). Selection
still starts with each thread's earliest unfinished run: filtering never lets a
later run overtake it. Tenant and deployment come exclusively from workload
mTLS. Empty selectors retain the unfiltered API. The Dots coordinator requests
`payload_type: "turn"`, so its orphan sweep cannot consume custom SDK jobs.
Go exposes `ClaimRunWithFilter(..., ClaimFilter{PayloadType: "turn"})`; Python
exposes `claim_run(..., payload_type="turn")` and the same keyword on
`claim_run_for`. Existing claim calls and their wire bodies remain compatible.

## Storage layout

All State data lives in the `ahp_state` schema across thirteen tables:

- `threads` — one row per thread, carries the monotonic lease `epoch`.
- `runs` — runs belonging to a thread, with status and lease holder.
- `checkpoints` — checkpoint metadata (id, namespace, version, digest, lineage).
- `checkpoint_parts` — ordered byte parts of a checkpoint payload.
- `checkpoint_blobs` — content-addressed blobs shared across parts.
- `checkpoint_writes` — namespace-explicit pending writes.
- `kv` — key/value items scoped to a thread namespace, with CAS version.
- `kv_ops` — idempotency journal for KV mutations.
- `intents` — external-effect intents and their reconciliation state.
- `id_operations` — idempotency keys for create operations.
- `inbox_events` — per-thread inbox for suspend/resume delivery.
- `stream_events` — append-only ordered stream per thread.
- `legalhold` — scope/thread legal-hold markers blocking destructive control.

Run status is one of `pending`, `running`, `waiting`, `done`, `failed`,
`canceled`; `done`/`failed`/`canceled` are terminal.

## HTTP surface

Every call is `POST /v1/state/{Operation}` with a strict JSON body. The
transport accepts a non-`POST`, an unexpected path prefix, or an unknown
operation name only as a not-found error, and rejects unknown JSON keys.

The operation catalog has 36 entries:

- Lifecycle: `CreateThread`, `GetThread`, `GetThreadByContext`, `CreateRun`,
  `GetRun`, `ListRuns`, `Heartbeat`, `Release`, `Suspend`.
- Lease acquisition: `ClaimRun` (workload-only, see below).
- Checkpoints: `PutCheckpoint`, `GetCheckpoint`, `ListCheckpoints`, `GetPart`.
- Pending writes: `PutWrites`, `GetWrites` (both namespace-explicit).
- Key/value: `PutKV`, `DeleteKV`, `GetKV`, `SearchKV`.
- Intents: `RecordIntent`, `UpdateIntent`, `ReconcileIntent`, `GetIntent`.
- Inbox: `Enqueue`, `ReadInbox`, `AckInbox`.
- Stream: `AppendStream`, `ReadStream`.
- Batch: `PutBatch`.
- Control plane: `DeleteThread`, `DeleteSubject`, `SetLegalhold`,
  `RemoveLegalhold`, `ExportThread`, `ImportThread`.

## Identity and authorization (non-negotiable)

- The isolation scope is taken **only** from the authenticated request context
  (`transport.FromContext`). Request bodies never carry `scope`, `tenant`,
  `subject`, or `deployment`; strict JSON rejects those keys.
- `ClaimRun` is the **only** workload route: it uses `transport.Workload()`
  (mutual TLS, deployment-only, no bearer token) because the worker has not
  been issued a task token yet.
- Every other operation uses `transport.Task` — mutual TLS **plus** a bound
  task token, validated against the fixed audience `ahp` and capability
  `state`, then authorized through the injected `transport.Authorize`.
- `auth.Subject` on a task call is always the **original** subject, never the
  task id.
- A granted lease (`ClaimRun`) is returned only together with a freshly issued
  task token from the injected `TaskIssuer`. If issuing is denied, the lease is
  released as canceled and the denial is returned instead.

`stateapi.New` requires the store, verifier, authorizer, and issuer; a nil in
any position fails closed at construction. Requests are body-size bounded.

## Lease, fencing, FIFO, suspend/resume

Claim is FIFO per thread: candidate runs are ordered by enqueue sequence, and
exactly one concurrent claimant wins a given run (verified under 100-way
concurrency). A successful claim advances the thread `epoch`, so every lease
carries a monotonic `Fence{run_id, worker, epoch}`. Heartbeat renews the lease
under the current epoch; a stale epoch cannot act on the run, which is how a
takeover of a dead worker's lease is fenced out. Suspend parks a run, deliveries
accumulate in the per-thread inbox, and resume replays them in order.

## Checkpoints, writes, and ordering

Checkpoints are opaque bytes, content-addressed by digest, versioned, and
scoped by namespace. Large payloads are split into ordered parts with
content-addressed blobs. A reused key whose payload digest differs returns
`ErrKeyReuse`. Pending writes are namespace-explicit on both read and write,
including the LangGraph root namespace (empty string). `intents` record an
effect before it is attempted; an `UNKNOWN` outcome is never auto-replayed and
must be reconciled explicitly. `stream_events` give a per-thread ordered log.

## Key/value

`kv` items are scoped to a thread namespace and carry a CAS version, so a
conditional update admits exactly one winner under concurrency. `kv_ops` makes
each mutation idempotent by key, so a replay returns the original result rather
than applying twice, and the same namespace can be shared across threads.

## Atomic batch and crash safety

`PutBatch` applies a checkpoint, its parts, pending writes, a stream append,
and a run-status transition in a single PostgreSQL transaction under the run
lease (`internal/state/batch.go`). The batch is all-or-nothing: a process crash
or restart mid-operation leaves either every effect or none, so durable state
and the run status can never diverge from the outbox of stream events.

## Control plane

`internal/state/control.go` provides the operator-facing surface with no agent
hooks involved: `DeleteThread` cascades a thread's rows within its scope and
refuses (`ErrForbidden`) when a legal-hold applies; `DeleteSubject` removes a
subject's threads while skipping held ones; `SetLegalhold`/`RemoveLegalhold`
cover scope-level and thread-level holds; `ExportThread`/`ImportThread` move a
thread (checkpoints, writes, kv, stream, intents) as a portable JSON export
across scopes. All control work is scope-cascaded and runs without invoking
agent code.

## Durable thread listing (ListThreads)

`ListThreads` is the durable, scoped enumeration of the caller's threads — the
State-plane backing for Agents Space chat/project listing (PDF §3.2: as-web chat
surface reads the list, as-workspace presents it, Run Coordinator owns the
transcripts). It is a read task-route (`POST /v1/state/ListThreads`), so its
isolation scope is the verified principal exactly like `GetThread`/`ListRuns`.

Request (strict JSON; any scope/tenant/subject/deployment key is rejected):

```json
{ "cursor": "<opaque, empty for first page>", "limit": 0 }
```

`limit <= 0` selects the server default (50); it is capped at 200.

Response:

```json
{ "threads": [ { "id", "context_id", "scope", "epoch", "created_at" }, ... ],
  "next_cursor": "<opaque, empty when exhausted>", "has_more": true }
```

Semantics (PDF §2.5 guarantees, enforced):

- Scope from token, never from the request. The isolation scope
  (tenant, deployment, subject) is `transport.FromContext`; a task token only
  ever enumerates the threads of its own scope. Threads in a foreign scope do
  not exist for the caller and can never be listed or probed, even with a forged
  cursor.
- Stable deterministic keyset. Order is `(created_at DESC, id DESC)` — a total
  order over durable rows. `cursor` is an opaque `(created_at, id)` bound; the
  next page is every in-scope thread sorting strictly after it. Keyset is stable
  under concurrent insert and delete: already-returned rows never change key, so
  a page is a key bound, not a mutable offset — a thread created mid-pagination
  may enter the front but can never duplicate or skip an unseen row.
- Deletion vs retention follows Lifecycle. A `DeleteThread`/`DeleteSubject`
  removes the row, so the thread is absent from the list; a thread whose deletion
  is blocked by legal hold was never removed, so it is still listed (retained =
  present, deleted = absent). No soft-delete flag is invented.
- Real indexed query. The keyset is served by the composite index
  `threads_scope_order_idx` on `(tenant, deployment, subject, created_at DESC,
  id DESC)`, so scope filter + ordering + cursor bound are an index scan.

Go SDK: `(*Client).ListThreads(ctx, cursor string, limit int) ([]Thread, string,
bool, error)` returns the page, the next cursor (`""` when exhausted) and a
`has_more` flag. It presents only `{cursor, limit}`; identity is token-derived.

## SDKs

**Go** (`sdk/go/state`) is the typed client over the HTTP surface: thread/run
lifecycle, claim/heartbeat/release, checkpoints, writes, KV, intents, inbox,
stream, and batch, with mTLS and bound task-token plumbing supplied by
`pkg/transport`.

**Python** (`sdk/python/ahp_state`) adapts LangGraph onto State over HTTP:
`AHPCheckpointSaver` implements `BaseCheckpointSaver` and `AHPStore` implements
`BaseStore`, so real `interrupt`/`resume` and pending writes round-trip through
the State API. The native LangGraph `PostgresSaver` is **not** used; all durable
state goes through HTTP. Pinned dependency versions are recorded in
`sdk/python/requirements.txt` and `sdk/python/ahp_state/requirements.txt`.

## Acceptance

`tests/state/run_acceptance.sh` is the single command that builds the
out-of-process serve binary, vets and runs the Go unit + integration suites
(real PostgreSQL, real mTLS `httptest` server mounting the production
`stateapi` handler, real Ed25519 task tokens), runs the Python adapter tests
against the serve binary, and prints a report. It validates that the target
database matches the allowlisted isolated database before touching PostgreSQL.
The Go `tests/state` suite covers the lifecycle/FIFO/claim/heartbeat/takeover
path, checkpoint and KV semantics, intents with no-auto-replay, the batch
crash/restart invariant, control-plane cascade/legal-hold/export-import, and
negative isolation/revocation cases. It also covers durable scoped `ListThreads`: two-subject and cross-scope
isolation, keyset pagination stable under concurrent insert/delete, deleted vs
legal-held retention, foreign/forged cursor containment, body-scope rejection, and
the backing index being present.

### Known product-side gap (outside this block)

`cmd/ahp` startup currently invokes only `Store.Migrate`; it does not invoke the
full control-plane migration. This is outside the State block's owned paths and
is flagged for the architect rather than patched here.
