# Agents Space + Core runtime

**Owners:** `internal/space` (Coordinator, Handler, StateClient), `internal/core`
(model/tool loop, compaction, skills), `cmd/as-runner` (single-deployment
binary), `web` (browser UI), `sdk/go/agent` (Go SDK), `examples` (runnable
journeys), `tests/space`.

The Space is a **stateless coordinator**: it owns turn lifecycle and orchestration
logic but persists nothing itself. All durability (threads, runs, leases,
intents, checkpoints, inbox, stream, KV) lives behind the **State API**
(`internal/state`, owned by another contractor). Space talks to State only over
HTTP and keeps **no Postgres connection of its own**.

## Architecture

```
browser UI (web)  ──┐
Go SDK (sdk/go)   ──┼──> as-runner ──> Handler (/v1/space/*)
other agent       ──┘                      │
                                     Coordinator ──> Core runtime (model/tool loop)
                                           │
                                     StateClient ──HTTP──> State API (/v1/state/*)
```

- **Handler** maps 12 Space operations to Coordinator methods. Scope is taken
  *only* from the authenticated principal (`transport.FromContext`), never from
  the request body. The `Auth` middleware in `HandlerConfig` injects a
  `transport.Principal` whose `Scope` becomes the turn's scope.
- **Coordinator** accepts turns, claims/leases runs via State, dispatches pending
  turns to Core in **FIFO** order (oldest pending first — the State queue guarantee,
  pinned by `TestCoordinatorDispatchesInFIFOOrder`), emits stream events, handles
  HITL/steering/cancellation/recovery, and writes the outbox.
- **Core runtime** is a real model/tool loop: it calls a `ModelClient`, executes
  `ToolRegistry` calls, runs the `Compactor` under a token budget, honours a
  bounded step count, and is cancellation/resume-aware. Skills
  (`SkillRegistry`) expose scope-visible tools. `ModelClient` has two real
  implementations: a scripted one used by the in-process tests, and
  `OpenAICompat` (`internal/core/openai_compat.go`) — a stdlib-only client that
  speaks `POST {base}/v1/chat/completions` and maps `tool_calls` /
  `finish_reason` / `usage`. It is configured from `AHP_LLM_BASE_URL` /
  `AHP_LLM_API_KEY` / `AHP_LLM_MODEL` and fails closed on an empty or
  non-`http(s)` base URL or empty model, never echoing secret values.

## Turn lifecycle

`pending → running → (steering) → done|failed|canceled`. Terminal states are
`done`, `failed`, `canceled`. A turn is executed under a State **lease**
(worker + epoch); the coordinator heartbeats the lease and always releases it,
even after cancellation (the release context is detached from the run context).

A `CancelTurn` for a turn that is already `done` or `failed` is refused with
`CONFLICT` (HTTP 409) and changes nothing: there is no work left to stop, and
reporting `canceled` would give the turn an outcome its run never had. The
refusal names the status it refused. A repeat `CancelTurn` of an already
`canceled` turn stays a success and writes no second terminal frame (its journal
key is the turn's), and a turn whose outcome this process cannot read
(`unknown`) stays cancellable — stopping a run of unknowable result is what a
cancel is for. When this process still believes a turn is running but State
refuses the cancel's release (`STALE_EPOCH`: somebody already closed the run),
Space reads the run row and adopts the status it finds rather than stamping
`canceled`, and answers `CONFLICT` naming that status; `GetTurn` and the run row
never disagree afterwards. An execution that is still winding down in this
process cannot restamp a turn that is already terminal either — its own verdict
(e.g. "failed: durable stream hole", which is what a killed execution of an
already-closed run produces) is logged as a disagreement, not written into the
status a reader sees.

A turn parked on a HITL gate is `awaiting_hitl`, and it keeps that name after a runner restart. The
durable fact is a State run row parked as `waiting` with `wait_for=hitl_response`; `GetTurn` reads
the reason for the wait and not only the status, so a reader that has nothing but State says what
the runner still holding the lease says. A run parked on any other wait target keeps `waiting`,
because Space did not create that wait and would be inventing a gate nobody asked for.
`awaiting_steering` has no durable marker at all (steering records an inbox event and never parks
the run), so a cold read of a steering turn reports `running` — a stated gap, not a guess.

A gate's own record is the thread's inbox: `SubmitHITL` writes the gate as event key `hitl:<id>`
(`hitl_request`) and parks the run on it, `RespondHITL` writes the decision as key
`hitl_response:<id>`. `GetHITL` reads those two events and nothing else, so a gate outlives the process
that created it. A gate keeps the FIRST decision it was given: a second, different answer is `CONFLICT`
naming the recorded one, because State keys the answer by the gate and refuses a different payload under
that key — but what the caller got was never an answer about the gate: on the real server State's
refusal was relayed as `INVALID_ARGUMENT` ("key reused with a different body"), and on a plane that does
not check the digest at all the answer was `{"ok":true}`. Neither is a decision an approval card can
read. Repeating the identical decision stays an idempotent success and writes nothing. An acknowledged
inbox event is not readable through State's published `ReadInbox`, so a gate whose request event a
consumer has already acknowledged reads as "no record here" — what this read checked, not a claim that
the gate never existed. `responded_at` stays empty for the same reason: the published inbox event
carries no instant, and an approval timestamp taken from the reader's clock would move on every refresh.

A gap this lane cannot close inside its own zone and therefore does not paper over: State wakes a
parked run on the event KIND, and every Space answer carries the same kind, so an answer naming a gate
nobody submitted still releases a run parked on a real, still-unanswered gate. Refusing that needs
either a per-gate wait target (which changes an event kind another runtime filters on) or an
acked-inclusive inbox read from State; both are contract decisions, asked on the forum.

An answer recorded before the run is parked is repaired, not waited for. `SubmitHITL` records the gate
first and parks the run second, and State wakes a parked run only while an inbox event is being
INSERTED. A decision that became durable between those two calls therefore woke nothing — and that is
the ordinary case for anyone who approves by watching the thread's inbox, because the gate event is
what publishes the gate's id — while the park that followed waited on a signal that had already been
and gone: a run parked forever with no deadline, its answer unacknowledged one call away, and no error
returned to anyone. Reordering does not fix this, it moves the permanent stall somewhere worse: with
the park first, a gate event that then fails to be written leaves the run parked on a gate nobody was
ever told about, and Space cannot undo a park (there is no unsuspend, and `Release` needs the lease
`Suspend` has already cleared). So the durable record is written first and the missed wake is repaired
FROM it: after parking, `SubmitHITL` reads the gate again and, when it already holds a decision and the
run is still parked on this wait target, inserts one further `hitl_response` event under key
`hitl_response:<id>:wake`. That event is a doorbell, not a verdict: the gate's own answer keeps its key
and remains the only decision event a reader of that key matches. The repair acts only on a decision
the record positively holds, never on an absence, and a repair that cannot be made is logged rather
than returned — the gate and the park are durable, and telling the executor that the gate was never
created would be the worse lie. A caller whose gate was already answered gets the recorded status
instead of `pending`, so it is not invited to wait for a decision that has already been taken. Two
residuals are stated rather than hidden: a decision acknowledged inside that window is invisible to the
published `ReadInbox` and leaves the run parked, which is the acked-inclusive read asked for above, and
a caller whose token may not read the thread's inbox gets an honest park plus a log line instead of a
repair.

A cancel cannot close a run this process parked on its own gate, and it now says so before touching
anything. `SubmitHITL` parks with `Suspend`, and `Suspend` hands the thread lease back, while every
durable close of a run is fenced by that lease and State publishes no operation that closes a waiting
run without one. A cancel that went through anyway appended no terminal frame (that append is fenced
too), got `STALE_EPOCH` for its release — a refusal this path tolerates by design — and then read the
row behind it, where the rule in force adopted only TERMINAL statuses: a row still saying `waiting` was
skipped, the turn was stamped `canceled` in memory, and the caller received `200` about a run that is
still the thread's first unfinished run, so every later turn of that thread queued behind it while the
canceled turn's terminal frame never became durable. Adoption that stops at terminal was the hole, not
the guard. What a refused close reads is now adopted WHOLE — `waiting` and `pending` included — mapped
through the same vocabulary the cold read uses (`spaceTurnStatus`), so the process still holding a turn
and the process that rebuilt it from State cannot disagree about what it is; a non-terminal adoption
corrects what a reader is told without stopping an execution this cancel did not get the right to
stop. Refusal is also the only answer that leaves the caller a way out, which is why the check happens
before the credential revoke: the gate is still open, so answering it releases the run and the same
cancel then closes it, whereas revoking first would both break this function's own rule that a refusal
mutates nothing and kill the very run whose park the caller was just told is stuck. The guard acts on
positive evidence of its own park only — `awaiting_hitl`, confirmed by a durable row saying `waiting`
with `wait_for=hitl_response` read under the caller's token — because a row that cannot be read is not
evidence of a park and a cancel is a security action, so that cancel proceeds. Stated residual, and it
belongs to the same contract ask as the per-gate wait target and the acked-inclusive read above: after
the gate is answered the run is `pending` with no lease, so Space cannot close it either. Lifting a
park (`unsuspend`, or a close of a waiting run that does not require the lease `Suspend` cleared) is a
contract decision, not something Space may invent here.

## Space API (`POST /v1/space/<Op>`)

| Op | Purpose |
| --- | --- |
| `AcceptTurn` | Submit a turn (idempotent by `key`); returns `turn_id`/`thread_id`/`status`. |
| `DispatchTurn` | Ask the deployment to claim and start the next pending run. |
| `CancelTurn` | Cancel an active turn (context cancel → `canceled`, not `failed`). Already `done`/`failed` → `CONFLICT`, nothing mutated; already `canceled` → idempotent success; a turn this process parked on its own gate → `CONFLICT`, nothing mutated, because the park left no lease with which to close the run. |
| `Steer` | Inject a steering instruction into a running turn. |
| `SubmitHITL` | Register a human-in-the-loop gate on a run. The turn reads `awaiting_hitl`, before and after a restart. A decision that became durable before the run was parked releases it instead of leaving it parked on a wake that already happened. |
| `RespondHITL` | Approve/reject a HITL gate. A gate keeps the first decision it was given: a second, different answer is `CONFLICT` naming it; repeating the identical decision is an idempotent success that writes nothing. |
| `GetHITL` | Read one gate as its durable record states it (`pending`/`approved`/`rejected` plus the recorded response). |
| `EmitOutbox` | Emit a durable outbox item for downstream delivery. |
| `SaveCheckpoint` | Persist turn checkpoint (see keying below). |
| `Recover` | Re-adopt turns whose lease went stale. |
| `GetTurn` | Read one turn snapshot. |
| `ListTurns` | List tracked turns. |
| `ReadStream` | Read durable stream events for a thread after a cursor. |

Every op is `POST` with a JSON body. Errors use the shared `contracts` error
codes (`not_found`, `conflict`, `stale_epoch`, `forbidden`, `invalid`,
`key_reuse`, `lease_lost`).

## Human download route (`GET /u-ahp/v1/artifacts/download`)

The browser-facing way to get a file. Frozen name and query form:
`forum-outbox/20261031T181611Z-space-ui-download-route-freeze.json`.

```
GET /u-ahp/v1/artifacts/download?artifact_id=<id>&version=<exact>
```

| Aspect | Behaviour |
| --- | --- |
| Authority | The human JWT is verified and exchanged for a scoped task token by the SAME middleware that guards `/v1/space/*`. The route carries that token onward, never the browser's credential. |
| Identity toward Artifacts | Space's own workload mTLS certificate (`AS_WORKLOAD_CERT/KEY/CA`) plus the exchanged bearer. |
| Version | Mandatory and EXACT. An empty/blank `version`, or `latest` in any case, is `400 INVALID_ARGUMENT` with `reason: exact_version_required` - before any call to Artifacts. |
| Rights | Decided by Artifacts when the grant is ISSUED (tenant, capability, conversation gate, label ceiling, lifecycle). Space re-decides nothing. |
| Bytes | Streamed: `Content-Type`/`Content-Length`/`Content-Range`/`ETag`/`Accept-Ranges` of the exact version pass through, `Range` is proxied, `Cache-Control: no-store` always. |
| Refusals | Artifacts' `code` AND `reason` are returned unchanged (`grant_expired` and `grant_used` stay two answers; a tombstone stays `404 artifact_deleted`, a legal hold `403 artifact_legal_hold`). No refusal collapses into `500`. |
| Never | Caches, buffers a whole artifact, writes an artifact, redirects, issues a presigned URL, or forwards the human JWT. |

Wiring: `internal/space/ui_download.go` (handler), `cmd/as-runner` (mount). The route is
mounted only when `AS_ARTIFACTS_URL` is set; setting it without a workload certificate
refuses to start the deployment rather than serving a route that could only answer by
reusing the caller's own credential. Optional `AS_ARTIFACT_GRANT_TTL` (Go duration) asks
Artifacts for a specific grant lifetime; unset takes Artifacts' default.

## State contract & keying (Space → State)

Space calls these State operations: `CreateThread`, `GetThread`, `CreateRun`,
`GetRun`, `ClaimRun`, `Heartbeat`, `Release`, `Suspend`, `RecordIntent`,
`UpdateIntent`, `SaveCheckpoint`, `LoadCheckpoint`, `WriteInbox`, `ReadInbox`,
`AckInbox`, `AppendStream`, `ReadStream`, `KVSet`, `KVGet`.

**Keying decision (this block):** checkpoints and stream events are keyed by
`thread_id`, not `run_id`, so a cold resume finds the latest checkpoint and a
reader follows one logical conversation across runs/restarts:

- `SaveCheckpoint` body carries `thread_id` (a checkpoint belongs to the thread).
- `LoadCheckpoint` reads by `thread_id`.
- `AppendStream` body carries **both** `thread_id` and `run_id`; events are
  stored/ordered under `thread_id` and read back by `thread_id`.
- `ReadStream` reads by `thread_id` + `after` cursor.

This is the cross-contractor contract raised to the State contractor on the
forum (see `forum/posts/`).

Space consumes the **official** State service, so the contract is exercised
against the real accepted server, never a stand-in. The integration tests build
a `space.StateClient` and drive it against a freshly booted `internal/stateapi`
(the accepted `461aa2ce` State API) over real PostgreSQL, real mTLS and real
Ed25519 task tokens (`tests/space/state_api_contract_test.go`,
`tests/space/durable_state_test.go`, build tag `state_integration`). These
tests assert the exact accepted wire shape — nested `{fence, …}` bodies for
fenced writes, `{events:[…]}` / `{checkpoints:[…]}` object envelopes for list
reads, strict decoding (unknown body field → `INVALID_ARGUMENT`), scope derived
only from the task token, per-subject SQL isolation, and fence/epoch fencing.
A `reboot` step stands up a brand-new pool + handler + listener over the same
database to model a real process restart (State holds no in-memory authority, so
this is exactly what a k8s restart does to it).

## Resume & streaming

On dispatch, if a turn already carries a checkpoint with messages, the Core
runtime seeds its conversation from the checkpoint and does **not** re-append the
original input. Every Core step is appended to the thread stream as an event
whose `kind` is the step type (`model`, `tool`, `compaction`, `error`, `done`);
after the run settles, a terminal `status` event (`done`/`failed`/`canceled`) is
emitted so readers close without polling. Stream writes are detached from the run
context so cancellation stays observable to readers.

## SDK (`sdk/go/agent`)

`NewClient(Config{BaseURL, Token, HTTP})` wraps one deployment. Methods mirror
the Space ops; `Delegate(ctx, targetClient, input)` performs agent-to-agent
delegation by accepting a fresh, independently-scoped turn on the target. See
`examples/two-agents` for the full client-side journey.

## Emil adapter (`adapters/emil`)

`adapters/emil` adapts the Emil `core-agent` (A2A JSON-RPC, adapted from the
approved read-only upstream `go-platform/.local/emil-core-src` @ `5ed238b4`) as a
**delegated third agent**. `emil.New(Config{BaseURL,...})` discovers the agent
card at `/.well-known/agent-card.json`, and `Delegate(ctx, text)` sends an A2A
task and polls `tasks/get`. It satisfies `core.ToolExecutor` (tool name
`emil.delegate`), so the Core runtime can delegate to Emil through the same tool
loop as any other tool.

`Delegate` returns a `Provenance` recording `AgentName`, `AgentVersion`,
`AgentCardSHA256` (digest of the exact bytes served), `TaskID`, `ContextID`,
`TaskState`, and whether an auth token was present (`AuthType`) — **never** the
token value. A live wire test (`EMIL_BASE_URL=... go test -tags live
./adapters/emil`) drives the real container: it asserts card discovery, task /
context assignment, a second `tasks/get` round-trip, and that no secret leaks
into provenance.

## Running

```sh
AS_LISTEN=:8080 AS_STATE_URL=http://localhost:8081 AS_WORKER_ID=runner-1 ./as-runner
```

Serves the API at `/v1/space/`, the UI at `/`, and `/healthz`. The UI is a real
client of `/v1/space/*` (two-subject browser flow, chat, stream, HITL approvals,
developer/admin log); it is not a mock.

## Verification (this block)

```sh
export GOCACHE=/tmp/go-cache
# Default gate (no external services required):
go test ./tests/space ./web ./internal/space/... ./internal/core \
  ./adapters/emil ./cmd/as-runner -race -count=1

# State contract against the REAL accepted State API over PostgreSQL (AHP_TEST_DATABASE_URL):
go test ./tests/space -tags state_integration -count=1
```

Covered in-process / over-HTTP (no Postgres): accept + idempotency,
dispatch→complete + stream markers, bounded steps, cancellation, HITL flow,
steering, outbox, recovery, checkpoint save, cold-resume-from-checkpoint, Core
tool loop + compaction, token compactor, skill registry + skill-backed loop,
two-deployment SDK delegation, the UI handler, the HTTP Space contract surface
(`/v1/space/*`, principal-scoped, routing, dispatch + stream), reliability
(stale-epoch fencing on checkpoint & stream, private-context isolation across
threads), and — under the `state_integration` tag — the **real** State API
contract: full lifecycle (create→claim→checkpoint→suspend→inbox→resume, one run,
higher epoch on requeue), strict-decode / missing-bearer / stale-epoch /
foreign-tenant / foreign-token-on-owner-cert security probes, durability across
a process restart, the UNKNOWN→`ReconcileIntent` gate, same-run crash/resume
`STALE_EPOCH` fencing, and per-subject private-context isolation.

### UI browser flow — proof and boundary
The two-subject browser flow is proven at the **real Space HTTP contract the
browser consumes**: `TestSpaceTwoSubjectBrowserFlow` / `TestSpaceAnonymousRejected`
drive `/v1/space/*` for two principals and assert scope isolation (cross-subject
`GetTurn`/`ReadStream`/`CancelTurn`/`Steer`/`RespondHITL` → 404, anonymous → 403),
and `TestSpaceHTTPDispatchAndStream` asserts the exact wire shape `web/static/app.js`
reads — the object envelope `{"events":[...]}` with per-event `kind`/`sequence`/
`payload` (the earlier client wrongly read a bare array and `ev.seq`). This is the
substantive behavioral proof behind the browser UI; it is not a literal headless
click-through (that additionally needs a standalone State server binary, which is
outside this block's owned `cmd/`). Byte-level artifact **download**, the connector
catalog and Computer tools are served by the execution/data contracts and remain
tracked acceptance requirements; the UI surfaces only artifact metadata + provenance
parsed from real `tool` stream events (display-only, no fabricated bytes). The
developer/admin panel additionally issues a real `/v1/space/GetTurn` snapshot and a
two-subject **isolation probe** that calls `GetTurn` against another subject's turn and
expects `NOT_FOUND` — exercising at the browser level the same scope boundary asserted at
the HTTP layer. `web/embed_test.go` (`TestEmbeddedClientExposesTurnAndIsolation`) locks
that the embedded client and shell wire up both calls against `/v1/space/*`.

Core `ModelClient` coverage: OpenAI-compat round-trip, auth header, fail-closed
and bounded provider-error paths, and a full `Runtime.Run` model/tool loop driven
over a real `httptest` OpenAI-compatible HTTP wire. Emil adapter coverage: agent
card parse + digest, JSON-RPC error mapping, `tasks/get`, auth passthrough +
redaction, completed-with-artifact, and failed/`MODEL_UNAVAILABLE`.

Live (tagged, gated on external availability — not part of the default gate):

- `EMIL_BASE_URL=... go test -tags live ./adapters/emil` drives the real Emil
  container over the wire.
- `go test -tags live_llm ./tests/space` is a Core-loop / wire test: it drives a real
  native-Qwen model round-trip through the OpenAI-compat client and the Core
  model→tool→provenance→download wiring on real model output, but the Emil tool is
  fronted by an in-process `httptest` Emil and State is supplied by `newMockState()`
  (test doubles, **not** the live services). It enforces the native-Qwen host + model
  allowlist and **fails, never skips**, when `AHP_LLM_*` is unset or points off the
  allowlist. It is **not** the whole-LIVE journey.
- `bash tests/space/run-live-real.sh` (`go test -tags live_real -race`) is the
  **whole-LIVE journey** and the real-e2e gate. **MANDATORY** path (fails closed if
  any is missing): real native-Qwen → **AHP's own product `save_artifact` tool**
  (Data/artifacts → Seal) writes the model's bytes to a **real versioned MinIO**
  object (sha256 PUT==GET, checked by `VersionID`) → durable State over real
  PostgreSQL → download → survives a runner **restart** → foreign-scope
  private-context `NOT_FOUND`. **Emil is an OPTIONAL provenance hop, NOT a gate**
  (`decisions/2026-10-30-emil-image-scope-correction.md`): when `EMIL_BASE_URL`
  points at a reachable real Emil `core-agent` the hop runs for real over A2A
  (no `httptest` stand-in) and is recorded (e.g. `core_artifact_save=false`, proving
  the seal is AHP-side); when unset/unreachable it is skipped-by-design and the
  mandatory journey still runs. The script no longer requires `EMIL_BASE_URL`.
  No `httptest`/`newMockState`/`:19081` stand-in appears on the proven path (the only
  `httptest` server is the product `internal/stateapi` under test over PG).
- `go test -tags state_integration ./tests/space` runs the Space wire + durability
  suite against the **real accepted PG State** (`internal/state` / `internal/stateapi`,
  byte-identical to accepted State `461aa2ce` in this tree) and a real PostgreSQL:
  create→claim(new epoch)→heartbeat→intents→checkpoint→inbox→stream→suspend(requeue)
  →reclaim(higher epoch)→release, plus durable-survives-restart, intent UNKNOWN
  reconcile, crash/resume STALE_EPOCH with old-writer refusal, and private-context
  isolation across subjects.
  It also runs `TestSDKTwoDeploymentsDelegate_RealState` (acceptance #4): two
  independent Space deployments drive the shipped Go SDK (`agent.NewClient`) against
  the production `space.Handler`+`Coordinator` over **real** State, each running a
  turn to done with its events read back from real PG, plus a **third-agent
  delegation** A→B and cross-tenant scope isolation (not the mock-peer contract test). It needs a PG DSN; on Docker-Desktop-for-macOS the host
  TCP path is NAT'd to the Docker gateway so pg_hba `trust` (loopback-only) does not
  apply and pgx authenticates via SCRAM. Reproduce with
  `eval "$(tests/space/pg-bootstrap.sh ahp_hive_space_product | grep '^export')"`
  (sets only `PGPASSFILE` + `AHP_TEST_DATABASE_URL`; the password stays in a mode-600
  passfile and is never printed; SCRAM means the plaintext never crosses the host
  boundary). It skips cleanly when neither `AHP_TEST_DATABASE_URL` nor `DATABASE_URL`
  is set.

## §3.2 six-component API contract (backend ownership)

PDF rev3.2 §3.2 names six Space components. Space exposes the authorized facade
(`POST /v1/space/<Op>`, human bearer verified in `internal/space/auth.go`; scope only
from the token, never a UI `?subject=`). UI talks ONLY through this facade; Space never
exposes raw State-admin privileges and never fakes a neighbour backend.

| Component | Owns (§3.2) | Depends on (AHP lane) | Space backend today |
| --- | --- | --- | --- |
| as-web | chat/stream/files/Computer/questions/approvals/connections UI | ahp-gateway, Artifacts, Compute, Tool&CredGW | chat+stream+approvals IMPLEMENTED (`AcceptTurn`/`DispatchTurn`/`CancelTurn`/`Steer`/`ReadStream`/`SubmitHITL`/`RespondHITL`) + SSE replay-cursor (`GET /v1/space/stream/{thread_id}?after` / `Last-Event-ID`); Human→scoped-Task boundary + strict body decode done (see below). files→Artifacts, Computer→Compute, connections→CredGW are other-lane APIs — facade proxy NOT built (do not fake) |
| as-workspace | projects, members, roles, invitations, chat list | Identity & Policy (IAM), State | durable chat list IMPLEMENTED (`ListChats` over State `ListThreads`, subject-isolated + restart-safe `GetThread` thread auth). projects/members/roles/invites = Identity&Policy APIs (NOT a Space store; facade must not fake them) |
| as-profile | Dot personal settings over tenant AgentConfig, personal skills+connections, Computer policy | Registry, Tool&CredGW | ALL upstream (Registry/CredGW); Space owns no profile store |
| as-admin | tenant policies, skill/connector-template catalog, model routes, allowed custom agents, agent-configurations | Registry, Identity & Policy, Model Gateway | ALL upstream; Space owns none |
| Run Coordinator | contexts/transcripts, turns+steps, inbox, leases/epochs, receipts, HITL, budget split | State API, Identity&Policy, Model GW | IMPLEMENTED (this package; see §3.3 mapping). Durable per-subject turn enumeration DONE: `ListRunHistory` over State `ListRuns` (restart-safe), durable chat list via `ListChats`/`ListThreads` |
| as-turn-runner | nothing durable (stateless Core Agent pool) | Model GW, Tool&CredGW, Memory, Artifacts, Compute | IMPLEMENTED (`internal/core.Runtime` + `cmd/as-runner`) |

### §3.3 turn protocol → implementation
- `AcceptTurn` → `AcceptTurn` (also pins AgentConfig revision+deployment version — see below).
- `AcquireLease`/`Heartbeat`/`Suspend` → `DispatchTurn` + the run's internal lease/keepalive; runner talks to State directly via `adapters/emil/runstore` + `internal/space.StateClient`.
- `CommitIntent`/`CommitStep` (intent receipts; `STALE_EPOCH`; `DISPATCHED` key; `SIDE_EFFECT_UNKNOWN`) → `StateClient.Record/Update/Get/ReconcileIntent` driven runner-side. The per-step durable key is canonical `space.StepKey(loopID=run_id, seq)` applied in the PRODUCTION Core loop (`internal/core/durability.go`), with replay (same key+same fingerprint → existing unchanged) and permanent reject (`ErrKeyReuse`) for a reused key with a different fingerprint.
- `ReadInbox` → `Steer`/`CancelTurn` + runner inbox read; `CreateHumanRequest` → `SubmitHITL`/`RespondHITL`; `CommitMessage`→event → durable stream (`AppendStream`), UI reads via `ReadStream`.

### AgentConfig pin (PDF rev3.2 L373/948/2399)
Space is the CARRIER (Registry owns revisions). `AcceptTurn` pins `AgentConfigRef{revision,
deployment_version}` into the durable run payload; it is carried to the runner via
`CoreRequest.AgentConfig` on hot dispatch and restored on cold resume
(`restoreTurnFromRunPayload`), so a mid-flight rollout cannot change an already-started task.

### Human→scoped-Task boundary (PDF rev3.2 §3.5)
The browser authenticates with a HUMAN JWT (`aud ahp-web`, from Identity). State
requires a short-lived scoped **Task** token (different `aud`) + Space workload mTLS;
the two audiences cannot be merged and the human JWT must NEVER reach State.
`internal/space/exchange.go` closes this with an explicit trusted server→server
operation:
- `ExchangeMiddleware` wraps the human-auth middleware; on a verified `HumanPrincipal`
  (subject only — the browser never chooses tenant/deployment) it calls a
  `TaskExchanger` (`IdentityGrantExchanger`: Space→Identity over Space workload mTLS;
  Identity holds the signer; tenant/deployment are SERVER-FIXED from Space config) and
  re-injects the request with `WithScopedTask`. `Handler.stateToken` uses ONLY that
  token; with `RequireScopedTask` a missing/unchanged human bearer is refused **403**
  (never forwarded). The browser is never handed a Task or a signing credential.
- **Strict body decode** (`readJSON` → `DisallowUnknownFields`): a browser cannot
  smuggle `tenant`/`subject`/`deployment`/`scope` into any request body — hard **400**,
  scope comes ONLY from the verified principal + the exchange (`strict_decode_test.go`).
- `whoami`/session/roles remain Identity's contract (Identity lane); Space does NOT
  mint or duplicate identity. as-profile/as-admin personal+tenant settings remain
  Registry/Identity writes; Space is the CARRIER (pins the resolved AgentConfig
  revision per turn), not their store.

### State API additions — status (as-workspace / durable turns)
- `ListThreads {cursor?,limit} → {threads:[...],next_cursor,has_more}` subject-isolated — DONE (State `ListThreads` accepted, integrated commit `7df96b34`). Space facade `POST /v1/space/ListChats` wires it; the real-State tier runs under `-tags state_integration,listthreads_integration` on the integrated tree so the standalone default suite stays green.
- `ListRuns {thread_id,cursor,limit} → {runs:[...],next_cursor}` subject-isolated — DONE. Space facade `POST /v1/space/ListRunHistory {thread_id}` reconstructs run history from State (restart-safe), NOT from the in-memory turn table, so a returning user after a process restart sees their history. Real-State tier covered (`list_run_history_realstate_test.go`).

## Remaining acceptance requirements (blocked on neighbours / access)

- **Whole-LIVE journey** (`run-live-real.sh`, tag `live_real`) is implemented and
  fail-closed, and was run green this session on real Mac infra: native-Qwen → AHP's
  own `save_artifact` tool → **real versioned MinIO** (sha256 PUT==GET) → **real
  PostgreSQL** State → runner **restart** → private-context `NOT_FOUND`; an OPTIONAL
  real Emil `core-agent` A2A hop runs when reachable and is skipped-by-design
  otherwise (Emil is **not** a gate). It remains **pending independent
  (author ≠ verifier) verification** at the composed gate; **not** self-declared
  accepted.
- **Built `as-runner` carries no stubs and a real model+executor** (VM-plane
  acceptance condition), verified on Mac at the binary level: `go tool nm` shows
  `go:itab.*core.OpenAICompatClient→core.ModelClient` (native-Qwen) and
  `go:itab.*adapters/emil.Adapter→core.ToolExecutor` (real A2A) with **0** matches for
  `stubModel|stubExecutor|echoTool|fakeSink|newMockState`; with `AHP_LLM_*` unset the
  binary **exits rc=1** (fail-closed, no silent stub). `cmd/as-runner/wiring_test.go`
  codifies the same as regression (fail-closed model, real client, real save_artifact,
  real Emil adapter, no MCP-secret leak). Residual = the director running `-race ./...`
  green on the VM (Mac disk/egress limits, not bypassed).
- **Official PG State** is now accepted (`461aa2ce`) and this tree carries it
  byte-identically (no overlay / no replace / no go.work); the durability + contract
  suite above runs against it on real PostgreSQL via `state_integration`. Two-subject
  and two-deployment persistence over that wire are covered there.
- Connectors / files / Computer surfaces in the UI populate only once those APIs
  exist; they are labelled as such in the UI, not faked.

These are stated as open, not closed by fixtures.
