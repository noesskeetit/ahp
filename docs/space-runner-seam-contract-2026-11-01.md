# Space ↔ Runner seam contract (2026-11-01)

Owner: `vm2-state-runner` (State / Run Coordinator / runner).
Requested by: hive-deputy, `20261101T095855Z` §2 п.3.
Audience: `vm2-a2a-multinode` (block 7), `vm2-sdk-langgraph` (block 4/6), `vm2-frontend` (block 1/2).

Purpose: publish the **exact** seam names — routes, methods, fields, codes, durability rules — so
consumers do not have to read `internal/space` or `internal/core`. This document **describes the
existing seam**; it changes no semantics. Every line here is derived from the source at commit
`4c7ea24` (base `debf8b1`) and each entry carries a `file:line` so it can be re-verified instead of
trusted.

**One later revision is marked where it appears:** the `CancelTurn` response field
`terminal_frame_durable` (and its `note`) exists only after `4c7ea24` — added 2026-11-01 for the
measured silent journal failure (forum `20261101T104800Z` §1 item 4, pinned by
`tests/space/cancel_terminal_frame_reported_test.go`). A second revision is §4.1: the
`RunBinding.MaxSteps` / `MaxStepsStated` pair and the rule that Space persists an unstated
`max_steps` as unstated (director assignment `20261101T113922Z` row 4, contract requested by
`vm2-a2a-multinode` `20261101T115659Z` §2). Everything else here is still `4c7ea24`.

> **Root correction, restated because it is the whole point of п.3:** the first agent calls a
> **second deployment as the original subject**. It does *not* "assign a task to a second employee"
> and it does *not* change identity. The minted task token carries the turn's subject **unchanged**
> (§4). A second human user is a separate, independent subject **only** for isolation testing (§7) —
> that is a test fixture, not the delegation path.

---

## 1. Space edge (what block 1/2 may call)

One route shape for everything: `POST /v1/space/{Operation}` with a CamelCase operation name.
`internal/space/handler.go:49-64` is the authoritative ops map.

| Operation | Request body | Notes |
|---|---|---|
| `AcceptTurn` | `AcceptTurnRequest` | creates the turn; `types.go:256` |
| `DispatchTurn` | no body — dispatches accepted turns under the caller's scope | `handler.go:125` |
| `CancelTurn` | `{"turn_id": …}` | **`{ok, terminal_frame_durable, note?}`** — revokes this run's child-grant subtree and closes the durable stream (§6); `terminal_frame_durable:false` means the cancel happened and the answer says the transcript did not get its terminal frame |
| `Steer` | `SteerRequest{turn_id,instruction}` | `types.go:323` |
| `SubmitHITL` | `HITLSubmitRequest{turn_id,kind,prompt,deadline?}` + `run_id` | `types.go:300` |
| `RespondHITL` | `HITLRespondRequest{hitl_id,approved,response?}` + `thread_id` | `types.go:308` |
| `EmitOutbox` | `OutboxEmitRequest{turn_id,key,kind,payload}` | `types.go:315` |
| `SaveCheckpoint` | checkpoint body | `types.go:177` |
| `Recover` | no body — worker-global sweep | response is filtered to the caller's scope, `handler.go:293-296` |
| `GetTurn` | `{"turn_id": …}` | |
| `ListTurns` | thread body | |
| `ListRunHistory` | `{"thread_id": …}` | |
| `ListChats` | `{cursor,limit}` | |
| `DeleteChat` | `{"thread_id": …}` | deletes the caller's own chat; refused `409` while one of its runs is still `pending/running/waiting`, `403` under legalhold, `404` for a foreign or absent thread (§1.1) |
| `ReadStream` | `{thread_id,after,limit}` | JSON projection of the same frames as SSE |

The **only** GET on this edge is the SSE tail:
`GET /v1/space/stream/{thread_id}` — `internal/space/handler.go:71`. PDF rev 3.2 requires an SSE
replay cursor here, not a websocket.

Hard rules for consumers:

- **Subject/scope never travels in a body.** Tenant/subject/deployment are derived server-side from
  the verified human token plus the server-owned exchange (§2). Bodies decode strictly: an unknown
  field is a rejection, not something to ignore.
- `HITLStatus` values are exactly `pending` / `approved` / `rejected` / `expired` (`types.go:205-212`).
- The per-run token is `json:"-"`: it is **never** present in any HTTP or SSE response. Do not build
  a client that expects to read it back.

### 1.1 `DeleteChat` - deleting a chat (block 8)

`POST /v1/space/DeleteChat` `{"thread_id": ...}` -> `200 {"deleted":true,"thread_id":...}`.
Path: `internal/space/handler.go` (`deleteChat`) -> `Coordinator.DeleteChat` ->
`StateClient.DeleteThread` -> the published State op `DeleteThread`.

| Situation | Answer | Who decides |
|---|---|---|
| caller owns the chat, every run terminal | `200`, thread gone | State (scope-filtered `DELETE` + `ON DELETE CASCADE`) |
| some run of the chat is `pending` / `running` / `waiting` | `409 CONFLICT` | `Coordinator.DeleteChat`, reading **durable** runs |
| thread under legalhold | `403 FORBIDDEN` | State (`legalhold` table) |
| another subject's thread, or no such thread | `404 NOT_FOUND` | `authorizeThread`, then State again |

- **The `409` is a security property, not a UX rule.** The cascade deletes the run's intent
  journal, and that journal is the only barrier keeping a re-delivered run from repeating an
  external effect (`RecordIntent`/`UpdateIntent`/`ReconcileIntent`). Deleting under a live run
  destroys the evidence its own retry needs, so the caller must `CancelTurn` first: cancel
  revokes the run's credential and closes the row durably, and then the same delete succeeds.
- **`404`, never `403`, on a foreign thread** - the same no-leak rule as every other thread op, so
  the route cannot be used to discover whether another subject owns an id.
- **The liveness read is durable on purpose.** It asks State for the thread's runs rather than
  trusting the in-memory turn table, so a row still marked `running` after a crash blocks the
  delete instead of being quietly erased together with the history a re-claim needs.
- **Lost ACK.** A transport failure here is not a refusal. Retry the same `thread_id`: the second
  attempt answers `404`, which is how the caller learns the delete really is durable.
- **Not covered by this route:** data a run wrote under the conversation's `context_id` on other
  planes. Only State's own thread-scoped rows cascade today.

## 2. Human → task: how a normal user run gets a task scope

| Symbol | Location |
|---|---|
| `space.IdentityExchangePath = "/v1/identity/control/exchange"` | `internal/space/exchange.go:24` |
| `space.TaskExchanger.Exchange(ctx, humanPrincipal, task) (taskToken, attenuatedTask, error)` | `exchange.go:45` |
| `space.ExchangeMiddleware(exchanger, auth)` | `exchange.go:104` |
| `space.ScopedTaskFromContext(ctx) (string, bool)` | `exchange.go:93` |
| `space.WithAuthorityTask` / `space.AuthorityTaskFromContext` | `exchange.go:84,87` |

A human access token carries **no Deployment** and is therefore never valid on the execution plane.
The middleware exchanges it for a task token bound to the turn's scope; downstream code reads the
task via `ScopedTaskFromContext` and must fail closed when it is absent.

## 3. Per-run grant: every run executes on its own child grant

| Symbol | Location |
|---|---|
| `space.RunGrantSpec{Task,Context,Capabilities,TTL,MaxActive,MaxFanout,MaxDepth,AllowSelf}` | `internal/space/rungrant.go:50` |
| `space.RunGrantIssuer.Delegate(ctx, parentToken, parentTask, spec) (childToken, error)` | `rungrant.go:70` |
| `space.RunGrantIssuer.Cancel(ctx, childToken, childTask) error` | `rungrant.go` (roots/{childTask}/cancel) |
| Identity routes: `POST /v1/identity/roots` · `/v1/identity/roots/{task}/delegate` · `/v1/identity/roots/{task}/cancel` | `internal/identityapi/handlers.go:118,126` |

`parentTask` must be the **attenuated** task from §2 — never a broad authority, never a browser
value. The issuer is fail-closed by contract: a failed `Delegate` never yields the parent token, and
a run never falls back to the shared ingress credential. `roots/{task}/cancel` cancels that child
**and its subtree**; `control/cancel-subtree` is the separate operator kill switch.

## 4. Calling a SECOND deployment — the delegation path as it exists

This is the answer to "how does a user run reach deploy #2". The subject does not change.

1. The coordinator holds an **internal task signer**; it is the only party allowed to mint a task
   delegation, so a browser caller can never escalate into task scope
   (`internal/space/mint.go:15-22`, forum directive d71a7b0e (B)).
2. `Coordinator.MintTaskToken(scope contracts.Scope, req TaskTokenRequest) (string, error)` —
   `mint.go:46`.
   `TaskTokenRequest{Task,Root,Context,Grant,Depth,TTL}` — `mint.go:23`. `Task` may be empty
   (coordinator generates); `Root`, `Context`, `Grant` are required; `Depth >= 0`.
3. **Scope completeness is the "original subject" mechanism.** `scope` must carry
   `Tenant` + `Deployment` + `Subject`, and the `Subject` is the turn's own subject, copied
   unchanged. `MintTaskToken` fails closed: missing task signer → `FORBIDDEN`; incomplete scope or
   missing root/context/grant or negative depth → `INVALID` (`mint.go:47-58`). There is no
   parameter by which a second subject could be substituted.
4. The workload is then reached at the gateway's public A2A path:
   `gateway.DefaultPublicPath = "/a2a/public"` (`internal/gateway/server.go:16`), with
   `DefaultInternalPath = "/a2a/internal"` (`:17`) reserved for internal traffic. The public path is
   the one that accepts a minted task token.
5. Core reaches it as an ordinary tool, so it inherits the runner's whole mutation-journal contract
   (§5) for free:

| Symbol | Location |
|---|---|
| `emil.ToolName = "emil.delegate"` (the Core tool name) | `adapters/emil/adapter.go:17` |
| `emil.New(emil.Config{BaseURL,HTTPClient,Bearer,CardCacheTTL}) (*Adapter, error)` | `adapter.go:102` |
| `(*Adapter).Delegate(ctx, prompt) (*Result, *Provenance, error)` | `adapter.go:152` |
| `(*Adapter).Execute(ctx, name, args) (json.RawMessage, error)` | `adapter.go:230` |

`emil.New` requires `BaseURL` (fail-closed, `adapter.go:106`). `CardCacheTTL` caches the Agent Card
across delegates; zero disables the cache.

**Consequence for block 7:** a second-deploy call is one journalled mutation of the calling run.
Its idempotency key, its `dispatched`/`unknown` handling and its reconciliation come from §5 — the
second deploy must not invent its own retry-on-lost-ACK.

### 4.1 The step budget a caller stated is carried by `RunBinding`, not by the adapter

A delegation must say which step budget it forwards, because the execution plane journals
`step_budget_source` and "8 asked, 4 effective" is otherwise invisible after the fact
(`internal/gateway/budget.go`). The number is therefore part of the binding the runtime already
injects into every tool call (`core.WithRunBinding`, `internal/core/runtime.go`):

| Field | Meaning |
|---|---|
| `RunBinding.MaxSteps` | the per-attempt budget the **caller** stated for this run; `0` when none was stated |
| `RunBinding.MaxStepsStated` | whether a caller stated one at all — the only way to tell a stated `64` from an unstated budget |

Rules, in both directions:

* `MaxSteps` is the caller's number and nothing else. The loop's own fallback bound (64 steps,
  `internal/core/runtime.go`) is a fact about that loop's life, is applied to the loop, and is
  **not** forwarded: an unstated budget forwards nothing, and the child's own deployment
  default answers it (`step_budget_source=deployment`).
* Space stops the laundering upstream instead of re-deriving it: `AcceptTurn` persists the
  accepted `max_steps` as accepted (`internal/space/coordinator.go`), so a run whose caller said
  nothing keeps saying so after a cold restart. Consequence for the published shape:
  `Turn.max_steps` is what the caller stated — `0` means nobody stated a budget, not "the run
  may take zero steps".
* Measurement: `internal/core/run_binding_budget_test.go` (stated / unstated / unchanged
  mid-run) and `tests/space/step_budget_statedness_test.go` (hot and cold dispatch keep the
  statedness).
* Until this field existed the number reached a delegating tool through a context stamp owned
  by vm2-a2a-multinode (`adapters/emil/step_budget.go`), which is marked as removable once
  `RunBinding` carries the budget natively. That switch is the adapter owner's, not this
  document's.

## 5. State seam (runner-side): leases and the mutation journal

Client methods on `*space.StateClient`, `internal/space/stateclient.go`:

| Method | Line | Contract note |
|---|---|---|
| `CreateThread` | :126 | |
| `CreateRun` | :147 | |
| `ClaimRun` | :170 | `token=""` — authorized by certificate; returns `{Lease:{Run,Worker,Epoch}, TaskToken}` |
| `Heartbeat` | :182 | renews the lease |
| `Release` | :191 | |
| `Suspend` | :200 | |
| `RecordIntent` | :215 | journals an effect **before** it is attempted |
| `UpdateIntent` | :237 | advances the row's status |
| `GetIntent` | :257 | |
| `ReconcileIntent` | :272 | the only exit from `unknown`; requires the exact digest |
| `PutCheckpoint` | :297 | / `ListCheckpoints` :328 |
| `AppendStream` | :421 | / `ReadStream` :436 |

`space.Fence{RunID,Worker,Epoch}` is the lease proof carried on every write. Codes a consumer must
handle: `NOT_FOUND`, `CONFLICT`, `FORBIDDEN`, `INVALID_ARGUMENT`, `KEY_REUSE_MISMATCH`,
`STALE_EPOCH`.

Intent statuses are exactly `recorded` / `dispatched` / `succeeded` / `failed` / `unknown` /
`fenced` (`internal/state/intents.go:16`). Consumer rules:

- A **key** is `(run_id, node, call)`; `call` is a **position in the run's effect sequence**, not a
  per-process counter and not a model tool-call id. `sdk.OperationKey(runID,nodeID,callIndex)` is the
  published form. A model re-mints its tool-call id on every physical attempt, so keying on it makes
  replay detection impossible.
- `recorded` = never dispatched → the effect may be performed. `dispatched`/`unknown`/`fenced` =
  the effect may or may not have reached the world → **never auto-replayed**, reconcile or don't.
- `unknown` is reachable **only** through `ReconcileIntent` with the stored digest. A wrong digest
  is refused.
- A repeat of an already-applied transition returns the stored row; it is not a second effect. That
  is what makes re-sending an identical write safe when only its ACK was lost.

## 6. Durable stream and step keys

| Symbol | Location | Rule |
|---|---|---|
| `space.StepKey(loopID, step)` | `internal/space/durability.go:27` | keys a step body by **logical position**, not by this life's counter |
| `space.CancelKey(turnID)` | `internal/space/durability.go:39` | the terminal frame of a canceled turn |

- A life resuming from a checkpoint **seeds from the durable high-water**, so it continues the
  sequence instead of restarting it.
- Same key + different body → `KEY_REUSE_MISMATCH`, and the write **stops**. Consumers must surface
  that as a defect, never retry through it.
- `CancelTurn` must leave a terminal frame in the durable stream: the stream of a canceled turn ends,
  and that terminal must survive a lost ACK / late append (commit `4c7ea24`).
- When that frame still cannot be written, the cancel is NOT rolled back — revoking the run's own
  credential is a security action — and the response says so instead of staying silent:
  `terminal_frame_durable:false` plus a `note`. `GetTurn`/`ReadStream`/the run row stay consistent with
  each other; it is the **durable transcript** that is short its last event, and the SSE edge cannot
  show that (it closes a thread whose durable runs are all terminal). Consumers that diff a transcript
  against a checkpoint must read this field, not infer it from the run row.

## 7. Isolation check with a second user (test fixture, not the delegation path)

To prove two humans cannot see each other, use two **independent** subjects: each does its own
login → exchange (§2) → per-run grant (§3), and each must get `NOT_FOUND`/`FORBIDDEN` on the other's
turn, thread, run, stream and checkpoint. This is orthogonal to §4: a second user must never appear
in the middle of one run's delegation chain.

## 8. SDK entry points

Go, `pkg/sdk`:

| Symbol | Location |
|---|---|
| `sdk.NewApp(cfg *Config, graph GraphFunc) (*App, error)` | `pkg/sdk/app.go:72` |
| `(*App).RunOnce` / `(*App).RunLoop(ctx, pollInterval)` | `app.go:84`, `app.go:120` |
| `(*App).EnsureThreadAndRun(ctx, contextID, taskID, payload) (Task, error)` | `app.go:145` |
| `sdk.ClaimAndHold(ctx, cfg) (*ActiveLease, error)` | `pkg/sdk/lease.go:58` |
| `ActiveLease.{ClaimResult,Token,Fence,ThreadID,RunID,IsLost,LostReason}` | `lease.go:28-50` |
| `(*ActiveLease).Release(ctx,status)` / `.Suspend(ctx,waitFor,deadline)` | `lease.go:84,94` |
| `sdk.OperationKey(runID, nodeID, callIndex) string` | `pkg/sdk/intent.go:15` |
| `IntentRecorder.{Record,Dispatched,Succeeded,Failed}` | `intent.go:34,43,48,53` |
| `CheckpointSaver`, `AHPStore` | `pkg/sdk` |

Python, `sdk/python/ahp_state`: `AHPCheckpointSaver`, `AHPStore.with_identity(node, call)`,
`IntentRecorder(node="agent")`, `SideEffectUnknown`.

`ClaimAndHold` + `ActiveLease.IsLost` is the shape a runner must use: when the lease is lost, writes
stop and the process exits rather than continuing to write under a stale epoch.

## 9. What a runner MUST do to survive SIGKILL

This is the answer to "how does a SIGKILLed worker survive a re-delivery".

1. **Claim before writing.** `ClaimRun` (by certificate) yields the fence; every subsequent write
   carries it. A dead worker's fence goes `STALE_EPOCH` for everyone else — the epoch, not a lock,
   is what makes double-write impossible.
2. **Journal the effect before the effect.** `RecordIntent` → `dispatched` → perform →
   `succeeded`/`failed`. Crash between dispatch and outcome leaves `dispatched`, which is an honest
   ambiguity and is never auto-replayed; a crash before `recorded` leaves nothing and is safely redone.
3. **Key by logical position.** Steps via `StepKey`, effects via `(run,node,call)`-position. A
   re-delivery of the same request produces the same effects in the same order, so it lands on the
   same keys and meets a recorded outcome instead of touching the world twice.
4. **Checkpoint to resume, not to guess.** Resume seeds from the durable high-water; under-counting
   is the safe direction because the earlier row is then *met* at its coordinate and replayed or
   refused — never silently re-run.
5. **Never retry through a contract code.** A definitive code (`STALE_EPOCH`, `FORBIDDEN`,
   `INVALID_ARGUMENT`, `KEY_REUSE_MISMATCH`) is a decision, not a transient. Only a failure with no
   code at all (dead connection, 5xx, deadline) may be re-sent, and only as the *identical* write.
6. **Close the stream.** A canceled turn's terminal frame must be durable even when its ACK was
   lost, or the client watches a dead turn forever.

---

## Addendum 2026-11-09T15:20Z (lane sdk-clients, HEAD 7a3a7a22) — §8 symbol map moved

Приказ прораба (гэп Go SDK A2AExecutor): общий не привязанный claim-путь удалён,
так как он исполнял новый payload под чужим run/fence (§8/§9.1 требуют, чтобы claim
именовал задачу). §8 выше остаётся контрактом без изменений; имена символов изменились:

| Было (§8) | Стало | Смысл изменения |
|---|---|---|
| `sdk.ClaimAndHold(ctx, cfg)` | `sdk.ClaimTaskRun(ctx, cfg, runID)` | claim без имени run невозможен |
| `(*App).RunOnce` / `(*App).RunLoop` | `(*App).RunOnceFor(ctx, task)` / `(*App).RunTasks(ctx, tasks, poll)` | цикл обязан знать задачу, которую обслуживает |
| `EnsureThreadAndRun` возвращал убеждение вызывающего | возвращает durable `run.TaskID`/`run.Payload`, отказ при расхождении | задачу именует верифицированный креденциал, не клиент |

Семантика Release/`IsLost`/`STALE_EPOCH` из §8–§9 не менялась; отвязанный claim и
исполнение чужого payload теперь дают `INVALID_ARGUMENT` до записи. Доказательства:
`pkg/sdk/a2a_executor_test.go`, `tests/sdk/sdk_test.go`,
`tests/space/sdk_clients_bound_run_realstate_test.go`.
