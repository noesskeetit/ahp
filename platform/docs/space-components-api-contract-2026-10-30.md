# Space component API contract vs PDF rev3.2 §3.2 (2026-10-30)

Purpose: the objective requires Space backend to expose concrete API contracts for the six Space
components and to report implemented/missing `as-workspace` / `as-profile` / `as-admin` against PDF
rev3.2 §3.2. This is the source-derived contract surface for `@hive-frontend` / `@delivery` /
`@hive-director`. Requirements source: `ahp-agents-space-architecture (2) (1).pdf` rev 3.2
(sha256 044e045b…4821a7e2), §3.2 service table + §3.3 run protocol. No other doc expands scope.

STATUS CAVEAT (honest, no fake readiness): everything below is implemented-at-source and
offline-unit-tested. The whole Space prototype is UNACCEPTED. Build/`-race`/real-PG/IV verification
and the deployed real journey are PENDING on the Cloud.ru VM (local workers paused for migration).

## §3.2 authoritative ownership (PDF §3.2 service table)
| Component | Owns (PDF §3.2) | Backed by AHP |
|---|---|---|
| as-web | UI state: chat, streaming, file browser, Computer portal, agent requests, approvals & connections | ahp-gateway, Artifacts, Compute (portal), Tool & Credential Gateway (approvals) |
| as-workspace | Projects, members, roles, invitations, **chat list** | Identity & Policy (projects as IAM resources) |
| as-profile | Dot personal settings over tenant AgentConfig, personal skills & connections, Computer policy | Registry (AgentConfig, skills), Tool & Credential Gateway (connections) |
| as-admin | Tenant policies, skills & connector-template catalog, model routes, allowed custom agents, config-agents | Registry, Identity & Policy, Model Gateway |
| Run Coordinator | Chat contexts & transcripts, runs (runner + a2a) and runner steps, inbox, leases/epochs, receipts, HITL, run-budget split | State API, Identity & Policy (task grants), Model Gateway |
| as-turn-runner | Nothing durable: stateless S-class Core Agent pool | Model GW, Tool & Credential GW, Memory, Artifacts, Compute |

PDF §3.1 boundary (repeated §3.2 note): Agents Space drives runs/steps itself and takes memory,
files, Computer, models, tools FROM AHP via authorized facades. Space MUST NOT reimplement those
backends; it consumes them through the authorized Space facade with a scoped task.

## Wire envelope (Space edge)
- Every operation: `POST /v1/space/{Operation}` (CamelCase). Single read-only GET:
  `GET /v1/space/stream/{thread_id}` (SSE; PDF requires SSE replay cursor, not WS).
- Subject/scope is derived server-side from the verified human token + server-owned task exchange
  (decision 2026-10-30-exchange-scope-authority-model). Body NEVER carries tenant/subject/
  deployment; strict decode rejects unknown fields. UI sends NO subject query.
- Tier-2: each run executes on its OWN delegated child grant (Identity delegate→mint), never the
  ingress authority; run grant token is `json:"-"` and is excluded from every HTTP/SSE response.

## Run Coordinator + as-web turn facade — IMPLEMENTED (source, offline-tested)
Ops (from `internal/space/handler.go` ops map). DTOs from `internal/space/types.go`.

| Op | Request | Response | id-binding |
|---|---|---|---|
| AcceptTurn | `{key, context_id, input, max_steps?, agent_config?}` | `{turn_id, thread_id, status}` | durable thread_id + turn_id minted by State (CreateThread/CreateRun); browser `key`/`context_id` are idempotency labels under scope, not authority. An absent/zero `max_steps` means **nobody stated a budget** and is persisted as such — the runner's own fallback bound (64) bounds the loop but is never attributed to the caller (`docs/space-runner-seam-contract-2026-11-01.md` §4.1); `Turn.max_steps` is the stated value, `0` = unstated |
| DispatchTurn | `{}` | `{ok}` | claims next pending run in scope |
| CancelTurn | `{turn_id}` | `{ok, terminal_frame_durable, note?}` | authorize→revoke THIS run's child grant subtree; failure is an error, never a false 200. `terminal_frame_durable:false` = the cancel is durable but the thread's durable transcript has no terminal event (the cancel is deliberately NOT rolled back for a journal failure; see `docs/space-runner-seam-contract-2026-11-01.md` §6) |
| Steer | `{turn_id, instruction}` | `{ok}` | run inbox steering |
| SubmitHITL | `{turn_id, kind, prompt, deadline?}` | HITL | creates/updates approval gate |
| RespondHITL | `{hitl_id, approved, response?}` | HITL | human response |
| EmitOutbox | `{turn_id, key, kind, payload}` | `{ok}` | delivery item append |
| SaveCheckpoint | `{turn_id, sequence, state, messages}` | `{checkpoint}` | mid-run checkpoint |
| Recover | `{}` | `{recovered[], unknown[]}` | cold restart projection |
| GetTurn | `{turn_id}` | `Turn` | token-free (`RunGrant json:"-"`). A **failed** turn carries `failure_reason` — the same cause the thread stream carried (Core's error / a StepError frame's `error` / a durability failure), never empty for `status=failed` and never present on a healthy turn (D8, forum `20261101T123847Z` §4). It survives a runner restart: a turn this process no longer remembers is rebuilt from State's Run row for the status, and the cause is then read back out of the durable transcript under the caller's own task token, attributed by the writer's own step-key (`<turn_id>:<step>`, kind `error`) — it is never duplicated into the run row, because two places naming a run's death can disagree. Stated limits: a turn refused in preflight has no `error` frame to read back and reports exactly that; a transcript that cannot be read says nothing about the cause rather than guessing |
| ListTurns | `{}` | `[]Turn` | token-free. Deliberately does **not** carry `failure_reason`: rebuilding it costs one bounded transcript read per failed turn, and a history page is not worth it — the cause belongs to the single-turn read |
| ReadStream | `{thread_id, after, limit}` | `{events:[...]}` | object envelope, not array |
| ListRunHistory | `{thread_id,...}` | runs | per-thread history |
| ListChats | `{cursor?, limit?}` | `{chats:[...], next_cursor?, has_more?}` | cross-thread chat list (as-workspace "chat list") |

SSE (`GET /v1/space/stream/{thread_id}`): envelope `{type, cursor, thread_id, event_id, payload}`;
`id:` == durable `cursor` (== ReadStream sequence); resume via `?after=` or `Last-Event-ID`
(higher wins). Covers as-web chat + streaming.

## Missing in Space — and WHY it is correctly OUT of internal/space (route to real owner)
These are §3.2 services backed by other AHP services, not by the Run Coordinator. Reimplementing
them inside `internal/space` would violate the §3.1/§3.2 boundary and duplicate other lanes.

1. **as-workspace — projects / members / roles / invitations.** MISSING in Space by design.
   Owner: **Identity & Policy** (projects are IAM resources). Space consumes this via the authorized
   facade; Space already provides the chat LIST (`ListChats`). Frontend should call Identity for
   project/member/role admin, NOT Space.
2. **as-profile — Dot personal settings, personal skills & connections, Computer policy.** MISSING
   in Space by design. Owner: **Registry** (AgentConfig/skills) + **Tool & Credential Gateway**
   (connections). Space only consumes a pinned `agent_config` (revision+deployment) at AcceptTurn.
3. **as-admin — tenant policies, skills/connector-template catalog, model routes, allowed custom
   agents, config-agents.** MISSING in Space by design. Owner: **Registry** + **Identity & Policy** +
   **Model Gateway**. Space consumes the resulting pinned config; it is not an admin control plane.
4. **as-web — file browser, Computer portal, connections, approvals (delivery).** MISSING in Space
   by design: §3.1 says Space takes files/Computer/connections/approvals FROM AHP. Owner:
   **Artifacts** (files), **Compute** (Computer portal), **Tool & Credential Gateway**
   (connections/approvals). The Space side of approvals is the HITL orchestration gate
   (SubmitHITL/RespondHITL), which IS implemented; the approval *effect* is executed by AHP.

## What Space MUST add to fully serve the UI (in-lane backlog, gated on VM build/test)
- Confirm as-web turn-facade DTOs match `@hive-frontend` `spaceFacade.ts` (already aligned to
  `POST /v1/space/{Op}` with fail-closed MISSING_UPSTREAM). Publish/lock exact req/resp field names
  (this table is the Space-side source of truth).
- Verify SSE replay-cursor semantics against the frontend resume path (OG-2 high-water seeding is in
  progress under the SP-1/SP-2 recovery fixes).
- Provide the authorized-facade client seams Space needs to consume Artifacts/Compute/Tool&Credential
  (files/Computer/approvals-effect) WITHOUT becoming those backends — coordinate exact endpoints
  with `@delivery`/gateway (TLS/cookie/base-URL/`/user`|`/dev`|`/admin` host split is gateway/delivery).
- Do NOT implement as-workspace/as-profile/as-admin admin surfaces in Space; publish the concrete
  Identity/Registry/Model-GW contracts they require to those owners.

## Consumer routing (send real API needs directly to owners)
- @hive-frontend: use the table above for chat/stream/HITL/control/chat-list; for projects/members/
  roles call Identity; for profile/skills/connections call Registry + Tool&Credential GW; for admin
  call Registry/Model GW. Space sends no subject; UI never mints tasks or picks subject/deployment.
- @delivery: gateway must enforce `/user`|`/dev`|`/admin` host split + scoped-task exchange so the
  six surfaces reach the right deployable; Space exposes only its facade ops above.
- @hive-director: §3.2 ownership is preserved; no Space reimplementation of Identity/Registry/
  Model-GW/Artifacts/Compute. Prototype UNACCEPTED; SP-1/SP-2 + production-mode joint test pending VM.
