# Identity & Policy

Task-tree identity, delegation, live authorization and information-flow labels.
This is the platform's authority spine: every grant is the authority, and the
store enforces every rule itself because the request path is untrusted input.

## Scope of this block

Owned paths: `internal/identity`, `internal/identityapi`, `pkg/auth`,
`pkg/labels`, `pkg/transport`, `tests/identity`, `docs/identity.md`.

## Trust model (read this first)

- **Local Ed25519 issuer only — NOT production corporate IAM.** There is no
  real corporate IAM environment in this build. The JWT issuer, its signing key,
  and the mTLS CA used by the acceptance harness are **local fixtures** that
  emulate the contract. Never describe these fixtures as "production".
- **Fail closed.** A nil `Authorize` callback, a missing/incorrect token use, a
  token bound to another workload, a missing verified client chain, a revoked or
  expired grant, or a foreign scope all deny the request. Nothing degrades to
  "allow" on a lookup miss.
- **currentIAM adapter contract.** Live authorization is `Store.Authorize` /
  `Store.Evaluate`, consulted on every task request. They re-read the live grant
  (status, expiry on the PostgreSQL clock, subject-revocation) and intersect the
  required capability against the grant's stored capability set. There is no
  separate identity cache: revocation/cancel is visible on the next request
  without any cooperation from the agent holding the token.

## Three authentication tiers (`pkg/transport`)

| Tier | Verifier | Evidence required | Used by |
|------|----------|-------------------|---------|
| `Human(verifier, aud)` | Ed25519 JWT, `token_use=human` | Human JWT bound to a real `sub`; no mTLS | `POST /v1/identity/roots` |
| `Task(verifier, aud, cap, authorize)` | Ed25519 JWT, `token_use=task` | Task JWT **and** mTLS; client-cert URI SAN must equal the token's workload SPIFFE ID; then live `authorize` | root read, delegate, mint, evaluate, labels |
| `Workload()` | none (control plane) | mTLS only; actor = verified client-cert workload | `control/cancel-subtree`, `control/revoke-subject` |

Human approval tokens and internal task tokens are **not interchangeable**: a
task token cannot open a human root, a human token cannot drive an agent route,
and a valid task token without a matching client certificate is refused.

Workload identity: `spiffe://ahp/tenant/{tenant}/deployment/{deployment}`
(`auth.WorkloadID`). Claims carry a genuine subject (`sub`), a separate `task`
claim, `tenant`, and deployment (`azp`).

## HTTP surface (`internal/identityapi`)

| Method & path | Tier | Capability |
|---------------|------|------------|
| `GET  /healthz` | none | — |
| `POST /v1/identity/roots` | Human | — |
| `GET  /v1/identity/roots/{task}` | Task | `identity:read` |
| `POST /v1/identity/roots/{task}/delegate` | Task | `identity:delegate` |
| `POST /v1/identity/roots/{task}/mint` | Task | `identity:mint` |
| `POST /v1/identity/roots/{task}/evaluate` | Task | `identity:evaluate` |
| `POST /v1/identity/roots/{task}/labels` | Task | `identity:labels` |
| `POST /v1/identity/control/cancel-subtree` | Workload | — |
| `POST /v1/identity/control/revoke-subject` | Workload | — |
| `POST /v1/identity/control/introspect` | Workload | — |
| `POST {prefix}/auth/login` | none (credential) | — |
| `POST {prefix}/auth/token` | Browser session cookie | — |
| `POST {prefix}/auth/refresh` | Browser session cookie | — |
| `POST {prefix}/auth/logout` | Browser session cookie | — |
| `GET  {prefix}/me/user` | Browser bearer (human, web audience) | — |
| `GET  {prefix}/me/flags` | Browser bearer (human, web audience) | — |

`{prefix}` is `/u-ahp/v1` unless `BrowserConfig.Prefix` says otherwise; with
`Browser` unset on `Config` the last six routes do not exist at all.

Agent-plane and control-plane authentication failures map to `403`. The
browser surface answers `401 UNAUTHENTICATED` where the caller must sign in
again, because a UI branches on that difference. A foreign
tenant/deployment/subject is **invisible** (`404`), so the API never leaks
existence across scopes.

## Browser session surface (Agent Space UI)

The UI (`as-web`) is an untrusted public client. This surface is what makes the
architecture's public entry — "login via IdP, staff JWT, IAM ExtAuthz" — real
inside one cell, without a corporate IdP.

**Chosen local login flow.** First-party credential POST, not a redirect IdP.
The browser never receives a signing or platform secret.

1. `POST /auth/login` `{username,password}` → the store resolves the account and
   therefore the **tenant and subject**; the request has no field that can name
   either, so an identity cannot be chosen. The body is decoded with unknown
   fields rejected, which turns `tenant`/`subject`/`deployment` in a login body
   into `400`. An unknown username and a wrong password return the same body
   after the same argon2id cost, so login cannot enumerate accounts. A live
   session id goes into an `HttpOnly; SameSite=Strict` cookie (plus `Secure`
   whenever TLS or `X-Forwarded-Proto: https` is present).
2. `POST /auth/token` exchanges the session cookie for a short-lived human JWT
   (`aud` = the browser audience, `exp` = `identity.BrowserAccessTTL`), which the
   UI holds **in memory**.
3. `POST /auth/refresh` advances the session epoch, retiring every token minted
   before it, and pushes only the *idle* deadline. The session's **absolute**
   deadline is immovable — the database trigger refuses to move it later, so
   renewing proves liveness and never extends authority.
4. `POST /auth/logout` revokes the session row.

Every cookie-bearing call must also carry `X-AHP-Session: 1`; SameSite=Strict is
the first CSRF defence and the header is the second.

**Audiences are separated.** A browser access token is minted for
`BrowserConfig.AccessAudience` (default `<control audience>/web`, i.e.
`ahp-web`), *not* for the control-plane human audience. Signing in to the UI is
therefore not the general human authority: a browser credential is refused by
`POST /v1/identity/roots`, and a control-plane human credential is refused by
`/me/*`. A service that wants to serve browser traffic (the Space facade) lists
the browser audience explicitly and must never accept the control-plane human
audience on a browser-reachable path. Human → authorized Space → scoped task
stays the only route to a task token.

**Revocation beats expiry.** Each mint carries a `jti` (the one optional claim in
`pkg/auth`'s payload; absent on every task and control-plane human token, so
their bytes are unchanged) bound to its session in
`ahp_identity.browser_token_bindings`. `/me/*` checks that binding against the
database clock on every request, so logout, epoch rotation and a subject
tombstone stop an *already issued* token immediately. Workload services get the
same answer over `POST /v1/identity/control/introspect`: a stateless JWT becomes
a revocation-aware decision instead of a wait for `exp`.

**Subject revocation reaches the browser.** `RevokeSubject` revokes that
subject's sessions and retires their tokens inside the same transaction, so a
tombstone never leaves a live cookie or a live credential behind, and the revoked
subject cannot log back in.

**Prototype boundary.** `SeedBrowserUser` provisions accounts and argon2id
credentials into PostgreSQL. That is the seam where a corporate IdP belongs; it
is a real local credential store, and it is **not** a production identity
provider. See "External IAM limitations".

## Grant & delegation model

- **Subject invariance.** A request carries no subject; the child always lives
  in the parent scope (tenant, deployment, subject). Delegation can never cross
  a tenant, deployment or subject boundary.
- **Actor chain.** Each delegation appends the deployment. A delegation would
  repeat an actor and is refused **unless** the parent enabled `allow_self`
  (self exception). Because delegation stays inside the caller's deployment,
  every HTTP delegation is a same-deployment re-entry and therefore requires the
  trusted root to have enabled `allow_self`.
- **Ceilings (trusted-manifest only).** `POST /roots` ignores any
  caller-supplied ceiling / `allow_self` and applies defaults
  (`DefaultMaxDepth = 4`). Hard bounds: `MaxTreeDepth = 16`, `MaxTreeFanout =
  1024`, `MaxTreeActive = 100000`, `MaxTokenTTL = 15m`. A child may only
  **narrow** its parent's ceilings, never widen them.
- **Rights intersection.** A child capability set must be a non-empty exact
  subset of the parent's. Live `evaluate`/`authorize` re-checks this against the
  stored grant.
- **Budget / expiry.** Effective expiry is `min(clock + ttl, parent.ExpiresAt)`;
  a child can never outlive its parent. Minting a fresh task token does not
  extend a grant.
- **Atomic quotas.** Fan-out, subtree-active and per-ancestor-active limits are
  counted on the PostgreSQL clock under the root advisory lock — the same lock
  cancellation holds — so no child is born inside a cancelled subtree and a
  replayed child key never consumes budget.

## Bootstrap of self-capable trees

HTTP cannot set `allow_self`, so a public human body can never grant itself
self-delegation. A self-delegation-capable tree is opened through the trusted
manifest (`Store.CreateRoot(..., AllowSelf: true)`), after which HTTP
delegation can propagate that self-capability down the chain. This is the
documented bootstrap boundary: privilege to re-enter a deployment originates
only from a trusted manifest, never from a caller body.

## Cancellation & revocation

`cancel-subtree` and `revoke-subject` (control-plane mTLS) take effect
immediately. The next task request is denied by live authorization without any
cooperation from the agent, and neither cancellation nor token re-mint extends
an expired/revoked grant.

## Information-flow labels (`pkg/labels`)

- **Monotonic, child→ancestor.** Labels accumulate; a child's context inherits
  the parent journal and ancestor grants inherit accumulated labels. A later
  turn inherits prior context. Siblings stay clean.
- **Sink policy.** `CheckWrite` fails closed on malformed labels, unknown taints
  and unknown kinds. Secret taint may never be exported. External /
  untrusted-external taint may not reach a trusted sink unless the audience is
  external or trusted policy passes `AllowExternal`; `AllowExternal` never
  relaxes subject/workspace visibility. Private data flows only to a matching
  private subject; multi-subject data flows nowhere.

## Concurrency & durability

The store is exercised under `-race` with real PostgreSQL, so two correctness
guarantees are part of the contract, not incidental:

- **Schema migration is serialized.** `Store.Migrate` takes a fixed service-wide
  PostgreSQL advisory lock (`pg_advisory_lock`, key `720261029`) on a dedicated
  pooled connection for the duration of the idempotent DDL, then releases it.
  This lets the `internal/identity` and `tests/identity` packages migrate the
  same isolated database concurrently without DDL deadlocks (SQLSTATE 40P01).
- **Serialization conflicts are retried with randomized backoff.** Grant,
  journal, and quota writes run through `runTxMode`, which retries only known
  PostgreSQL serialization failures (40001) and deadlocks (40P01), up to
  `maxTxAttempts`. The retry sleeps a jittered backoff (`jitteredBackoff`,
  equal-jitter over `math/rand`) so concurrently aborting transactions do not
  re-collide in lockstep and starve a writer. Quota checks are additionally
  serialized per-tree under the root advisory lock.

These invariants are validated by `TestConcurrentDelegateVersusCancelSubtree`
and `TestConcurrentDelegateRespectsFanout`, and by repeated `-race` runs of
`TestCreateRootDistinctRootsNoConflict`.

## External IAM limitations (exact)

- No real corporate IAM is wired. `pkg/auth` uses a **local** Ed25519 key as the
  only issuer; the acceptance CA is a **local** fixture. These are contract
  stand-ins, not a production trust root.
- The integration boundary for a real corporate IdP is the `Verifier` (trusted
  issuer/JWKS) and the mTLS client CA pool that populates `r.TLS.VerifiedChains`.
  Swapping in a corporate issuer means supplying a real verifier key set and a
  real workload CA — the routes and enforcement stay identical.
- No corporate **credential store** exists either. Browser logins authenticate
  against `ahp_identity.browser_users` / `browser_credentials` (argon2id) that
  this block seeds locally. A production cell authenticates at the corporate
  IdP and the browser never posts a credential to AHP; the swap is to drop the
  local login path and validate the IdP's assertion in its place — every route,
  session rule, audience split and revocation check above stays identical.
- No browser certificate path exists: mTLS stays on the workload/control plane.
  A UI cannot be issued a workload certificate, so the browser leg is TLS +
  HttpOnly session + short human JWT, and `introspect` is what makes that
  stateless credential revocation-aware for services.
- Per-source login rate limiting / lockout is not implemented here; the
  architecture places it at the gateway's ExtAuthz. Argon2id is the only
  protection this service applies to a credential guess.
- Until those swaps happen, treat the environment as **fail-closed emulated
  IAM**: correct semantics, local keys, explicitly **not** production.

## Single acceptance command

Real PostgreSQL + real HTTP + real Ed25519 crypto + mTLS, race detector on:

```sh
cd "/Users/aogabbasov/AI Agents/ahp/company-brain/worktrees/identity"
set -a; . ./.env.identity; set +a
export GOCACHE="$(pwd)/.gocache" GOFLAGS=-mod=mod
go test -race -count=1 \
  ./internal/identity/... ./internal/identityapi/... \
  ./pkg/auth/... ./pkg/labels/... ./pkg/transport/... ./tests/identity/...
```

`AHP_TEST_DATABASE_URL` points at the isolated `ahp_hive_identity` database in
the project's isolated `ahp-handbook` Postgres container; child-process tests
write only to that database. Tests use globally unique scope/context ids so a
persistent database never produces false collisions.

## Acceptance criteria → coverage (`tests/identity/acceptance_test.go`)

1. `TestHTTPLifecycleAndLiveAuthorize` — real mTLS+JWT root/delegate/mint/
   live-authorize over HTTP; subject invariance, actor-chain growth, rights
   intersection (grandchild denied `identity:delegate` → 403).
2. `TestLimitsQuotasAndBudget` — default depth 4, manifest depth, narrowing-only,
   cycle deny + self exception, atomic fanout, subtree-active, budget clamps
   child expiry to parent.
3. `TestCancelRevokeAreImmediateAndNonExtending` — immediate effect, no agent
   cooperation, expiry/token-renew never extends.
4. `TestLabelsMonotonicAndSinks` — monotonic, child→ancestor, sibling clean,
   private/external/secret sinks.
5. `TestTokenNonInterchangeabilityAndInvisibility` — human vs task token roles,
   mTLS requirement, workload/cert binding, cross-scope invisibility (404).
6. Race scenarios — `TestConcurrentDelegateVersusCancelSubtree`,
   `TestConcurrentDelegateRespectsFanout`.

Browser surface (`tests/identity/browser_acceptance_test.go`, real HTTPS, real
PostgreSQL, real tokens, cookie-carrying clients):

7. `TestBrowserLoginWhoamiAndLogout` — two distinct users in two tenants log in,
   `/me/user` and `/me/flags` report exactly their own account, logout kills the
   previously issued token and blocks re-minting, and one subject's logout does
   not touch the other's session.
8. `TestBrowserLoginRefusesChosenIdentityAndBadInput` — identical answer for
   unknown account and wrong password; `tenant`/`subject`/`deployment` in a login
   body → `400`; missing CSRF header → `403`; no session → `401`; any query
   parameter on `/me/user` → `400`.
9. `TestBrowserRevocationIsImmediateWithoutTheBrowser` — epoch rotation retires
   the old token; `RevokeSubject` kills the live session, its newest token and
   any re-login, with no client cooperation.
10. `TestBrowserRefreshNeverExtendsSessionAuthority` — five refreshes move the
    epoch and never the absolute deadline; a direct SQL attempt to extend it is
    refused by the trigger; a session past its absolute deadline cannot refresh.
11. `TestBrowserHumanAndTaskAndBrowserCredentialsAreNotInterchangeable` — browser
    credential refused by the control plane and accepted only on the browser
    surface; control human credential refused on the browser surface; browser
    credential refused on a task route even with a valid workload certificate;
    task token refused on `/me/user`.
12. `TestBrowserIntrospectionIsWorkloadOnlyAndRevocationAware` — route unreachable
    without a verified certificate; active for a live session, inactive for an
    unissued token and after logout.
13. `TestBrowserForeignTenantAccountsAreInvisible` — a foreign tenant subject is
    `NOT_FOUND`, never a hint; two sessions of one principal mint distinct
    credentials and revoke independently; a root lands only in the token's scope.
14. `TestBrowserConcurrentRefreshAndRevocation` — concurrent refreshes over one
    session all serialise, and a revocation racing live traffic always ends with
    the token dead.
