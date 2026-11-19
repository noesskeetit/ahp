# AHP baseline profiling / load harness

Reproducible measurement harness for the AHP platform. Every phase spins up its
**own isolated platform instance** (own free port, run-scoped mTLS CA + SPIFFE
client certs, own tenant/deployment/identity) and writes JSON + CSV to the
evidence directory. Latency is recorded per request (TTFB via `net/http/httptrace`,
first stream byte for streaming). Samplers are read-only. No secret, credential,
or user data is written to any report or log.

## Reproduce (one command)

All runs go through the lane runner (it injects the lane environment, records the
exact git revision, and writes a private per-run log):

```sh
cd "/Users/aogabbasov/AI Agents/ahp/company-brain/private/pdf-closure-20261102"

# whole baseline set (overhead -> recovery -> soak -> depfail -> llm), sequential:
bash profiling-baseline/profiling/run-baseline.sh

# or a single phase, e.g. the deterministic overhead ladder:
python3 run-tests.py --lane profiling-baseline \
    go run ./profiling/cmd/ahp-profile --phase overhead --step-s 20 \
    --out ../evidence --repo .
```

`--out ../evidence` is relative to the worktree (run-tests runs with `cwd` set to
the lane source), so reports land in `pdf-closure-20261102/evidence/`.

## Phases

| flag | what it measures | class |
|------|------------------|-------|
| `smoke`    | wiring sanity (health, thread/run lifecycle, mTLS, token) — NOT a load test | smoke |
| `overhead` | closed-loop ladder 1/5/10/20/30 then extension until a degradation boundary, per workload | deterministic platform overhead |
| `recovery` | steady step before/after a heavy burst, same instance, no restart | deterministic platform overhead |
| `soak`     | long steady mixed load; RSS/goroutine slope per minute (leak signal) | deterministic platform overhead |
| `depfail`  | steady load, dependency (PG) cut then restored, no restart | dependency-loss behavior |
| `llm`      | REAL Qwen end-to-end, volume-bounded, tokens only | real LLM E2E |

Workloads driven through the real mTLS product face: `edge_health`,
`state_read_thread`, `kv_write`, `kv_read`, `ckpt_write`.

## What each measurement is tied to

Every report embeds: machine fingerprint (hostname, OS, arch, cores, RAM),
platform version (git revision, dirty flag, built binary SHA-256), the exact load
profile (ladder, step seconds, pattern), the instance (port, tenant, deployment,
lane DB), and per-step CPU/RSS, heap/goroutines (`/debug-stats`), PG
(`pg_stat_database`/activity/db size), Redis (`INFO`), MinIO health, and Docker
block/net IO for the lane containers.

## Degradation boundary rule (documented)

A step beyond the core ladder is the boundary if: error rate > 5%, or p95 > 10x
the 1-concurrency baseline, or p99 > 10 s. The reached boundary is recorded in the
report notes per workload; otherwise "no degradation boundary reached within the
configured ladder".

## Real LLM E2E (`llm`) — prerequisites and honesty

The Model Gateway's production face is label-aware routed: it requires a provider
table (`AHP_MODEL_PROVIDERS_FILE`) once the task-label mount is present, and a
context label journal for the request's context (created by the identity exchange).
The harness mounts the smallest honest single-copy table (provider `modelrun`,
the model this lane is configured for, one wildcard route). With exactly one
declared copy the bound provider is inferred, so the per-tenant quota gate stays
unmounted for the throwaway profiling tenant. Authorization is the product path:
operator-provisioned human account (`identity.SeedBrowserUser`, the documented
seam) then `/u-ahp/v1/auth/login` -> `/u-ahp/v1/auth/token` ->
`/v1/identity/control/exchange` scoped task token (capability `model`).

Token counts come from the platform generation API (durable usage). The platform
exposes TOKENS (prompt/completion/total) and token budgets, never currency. No
token figure in any report may be read as money. A generation that returns an
empty completion (a reasoning model that spends the whole `max_tokens` budget on
reasoning before any visible text, `finish=length`) is refused by the platform as
FAILED **with the tokens it really consumed still charged**; the harness records
that. Raise `--llm-max-tokens` so the model can emit visible content and
generations reach `COMPLETED`.

## State-plane authorization (no self-signed grants)

Every State/Bootstrap call (`CreateThread` and the rest of `/v1/state/`) is
authorized the same product path as the model plane: an operator-provisioned
human account (`identity.SeedBrowserUser`, the documented one-shot seam, the same
role the product's `cmd/ahp/provision.go` `AHP_PROVISION_USERS_JSON` uses) is
exchanged at `/v1/identity/control/exchange` for a scoped task token with
capability `state`. Each instance runs with `AHP_ENABLE_BROWSER=1` and its own
deployment allowlisted via `AHP_IDENTITY_EXCHANGER_DEPLOYMENTS`, so the exchanger
is a real, allowlisted workload. The token's actor chain comes from the live
grant (`Store.MintTask` copies `g.Actors`), which is what the platform's
`Authorize` requires.

The harness **signs nothing itself.** It used to self-mint a `KindTask` token
from a dev seed; that token carried an empty `act` claim and every State call was
answered 403 `identity: token carries an empty actor chain`
(`internal/identity/tokens.go`). That self-sign path is removed. `harness` exposes
only a read-only `RequireNonEmptyActorChain` guard: a State run that cannot reach
the live exchange fails closed instead of minting a token that would only fail
downstream. Each artifact carries an `authorization_composition` block (who
provisioned the account, who signed the token, the exchange URL, the exchanger
deployment, the capabilities, and `harness_self_signed=false`) so this is visible
in evidence, not inferred.

- **pgvector is an operator prerequisite of the isolated lane DB, not a product
  defect.** The State plane needs a Postgres with the `vector` extension enabled
  on the lane database before `CreateThread` succeeds. When a lane DB lacked it,
  State calls failed for an infrastructure reason; the environment (not the
  product) is the place to fix that, and the harness must not report such a miss
  as a product result.

## Explicit boundaries (what is NOT measured)

- microVM / compute MOCK is not a real VM: it is not measured and no result here
  may be read as VM performance.
- This is single-node. No cross-node / cluster / autoscaling behavior is measured.
- The target production runtime topology and the memory-plane are not exercised.
- `smoke` is a wiring check, never a load result.
- The live stand (front `:29612`, runner `:29690`) is never contacted; every number
  comes from the harness's own isolated instance on its own port.
- Currency prices/cost are out of scope: only tokens, request counts, and platform
  token budgets/limits are reported.
