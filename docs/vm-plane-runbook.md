# VM-plane / L4 acceptance runbook (delivery handoff → integration-validation)

Status of this material: **handoff tooling only.** The composed prototype stays
**UNACCEPTED**; `accepted` is set only by root/director after an independent run.
Nothing in this runbook claims a deployment or a target-runtime result it did not
measure. **source ≠ deployment, host(Mac) ≠ target-runtime(VM).**

## Why a VM plane exists

The whole-system Mac journey (`npm run journey:live`) is the *journey component*
evidence. Whole-platform acceptance additionally requires L4 gates a disk/egress
-gated Mac MUST NOT fake. Single driver:

```
node scripts/vm-plane.mjs            # or: npm run vm:plane
```

Each gate prints `PASS` (ran here, rc=0), `RED` (capability present, genuinely
failed), or `PENDING` (capability absent HERE → deferred to VM). The driver never
turns a PENDING into a pass. Final line is `VM_PLANE=GREEN|PARTIAL|RED` and a
machine-readable report is written to `acceptance/vm-plane-report.json`
(override with `--report <path>`).

## Gate matrix

| gate | layer | proves | runs where | enable |
|---|---|---|---|---|
| `G0-native-qwen` | G0 | native-Qwen-only at runtime (no OpenAI/Astra), secret-free | any host w/ config | `AHP_NATIVE_QWEN_ASSERT`, `AHP_LLM_BASE_URL`, `AHP_NATIVE_QWEN_HOSTS` |
| `tier-static` | L1/L2 | Tier0/1/2A/2B manifests well-formed, promised kinds/names | **any** (non-mutating) | — |
| `tier-server-dryrun` | L3 | admission vs real API server (**not** L4 rollout) | VM w/ kubectl | `AHP_TARGET_RUNTIME=1 --kubectl-context <ctx>` |
| `go-race-whole-tree` | L4 | `-race ./...` data-race free | **VM** (swap gate) | `AHP_VM_PLANE_HEAVY=1` |
| `compose-image-build` | L4 | docker build of the real all-role ahp image | **VM** | `AHP_VM_PLANE_HEAVY=1` + docker |
| `otlp-collector-target` | L4 | spans to a REAL OTLP collector | VM | `AHP_OTLP_ENDPOINT=<collector>` |
| `data-plane-tenant-scope` | L4 | real presigned bytes + foreign-tenant deny (RFC-002 data-owner) | **VM** | `AHP_VM_PLANE_HEAVY=1` + `AHP_ENABLE_DATA=1` + MinIO |
| `load-p95-target` | L4 | control-API p95 on TARGET runtime (PDF §11) | **VM** | `AHP_LOAD_TARGET=1` |

`G0-native-qwen` and `tier-static` are the only gates the driver expects to turn
GREEN on a plain host; everything else is PENDING there → honest `VM_PLANE=PARTIAL`.

## G0 wiring (mandatory, per iv differential criterion #3)

Production `openai_compat.go` is fail-closed but holds **no host-allowlist**, so
native-only is enforced only at the test/external layer. Therefore the VM rebuild
runner MUST execute `native-qwen-assert.sh` as gate **G0 BEFORE the checkout bin
starts**. The driver runs it first, before any heavy launch. iv wires the same
assert into the rebuild/deploy step (delivery + execution lanes).

## One-shot VM invocation (integration-validation)

```
git clone --branch delivery/vm-plane-handoff <bundle-or-remote> ahp-vm && cd ahp-vm
git checkout fa62df2          # integrated head (delivery/integrated-90a33a5); product plane == accepted core c39d9627
# provision isolated services first (PG allowlist ahp_hive_*, MinIO scoped bucket,
# Qwen base, OTLP collector); secrets only from the VM secret store — never printed.
AHP_NATIVE_QWEN_HOSTS='<native-host-allowlist>' node scripts/vm-plane.mjs \
  --report acceptance/vm-plane-report.json
# to flip the L4 heavy/target/IAM gates:
AHP_VM_PLANE_HEAVY=1 AHP_LOAD_TARGET=1 AHP_ENABLE_DATA=1 \
AHP_OTLP_ENDPOINT='<collector>' AHP_NATIVE_QWEN_HOSTS='<native-host-allowlist>' \
  node scripts/vm-plane.mjs --kubectl-context '<ahp-cluster-ctx>'
```

`--kubectl-context` is mandatory for the server-dry-run leg and is never guessed:
the driver refuses to touch a cluster without an explicitly named context, so a
neighbour `kubectl` context can never be validated or mutated by accident.

## FROZEN composition (this round — do not chase tips without the director)

| plane | pin | note |
|---|---|---|
| baseline | `8b14a59` | go-platform baseline |
| composed seed | `012a849` | product = byte-identical to this candidate |
| **accepted integrated core** | `c39d9627` | product plane iv verifies == EMPTY diff to HEAD; `= 012a849 + accepted iv#74 durability`, `go.mod` unchanged (director `b9f9b174`) |
| Space (canonical tip) | `cdfc20c` | FREEZE; `git diff 461aa2ce..cdfc20c -- internal/state internal/stateapi` = empty |
| Execution | `ecd5854` | canonical Executor (RFC-001) |
| Identity | `a65ebc1` | accepted |
| Compute | `27cd4382` | accepted (delivery ownership_excludes: infra/compute) |
| State | `461aa2ce` | accepted, not moved |
| integrated branch | `delivery/integrated-90a33a5` (de-gate head; exact SHA + bundle sha256 in the newest forum `report`) | product==`c39d9627`, journey blob de-gated `f8ec7b96` — **frozen until iv verdict** |
| this handoff branch | `delivery/vm-plane-handoff` | VM/L4 driver + runbook (no product change) |

## Evidence hygiene

The driver inherits `scripts/ahp-env.mjs`: the DB is always rewritten to the
allowlisted isolated name (`ahp_hive_*`); captured stderr is passed through the
long-token redactor; no credential, DSN, or signed-URL value is ever printed or
stored. `source ≠ deployment` is asserted in every manifest gate.

## iv verification kit (r2) — `npm run verify:iv`

`scripts/iv-verify.mjs` is the deterministic entry for integration-validation's
independent re-run (director `20261030T031552Z`, r2 = one full pass + recovery +
fail-closed proof). Same honesty model as `vm:plane`: `PASS`/`RED`/`PENDING`, a JSON
report (`--report <path>`), and `IV_VERIFY=GREEN|PARTIAL|RED`. It never fabricates GREEN.

| leg | proves | runs where | gate |
|-----|--------|-----------|------|
| **G0** native-Qwen-only | runtime provider identity is native Qwen (no OpenAI/Astra fallback); enforced ONLY at this external gate because `openai_compat.go` carries no host allowlist. **Runs BEFORE any bin**: a DENY marks G0 `RED` and forces B/C to `PENDING` (criterion #3) | **VM** (env-injected) | `scripts/native-qwen-gate.sh` + `AHP_LLM_BASE_URL`/`AHP_LLM_MODEL`/`AHP_NATIVE_QWEN_HOSTS` |
| **A** fail-closed integrity | the live-journey harness cannot green-wash a forbidden condition: Emil `:19081` (kvm stand-in) and `AHP_E2E_ALLOW_MOCK=1` each MUST exit non-zero with their specific guard | **anywhere** (non-destructive; guards fire before any heavy build) | always on |
| **B** `journey:live` | real Qwen → real Emil-core `:18000` tool-call → real MinIO `sha256(PUT)==sha256(GET)` → real PG durable run | **VM (iv-owned)** | `AHP_IV_RUN_JOURNEY=1` |
| **C** pg-restart-recovery (G-PG/#74) | durable PG State survives a PostgreSQL restart: `CreateThread`/`CreateRun` → bounce PG → fresh service `GetRun` reads the same nonce back | **VM (iv-owned PG)** | `AHP_IV_PG_RESTART_CMD=<iv isolated-PG restart cmd>` |

Leg-B is deliberately **not** pre-run by delivery: iv is the independent verifier
(`author ≠ verifier`). Leg-C takes an *operator-supplied* restart command so delivery
never bounces the shared PostgreSQL container; it reuses the verified `spawnService /
mState / pollReady` helpers (the same code paths the journey proved). On a Mac with the
disk gate closed, the honest result is `PARTIAL` (A `PASS`, G0/B/C `PENDING`).

## Single checkout iv MUST use (binding)

The product plane iv verifies is the **accepted integrated core `c39d9627`** (director
`b9f9b174`: State byte-identical, product = Space + accepted iv#74 durability, `go.mod`
unchanged). `c39d9627` = composed seed `012a849` PLUS the accepted durability product, so
product diff vs `012a849` is intentionally NON-empty and vs `c39d9627` is EMPTY. The
binding single-checkout is the head of branch `delivery/integrated-90a33a5` (its de-gate
head), shipped as `acceptance/integration/delivery/delivery-integrated-90a33a5-<head>.bundle`
requiring `c39d9627`. The authoritative head SHA and bundle sha256 are published in the
newest forum `report`; do not hardcode a stale SHA.

Why the head must carry G0: an earlier doc head `33de84c` was bound here but did
**not** contain `legG0` (`git show 33de84c:scripts/iv-verify.mjs | grep -c legG0` = 0),
so checking it out would let iv run B/C with the mandatory criterion-#3 gate absent.
The bound head MUST satisfy all three invariants below; verify them on the checkout:

```
# 1a. dependency + compose integrity (NO oauth2 downgrade, NO compose drift): EMPTY
git diff c39d9627..HEAD -- go.mod go.sum compose.yaml pkg/    # EMPTY
# 1b. no delivery-authored product code: the ONLY product delta over the accepted core
#     c39d9627 is the canonical producer planes cherry-picked in (execution RFC-001 =
#     internal/executor/* + internal/core/{artifact_tool,credgw,runtime} +
#     cmd/ahp/role_execution.go; canonical Space tip = adapters/emil + cmd/as-runner +
#     internal/space/runfence). Every delta file must belong to one of those planes:
git diff c39d9627..HEAD --name-only -- internal/ cmd/ sdk/ adapters/ | \
  grep -vE '^(internal/executor/|cmd/ahp/role_execution\.go$|internal/core/(artifact_tool|credgw|runtime)|adapters/emil/|cmd/as-runner/|internal/space/runfence\.go$)' \
  && echo "UNEXPECTED PRODUCT FILE (RED)" || echo "product delta == canonical planes (OK)"
# 2. the criterion-#3 gate is actually wired (>=1 hit), and runs before B/C
grep -c legG0 scripts/iv-verify.mjs                       # >= 1
# 3. journey bytes carry the D2 de-gate (ruling b9f9b174): live-journey.mjs blob f8ec7b96
#    (NOT the pre-de-gate 37c286b0 that 996eed2 / c39d9627 carried)
git rev-parse HEAD:tests/e2e/live-journey.mjs             # f8ec7b96... (de-gated)
```

The bundle requires baseline `8b14a59` and must pass `git bundle verify`. Its tree
carries `scripts/iv-verify.mjs` (with `legG0`), `scripts/vm-plane.mjs`,
`scripts/native-qwen-gate.sh`, `tests/helpers/ahp-client.mjs`, this runbook, and the
`verify:iv` / `vm:plane` npm scripts. Checking out the handoff head does **not** move
or re-open the accepted `c39d9627` product verdict — same product plane, de-gated
journey bytes (`f8ec7b96`), plus the harness needed to execute the legs.

iv on the VM runs:
```bash
set -a; . ./.env; set +a
AHP_IV_RUN_JOURNEY=1 \
AHP_IV_PG_RESTART_CMD='docker restart iv_ahp_pg' \
  npm run verify:iv -- --report acceptance/iv-verify-report.json
```
G0 is wired **in-runner** (LEG-G0 execs `scripts/native-qwen-gate.sh` before B/C); an
unconfigured gate is `PENDING`, a DENY is `RED` and hard-blocks the bin-starting legs.
Expected on a green VM plane: `IV_VERIFY=GREEN (pass=5 red=0 pending=0)`.
