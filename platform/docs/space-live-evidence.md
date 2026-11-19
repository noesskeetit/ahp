# Space REAL live-journey evidence (live_real)

Status: **ready_for_director_review** (self-accept forbidden). Owner: space-product.
Seed/branch: `space-product` (inherited from integrated `012a849`). Tip under review: 9225854.
Requirements source: ONLY `ahp-agents-space-architecture (2) (1).pdf` rev 3.2
(sha256 044e045b73b7c34b642bd3c9eb5701b93eed470873a1e0a44e8b1d024821a7e2).
Emil-core `:18000` as-is is NOT a gate (decisions/2026-10-30-emil-image-scope-correction.md); the real
Emil hop here is OPTIONAL provenance, never faked (no httptest / newMockState / :19081 stand-in).

## What this run proves (one continuous `-race` run, no mocks on the proven path)

1. Native-Qwen only: `host=0038df35-…modelrun.inference.cloud.ru model=model-run-tqmbu-road`
   (enforced by the fail-closed gate `classifyNativeQwen`; foreign OpenAI/Astra host markers → FAIL).
2. Real model → OUR product tool → real MinIO: `save_artifact` sealed into bucket
   `ahp-space-live-<sfx>`, **sha256 PUT == GET**, version recorded. The seal is AHP Artifacts, not Emil.
   The mandatory artifact is a SUBSTANTIVE real native-Qwen document (re-run shows 546 bytes,
   model `stop=stop`, not a 2-byte trivial stub); the generator enforces a fail-closed size floor of 128 bytes.
3. Real Core loop: `model_steps=1 durable_emissions=2` against the live model with the product tool registered.
4. Real Emil A2A hop (`:18000`, message/send): `state=completed tool_calls=[core_skill_activate]
   core_artifact_save=false` → confirms Emil does not write the artifact; AHP Data/Artifacts owns the seal.
5. Durable across runner restart: run `status=done`, stream events and artifact version/sha restored from PG.
6. Private-context isolation: a foreign scope receives `NOT_FOUND` on the owner artifact (bytes do not leak).

Mandatory services are fail-closed: missing/unreachable PG / MinIO / native-Qwen → `t.Fatalf` (no `t.Skip`).
The only `httptest.Server` is the State SUT itself (`stateapi.New` over `state.New(pgxpool)` on the real
isolated PostgreSQL) — it is the system under test, not a stand-in for the model, MinIO, or Emil.

## Isolation

- PostgreSQL: `ahp_hive_space_product` ONLY (this run's isolated DB). No foreign DBs touched.
- MinIO scoped bucket `ahp-space-live-<sfx>`; S3 name valid (no underscore).
- Secrets sourced silently from sanctioned files; never printed. Raw log sanitized before commit.

## Reproduce

See `docs/space-live-run.sh` (no secrets embedded; reads only sanctioned sources). Prereqs:
- isolated PG `ahp_hive_space_product` reachable at 127.0.0.1:25432 (DSN with its password),
- native Qwen endpoint+key (user-allowed settings), MinIO root creds from `go-platform/.env`,
- optional real Emil on :18000,
- run the public native-Qwen host DIRECTLY (set `NO_PROXY` to cover `*.modelrun.inference.cloud.ru`;
  the harness HTTP(S)_PROXY is not required and may refuse).

Run:
`bash docs/space-live-run.sh`
Expected tail: `--- PASS: TestLiveRealEndToEnd` then `ok  …tests/space`.

## Model-generated tool execution (highest-priority proof)

A separate live_real test `TestLiveRealModelGeneratedToolExecution` proves the
PRODUCTION `core.Runtime.Run` loop drives a REAL, MODEL-GENERATED `save_artifact`
tool_call that the loop EXECUTES over REAL MinIO (re-run: model_steps=2 model->tool->model->done,
sealed versioned artifact, sha256 PUT==GET, 98 bytes, durable emissions>=1, StepDone reached).
This is NOT a manual seal leg and NOT a fixture. Fail-closed across 4 attempts (no t.Skip).
IV hook (independent):
```
AHP_ISOLATED_PG_DSN=... bash docs/space-live-run.sh   # journey
go test -race -tags live_real -run TestLiveRealModelGeneratedToolExecution ./tests/space/ -v
```

Raw sanitized log of this run: `docs/space-live-evidence-raw.txt`.

---

## FRESH live_real re-verification @ tip 1613dd8 (2026-10-30, real infra, no mocks)

`go test -race -tags live_real` against real Mac infra. Secrets never printed (runner
emits lengths only). Isolated PG = ahp_hive_space_product only. Emil = real, optional,
never faked (no httptest/newMockState/:19081 in any proven path).

### Endpoint health (HTTP codes)
- MinIO http://127.0.0.1:29000/minio/health/live -> 200  (ahp-handbook-go-minio-1)
- Emil  http://127.0.0.1:18000/.well-known/agent-card.json -> 200 (real core-agent; stand-in :19080 = DOWN, unused)
- native-Qwen host 0038df35-f6e5-4958-a4ec-0b289a0b0aef.modelrun.inference.cloud.ru, model model-run-tqmbu-road (allowlisted native; no OpenAI/Astra fallback)
- isolated PG 127.0.0.1:25432

### TestLiveRealEndToEnd  (PASS 12.59s)
- real native-Qwen artifact: bytes=468 stop="stop" sha=7714aa44460371eb
- MANDATORY artifact via OUR save_artifact tool: bucket=ahp-space-live-8a07892aeaf5 (S3-safe), versioned id/version=1ea86590-a49f-49d6-8e91-7ff31664b388, **PUT==GET bytes=468 sha match**
- core loop (real Qwen + product save_artifact tool registered): model_steps=1 durable_emissions=2
- real Emil provenance hop: state=completed tool_calls=[core_skill_activate] builtin=true **core_artifact_save=false** (seal is AHP Artifacts from model bytes, not Emil)
- durable after restart: run status=done stream_events=1 artifact_version=1ea86590… sha=7714aa44…
- private-context: foreign scope -> NOT_FOUND on owner artifact (no byte leak)

### TestLiveRealModelGeneratedToolExecution  (PASS 7.69s)
- MODEL-GENERATED tool execution (model -> model-GENERATED save_artifact tool_call -> runtime EXECUTES -> model): model_steps=2
- bucket=ahp-space-loop-031bf93659fa, version=2c707d58-4177-4bdf-8b7a-3f2a40137815, **PUT==GET sha=a3663e4a225d89ad bytes=98**, durable_emissions=4

Repro (offline-infra): tests/space/run-live-real.sh + loop runner under shared flock.
Boundary: this is Journey + tool-loop live proof. Live-SEAM acceptance over a REAL
State API (StateRunStore/CoordinatorBridge) is verified independently by iv (base
012a849), author!=verifier. Seam offline tests are client-conformance (fake State
SERVER over OUR client), explicitly NOT live-proof. Status: ready_for_director_review;
whole prototype UNACCEPTED until root.
