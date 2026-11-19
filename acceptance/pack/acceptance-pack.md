# delivery acceptance-pack

- status: **ready_for_director_review** (whole prototype UNACCEPTED)
- commit `61735058f2b8993b614a35ddeff1ad9ba147875a`  tree `fbc5a8f6e63ed8e6dca4e96618c24267cd5ea093`  base `8b14a59`  ranAt 2026-10-30T03:10:43.813Z
- isolation: db `ahp_hive_delivery`  minio `http://127.0.0.1:29000`
- summary: {"run":11,"cited":5,"gated":1}

| id | status | kind | evidence |
| --- | --- | --- | --- |
| G0-native-qwen-classifier | run:pass | run | {"pass":"17","fail":"0"} |
| build-offline-surface | run:pass | run | rc=0 |
| demo-serve-selftest | run:pass | run | {"planes":"readiness=MOUNTED identity=MOUNTED state=MOUNTED data(artifacts)=MOUNTED execution(a2a)=MOUNTED","create-run->PG":"PERSISTED run=20261030T031050.533832000Z_sm2ip77wee2vabvehbg5msez4a","emil-hop":"green task=9c60c238-636f-4f81-b5c5-760d7383135b","DEMO_UI":"green"} |
| model-artifact-pg | run:pass | run | {"model->artifact->PG":"passed=12 failed=0","DEMO_UI":"green"} |
| live-journey-whole | run:pass | run | {"LIVE_EMIL_TASK":"19257d15-c955-4950-8122-96ad932e325b","LIVE_EMIL_TOOL_CALLS":"1 LIVE_EMIL_MODEL_TURNS=2","LIVE_EMIL_MODEL_TURNS":"2","LIVE_PUT_SHA256":"69ebcfebe9c8bd82eb4adc35940553832136a364720c9c8c2e55790eca9a091f","LIVE_GET_SHA256":"69ebcfebe9c8bd82eb4adc35940553832136a364720c9c8c2e55790eca9a091f","LIVE_PG_RUN":"20261030T031119.425551000Z_6tielenkwigfkk4adb3aesf3xm","LIVE_JOURNEY":"GREEN"} |
| separated-roles | run:pass | run | Separated roles (state/identity/model) boot independently and emit per-role OTel spans. / [run-tests] e2e: 1 passed, 0 failed |
| otel-live-trace | run:pass | run | [otel-trace] 9 passed, 0 failed / [run-tests] e2e: 1 passed, 0 failed |
| tier-static-verify | run:pass | run | {"static": {"tier0": {"kinds": ["ConfigMap", "Deployment", "Service", "ServiceAccount"], "objects": 4}, "tier1": {"unit_files": 3}, "tier2a": {"kinds": ["Deployment", "Namespace", "Service", "ServiceAccount"], "objects": 10}, "tier2b": {"kinds": ["HorizontalPodAutoscaler", "NetworkPolicy", "PodDisruptionBudget", "Role", "RoleBinding"], "objects": 13}}} / {"server_dry_run":"not-requested","l4":"pending"} / RESULT: static_rc=0  (L4 target-runtime: PENDING/external unless captured on a real cluster) |
| chaos | run:pass | run | [unknown-outcome] 13 passed, 0 failed / chaos/unknown-outcome: PASSED / [run-tests] chaos: 8 passed, 0 failed |
| load | run:pass | run |   PDF §11 footprint measurement — REPORTED (local != target runtime) / [rss] PASSED / [run-tests] load: 3 passed, 0 failed |
| smoke | cited | cited | rc=0 prior runs |
| pdf-closure | cited | cited | REPRODUCIBLE every-PDF-section->evidence gate (acc #2): derives 18 top-level sections from the canonical PDF СОДЕРЖАНИЕ (sha 044e..a7e2, shasum-verified) and FAILs if coverage.md omits one. Proven this turn @81328ec: PDF_CLOSURE=PASS sections=18 rc=0; negative (drop a section row) -> FAIL rc=1; integrity (bad sha) -> rc=2. Needs canonical PDF OUTSIDE git, so it is NOT executed inside the offline pack. |
| integrated-crossplane | cited | cited | DIRECTOR independently verified cross-plane GREEN on VM target-runtime @ck42 abcfe84 (memory/artifacts/workspace/state/conformance rc=0) |
| go-build-whole-tree-offline | cited | cited | rc=0 offline @0934ddf (host modcache pre-seeded: go-sdk v1.8.0 + OTel v1.47.0 present) — G-GOSDK/G-OTEL-BUILD CLOSED on host |
| emil-tool-completion | run:pass | run | CLOSED via live-journey-whole: real Emil-core:18000 core_skill_activate succeeded + Emil provenance tool_calls>=1 (backend model-base fixed) |
| whole-system-race | cited | cited | DIRECTOR closed G-WHOLETEST-VM on VM (go1.26.2, -race rc=0 for state/conformance real-PG + workspace PG+MinIO on merged canonical 012a849+3033dd4); ck46 adds only scripts/docs (no product code), so product-code race result applies to this checkout |
| tier2a2b-l4-iam-compose-otlp | gated | gated | Tier L4 HA, IAM, compose app image build, OTLP collector target — target-runtime proof pending. Turnkey static+admission verifier ships: scripts/verify-tier-target.sh (static rc=0 this turn on canonical topology; --context <ctx> runs server-dry-run on VM-plane; L4 rollout stays pending) |
