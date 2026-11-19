# delivery acceptance-pack

- status: **ready_for_director_review** (whole prototype UNACCEPTED)
- commit `81328ecae779222e3d06fafa0e88a002ce00406a`  tree `23c7fb9db27f4b35e66fe7bb6206f1576f2203ff`  base `8b14a59`  ranAt 2026-10-30T00:20:39.782Z
- isolation: db `ahp_hive_delivery`  minio `http://127.0.0.1:29000`
- summary: {"run":7,"cited":7,"gated":2}

| id | status | kind | evidence |
| --- | --- | --- | --- |
| G0-native-qwen-classifier | run:pass | run | {"pass":"17","fail":"0"} |
| build-offline-surface | run:pass | run | rc=0 |
| demo-serve-selftest | run:pass | run | {"planes":"readiness=MOUNTED identity=MOUNTED state=MOUNTED data(artifacts)=MOUNTED execution(a2a)=MOUNTED","create-run->PG":"PERSISTED run=20261030T002045.114454000Z_kn7yydu5ti2lac3ru54zsm5ati","emil-hop":"external-gate task=16e651d0-ae0d-414f-bccd-1478a17b787f","DEMO_UI":"green-except-emil-tool"} |
| model-artifact-pg | run:pass | run | {"model->artifact->PG":"passed=12 failed=0","DEMO_UI":"green-except-emil-tool"} |
| separated-roles | run:pass | run | Separated roles (state/identity/model) boot independently and emit per-role OTel spans. / [run-tests] e2e: 1 passed, 0 failed |
| otel-live-trace | run:pass | run | [otel-trace] 9 passed, 0 failed / [run-tests] e2e: 1 passed, 0 failed |
| tier-static-verify | run:pass | run | {"static": {"tier0": {"kinds": ["ConfigMap", "Deployment", "Service", "ServiceAccount"], "objects": 4}, "tier1": {"unit_files": 3}, "tier2a": {"kinds": ["Deployment", "Namespace", "Service", "ServiceAccount"], "objects": 10}, "tier2b": {"kinds": ["HorizontalPodAutoscaler", "NetworkPolicy", "PodDisruptionBudget", "Role", "RoleBinding"], "objects": 13}}} / {"server_dry_run":"not-requested","l4":"pending"} / RESULT: static_rc=0  (L4 target-runtime: PENDING/external unless captured on a real cluster) |
| chaos | cited | cited | 6/6 (46 asserts) @ ck41 @24fb8ce |
| load | cited | cited | 3/3 checkpoint p95<=300ms RSS~18.8MB @ ck41 @24fb8ce |
| smoke | cited | cited | rc=0 prior runs |
| pdf-closure | cited | cited | REPRODUCIBLE every-PDF-section->evidence gate (acc #2): derives 18 top-level sections from the canonical PDF СОДЕРЖАНИЕ (sha 044e..a7e2, shasum-verified) and FAILs if coverage.md omits one. Proven this turn @81328ec: PDF_CLOSURE=PASS sections=18 rc=0; negative (drop a section row) -> FAIL rc=1; integrity (bad sha) -> rc=2. Needs canonical PDF OUTSIDE git, so it is NOT executed inside the offline pack. |
| integrated-crossplane | cited | cited | DIRECTOR independently verified cross-plane GREEN on VM target-runtime @ck42 abcfe84 (memory/artifacts/workspace/state/conformance rc=0) |
| go-build-whole-tree-offline | cited | cited | rc=0 offline @0934ddf (host modcache pre-seeded: go-sdk v1.8.0 + OTel v1.47.0 present) — G-GOSDK/G-OTEL-BUILD CLOSED on host |
| emil-tool-completion | gated | gated | combat Emil-core MODEL_UNAVAILABLE; hop wired+proven, backend needs native-Qwen runtime (G-EMIL-TOOL) |
| whole-system-race | cited | cited | DIRECTOR closed G-WHOLETEST-VM on VM (go1.26.2, -race rc=0 for state/conformance real-PG + workspace PG+MinIO on merged canonical 012a849+3033dd4); ck46 adds only scripts/docs (no product code), so product-code race result applies to this checkout |
| tier2a2b-l4-iam-compose-otlp | gated | gated | Tier L4 HA, IAM, compose app image build, OTLP collector target — target-runtime proof pending. Turnkey static+admission verifier ships: scripts/verify-tier-target.sh (static rc=0 this turn on canonical topology; --context <ctx> runs server-dry-run on VM-plane; L4 rollout stays pending) |
