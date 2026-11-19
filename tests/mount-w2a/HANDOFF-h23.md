# helper23 — mount registry+triggers into cmd/ahp (HANDOFF, partial)

Status: INTERROGATED at 19:50Z, returned with no code changes. Base e91cc5bd,
branch mount-registry-triggers-helper23. Working tree clean. This file is the
only artifact on the branch (design findings for the continuator).

## Root finding (the actual gap)
- internal/registry and internal/triggers are DB Store / Service packages with
  NO HTTP handlers. registry "routes.go" = data model-routes, not HTTP. So the
  composed front (cmd/ahp --role all) genuinely cannot serve catalog/triggers.
- Fix requires thin HTTP route-adapters (new) + a mount call in serveAll.

## Mount pattern to copy (cmd/ahp/main.go serveAll)
- baseMux() then mux.Handle(prefix, handler) per plane; planes mounted BEFORE
  State face; gates AHP_ENABLE_* default OFF; tryMountData/Execution/Workspace
  return errFeatureNotBuilt in untagged build.
- stateapi/identityapi mounted with authorize + verifier (devAuth,
  devIdentityAuth); mTLS enforced at runServer via tlsCfg (buildTLSConfig).
- Handlers wrapped by aot.HTTPMiddleware(role) at the end.
- Tenant-scope: registry methods take explicit tenant; triggers use
  contracts.Scope{Tenant,Deployment,Subject}. Derive scope from verified claims.

## Trigger dispatcher
- triggers.Service.ProcessDue(ctx, now, limit) IS the dispatcher cycle (no
  autonomous goroutine). "Mount behind a flag, run in role all" == run
  ProcessDue on a ticker inside role all, gated by AHP_ENABLE_TRIGGERS (or
  similar). Needs Config{Issuer,Dispatcher,Secrets,Tasks,FireTTL}.

## Contract test (goal = through MOUNTED front, not in-process)
- Harness exists: tests/registry-triggers/{helpers,composed,live}_test.go.
  frontClient = mTLS+Bearer+A2A JSON-RPC vs AHP_DEPLOYED_URL; harvest() asserts
  state/steps_consumed/close_reason/artifact/sha256 out-of-band.
- helper15 live_test.go is IN-PROCESS for registry/triggers (svc.ProcessDue).
  h23 gap: drive catalog-list + create-trigger + fire via the mounted front's
  new HTTP endpoints over mTLS, journey-strength asserts, foreign-tenant deny.
- Stand: own DB ahp_hive_h23*, own Vault, port 29830+, containers go-h23-*,
  build -tags 'ahp_exec ahp_data', role all with AHP_TLS_* mTLS. Adapt
  /tmp/h15/{mkres,deploy}-h15.sh + live-env.sh. G0 qwen38 gate:
  scripts/native-qwen-gate.sh (ALLOW required before spend). Secrets via
  /tmp/ahp-test-env.sh only.

## Gates (NOT run — nothing implemented)
- gofmt/vet/build/go test: not run. tests/space: not run.
