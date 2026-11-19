# AHP prototype completion implementation plan

> For agentic workers: execute with superpowers:subagent-driven-development; isolate ownership and review actual source plus fresh evidence.

Goal: deliver a working integrated AHP/Agents Space prototype against user PDF rev3.2, selected Downloads/example frontend, with explicitly mocked microVM only.
Architecture: retain implemented Go services and public SDKs; repair broken contracts and connect required external API/user paths. Root integrates separate implementation branches into one accepted runtime; component tests alone never close the project.
Tech stack: Go, PostgreSQL/pgvector, Redis, MinIO, Vault, real Qwen model + MCP, HTML/JS chosen UI, Python LangGraph SDK where required.
Spec: company-brain/private/spec/rev32-paged.txt; independent audit reports/independent-audit-20261102/*.md. PDF remains sole architecture scope; mock-compute and imported frontend are direct user exceptions.

## Constraints and rulings
- No foreign projects/containers, global cleanup, credentials in commits/logs, synthetic LLM successes, or weakening security to get green tests.
- Preserve all Qwen work; freeze its writers, leave product runtimes alive. Base be0cafb4 includes new approval fix; verify rather than rewrite blindly.
- Use real dedicated DBs for integration; independent workers never share mutable schemas. No default-output go build overwriting tracked ahp binary; always -o outside tree.
- All API mutation keys identify normalized requests; retries cannot become new effects. Scope from verified principal; administrative routes cannot use ordinary task privilege.
- PDF permits one State driver for PoC. Broader harness research and real VM comparisons are backlog, not a reason to withhold working prototype.
- No new design questions: user explicitly delegated completion after receiving concrete audit.

## Task ownership, tests and acceptance

- [ ] S1 Security worker: internal/tools approval+credentials, internal/stateapi admin boundaries, internal/memory mutation ledger. Re-run independent audit regressions; test scope/key/expiry/replay, task forbidden admin, changed-payload key refusal. Coordinate cmd/ahp mounts with root. Review then commit.
- [ ] R1 Runtime worker: pkg/sdk, sdk/go, adapters/emil, internal/treedelegate, internal/space cancellation. Reproduce wrong lease/ignored Release/lost ACK/parked HITL; fix claim-bound payload/token/heartbeat/fencing, uncertainty-safe child budget, durable cancellation. Test actual PG/Redis and isolated dropped responses. Commit scoped changes.
- [ ] U1 Frontend worker: web/aifactory, web/embed.go and browser tests. Preserve Downloads/example styling/navigation; repair nonempty list rendering/nested records/digest rejection/session lifecycle; connect original sections to actual supported routes, not merely F1-F12 panel. Add honest mock Compute lifecycle. Coordinate missing human API routes with root. Browser real data/foreign owner checks; commit.
- [ ] I1 Root integration: actual Registry deployment/version to selected agent target, model upstream routing by provider + live labels, public gateway APIs for custom runtimes, real trigger delegation and webhook ingress; eval producer + durable artifacts lifecycle. Write focused regression before each behavioral fix, integrate worker patches with conflict review.
- [ ] I2 Root live acceptance: one documented source commit + front/runner binary identity; selected frontend login→register/deploy real custom agent→task→model/tool/approval→artifact/download→cancel/restart, two subjects, quotas, memory/workspace/triggers/evals. Mock microVM visibly distinguished. Re-run narrow failures, no meaningless full-suite loops.
- [ ] I3 Final system checks: npm run check/smoke when applicable, real dependencies and Redis, targeted negative/chaos/load with unique test scopes. Document true external integration limits; update audit/goal matrix with evidence, not percentages.

## Review focus
1. Lost ACK after side effect: reserve and key persist until reconcile; no fictitious success/refund.
2. Nonempty API data and user operations must work in chosen UI, including existing-file authorization.
3. Privileged actions and user-owned connector credentials retain principal/tenant boundaries across every process.
4. Actual selected image/provider identity must affect execution, not only database bookkeeping.
5. Restart/lost lease stops stale execution; durable terminal failures propagate to user.

## Execution ledger
- 2026-11-02 root takeover: independent audit complete, product source baseline be0cafb4; Qwen freeze requested. Existing approval commit must be independently validated. Work occurs in separate root-* product worktrees; live stand remains until replacement proven.
