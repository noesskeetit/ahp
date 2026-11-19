# space-product — migration handoff (2026-10-30, laptop -> Cloud.ru VM)

Direct user order: relocate the swarm off the laptop to the Cloud.ru VM. No new local
build/test/model workers. Preserve uncommitted changes; do NOT revert the worktree.
Place-of-execution change only; the whole PDF rev3.2 task remains OPEN.

## Exact worktree / branch / HEAD
- Build worktree (writable, active): `/Users/aogabbasov/AI Agents/ahp/company-brain/space-product-build`
- Branch: `space/candidate-t1b3`
- HEAD: `1331f2c63105649bb10a819a61b5bf698da36c0c` (prior candidate; NOT accepted — root/director SP-1/SP-2)
- Base / prereq pin: `012a849` (integrated seed). Bundle prereq was `012a849`.
- Stale sibling `worktrees/space-product @ 674d388` (branch `swarm/space-product`) is a DIFFERENT old tree — do not graft onto it.

## Uncommitted changes (preserve; carried with the directory, NOT committed per user order)
- `M internal/space/coordinator.go`  (SP-1/SP-2 fixes + dead-code removal). sha256 41c8c899727b1b774233b075242ce0a855e78d51daa4decb95b39c02dda8908a
- `?? tests/space/preflight_fault_test.go` (NEW, untracked; SP fault-injection tests). sha256 5454c3df2aca2049da0e2d7f2f5db18b8429d142ce4b66e10cfd2ace94059e9d
- No other Space-lane diffs. Zero-diff guard confirmed empty for `pkg/auth pkg/transport pkg/contracts internal/state internal/stateapi` (authoritative-identity/State single source preserved).
- NOTE for VM/kvm-lab: the untracked test file travels with the directory. If state is imported via git-bundle only, `git add` it first (or copy the file) so it is not lost. Do not commit until SP tests are green (author!=verifier; no self-accept).

## Current task at handoff
Fix the two source-confirmed defects from the 1331f2c review (root
`acceptance/integration/root-reviews/space-1331f2c-execution-preflight-review.json` + director
`reports/director/reviews/space-1331f2c-director-review.json`):
- SP-1 (`executeTurn`): Core goroutine started BEFORE the OG-2 high-water scan; fail-closed path did not cancel Core (leaked goroutine + side effects) and raced on `stepErr`.
- SP-2 (`coordinator.go` cold rehydrate): transient `ListCheckpoints` error was treated as "no checkpoint" -> silent input-replay.

## What is DONE (in the dirty tree, not yet committed)
- Cold `ListCheckpoints` moved into `executeTurn` PREFLIGHT, before Core; error -> `failPreflight`
  (mark failed + Release, NO stream append); empty+nil = legit "no checkpoint".
- OG-2 `stepSeqHighWater` scan also runs in preflight; error/incomplete -> `failPreflight`.
- Core launched on `coreCtx = context.WithCancel(ctx)`; `emit()` on AppendStream error sets
  `emitErr` AND calls `coreCancel()` (a durable stream hole STOPS execution, not just emission).
- Results loop has no `break` (drains to close); `stepErr := <-done` JOIN after loop -> no data race,
  no goroutine leak. Terminal switch orders `emitErr` (failed) BEFORE cancel.
- Removed now-dead private `emitStatus` (was the preflight append path the reviews wanted gone).
- New `tests/space/preflight_fault_test.go`: isolated fault-injection fake State + counting core.
  Cases: SP-2 checkpoint-read fault, SP-1 high-water fault, plus a positive-control happy path
  (proves the harness reaches Core, so the zero-call assertions are non-vacuous).

## Last VERIFIED result (before stop)
- `go build ./internal/space/... ./cmd/as-runner/...` -> exit 0 (after dead-code removal).
- `gofmt -w` applied to both touched files.
- `go vet ./tests/space/` -> exit 0 (imports fixed to `github.com/ahp-platform/ahp/...`).
- NOT yet run: `go test` of the new SP fault tests (the stop order arrived first).

## Active child PIDs / sessions
- None. All commands this turn ran synchronously and returned; no background build/test/model
  worker of mine is running. Nothing to drain.

## Next steps ON VM (do these first, in order)
1. `source` the build env (GOCACHE/GOTMPDIR/GOMODCACHE + GOPROXY/GOSUMDB off/GOFLAGS=-mod=mod
   GOTOOLCHAIN=local PATH=/opt/homebrew/bin) per `/private/tmp/space-env.sh` (recreate on VM; do
   NOT override HOME/CODEX_HOME). Heavy builds serialize via the shared flock `/private/tmp/ahp-build.lock`.
2. `go build ./...` + `go vet ./internal/space/... ./tests/space/... ./adapters/emil/... ./cmd/as-runner/...`.
3. Offline: `go test -race -count=1 ./internal/space/... ./tests/space/... ./adapters/emil/... ./cmd/as-runner/...`.
   Expect the 3 new SP tests green; confirm the rest still green.
4. ONE real-PG run (no x3): load `private/acceptance-pg/space.env` (DB `ahp_hive_space_live` :25495;
   do NOT print creds, do NOT rename DB), then
   `go test -tags state_integration -race -count=1 -timeout 420s ./tests/space/...`.
   The director-required production-mode joint test (RequireRunGrant=true, real Identity
   exchange/delegation, real State PG+mTLS, cold restart, SP-1/SP-2 fault injections) is STILL TO BE
   ADDED alongside the offline tests (owner-test boundary: the existing realstate owner test is
   lease.Scope/claim-only, NOT production-mode/crash-consistency).
5. Graft/commit over `1331f2c`, then `git bundle` to `acceptance/bundles/space-candidate-<sha>-<UTC>.bundle`
   (prereq `012a849`, verify vs `986ac0a`), forum post with sha + `list-heads`. Status
   `ready_for_director_review` only; IV verifies independently (PG/mTLS, two-subject cold, negatives).

## Boundaries preserved
- Owns only: internal/space, internal/core, cmd/as-runner, web, examples, sdk/go/agent, tests/space,
  docs/space.md, adapters/emil, tests/e2e/whole-system.mjs. No shared `cmd/ahp`. No foreign reverts.
- pkg/auth single source = Identity (2c42002 canonical, JTI omitempty). Do not diverge.
- Emil name/tag/:18000 is NOT a gate. Real model/tool/artifact/durable/fail-closed required; never
  httptest/newMockState/stand-in :19081 as live proof. Requirements = PDF rev3.2 only.
- Whole prototype remains UNACCEPTED until root; do NOT mark any goal complete.
