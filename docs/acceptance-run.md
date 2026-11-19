# Reproducible acceptance run (no secrets in this file)

Whole prototype is UNACCEPTED; a green run below proves only the legs it names.

## 0. Which revision is actually running

```
cd <canon>                       # the mac/product-integration worktree
git rev-parse HEAD
lsof -nP -iTCP:29612 -sTCP:LISTEN   # front PID
lsof -nP -iTCP:29690 -sTCP:LISTEN   # runner PID
go version -m "$(lsof -p <PID> | awk '$4=="txt"{print $NF; exit}')" | grep vcs
```
Accept a live result only when `vcs.revision` of BOTH running binaries equals `HEAD`
and `vcs.modified=false`. An `ldflags` revision stamp alone is not provenance.

## 1. Live user journey (deployed stand)

Canonical, from a checkout that generated its own dev CA (`infra/certs`, git-ignored):

```
cd <canon>
export AHP_DEPLOYED_URL=https://127.0.0.1:29612
export NODE_EXTRA_CA_CERTS=$PWD/infra/certs/ca/ca.crt
export AHP_CANDIDATE_SHA=$(git rev-parse HEAD)
node scripts/journey-deployed.mjs
```

From a clean checkout (no CA private key, which is the normal state outside the
stand's own worktree) present already-issued client certificates instead of minting:

```
export AHP_DEPLOYED_URL=https://127.0.0.1:29612
export AHP_CLIENT_CERT_DIR=/path/to/certs      # needs client.crt + client.key
export AHP_DEPLOYED_CA=/path/to/certs/ca/ca.crt # trust anchor only, no key needed
node scripts/journey-deployed.mjs
```

Recognised overrides (all optional, defaults are this checkout's `infra/certs`):
`AHP_CERT_DIR`, `AHP_CLIENT_CERT_DIR`, `AHP_DEPLOYED_CA`, `AHP_DEPLOYED_CA_DIR`,
`AHP_CA_DIR` / `AHP_CA_FILE` / `AHP_CA_KEY` (minting only).

A pinned certificate is used **as its own identity**: the run reads the SPIFFE id from
the certificate SAN and mints tokens for that tenant/deployment. It cannot widen scope.
Legs that need a *second* identity (foreign-tenant isolation) still require minting; if
no CA key is present they print `SKIPPED (not verified)` and the summary reports
`PASS (n) + k SKIPPED`. A skipped leg is never a passed leg.

## 2. Go gate (isolated database, opt-in live legs)

```
source <test-env>   # exports AHP_TEST_DATABASE_URL, AS_DATA_DATABASE_URL,
                    # AHP_S3_*, AHP_LLM_*, AHP_LIVE_QWEN (values never committed)
go test -count=1 -p 1 -tags ahp_exec,ahp_data ./internal/... ./cmd/...
```
`-p 1` is required: live packages share one isolated database. Tests gated behind
`AHP_LIFECYCLE_LIVE`, `AHP_TOOLGW_LIVE`, `AHP_MEMORY_LIVE`, `AHP_LIVE_QWEN` SKIP when
unset — a SKIP is an unverified behaviour, not a pass.
Do not use `GOFLAGS=-mod=mod` (it mutates `go.mod` and fakes a dirty tree).

## 3. Known non-regressions on the canonical revision

`./internal/compute TestLimitScriptCarriesQuotaLimitsBeforeTheCommand` and the
`TestB13*/TestB134*/TestB137*` family in `./internal/compute/integration` fail identically on
the canonical revision and on its ancestor commit (verified in a throwaway worktree), so
they are pre-existing, not breakage introduced by today's integration. Cause unproven.
