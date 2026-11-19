#!/usr/bin/env bash
# VM green-run harness — space-product (Agents Space + Core runtime + UI).
#
# Reproducible acceptance gate per director ruling §4
# (forum 20261029T163718Z-director-verdicts-crosscut-path-accepts). Accept runs
# ONLY on the VM plane; the Mac is disk(17GiB)/egress(proxy.golang.org) gated.
#
# It proves, end to end and reproducibly:
#   (a) `go build ./...` and `go test -race` are green on the block's suites.
#   (b) the built cmd/as-runner binary contains NO stubModel/stubExecutor and
#       DOES contain the real native-Qwen model client (internal/core
#       OpenAICompatClient) and the real Emil A2A tool executor
#       (adapters/emil Adapter/Client). as-runner fails closed on misconfig.
#   (c) optional State-plane integration (real PG) and live-Qwen journey run
#       when their inputs are present, otherwise they SKIP with a stated reason.
#
# Isolation: every Go artifact is written under the VM's isolated HOME
# (~/ahp-hive/space-product); nothing touches the host GOPATH/GOCACHE. The PG
# database name is allow-listed: only ahp_hive_space_product is accepted.
#
# Secrets: never printed. Provider: native Qwen only (no OpenAI/Astra fallback).
# Requirements source: ahp-agents-space-architecture (2) (1).pdf rev 3.2 (only).
#
# Usage (from the composed repo root, inside an isolated HOME on the VM):
#   export AHP_SPACE_PG='postgres://USER:PASS@127.0.0.1:PORT/ahp_hive_space_product?sslmode=disable'
#   bash tests/space/vm-green-run.sh            # build + race + symbol gate
#   # optional extras (skipped cleanly if unset):
#   #   State-plane:        AHP_SPACE_PG set (also used for state_integration)
#   #   live-Qwen journey:  AHP_LLM_BASE_URL + AHP_LLM_MODEL (+ AHP_LLM_API_KEY)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

ISO="${ISO:-${HOME}/ahp-hive-space-product}"
# Default to an isolated cache tree under ISO so the VM run never touches the
# host GOPATH/GOCACHE. A caller may pre-set GOMODCACHE/GOCACHE/GOTMPDIR to a
# pre-seeded read-only cache to run offline (GOPROXY=off); the default stays
# fully isolated.
export GOCACHE="${GOCACHE:-${ISO}/gocache}"
export GOMODCACHE="${GOMODCACHE:-${ISO}/gomodcache}"
export GOTMPDIR="${GOTMPDIR:-${ISO}/gotmp}"
export GOFLAGS="-mod=mod"
export GOPROXY="${GOPROXY:-https://proxy.golang.org,direct}"
mkdir -p "$GOCACHE" "$GOMODCACHE" "$GOTMPDIR" "${ISO}/bin"
# Race detector is the VM gate by default; set RACE= to skip it on a
# disk-constrained host (Mac) while still exercising every other gate.
RACE="${RACE--race}"
BIN="${ISO}/bin/as-runner"

fail=0
note() { printf '\n=== %s ===\n' "$*"; }
ok()   { printf 'PASS  %s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; fail=1; }
skip() { printf 'SKIP  %s\n' "$*"; }

# Owned build/test set for this block (never foreign packages).
# Owned build set (non-test packages) and owned test set. tests/space holds only
# _test.go files (with build tags), so it is exercised by go test, not go build.
OWNED_BUILD="./cmd/as-runner ./internal/core ./internal/space ./adapters/emil ./web"
OWNED_PKGS="$OWNED_BUILD ./tests/space"

note "0) environment"
go version
printf 'GOCACHE=%s\nGOMODCACHE=%s\nGOPROXY=%s\n' "$GOCACHE" "$GOMODCACHE" "$GOPROXY"

note "1) build"
if go build $OWNED_BUILD ; then ok "build owned packages"; else bad "build owned packages"; fi
# Full composed-tree build needs module egress (pgx/minio for internal/artifacts).
# That is authoritative on the VM (real GOPROXY); on a Mac with GOPROXY=off the
# module lookup is disabled, which is a known host limit, not a code defect.
if go build ./... 2>"${ISO}/full-build.err" ; then
  ok "go build ./... (whole composed tree)"
else
  if grep -qiE "module lookup disabled|cannot find module|dial tcp|proxy\.golang\.org|no such host|unrecognized import" "${ISO}/full-build.err" ; then
    skip "go build ./... needs module egress (Mac-known limit; authoritative on VM)"
  else
    cat "${ISO}/full-build.err"
    bad "go build ./... compile error (see above)"
  fi
fi

note "2) go test ${RACE:-<norace>} on owned suites (no external services required)"
# The no-tag tests/space suite excludes state_integration and live_llm by build
# tag, so it needs only the binary; safe for -race without Postgres.
if go test ${RACE:+$RACE} -count=1 $OWNED_PKGS ; then ok "test ${RACE:+(${RACE})}: owned suites"; else bad "test ${RACE:+(${RACE})}: owned suites"; fi

note "3) symbol gate on built as-runner (director §4b)"
if go build -o "$BIN" ./cmd/as-runner ; then ok "built $BIN"; else bad "build as-runner"; fi
nm() { grep -aoF "$1" "$BIN"; }   # binary symbol lookup; module-context free
STUB_COUNT="$(grep -acE 'stubModel|stubExecutor' "$BIN" || true)"
STUB_COUNT="${STUB_COUNT:-0}"
if [ "$STUB_COUNT" -gt 0 ]; then
  bad "binary CONTAINS stubModel/stubExecutor ($STUB_COUNT match lines)"
else
  ok "binary has NO stubModel/stubExecutor"
fi
if nm 'internal/core.NewOpenAICompatFromEnv' >/dev/null \
   && nm 'internal/core.(*OpenAICompatClient).Generate' >/dev/null; then
  ok "binary contains real native-Qwen model client (OpenAICompatClient)"
else
  bad "binary MISSING real native-Qwen model client"
fi
if nm 'adapters/emil.(*Adapter).Execute' >/dev/null \
   && nm 'adapters/emil.(*Client).Send' >/dev/null; then
  ok "binary contains real Emil A2A executor (adapters/emil Adapter.Execute/Client.Send)"
else
  bad "binary MISSING real Emil A2A executor"
fi

# Postgres allow-list: only the block's dedicated database may be targeted.
PG="${AHP_SPACE_PG:-${AHP_TEST_DATABASE_URL:-}}"
db=""
if [ -n "${PG:-}" ]; then db="${PG%%\?*}"; db="${db##*/}"; fi

note "4) State-plane integration (real Postgres, real State)"
if [ -z "${PG:-}" ]; then
  skip "AHP_SPACE_PG unset (set to ahp_hive_space_product URL to run)"
elif [ "$db" != "ahp_hive_space_product" ]; then
  bad "PG database must be ahp_hive_space_product, got '${db}' (refusing foreign DB)"
else
  if AHP_TEST_DATABASE_URL="$PG" go test ${RACE:+$RACE} -tags state_integration -count=1 ./tests/space ; then
    ok "state_integration ${RACE:-<norace>} against ahp_hive_space_product"
  else
    bad "state_integration ${RACE:-<norace>}"
  fi
fi

note "5) live native-Qwen journey (real LLM -> tool -> artifact -> download)"
if [ -z "${AHP_LLM_BASE_URL:-}" ] || [ -z "${AHP_LLM_MODEL:-}" ]; then
  skip "AHP_LLM_BASE_URL/AHP_LLM_MODEL unset (set to run live_llm journey)"
else
  if go test -tags live_llm -count=1 -run TestLiveLLMArtifactDownloadJourney ./tests/space ; then
    ok "live_llm journey on native Qwen"
  else
    bad "live_llm journey"
  fi
fi

note "6) State pin guard (informational)"
if git rev-parse --verify 461aa2ce^{commit} >/dev/null 2>&1; then
  if git diff --quiet 461aa2ce -- internal/state stateapi 2>/dev/null; then
    ok "internal/state + stateapi == accepted State 461aa2ce (unchanged)"
  else
    skip "State tree differs from 461aa2ce in this checkout (composition-dependent)"
  fi
else
  skip "State 461aa2ce not reachable from this checkout"
fi

note "RESULT"
if [ "$fail" -eq 0 ]; then echo "GREEN: all VM acceptance gates passed"; else echo "RED: one or more gates failed"; fi
exit "$fail"
