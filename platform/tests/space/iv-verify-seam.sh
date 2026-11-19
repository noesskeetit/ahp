#!/usr/bin/env bash
# iv-verify-seam.sh — INDEPENDENT verification harness for the space-product seam
# candidate. Runs every author-side gate from a clean checkout whose history
# already contains base 012a849. Tiers are honestly separated and labelled so a
# verifier never mistakes a lower tier for a higher one:
#
#   T0 offline      : go build ./... + go vet (incl. build tags). No services.
#   T1 conformance  : `go test ./adapters/emil` runs an httptest SERVER over OUR
#                     own internal/space StateClient/Coordinator. => CLIENT/HANDLER
#                     CONFORMANCE ONLY. It is explicitly NOT live proof.
#   T2 real-state   : state_integration seam tests over a REAL PostgreSQL. FAILS
#                     closed if the DSN is missing AND fails if ANY seam test
#                     SKIPS (no silent skip-not-fail). Real State API, fence in
#                     body, OBJECT envelopes, scope from task-token.
#   T3 live journey : delegates to run-live-real.sh (real native-Qwen model + real
#                     save_artifact tool -> real MinIO sha PUT==GET -> real PG
#                     State durable after restart -> private-context isolation).
#                     Emil is an OPTIONAL provenance hop, NOT a gate.
#
# NEVER prints secrets/DSN. Missing dependency => FAIL (never a silent pass).
set -uo pipefail

TIER="${1:-all}"
BASE="${SPACE_VERIFY_BASE:-012a849}"
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
GOCACHE="${GOCACHE:-/tmp/gocache-space}" GOTMPDIR="${GOTMPDIR:-/tmp/gotmp-space}"
export GOCACHE GOTMPDIR GOPROXY="${GOPROXY:-off}" GOFLAGS="${GOFLAGS:--mod=mod}" CGO_ENABLED="${CGO_ENABLED:-1}" GOTOOLCHAIN="${GOTOOLCHAIN:-local}"
# Honor a caller-provided GOMODCACHE (e.g. a writable path on hosts where the
# default module cache is read-only); otherwise fall back to the go default.
: "${GOMODCACHE:=$(go env GOMODCACHE 2>/dev/null)}"
export GOMODCACHE
mkdir -p "$GOTMPDIR"
cd "$REPO"

rc_all=0
say(){ printf '\n== %s ==\n' "$*"; }
fail(){ echo "VERIFY-FAIL: $*"; rc_all=1; }

# prereq: the working tree must contain the authoritative base.
if ! git merge-base --is-ancestor "$BASE" HEAD 2>/dev/null; then
  echo "NOTE: base $BASE is not an ancestor of HEAD=$(git rev-parse --short HEAD); verifying anyway (fetch the candidate bundle over a tree that contains $BASE)."
fi
echo "tip=$(git rev-parse --short HEAD)  base=$BASE"

# Real-Qwen host must be reached directly (corporate HTTPS proxy is unavailable here).
if [ -n "${AHP_LLM_BASE_URL:-}" ]; then
  QH="$(printf '%s' "${AHP_LLM_BASE_URL}" | awk -F/ '{print $3}')"
  export NO_PROXY="localhost,127.0.0.1,::1,${QH},.modelrun.inference.cloud.ru,.cloud.ru${NO_PROXY:+,$NO_PROXY}"
  export no_proxy="$NO_PROXY"
fi

if [ "$TIER" = all ] || [ "$TIER" = offline ]; then
  say "T0 offline: build + vet (no services)"
  go build ./... || fail "go build ./..."
  go vet ./... || fail "go vet ./..."
  go vet -tags state_integration ./tests/space/ ./adapters/emil/ || fail "go vet state_integration"
  go vet -tags live_real ./tests/space/ ./adapters/emil/ || fail "go vet live_real"
fi

if [ "$TIER" = all ] || [ "$TIER" = conformance ]; then
  say "T1 client/handler CONFORMANCE (httptest SERVER over OUR client) -- NOT live proof"
  go test -race -count=1 ./adapters/emil/ || fail "adapters/emil conformance"
fi

if [ "$TIER" = all ] || [ "$TIER" = realstate ]; then
  say "T2 real-state seam over REAL PostgreSQL (fail-closed on missing DSN or any SKIP)"
  DSN="${AHP_TEST_DATABASE_URL:-${DATABASE_URL:-}}"
  if [ -z "$DSN" ]; then
    fail "no PostgreSQL DSN (set AHP_TEST_DATABASE_URL). Real-state seam MUST NOT skip."
  else
    unset DATABASE_URL   # force the isolated DSN only; never fall back to a shared DB
    AHP_TEST_DATABASE_URL="$DSN" \
      go test -race -count=1 -v -tags state_integration \
        ./tests/space/ > /tmp/space-iv-realstate.log 2>&1
    t2=$?
    grep -E '^(=== RUN|--- PASS|--- FAIL|--- SKIP|ok|FAIL)' /tmp/space-iv-realstate.log | grep -vE '^=== RUN' | tail -40
    skips="$(grep -cE '^--- SKIP' /tmp/space-iv-realstate.log || true)"
    passes="$(grep -cE '^--- PASS' /tmp/space-iv-realstate.log || true)"
    echo "real-state: PASS=$passes SKIP=$skips rc=$t2 (log /tmp/space-iv-realstate.log)"
    [ "$t2" -eq 0 ] || fail "real-state seam rc=$t2"
    [ "$skips" -eq 0 ] || fail "real-state seam SKIPPED $skips test(s): fail-closed (a real-state leg must never skip)"
    [ "$passes" -ge 15 ] || fail "expected >=15 real-state PASS, got $passes"
  fi
fi

if [ "$TIER" = live ]; then
  say "T3 LIVE journey (real native-Qwen + save_artifact tool + MinIO sha + durable PG)"
  bash tests/space/run-live-real.sh || fail "live journey (see run-live-real.sh)"
fi

say "RESULT"
if [ "$rc_all" -eq 0 ]; then echo "VERIFY-GREEN tier=$TIER"; else echo "VERIFY-RED tier=$TIER"; fi
exit "$rc_all"
