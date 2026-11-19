#!/usr/bin/env bash
# CI acceptance gate for the AHP SDK block.
# Runs on Mac (arm64) with no external PostgreSQL required.
# Exit code 0 = all checks pass.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
GOCACHE="${GOCACHE:-/tmp/go-cache}"
export GOCACHE

echo "=== AHP SDK CI Acceptance Check ==="
echo "Repo: $REPO_ROOT"
echo "Go:   $(go version)"
echo

PASS=0
FAIL=0

run() {
  local label="$1"; shift
  echo -n "  [$label] "
  if "$@" >/dev/null 2>&1; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL"
    FAIL=$((FAIL + 1))
  fi
}

echo "--- Build ---"
run "build-all"       go build ./...
run "build-sdk"       go build ./sdk/... ./pkg/sdk/...

echo "--- Unit tests (no DB) ---"
run "test-contracts"   go test ./pkg/contracts/
run "test-auth"        go test ./pkg/auth/
run "test-labels"      go test ./pkg/labels/
run "test-netpolicy"   go test ./pkg/netpolicy/
run "test-transport"   go test ./pkg/transport/
run "test-sdk-state"   go test ./sdk/go/state/
run "test-sdk-integ"   go test ./tests/sdk/

echo "--- Vet ---"
run "vet-all" go vet ./sdk/... ./pkg/sdk/... ./tests/sdk/...

echo "--- ahp-sdk check (self-lint) ---"
run "ahp-sdk-check" go run ./pkg/sdk/cmd/ahp-sdk-check/ ./pkg/sdk/ ./sdk/

echo "--- Python conformance (real LangGraph 0.6.11) ---"
PYTHON="${AHP_PYTHON:-/tmp/ahp-sdk-venv/bin/python}"
if [ -x "$PYTHON" ]; then
  run "python-conformance" env PYTHONPATH="$REPO_ROOT/sdk/python" "$PYTHON" -m pytest "$REPO_ROOT/sdk/python/tests/" -q --ignore="$REPO_ROOT/sdk/python/tests/test_real_stand.py"
else
  echo "  [python-conformance] SKIP (no Python at $PYTHON; set AHP_PYTHON)"
fi

echo
echo "Results: $PASS passed, $FAIL failed"
if [ "$FAIL" -gt 0 ]; then
  echo "FAILED"
  exit 1
fi
echo "ALL CHECKS PASSED"
