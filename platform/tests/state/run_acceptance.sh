#!/usr/bin/env bash
# Single-command acceptance for the durable State block.
#
# Builds the out-of-process serve binary, vets and runs the Go unit +
# integration suites (real PostgreSQL + real mTLS server + real Ed25519 task
# tokens), then runs the Python LangGraph adapter tests against the serve
# binary. It refuses to run unless the target database matches the allowlisted
# isolated database, so it can never touch a shared or foreign database.
#
# Requirements source: user PDF rev 3.2 only. Credentials are never printed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
fi

export GOCACHE="${GOCACHE:-/tmp/go-cache-state}"

SERVE_BIN="${AHP_ACCEPT_SERVE_BIN:-/tmp/state-serve}"
PYTHON="${AHP_PYTHON:-/tmp/lg-venv/bin/python}"

# --- Isolated-database guard -------------------------------------------------
DSN="${AHP_TEST_DATABASE_URL:-${DATABASE_URL:-}}"
if [ -z "$DSN" ]; then
  echo "acceptance: AHP_TEST_DATABASE_URL is not set" >&2
  exit 1
fi
# Extract only the database name; never echo the credential-bearing URL.
DB_NAME="$(printf '%s' "$DSN" | sed -E 's#^[a-z+]+://[^/]*/##; s#[?#].*$##')"
ALLOWLIST="${AHP_ACCEPT_DB_ALLOWLIST:-ahp_hive_state}"
case ",${ALLOWLIST}," in
  ",${DB_NAME},") ;;
  *)
    echo "acceptance: refusing to run; target db '${DB_NAME}' is not in allowlist '${ALLOWLIST}'" >&2
    exit 1
    ;;
esac
echo "acceptance: isolated database target = ${DB_NAME}"

# --- Build -------------------------------------------------------------------
echo "acceptance: building serve binary"
go build -o "$SERVE_BIN" ./cmd/state-stand

echo "acceptance: go vet"
go vet ./internal/state/... ./internal/stateapi/... ./sdk/go/state/...

# --- Go unit + integration ---------------------------------------------------
echo "acceptance: go test (unit + integration)"
go test -p 1 -count=1 ./internal/state/... ./internal/stateapi/... ./sdk/go/state/...
AHP_STATE_SERVE_BIN="$SERVE_BIN" go test -p 1 -count=1 ./tests/state/...

# --- Python LangGraph adapter ------------------------------------------------
if [ -x "$PYTHON" ]; then
  echo "acceptance: python adapter tests"
  ( cd sdk/python && AHP_STATE_SERVE_BIN="$SERVE_BIN" "$PYTHON" -m unittest -v tests.test_state_adapter )
else
  echo "acceptance: python interpreter '$PYTHON' not found; skipping python adapter" >&2
  exit 1
fi

echo "acceptance: REPORT db=${DB_NAME} go=$(go version | awk '{print $3}') serve=${SERVE_BIN} python=$($PYTHON --version 2>&1)"
echo "acceptance: PASS"
