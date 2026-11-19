#!/usr/bin/env bash
# Real-HTTP acceptance scenario for the frozen State candidate.
#
# What it does, in order:
#   1. loads THIS lane's isolated database env (never printed, never echoed);
#   2. refuses any database that is not on the allowlist;
#   3. builds (or takes) the two binaries: the candidate server and the probe;
#   4. boots the candidate: real HTTPS+mTLS, real PostgreSQL, schema created by
#      the product migrations only - no manual SQL, no mock, no httptest;
#   5. runs the probe: every State operation over the wire, with the real HTTP
#      status, contract error code and observed post-conditions appended to a
#      JSONL evidence file;
#   6. pins candidate identity (head + dirty-diff sha256 + binary sha256).
#
# Concurrency rules: builds/tests are gated by the hive build lock. When this
# script runs inside a locked session (ahp-lock-run sets AHP_LOCK_SESSION and
# AHP_GO_LOCK_HELD) the go shim must not re-acquire the same lock.
#
# Secrets: the DSN, tokens and private keys never reach stdout or the evidence;
# records carry only body hashes. Only the database NAME is printed.
#
# Usage (through the lock):
#   python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock \
#     --wait 1800 -- env -u AHP_S3_ENDPOINT ./tests/state/serve/run-http-e2e.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

# The hive builds modules offline and without embedding VCS stamps.
export GOFLAGS="${GOFLAGS:--mod=mod -buildvcs=false}"

ENV_FILE="${AHP_STATE_ENV_FILE:-/home/alex/ahp-hive/private/root-acceptance-pg/state-owner.env}"
ALLOWLIST="${AHP_STATE_DB_ALLOWLIST:-ahp_hive_state_owner}"
RUN_ID="${AHP_STATE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
OUT_DIR="${AHP_STATE_EVIDENCE_DIR:-/home/alex/ahp-hive/run/state-owner-20261031/evidence}"
SERVE_BIN="${AHP_STATE_SERVE_BIN:-/tmp/state-candidate-$RUN_ID/state-stand}"
PROBE_BIN="${AHP_STATE_PROBE_BIN:-/tmp/state-candidate-$RUN_ID/state-consumer}"
BIN_DIR="$(dirname "$SERVE_BIN")"
EVIDENCE="$OUT_DIR/state-http-e2e-$RUN_ID.jsonl"
MANIFEST="$OUT_DIR/candidate-identity-$RUN_ID.json"
WORK="$(mktemp -d /tmp/state-e2e-$RUN_ID.XXXXXX)"
HANDSHAKE="$WORK/handshake.json"

log() { printf 'state-e2e: %s\n' "$*"; }
die() { printf 'state-e2e: %s\n' "$*" >&2; exit 1; }

# --- isolated database -------------------------------------------------------
[ -r "$ENV_FILE" ] || die "lane env file is not readable: $ENV_FILE"
set -a
# shellcheck disable=SC1091
. "$ENV_FILE"
set +a
DSN="${AHP_TEST_DATABASE_URL:-${AHP_DATABASE_URL:-}}"
[ -n "$DSN" ] || die "the lane env file carries no database URL"
DB_NAME="$(printf '%s' "$DSN" | sed -E 's#^[a-z+]+://[^/]*/##; s#[?#].*$##')"
case ",${ALLOWLIST}," in
  ",${DB_NAME},") ;;
  *) die "refusing to run: database '${DB_NAME}' is not on the allowlist" ;;
esac
log "isolated database = ${DB_NAME}"

# A fresh scope per run keeps ClaimRun (which is deployment-level by contract)
# inside this scenario's own queue, so the evidence can never be polluted by
# leftovers of another run and nothing of another lane is touched.
export AHP_STATE_TENANT="${AHP_STATE_TENANT:-t-${RUN_ID}}"
export AHP_STATE_DEPLOYMENT="${AHP_STATE_DEPLOYMENT:-d-${RUN_ID}}"
export AHP_STATE_SUBJECT="${AHP_STATE_SUBJECT:-user-e2e-${RUN_ID}}"
export AHP_STATE_DSN="$DSN"
export AHP_STATE_HANDSHAKE="$HANDSHAKE"
export AHP_STATE_ADDR="${AHP_STATE_ADDR:-127.0.0.1:0}"

# --- build -------------------------------------------------------------------
mkdir -p "$BIN_DIR" "$OUT_DIR"
if [ ! -x "$SERVE_BIN" ] || [ ! -x "$PROBE_BIN" ]; then
  log "building candidate + probe (lock session: ${AHP_LOCK_SESSION:-outside})"
  # No -trimpath: this is how the platform is built everywhere else, so the
  # director's binary and this one are produced the same way. Binary hashes are
  # therefore NOT comparable across machines - the candidate is identified by
  # its git head + dirty-diff hash, which the manifest below records.
  go build -o "$SERVE_BIN" ./cmd/state-stand
  go build -o "$PROBE_BIN" ./cmd/state-consumer
fi
[ -x "$SERVE_BIN" ] || die "candidate binary is missing: $SERVE_BIN"
[ -x "$PROBE_BIN" ] || die "probe binary is missing: $PROBE_BIN"
chmod 700 "$WORK"

# --- boot the candidate ------------------------------------------------------
log "starting candidate (HTTPS+mTLS, product migrations, real PG)"
env -u AHP_S3_ENDPOINT "$SERVE_BIN" >"$WORK/serve.log" 2>&1 &
SERVE_PID=$!
cleanup() {
  kill "$SERVE_PID" 2>/dev/null || true
  wait "$SERVE_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

ready=0
for _ in $(seq 1 240); do
  if grep -q '^READY$' "$WORK/serve.log" 2>/dev/null; then ready=1; break; fi
  if ! kill -0 "$SERVE_PID" 2>/dev/null; then break; fi
  sleep 0.5
done
if [ "$ready" != 1 ]; then
  # The bridge prints only its own error text; no credential is ever echoed.
  sed -n '1,20p' "$WORK/serve.log" >&2 || true
  die "candidate did not become ready"
fi
log "candidate ready (pid $SERVE_PID, address $(sed -n 's#.*"base_url":"\(https://[^"]*\).*#\1#p' "$HANDSHAKE" | head -1))"

# --- run the probe -----------------------------------------------------------
log "running the wire probe -> $EVIDENCE"
rc=0
env -u AHP_S3_ENDPOINT "$PROBE_BIN" \
  -handshake "$HANDSHAKE" \
  -evidence "$EVIDENCE" \
  -worker "w-${RUN_ID}" || rc=$?

# --- candidate identity ------------------------------------------------------
if [ -x "$(command -v python3)" ] && [ -f /home/alex/ahp-hive/tools/candidate-manifest.py ]; then
  python3 /home/alex/ahp-hive/tools/candidate-manifest.py \
    --worktree "$ROOT" --binary "$SERVE_BIN" --out "$MANIFEST" \
    --note "State candidate: cmd/state-stand + cmd/state-consumer; evidence $EVIDENCE; run $RUN_ID" >/dev/null ||
    log "warning: identity manifest could not be written"
fi

log "evidence:  $EVIDENCE"
log "identity:  $MANIFEST"
log "probe rc:  $rc"
exit "$rc"
