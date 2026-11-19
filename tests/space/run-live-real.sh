#!/usr/bin/env bash
# real-e2e-gate: ONE fail-closed LIVE journey (tag live_real) against REAL
# services only. It reads every secret from the process env and NEVER hardcodes
# or echoes one. Missing service/credentials => the test FAILS (never skips).
#
# Required env (exported by the isolated AHP acceptance runtime):
#   AHP_TEST_DATABASE_URL (or DATABASE_URL)  real PostgreSQL DSN
#   AHP_S3_ENDPOINT                            real MinIO base URL (http/https)
#   AHP_MINIO_ROOT_USER / AHP_MINIO_ROOT_PASSWORD
#   AHP_LLM_BASE_URL / AHP_LLM_MODEL / AHP_LLM_API_KEY   native Qwen ONLY (MANDATORY)
#   EMIL_BASE_URL                              OPTIONAL Emil provenance hop (NOT a gate;
#                                              per decisions/2026-10-30-emil-image-scope-correction.md
#                                              the Emil container name/port/immutability are not product
#                                              requirements -- unset => the Emil hop is skipped-by-design
#                                              and the MANDATORY journey still runs fail-closed).
set -euo pipefail

# Go build env: honour caller overrides, else use a local sandbox-friendly cache.
export GOCACHE="${GOCACHE:-/tmp/gocache-space}"
export GOTMPDIR="${GOTMPDIR:-/tmp/gotmp-space}"
export GOPROXY="${GOPROXY:-off}" GOFLAGS="${GOFLAGS:--mod=mod}" CGO_ENABLED="${CGO_ENABLED:-1}" GOTOOLCHAIN="${GOTOOLCHAIN:-local}"
unset GOMODCACHE || true
mkdir -p "$GOTMPDIR"

for v in AHP_S3_ENDPOINT AHP_MINIO_ROOT_USER AHP_MINIO_ROOT_PASSWORD AHP_LLM_BASE_URL AHP_LLM_MODEL AHP_LLM_API_KEY; do
  if [ -z "${!v:-}" ]; then echo "real-e2e-gate: required env $v is unset (fail-closed)" >&2; exit 2; fi
done
if [ -z "${AHP_TEST_DATABASE_URL:-${DATABASE_URL:-}}" ]; then echo "real-e2e-gate: no PostgreSQL DSN in env (fail-closed)" >&2; exit 2; fi
if [ -z "${EMIL_BASE_URL:-}" ]; then
  echo "real-e2e-gate: EMIL_BASE_URL unset -> Emil provenance hop SKIPPED (Emil is NOT a gate); the MANDATORY path native-Qwen -> save_artifact tool -> real MinIO -> real PG State -> restart -> private-context still runs fail-closed." >&2
fi

# The native Qwen host and local services must be reached directly (the corporate
# HTTPS proxy is unavailable in the isolated runtime). This bypasses ONLY the
# loopback + native-Qwen hosts; traffic still hits the real native Qwen endpoint.
QH="$(printf '%s' "${AHP_LLM_BASE_URL:-}" | awk -F/ '{print $3}')"
export NO_PROXY="localhost,127.0.0.1,::1,${QH},.modelrun.inference.cloud.ru,.cloud.ru${NO_PROXY:+,$NO_PROXY}"
export no_proxy="$NO_PROXY"

cd "$(dirname "$0")/../.."
echo "real-e2e-gate: running TestLiveRealEndToEnd (MANDATORY real Qwen+save_artifact+MinIO+PG+restart+private-context; Emil hop optional, -race)" >&2
exec go test -tags live_real -race -count=1 -v -run TestLiveRealEndToEnd ./tests/space/
