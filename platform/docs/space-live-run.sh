#!/usr/bin/env bash
# Repro of the Space live_real journey (REAL services, fail-closed). No secrets are embedded:
# it reads only sanctioned sources. Run from the repo root (a tree containing integrated 012a849).
set +eu +o pipefail

# --- Go build env (offline; -race needs CGO) ---
export GOCACHE="${GOCACHE:-/tmp/gocache-space}" GOTMPDIR="${GOTMPDIR:-/tmp/gotmp-space}"
export GOPROXY=off GOFLAGS=-mod=mod CGO_ENABLED=1 GOTOOLCHAIN=local
unset GOMODCACHE; mkdir -p "$GOTMPDIR"
unset AHP_DB_URL DATABASE_URL PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE AS_ARTIFACT_BUCKET AS_DATA_DATABASE_URL

# --- prerequisites you must provide (paths; never inline the values) ---
: "${AHP_ISOLATED_PG_DSN:?set AHP_ISOLATED_PG_DSN to the isolated ahp_hive_space_product DSN}"
GP="${GP:-/Users/aogabbasov/AI Agents/ahp/go-platform}"           # provides MinIO root creds
AMR="${AMR:-/Users/aogabbasov/agent_memory_research/.env}"          # provides the two native-Qwen settings
getv(){ grep -m1 "^$1=" "$2" 2>/dev/null | cut -d= -f2- ; }

export AHP_TEST_DATABASE_URL="$AHP_ISOLATED_PG_DSN"
export AHP_S3_ENDPOINT="${AHP_S3_ENDPOINT:-http://127.0.0.1:29000}"
export AHP_MINIO_ROOT_USER="${AHP_MINIO_ROOT_USER:-$(getv AHP_MINIO_ROOT_USER "$GP/.env")}"
export AHP_MINIO_ROOT_PASSWORD="${AHP_MINIO_ROOT_PASSWORD:-$(getv AHP_MINIO_ROOT_PASSWORD "$GP/.env")}"
# Native Qwen only (fail-closed gate rejects foreign hosts/models):
export AHP_LLM_BASE_URL="${AHP_LLM_BASE_URL:-$(getv qwen_38_flash_next_endpoint "$AMR")}"
export AHP_LLM_API_KEY="${AHP_LLM_API_KEY:-$(getv aifactory_mlinference_key "$AMR")}"
export AHP_LLM_MODEL="${AHP_LLM_MODEL:-model-run-tqmbu-road}"
export EMIL_BASE_URL="${EMIL_BASE_URL:-http://127.0.0.1:18000}"   # OPTIONAL provenance, never a gate

# reach the public native-Qwen host DIRECTLY (harness proxy may refuse); loopback infra is direct.
export NO_PROXY=".modelrun.inference.cloud.ru,cloud.ru,127.0.0.1,localhost,::1" no_proxy="$NO_PROXY"
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy

# presence self-check (lengths only, never values)
for v in AHP_TEST_DATABASE_URL AHP_S3_ENDPOINT AHP_MINIO_ROOT_USER AHP_MINIO_ROOT_PASSWORD \
         AHP_LLM_BASE_URL AHP_LLM_MODEL AHP_LLM_API_KEY EMIL_BASE_URL; do
  eval "x=\${$v}"; printf 'env %s nonempty=%s\n' "$v" "$([ -n "$x" ] && echo yes || echo no)"; done

cd "$(git rev-parse --show-toplevel 2>/dev/null || echo .)"
go test -race -count=1 -timeout 900s -tags live_real -run 'TestLiveRealEndToEnd' ./tests/space/ -v
