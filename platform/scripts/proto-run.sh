#!/usr/bin/env bash
# Prototype run (UNACCEPTED). Boots one separated role against the isolated
# PostgreSQL (DB-name allowlist ahp_hive_*). The DSN comes from ahp-env.mjs, which
# reads the shared .env (git-ignored) and rewrites the DB name to AHP_DB_NAME.
# Model role requires a NATIVE Qwen endpoint (AHP_LLM_BASE_URL/AHP_LLM_MODEL);
# no OpenAI/Astra fallback is wired or allowed. Secrets stay in the environment.
set -euo pipefail
cd "$(dirname "$0")/.."
ROLE="${1:-state}"; PORT="${2:-18080}"
export AHP_DB_NAME="${AHP_DB_NAME:-ahp_hive_delivery}"
[ -e .env ] || { [ -e ../.env ] && ln -sf ../.env .env || true; }
eval "$(node scripts/ahp-env.mjs --export 2>/dev/null || true)"
if [ -z "${AHP_DATABASE_URL:-}" ]; then echo "ERROR: AHP_DATABASE_URL unavailable (need .env with isolated DSN)" >&2; exit 3; fi
BIN="${AHP_BIN:-./bin/ahp}"; [ -x "$BIN" ] || { echo "build first: bash scripts/proto-build.sh" >&2; exit 2; }
# G0 (criterion #3): no checkout bin starts without the native-Qwen gate. Fail-closed.
bash scripts/native-qwen-gate.sh || { echo "ABORT: G0 gate denied bin start" >&2; exit 4; }
export AHP_DEV_JWT_SEED="${AHP_DEV_JWT_SEED:-$(openssl rand -hex 32)}"
TLS=(); [ -n "${AHP_TLS_CERT:-}" ] && TLS=(--tls-cert "$AHP_TLS_CERT" --tls-key "$AHP_TLS_KEY" --tls-ca "${AHP_TLS_CA:-}")
echo "role=$ROLE listen=:$PORT db=${AHP_DB_NAME}"
exec "$BIN" --role "$ROLE" --listen ":$PORT" "${TLS[@]}"
