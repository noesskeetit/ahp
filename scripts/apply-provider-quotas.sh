#!/usr/bin/env bash
# Apply the committed provider-quota seed to a gateway database and prove the
# result is bounded.
#
# Why a script and not a README line: "no row means unlimited" is the gate's
# documented semantics, so a deployment whose limits live in somebody's terminal
# history is an unbounded deployment that looks configured. This script is the
# reproducible act of configuring, and it exits non-zero if it leaves ANY tenant
# this gateway has served without a finite limit.
#
# DSN resolution (never printed): AHP_QUOTA_DSN, else AHP_DATABASE_URL, else
# AHP_TEST_DATABASE_URL. Run it against the gateway's own database:
#
#   AHP_QUOTA_DSN="$DATABASE_URL" scripts/apply-provider-quotas.sh
#   scripts/apply-provider-quotas.sh infra/quotas/other-provider.sql
#
# Idempotent: the seed is a set of UPSERTs, so a re-deploy re-applies it, and the
# seed's own DO block refuses to publish limits that would leave an observed
# tenant unbounded (see infra/quotas/modelrun-tenant-limits.sql).
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEED="${1:-infra/quotas/modelrun-tenant-limits.sql}"
PROVIDER="${AHP_QUOTA_PROVIDER:-modelrun}"

if [[ -z "${AHP_QUOTA_DSN:-}${AHP_DATABASE_URL:-}${AHP_TEST_DATABASE_URL:-}" ]]; then
  echo "apply-provider-quotas: set AHP_QUOTA_DSN (or AHP_DATABASE_URL / AHP_TEST_DATABASE_URL)" >&2
  exit 2
fi

cd "$REPO_ROOT"

# Offline by default: the tool builds from the module cache, not the network.
export GOPROXY="${GOPROXY:-off}"
export GOFLAGS=

echo "==> apply $SEED (provider=$PROVIDER)"
go run ./scripts/quotactl -action apply -file "$SEED" -provider "$PROVIDER"

echo "==> published limits"
go run ./scripts/quotactl -action list -provider "$PROVIDER"

echo "==> audit (tenants this gateway actually served)"
go run ./scripts/quotactl -action audit -provider "$PROVIDER" -unbounded-only
