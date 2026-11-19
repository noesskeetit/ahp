#!/usr/bin/env bash
# Apply the committed Memory capability seed and PROVE the grants landed.
#
# Why a script and not a README line: the plane's review/label/delete/erasure
# actions are refused unless the human's grant carries them, and provisioning
# used to accept any string. An operator who configures this from memory gets a
# working-looking stand whose review step 403s forever. This script is the
# reproducible act of configuring, and it exits non-zero when a targeted user
# does not end up holding every published capability.
#
# DSN resolution (never printed): AHP_MEMORY_DSN, else AHP_DATABASE_URL, else
# AHP_TEST_DATABASE_URL.
#
#   scripts/apply-memory-capabilities.sh "accept"           # reader only? no: full custodian bundle
#   scripts/apply-memory-capabilities.sh "accept,foreign"
#
# Idempotent: the seed UPSERTs on browser_grants_pk, so re-applying after a
# schema rollback is safe and hand-edited rows are not required.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SEED="infra/memory/capability-roles.sql"
USERS="${1:-accept}"

if [[ -z "${AHP_MEMORY_DSN:-}${AHP_DATABASE_URL:-}${AHP_TEST_DATABASE_URL:-}" ]]; then
  echo "apply-memory-capabilities: set AHP_MEMORY_DSN (or AHP_DATABASE_URL / AHP_TEST_DATABASE_URL)" >&2
  exit 2
fi
DSN="${AHP_MEMORY_DSN:-${AHP_DATABASE_URL:-${AHP_TEST_DATABASE_URL:-}}}"

cd "$REPO_ROOT"

# Turn "a,b" into ARRAY['a','b'] for the seed. Quoted, no eval of the values.
USERS_SQL="ARRAY[$(printf "%s" "$USERS" | tr ',' '\n' | sed "s/^ *//; s/ *$//; s/.*/'&'/" | paste -sd, -)]"

echo "==> apply $SEED"
psql "$DSN" -v ON_ERROR_STOP=1 -v memory_users="$USERS_SQL" -f "$SEED"

echo "==> verify (expect 5 capabilities per targeted user)"
EXPECTED=$(($(printf "%s" "$USERS" | tr ',' '\n' | grep -c .) * 5))
GOT=$(psql "$DSN" -Atc "
  SELECT count(*) FROM ahp_identity.browser_grants g
    JOIN ahp_identity.browser_credentials c ON c.user_id = g.user_id
   WHERE g.kind = 'permission'
     AND c.username = ANY(${USERS_SQL}::text[])
     AND g.value IN ('memory','memory:review','memory:label','memory:delete','memory:delete-subject')")
if [[ "$GOT" -lt "$EXPECTED" ]]; then
  echo "apply-memory-capabilities: $GOT grants found, expected at least $EXPECTED" >&2
  exit 1
fi
echo "OK: $GOT memory capability grants present for [$USERS]"
