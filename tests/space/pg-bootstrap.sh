#!/usr/bin/env bash
# Reproducible PG bootstrap for the `state_integration` Space tests.
#
# Why this exists: on Docker Desktop for macOS a host->published-port TCP
# connection is NAT'd, so Postgres inside ahp-handbook-go-postgres-1 sees the
# Docker gateway source, NOT 127.0.0.1/::1. The trust lines in pg_hba.conf only
# match loopback, so such connections fall through to `host all all all
# scram-sha-256`. pgx therefore must authenticate. We satisfy that with SCRAM
# (a challenge/response: the plaintext password never crosses the host
# boundary, only the SCRAM proof) delivered via a PGPASSFILE so the DSN stays
# password-free exactly as the harness dbUrl produces it.
#
# This touches ONLY the allowlisted, per-worktree role/db it is given. It never
# prints the password. Idempotent.
set -euo pipefail

DB="${1:-ahp_hive_space_product}"
CONTAINER="${AHP_HIVE_PG_CONTAINER:-ahp-handbook-go-postgres-1}"

# Mirror the harness allowlist so this script cannot touch foreign roles.
if ! printf '%s' "$DB" | grep -Eq '^ahp_hive_[a-z][a-z0-9_]{0,23}$'; then
  echo "refusing: db name not in allowlist ^ahp_hive_[a-z][a-z0-9_]{0,23}$" >&2
  exit 2
fi

PASSFILE="${TMPDIR:-/tmp}/pgpass-${DB}"
umask 077
pw="$(openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 28)"

# Change the role password via STDIN (never argv, never stdout).
docker exec -i "$CONTAINER" sh -c 'psql -U "$POSTGRES_USER" -d postgres -v ON_ERROR_STOP=1 -q' \
  <<< "ALTER ROLE ${DB} PASSWORD '${pw}';"

printf '127.0.0.1:25432:%s:%s:%s\n' "$DB" "$DB" "$pw" > "$PASSFILE"
chmod 600 "$PASSFILE"
unset pw

echo "# SCRAM verifier set for role ${DB}; password is in ${PASSFILE} (mode 600, not printed)."
echo "export PGPASSFILE=${PASSFILE}"
echo "export AHP_TEST_DATABASE_URL='postgres://${DB}@127.0.0.1:25432/${DB}?sslmode=disable'"
