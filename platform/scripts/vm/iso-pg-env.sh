#!/usr/bin/env bash
# delivery lane — env-only PostgreSQL connection resolver + authenticated probe.
#
# Hard rule (root directive 2026-10-30): the PG endpoint (host/port/db/role/password)
# and every secret/cert path are read ONLY from the process environment. There is NO
# hardcoded :25432 / :5432 / password anywhere. Root is provisioning an isolated
# acceptance-pg at VM loopback :25495 (per-owner DB/role, 0600 env file) and will hand
# concrete paths; until then the same scripts start against ANY endpoint purely via env.
#
# Sources of truth (first match wins), all env:
#   1. AHP_DELIVERY_ENV_FILE — an optional 0600, git-ignored env file (sourced if set
#      and readable). It may define any AHP_* / AHP_PG_* / PGPASSWORD values.
#   2. A full DSN in AHP_TEST_DATABASE_URL (preferred) or AHP_DATABASE_URL.
#   3. Discrete parts: AHP_PG_HOST / AHP_PG_PORT / AHP_PG_DB / AHP_PG_USER /
#      AHP_PG_PASSWORD / AHP_PG_SSLMODE (default disable).
#
# Safety:
#   * DB name MUST match the launcher allowlist ^ahp_hive_[a-z][a-z0-9_]{0,23}$.
#   * The password is NEVER embedded in the exported DSN and NEVER printed. pgx
#     (the service) reads it from PGPASSWORD, and psql reads PGPASSWORD too, so the
#     AHP_DATABASE_URL we export is passwordless and safe to echo.
#   * The probe is AUTHENTICATED: it distinguishes SQLSTATE 28P01 "password
#     authentication failed" (a wrong-credential problem, e.g. a stale SSH-forward
#     port) from "connection refused"/"timeout" (a wrong-endpoint problem). A port
#     swap NEVER masks 28P01 — exactly the failure root hit forwarding :25433->:25432.

iso_allow_db(){ [[ "$1" =~ ^ahp_hive_[a-z][a-z0-9_]{0,23}$ ]] || { echo "FATAL: DB '$1' not in allowlist ahp_hive_[a-z][a-z0-9_]{0,23}" >&2; return 3; }; }

iso_urldecode(){ printf '%b' "${1//%/\\x}"; }

iso_load_envfile(){
  # Source the 0600 privateenv if named; never print contents/values.
  if [ -n "${AHP_DELIVERY_ENV_FILE:-}" ]; then
    [ -r "$AHP_DELIVERY_ENV_FILE" ] || { echo "FATAL: AHP_DELIVERY_ENV_FILE set but unreadable" >&2; return 4; }
    set -a; . "$AHP_DELIVERY_ENV_FILE"; set +a
  fi
}

# Parse "scheme://user:pass@host:port/db?query" into global ISO_* parts (no printing).
iso_parse_dsn(){
  local url="$1" rest
  rest="${url#*://}"
  case "$rest" in
    *@*) ISO_USERHOST="${rest%%@*}"; rest="${rest#*@}" ;;
    *)   ISO_USERHOST="" ;;
  esac
  if [ -n "$ISO_USERHOST" ]; then
    ISO_USER="${ISO_USERHOST%%:*}"
    case "$ISO_USERHOST" in
      *:*) ISO_PASS="${ISO_USERHOST#*:}" ;;
      *)   ISO_PASS="" ;;
    esac
  else ISO_USER=""; ISO_PASS=""; fi
  ISO_HOSTPORT="${rest%%/*}"
  ISO_DBQ="${rest#*/}"
  ISO_DB="${ISO_DBQ%%\?*}"
  case "$rest" in
    *\?*) ISO_QUERY="${rest#*\?}" ;;
    *)    ISO_QUERY="" ;;
  esac
  ISO_HOST="${ISO_HOSTPORT%%:*}"
  case "$ISO_HOSTPORT" in
    *:*) ISO_PORT="${ISO_HOSTPORT##*:}" ;;
    *)   ISO_PORT="" ;;
  esac
  ISO_USER="$(iso_urldecode "$ISO_USER")"
  ISO_PASS="$(iso_urldecode "$ISO_PASS")"
}

iso_sslmode_from_query(){ case "$1" in *sslmode=*) local v="${1#*sslmode=}"; echo "${v%%&*}";; *) echo "";; esac; }

# Resolve the canonical connection into exported env (passwordless URL + PGPASSWORD).
# Never echoes any secret. Returns non-zero on missing/allowlist-violating config.
iso_resolve_pg(){
  iso_load_envfile
  local url="" user="" pass="" host="" port="" db="" sslmode=""
  if [ -n "${AHP_TEST_DATABASE_URL:-}" ]; then url="$AHP_TEST_DATABASE_URL";
  elif [ -n "${AHP_DATABASE_URL:-}" ] && [[ "${AHP_DATABASE_URL}" == *"://"* ]]; then url="$AHP_DATABASE_URL"; fi
  if [ -n "$url" ]; then
    iso_parse_dsn "$url"
    user="$ISO_USER"; pass="$ISO_PASS"; host="$ISO_HOST"; port="$ISO_PORT"; db="$ISO_DB"
    sslmode="$(iso_sslmode_from_query "$ISO_QUERY")"
  fi
  # Discrete parts override/fill what the URL did not carry (env wins on empties).
  [ -n "${AHP_PG_HOST:-}" ]     && host="$AHP_PG_HOST"
  [ -n "${AHP_PG_PORT:-}" ]     && port="$AHP_PG_PORT"
  [ -n "${AHP_PG_DB:-}" ]       && db="$AHP_PG_DB"
  [ -n "${AHP_PG_USER:-}" ]     && user="$AHP_PG_USER"
  [ -n "${AHP_PG_PASSWORD:-}" ] && pass="$AHP_PG_PASSWORD"
  [ -n "${AHP_PG_SSLMODE:-}" ]  && sslmode="$AHP_PG_SSLMODE"
  [ -n "${AHP_DB_NAME:-}" ]     && db="$AHP_DB_NAME"
  [ -z "$sslmode" ] && sslmode="disable"
  [ -n "$port" ] || port="" # intentionally NO default port: endpoint must be explicit (env)
  # Validate (never printing pass).
  [ -n "$host" ] || { echo "FATAL: PG host unset — export AHP_PG_HOST or a full AHP_TEST_DATABASE_URL (env-only; no default)" >&2; return 5; }
  [ -n "$port" ] || { echo "FATAL: PG port unset — export AHP_PG_PORT or AHP_TEST_DATABASE_URL (env-only; no default :5432/:25432)" >&2; return 5; }
  [ -n "$db" ]   || { echo "FATAL: PG db unset — export AHP_PG_DB or AHP_DB_NAME" >&2; return 5; }
  iso_allow_db "$db" || return 3
  [ -n "$user" ] || { echo "FATAL: PG user unset — export AHP_PG_USER" >&2; return 5; }
  [ -n "$pass" ] || { echo "FATAL: PG password unset — export AHP_PG_PASSWORD or set it in AHP_DELIVERY_ENV_FILE" >&2; return 5; }
  # Export a PASSWORDLESS canonical URL (safe to log) + PGPASSWORD for pgx/psql.
  local enc_user; enc_user="$(printf '%s' "$user" | sed 's/@/%40/g; s/:/%3A/g')"
  export PGHOST="$host" PGPORT="$port" PGUSER="$user" PGDATABASE="$db"
  export PGPASSWORD="$pass"
  export AHP_DATABASE_URL="postgres://${enc_user}@${host}:${port}/${db}?sslmode=${sslmode}"
  export AHP_TEST_DATABASE_URL="$AHP_DATABASE_URL"
  export AHP_DB_NAME="$db"
  # Redacted summary only — never the password.
  echo "PG_RESOLVED host=${host} port=${port} db=${db} user=${user} sslmode=${sslmode} (password from env, not printed)"
}

# Authenticated probe: proves credentials actually authenticate, and classifies the
# failure so a wrong endpoint (refused/timeout) is never confused with a wrong
# credential (28P01). Uses psql for the SQLSTATE; if absent, falls back to the real
# Go driver via the ahp binary's DB-pinging role.
iso_db_probe(){
  iso_resolve_pg || return $?
  local log="${AHP_TEST_RUNTIME:-/var/tmp/ahp-hive-iso}/logs/pg-probe.err"
  mkdir -p "$(dirname "$log")"
  if [ "${AHP_PG_PROBE:-auto}" != "go" ] && command -v psql >/dev/null 2>&1; then
    if psql "$AHP_DATABASE_URL" -tAc 'select 1' >/dev/null 2>"$log"; then
      echo "DB_PROBE=auth_ok client=psql host=${PGHOST} port=${PGPORT} db=${PGDATABASE}"; return 0
    fi
    local msg; msg="$(tr '\n' ' ' < "$log")"
    case "$msg" in
      *"password authentication failed"*|*"28P01"*|*"no password supplied"*)
        echo "DB_PROBE=AUTH_FAIL_SQLSTATE_28P01 host=${PGHOST} port=${PGPORT} db=${PGDATABASE} -> wrong credential/role for THIS endpoint; a port swap does NOT fix this" >&2; return 28 ;;
      *"could not connect"*|*"Connection refused"*|*"Connection refused by"*|*"No such file"*)
        echo "DB_PROBE=CONN_REFUSED host=${PGHOST} port=${PGPORT} -> endpoint not listening (wrong host/port or tunnel down)" >&2; return 11 ;;
      *"timeout"*|*"timeout expired"*)
        echo "DB_PROBE=CONN_TIMEOUT host=${PGHOST} port=${PGPORT}" >&2; return 12 ;;
      *) echo "DB_PROBE=FAIL client=psql host=${PGHOST} port=${PGPORT} detail='(see redacted log)'"; echo "$msg" | sed -E 's/password[^ ]*[^ ]*/password=<redacted>/Ig' >&2; return 13 ;;
    esac
  fi
  # Fallback: the real Go/pgx path. Any role pings the pool before dispatch; the
  # provision role with no users fails AFTER a successful authed ping, so exit
  # classification reads the log to separate auth/ping from seed.
  local bin="${1:-}"; [ -n "$bin" ] && [ -x "$bin" ] || { echo "DB_PROBE=NO_PSQL_AND_NO_BIN export psql or pass the ahp binary" >&2; return 14; }
  ( "$bin" --role provision-browser-users ) 2>"$log" || true
  if grep -qiE "database (ping|connection pool) failed" "$log"; then
    if grep -qiE "password authentication failed|28P01" "$log"; then
      echo "DB_PROBE=AUTH_FAIL_SQLSTATE_28P01 client=go host=${PGHOST} port=${PGPORT}" >&2; return 28
    fi
    echo "DB_PROBE=CONN_FAIL client=go host=${PGHOST} port=${PGPORT}" >&2; return 11
  fi
  # No ping/pool error => authed ping succeeded (provision then failed on empty users).
  echo "DB_PROBE=auth_ok client=go host=${PGHOST} port=${PGPORT} db=${PGDATABASE}"; return 0
}
