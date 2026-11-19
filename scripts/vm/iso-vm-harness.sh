#!/usr/bin/env bash
# delivery lane — VM isolated AHP test harness (root personal VM). Foreground IV use.
# ISOLATION: PostgreSQL databases/roles MUST match ^ahp_hive_[a-z][a-z0-9_]{0,23}$;
# MinIO bucket ns from AHP_TEST_BUCKET_NS; mTLS certs generated under RUNTIME only.
# Third-party containers/DBs are NEVER touched. Secrets come from env only, never logged.
# ENDPOINT IS ENV-ONLY (root directive 2026-10-30): PG host/port/db/role/password and
# every cert/secret path are read from AHP_DELIVERY_ENV_FILE / AHP_* / AHP_PG_* env. There
# is NO hardcoded :25432 / :5432 / password. Root's isolated acceptance-pg (:25495) needs
# only env; the same scripts run against ANY endpoint unchanged. `db-probe` authenticates
# and separates SQLSTATE 28P01 (wrong credential) from connection-refused (wrong endpoint).
set -euo pipefail
CMD="${1:-help}"; shift || true
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./iso-pg-env.sh
. "$HERE/iso-pg-env.sh"
: "${AHP_BUNDLE:?set AHP_BUNDLE=/abs/path/composed-iso29700-<short>.bundle}"
: "${AHP_PREREQ:=8b14a59}"
RUNTIME="${AHP_TEST_RUNTIME:-/var/tmp/ahp-hive-iso}"
DB="${AHP_DB_NAME:-${AHP_TEST_DB:-ahp_hive_delivery}}"
BE_PORT="${AHP_TEST_BACKEND_PORT:-29700}"; OR_PORT="${AHP_TEST_ORIGIN_PORT:-29710}"
BUCKET_NS="${AHP_TEST_BUCKET_NS:-ahp-iso-test}"
BIN="$RUNTIME/bin/ahp-full"; CERTS="$RUNTIME/certs"; CHK="$RUNTIME/checkout"
mkdir -p "$RUNTIME/bin" "$CERTS" "$RUNTIME/logs"

allow_db(){ [[ "$1" =~ ^ahp_hive_[a-z][a-z0-9_]{0,23}$ ]] || { echo "FATAL: DB '$1' not in allowlist ahp_hive_[a-z][a-z0-9_]{0,23}"; exit 3; }; }
env_no_proxy(){ export NO_PROXY='localhost,127.0.0.1,::1'; unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy; }
env_go(){ export GOFLAGS=-mod=mod GOPROXY="${AHP_GOPROXY:-off}" GOMODCACHE="${AHP_GOMODCACHE:-$HOME/go/pkg/mod}" GOCACHE="${AHP_GOCACHE:-/tmp/ahp-gocache}"; env_no_proxy; }

gen_certs(){
  command -v openssl >/dev/null || { echo "FATAL: openssl required"; exit 4; }
  ( cd "$CERTS"
    [ -f ca.key ] || { openssl req -x509 -newkey ed25519 -keyout ca.key -out ca.crt -days 1 -subj "/CN=ahp-iso-ca" -noenc; }
    [ -f server.crt ] || { openssl req -newkey ed25519 -keyout server.key -out server.csr -subj "/CN=ahp-composed-backend" -noenc; printf 'subjectAltName=DNS:ahp-composed-backend,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > srv.ext; openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -extfile srv.ext -days 1 -out server.crt; }
    [ -f client.crt ] || { openssl req -newkey ed25519 -keyout client.key -out client.csr -subj "/CN=ahp-composed-backend" -noenc; printf 'subjectAltName=DNS:ahp-composed-backend\nextendedKeyUsage=clientAuth\n' > cli.ext; openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -extfile cli.ext -days 1 -out client.crt; }
    chmod 600 *.key )
  echo "certs ready: $(ls "$CERTS" | tr '\n' ' ')"
}
import_build(){
  allow_db "$DB"
  command -v git >/dev/null; command -v go >/dev/null
  [ -d "$CHK/.git" ] || { mkdir -p "$CHK"; git init -q "$CHK"; git -C "$CHK" remote add origin "$AHP_BUNDLE"; }
  env_no_proxy
  git -C "$CHK" fetch -q "$AHP_BUNDLE" "refs/heads/swarm/delivery-composed-next" 2>/dev/null || git -C "$CHK" fetch -q "$AHP_BUNDLE" HEAD
  git -C "$CHK" checkout -q -B iso-run FETCH_HEAD
  echo "imported head=$(git -C "$CHK" rev-parse HEAD)"
  ( cd "$CHK"; env_go; go build -trimpath -tags "ahp_exec ahp_data" -o "$BIN" ./cmd/ahp )
  echo "built bin_sha256=$(shasum -a 256 "$BIN" 2>/dev/null || sha256sum "$BIN")"
}
export_certs_env(){ export AHP_TLS_CA="$CERTS/ca.crt" AHP_TLS_SERVER_CERT="$CERTS/server.crt" AHP_TLS_SERVER_KEY="$CERTS/server.key" AHP_TLS_CLIENT_CERT="$CERTS/client.crt" AHP_TLS_CLIENT_KEY="$CERTS/client.key" AHP_UPSTREAM_SERVERNAME=ahp-composed-backend; }
run_backend(){ # foreground mTLS backend :BE_PORT, isolated PG, browser on
  allow_db "$DB"; iso_resolve_pg; export_certs_env; env_no_proxy
  exec "$BIN" --role all --listen ":$BE_PORT" --tls-cert "$AHP_TLS_SERVER_CERT" --tls-key "$AHP_TLS_SERVER_KEY" --tls-ca "$AHP_TLS_CA"
}
run_origin(){ # foreground human-origin :OR_PORT -> mTLS backend (client cert held here only)
  export_certs_env; env_no_proxy
  export AHP_HUMAN_ORIGIN_LISTEN="127.0.0.1:$OR_PORT" AHP_HUMAN_ORIGIN_UPSTREAM="https://127.0.0.1:$BE_PORT" AHP_UPSTREAM_TLS=1
  export AHP_HUMAN_ORIGIN_UI="$CHK/web/static"
  exec node "$CHK/scripts/human-origin.mjs"
}
case "$CMD" in
  gen-certs) gen_certs ;;
  import-build) import_build ;;
  db-probe) iso_db_probe "$BIN" ;;
  run-backend) run_backend ;;
  run-origin) run_origin ;;
  verify) exec node "$CHK/scripts/vm-journey.mjs" ;;
  *) sed -n '1,4p' "$0"; echo "usage: $0 {gen-certs|import-build|db-probe|run-backend|run-origin|verify}"; echo "PG endpoint from env ONLY (AHP_DELIVERY_ENV_FILE / AHP_TEST_DATABASE_URL / AHP_PG_*). ports backend=$BE_PORT origin=$OR_PORT db=$DB bucket_ns=$BUCKET_NS" ;;
esac
