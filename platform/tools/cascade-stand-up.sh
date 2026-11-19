set -eu
# Bring up an ISOLATED composed stand for the deletion-cascade obligation, on ports and
# databases of its own, and never touching the live stand (:18080/:18081/:18082), its PKI or
# its database. Why isolated: the obligation is a deletion, and DeleteSubject/DeleteThread
# measured on a shared stand destroys other lanes' evidence.
#
#   agg   127.0.0.1:18094  mTLS (client certificate required) - State, Identity control,
#                          Artifacts/Data plane, the cascade
#   human 127.0.0.1:18095  TLS, browser edge - login and /auth/token only
#
# Everything it needs is built from THIS repository tree by the caller into $RUNDIR/bin
# (tools/cascade-stand-up.sh expects bin/composed). DSN and passwords are sourced from 0600
# files and never printed; only file NAMES and non-secret names appear in the log.
#
#   usage: sh tools/cascade-stand-up.sh [--restart]
HIVE=/home/alex/ahp-hive
RUNDIR=${AHP_CASCADE_RUN:-$HIVE/run/delivery-cascade-stand}
BIN=${AHP_CASCADE_BIN:-$RUNDIR/bin/composed}
PORT_AGG=${AHP_CASCADE_PORT_AGG:-127.0.0.1:18094}
PORT_HUMAN=${AHP_CASCADE_PORT_HUMAN:-127.0.0.1:18095}
ISO_ENV=${AHP_CASCADE_ISO_ENV:-$HIVE/private/delivery-cascade/iso.env}
MINIO_ENV=${AHP_CASCADE_MINIO_ENV:-$HIVE/env/journey-minio.env}
BUCKET=${AHP_CASCADE_BUCKET:-ahp-cascade-iso}
TENANT=${AHP_CASCADE_TENANT:-ahpcasc}
DEPLOYMENT=${AHP_CASCADE_DEPLOYMENT:-spacesvc}
LOG="$RUNDIR/logs/stand.log"
PKI="$RUNDIR/pki"
mkdir -p "$RUNDIR/logs" "$RUNDIR/pids" "$PKI" "$RUNDIR/evidence"
say(){ printf '%s %s\n' "$(date -u +%FT%TZ)" "$*" | tee -a "$LOG"; }
[ -x "$BIN" ] || { say "REFUSE: $BIN is not built (build ./cmd/ahp from this tree first)"; exit 2; }
[ -f "$ISO_ENV" ] || { say "REFUSE: isolated database env $ISO_ENV missing"; exit 2; }

stop_one(){ # $1 name
  local pidfile="$RUNDIR/pids/$1.pid"
  [ -f "$pidfile" ] || return 0
  local pid; pid=$(cat "$pidfile")
  if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; sleep 2; kill -9 "$pid" 2>/dev/null || true; say "stopped $1 pid=$pid"; fi
  rm -f "$pidfile"
}
if [ "${1:-}" = "--restart" ]; then stop_one agg; stop_one human; fi
for n in agg human; do
  if [ -f "$RUNDIR/pids/$n.pid" ] && kill -0 "$(cat "$RUNDIR/pids/$n.pid")" 2>/dev/null; then
    say "$n already running pid=$(cat "$RUNDIR/pids/$n.pid")"; exit 0
  fi
done

# --- own PKI: one CA, a server cert, and TWO workload identities (the PDF takes the caller's
# deployment from ITS OWN certificate, so the exchange caller needs one of its own) -----------
if [ ! -s "$PKI/ca.crt" ]; then
  ( umask 077; cd "$PKI"
    openssl genpkey -algorithm ed25519 -out ca.key
    openssl req -x509 -new -key ca.key -sha256 -days 7 -subj "/CN=ahp-cascade-ca" \
      -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" -out ca.crt
    openssl genpkey -algorithm ed25519 -out server.key
    openssl req -new -key server.key -subj "/CN=ahp-cascade-server" -out server.csr
    printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\nkeyUsage=critical,digitalSignature\n' > srv.ext
    openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 7 -sha256 -extfile srv.ext -out server.crt
    issue(){ openssl genpkey -algorithm ed25519 -out "$1.key"
      openssl req -new -key "$1.key" -subj "/CN=$2" -out "$1.csr"
      printf 'subjectAltName=URI:spiffe://ahp/tenant/%s/deployment/%s\nextendedKeyUsage=clientAuth\nkeyUsage=critical,digitalSignature\n' "$TENANT" "$3" > "$1.ext"
      openssl x509 -req -in "$1.csr" -CA ca.crt -CAkey ca.key -CAcreateserial -days 7 -sha256 -extfile "$1.ext" -out "$1.crt"; }
    issue client-work ahp-cascade-workload "$DEPLOYMENT"
  ) || { say "REFUSE: PKI generation failed"; exit 3; }
  say "own PKI created (tenant=$TENANT deployment=$DEPLOYMENT) at $PKI"
fi

set -a; . "$ISO_ENV"; set +a
[ -s "$RUNDIR/dev-jwt.seed" ] || { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$RUNDIR/dev-jwt.seed"; chmod 600 "$RUNDIR/dev-jwt.seed"; }
AHP_DEV_JWT_SEED=$(cat "$RUNDIR/dev-jwt.seed"); export AHP_DEV_JWT_SEED
export AHP_ROLE=all AHP_DEV_KID=${AHP_DEV_KID:-dev-1}
export AHP_IDENTITY_EXCHANGER_DEPLOYMENTS="$DEPLOYMENT"
export AHP_TLS_CERT="$PKI/server.crt" AHP_TLS_KEY="$PKI/server.key"
export AHP_S3_REGION=${AHP_S3_REGION:-us-east-1}

# --- object store: the data plane mounts only when the store actually answers ---------------
set -a; . "$MINIO_ENV"; set +a
S3UP=0
for attempt in 1 2; do curl -s -o /dev/null --max-time 20 "${AHP_S3_ENDPOINT:-}/minio/health/live" && { S3UP=1; break; }; sleep 2; done
if [ "$S3UP" = "1" ]; then
  export AHP_ENABLE_DATA=1 AHP_S3_BUCKET="$BUCKET"
  say "object store answers -> data plane enabled (bucket=$BUCKET)"
else
  export AHP_ENABLE_DATA=0
  say "object store DOWN -> data plane NOT mounted; the cascade would be unwired (fails loudly, never silently)"
fi

# --- one browser human of our own, provisioned by the pinned binary itself -------------------
if [ ! -s "$RUNDIR/stand-user.env" ]; then
  ( umask 077; cat > "$RUNDIR/stand-user.env" <<EOF
STAND_SUBJECT=cascade-human
STAND_USERNAME=cascade-human
STAND_PASSWORD=$(python3 -c "import secrets;print(secrets.token_urlsafe(18))")
EOF
  )
  say "stand user generated at $RUNDIR/stand-user.env (value not printed)"
fi
set -a; . "$RUNDIR/stand-user.env"; set +a
AHP_PROVISION_USERS_JSON=$(python3 - <<PY
import json, os
print(json.dumps([{"id": "cascade-human", "tenant": "$TENANT", "subject": os.environ["STAND_SUBJECT"],
  "display_name": "Cascade Stand Human", "email": "cascade@example.invalid",
  "username": os.environ["STAND_USERNAME"], "password": os.environ["STAND_PASSWORD"],
  "roles": ["agent"],
  "permissions": ["space:access","task:delegate","identity:delegate","identity:cancel","artifact:read","state"]}]))
PY
)
export AHP_PROVISION_USERS_JSON
"$BIN" --role provision-browser-users >>"$RUNDIR/logs/provision.log" 2>&1 \
  || { say "REFUSE: user provisioning failed (see logs/provision.log)"; exit 4; }
say "provisioned browser subject=$STAND_SUBJECT tenant=$TENANT"

# the aggregate: mTLS everywhere (the workload identity comes from the client certificate)
AHP_TLS_CA="$PKI/ca.crt" AHP_ENABLE_BROWSER= AHP_ENABLE_DATA="$AHP_ENABLE_DATA" \
  nohup env AHP_DEV_JWT_SEED="$AHP_DEV_JWT_SEED" AHP_ROLE=all AHP_LISTEN="$PORT_AGG" \
  AHP_IDENTITY_EXCHANGER_DEPLOYMENTS="$DEPLOYMENT" AHP_ENABLE_BROWSER=1 \
  AHP_BROWSER_AUDIENCE=ahp-browser AHP_TLS_CERT="$PKI/server.crt" \
  AHP_TLS_KEY="$PKI/server.key" AHP_TLS_CA="$PKI/ca.crt" AHP_S3_REGION="$AHP_S3_REGION" \
  AHP_S3_BUCKET="$BUCKET" AHP_ENABLE_DATA="$AHP_ENABLE_DATA" \
  AHP_TEST_DATABASE_URL="$AHP_TEST_DATABASE_URL" AHP_DATABASE_URL="$AHP_DATABASE_URL" \
  AHP_MINIO_ROOT_USER="$AHP_MINIO_ROOT_USER" AHP_MINIO_ROOT_PASSWORD="$AHP_MINIO_ROOT_PASSWORD" \
  AHP_S3_ENDPOINT="$AHP_S3_ENDPOINT" \
  "$BIN" --role all >>"$RUNDIR/logs/agg.log" 2>&1 &
echo $! > "$RUNDIR/pids/agg.pid"
say "started agg pid=$(cat "$RUNDIR/pids/agg.pid") listen=$PORT_AGG (mTLS)"
# the browser edge: TLS without client authentication, no data plane
# The browser edge starts only after the aggregate is healthy. Both processes run
# the same additive Migrate over one database at startup, and two processes doing
# "CREATE ... IF NOT EXISTS" at the same instant lose PostgreSQL's catalog race
# (measured here: "model migrate: duplicate key value ... pg_namespace_nspname_index
# (SQLSTATE 23505)" killed the second one). Serialising startup is this stand's
# half of the fix; the data plane's half is that a lost catalog race is success.
wait_healthy() {
  port=$1
  attempt=0
  while [ "$attempt" -lt 40 ]; do
    attempt=$((attempt+1))
    # The aggregate demands a client certificate on EVERY request, health included,
    # so a probe without one dies in the handshake and reads as "not healthy".
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 --cacert "$PKI/ca.crt" \
      --cert "$PKI/client-work.crt" --key "$PKI/client-work.key" "https://$port/healthz" 2>/dev/null || true)
    [ "$code" = "200" ] && return 0
    sleep 2
  done
  return 1
}
wait_healthy "$PORT_AGG" || { say "FAIL: $PORT_AGG never became healthy (see logs/agg.log)"; exit 5; }
say "healthy: $PORT_AGG"

nohup env AHP_DEV_JWT_SEED="$AHP_DEV_JWT_SEED" AHP_ROLE=all AHP_LISTEN="$PORT_HUMAN" \
  AHP_ENABLE_BROWSER=1 AHP_BROWSER_AUDIENCE=ahp-browser AHP_ENABLE_DATA=0 \
  AHP_IDENTITY_EXCHANGER_DEPLOYMENTS="$DEPLOYMENT" AHP_TLS_CERT="$PKI/server.crt" \
  AHP_TLS_KEY="$PKI/server.key" \
  AHP_TEST_DATABASE_URL="$AHP_TEST_DATABASE_URL" AHP_DATABASE_URL="$AHP_DATABASE_URL" \
  "$BIN" --role all >>"$RUNDIR/logs/human.log" 2>&1 &
echo $! > "$RUNDIR/pids/human.pid"
say "started human pid=$(cat "$RUNDIR/pids/human.pid") listen=$PORT_HUMAN (browser edge)"

for port in "$PORT_AGG" "$PORT_HUMAN"; do
  ok=0
  for i in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 --cacert "$PKI/ca.crt" \
      --cert "$PKI/client-work.crt" --key "$PKI/client-work.key" "https://$port/healthz" 2>/dev/null || true)
    [ "$code" = "200" ] && { ok=1; break; }
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 --cacert "$PKI/ca.crt" "https://$port/healthz" 2>/dev/null || true)
    [ "$code" = "200" ] && { ok=1; break; }
    sleep 2
  done
  [ "$ok" = "1" ] || { say "FAIL: $port never became healthy (see logs/)"; exit 5; }
  say "healthy: $port"
done
say "ISOSTAND_UP=PASS agg=$PORT_AGG human=$PORT_HUMAN db=${AHP_DB_NAME:-unknown} bucket=$BUCKET binary=$(sha256sum "$BIN" | cut -c1-16)"
