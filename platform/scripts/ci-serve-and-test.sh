#!/usr/bin/env bash
# Delivery integration driver. Builds the service, starts it on the isolated
# block database, then runs the HTTP suites (smoke/e2e/chaos/load) capturing
# evidence to .ahp-evidence/. Background service stays alive only for the
# lifetime of this script invocation (detached daemons do not survive the
# harness session). Never prints secrets.
set -u
cd "$(dirname "$0")/.." || exit 1
export GOCACHE=/tmp/ahp-gocache GOMODCACHE=/tmp/ahp-modcache GOPATH=/tmp/ahp-gopath
EV=.ahp-evidence
mkdir -p "$EV"
mask() { sed -E 's#//[^@/]*:[^@/]*@#//***:***@#g'; }

echo "== build bin/ahp =="
go build -o bin/ahp ./cmd/ahp 2> "$EV/build.log"
BUILD_RC=$?
echo "build rc=$BUILD_RC"
[ "$BUILD_RC" -ne 0 ] && { tail -20 "$EV/build.log" | mask; exit 1; }
./bin/ahp --version 2>&1 | mask

echo "== isolated env (masked) =="
node scripts/ahp-env.mjs 2>&1 | mask

# Bring the isolated DB vars into this shell. Values are written by ahp-env and
# eval'd; they are NOT echoed here.
eval "$(node scripts/ahp-env.mjs --export)"
API_URL="${AHP_API_URL:-http://127.0.0.1:28080}"
HOSTPORT="$(node -e 'const u=new URL(process.argv[1]); process.stdout.write(u.hostname+":"+u.port)' "$API_URL")"
echo "API_URL=$API_URL  listen=$HOSTPORT  db=$AHP_DB_NAME"

: > "$EV/service.log"
# G0 (criterion #3): no checkout bin starts without the native-Qwen gate. Fail-closed.
bash scripts/native-qwen-gate.sh || { echo "ABORT: G0 gate denied bin start" >&2; exit 4; }
AHP_ROLE=all AHP_LISTEN="$HOSTPORT" ./bin/ahp --role all --verbose >>"$EV/service.log" 2>&1 &
SVC_PID=$!
echo "service pid=$SVC_PID"

code=000
for i in $(seq 1 40); do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$API_URL/healthz" 2>/dev/null || echo 000)
  [ "$code" = "200" ] && break
  kill -0 "$SVC_PID" 2>/dev/null || { echo "service died early"; break; }
  sleep 0.5
done
echo "healthz code=$code"
echo -n "readyz: "; curl -s "$API_URL/readyz" | mask; echo

export AHP_API_URL="$API_URL"
export AHP_PID="$SVC_PID"

run() {
  name=$1; shift
  echo "== $name =="
  "$@" > "$EV/$name.log" 2>&1
  r=$?
  tail -12 "$EV/$name.log" | mask
  echo "RC[$name]=$r"
  echo "$r" > "$EV/$name.rc"
}

run smoke npm run --silent smoke
run e2e   npm run --silent test:e2e
run chaos npm run --silent test:chaos
run load  npm run --silent test:load

echo "== SUMMARY =="
for t in smoke e2e chaos load; do echo "$t rc=$(cat "$EV/$t.rc" 2>/dev/null)"; done

echo "== service log tail (masked, OTel spans present => tracing active) =="
tail -8 "$EV/service.log" | mask

kill "$SVC_PID" 2>/dev/null
wait "$SVC_PID" 2>/dev/null
echo "done"
