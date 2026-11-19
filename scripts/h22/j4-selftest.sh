#!/usr/bin/env bash
# helper22: hermetic discriminator self-test of scripts/h22/j4-verify.mjs.
#
# It builds its own out-of-band SQL reader from the repository, uses node and a PostgreSQL
# from the environment, and writes ONLY to a database of its own (ahp_hive_h22j4b), which it
# creates and drops. Nothing outside that database is touched, and no prebuilt binary or
# /dev/shm path is assumed, so the same script must pass on a Mac and on VM2.
#
#   NODE=... AHP_TEST_DATABASE_URL=... scripts/h22/j4-selftest.sh
#   AHP_J4_PG_ADMIN_URL=postgres://user@host:5432/postgres   optional admin DSN override
#
# Exit code is non-zero if any expected FAIL/PASS is not observed.
set -u

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
V="${J4_VERIFY:-$REPO/scripts/h22/j4-verify.mjs}"   # override only for meta-tests of this self-test
[ -f "$V" ] || { echo "SELFTEST: 0 passed / 1 failed (verifier not found at $V)"; exit 1; }

NODE="${NODE:-$(command -v node || true)}"
[ -n "$NODE" ] || { echo "SELFTEST: 0 passed / 1 failed (no node on PATH; set NODE=...)"; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
T="$TMP/h22tools"
if ! (cd "$REPO" && go build -o "$T" ./scripts/h22tools) 2>"$TMP/build.err"; then
  echo "SELFTEST: 0 passed / 1 failed (cannot build scripts/h22tools: $(head -c 200 "$TMP/build.err"))"; exit 1
fi

PGURL="${AHP_J4_PG_ADMIN_URL:-${AHP_TEST_DATABASE_URL:-}}"
[ -n "$PGURL" ] || { echo "SELFTEST: 0 passed / 1 failed (set AHP_TEST_DATABASE_URL or AHP_J4_PG_ADMIN_URL)"; exit 1; }
ADMIN="${PGURL%%\?*}"; ADMIN="${ADMIN%/*}/postgres?sslmode=disable"
DBNAME="${AHP_J4_SELFTEST_DB:-ahp_hive_h22j4b}"
D2="${ADMIN%/*}/$DBNAME?sslmode=disable"
q() { "$T" pg "$1" "$2"; }

PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  [ok]   $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  [BAD]  $1"; }
# expect <description> <file> <regex the file must contain>
expect() { if grep -qE "$3" "$2"; then ok "$1"; else bad "$1 (expected /$3/ in $2)"; tail -3 "$2" | sed 's/^/         /'; fi; }
# expect_rc <description> <actual rc> <expected rc>
expect_rc() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (rc=$2, wanted $3)"; fi; }

echo "### self-test t=$(date -u +%FT%TZ) repo=$REPO node=$($NODE -v) db=$DBNAME"
q "$ADMIN" "DROP DATABASE IF EXISTS $DBNAME" >/dev/null 2>&1
if ! q "$ADMIN" "CREATE DATABASE $DBNAME" >/dev/null 2>&1; then
  echo "SELFTEST: 0 passed / 1 failed (cannot CREATE DATABASE $DBNAME at this PG: privilege)"; exit 1
fi
trap 'q "$ADMIN" "DROP DATABASE IF EXISTS $DBNAME" >/dev/null 2>&1; rm -rf "$TMP"' EXIT

for s in ahp_exec ahp_exec_a ahp_state ahp_artifacts; do q "$D2" "CREATE SCHEMA IF NOT EXISTS $s" >/dev/null; done
for s in ahp_exec ahp_exec_a; do
  q "$D2" "CREATE TABLE IF NOT EXISTS $s.tasks (task_id text PRIMARY KEY, tenant text, deployment text, subject text, caller text, parent_task text DEFAULT '', context_id text, state text, attempts int DEFAULT 1, metadata jsonb, created_at timestamptz DEFAULT now())" >/dev/null
  q "$D2" "CREATE TABLE IF NOT EXISTS $s.operations (key text)" >/dev/null
done
q "$D2" "CREATE TABLE IF NOT EXISTS ahp_state.runs (id text PRIMARY KEY, thread_id text DEFAULT '', task_id text DEFAULT '', status text, sequence bigint DEFAULT 1, created_at timestamptz DEFAULT now(), updated_at timestamptz DEFAULT now(), payload jsonb)" >/dev/null
q "$D2" "CREATE TABLE IF NOT EXISTS ahp_state.stream_events (run_id text, kind text)" >/dev/null
q "$D2" "CREATE TABLE IF NOT EXISTS ahp_artifacts.artifacts (id text, task_id text, tenant text, subject text, object text, expected_sha text, sha256 text, size bigint, version text, sealed_at timestamptz, deleted boolean DEFAULT false, created_at timestamptz DEFAULT now())" >/dev/null

P=11111111-1111-4111-8111-111111111111
C=22222222-2222-4222-8222-222222222222
seed() {
  q "$D2" "DELETE FROM ahp_exec.tasks" >/dev/null
  q "$D2" "DELETE FROM ahp_exec_a.tasks" >/dev/null
  q "$D2" "DELETE FROM ahp_state.runs" >/dev/null
  q "$D2" "DELETE FROM ahp_state.stream_events" >/dev/null
  q "$D2" "DELETE FROM ahp_artifacts.artifacts" >/dev/null
  local ameta="$1" runpay="$2"
  local mb="{\"ahp_parent_task\":\"$P\",\"max_steps\":3,\"step_budget_source\":\"caller\",\"step_budget_effective\":3,\"steps_consumed\":1,\"close_reason\":\"model_finished\"}"
  q "$D2" "INSERT INTO ahp_exec_a.tasks (task_id,tenant,deployment,subject,caller,parent_task,context_id,state,attempts,metadata) VALUES ('$P','dev-alpha','h10dep-a','subj-a','runner','','ctx1','TASK_STATE_COMPLETED',2,'$ameta')" >/dev/null
  q "$D2" "INSERT INTO ahp_exec.tasks (task_id,tenant,deployment,subject,caller,parent_task,context_id,state,attempts,metadata) VALUES ('$C','dev-alpha','h10dep-b','subj-a','a2a','$P','ctx1','TASK_STATE_COMPLETED',1,'$mb')" >/dev/null
  q "$D2" "INSERT INTO ahp_state.runs (id,status,payload) VALUES ('$P','done','$runpay')" >/dev/null
  local evsql; evsql="INSERT INTO ahp_state.stream_events (run_id,kind) VALUES ('$P','model'),('$P','model'),('$P','tool_call')"
  q "$D2" "$evsql" >/dev/null
  q "$D2" "INSERT INTO ahp_artifacts.artifacts (task_id,tenant,subject,object,sha256) VALUES ('$P','dev-alpha','subj-a','x/1','aa')" >/dev/null
}
ARGS="--task $C --parent $P --pg-b $D2 --pg-a $D2 --schema-a ahp_exec_a --expect-budget 3 --tools $T"
run() { "$NODE" "$V" "$@" >"$TMP/out.$CASE" 2>&1; RC=$?; }

CASE=c1; echo "--- case 1: no arguments must be a usage error"; run; expect_rc "case1 rc=2" "$RC" 2
CASE=c2; echo "--- case 2: unknown task id must fail everything"; run --task 00000000-0000-4000-8000-000000000000 --pg-b "$D2" --pg-a "$D2" --schema-a ahp_exec_a --tools "$T"; expect_rc "case2 rc=1" "$RC" 1; expect "case2 zero passes" "$TMP/out.$CASE" '"pass":0'
CASE=c3; echo "--- case 3: complete chain WITHOUT --cancel-task must not be green"; seed '{"max_steps":3,"steps_consumed":2,"close_reason":"model_finished"}' '{"close_reason":"model_finished","max_steps":3}'; run $ARGS; expect "case3 A6b FAILs by default" "$TMP/out.$CASE" '^\[FAIL\] A6b'; expect "case3 A6 PASSes on a real match" "$TMP/out.$CASE" '^\[PASS\] A6 '
CASE=c4; echo "--- case 4: a canceled run with its own reason must PASS A6b"; q "$D2" "INSERT INTO ahp_state.runs (id,status,payload) VALUES ('canc-1','cancelled','{\"close_reason\":\"canceled\"}')" >/dev/null; run $ARGS --cancel-task canc-1; expect "case4 A6b PASS" "$TMP/out.$CASE" '^\[PASS\] A6b'
CASE=c5; echo "--- case 5: SUBSTITUTION on the run plane must FAIL A6b"; q "$D2" "INSERT INTO ahp_state.runs (id,status,payload) VALUES ('canc-2','cancelled','{\"close_reason\":\"model_finished\"}')" >/dev/null; run $ARGS --cancel-task canc-2; expect "case5 A6b catches the substitution" "$TMP/out.$CASE" '^\[FAIL\] A6b'
CASE=c6; echo "--- case 6: SUBSTITUTION on the task plane must FAIL A6b"; q "$D2" "INSERT INTO ahp_exec.tasks (task_id,tenant,deployment,subject,caller,parent_task,context_id,state,attempts,metadata) VALUES ('canc-3','dev-alpha','h10dep-b','subj-a','a2a','$P','ctx1','TASK_STATE_CANCELED',1,'{\"close_reason\":\"model_finished\"}')" >/dev/null; run $ARGS --cancel-task canc-3; expect "case6 A6b catches the substitution on B" "$TMP/out.$CASE" '^\[FAIL\] A6b'
CASE=c7; echo "--- case 7: A6 must FAIL when the run plane journals no reason"; seed '{"max_steps":3,"steps_consumed":2}' '{}'; run $ARGS; expect "case7 A6 FAILs" "$TMP/out.$CASE" '^\[FAIL\] A6 '
CASE=c8; echo "--- case 8: A6 must FAIL when the planes disagree"; seed '{"max_steps":3,"steps_consumed":2,"close_reason":"canceled"}' '{"close_reason":"canceled","max_steps":3}'; run $ARGS; expect "case8 A6 FAILs on disagreement" "$TMP/out.$CASE" '^\[FAIL\] A6 '

echo "SELFTEST: $PASS passed / $FAIL failed"
[ "$FAIL" -eq 0 ]
