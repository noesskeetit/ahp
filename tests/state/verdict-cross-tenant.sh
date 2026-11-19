#!/bin/sh
# Verdict leg: TWO TENANTS IN ONE PostgreSQL DATABASE.
#
# Why this leg exists: the composed HTTP stand cannot measure tenant isolation
# in the data plane. Its dev-PKI gives the human and the workload the same
# tenant and admits exactly one exchange, so a human of another tenant is
# refused at AcceptTurn and never reaches the tenant predicate in State. Here
# both tenants are real principals of the same running State server: real
# workload certificates, real Ed25519 task tokens, and every row created by the
# product layer through the HTTP surface. No SQL fixtures, no stand PKI.
#
# The candidate must be frozen: tracked_dirty=0 and untracked=0 are checked, and
# the identity of the tested code is recorded twice - as the test-binary hash
# and as the hash of /proc/<pid>/exe of the running test process.
#
# Hive rule: a FAIL costs a non-zero return code. Every checked fact prints one
# `NAME=...` line, the leg prints CROSS_TENANT=PASS or CROSS_TENANT=FAIL, and
# only PASS exits 0.
#
# Run through the hive build lock (this leg compiles a test binary):
#   python3 /home/alex/ahp-hive/tools/ahp-lock-run.py --lock /home/alex/ahp-hive/run/build.lock \
#     --wait 3600 -- sh /home/alex/ahp-hive/worktrees/state-owner-20261031/tests/state/verdict-cross-tenant.sh
set -u

WT=/home/alex/ahp-hive/worktrees/state-owner-20261031
ENVF=${AHP_STATE_ENV_FILE:-/home/alex/ahp-hive/private/root-acceptance-pg/state-owner.env}
ALLOWLIST=${AHP_STATE_DB_ALLOWLIST:-ahp_hive_state_owner}
RUN_ID=${AHP_STATE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}
OUT_DIR=${AHP_STATE_EVIDENCE_DIR:-/home/alex/ahp-hive/run/state-owner-20261031/evidence}
MIN_CLAIMS=${AHP_STATE_CROSS_TENANT_CLAIMS:-58}
TEST_BIN=${AHP_STATE_CROSS_TENANT_BIN:-/tmp/cross-tenant-test-$RUN_ID}
WORK=/tmp/cross-tenant-$RUN_ID
LOG=$WORK/go-test.log

export PATH=/home/alex/ahp-hive/tools/go/bin:$PATH
export GOROOT=/home/alex/ahp-hive/tools/go
export GOCACHE=${GOCACHE:-/tmp/go-cache-state}
export GOPATH=${GOPATH:-/home/alex/ahp-hive/gopath}
export CGO_ENABLED=1 GOTOOLCHAIN=local GOSUMDB=off GOPRIVATE='*' GOWORK=off
export GOFLAGS="-mod=mod -buildvcs=false"

say() { printf 'cross-tenant: %s\n' "$*"; }

mkdir -p "$WORK" "$OUT_DIR" || exit 2
cd "$WT" || exit 2

HEAD=$(git rev-parse HEAD)
TRACKED_DIRTY=$(git status --porcelain --untracked-files=no | wc -l | tr -d ' ')
UNTRACKED=$(git status --porcelain --untracked-files=normal | grep -c '^??' || true)
UNTRACKED=${UNTRACKED:-0}

DB_NAME=unset
DB_OK=0
if [ -r "$ENVF" ]; then
  set -a
  # shellcheck disable=SC1091
  . "$ENVF"
  set +a
  DSN=${AHP_TEST_DATABASE_URL:-${AHP_DATABASE_URL:-}}
  DB_NAME=${AHP_DB_NAME:-$(printf '%s' "$DSN" | sed -E 's#^[a-z+]+://[^/]*/##; s#[?#].*$##')}
  case ",${ALLOWLIST}," in
    ",${DB_NAME},") DB_OK=1 ;;
  esac
fi

say "compiling the cross-tenant test binary"
BUILD_LOG=$WORK/build.log
env -u AHP_S3_ENDPOINT go test -c -o "$TEST_BIN" ./internal/stateapi > "$BUILD_LOG" 2>&1
BUILD_RC=$?
[ "$BUILD_RC" = 0 ] || sed -n '1,30p' "$BUILD_LOG"

PROC_SHA=not-captured
BIN_SHA=not-computed
RC=1
if [ "$BUILD_RC" = 0 ] && [ -x "$TEST_BIN" ]; then
  BIN_SHA=$(sha256sum "$TEST_BIN" | cut -d' ' -f1)
  say "running TestCrossTenantDataPlaneIsolation against database ${DB_NAME}"
  env -u AHP_S3_ENDPOINT "$TEST_BIN" \
    -test.run '^TestCrossTenantDataPlaneIsolation$' \
    -test.v -test.timeout 25m > "$LOG" 2>&1 &
  TEST_PID=$!
  tries=0
  while [ "$tries" -lt 100 ]; do
    if [ -r "/proc/$TEST_PID/exe" ]; then
      captured=$(sha256sum "/proc/$TEST_PID/exe" 2>/dev/null | cut -d' ' -f1)
      if [ -n "$captured" ]; then PROC_SHA=$captured; break; fi
    fi
    kill -0 "$TEST_PID" 2>/dev/null || break
    tries=$((tries + 1))
    sleep 0.2
  done
  wait "$TEST_PID"
  RC=$?
else
  say "the test binary did not build"
fi

count() { n=$(grep -c -- "$1" "$LOG" 2>/dev/null || true); printf '%s' "${n:-0}"; }
PASS=$(count 'verdict=PASS')
FAIL=$(count 'verdict=FAIL')
CLAIMS=$(sed -n 's/^SUITE|claims=\([0-9]*\).*/\1/p' "$LOG" 2>/dev/null | tail -1)
[ -n "$CLAIMS" ] || CLAIMS=0
SKIPPED=$(count '\-\-\- SKIP')
PANIC=$(count 'panic:')
RACES=$(count 'DATA RACE')
PANIC=$(( ${PANIC:-0} + ${RACES:-0} ))
DATABASE=$(sed -n 's/^EVIDENCE|database=\([^|]*\).*/\1/p' "$LOG" 2>/dev/null | tail -1)
[ -n "$DATABASE" ] || DATABASE=unknown
TENANTS=$(sed -n 's/^EVIDENCE|\(tenant_a=[^|]*|deployment_a=[^|]*|tenant_b=[^|]*|deployment_b=[^|]*\).*/\1/p' "$LOG" 2>/dev/null | tail -1)
[ -n "$TENANTS" ] || TENANTS=unknown

VERDICT=PASS
[ "$BUILD_RC" = 0 ] || VERDICT=FAIL
[ "$RC" = 0 ] || VERDICT=FAIL
[ "$TRACKED_DIRTY" = 0 ] || VERDICT=FAIL
[ "$UNTRACKED" = 0 ] || VERDICT=FAIL
[ "$DB_OK" = 1 ] || VERDICT=FAIL
[ "$FAIL" = 0 ] || VERDICT=FAIL
[ "$SKIPPED" = 0 ] || VERDICT=FAIL
[ "$PANIC" = 0 ] || VERDICT=FAIL
[ "$CLAIMS" -ge "$MIN_CLAIMS" ] || VERDICT=FAIL

EVIDENCE_POINTER=$WORK/evidence-pointer.json
python3 - "$OUT_DIR/cross-tenant-$RUN_ID" "$LOG" "$HEAD" "$TRACKED_DIRTY" "$UNTRACKED" \
  "$DATABASE" "$BIN_SHA" "$PROC_SHA" "$RC" "$BUILD_RC" "$VERDICT" "$RUN_ID" > "$EVIDENCE_POINTER" 2>"$WORK/evidence-error.log" <<'PY'
import hashlib, json, os, sys, datetime
out, log, head, tracked, untracked, database, bin_sha, proc_sha, rc, build_rc, verdict, run_id = sys.argv[1:14]
def now():
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
os.makedirs(out, exist_ok=True)
claims = []
identity = {}
with open(log, encoding='utf-8', errors='replace') as handle:
    for line in handle:
        line = line.strip()
        if line.startswith('ASSERT|name='):
            fields = dict(p.split('=', 1) for p in line.split('|')[1:] if '=' in p)
            claims.append({'ts': now(), 'kind': 'claim', 'run_id': run_id, **fields})
        elif line.startswith('EVIDENCE|') or line.startswith('SUITE|'):
            fields = dict(p.split('=', 1) for p in line.split('|')[1:] if '=' in p)
            identity[fields.pop('kind', None) or line.split('|')[0]] = fields
records = claims + [{
    'ts': now(), 'kind': 'verdict', 'run_id': run_id, 'verdict': verdict,
    'git_head': head, 'tracked_dirty': int(tracked), 'untracked': int(untracked),
    'database': database, 'test_binary_sha256': bin_sha, 'proc_exe_sha256': proc_sha,
    'test_rc': int(rc), 'build_rc': int(build_rc), 'claims': len(claims),
    'pass': sum(1 for c in claims if c.get('verdict') == 'PASS'),
    'fail': sum(1 for c in claims if c.get('verdict') == 'FAIL'),
}]
evidence = os.path.join(out, 'evidence.jsonl')
with open(evidence, 'w', encoding='utf-8') as handle:
    for record in records:
        handle.write(json.dumps(record, sort_keys=True) + '\n')
matrix = os.path.join(out, 'verdict.md')
with open(matrix, 'w', encoding='utf-8') as handle:
    handle.write('# Cross-tenant data plane — %s\n\n' % verdict)
    handle.write('- head `%s` tracked_dirty=%s untracked=%s\n' % (head, tracked, untracked))
    handle.write('- database `%s`\n' % database)
    handle.write('- test binary sha256 `%s`\n' % bin_sha)
    handle.write('- /proc/<pid>/exe sha256 `%s`\n' % proc_sha)
    handle.write('- claims=%d pass=%d fail=%d rc=%s\n\n' % (len(claims), records[-1]['pass'], records[-1]['fail'], rc))
    handle.write('| утверждение | факт | вердикт |\n|---|---|---|\n')
    for claim in claims:
        handle.write('| `%s` | %s | %s |\n' % (claim.get('name'), claim.get('detail'), claim.get('verdict')))
print(json.dumps({'evidence': evidence, 'matrix': matrix,
                  'evidence_sha256': hashlib.sha256(open(evidence, 'rb').read()).hexdigest(),
                  'identity': identity}, sort_keys=True))
PY
EVIDENCE_RC=$?
EVIDENCE_JSON=$(cat "$EVIDENCE_POINTER" 2>/dev/null)
if [ "$EVIDENCE_RC" != 0 ] || [ -z "$EVIDENCE_JSON" ]; then
  say "evidence writing failed (rc $EVIDENCE_RC): $(sed -n '1,5p' "$WORK/evidence-error.log")"
  EVIDENCE_JSON=FAIL
  VERDICT=FAIL
fi

echo "git_head=$HEAD"
echo "tracked_dirty=$TRACKED_DIRTY"
echo "untracked=$UNTRACKED"
echo "database=$DATABASE"
echo "tenants=$TENANTS"
echo "test_binary_sha256=$BIN_SHA"
echo "proc_exe_sha256=$PROC_SHA"
echo "build_rc=$BUILD_RC"
echo "test_rc=$RC"
echo "claims=$CLAIMS"
echo "PASS=$PASS"
echo "FAIL=$FAIL"
echo "SKIP=$SKIPPED"
echo "panic_or_race=$PANIC"
echo "evidence=$EVIDENCE_JSON"
echo "CROSS_TENANT=$VERDICT"
[ "$VERDICT" = PASS ] || exit 1
exit 0
