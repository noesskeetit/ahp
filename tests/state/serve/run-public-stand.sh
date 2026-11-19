#!/usr/bin/env bash
# Publish the frozen State candidate as a LONG-LIVED public stand.
#
# This is the leg that makes the candidate exist as an endpoint: it boots
# cmd/state-stand on the agreed stand port (18092) against THIS lane's own
# database, pins the running process (sha256 of /proc/<pid>/exe plus a pin
# manifest with tracked_dirty=0 and untracked=0 and the database name), then
# drives cmd/state-consumer against it as an AUTHOR SELFTEST. The server keeps
# running after the leg exits, so an independent verifier can walk the same
# handshake on the same endpoint.
#
# It does NOT build: pass AHP_STATE_SERVE_BIN / AHP_STATE_PROBE_BIN produced by
# a locked build session. Selftest failures cost a non-zero return code.
#
# Secrets: the handshake file (0600, written by the candidate) is the only place
# credentials live. This leg prints and records PATHS, never contents.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
cd "$ROOT"

ENV_FILE="${AHP_STATE_ENV_FILE:-/home/alex/ahp-hive/private/root-acceptance-pg/state-owner.env}"
ALLOWLIST="${AHP_STATE_DB_ALLOWLIST:-ahp_hive_state_owner}"
ADDR="${AHP_STATE_PUBLIC_ADDR:-127.0.0.1:18092}"
RUN_ID="${AHP_STATE_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
PUB_DIR="${AHP_STATE_PUBLIC_DIR:-/home/alex/ahp-hive/run/state-owner-20261031/stand-18092}"
SERVE_BIN="${AHP_STATE_SERVE_BIN:?AHP_STATE_SERVE_BIN is required (build it under run/build.lock)}"
PROBE_BIN="${AHP_STATE_PROBE_BIN:?AHP_STATE_PROBE_BIN is required (build it under run/build.lock)}"
HANDSHAKE="$PUB_DIR/handshake.json"
EVIDENCE="$PUB_DIR/evidence/consumer-selftest-$RUN_ID.jsonl"
FROZEN="$PUB_DIR/FROZEN-$RUN_ID.json"
STAND_LOG="$PUB_DIR/stand-$RUN_ID.log"
ACCESS="$PUB_DIR/ACCESS-$RUN_ID.md"

log() { printf 'state-stand-18092: %s\n' "$*"; }
die() { printf 'state-stand-18092: %s\n' "$*" >&2; printf 'PUBLIC_STAND=FAIL\n'; exit 1; }

# --- inputs ------------------------------------------------------------------
[ -r "$ENV_FILE" ] || die "lane env file is not readable: $ENV_FILE"
[ -x "$SERVE_BIN" ] || die "candidate binary is not executable: $SERVE_BIN"
[ -x "$PROBE_BIN" ] || die "consumer binary is not executable: $PROBE_BIN"

set -a
# shellcheck disable=SC1091
. "$ENV_FILE"
set +a
DSN="${AHP_TEST_DATABASE_URL:-${AHP_DATABASE_URL:-}}"
[ -n "$DSN" ] || die "the lane env file carries no database URL"
DB_NAME="${AHP_DB_NAME:-$(printf '%s' "$DSN" | sed -E 's#^[a-z+]+://[^/]*/##; s#[?#].*$##')}"
case ",${ALLOWLIST}," in
  ",${DB_NAME},") ;;
  *) die "refusing to run: database '${DB_NAME}' is not on the allowlist" ;;
esac

HEAD=$(git rev-parse HEAD)
TRACKED_DIRTY=$(git status --porcelain --untracked-files=no | wc -l | tr -d ' ')
UNTRACKED=$(git status --porcelain --untracked-files=normal | grep -c '^??' || true)
UNTRACKED=${UNTRACKED:-0}
# A public endpoint is only meaningful if it is traceable to frozen code.
[ "$TRACKED_DIRTY" = 0 ] || die "the worktree is dirty (tracked_dirty=$TRACKED_DIRTY): refusing to publish"
[ "$UNTRACKED" = 0 ] || die "the worktree has untracked files ($UNTRACKED): refusing to publish"

mkdir -p "$PUB_DIR/evidence" || die "cannot create $PUB_DIR"
chmod 700 "$PUB_DIR" || die "cannot chmod $PUB_DIR"

# --- the port must be free, or held by a stand that is already published -----
if [ -f "$PUB_DIR/stand.pid" ]; then
  OLD_PID=$(cat "$PUB_DIR/stand.pid")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    die "a stand is already published on pid $OLD_PID (handshake $HANDSHAKE); stop it first"
  fi
  mv "$PUB_DIR/stand.pid" "$PUB_DIR/stand.pid.stale-$RUN_ID" || die "cannot retire the stale pid file"
fi
if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | awk '{print $4}' | grep -Eq "[:.]${ADDR##*:}\$"; then
  die "port ${ADDR##*:} is already listening"
fi

# --- boot the frozen candidate on the agreed stand port ----------------------
export AHP_STATE_DSN="$DSN"
export AHP_STATE_HANDSHAKE="$HANDSHAKE"
export AHP_STATE_ADDR="$ADDR"
export AHP_STATE_DB_ALLOWLIST="$ALLOWLIST"
export AHP_STATE_TENANT="${AHP_STATE_TENANT:-t-${RUN_ID}}"
export AHP_STATE_DEPLOYMENT="${AHP_STATE_DEPLOYMENT:-d-${RUN_ID}}"
export AHP_STATE_SUBJECT="${AHP_STATE_SUBJECT:-user-accept-${RUN_ID}}"
export AHP_STATE_FOREIGN_TENANT="${AHP_STATE_FOREIGN_TENANT:-t-${RUN_ID}-foreign}"
# Task tokens are capped at 15 minutes by the platform, so the candidate
# re-mints them: an out-of-process verifier that re-reads the handshake always
# finds a live token.
export AHP_STATE_REFRESH_SECONDS="${AHP_STATE_REFRESH_SECONDS:-420}"

log "booting $SERVE_BIN on $ADDR, database $DB_NAME"
setsid nohup env -u AHP_S3_ENDPOINT "$SERVE_BIN" >"$STAND_LOG" 2>&1 < /dev/null &
SERVE_PID=$!
echo "$SERVE_PID" > "$PUB_DIR/stand.pid"

ready=0
for _ in $(seq 1 240); do
  if grep -q '^READY$' "$STAND_LOG" 2>/dev/null; then ready=1; break; fi
  kill -0 "$SERVE_PID" 2>/dev/null || break
  sleep 0.5
done
[ "$ready" = 1 ] || { sed -n '1,20p' "$STAND_LOG" >&2; die "the candidate did not become ready"; }

# The identity of a running server is the binary it was started from, read out
# of /proc while the process is alive.
PROC_EXE_SHA=$(sha256sum "/proc/$SERVE_PID/exe" 2>/dev/null | cut -d' ' -f1)
[ -n "$PROC_EXE_SHA" ] || die "cannot read /proc/$SERVE_PID/exe to freeze the candidate"
FILE_EXE_SHA=$(sha256sum "$SERVE_BIN" | cut -d' ' -f1)
BASE_URL=$(sed -n 's#.*"base_url":"\([^"]*\)".*#\1#p' "$HANDSHAKE" | head -1)
log "candidate ready: pid $SERVE_PID at $BASE_URL"

# --- author selftest over the wire ------------------------------------------
mkdir -p "$(dirname "$EVIDENCE")"
log "running the consumer selftest -> $EVIDENCE"
SELFTEST_LOG="$PUB_DIR/selftest-$RUN_ID.log"
env -u AHP_S3_ENDPOINT "$PROBE_BIN" \
  -handshake "$HANDSHAKE" \
  -evidence "$EVIDENCE" \
  -worker "w-selftest-${RUN_ID}" \
  -note "author selftest against the published stand on port ${ADDR##*:}, run $RUN_ID" \
  > "$SELFTEST_LOG" 2>&1
PROBE_RC=$?
CONSUMER_SUMMARY=$(grep 'state-consumer: calls=' "$SELFTEST_LOG" | tail -1)
[ -n "$CONSUMER_SUMMARY" ] || CONSUMER_SUMMARY="no summary line in $SELFTEST_LOG"
log "selftest rc=$PROBE_RC :: $CONSUMER_SUMMARY"
EVIDENCE_SHA=$(sha256sum "$EVIDENCE" 2>/dev/null | cut -d' ' -f1)
[ -n "$EVIDENCE_SHA" ] || die "the consumer produced no evidence file"

# --- freeze + publish -------------------------------------------------------
MANIFEST="$PUB_DIR/candidate-identity-$RUN_ID.json"
if [ -x "$(command -v python3)" ] && [ -f /home/alex/ahp-hive/tools/candidate-manifest.py ]; then
  python3 /home/alex/ahp-hive/tools/candidate-manifest.py \
    --worktree "$ROOT" --binary "$SERVE_BIN" --out "$MANIFEST" \
    --note "published State stand on port ${ADDR##*:}; selftest evidence $EVIDENCE" >/dev/null ||
    log "warning: hive identity manifest could not be written"
fi

python3 - "$FROZEN" "$RUN_ID" "$HEAD" "$TRACKED_DIRTY" "$UNTRACKED" "$DB_NAME" "$SERVE_PID" \
  "$PROC_EXE_SHA" "$FILE_EXE_SHA" "$BASE_URL" "$ADDR" "$HANDSHAKE" "$EVIDENCE" "$EVIDENCE_SHA" \
  "$PROBE_RC" "$STAND_LOG" "$MANIFEST" "$SERVE_BIN" "$CONSUMER_SUMMARY" <<'PY'
import json, sys, datetime
out, run_id, head, tracked, untracked, database, pid, proc_sha, file_sha, base_url, addr, \
    handshake, evidence, evidence_sha, probe_rc, stand_log, manifest, serve_bin, summary_line = sys.argv[1:19]
ops, seen, records, failed = [], set(), 0, []
try:
    with open(evidence, encoding='utf-8', errors='replace') as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            records += 1
            op = record.get('op')
            if op and op not in seen:
                seen.add(op)
                ops.append(op)
            if record.get('ok') is False:
                failed.append({'seq': record.get('seq'), 'op': record.get('op'),
                               'status': record.get('status'), 'assertions': record.get('assertions')})
except OSError as error:
    print('warning: evidence not readable: %s' % error)
payload = {
    'frozen': 'FROZEN',
    'run_id': run_id,
    'ts': datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    'git_head': head,
    'tracked_dirty': int(tracked),
    'untracked': int(untracked),
    'database': database,
    'serve_binary': serve_bin,
    'pid': int(pid),
    'proc_exe_sha256': proc_sha,
    'binary_file_sha256': file_sha,
    'base_url': base_url,
    'listen_address': addr,
    'handshake_file': handshake,
    'evidence_file': evidence,
    'evidence_sha256': evidence_sha,
    'consumer_rc': int(probe_rc),
    'stand_log': stand_log,
    'identity_manifest': manifest,
    'consumer_summary_line': summary_line,
    'evidence_records': records,
    'evidence_failures': failed,
    'operations_observed': len(ops),
    'operations': ops,
}
with open(out, 'w', encoding='utf-8') as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write('\n')
print(json.dumps({'frozen': out, 'proc_exe_sha256': proc_sha, 'operations': len(ops),
                  'evidence_sha256': evidence_sha, 'consumer_rc': int(probe_rc)}, sort_keys=True))
PY
FROZEN_JSON=$?
[ "$FROZEN_JSON" = 0 ] || die "the FROZEN manifest could not be written"

{
  printf '# State HTTP stand (порт %s) — доступно независимому верификатору\n\n' "${ADDR##*:}"
  printf -- '- эндпоинт: `%s` (HTTPS + обязательный client-сертификат)\n' "$BASE_URL"
  printf -- '- git head кандидата: `%s` (tracked_dirty=%s, untracked=%s)\n' "$HEAD" "$TRACKED_DIRTY" "$UNTRACKED"
  printf -- '- база: `%s` (только имя; DSN не публикуется)\n' "$DB_NAME"
  printf -- '- pid процесса: `%s`, sha256 `/proc/<pid>/exe`: `%s`\n' "$SERVE_PID" "$PROC_EXE_SHA"
  printf -- '- sha256 файла бинаря: `%s`\n' "$FILE_EXE_SHA"
  printf -- '- handshake (0600, ПУТЬ): `%s`\n' "$HANDSHAKE"
  printf -- '- CA / client-сертификат / ключ (ПУТИ): см. поля `ca_file`, `cert_file`, `key_file` handshake\n'
  printf -- '- чужой tenant (для проверки изоляции): поля `foreign_tenant`, `foreign_cert_file`, `foreign_key_file`, `foreign_task_token`\n'
  printf -- '- task token: поле `task_token` handshake, переуказывается автоматически каждые %s секунд\n' "$AHP_STATE_REFRESH_SECONDS"
  printf -- '- самопроверка автора: evidence `%s` sha256 `%s` consumer_rc=%s\n' "$EVIDENCE" "$EVIDENCE_SHA" "$PROBE_RC"
} > "$ACCESS"

echo "git_head=$HEAD"
echo "tracked_dirty=$TRACKED_DIRTY"
echo "untracked=$UNTRACKED"
echo "database=$DB_NAME"
echo "listen_address=$ADDR"
echo "base_url=$BASE_URL"
echo "serve_pid=$SERVE_PID"
echo "proc_exe_sha256=$PROC_EXE_SHA"
echo "binary_file_sha256=$FILE_EXE_SHA"
echo "handshake=$HANDSHAKE"
echo "access=$ACCESS"
echo "frozen=$FROZEN"
echo "selftest_evidence=$EVIDENCE"
echo "selftest_evidence_sha256=$EVIDENCE_SHA"
echo "consumer_rc=$PROBE_RC"
if [ "$PROBE_RC" != 0 ]; then
  printf 'PUBLIC_STAND=FAIL\n'
  exit "$PROBE_RC"
fi
printf 'PUBLIC_STAND=PASS\n'
exit 0
