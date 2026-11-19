#!/bin/sh
# Independent re-run of the workspace upload-into-project-context leg on an
# ISOLATED LANE origin (NOT prod, NOT the director's :183xx). For the verifier
# (vm2-foreman): author != verifier. Reproduces the PASS without hand-holding.
#
#   sh scripts/asws-upload-leg.sh
#
# Brings up (or reuses) a private PostgreSQL and MinIO on THIS host's loopback,
# runs tests/aswsupload, and writes durable evidence. Nothing on prod ports or
# other lanes' data is touched. All ports/paths are overridable.
set -eu

REPO=${AHP_AWSWS_REPO:-$(cd "$(dirname "$0")/.." && pwd)}
GO=${GO:-/home/alex/ahp-hive/tools/go/bin/go}
PGBIN=${PGBIN:-/home/alex/ahp-handbook/tools/pg/root/bin}
MINIO=${MINIO:-/home/alex/ahp-handbook-compute/.tools/bin/minio}

PGPORT=${AHP_AWSWS_PGPORT:-25471}
S3PORT=${AHP_AWSWS_S3PORT:-16910}
CONSPORT=${AHP_AWSWS_CONSPORT:-16911}
PGDATA=${AHP_AWSWS_PGDATA:-/dev/shm/ahp_ws1_pgdata}
PGSOCK=${AHP_AWSWS_PGSOCK:-/dev/shm/ahp_ws1_pg}
S3DATA=${AHP_AWSWS_S3DATA:-/dev/shm/ahp_ws1_minio}
PGDB=${AHP_AWSWS_DB:-ahp_ws1}
S3USER=${AHP_MINIO_ROOT_USER:-ahpws1root}
S3PASS=${AHP_MINIO_ROOT_PASSWORD:-ahpws1secret}
BUCKET=${AHP_AWSWS_BUCKET:-as-artifacts-aswsleg}
GOCACHE=${GOCACHE:-/dev/shm/gowsc}
TMPDIR=${TMPDIR:-/dev/shm/gowstmp}
GOMODCACHE=${GOMODCACHE:-/home/alex/ahp-hive/gopath/pkg/mod}
RUN=${AHP_AWSWS_RUN:-/dev/shm/asws-leg-run}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
EV=${AHP_LEG_EVIDENCE:-$RUN/evidence/leg-$STAMP.log}
LOCK=${AHP_BUILD_LOCK:-/home/alex/ahp-hive/run/build.lock}
mkdir -p "$RUN/evidence" "$GOCACHE" "$TMPDIR" "$PGSOCK"

listening(){ (ss -ltn 2>/dev/null || netstat -ltn 2>/dev/null) | grep -q ":$1"; }

# --- PostgreSQL (reuse existing cluster, or init a fresh one on tmpfs) ---
if ! listening "$PGPORT"; then
  if [ ! -f "$PGDATA/PG_VERSION" ]; then
    "$PGBIN/initdb" -U alex -A trust --pgdata="$PGDATA" >/dev/null
    echo "CREATE DATABASE $PGDB;" | "$PGBIN/postgres" --single -D "$PGDATA" postgres >/dev/null 2>&1 || true
  fi
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$RUN/pg.log" -o "-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$PGSOCK -c fsync=off" start >/dev/null
  i=0; until listening "$PGPORT" || [ $i -ge 30 ]; do sleep 1; i=$((i+1)); done
fi

# --- MinIO (tmpfs, own ports) ---
if ! listening "$S3PORT"; then
  MINIO_ROOT_USER="$S3USER" MINIO_ROOT_PASSWORD="$S3PASS" \
    setsid nohup "$MINIO" server "$S3DATA" --address "127.0.0.1:$S3PORT" --console-address "127.0.0.1:$CONSPORT" >"$RUN/minio.log" 2>&1 </dev/null &
  i=0; until curl -sf --max-time 4 "http://127.0.0.1:$S3PORT/minio/health/live" >/dev/null 2>&1 || [ $i -ge 40 ]; do sleep 1; i=$((i+1)); done
fi

cd "$REPO"
echo "== lane upload leg ($STAMP) PG=:$PGPORT S3=:$S3PORT bucket=$BUCKET evidence=$EV =="
flock "$LOCK" env \
  GOCACHE="$GOCACHE" TMPDIR="$TMPDIR" GOMODCACHE="$GOMODCACHE" GOMAXPROCS=2 GOPROXY=off GOFLAGS=-mod=mod CGO_ENABLED=0 \
  AHP_TEST_DATABASE_URL="postgres://alex@127.0.0.1:$PGPORT/$PGDB?sslmode=disable" \
  AHP_S3_ENDPOINT="http://127.0.0.1:$S3PORT" AHP_S3_REGION="us-east-1" \
  AHP_MINIO_ROOT_USER="$S3USER" AHP_MINIO_ROOT_PASSWORD="$S3PASS" \
  AHP_S3_BUCKET="$BUCKET" AHP_LEG_EVIDENCE="$EV" \
  "$GO" test -tags ahp_data,ahp_exec -p 2 -count=1 -v -run TestUploadIntoProjectContextThroughOrigin ./tests/aswsupload 2>&1 | tee "$EV.test"

echo "== evidence: $EV =="
[ -f "$EV" ] && cat "$EV"
