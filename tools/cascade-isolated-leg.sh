#!/bin/sh
# Acceptance leg (delivery): the PDF deletion cascade measured against an
# ISOLATED database and a throwaway bucket, never the shared stand.
#
#   "7. Удаление. Каскадом по контексту, субъекту и tenant. Каждая запись знает
#    tenant, деплой, субъект и контекст - этого хватает для удаления, retention и
#    удержания по юридическому требованию."
#   "Lifecycle | DeleteThread, DeleteSubject, ... | Выполняет control plane AHP
#    ... агенту делать ничего не нужно"
#
# What it proves, in order:
#   - every artifact row carries the four support fields (tenant/deployment/
#     subject/context) the PDF names, and a task binding;
#   - State/DeleteThread through the real composed wiring tombstones the thread's
#     artifacts, so reads and downloads answer NOT_FOUND, with no agent hook;
#   - State/DeleteSubject does the same for a subject - measured here and not on
#   the shared stand, because DeleteSubject destroys that stand's data;
#   - a legal hold pins its artifact through both verbs;
#   - a repeated cascade is a numeric no-op;
#   - a cascade cannot cross tenant, deployment or subject.
#
# Everything runs through the fleet build lock; nothing here restarts or even
# touches the live composition. Secrets are sourced, never printed.
#
#   usage: sh tools/cascade-isolated-leg.sh          # verdict on stdout
#          RACE=1 sh tools/cascade-isolated-leg.sh   # same, under -race
set -eu
HIVE=/home/alex/ahp-hive
WORKTREE=${AHP_DELIVERY_WORKTREE:-$HIVE/worktrees/swarm-delivery}
LOCK=${AHP_BUILD_LOCK:-$HIVE/run/build.lock}
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT=${OUT:-$HIVE/state/journey-runs/cascade-isolated-$STAMP}
mkdir -p "$OUT"

# Isolation is the point of this leg, so it prefers a database of its own -
# DeleteSubject destroys everything in its scope, and on the stand's database
# that would destroy other lanes' evidence. The dedicated database is created
# once out of band (see the ACCEPTANCE record); falling back to the demo DSN is
# still honest because every test uses a throwaway tenant and bucket, but the
# verdict then says so.
ISO_ENV=${AHP_CASCADE_ISO_ENV:-$HIVE/private/delivery-cascade/iso.env}
ISO=shared-database-throwaway-tenants
if [ -f "$ISO_ENV" ]; then
  set -a; . "$ISO_ENV"; set +a
  ISO=dedicated-database:${AHP_DB_NAME:-unknown}
fi
set -a
. "$HIVE/env/journey-minio.env"
set +a
: "${AHP_TEST_DATABASE_URL:?isolated PG DSN missing}"
: "${AHP_S3_ENDPOINT:?isolated MinIO endpoint missing}"

RACEFLAG=""
if [ "${RACE:-0}" = "1" ]; then RACEFLAG=-race; fi
# The composed wiring this leg measures is behind the same build tags as the
# composed stand binary; without them cmd/ahp/cascade_test.go is not even
# compiled, and a leg that measures nothing looks exactly like a leg that passed.
TAGS=${AHP_TEST_TAGS:-ahp_exec,ahp_data}
# The toolchain and caches are pinned the way every other fleet leg pins them,
# so this leg measures this tree and never reaches for the network.
export GOROOT=${GOROOT:-$HIVE/tools/go}
export PATH="$GOROOT/bin:$PATH"
# The stand builds with CGO_ENABLED=0; the race detector is the one thing that
# needs cgo, so the -race series turns it back on instead of silently not racing.
CGO_DEFAULT=0
if [ -n "$RACEFLAG" ]; then CGO_DEFAULT=1; fi
export CGO_ENABLED=${CGO_ENABLED:-$CGO_DEFAULT}
export GOCACHE=${GOCACHE:-$HIVE/gocache}
export GOMODCACHE=${GOMODCACHE:-$HIVE/gomodcache}
export GOPROXY=off GOSUMDB=off GOPRIVATE='*' GOTOOLCHAIN=local GOWORK=off
export GOFLAGS="-mod=mod -buildvcs=false"

cd "$WORKTREE" || { echo "CASCADE_ISOLATED=FAIL (worktree $WORKTREE missing)"; exit 1; }

# Packages are run one at a time (-p 1): they share one PostgreSQL and one MinIO,
# and on a box already carrying the fleet's build queue a contended MinIO answers a
# bucket call later than the 30s test context allows, which reads as a test failure
# and is really the leg competing with itself.
# One ticket per measurement session: a caller that already holds the fleet build
# lock sets AHP_DIRECT_NO_LOCK=1 and the tests run directly, instead of the leg
# queueing behind a lock its own parent is holding.
LOCKWRAP="python3 $HIVE/tools/ahp-lock-run.py --lock $LOCK --wait 5400 --"
[ -n "${AHP_DIRECT_NO_LOCK:-}" ] && LOCKWRAP=""

# The cascade cases are the subject of the leg; the rest of the data-plane suite is
# named too, because a fix that passes the cascade and breaks seal/idempotency/
# label admission is not a fix. Nothing matching Migrate is ever run here:
# cmd/ahp/migrate_test.go drops the shared state schema.
RUN='Cascade|Binding|DeleteThread|DeleteSubject|Legalhold|ReadScope|CallerContext'
RUN="$RUN|SplitStatements|IsDuplicateObject|ReadIsScoped|SiblingTask|BeginUploadIdempotency"
RUN="$RUN|DeleteTombstone|DerivationReadiness|OpenRange|OverwriteKeepsPinnedVersion"
RUN="$RUN|PrivateFactNoEscape|PublishOnlyOwner|PublishToWorkspace|RetentionLegalHoldPurge"
RUN="$RUN|SealWrongHashRejected|UnknownRouteAndMethod|UploadSeal|CheckPublication"
RUN="$RUN|LifecycleGate|LifecycleForeign|AuthRevocation|NewRejectsNil"

RAN=0
FAILED=0
set +e
# shellcheck disable=SC2086
$LOCKWRAP go test -tags "$TAGS" $RACEFLAG -count=1 -v -p 1 -timeout 2400s \
    -run "$RUN" \
    ./internal/artifacts/ ./internal/artifactsapi/ ./cmd/ahp/ > "$OUT/go-test.log" 2>&1
RC=$?
set -e
RAN=$(grep -c '^=== RUN' "$OUT/go-test.log" || true)
FAILED=$(grep -c '^--- FAIL' "$OUT/go-test.log" || true)
PASSED=$(grep -c '^--- PASS' "$OUT/go-test.log" || true)
SKIPPED=$(grep -c '^--- SKIP' "$OUT/go-test.log" || true)
echo "tags=$TAGS race=${RACEFLAG:-none} worktree=$WORKTREE"

if [ "$RC" -eq 0 ] && [ "$RAN" -gt 0 ] && [ "$FAILED" -eq 0 ] && [ "$SKIPPED" -eq 0 ]; then
  VERDICT=PASS
else
  VERDICT=FAIL
fi
echo "CASCADE_ISOLATED=$VERDICT ran=$RAN pass=$PASSED fail=$FAILED skip=$SKIPPED rc=$RC isolation=$ISO"
echo "EVIDENCE=$OUT"
[ "$VERDICT" = "PASS" ]
