#!/usr/bin/env bash
# Regression test for scripts/build-candidate.sh disk hygiene (ENOSPC incident 2026-11-09).
# Runs the builder with a FAKE go (two modes: success and forced mid-build failure) and
# ASSERTS that no new ahp-*/tmp.* directories appear in $TMPDIR afterwards, on either
# path. Any leftover temp = test failure. No real compilation, no docker, no secrets.
# Usage: scripts/test-build-candidate-hygiene.sh   (host: darwin/go toolchain not needed)
set -uo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
SCRIPT="$REPO_ROOT/scripts/build-candidate.sh"
AHP_ROOT_DEFAULT="$(cd "$REPO_ROOT/../../.." && pwd)"
CACHE_ROOT="${AHP_BUILD_CACHE_ROOT:-$AHP_ROOT_DEFAULT/private/pdf-closure-20261102/tools/go-build-cache}"
RUNS_DIR="$CACHE_ROOT/runs"
T="${TMPDIR:-/tmp}"
REVISION="$(git rev-parse HEAD)"
PASS=0; FAIL=0
snap(){ ls "$T" 2>/dev/null | grep -E '^(ahp-|tmp\.)' | sort; }
runs_snap(){ ls "$RUNS_DIR" 2>/dev/null | grep '^run\.' | sort; }
ok(){ echo "PASS: $1"; PASS=$((PASS+1)); }
bad(){ echo "FAIL: $1"; FAIL=$((FAIL+1)); }

# stub 'go' lives OUTSIDE $TMPDIR (under the bounded cache root) so the stub itself
# cannot pollute the thing we are measuring; self-removed at exit.
STUB="$(mktemp -d "$RUNS_DIR/hyg-stub.XXXXXXXX")"
finish(){ local rc=$?; /usr/bin/env python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1], ignore_errors=True)' "$STUB"; exit $rc; }
trap finish EXIT INT TERM

cat > "$STUB/go" <<'STUBEOF'
#!/usr/bin/env bash
set -u
case "${1:-}" in
  env)
    case "${2:-}" in GOOS) echo "${FAKE_GO_OS:-darwin}";; GOARCH) echo "${FAKE_GO_ARCH:-arm64}";; *) echo "";; esac ;;
  version)
    if [ "${2:-}" = "-m" ]; then
      printf 'path\tcommand-line-arguments\n\tbuild\tvcs.revision=%s\n\tbuild\tvcs.modified=false\n\tbuild\tGOOS=%s\n\tbuild\tGOARCH=%s\n' \
        "${FAKE_GO_REV:-0000000000000000000000000000000000000000}" "${FAKE_GO_OS:-darwin}" "${FAKE_GO_ARCH:-arm64}"
    else
      echo "go version fakego1.0 ${FAKE_GO_OS:-darwin}/${FAKE_GO_ARCH:-arm64}"
    fi ;;
  build)
    o=""; cmd=""
    args=("$@"); i=0
    while [ $i -lt ${#args[@]} ]; do
      a="${args[$i]}"
      case "$a" in
        -o) i=$((i+1)); o="${args[$i]}";;
        ./cmd/*) cmd="${a#./cmd/}";;
      esac
      i=$((i+1))
    done
    if [ -n "${FAKE_GO_FAIL_CMD:-}" ] && [ "$cmd" = "$FAKE_GO_FAIL_CMD" ]; then
      echo "fake go: induced failure for cmd/$cmd" >&2; exit 1
    fi
    printf 'fake-binary-content-for-%s\n' "$cmd" > "$o"; chmod +x "$o" ;;
  clean) exit 0 ;;
  *) exit 0 ;;
esac
STUBEOF
chmod +x "$STUB/go"

echo "== hygiene regression: baseline snapshots =="
T_BEFORE="$(snap)"; R_BEFORE="$(runs_snap)"
echo "tmp entries before: $(printf '%s\n' "$T_BEFORE" | grep -c . || true)"

run_case(){ # name expected_rc env...
  local name="$1"; shift; local expected="$1"; shift
  local t_after r_after rc
  PATH="$STUB:$PATH" FAKE_GO_REV="$REVISION" AHP_BUILD_CACHE_ROOT="$CACHE_ROOT" "$@" \
    "$SCRIPT" --out "$STUB/out-$name" >"$STUB/$name.log" 2>&1; rc=$?
  t_after="$(snap)"; r_after="$(runs_snap)"
  if [ "$expected" = ok ] && [ "$rc" = 0 ]; then ok "$name: exit 0"; elif [ "$expected" = fail ] && [ "$rc" != 0 ]; then ok "$name: exit $rc (failure path exercised)"; else bad "$name: unexpected exit rc=$rc (see $STUB/$name.log)"; fi
  if [ "$t_after" = "$T_BEFORE" ]; then ok "$name: no new ahp-*/tmp.* dirs in \$TMPDIR"; else bad "$name: TEMP LEAKED: $(comm -13 <(printf '%s\n' "$T_BEFORE") <(printf '%s\n' "$t_after") | tr '\n' ' ')"; fi
  if [ "$r_after" = "$R_BEFORE" ]; then ok "$name: no leaked run.* in bounded cache runs/"; else bad "$name: run.* leaked in runs/: $(comm -13 <(printf '%s\n' "$R_BEFORE") <(printf '%s\n' "$r_after") | tr '\n' ' ')"; fi
}

echo "== case 1: success path =="
run_case success ok env
echo "== case 2: induced build failure (state-consumer) =="
run_case failure fail env FAKE_GO_FAIL_CMD=state-consumer

echo "== case 3: KEEP_TMP=1 keeps run dir in bounded cache (outside \$TMPDIR), then removed =="
KEEP_TMP=1 PATH="$STUB:$PATH" FAKE_GO_REV="$REVISION" AHP_BUILD_CACHE_ROOT="$CACHE_ROOT" \
  "$SCRIPT" --out "$STUB/out-keep" >"$STUB/keep.log" 2>&1 && rc=0 || rc=$?
kept="$(comm -13 <(printf '%s\n' "$R_BEFORE") <(runs_snap) | head -1)"
if [ "$rc" = 0 ] && [ -n "$kept" ] && [ -d "$RUNS_DIR/$kept" ]; then ok "KEEP_TMP=1: $kept kept for inspection"; else bad "KEEP_TMP=1 did not keep a run dir (rc=$rc)"; fi
if [ "$(snap)" = "$T_BEFORE" ]; then ok "KEEP_TMP=1: clone still removed from \$TMPDIR"; else bad "KEEP_TMP=1 leaked clone into \$TMPDIR"; fi
/usr/bin/env python3 -c 'import shutil,sys; shutil.rmtree(sys.argv[1], ignore_errors=True)' "$RUNS_DIR/$kept" 2>/dev/null || true

echo "== case 4: --keep-temp flag parses before any temp is made (bad revision -> rc 2) =="
"$SCRIPT" --keep-temp --revision deadbeef >/dev/null 2>&1; rc=$?
[ "$rc" = 2 ] && ok "--keep-temp flag accepted, validation still fails closed rc=2" || bad "--keep-temp flag: rc=$rc expected 2"
if [ "$(snap)" = "$T_BEFORE" ]; then ok "flag smoke: \$TMPDIR untouched"; else bad "flag smoke leaked \$TMPDIR"; fi

echo "== RESULT: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" = 0 ]
