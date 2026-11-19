#!/bin/sh
# Run personal-Dot lane scenarios through the lane runner and print only the
# failure lines plus the evidence paths (full logs stay in the evidence dir).
set -eu
LANE_ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
RT="$LANE_ROOT/evidence/dot-e2e-runtime"
INSTANCE=${AHP_DOT_E2E_INSTANCE:-$(ls -d "$RT"/inst-* | tail -1)}
RUN=${1:-./tests/dote2e/}
shift || true
AHP_DOT_E2E_INSTANCE="$INSTANCE" \
AHP_DOT_E2E_EVIDENCE="${AHP_DOT_E2E_EVIDENCE:-$RT/evidence}" \
  python3 "$LANE_ROOT/run-tests.py" --lane dot-e2e go test "$RUN" -count=1 "$@" > /tmp/dote2e-run.json || true
LAST=$(tail -n 1 /tmp/dote2e-run.json)
LOG=$(printf %s "$LAST" | python3 -c "import json,sys;print(json.load(sys.stdin)['log'])")
RC=$(printf %s "$LAST" | python3 -c "import json,sys;print(json.load(sys.stdin)['exit_code'])")
echo "runner_exit=$RC log=$LOG"
grep -E '^(--- )?(FAIL|ok|PASS)|_test.go:[0-9]+:' "$LOG" | cut -c1-200 | head -40
exit "$RC"
