#!/bin/sh
# P0-2 aggregate: the coherent personal-Dot user journey on real owners.
# One script, one summary log, exit 0 only when every leg is green on the
# same clean revision. Legs (per PDF rev3.2 L2044,2054,2056,2100,2256-2259,930-946):
#   journey J1-J11 (profile-pin -> real Qwen -> GetResult -> memory -> file SHA
#   -> history reload -> cancel -> foreign user denied -> human read; J10
#   records DEF-DOTE2E-9, the browser-turn defect owned by State),
#   HITL approve + cancel, reload-persistence, restart without duplicate
#   effect, custom agents on the Go and Python SDKs without Agent Space.
# Run from the harness root: dot-e2e/tests/dote2e/run-journey.sh
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
SUMMARY="$ROOT/evidence/dot-e2e-journey-$STAMP.log"
REV=$(cd "$ROOT/dot-e2e" && git rev-parse HEAD)
FAILURES=0
{
  echo "lane=dot-e2e revision=$REV started=$STAMP"
  echo "== phase A: journey + independent legs (no service restart)"
  "$HERE/run.sh" ./tests/dote2e/ -run 'TestPersonalDotUserJourneyOnRealOwners|TestDotRunsFromAgentConfigRevisionAndPersonalProfile|TestDotTurnIsATaskWithGrantAndToken|TestToolApprovalIsSignedByTheEmployee|TestHitlCancelNeverExecutesTheEffect|TestDotHistoryReloadsFromDurableStore|TestPersonalDotMemoryIsScopedByOwnerKey|TestArtifactUploadSealDownloadSameSubject|TestPersonalComputerIsKeyedPerDotAndRealm|TestCustomAgentRunsStandaloneOnGoSDKs|TestCustomAgentRunsStandaloneViaPythonSDK' || FAILURES=$((FAILURES+1))
  echo "== phase B: service restart, no duplicate effect (owned by State)"
  AHP_DOT_E2E_ALLOW_RESTART=1 "$HERE/run.sh" ./tests/dote2e/ -run 'TestRecoveryKeepsDurableStateAcrossRestart' || FAILURES=$((FAILURES+1))
  echo "finished=$(date -u +%Y%m%dT%H%M%SZ) failures=$FAILURES"
} > "$SUMMARY" 2>&1
cat "$SUMMARY"
[ "$FAILURES" -eq 0 ] || exit 1
