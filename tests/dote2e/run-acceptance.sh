#!/bin/sh
# Acceptance run of the personal Dot on the pinned candidate: the journey plus the
# two SDK standalone scenarios, under the STRICT terminal rule and ACCEPTANCE mode,
# followed by a verdict reader.
#
# WHY THE READER EXISTS: `go test` returns 0 while the artifact it wrote says
# verdict=BLOCKED. rc=0 is not acceptance - the verdict field is. Under
# AHP_DOT_E2E_ACCEPT=1 the harness also reddens the test on a non-PASS verdict, so
# both halves of the pipeline agree.
#
# Required env (values are never printed):
#   AHP_EXPECTED_REV      exact acceptance SHA; the rev guard refuses any other
#   AHP_PRODUCT_REV       same SHA; written into every artifact
#   AHP_ACCEPTANCE_MANIFEST  manifest of that SHA (source.revision / source.tree)
#   AHP_DOT_E2E_PYTHON    lane interpreter with httpx, or the Python leg SKIPs
#   AHP_DOT_E2E_INSTANCE  stand directory (defaults to the newest inst-*)
#   AHP_SECRET_STORE_DIR  optional: directory of real operational stores; when set the
#                       secret-hygiene sweep checks this lane against them, when unset that
#                       sweep skips with a gate-visible SKIP_ENV token (never a pass)
# The scenarios are launched through the lane runner (run.sh -> run-tests.py). An
# operator whose runner selects env differently may substitute it here; nothing in
# this script decides which database the stand uses - the instance scope does.
#
# DIVISION OF LABOUR with scripts/skip-verdict.mjs (Russell, DEF-GATE-SKIP-POLICY) -
# two different objects, deliberately not one counter:
#   * skip-verdict.mjs reads the TEST STREAM (`go test -json`) and owns the skip
#     policy: a skipped leg, a PASS parent that measured nothing, or a silent skip
#     is not a pass. It never reads a scenario artifact.
#   * cmd/dote2e-verdict reads the SCENARIO ARTIFACT's verdict field and owns the
#     acceptance decision for a run: PASS only at the expected revision, with legs,
#     on a clean tree; BLOCKED/FAIL/unusable get distinct codes. It never counts
#     go-test results.
# Neither recomputes the other's number, and neither is a substitute for the other:
# an artifact can say PASS while the stream that produced it skipped half the legs,
# which is exactly why both exist. To run this series under his policy, wrap the
# runner invocation with `node scripts/skip-verdict.mjs -- <cmd>` (his tool takes a
# command); no change is needed here and none is made, so there is one
# implementation of the skip policy in the tree.
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../../.." && pwd)
RT="$ROOT/evidence/dot-e2e-runtime"
EVIDENCE=${AHP_DOT_E2E_EVIDENCE:-$RT/evidence}
INSTANCE=${AHP_DOT_E2E_INSTANCE:-$(ls -d "$RT"/inst-* | tail -1)}
: "${AHP_EXPECTED_REV:?set AHP_EXPECTED_REV to the exact acceptance SHA}"
: "${AHP_PRODUCT_REV:?set AHP_PRODUCT_REV to the exact acceptance SHA}"
: "${AHP_ACCEPTANCE_MANIFEST:?the rev guard needs the acceptance manifest of this SHA}"
mkdir -p "$EVIDENCE"
BEFORE=$(mktemp); AFTER=$(mktemp); NEW=$(mktemp)
find "$EVIDENCE" -name 'dot-e2e-scenario-*.json' | sort > "$BEFORE"
export AHP_DOT_E2E_INSTANCE="$INSTANCE" AHP_DOT_E2E_EVIDENCE="$EVIDENCE"
export AHP_DOT_E2E_STRICT_TERMINAL=1 AHP_DOT_E2E_ACCEPT=1
echo "acceptance run: instance=$(basename "$INSTANCE") strict=1 accept=1 rev=$AHP_EXPECTED_REV"
"$HERE/run.sh" ./tests/dote2e/ -run 'TestPersonalDotUserJourneyOnRealOwners'
"$HERE/run.sh" ./tests/dote2e/ -run 'TestCustomAgentRunsStandaloneOnGoSDKs'
"$HERE/run.sh" ./tests/dote2e/ -run 'TestCustomAgentRunsStandaloneViaPythonSDK'
find "$EVIDENCE" -name 'dot-e2e-scenario-*.json' | sort > "$AFTER"
comm -13 "$BEFORE" "$AFTER" > "$NEW"
[ -s "$NEW" ] || { echo "no scenario artifact was written - nothing to read a verdict from"; exit 3; }
cat "$NEW"
# The reader, not the runner, decides whether this series is acceptance evidence:
# exit 0 all PASS, 4 BLOCKED, 5 FAIL/DEFECT, 3 unreadable/dry-run/dirty/foreign rev.
# A list file, not shell word-splitting: artifact paths in this workspace contain
# spaces, and a split path would make the acceptor read the wrong file.
#
# WHY NOT `go run`: measured on 2026-11-10T05:23Z, `go run ./cmd/dote2e-verdict`
# prints "exit status 3" and then returns 1 of its own. The distinction this tool
# exists to publish (0/3/4/5) would arrive at the caller as a flat 1 - the same
# lying-exit-code class this band closes elsewhere. So: build, then exec the binary.
BIN=$(mktemp -d)/dote2e-verdict
go build -o "$BIN" ./tests/dote2e/cmd/dote2e-verdict || exit 3
set +e
"$BIN" -expect-rev "$AHP_PRODUCT_REV" "@$NEW"
RC=$?
set -e
echo "verdict reader exit=$RC (0=all PASS, 3=unusable, 4=BLOCKED, 5=FAIL/DEFECT) - rc=0 is not acceptance, the verdict field is"
exit "$RC"
