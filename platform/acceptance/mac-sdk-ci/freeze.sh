#!/usr/bin/env bash
# Creates the immutable SDK candidate commit and records its SHA.
# Run this ONCE in the mac-sdk-ci worktree.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "=== Freezing SDK candidate ==="

git add \
  pkg/sdk/ \
  sdk/go/state/ \
  sdk/python/ \
  tests/sdk/ \
  docs/sdk/ \
  acceptance/mac-sdk-ci/ \
  internal/state/checkpoint_parts.go \
  internal/state/kv.go

git commit -m "sdk: immutable candidate rev 3 (token-leak lint, behavior tests, delete_thread)

Implements PDF rev 3.2 §4.2/§6.2:
- ahp-sdk check token-flow analysis (tokenflow.go, 5 tests)
- Real LangGraph behavior tests (resume, pending writes, 23/23)
- DeleteThread SDK client (Go + Python, tested)
- sync methods documented (NotImplementedError)
- CI gate 12/12 PASS
- 3 compile fixes for internal/state (arity/shadow only)

Ready for acceptance review."

SHA=$(git rev-parse HEAD)
echo "$SHA" > acceptance/mac-sdk-ci/CANDIDATE_SHA
git add acceptance/mac-sdk-ci/CANDIDATE_SHA
git commit --amend --no-edit
FINAL=$(git rev-parse HEAD)
echo "$FINAL" > acceptance/mac-sdk-ci/CANDIDATE_SHA
echo "FINAL SHA: $FINAL"
echo "Recorded in acceptance/mac-sdk-ci/CANDIDATE_SHA"
