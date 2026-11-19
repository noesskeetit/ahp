#!/usr/bin/env bash
# Sequentially run every profiling phase against ONE isolated instance per phase
# (own port / own identity; shared lane DB + shared PG/Redis/MinIO, so phases run
# strictly one-at-a-time to keep the measurements clean). Invoked via the lane
# runner run-tests.py, which injects the lane environment and records revision.
#
# Usage: bash profiling/run-baseline.sh
# Results land in ../evidence relative to the worktree = pdf-closure-20261102/evidence.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"        # .../pdf-closure-20261102/profiling-baseline/profiling
STAMP="$(cd "$HERE/.." && pwd)"                              # worktree root
STAMP_ROOT="$(cd "$STAMP/.." && pwd)"                        # pdf-closure-20261102 (holds run-tests.py)
cd "$STAMP_ROOT"
MARK="$STAMP_ROOT/evidence/profiling-run-baseline.markers"
: > "$MARK"
run () {
  local phase="$1"; shift
  echo "=== $(date -u +%FT%TZ) START $phase $* ===" | tee -a "$MARK"
  python3 run-tests.py --lane profiling-baseline go run ./profiling/cmd/ahp-profile \
      --phase "$phase" --out ../evidence --repo . "$@" >>"$MARK" 2>&1
  echo "=== $(date -u +%FT%TZ) DONE $phase exit=$? ===" | tee -a "$MARK"
}

# (a) deterministic platform overhead: full ladder 1/5/10/20/30 then extension
#     until the documented degradation boundary, 20s per step (throughput-stable
#     percentiles) to bound total wall time.
run overhead --step-s 20

# (a) recovery: same steady step before/after a heavy burst, no restart.
run recovery --burst-s 60 --burst-conc 60 --idle-s 45

# (a) leak analysis: 6-minute steady mixed load; RSS/goroutine slopes per minute.
run soak --soak-minutes 6

# (a) dependency-loss behavior: cut PG mid-run, hold the outage, restore.
run depfail --steady-s 45 --outage-s 20

# (b) REAL Qwen end-to-end: token-bounded, small volume, tokens (not currency).
run llm --llm-c1 2 --llm-c5 4 --llm-stream 2 --llm-max-tokens 256

echo "=== $(date -u +%FT%TZ) ALL PHASES COMPLETE ===" | tee -a "$MARK"
