#!/usr/bin/env bash
# Reproducible acceptance runner for the class-X compute supervisor.
#
# This script is honest about what it proves on the machine it runs on:
#
#   Stage A  build  : compiles the compute-host binary (runs anywhere).
#   Stage B  gate   : proves the supervisor FAILS CLOSED and never binds its
#                     socket when the microVM runtime cannot be provided. This
#                     runs on ANY OS (including the macOS control host) and is
#                     the deterministic, portable proof that no microVM is ever
#                     simulated under a misleading label.
#   Stage C  live   : the real Linux/KVM smoke (boot a Firecracker VM, acquire,
#                     exec, release). This ONLY runs on a Linux host with a
#                     working /dev/kvm and the Firecracker images supplied via
#                     the AHP_FC_* env. Where those are absent it prints
#                     SKIPPED and exits 0 WITHOUT faking a result. The shared
#                     Linux/KVM node lives in labs/kvm (forum topic
#                     20261029-kvm-lab-activation); do not duplicate it.
#
# Exit codes:
#   0  every stage that was applicable to this host passed (live portion may be
#      SKIPPED when KVM is not present — that is an open external gate, not a
#      failure to fake).
#   1  a stage that COULD run on this host failed.
set -uo pipefail

# ---- locate the repo root (this script sits in infra/compute/compute-supervisor)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
export GOCACHE="${GOCACHE:-/tmp/ahp-compute-gocache}"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
BIN="$WORK/compute-host"
SOCK="$WORK/supervisor.sock"

pass=0; fail=0
say()  { printf '%s\n' "$*"; }
ok()   { printf '  ok    %s\n' "$*"; pass=$((pass+1)); }
bad()  { printf '  FAIL  %s\n' "$*"; fail=$((fail+1)); }
skip() { printf '  SKIP  %s\n' "$*"; }

say "== Stage A: build compute-host (repo=$REPO_ROOT) =="
if ( cd "$REPO_ROOT" && go build -o "$BIN" ./cmd/compute-host ); then
  [ -x "$BIN" ] && ok "built $BIN" || bad "build produced no binary"
else
  bad "go build failed"
  say "== result: FAIL (build) =="
  exit 1
fi

# ---- Stage B: fail-closed proof (runs on EVERY OS) -------------------------
# Clear every knob that could make a runtime available, so the supervisor must
# refuse. With AHP_COMPUTE_RUNTIME unset it defaults to "firecracker", which is
# Unavailable on darwin (GOOS), on Linux without /dev/kvm, and on Linux without
# the image paths. Either way it must NOT bind the socket.
say "== Stage B: fail-closed proof (no microVM simulated) =="
errfile="$WORK/supervise.err"
set +e
env -u AHP_COMPUTE_RUNTIME -u AHP_FC_BINARY -u AHP_FC_KERNEL -u AHP_FC_ROOTFS \
    -u AHP_FC_DATADIR -u AHP_COMPUTE_FC_SOCKET \
    AHP_COMPUTE_SUPERVISOR_SOCKET="$SOCK" \
    "$BIN" supervise >"$WORK/supervise.out" 2>"$errfile"
rc=$?
set -e
rc=${rc:-1}
err="$(cat "$errfile" 2>/dev/null || true)"

if [ "$rc" -ne 0 ]; then
  ok "supervise exited non-zero (rc=$rc) without a usable runtime"
else
  bad "supervise exited 0 — it should have failed closed"
fi
if printf '%s' "$err" | grep -qiE 'compute-host:.*unavailable|unavailable:|requires Linux|KVM not available|missing required supervisor config'; then
  ok "stderr reports a fail-closed Unavailable reason"
else
  bad "stderr did not report an Unavailable reason; got: ${err:-<empty>}"
fi
if [ -e "$SOCK" ]; then
  bad "supervisor socket was created ($SOCK) despite failing closed — this would fake availability"
else
  ok "no supervisor socket was bound (fail-closed holds)"
fi

# ---- Stage C: live Linux/KVM smoke (gated; never faked) --------------------
say "== Stage C: live class-X smoke (Linux + /dev/kvm + Firecracker images) =="
live_ready=0
if [ "$(uname -s)" = "Linux" ] && [ -e /dev/kvm ] && [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
  if [ -n "${AHP_FC_BINARY:-}" ] && [ -n "${AHP_FC_KERNEL:-}" ] && \
     [ -n "${AHP_FC_ROOTFS:-}" ] && [ -n "${AHP_FC_DATADIR:-}" ]; then
    live_ready=1
  else
    skip "KVM present but AHP_FC_{BINARY,KERNEL,ROOTFS,DATADIR} incomplete"
  fi
else
  skip "live KVM portion — external gate OPEN (no /dev/kvm on this host)"
  skip "use the shared Linux/KVM node from labs/kvm (forum 20261029-kvm-lab-activation)"
fi

if [ "$live_ready" -eq 1 ]; then
  say "  starting supervisor on $SOCK ..."
  AHP_COMPUTE_RUNTIME=firecracker AHP_COMPUTE_SUPERVISOR_SOCKET="$SOCK" \
    "$BIN" supervise >"$WORK/live.out" 2>&1 &
  sup_pid=$!
  health=""
  for _ in $(seq 1 30); do
    if health="$(curl -fsS --max-time 2 --unix-socket "$SOCK" \
        http://localhost/v1/health 2>/dev/null)"; then
      break
    fi
    sleep 0.5
  done
  if printf '%s' "$health" | grep -qiE 'ok|ready|available'; then
    ok "/v1/health reports the VM runtime is available"
  else
    bad "/v1/health did not become healthy; supervisor log:"; sed 's/^/      /' "$WORK/live.out" || true
  fi
  if [ "${AHP_COMPUTE_ACCEPTANCE_SMOKE:-0}" = "1" ]; then
    say "  acquire/exec/release smoke (requires AHP_FC_GUEST_AGENT) ..."
    if curl -fsS --max-time 5 --unix-socket "$SOCK" \
         -H 'content-type: application/json' \
         -d '{"computer_id":"acc-1","incarnation":1,"volume_id":"acc-vol","fence":"acc-fence","class":"X","profile":"ephemeral"}' \
         http://localhost/v1/acquire >/dev/null 2>&1; then
      ok "/v1/acquire accepted a class-X request"
    else
      bad "/v1/acquire failed — check guest agent wiring"
    fi
  else
    skip "acquire/exec/release smoke (set AHP_COMPUTE_ACCEPTANCE_SMOKE=1 to run)"
  fi
  kill "$sup_pid" 2>/dev/null || true
  wait "$sup_pid" 2>/dev/null || true
fi

# ---- result ----------------------------------------------------------------
say "== result =="
say "  pass=$pass fail=$fail"
if [ "$fail" -ne 0 ]; then
  say "RESULT: FAIL"
  exit 1
fi
if [ "$live_ready" -ne 1 ]; then
  say "RESULT: PASS (fail-closed gate proven on this host; live KVM portion SKIPPED — external gate open)"
else
  say "RESULT: PASS (fail-closed gate + live KVM portion exercised)"
fi
exit 0
