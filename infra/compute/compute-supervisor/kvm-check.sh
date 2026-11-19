#!/usr/bin/env bash
# Pre-flight check for the class X microVM supervisor.
#
# Verifies that this host can actually run Firecracker before we try to start
# the supervisor. This script NEVER installs anything and NEVER simulates a
# hypervisor: it reports what is missing and exits non-zero so that an operator
# or CI gate cannot mistake an unready host for a ready one.
#
# Exit codes:
#   0  all prerequisites present
#   1  one or more prerequisites missing (the external gate is not closed)
set -uo pipefail

fail=0
ok()   { printf '  ok    %s\n' "$1"; }
bad()  { printf '  MISS  %s\n' "$1"; fail=1; }
warn() { printf '  warn  %s\n' "$1"; }

echo "== compute supervisor pre-flight ($(uname -s)/$(uname -m)) =="

# 1. Operating system. Firecracker's KVM driver is Linux-only.
if [ "$(uname -s)" = "Linux" ]; then
  ok "Linux host"
else
  bad "not Linux (uname -s=$(uname -s)); the microVM supervisor requires Linux"
fi

# 2. /dev/kvm must exist and be read/writable by us (or root).
if [ -e /dev/kvm ]; then
  if [ -r /dev/kvm ] && [ -w /dev/kvm ]; then
    ok "/dev/kvm present and rw for $(id -un)"
  elif [ "$(id -u)" = "0" ]; then
    warn "/dev/kvm present but not rw for this user; run as root or add the kvm group"
  else
    bad "/dev/kvm present but not read/writable; add user to the kvm group"
  fi
else
  bad "/dev/kvm missing (this host cannot run class X; do NOT simulate a microVM)"
fi

# 3. KVM kernel module loaded (best effort; /dev/kvm existing implies it).
if [ -e /dev/kvm ]; then
  if command -v lsmod >/dev/null 2>&1 && lsmod | grep -qE '^kvm'; then
    ok "kvm module loaded"
  else
    warn "kvm module not visible via lsmod (may be built-in); /dev/kvm exists"
  fi
fi

# 4. firecracker binary reachable (path from env or PATH).
FC_BIN="${AHP_FC_BINARY:-firecracker}"
if command -v "$FC_BIN" >/dev/null 2>&1 || [ -x "$FC_BIN" ]; then
  ok "firecracker binary: $FC_BIN"
else
  bad "firecracker binary not found (set AHP_FC_BINARY or put 'firecracker' on PATH)"
fi

# 5. Image paths (required by the supervisor). Checked for existence only.
for var in AHP_FC_KERNEL AHP_FC_ROOTFS AHP_FC_DATADIR; do
  val="${!var:-}"
  if [ -z "$val" ]; then
    bad "$var not set"
  elif [ "$var" = "AHP_FC_DATADIR" ]; then
    [ -d "$val" ] && ok "$var=$val exists" || bad "$var=$val does not exist"
  else
    [ -f "$val" ] && ok "$var=$val exists" || bad "$var=$val does not exist"
  fi
done

# 6. Nested virtualization note (informational, common on cloud/dev hosts).
if [ -d /sys/module/kvm_intel/parameters ] || [ -d /sys/module/kvm_amd/parameters ]; then
  ok "hardware KVM module parameters present"
fi

echo "== result =="
if [ "$fail" -ne 0 ]; then
  echo "NOT READY: resolve the MISS items above. The class X external gate stays open."
  exit 1
fi
echo "READY: this host can run the class X microVM supervisor."
