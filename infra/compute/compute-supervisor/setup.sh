#!/usr/bin/env bash
# Node setup for the class X microVM supervisor.
#
# This prepares a Linux/KVM node to host the supervisor: it loads the KVM kernel
# modules, checks device access, and creates the data/socket directories. It is
# deliberately IDLE of any downloads, image URLs, or secrets. Fetching the
# Firecracker binary, guest kernel, and rootfs image is an EXTERNAL prerequisite
# owned by the kvm-lab operator (forum topic 20261029-kvm-lab-activation), not
# something this repo embeds or automates. Point AHP_FC_BINARY / AHP_FC_KERNEL /
# AHP_FC_ROOTFS at the images the operator provides.
#
# Run as root (or via sudo). No secrets are written or read here.
set -uo pipefail

err() { printf 'ERROR: %s\n' "$1" >&2; }

if [ "$(id -u)" != "0" ]; then
  err "run as root (sudo) to load modules and create /run directories"
  exit 1
fi
if [ "$(uname -s)" != "Linux" ]; then
  err "this host is not Linux; the supervisor cannot run here — do not simulate a microVM"
  exit 1
fi

# 1. Load KVM modules (module names differ by CPU vendor).
modprobe kvm 2>/dev/null || err "modprobe kvm failed (built-in? check /dev/kvm)"
if grep -qiG 'vendor_id[[:space:]]*:[[:space:]]*GenuineIntel' /proc/cpuinfo 2>/dev/null; then
  modprobe kvm_intel 2>/dev/null || true
elif grep -qiG 'vendor_id[[:space:]]*:[[:space:]]*AuthenticAMD' /proc/cpuinfo 2>/dev/null; then
  modprobe kvm_amd 2>/dev/null || true
fi

# 2. Device access: create a udev rule so a dedicated group can use /dev/kvm.
#    The service user should be in the 'kvm' group.
KVM_GROUP="${KVM_GROUP:-kvm}"
if ! getent group "$KVM_GROUP" >/dev/null 2>&1; then
  groupadd "$KVM_GROUP" 2>/dev/null || true
fi
if [ -e /dev/kvm ]; then
  chgrp "$KVM_GROUP" /dev/kvm 2>/dev/null || true
  chmod 0660 /dev/kvm 2>/dev/null || true
  cat >/etc/udev/rules.d/99-ahp-kvm.rules <<'RULE'
# Allow the kvm group to use the hypervisor (managed by AHP compute setup).
KERNEL=="kvm", GROUP="kvm", MODE="0660"
RULE
  udevadm control --reload-rules 2>/dev/null || true
fi

# 3. Runtime directories for the socket and per-VM scratch.
mkdir -p /run/ahp
chmod 0750 /run/ahp

echo
echo "Setup steps done."
echo "Next (EXTERNAL, provided by kvm-lab operator — this repo embeds no URLs):"
echo "  * place the firecracker binary and set AHP_FC_BINARY"
echo "  * place the guest kernel (vmlinux) and set AHP_FC_KERNEL"
echo "  * place the base rootfs (ext4) and set AHP_FC_ROOTFS"
echo "  * set AHP_FC_DATADIR to a writable scratch dir (e.g. /var/lib/ahp-compute)"
echo "  * add the service user to the '$KVM_GROUP' group"
echo "Then run ./kvm-check.sh to confirm the node is READY."
