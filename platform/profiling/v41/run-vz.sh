#!/bin/bash
set -euo pipefail
umask 077
base="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
phase="${1:-capabilities}"
case "$phase" in
    capabilities|smoke|p0|p1|p2|p3|p4|fc1|fc2|all-process|all-fc) ;;
    *) echo "Unknown lab phase: $phase" >&2; exit 64 ;;
esac
if [[ "$(uname -sm)" != 'Darwin arm64' ]]; then
    echo 'Apple Silicon macOS with nested Apple Virtualization is required' >&2
    exit 65
fi
if [[ $# -ge 2 ]]; then
    mkdir -- "$2"
    output="$(cd -- "$2" && pwd)"
else
    output="$(mktemp -d /tmp/ahp-v41-evidence.XXXXXX)"
fi
if [[ $# -gt 2 ]]; then
    shift 2
    if [[ "$1" != -- || "$phase" = capabilities ]]; then
        echo "usage: $0 [phase] [new host evidence directory] [-- profiling CLI options]" >&2
        exit 64
    fi
    shift
else
    set --
fi
echo "AHP_V41_EVIDENCE=$output"
cpu="${AHP_LAB_CPU:-10}"
memory="${AHP_LAB_MEMORY_MIB:-16384}"
disk="${AHP_LAB_PG_DISK_MIB:-1280}"
timeout="${AHP_LAB_TIMEOUT_SEC:-14400}"
minio_disk="${AHP_LAB_MINIO_DISK:-0}"
if [[ "$minio_disk" != 0 && "$minio_disk" != 1 ]]; then
    echo 'AHP_LAB_MINIO_DISK must be 0 or 1' >&2
    exit 64
fi
for value in "$cpu" "$memory" "$disk" "$timeout"; do
    [[ "$value" =~ ^[1-9][0-9]{0,5}$ ]] || { echo "Invalid lab integer: $value" >&2; exit 64; }
done
if (( cpu < 8 || cpu > $(sysctl -n hw.ncpu) || memory < 12288 || disk < 512 || timeout < 60 )); then
    echo "Lab sizing requires at least 8 vCPUs (at most the host CPU count), 12288 MiB RAM, 512 MiB disk and 60 s timeout" >&2
    exit 64
fi

scratch="$(mktemp -d "$output/.vm.XXXXXX")"
trap 'rm -rf -- "$scratch"' EXIT
l1="${AHP_LAB_L1_ASSETS:-/tmp/ahp-v4-review.EnT6vw/lab/l1}"
pg="${AHP_LAB_PG_INITRAMFS:-/tmp/ahp-v32-kvm-pg-bridge/native-pg-addon.cpio.gz}"
kernel="${AHP_LAB_VZ_KERNEL:-/tmp/ahp-fc.782AaF/l1-Image}"
kernel_apk="${AHP_LAB_KERNEL_APK:-$scratch/linux-virt.apk}"
contrib_apk="${AHP_LAB_PG_CONTRIB_APK:-$scratch/postgresql18-contrib.apk}"
rootfs="$scratch/staging"
mkdir -p "$rootfs"/{assets,bin,dev,etc,mods,proc,sbin,sys,usr/lib,usr/share,tmp}
require_space() {
    local reserve="$1"
    local free
    free="$(df -Pk "$output" | awk 'NR == 2 {print $4}')"
    if (( free < (disk + reserve) * 1024 )); then
        echo "Free space ${free} KiB is below the PostgreSQL disk budget plus ${reserve} MiB; use a larger volume or adjust AHP_LAB_PG_DISK_MIB" >&2
        exit 74
    fi
}
require_space 768

fetch() {
    local path="$1"
    local url="$2"
    if [[ ! -s "$path" ]]; then
        curl -fLSs --retry 2 --max-time 90 -o "$path" "$url"
    fi
}
pin() {
    local expected="$1"
    local path="$2"
    local actual
    actual="$(shasum -a 256 "$path" | awk '{print $1}')"
    if [[ "$actual" != "$expected" ]]; then
        echo "Input hash mismatch for $path" >&2
        exit 65
    fi
    printf '%s  %s\n' "$actual" "$path" >>"$output/inputs.sha256"
}
fetch "$kernel_apk" https://dl-cdn.alpinelinux.org/alpine/v3.22/main/aarch64/linux-virt-6.12.118-r0.apk
fetch "$contrib_apk" https://dl-cdn.alpinelinux.org/alpine/v3.23/main/aarch64/postgresql18-contrib-18.7-r0.apk
pin a78f4198dbdfa84a342f0146d9b8509ec89b76b760b6830a4374b98aead53949 "$kernel_apk"
pin 697cddc0ac1cdb51b07c1bcf1535ffaacabf83f80f9f25f630f53a876a8040f9 "$contrib_apk"
pin e3fffd9dc38dc6eda2959ece2a90d2cc532a89c9d13ce7f46edd082437a0c07c "$kernel"
pin d85c339f336bb20e1e67753f03a9bb6b08b6d1de831d213c5657e204bb8c37f8 "$pg"
pin f7168507c37b2b047ea2b30433cbe4faa3986c36d1209c046fd1814118ce3d48 "$l1/bin/firecracker"
pin 8b41640b1b2d8d029edcaef31d2d9419338450e961687e39555d4e2b9ac88ee8 "$l1/bin/v41-minio"
pin 61baeae1ac6197be4fc5c71fa78df266acdc33c54570290d2f611c2b42c105be "$l1/assets/guest-kernel"
pin a6bb1e6e349cbdfa690a78cd0017413c5446bdae02ee003ceb682d4ae1a004ff "$l1/bin/busybox"
pin 6a0e832c4763a61cb843f9a7216d2cb406f12387fb11f3c63c30649c62ed0aaf "$l1/sbin/mke2fs"
pin a481898570ed03ba81eb20b37d0cbddd0809ee63281f391950931e163f00ab76 "$l1/usr/lib/libblkid.so.1"
pin 934f4c866bcb5b21961279c81384e4db0f5f0ace30360514690c0944a9aeed09 "$l1/usr/lib/libcom_err.so.2"
pin 9900f28500fa2fd548953869dc8160f7765632c6b4b28f8c6487247b2b079f22 "$l1/usr/lib/libe2p.so.2"
pin c63faf586cf1af77be63afea98a27b8361acf48da9d8bde645752c2a07005df2 "$l1/usr/lib/libeconf.so.0"
pin e231e2f4493a9b5f553ca61b553c199c2b586c710ca6563f4815370a94f905c3 "$l1/usr/lib/libext2fs.so.2"
pin 8413c719470bfada1cf5f0d2c02a673fcef47a12ce2e5ed1a95ddd44a9fc2717 "$l1/usr/lib/libss.so.2"
pin 77ece87e1154e1763eb20ffadb44d0080bba8afdfff9a90f5279a57d2d45b167 "$l1/usr/lib/libuuid.so.1"
go version -m "$l1/bin/v41-minio" | sed -n '1,3p' >"$output/builds.txt"
shasum -a 256 "$base/run-vz.sh" "$base/lab-init.sh" "$base/results-send.go.in" \
    "$base/vz-lab.m" "$base/vz.entitlements.plist" "$base/extract-results.py" >>"$output/inputs.sha256"

node() {
    local source="$1"
    local target="$2"
    cp -c "$source" "$target" 2>/dev/null || cp "$source" "$target"
}
for name in busybox firecracker v41-minio; do
    node "$l1/bin/$name" "$rootfs/bin/$name"
done
node "$l1/sbin/mke2fs" "$rootfs/sbin/mke2fs"
node "$l1/assets/guest-kernel" "$rootfs/assets/guest-kernel"
for name in libblkid.so.1 libcom_err.so.2 libe2p.so.2 libeconf.so.0 libext2fs.so.2 libss.so.2 libuuid.so.1; do
    node "$l1/usr/lib/$name" "$rootfs/usr/lib/$name"
done
read_modules() {
    local module
    for module in "$@"; do
        tar -xOf "$kernel_apk" "lib/modules/6.12.118-0-virt/kernel/$module.ko.gz" |
            gzip -dc >"$rootfs/mods/${module##*/}.ko"
    done
}
read_modules drivers/char/hw_random/rng-core drivers/char/hw_random/virtio-rng drivers/virtio/virtio_mmio \
    drivers/block/virtio_blk crypto/crc32c_generic lib/crc16 fs/mbcache fs/jbd2/jbd2 fs/ext4/ext4 \
    net/vmw_vsock/vsock net/vmw_vsock/vmw_vsock_virtio_transport_common net/vmw_vsock/vmw_vsock_virtio_transport
tar -xf "$contrib_apk" -C "$rootfs" --include 'usr/lib/postgresql18/pg_stat_statements.so' \
    --include 'usr/share/postgresql18/extension/pg_stat_statements*'
node "$base/lab-init.sh" "$rootfs/init"
chmod 0755 "$rootfs/init"
chmod 0644 "$rootfs/usr/lib/postgresql18/pg_stat_statements.so" \
    "$rootfs/usr/share/postgresql18/extension/"pg_stat_statements*
printf 'export AHP_PROFILE_PHASE=%s\nAHP_PROFILE_TIMEOUT=%s\nAHP_MINIO_DISK=%s\n' \
    "$phase" "$((timeout - 40))" "$minio_disk" >"$rootfs/lab-options"
python3 - "$rootfs/lab-run" "$phase" "$@" <<'PYTHON'
import shlex
import sys
from pathlib import Path

path, phase, *options = sys.argv[1:]
if any(option.lstrip("-").split("=", 1)[0] in {"out", "phase"} for option in options):
    sys.exit("The host script owns --out and --phase")
command = ["/bin/ahp-v41-profile", "run", "--phase", phase, "--out", f"/results/{phase}", *options]
Path(path).write_text(shlex.join(["exec", *command]) + "\n")
PYTHON

compile_linux() {
    env GOWORK=off GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -trimpath -o "$@"
}
cp "$base/results-send.go.in" "$scratch/results-send.go"
compile_linux "$rootfs/bin/profile-results-send" "$scratch/results-send.go"
if [[ "$phase" != capabilities ]]; then
    bins="${AHP_LAB_BIN_DIR:-$scratch/binaries}"
    if [[ -z "${AHP_LAB_BIN_DIR:-}" ]]; then
        assembled="${AHP_LAB_ASSEMBLY:-$scratch/assembled}"
        if [[ -z "${AHP_LAB_ASSEMBLY:-}" ]]; then
            if [[ ! -f "$base/../../../assemble.py" ]]; then
                echo 'Pass AHP_LAB_ASSEMBLY or AHP_LAB_BIN_DIR when running from an assembled tree' >&2
                exit 65
            fi
            python3 "$base/../../../assemble.py" "$assembled"
        fi
        "$base/build-linux.sh" "$assembled" "$bins"
    fi
    shasum -a 256 "$base/profile-init.go.in" "$base/profile-finish.go.in" "$base/prepare-overlay.py" >>"$output/inputs.sha256"
    for name in ahp-v41-profile run-coordinator state-api ahp-vm-runtime; do
        node "$bins/$name" "$rootfs/bin/$name"
        test -x "$rootfs/bin/$name"
        shasum -a 256 "$bins/$name" >>"$output/inputs.sha256"
        go version -m "$bins/$name" >>"$output/builds.txt"
    done
    case "$phase" in
        fc1|fc2|all-fc)
            mkdir -p "$rootfs/fc-root"/{bin,dev,proc,sbin,sys,tmp}
            node "$rootfs/bin/busybox" "$rootfs/fc-root/bin/busybox"
            node "$bins/ahp-v41-profile" "$rootfs/fc-root/bin/ahp-v41-profile"
            node "$base/fc-init.sh" "$rootfs/fc-root/sbin/init"
            chmod 0755 "$rootfs/fc-root/sbin/init"
            ;;
    esac
fi

# Guest unprivileged postgres needs to traverse the initramfs paths.
find "$rootfs" -type d -exec chmod 0755 {} +
image="$scratch/initramfs.cpio.gz"
node "$pg" "$image"
(cd "$rootfs" && find . -print | cpio -o -H newc 2>"$scratch/cpio.log" | gzip -1) >> "$image"
gzip -t "$image"
shasum -a 256 "$image" | awk '{print $1 "  initramfs.cpio.gz"}' >>"$output/inputs.sha256"
clang -fobjc-arc -Wall -Wextra -Wno-unused-parameter -framework Foundation -framework Virtualization \
    "$base/vz-lab.m" -o "$scratch/vz-lab"
codesign --sign - --entitlements "$base/vz.entitlements.plist" "$scratch/vz-lab"
require_space 512
diskfile="$scratch/pg.raw"
python3 - "$diskfile" "$disk" <<'PY'
import sys
with open(sys.argv[1], "xb") as image:
    image.truncate(int(sys.argv[2]) * 2**20)
PY
{
    printf 'phase=%s\nhost_start_utc=%s\n' "$phase" "$(date -u +%FT%TZ)"
    sw_vers
    sysctl hw.model machdep.cpu.brand_string hw.ncpu hw.memsize
    go version
    printf 'vz_cpus=%s\nvz_memory_mib=%s\npg_disk_mib=%s\ntimeout_sec=%s\n' "$cpu" "$memory" "$disk" "$timeout"
    printf 'pg_storage=VZ virtio-blk uncached full-sync, sparse raw, ext4 noatime\n'
    printf 'minio_on_shared_disk=%s\n' "$minio_disk"
    printf 'source_sha='; git -C "$base" rev-parse HEAD 2>/dev/null || printf 'unavailable\n'
    df -Pk "$output"
} >"$output/host.txt"
echo "AHP_V41_SERIAL=$output/serial.log"
rc=0
"$scratch/vz-lab" "$kernel" "$image" "$diskfile" "$output/results.tar.gz" "$cpu" "$memory" "$timeout" \
    >"$output/serial.log" 2>&1 || rc=$?
{
    printf 'host_finish_utc=%s\nvz_exit=%s\n' "$(date -u +%FT%TZ)" "$rc"
    du -k "$diskfile"
    df -Pk "$output"
} >>"$output/host.txt"
if [[ -f "$output/results.tar.gz" ]]; then
    gzip -t "$output/results.tar.gz"
    python3 "$base/extract-results.py" "$output/results.tar.gz" "$output/artifacts"
    shasum -a 256 "$output/results.tar.gz" > "$output/results.sha256"
fi
cat "$output/serial.log"
if (( rc != 0 )); then exit "$rc"; fi
grep -Eq '^AHP_V41_RESULTS_TRANSFER=0[[:space:]]*$' "$output/serial.log"
grep -Eq '^AHP_V41_LAB_EXIT=0[[:space:]]*$' "$output/serial.log"
test -f "$output/artifacts/substrate.txt"
