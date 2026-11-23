#!/bin/busybox sh
set -eu
bb=/bin/busybox
$bb mount -t devtmpfs devtmpfs /dev
exec >/dev/hvc0 2>&1
$bb mount -t proc proc /proc
$bb mount -t sysfs sysfs /sys
$bb mkdir -p /results /run-ahp/transfer /pgdisk /minio /dev/shm /tmp
$bb chmod 1777 /tmp
$bb mount -t tmpfs -o mode=1777,size=256m tmpfs /dev/shm
$bb mount -t tmpfs -o mode=0700,size=256m tmpfs /results
$bb mount -t tmpfs -o mode=0700,size=256m tmpfs /run-ahp/transfer
$bb mount -t cgroup2 cgroup2 /sys/fs/cgroup
$bb ip link set lo up
ulimit -n 65536
export PATH=/bin:/sbin:/usr/libexec/postgresql18 LC_ALL=C TZ=UTC
export AHP_PROFILE_OUT=/results
. /lab-options
pg=/usr/libexec/postgresql18
groups=/sys/fs/cgroup
pg_started=0
minio_pid=
results_ready=0

finish() {
    rc=$?
    trap - EXIT
    set +e
    if [ "$results_ready" = 1 ] && [ -d "$groups/ahp_sut" ]; then
        {
            for group in ahp_sut ahp_workers ahp_pg ahp_minio ahp_fc; do
                for param in cgroup.events cpu.stat memory.events memory.peak; do
                    printf '%s/%s=' "$group" "$param"
                    $bb tr '\n' ' ' <"$groups/$group/$param"
                    echo
                done
            done
        } > /results/cgroups-final.txt
    fi
    if [ "$pg_started" = 1 ]; then
        /sbin/su-exec 15432:15432 "$pg/pg_ctl" -w -t 20 -m fast -D /pgdisk/data stop
        $bb df -k /pgdisk /minio /results >>/results/substrate.txt
        $bb tail -n 200 /pgdisk/postgres.log > /results/postgres-tail.log
    fi
    if [ -n "$minio_pid" ]; then
        kill "$minio_pid"
        wait "$minio_pid"
    fi
    if [ "$results_ready" = 1 ]; then
        set -o pipefail
        archive=/run-ahp/transfer/results.tar.gz
        if { $bb tar -cf - -C /results . | $bb gzip -1 >"$archive"; } &&
            $bb gzip -t "$archive" && /bin/profile-results-send "$archive"; then
            echo AHP_V41_RESULTS_TRANSFER=0
        else
            echo AHP_V41_RESULTS_TRANSFER=1
            if [ "$rc" = 0 ]; then rc=1; fi
        fi
    fi
    echo "AHP_V41_LAB_EXIT=$rc"
    $bb poweroff -f
}
trap finish EXIT

for mod in rng-core virtio-rng virtio_mmio virtio_blk crc32c_generic crc16 mbcache jbd2 ext4 vsock vmw_vsock_virtio_transport_common vmw_vsock_virtio_transport; do
    if [ "$mod" = rng-core ]; then
        $bb insmod "/mods/$mod.ko" default_quality=1024
    else
        $bb insmod "/mods/$mod.ko"
    fi
done
results_ready=1

test -c /dev/kvm
test -b /dev/vda
/sbin/mke2fs -t ext4 -F -q -m 0 -E nodiscard,lazy_itable_init=0,lazy_journal_init=0 /dev/vda
$bb mount -t ext4 -o noatime /dev/vda /pgdisk
$bb mkdir -m 0700 /pgdisk/slots
export AHP_PROFILE_SLOT_PARENT=/pgdisk/slots
if [ "$AHP_MINIO_DISK" = 1 ]; then
    $bb mkdir -m 0700 /pgdisk/minio
    $bb mount --bind /pgdisk/minio /minio
    minio_store='same bounded virtio ext4 disk as PostgreSQL'
else
    $bb mount -t tmpfs -o mode=0700,size=768m tmpfs /minio
    minio_store='tmpfs (768 MiB filesystem, 1 GiB cgroup)'
fi
export AHP_PROFILE_STORAGE="PostgreSQL 18.7 and runtime slots on VZ virtio-blk uncached/full-sync ext4 over sparse host disk; MinIO on $minio_store"

echo '+cpu +memory +pids' >"$groups/cgroup.subtree_control"
make_group() {
    name="$1"
    limit="$2"
    cap="$3"
    $bb mkdir "$groups/$name"
    printf '%s\n' "$limit" >"$groups/$name/cpu.max"
    printf '%s\n' "$cap" >"$groups/$name/memory.max"
    echo 0 >"$groups/$name/memory.swap.max"
    test "$($bb cat "$groups/$name/cpu.max")" = "$limit"
    test "$($bb cat "$groups/$name/memory.max")" = "$cap"
    test "$($bb cat "$groups/$name/memory.swap.max")" = 0
}
make_group ahp_sut '400000 100000' 4294967296
make_group ahp_workers '200000 100000' "$AHP_WORKER_MEMORY_BYTES"
make_group ahp_pg '200000 100000' 4294967296
make_group ahp_minio '100000 100000' 1073741824
$bb mkdir "$groups/ahp_fc"
echo 0 >"$groups/ahp_fc/memory.swap.max"
test -w "$groups/ahp_fc/cgroup.kill"

{
    printf 'utc='; $bb date -u +%FT%TZ
    $bb uname -srm
    printf 'online_cpus='; $bb getconf _NPROCESSORS_ONLN 2>/dev/null || $bb grep -c '^processor' /proc/cpuinfo
    $bb grep -E '^(MemTotal|SwapTotal):' /proc/meminfo
    printf 'nofile_limit='; ulimit -n
    printf 'pid1_cgroup='; $bb cat /proc/self/cgroup
    printf 'fc_available='; test -c /dev/kvm && echo yes
    /bin/profile-results-send probe-kvm
    /bin/firecracker --version
    printf 'block_size_512b_sectors='; $bb cat /sys/class/block/vda/size
    printf 'runtime_slot_parent=%s\n' "$AHP_PROFILE_SLOT_PARENT"
    $bb grep -E ' /pgdisk | /minio | /results | /sys/fs/cgroup ' /proc/mounts
    $bb df -k /pgdisk /minio /results
    for group in ahp_sut ahp_workers ahp_pg ahp_minio ahp_fc; do
        for param in cpu.max memory.max memory.swap.max cgroup.events; do
            printf '%s/%s=' "$group" "$param"
            $bb tr '\n' ' ' <"$groups/$group/$param"
            echo
        done
    done
} > /results/substrate.txt
$bb cat /results/substrate.txt

$bb mkdir -p /pgdisk/socket
$bb touch /etc/passwd /etc/group
if ! $bb grep -q '^ahplab:' /etc/passwd; then
    printf 'ahplab:x:15432:15432::/pgdisk:/bin/sh\n' >>/etc/passwd
    printf 'ahplab:x:15432:\n' >>/etc/group
fi
$bb chown 15432:15432 /pgdisk /pgdisk/socket
$bb chmod 0700 /pgdisk /pgdisk/socket

"$bb" sh -eu -c '
  echo "$$" > /sys/fs/cgroup/ahp_pg/cgroup.procs
  /sbin/su-exec 15432:15432 /usr/libexec/postgresql18/initdb -D /pgdisk/data -U ahplab --no-locale --encoding=UTF8 --auth=trust
  exec /sbin/su-exec 15432:15432 /usr/libexec/postgresql18/pg_ctl -w -t 30 -D /pgdisk/data -l /pgdisk/postgres.log -o "\
     -h 127.0.0.1 -p 15432 -k /pgdisk/socket -c shared_buffers=1GB -c max_connections=250 \
     -c fsync=on -c synchronous_commit=on -c track_io_timing=on -c shared_preload_libraries=pg_stat_statements \
     -c pg_stat_statements.track=all -c max_wal_size=256MB -c min_wal_size=64MB \
     -c log_checkpoints=on -c log_lock_waits=on -c log_parameter_max_length_on_error=0" start
' >/run-ahp/pg-bootstrap.log 2>&1 || {
    $bb tail -n 30 /run-ahp/pg-bootstrap.log
    if [ -f /pgdisk/postgres.log ]; then $bb tail -n 30 /pgdisk/postgres.log; fi
    exit 1
}
pg_started=1
export AHP_PROFILE_PG_HOST=127.0.0.1 AHP_PROFILE_PG_PORT=15432
export AHP_PROFILE_PG_DSN='postgres://ahplab@127.0.0.1:15432/ahp_profile?sslmode=disable'
"$pg/createdb" -h 127.0.0.1 -p 15432 -U ahplab ahp_profile
"$pg/psql" -XAt -v ON_ERROR_STOP=1 "$AHP_PROFILE_PG_DSN" <<'SQL' > /results/postgres.txt
CREATE EXTENSION pg_stat_statements;
SELECT name || '=' || setting || COALESCE(' ' || unit, '') FROM pg_settings
WHERE name IN ('server_version','fsync','synchronous_commit','track_io_timing','max_connections',
 'shared_buffers','shared_preload_libraries','pg_stat_statements.track','data_directory','max_wal_size') ORDER BY name;
SELECT 'pg_stat_statements_version=' || extversion FROM pg_extension WHERE extname = 'pg_stat_statements';
CREATE TEMP TABLE profiling_probe (value text);
INSERT INTO profiling_probe VALUES ('probe');
SELECT 'stats_entries=' || count(*) FROM pg_stat_statements;
SQL
pid="$($bb head -n 1 /pgdisk/data/postmaster.pid)"
printf 'postgres_cgroup=' >>/results/postgres.txt
$bb cat "/proc/$pid/cgroup" >>/results/postgres.txt
$bb cat /results/postgres.txt

export MINIO_ROOT_USER=ahp-profile-local
MINIO_ROOT_PASSWORD="$($bb od -A n -N 24 -t x1 /dev/urandom | $bb tr -d ' \n')"
export MINIO_ROOT_PASSWORD
export AHP_PROFILE_MINIO_ENDPOINT=http://127.0.0.1:19000
export AHP_PROFILE_MINIO_ACCESS_KEY="$MINIO_ROOT_USER" AHP_PROFILE_MINIO_SECRET_KEY="$MINIO_ROOT_PASSWORD"
{
    /bin/v41-minio --version
    printf 'minio_store=%s\n' "$minio_store"
} > /results/minio.txt
"$bb" sh -eu -c '
    echo "$$" > /sys/fs/cgroup/ahp_minio/cgroup.procs
    exec /bin/v41-minio server /minio --address 127.0.0.1:19000 --console-address 127.0.0.1:19001
' >/run-ahp/minio.log 2>&1 &
minio_pid=$!
$bb timeout 45 "$bb" sh -eu -c '
    until /bin/busybox wget -q -O /dev/null http://127.0.0.1:19000/minio/health/ready; do
        /bin/busybox sleep 0.2
    done
' || {
    echo AHP_V41_MINIO_READINESS_FAILED
    exit 1
}
printf 'minio_ready=yes\nminio_cgroup=' >> /results/minio.txt
$bb cat "/proc/$minio_pid/cgroup" >> /results/minio.txt
$bb cat /results/minio.txt

export AHP_PROFILE_SUT_CGROUP="$groups/ahp_sut"
export AHP_PROFILE_WORKER_CGROUP="$groups/ahp_workers"
export AHP_PROFILE_FC_CGROUP="$groups/ahp_fc"
export AHP_PROFILE_FC_BINARY=/bin/firecracker
export AHP_PROFILE_FC_KERNEL=/assets/guest-kernel
export AHP_PROFILE_FC_ROOTFS=/assets/guest.ext4
export AHP_PROFILE_BIN_DIR=/bin
echo AHP_V41_SUBSTRATE_READY
if [ "$AHP_PROFILE_PHASE" = capabilities ]; then
    "$pg/psql" -XAt -v ON_ERROR_STOP=1 "$AHP_PROFILE_PG_DSN" <<'SQL' > /results/commit-probe.txt
\timing on
CREATE TABLE profiling_sync_probe (i integer NOT NULL);
SELECT format('INSERT INTO profiling_sync_probe VALUES (%s)', n)
    FROM generate_series(1, 16) AS n \gexec
DROP TABLE profiling_sync_probe;
SQL
    exit 0
fi
if [ -d /fc-root ]; then
    /sbin/mke2fs -t ext4 -q -F -d /fc-root /assets/guest.ext4 64M
fi
unset MINIO_ROOT_USER MINIO_ROOT_PASSWORD
output="/results/$AHP_PROFILE_PHASE"
$bb mkdir -p "$output"
$bb timeout -s TERM -k 5 "$AHP_PROFILE_TIMEOUT" "$bb" sh /lab-run
