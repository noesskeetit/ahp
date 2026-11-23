#!/bin/busybox sh
set -eu
bb=/bin/busybox
$bb mount -t proc proc /proc
$bb mount -t sysfs sysfs /sys
if [ ! -c /dev/null ]; then
    $bb mount -t devtmpfs devtmpfs /dev
fi
rc=0
"$bb" env -i PATH=/bin HOME=/tmp TERM=dumb /bin/ahp-v41-profile worker || rc=$?
echo "AHP_V41_FC_WORKER_EXIT=$rc"
# The runtime owns VMM teardown and the fence sample includes cgroup cleanup.
while :; do "$bb" sleep 120; done
