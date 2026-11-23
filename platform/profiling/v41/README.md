# Tool Gateway v4.1 profiling lab

The lab runs the profiling CLI and the production State API, Run Coordinator and VM Runtime from the assembled Go module. The baseline is `557090861c48f8b18f1881d907d85f52d8a023a0`. The build adds profiling instrumentation through command-specific Go overlays. The overlays verify their source anchors during the build. The product source tree stays intact.

## Apple Silicon replay

Use macOS with Apple Virtualization nested virtualization support, Xcode Command Line Tools, Python 3 and the Go toolchain required by the assembled module. On the Mac16,7 M4 Pro with 14 host cores and 48 GiB RAM, the default VM has 10 vCPUs and 16 GiB RAM. The VM has no network interface attached by macOS. The driver and MCP fixture use the VM's loopback interface, and the only host-facing channel transfers results over VZ vsock.

```sh
platform/profiling/v41/run-vz.sh capabilities /tmp/ahp-v41-capabilities
platform/profiling/v41/run-vz.sh smoke /tmp/ahp-v41-smoke
platform/profiling/v41/run-vz.sh p0 /tmp/ahp-v41-p0
platform/profiling/v41/run-vz.sh p1 /tmp/ahp-v41-p1
platform/profiling/v41/run-vz.sh p2 /tmp/ahp-v41-p2
platform/profiling/v41/run-vz.sh p3 /tmp/ahp-v41-p3
platform/profiling/v41/run-vz.sh p4 /tmp/ahp-v41-p4 -- --rate "$healthy_p2_turns_per_second"
platform/profiling/v41/run-vz.sh all-fc /tmp/ahp-v41-fc
```

Each invocation needs a new evidence directory. It creates a fresh PostgreSQL cluster and MinIO store. Use separate P1–P4 invocations to reset storage between phases. Combined `all-process` and `all-fc` are available. A shortened command such as `p1 ... -- --quick 3s --seed 0` is suitable for diagnostics. `build-linux.sh <assembled module> <new binary directory>` can compile once. Set `AHP_LAB_BIN_DIR` to reuse that directory, or `AHP_LAB_ASSEMBLY` to use a preassembled module. The latter is required when invoking this script from an assembled tree without a matching prebuilt binary directory.

Build the assembled sources from a clean product revision. The build embeds that revision in each run manifest. When invoking the builder from a source archive without Git metadata, set `AHP_LAB_PRODUCT_SHA` to its full commit SHA.

| Variable | Default | Purpose |
| --- | --- | --- |
| `AHP_LAB_CPU` | 10 | VM vCPUs, at least 8 |
| `AHP_LAB_MEMORY_MIB` | 16384 | VM RAM, at least 12288 |
| `AHP_LAB_PG_DISK_MIB` | 1280 | Maximum size of the sparse virtio disk |
| `AHP_LAB_MINIO_DISK` | 0 | Set to 1 to put MinIO on the same bounded ext4 device as PostgreSQL and runtime slots |
| `AHP_LAB_TIMEOUT_SEC` | 14400 | Host VM timeout in seconds with an earlier guest timeout for evidence transfer |
| `AHP_LAB_L1_ASSETS` | `/tmp/ahp-v4-review.EnT6vw/lab/l1` | Prior verified Linux BusyBox, mke2fs and dependencies, MinIO and Firecracker assets |
| `AHP_LAB_PG_INITRAMFS` | `/tmp/ahp-v32-kvm-pg-bridge/native-pg-addon.cpio.gz` | Prior PostgreSQL 18.7 Alpine initramfs layer |
| `AHP_LAB_VZ_KERNEL` | `/tmp/ahp-fc.782AaF/l1-Image` | Prior uncompressed Alpine Linux 6.12.118-0-virt kernel |
| `AHP_LAB_KERNEL_APK`, `AHP_LAB_PG_CONTRIB_APK` | Alpine mirror | Local copies of the pinned arm64 APKs when replaying offline |

The launcher reuses the existing acceptance artifacts and checks the input digests recorded in `run-vz.sh`. Keep copies of those inputs if replaying past temporary directory cleanup. The MinIO static binary reports development build metadata and Go module revision `9e49d5e7a648`. The Alpine v3.22 kernel package supplies virtio, ext4 and vsock modules for the pinned boot kernel. The v3.23 contrib package supplies `pg_stat_statements` 1.12. Firecracker receives a separate ext4 built by the VM with a clean worker environment and the pinned microVM kernel. [Alpine Linux kernel package](https://pkgs.alpinelinux.org/package/v3.22/main/aarch64/linux-virt) · [Alpine PostgreSQL contrib](https://pkgs.alpinelinux.org/package/v3.23/main/aarch64/postgresql18-contrib)

## Limits and evidence

| Component | Cgroup v2 CPU quota | Memory limit | Swap |
| --- | ---: | ---: | --- |
| State, Tools, Coordinator, Runtime | 4 vCPU | 4 GiB | 0 |
| Process workers | 2 vCPU | 2 GiB | 0 |
| PostgreSQL | 2 vCPU | 4 GiB | 0 |
| MinIO | 1 vCPU | 1 GiB | 0 |

The load driver and MCP fixture are outside the SUT cgroup but run inside the same VZ VM. CPU quotas match container `--cpus` semantics and let workloads migrate across virtual CPUs. Linux sets the file descriptor limit to 65536. The FC cgroup is outside the SUT cgroup, exposes `cgroup.kill`, and uses the VM's remaining budget. `capabilities` checks the real KVM API and runs 16 small synchronous transactions in the disposable database. Performance results come from the profiling CLI phases.

PostgreSQL 18.7 uses ext4 on an uncached VZ virtio block attachment in full-sync mode over an APFS sparse raw file. It enables `fsync`, `synchronous_commit`, `track_io_timing`, and `pg_stat_statements.track=all`, with 1 GiB of shared buffers and 250 connections. The small disk budget sets `max_wal_size=256MB` and `min_wal_size=64MB`. Runtime slots use a private directory on the same ext4 device, so `slot.json` syncs to disk. The launcher checks disk capacity before starting and aborts if free host space falls below 512 MiB during the run.

MinIO defaults to a separate 768 MiB tmpfs. Those storage timings reflect the memory-backed filesystem, which can fill during long artifact-heavy phases. For the longer profiles use, for example, `AHP_LAB_MINIO_DISK=1 AHP_LAB_PG_DISK_MIB=2560` on a host volume with enough free space. This option makes durable artifacts and PostgreSQL share the same virtio device and capacity. The lab preserves both stores throughout each phase.

`host.txt`, `inputs.sha256`, `builds.txt`, and `serial.log` identify the host, artifacts, and process result. The VM sends only `/results` to a private host archive over vsock. The guest stages a gzip-verified archive and sends its length and SHA-256 before the data. The host reads the declared payload of up to 256 MiB, verifies the digest, persists it, and acknowledges it without waiting for the connection to close. The launcher checks the compressed stream and extracts it into `artifacts/`. PostgreSQL data, MinIO storage, and generated authentication material stay in the disposable VM. The launcher removes its staging directory and PG disk on exit.

`state-finish-commits.tsv` records the session ID, finished turn sequence, promoted turn sequence (zero when absent), and Linux host monotonic nanoseconds immediately after the State API gets a successful PostgreSQL `COMMIT` response. Join it to `turns.jsonl` on the session ID and turn sequence. `durable_verified_ns` records the client's later verification of the terminal journal and MCP effects. Firecracker's tool durations use the guest's clock and are reported separately.

For a matched P4 control, pass `--inject-faults=false` with the same rate, lane count and duration as the fault run. Run `run-vz.sh fc2 <new evidence directory> -- --fc-lanes 16 --inject-faults=false` to isolate the 16-guest control. The fault variant uses `--inject-faults=true`. The manifest records the effective fault mode for each run.
