# Compute class-X microVM supervisor

This directory contains the host-side (out-of-guest) supervisor that serves the
**class X** microVM path described in the product architecture
(`ahp-agents-space-architecture` PDF, rev 3.2). Class X runs untrusted,
arbitrary developer code inside a real microVM (Firecracker, or Kata when an
operator wires one in). The supervisor is the daemon that owns the hypervisor;
the `compute-host serve` control plane reaches it over a unix socket via the
`firecracker` microVM backend.

## Trust model

* Classes **S** (trusted multi-tenant) and **C** (developer container/deploy)
  run on the `host` backend.
* Class **X** (arbitrary code) MUST run on a microVM backend. The control-plane
  registry refuses to bind class X to the `host` backend and fails closed when
  no microVM backend is registered — see `internal/compute/backend.go`
  (`Registry.Require`). There is **no in-process simulation** of a microVM.
* The supervisor itself (`compute-host supervise`) **fails closed**: without a
  Linux host, without `/dev/kvm`, or without the Firecracker image paths, it
  returns an `UnavailableError` and never binds the socket. This is what makes
  "no microVM is simulated under a misleading label" testable on any machine,
  including the macOS control host (white-box gate
  `internal/compute/gate_internal_test.go`).

> **This Mac has no `/dev/kvm`.** Class X cannot be exercised here. The
> supervisor must run on a Linux/KVM node. Do **not** simulate a microVM to
> make a check pass. See the kvm-lab stand (`labs/kvm`, coordinated on the forum
> topic `20261029-kvm-lab-activation`) for the shared Linux/KVM node — do not
> duplicate it.

## Build

From the repo root:

```sh
export GOCACHE=/tmp/ahp-compute-gocache
go build -o compute-host ./cmd/compute-host
```

## Run the supervisor (Linux + KVM only)

The supervisor reads its configuration entirely from the environment:

| Variable | Required | Meaning |
| --- | --- | --- |
| `AHP_COMPUTE_SUPERVISOR_SOCKET` | yes | unix socket the control plane dials (`compute-host supervise`) |
| `AHP_COMPUTE_RUNTIME` | no | `firecracker` (default) or `kata` |
| `AHP_FC_BINARY` | yes (firecracker) | path to the `firecracker` binary |
| `AHP_FC_KERNEL` | yes (firecracker) | path to the guest kernel (`vmlinux`) |
| `AHP_FC_ROOTFS` | yes (firecracker) | path to the base read-only rootfs (ext4) |
| `AHP_FC_DATADIR` | yes (firecracker) | per-VM working directory root |
| `AHP_FC_VCPUS` | no | default vCPU count (default `1`) |
| `AHP_FC_MEM_MB` | no | default memory in MiB (default `256`) |
| `AHP_FC_GUEST_AGENT` | no | in-guest agent base URL, e.g. `http://169.254.0.2:8080`. Without it a VM boots but `exec`/`pty` is unavailable. |
| `AHP_FC_TAP` | no | host tap device. Empty means **no network → deny-all egress** (fail-closed by default). |
| `AHP_FC_GUEST_MAC` | no | guest MAC; required together with `AHP_FC_TAP`. |

`Kata` is supported by the architecture but has **no in-tree runtime** here:
`AHP_COMPUTE_RUNTIME=kata` fails closed with `Unavailable` until an operator
supplies a `VMRuntime` to `NewSupervisor`. See `kata.env.example`.

Example (fill paths from the kvm-lab node, then run under systemd or directly):

```sh
set -a
. ./firecracker.env.example   # edit the paths first; never commit real paths/secrets
set +a
export AHP_COMPUTE_SUPERVISOR_SOCKET=/run/ahp/compute-fc.sock
./compute-host supervise
```

The control plane (`compute-host serve`) is pointed at the supervisor with
`AHP_COMPUTE_FC_SOCKET=/run/ahp/compute-fc.sock`; that registers the
`firecracker` backend so class X becomes served. Without it class X stays
fail-closed.

## Pre-flight checks

```sh
./kvm-check.sh    # exits non-zero if /dev/kvm, KVM module, or firecracker is missing
```

## Node setup

`setup.sh` loads the KVM modules, checks group/udev access and the data
directories. It does **not** download any images and does **not** embed URLs or
secrets — fetching the Firecracker binary, guest kernel, and rootfs is an
external prerequisite provided by the kvm-lab operator (see `setup.sh` notes).

## Acceptance runner

`acceptance-kvm.sh` is the reproducible Linux/KVM acceptance runner. It always
proves the **fail-closed** property (works on any OS, including macOS): with no
Firecracker config the supervisor returns `Unavailable` and does not create the
socket. On a Linux/KVM node with full `AHP_FC_*` config it additionally boots a
guest and smoke-tests `/v1/health`, `/v1/acquire`, `/v1/exec`, `/v1/release`.
On a Mac the live-VM portion is reported **SKIPPED — KVM gate open**, never
faked.

## systemd

`compute-supervisor.service` is a template unit. Set the `EnvironmentFile` to a
real, unworld-readable env file on the node.
