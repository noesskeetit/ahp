package compute

import (
	"context"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// Env variables that configure the Linux microVM supervisor. These are the
// concrete external requirements handed to the operations foreman once the
// control-plane work is done; a Linux/KVM node must provide them.
const (
	EnvRuntime      = "AHP_COMPUTE_RUNTIME" // "firecracker" (default) | "kata"
	EnvFCBinary     = "AHP_FC_BINARY"       // path to firecracker
	EnvFCKernel     = "AHP_FC_KERNEL"       // path to guest kernel (vmlinux)
	EnvFCRootFS     = "AHP_FC_ROOTFS"       // path to base rootfs ext4
	EnvFCDataDir    = "AHP_FC_DATADIR"      // per-VM working directory root
	EnvFCVCPUs      = "AHP_FC_VCPUS"        // default vCPU count
	EnvFCMemMB      = "AHP_FC_MEM_MB"       // default memory in MiB
	EnvFCGuestAgent = "AHP_FC_GUEST_AGENT"  // in-guest agent base URL
	EnvFCTap        = "AHP_FC_TAP"          // optional host tap device
	EnvFCGuestMAC   = "AHP_FC_GUEST_MAC"    // optional guest MAC
)

// Supervise is the Linux supervisor entrypoint: it builds a real VMRuntime from
// the environment and serves the class X supervisor protocol on the unix socket
// until ctx is cancelled.
//
// It fails closed and verifiably: on a non-Linux host, a host without /dev/kvm,
// or a Linux host that has not supplied the Firecracker images via the env
// variables above, it returns an UnavailableError rather than starting a fake
// supervisor. This is what makes the "no microVM is simulated" property testable
// on any machine, including the macOS control host.
func Supervise(ctx context.Context, socket string) error {
	rt, err := RuntimeFromEnv()
	if err != nil {
		return err
	}
	sup, err := NewSupervisor(socket, rt)
	if err != nil {
		return err
	}
	return sup.ListenAndServe(ctx)
}

// RuntimeFromEnv constructs the VMRuntime named by AHP_COMPUTE_RUNTIME from
// environment configuration. It is exported so the supervisor cmd and tests can
// exercise the same gate the production supervisor uses.
func RuntimeFromEnv() (VMRuntime, error) {
	name := strings.ToLower(strings.TrimSpace(os.Getenv(EnvRuntime)))
	if name == "" {
		name = "firecracker"
	}
	switch name {
	case "firecracker":
		if runtime.GOOS != "linux" {
			return nil, Unavailable("firecracker", "the compute supervisor requires Linux; this host is "+runtime.GOOS)
		}
		if msg := kvmHostProblem(); msg != "" {
			return nil, Unavailable("firecracker", msg)
		}
		cfg, err := firecrackerConfigFromEnv()
		if err != nil {
			return nil, err
		}
		return NewFirecrackerRuntime(cfg)
	case "kata":
		// Kata is a supported microVM class by the architecture, but no in-tree
		// Kata VMRuntime ships here. The Supervisor accepts one if an operator
		// wires it; until then this path refuses instead of pretending.
		return nil, Unavailable("kata", "no in-tree Kata VMRuntime; supply one to NewSupervisor")
	default:
		return nil, invalid("compute: unknown " + EnvRuntime + "=" + name)
	}
}

func firecrackerConfigFromEnv() (FirecrackerConfig, error) {
	cfg := FirecrackerConfig{
		Binary:      os.Getenv(EnvFCBinary),
		KernelImage: os.Getenv(EnvFCKernel),
		RootFSImage: os.Getenv(EnvFCRootFS),
		DataDir:     os.Getenv(EnvFCDataDir),
		GuestAgent:  os.Getenv(EnvFCGuestAgent),
		TapName:     os.Getenv(EnvFCTap),
		GuestMAC:    os.Getenv(EnvFCGuestMAC),
	}
	if v := os.Getenv(EnvFCVCPUs); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.VCPUCount = n
		}
	}
	if v := os.Getenv(EnvFCMemMB); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			cfg.MemSizeMB = n
		}
	}
	// Enumerate the specific missing pieces so the foreman receives a precise
	// external requirement instead of a generic failure.
	var missing []string
	require := func(envVar, val string) {
		if val == "" {
			missing = append(missing, envVar)
		}
	}
	require(EnvFCBinary, cfg.Binary)
	require(EnvFCKernel, cfg.KernelImage)
	require(EnvFCRootFS, cfg.RootFSImage)
	require(EnvFCDataDir, cfg.DataDir)
	if len(missing) > 0 {
		return cfg, Unavailable("firecracker", "missing required supervisor config env: "+strings.Join(missing, ", "))
	}
	return cfg, nil
}
