//go:build !linux

package compute

import "os"

// openHostPTY fails closed on non-Linux build hosts (e.g. the macOS control
// machine) because the interactive PTY contract is a property of the Linux
// target runtime. We return Unavailable instead of faking a terminal: a
// non-interactive substitute would silently violate the PTY acceptance clause.
func openHostPTY(_ PTYSpec) (*os.File, string, error) {
	return nil, "", Unavailable("host", "interactive PTY requires the Linux target runtime")
}

// ptyResize mirrors openHostPTY and is unreachable on non-Linux hosts.
func ptyResize(_ uintptr, _, _ uint16) error {
	return Unavailable("host", "interactive PTY requires the Linux target runtime")
}

// The host sandbox's resource ceilings are Linux resource numbers; on other build
// hosts the sandbox cannot read a ceiling and reports it as unavailable rather than
// guessing one.
const (
	sandboxNProc  = 6 // RLIMIT_NPROC on Linux
	sandboxNoFile = 7 // RLIMIT_NOFILE on Linux
	sandboxAS     = -1 // no address-space ceiling is applied on this platform
)

// hardLimit fails closed: this build host has no readable resource ceiling here.
func hardLimit(_ int) (uint64, error) {
	return 0, Unavailable("host", "resource limits are not readable on this platform")
}

// supportsMemoryCeiling is false here: the resource number for an address-space
// ceiling is platform-specific, and guessing it would set some OTHER limit. A
// declared memory quota on a build host that cannot enforce it is refused instead
// of being dropped, which is the same rule the pids and file ceilings follow.
func supportsMemoryCeiling() bool { return false }
