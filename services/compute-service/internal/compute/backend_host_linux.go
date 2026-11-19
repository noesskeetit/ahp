//go:build linux

package compute

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// openHostPTY allocates a real pseudoterminal on the Linux target runtime using
// the ptmux/pts interface directly. golang.org/x/sys/unix in this module does
// not export Openpty, so we drive the ioctls by hand:
//   - open /dev/ptmx (no controlling terminal),
//   - TIOCGPTN to learn the pts number,
//   - TIOCSPTLCK to unlock the matching /dev/pts/<n> slave.
//
// This is the only code path that produces an interactive PTY; on any other
// platform the equivalent function fails closed with Unavailable.
func openHostPTY(spec PTYSpec) (*os.File, string, error) {
	ptmx, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", Unavailable("host", fmt.Sprintf("cannot open /dev/ptmx: %v", err))
	}
	num, err := unix.IoctlGetInt(ptmx, unix.TIOCGPTN)
	if err != nil {
		_ = unix.Close(ptmx)
		return nil, "", Unavailable("host", fmt.Sprintf("TIOCGPTN failed: %v", err))
	}
	if err := unix.IoctlSetPointerInt(ptmx, unix.TIOCSPTLCK, 0); err != nil {
		_ = unix.Close(ptmx)
		return nil, "", Unavailable("host", fmt.Sprintf("TIOCSPTLCK failed: %v", err))
	}
	if spec.Cols > 0 && spec.Rows > 0 {
		// Best effort: size the terminal before the slave is opened.
		_ = unix.IoctlSetWinsize(ptmx, unix.TIOCSWINSZ, &unix.Winsize{Row: spec.Rows, Col: spec.Cols})
	}
	// Wrap the raw master fd in an *os.File so it closes with the session.
	master := os.NewFile(uintptr(ptmx), "/dev/ptmx")
	return master, fmt.Sprintf("/dev/pts/%d", num), nil
}

// ptyResize applies a window size change to the master side of the pty.
func ptyResize(fd uintptr, cols, rows uint16) error {
	if err := unix.IoctlSetWinsize(int(fd), unix.TIOCSWINSZ, &unix.Winsize{Row: rows, Col: cols}); err != nil {
		return Unavailable("host", fmt.Sprintf("TIOCSWINSZ failed: %v", err))
	}
	return nil
}

// sandboxNProc and sandboxNoFile name the two resource ceilings the host sandbox
// applies. The numbers are platform constants, so they live here and the portable
// backend refers to these names.
const (
	sandboxNProc  = unix.RLIMIT_NPROC
	sandboxNoFile = unix.RLIMIT_NOFILE
	// sandboxAS bounds the address space of the confined process. It is a ceiling
	// on virtual memory, not cgroup RSS accounting: it stops an allocation that
	// passes the quota, and it is not a claim that measured resident memory equals
	// the quota. cgroup memory.max would be the honest equivalent of the class
	// quota, and this host does not delegate a cgroup subtree to the service user.
	sandboxAS = unix.RLIMIT_AS
)

// supportsMemoryCeiling reports whether this platform can bound a confined
// command's address space at all.
func supportsMemoryCeiling() bool { return true }

// hardLimit is the largest value this process may set for res. Raising past it is
// refused by the kernel, so a quota whose class default already sits above this
// host's ceiling must be clamped rather than handed to the shell verbatim.
func hardLimit(res int) (uint64, error) {
	var lim unix.Rlimit
	if err := unix.Getrlimit(res, &lim); err != nil {
		return 0, Unavailable("host", fmt.Sprintf("cannot read this host's resource limit: %v", err))
	}
	return lim.Max, nil
}
