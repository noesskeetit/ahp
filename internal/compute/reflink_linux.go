//go:build linux

package compute

import (
	"fmt"
	"os"
	"syscall"
)

// ficlone is the Linux FICLONE ioctl request code: it asks the filesystem to
// create a reflink (copy-on-write) clone of a file. On filesystems that do not
// support it (ext4 without the feature, tmpfs, ...) the ioctl fails and the
// caller falls back to a byte copy.
const ficlone = 0x40049409

func reflinkCopy(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, out.Fd(), uintptr(ficlone), in.Fd()); errno != 0 {
		return fmt.Errorf("reflink: %w", errno)
	}
	return nil
}
