package toolsapi

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncEffectFile(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
	return err
}
