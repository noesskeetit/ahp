package vmruntime

import (
	"errors"
	"syscall"

	"golang.org/x/sys/unix"
)

const processWaitSupported = true

func waitProcessExit(pid int) error {
	syscall.ForkLock.RLock()
	fd, err := unix.Kqueue()
	if err == nil {
		unix.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return err
	}
	defer unix.Close(fd)

	change := []unix.Kevent_t{{
		Ident: uint64(pid), Filter: unix.EVFILT_PROC,
		Flags: unix.EV_ADD | unix.EV_ONESHOT, Fflags: unix.NOTE_EXIT,
	}}
	for {
		_, err := unix.Kevent(fd, change, nil, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		break
	}

	var events [1]unix.Kevent_t
	for {
		n, err := unix.Kevent(fd, nil, events[:], nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if n == 1 && events[0].Flags&unix.EV_ERROR != 0 {
			return syscall.Errno(events[0].Data)
		}
		if n == 1 && events[0].Fflags&unix.NOTE_EXIT != 0 {
			return nil
		}
		return unix.EIO
	}
}
