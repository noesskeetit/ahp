//go:build linux

package main

import (
	"fmt"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Install a TSYNC filter on ALL runtime threads before reading untrusted bytes.
// Default deny also excludes open/openat, socket/connect, fork, exec and ptrace.
func sandbox() error {
	// no_new_privs belongs to an OS thread. Keep PR_SET_NO_NEW_PRIVS and
	// SECCOMP_SET_MODE_FILTER on that thread even if a syscall yields the P.
	// TSYNC installs the filter on all runtime threads before we unpin.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	for resource, limit := range map[int]uint64{unix.RLIMIT_CPU: 2, unix.RLIMIT_AS: 2 << 30, unix.RLIMIT_FSIZE: 0, unix.RLIMIT_CORE: 0, unix.RLIMIT_NOFILE: 16} {
		if err := unix.Setrlimit(resource, &unix.Rlimit{Cur: limit, Max: limit}); err != nil {
			return err
		}
	}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		return err
	}
	var arch uint32
	switch runtime.GOARCH {
	case "amd64":
		arch = unix.AUDIT_ARCH_X86_64
	case "arm64":
		arch = unix.AUDIT_ARCH_AARCH64
	default:
		return fmt.Errorf("unsupported sandbox architecture")
	}
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 4},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: arch, Jt: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_KILL_PROCESS},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
	}
	// Runtime preemption may signal its own threads, never another process.
	filter = append(filter,
		unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_TGKILL, Jf: 4},
		unix.SockFilter{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 16},
		unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: uint32(unix.Getpid()), Jt: 1},
		unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)},
		unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
	)
	calls := []uint32{unix.SYS_READ, unix.SYS_WRITE, unix.SYS_CLOSE, unix.SYS_FSTAT, unix.SYS_LSEEK,
		unix.SYS_MMAP, unix.SYS_MUNMAP, unix.SYS_MPROTECT, unix.SYS_MADVISE, unix.SYS_BRK,
		unix.SYS_RT_SIGACTION, unix.SYS_RT_SIGPROCMASK, unix.SYS_RT_SIGRETURN, unix.SYS_SIGALTSTACK,
		unix.SYS_FUTEX, unix.SYS_SCHED_YIELD, unix.SYS_NANOSLEEP, unix.SYS_CLOCK_NANOSLEEP, unix.SYS_CLOCK_GETTIME,
		unix.SYS_GETPID, unix.SYS_GETTID, unix.SYS_EXIT, unix.SYS_EXIT_GROUP,
		unix.SYS_SCHED_GETAFFINITY, unix.SYS_GETRANDOM, unix.SYS_RESTART_SYSCALL}
	for _, call := range calls {
		filter = append(filter, unix.SockFilter{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: call, Jf: 1}, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW})
	}
	filter = append(filter, unix.SockFilter{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(unix.EPERM)})
	prog := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	result, _, errno := unix.Syscall(unix.SYS_SECCOMP, unix.SECCOMP_SET_MODE_FILTER, unix.SECCOMP_FILTER_FLAG_TSYNC, uintptr(unsafe.Pointer(&prog)))
	if errno != 0 {
		return errno
	}
	if result != 0 {
		return fmt.Errorf("sandbox thread synchronization refused")
	}
	return nil
}
