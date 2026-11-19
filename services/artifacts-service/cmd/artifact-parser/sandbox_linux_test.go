//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSandboxDeniesNetworkFilesystemExecAndOversizedMemory(t *testing.T) {
	if os.Getenv("AHP_SANDBOX_PROBE") == "1" {
		runtime.GOMAXPROCS(1)
		runtime.GC()
		if err := sandbox(); err != nil {
			os.Exit(10)
		}
		if _, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0); !errors.Is(err, unix.EPERM) {
			os.Exit(11)
		}
		if _, err := unix.Open("/etc/passwd", unix.O_RDONLY, 0); !errors.Is(err, unix.EPERM) {
			os.Exit(12)
		}
		if err := unix.Tgkill(1, 1, 0); !errors.Is(err, unix.EPERM) {
			os.Exit(15)
		}
		if _, _, err := unix.RawSyscall(unix.SYS_EXECVE, 0, 0, 0); err != unix.EPERM {
			os.Exit(13)
		}
		if _, err := unix.Mmap(-1, 0, 3<<30, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANON); !errors.Is(err, unix.ENOMEM) {
			os.Exit(14)
		}
		os.Exit(0)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestSandboxDeniesNetworkFilesystemExecAndOversizedMemory$")
	cmd.Env = []string{"AHP_SANDBOX_PROBE=1", "GOMAXPROCS=1"}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("sandbox probe: %v %s", err, out)
	}
}

func TestSandboxCPULimit(t *testing.T) {
	if os.Getenv("AHP_SANDBOX_CPU") == "1" {
		runtime.GOMAXPROCS(1)
		runtime.GC()
		if sandbox() != nil {
			os.Exit(10)
		}
		for {
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSandboxCPULimit$")
	cmd.Env = []string{"AHP_SANDBOX_CPU=1", "GOMAXPROCS=1"}
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatal("CPU rlimit did not kill parser")
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("CPU probe must be killed: %v", err)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || (status.Signal() != syscall.SIGKILL && status.Signal() != syscall.SIGXCPU) {
		t.Fatalf("CPU probe exited without CPU kill: %v", err)
	}
}
