package compute

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// pointKVMAt aims the KVM gate at a path the test controls, and puts the real
// device back afterwards. Production always reads /dev/kvm.
func pointKVMAt(t *testing.T, path string) {
	t.Helper()
	prev := kvmDevicePath
	kvmDevicePath = path
	t.Cleanup(func() { kvmDevicePath = prev })
}

// TestKVMGateSeparatesForbiddenFromAbsent is the reason this gate exists at all.
// On a shared VM the device node is normally present with mode 0660 root:kvm
// while the calling user is not in group kvm; a gate that only stats the node
// calls that host ready, and the guest then dies on EACCES inside the component
// that was trusted to name the blocker. The gate must open the device, and it
// must say which of the two problems it found.
func TestKVMGateSeparatesForbiddenFromAbsent(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the KVM device gate is a Linux question")
	}
	if os.Geteuid() == 0 {
		t.Skip("root can open anything, so a refusal cannot be measured here")
	}

	t.Run("present but forbidden", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kvm-forbidden")
		if err := os.WriteFile(path, []byte("not a device"), 0o600); err != nil {
			t.Fatalf("seed: %v", err)
		}
		if err := os.Chmod(path, 0o000); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("the fixture must exist for this case to mean anything: %v", err)
		}
		if f, err := os.OpenFile(path, os.O_RDWR, 0); err == nil {
			_ = f.Close()
			t.Skip("this user can open the fixture, so refusal cannot be measured here")
		}
		// The stat-only gate would have called this host ready.
		pointKVMAt(t, path)
		msg := kvmProblem()
		if msg == "" {
			t.Fatal("a forbidden device must not read as available")
		}
		if !strings.Contains(msg, "may not use it") {
			t.Fatalf("a forbidden device must be named as a permission problem, got %q", msg)
		}
		if strings.Contains(msg, "does not exist") {
			t.Fatalf("a present device must not be reported as missing: %q", msg)
		}
	})

	t.Run("absent", func(t *testing.T) {
		pointKVMAt(t, filepath.Join(t.TempDir(), "no-such-node"))
		msg := kvmProblem()
		if msg == "" {
			t.Fatal("a missing device must not read as available")
		}
		if !strings.Contains(msg, "does not exist") {
			t.Fatalf("a missing device must be named as missing, got %q", msg)
		}
	})

	t.Run("usable", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kvm-usable")
		if err := os.WriteFile(path, []byte("stand-in"), 0o600); err != nil {
			t.Fatalf("seed: %v", err)
		}
		pointKVMAt(t, path)
		if msg := kvmProblem(); msg != "" {
			t.Fatalf("an openable device must read as available, got %q", msg)
		}
	})
}

// TestMicroVMProbeRefusesForbiddenKVM drives the refusal through the public
// Probe, so the failure is the machine-readable UnavailableError a caller
// branches on, with the permission reason in it rather than a bare "not
// available". The supervisor is never contacted: the node gate fails first.
func TestMicroVMProbeRefusesForbiddenKVM(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the KVM device gate is a Linux question")
	}
	path := filepath.Join(t.TempDir(), "kvm-forbidden")
	if err := os.WriteFile(path, []byte("not a device"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if f, err := os.OpenFile(path, os.O_RDWR, 0); err == nil {
		_ = f.Close()
		t.Skip("this user can open the fixture, so refusal cannot be measured here")
	}
	pointKVMAt(t, path)

	b := NewMicroVMBackend("firecracker", filepath.Join(t.TempDir(), "no-supervisor.sock"))
	err := b.Probe(context.Background())
	if err == nil {
		t.Fatal("Probe must refuse when the KVM node cannot be opened")
	}
	if !IsUnavailable(err) {
		t.Fatalf("Probe error must be UnavailableError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "may not use it") {
		t.Fatalf("the refusal must name the permission problem, got %q", err.Error())
	}
}
