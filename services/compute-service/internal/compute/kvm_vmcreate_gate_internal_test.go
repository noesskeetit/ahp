package compute

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// A host can open /dev/kvm read-write and still be unable to boot a guest.
// VM1 is exactly that machine: it is itself a KVM guest, the node answers
// KVM_CHECK_EXTENSION (KVM_CAP_MAX_VCPUS = 1024), and KVM_CREATE_VM returns
// EINVAL. A gate that stops at openability calls such a host ready, the class-X
// claim then travels upstream as "isolated", and the guest dies later inside
// the component that was trusted to name the blocker - the failure mode
// TestKVMGateSeparatesForbiddenFromAbsent was written to prevent, one layer up.
func TestKVMGateRejectsHostThatCannotCreateAVM(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the KVM VM-creation gate is a Linux question")
	}
	path := filepath.Join(t.TempDir(), "kvm-openable-but-not-a-device")
	if err := os.WriteFile(path, []byte("stand-in"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	pointKVMAt(t, path)

	// The existing contract: opening is not existence, so this half is silent.
	if msg := kvmProblem(); msg != "" {
		t.Fatalf("this fixture is openable, the permission gate must stay quiet, got %q", msg)
	}
	// The new contract: openable is not bootable.
	msg := kvmVMCreateProblem()
	if msg == "" {
		t.Fatal("a node that cannot create a VM must not read as available")
	}
	if !strings.Contains(msg, "create a VM") {
		t.Fatalf("the refusal must name VM creation as the failing step, got %q", msg)
	}
	if strings.Contains(msg, "does not exist") || strings.Contains(msg, "may not use it") {
		t.Fatalf("the node exists and opens; neither may be claimed: %q", msg)
	}
	if combined := kvmHostProblem(); combined == "" {
		t.Fatal("the combined gate the callers use must refuse this host")
	} else if !strings.Contains(combined, "create a VM") {
		t.Fatalf("the combined gate must carry the VM-creation reason, got %q", combined)
	}
}

// TestMicroVMProbeRefusesHostThatCannotCreateAVM drives the same refusal through
// the public Probe, so a caller branching on IsUnavailable never sees a
// "reachable supervisor" answer for a host that cannot boot anything.
func TestMicroVMProbeRefusesHostThatCannotCreateAVM(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the KVM VM-creation gate is a Linux question")
	}
	path := filepath.Join(t.TempDir(), "kvm-openable-but-not-a-device")
	if err := os.WriteFile(path, []byte("stand-in"), 0o600); err != nil {
		t.Fatalf("seed: %v", err)
	}
	pointKVMAt(t, path)

	b := NewMicroVMBackend("firecracker", filepath.Join(t.TempDir(), "no-supervisor.sock"))
	err := b.Probe(context.Background())
	if err == nil {
		t.Fatal("Probe must refuse a host that cannot create a VM")
	}
	if !IsUnavailable(err) {
		t.Fatalf("Probe error must be UnavailableError, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "create a VM") {
		t.Fatalf("the refusal must name VM creation, got %q", err.Error())
	}
	if strings.Contains(err.Error(), "supervisor not reachable") {
		t.Fatalf("the node gate must fail before the supervisor is contacted: %q", err.Error())
	}
}

// TestKVMHostProblemOnThisMachine is a leg, not an assertion: it records what
// this host actually answers, so a run log states whether the gate closed a
// real device or only a fixture. Nothing here may pass by asserting the
// environment; it only reports.
func TestKVMHostProblemOnThisMachine(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("no KVM question on this OS")
	}
	t.Logf("kvmProblem(this host) = %q", kvmProblem())
	t.Logf("kvmVMCreateProblem(this host) = %q", kvmVMCreateProblem())
	t.Logf("kvmHostProblem(this host) = %q", kvmHostProblem())
}
