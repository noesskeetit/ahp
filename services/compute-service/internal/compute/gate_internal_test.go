package compute

import (
	"context"
	"os"
	"testing"
)

// unsetSupervisorEnv clears every environment variable the supervisor gate
// reads, restoring them on cleanup. With these cleared, RuntimeFromEnv must
// fail closed on every OS: non-Linux hosts refuse at the GOOS check, Linux
// hosts refuse at the missing-config check. It must never build a runtime.
func unsetSupervisorEnv(t *testing.T) {
	t.Helper()
	keys := []string{
		EnvRuntime, EnvFCBinary, EnvFCKernel, EnvFCRootFS, EnvFCDataDir,
		EnvFCVCPUs, EnvFCMemMB, EnvFCGuestAgent, EnvFCTap, EnvFCGuestMAC,
	}
	for _, k := range keys {
		if v, ok := os.LookupEnv(k); ok {
			t.Cleanup(func(k, v string) func() {
				return func() { os.Setenv(k, v) }
			}(k, v))
		}
		os.Unsetenv(k)
	}
}

// TestFailClosedRuntimeFromEnvWithoutConfig proves the supervisor gate refuses
// to fabricate a microVM runtime when its environment is not supplied. This is
// the testable core of the "no microVM is simulated" property and it holds on
// the macOS control host (GOOS check) and on a config-less Linux host.
func TestFailClosedRuntimeFromEnvWithoutConfig(t *testing.T) {
	unsetSupervisorEnv(t)
	rt, err := RuntimeFromEnv()
	if err == nil {
		t.Fatalf("RuntimeFromEnv must fail with no supervisor env, got runtime %v", rt)
	}
	if !IsUnavailable(err) {
		t.Fatalf("RuntimeFromEnv error must be UnavailableError, got %T: %v", err, err)
	}
	if rt != nil {
		t.Fatalf("no runtime must be returned on failure, got %v", rt)
	}
}

// TestRequireClassXWithoutMicroVMBackend proves that with only a host backend
// registered, class X fails closed instead of silently running on the host.
func TestRequireClassXWithoutMicroVMBackend(t *testing.T) {
	reg := NewRegistry(NewHostBackend(t.TempDir()))
	b, err := reg.Require(context.Background(), ClassX)
	if err == nil {
		t.Fatalf("Require(X) with no microVM backend must fail, got backend %q", b.Name())
	}
	if !IsUnavailable(err) {
		t.Fatalf("Require(X) error must be UnavailableError, got %T: %v", err, err)
	}
}

// TestRequireClassXRefusesHostBindingOverride is the regression guard for the
// privilege-escalation hole in which an operator (or a misconfiguration)
// rebinds class X to the host backend via SetClassBackend. Arbitrary untrusted
// code must NEVER be routable to the host backend, so Require must still fail
// closed on the override.
func TestRequireClassXRefusesHostBindingOverride(t *testing.T) {
	reg := NewRegistry(NewHostBackend(t.TempDir()))
	reg.SetClassBackend(ClassX, "host")
	b, err := reg.Require(context.Background(), ClassX)
	if err == nil {
		t.Fatalf("Require(X) must refuse a host backend even when rebound, got %q", b.Name())
	}
	if !IsUnavailable(err) {
		t.Fatalf("Require(X) override error must be UnavailableError, got %T: %v", err, err)
	}
}

// TestRequireTrustedClassesUseHost verifies the hardened guard does not
// over-reach: trusted classes S and C legitimately run on the host backend and
// must still resolve to it when no microVM is registered.
func TestRequireTrustedClassesUseHost(t *testing.T) {
	reg := NewRegistry(NewHostBackend(t.TempDir()))
	for _, c := range []Class{ClassS, ClassC} {
		b, err := reg.Require(context.Background(), c)
		if err != nil {
			t.Fatalf("Require(%s) on host backend must succeed, got %v", c, err)
		}
		if b.Name() != "host" {
			t.Fatalf("Require(%s) resolved backend %q, want host", c, b.Name())
		}
	}
}

// TestSuperviseFailClosedWithoutEnv shows the production supervisor entrypoint
// fails closed (Unavailable) before it ever binds a socket when the runtime
// cannot be built. Because RuntimeFromEnv returns first, no listener is created
// and no fake guest is started, so this is safe to assert on any OS.
func TestSuperviseFailClosedWithoutEnv(t *testing.T) {
	unsetSupervisorEnv(t)
	ctx := context.Background()
	socket := t.TempDir() + "/supervisor.sock"
	if err := Supervise(ctx, socket); !IsUnavailable(err) {
		t.Fatalf("Supervise with no runtime env must fail Unavailable, got %T: %v", err, err)
	}
	if _, err := os.Stat(socket); err == nil {
		t.Fatalf("supervisor must not bind a socket when the gate fails closed")
	}
}
