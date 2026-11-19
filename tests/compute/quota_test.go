package compute_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestQuotaClassDefaults pins the three security-class envelopes from the PDF
// (S trusted multi-tenant, C developer-code, X arbitrary code). A zero request
// must always land on a concrete, defensible default — never zero.
func TestQuotaClassDefaults(t *testing.T) {
	s := compute.ClassDefaults(compute.ClassS)
	if s.MaxPIDs != 65536 || s.MaxMemoryMB != 16384 {
		t.Fatalf("S defaults: pids=%d mem=%d, want 65536/16384", s.MaxPIDs, s.MaxMemoryMB)
	}
	if s.MaxSessionTTL != 24*time.Hour {
		t.Fatalf("S ttl = %v, want 24h", s.MaxSessionTTL)
	}
	c := compute.ClassDefaults(compute.ClassC)
	if c.MaxPIDs != 32768 || c.MaxMemoryMB != 8192 {
		t.Fatalf("C defaults: pids=%d mem=%d, want 32768/8192", c.MaxPIDs, c.MaxMemoryMB)
	}
	x := compute.ClassDefaults(compute.ClassX)
	if x.MaxPIDs != 8192 || x.MaxMemoryMB != 4096 {
		t.Fatalf("X defaults: pids=%d mem=%d, want 8192/4096", x.MaxPIDs, x.MaxMemoryMB)
	}
	if x.MaxSessionTTL != time.Hour {
		t.Fatalf("X ttl = %v, want 1h", x.MaxSessionTTL)
	}
	// Every class gets its own strict PTY budget (the shared-LRU footgun guard).
	for _, cl := range []compute.Class{compute.ClassS, compute.ClassC, compute.ClassX} {
		if got := compute.ClassDefaults(cl).MaxPTYs; got != compute.MaxPTYsPerWorkspace {
			t.Fatalf("%s MaxPTYs = %d, want %d", cl, got, compute.MaxPTYsPerWorkspace)
		}
	}
}

// TestResolveQuotaXCannotWiden is the security core of the quota policy: a
// caller in the untrusted class X may only ever TIGHTEN its envelope. Any
// request wider than the hard ceiling is clamped back down, never honored.
func TestResolveQuotaXCannotWiden(t *testing.T) {
	got := compute.ResolveQuota(compute.ClassX, compute.Quota{
		MaxPIDs:            999999,
		MaxMemoryMB:        999999,
		MaxCPUMillisPerSec: 999999,
		MaxOpenFiles:       99999999,
		MaxStorageMB:       99999999,
		MaxSessionTTL:      72 * time.Hour,
	})
	if got.MaxPIDs != 8192 {
		t.Fatalf("X widened MaxPIDs to %d, want clamped 8192", got.MaxPIDs)
	}
	if got.MaxMemoryMB != 4096 {
		t.Fatalf("X widened MaxMemoryMB to %d, want clamped 4096", got.MaxMemoryMB)
	}
	if got.MaxCPUMillisPerSec != 2000 {
		t.Fatalf("X widened CPU to %d, want clamped 2000", got.MaxCPUMillisPerSec)
	}
	if got.MaxSessionTTL > time.Hour {
		t.Fatalf("X widened TTL to %v, want <= 1h", got.MaxSessionTTL)
	}
}

// TestResolveQuotaNarrowingHonored proves a legitimate tighten request survives
// resolution unchanged, while a zero request falls back to the class default.
func TestResolveQuotaNarrowingHonored(t *testing.T) {
	got := compute.ResolveQuota(compute.ClassS, compute.Quota{MaxPIDs: 100})
	if got.MaxPIDs != 100 {
		t.Fatalf("S narrowing not honored: MaxPIDs = %d, want 100", got.MaxPIDs)
	}
	// Untouched fields still take the default envelope.
	if got.MaxMemoryMB != 16384 {
		t.Fatalf("S untouched MaxMemoryMB = %d, want default 16384", got.MaxMemoryMB)
	}
	// X narrowing is honored too (still within ceiling).
	narrow := compute.ResolveQuota(compute.ClassX, compute.Quota{MaxPIDs: 8})
	if narrow.MaxPIDs != 8 {
		t.Fatalf("X narrowing not honored: MaxPIDs = %d, want 8", narrow.MaxPIDs)
	}
}

// TestComputerPersistsResolvedQuota locks the store behavior: CreateComputer
// persists the RESOLVED envelope (not the raw request), so the process budget
// enforced at command dispatch is real. Includes the X-widen clamp round-trip.
func TestComputerPersistsResolvedQuota(t *testing.T) {
	st := newStore(t)
	sc := scope()

	sComputer := mustCreate(t, st, sc, compute.ClassS, compute.ProfileEphemeral, "")
	if sComputer.Quota.MaxPIDs != 65536 || sComputer.Quota.MaxMemoryMB != 16384 {
		t.Fatalf("persisted S quota: pids=%d mem=%d, want 65536/16384",
			sComputer.Quota.MaxPIDs, sComputer.Quota.MaxMemoryMB)
	}

	cComputer := mustCreate(t, st, sc, compute.ClassC, compute.ProfileEphemeral, "")
	if cComputer.Quota.MaxPIDs != 32768 {
		t.Fatalf("persisted C MaxPIDs = %d, want 32768", cComputer.Quota.MaxPIDs)
	}

	// X created with no quota gets the tight envelope.
	xComputer, err := st.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassX, Profile: compute.ProfileEphemeral,
	}, "key-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer(X): %v", err)
	}
	if xComputer.Quota.MaxPIDs != 8192 || xComputer.Quota.MaxMemoryMB != 4096 {
		t.Fatalf("persisted X quota: pids=%d mem=%d, want 8192/4096",
			xComputer.Quota.MaxPIDs, xComputer.Quota.MaxMemoryMB)
	}

	// X trying to widen must be persisted clamped — the raw request never lands.
	xWiden, err := st.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class:   compute.ClassX,
		Profile: compute.ProfileEphemeral,
		Quota:   compute.Quota{MaxPIDs: 999999, MaxMemoryMB: 999999},
	}, "key-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateComputer(X widen): %v", err)
	}
	if xWiden.Quota.MaxPIDs != 8192 || xWiden.Quota.MaxMemoryMB != 4096 {
		t.Fatalf("X-widen persisted un-clamped: pids=%d mem=%d, want 8192/4096",
			xWiden.Quota.MaxPIDs, xWiden.Quota.MaxMemoryMB)
	}

	// And the clamp must survive a read-back from storage.
	reread, err := st.GetComputer(ctx(t), sc, xWiden.ID)
	if err != nil {
		t.Fatalf("GetComputer: %v", err)
	}
	if reread.Quota.MaxPIDs != 8192 {
		t.Fatalf("re-read X MaxPIDs = %d, want clamped 8192", reread.Quota.MaxPIDs)
	}
}

// TestPersonalComputerRequiresTrustedS is the contract gate that a personal
// Computer (the employee's persistent machine) may only exist in class S.
func TestPersonalComputerRequiresTrustedS(t *testing.T) {
	if !compute.PersonalRequiresTrustedClass(compute.ProfilePersonal, compute.ClassS) {
		t.Fatal("personal+ S must be allowed")
	}
	if compute.PersonalRequiresTrustedClass(compute.ProfilePersonal, compute.ClassC) {
		t.Fatal("personal+ C must be forbidden")
	}
	if compute.PersonalRequiresTrustedClass(compute.ProfilePersonal, compute.ClassX) {
		t.Fatal("personal+ X must be forbidden")
	}
	// Non-personal profiles are unrestricted by this rule.
	for _, cl := range []compute.Class{compute.ClassS, compute.ClassC, compute.ClassX} {
		if !compute.PersonalRequiresTrustedClass(compute.ProfileEphemeral, cl) {
			t.Fatalf("ephemeral+ %s must be allowed", cl)
		}
	}

	// At the store level a personal C/X computer is refused outright.
	st := newStore(t)
	if _, err := st.CreateComputer(ctx(t), scope(), compute.NewComputerRequest{
		Class: compute.ClassX, Profile: compute.ProfilePersonal, Realm: "realm-1",
	}, "key-"+contracts.NewID()); err == nil {
		t.Fatal("personal X should be rejected")
	}
}
