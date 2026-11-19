package compute_test

import (
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestStoreCreateComputerLifecycleProfiles(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()

	eph := mustCreate(t, st, sc, compute.ClassC, compute.ProfileEphemeral, "")
	if eph.Status != compute.ComputerProvisioning {
		t.Errorf("new computer status = %q, want provisioning", eph.Status)
	}
	if eph.VolumeID == "" {
		t.Error("computer must be allocated a volume id at creation")
	}

	// project requires a workspace id.
	if _, err := st.CreateComputer(c, sc, compute.NewComputerRequest{Class: compute.ClassC, Profile: compute.ProfileProject}, "key-"+contracts.NewID()); err == nil || contracts.Code(err) != contracts.ErrInvalid {
		t.Errorf("project without workspace id must be INVALID, got %v", err)
	}
	proj := mustCreate(t, st, sc, compute.ClassC, compute.ProfileProject, "ws-"+contracts.NewID())
	if proj.WorkspaceID == "" {
		t.Error("project computer must carry its workspace id")
	}

	// personal is reserved for the trusted class S.
	if _, err := st.CreateComputer(c, sc, compute.NewComputerRequest{Class: compute.ClassC, Profile: compute.ProfilePersonal}, "key-"+contracts.NewID()); err == nil || contracts.Code(err) != contracts.ErrForbidden {
		t.Errorf("personal on class C must be FORBIDDEN, got %v", err)
	}
	if _, err := st.CreateComputer(c, sc, compute.NewComputerRequest{Class: compute.ClassX, Profile: compute.ProfilePersonal}, "key-"+contracts.NewID()); err == nil || contracts.Code(err) != contracts.ErrForbidden {
		t.Errorf("personal on class X must be FORBIDDEN, got %v", err)
	}
	// A personal Computer for class S is allowed (separate subject so it does
	// not collide with other personal computers in this scope).
	personalSc := sc
	personalSc.Subject = "user-bob"
	if _, err := st.CreateComputer(c, personalSc, compute.NewComputerRequest{Class: compute.ClassS, Profile: compute.ProfilePersonal}, "key-"+contracts.NewID()); err != nil {
		t.Fatalf("personal on class S must be allowed, got %v", err)
	}
}

func TestStoreIdempotencyAndUniqueness(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	key := "idem-" + contracts.NewID()

	req := compute.NewComputerRequest{Class: compute.ClassC, Profile: compute.ProfileEphemeral}
	a, err := st.CreateComputer(c, sc, req, key)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	// Same key, same body: idempotent replay returns the same computer.
	b, err := st.CreateComputer(c, sc, req, key)
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	if a.ID != b.ID {
		t.Errorf("idempotent replay returned a new id: %s vs %s", a.ID, b.ID)
	}
	// Same key, different body: KEY_REUSE.
	if _, err := st.CreateComputer(c, sc, compute.NewComputerRequest{Class: compute.ClassX, Profile: compute.ProfileEphemeral}, key); err == nil || contracts.Code(err) != contracts.ErrKeyReuse {
		t.Errorf("same key with different body must be KEY_REUSE, got %v", err)
	}

	// Only one active personal Computer per (tenant, subject, realm).
	p := sc
	p.Subject = "user-carol"
	if _, err := st.CreateComputer(c, p, compute.NewComputerRequest{Class: compute.ClassS, Profile: compute.ProfilePersonal}, "key-"+contracts.NewID()); err != nil {
		t.Fatalf("first personal: %v", err)
	}
	if _, err := st.CreateComputer(c, p, compute.NewComputerRequest{Class: compute.ClassS, Profile: compute.ProfilePersonal}, "key-"+contracts.NewID()); err == nil || contracts.Code(err) != contracts.ErrConflict {
		t.Errorf("second active personal must be CONFLICT, got %v", err)
	}

	// Only one active project Computer per workspace.
	ws := "ws-" + contracts.NewID()
	if _, err := st.CreateComputer(c, sc, compute.NewComputerRequest{Class: compute.ClassC, Profile: compute.ProfileProject, WorkspaceID: ws}, "key-"+contracts.NewID()); err != nil {
		t.Fatalf("first project: %v", err)
	}
	if _, err := st.CreateComputer(c, sc, compute.NewComputerRequest{Class: compute.ClassC, Profile: compute.ProfileProject, WorkspaceID: ws}, "key-"+contracts.NewID()); err == nil || contracts.Code(err) != contracts.ErrConflict {
		t.Errorf("second active project in same workspace must be CONFLICT, got %v", err)
	}
}

func TestStoreCrossScopeIsolation(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	owner := scope()
	other := scope()
	computer := mustCreate(t, st, owner, compute.ClassC, compute.ProfileEphemeral, "")

	// The owner can read it.
	if _, err := st.GetComputer(c, owner, computer.ID); err != nil {
		t.Fatalf("owner GetComputer: %v", err)
	}
	// Another tenant cannot (NOT_FOUND, not FORBIDDEN, to avoid leaking existence).
	if _, err := st.GetComputer(c, other, computer.ID); err == nil || contracts.Code(err) != contracts.ErrNotFound {
		t.Errorf("cross-scope GetComputer must be NOT_FOUND, got %v", err)
	}
}

func TestStoreIncarnationFencing(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	computer := mustCreate(t, st, sc, compute.ClassS, compute.ProfileEphemeral, "")

	inc1, err := st.AcquireIncarnation(c, sc, computer.ID, "host", "node-a", "gen-1")
	if err != nil {
		t.Fatalf("acquire 1: %v", err)
	}
	if inc1.Status != compute.IncarnationRunning || inc1.Fence != 1 {
		t.Errorf("first incarnation = status %q fence %d, want running/1", inc1.Status, inc1.Fence)
	}
	if err := st.CheckFence(c, sc, computer.ID, inc1.ID); err != nil {
		t.Fatalf("live incarnation must hold the fence: %v", err)
	}

	// A second acquire raises the fence and steals it from the first.
	inc2, err := st.AcquireIncarnation(c, sc, computer.ID, "host", "node-b", "gen-2")
	if err != nil {
		t.Fatalf("acquire 2: %v", err)
	}
	if inc2.Fence != 2 {
		t.Errorf("second incarnation fence = %d, want 2", inc2.Fence)
	}
	if err := st.CheckFence(c, sc, computer.ID, inc1.ID); err == nil || contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Errorf("stale incarnation must be STALE_EPOCH, got %v", err)
	}
	if err := st.CheckFence(c, sc, computer.ID, inc2.ID); err != nil {
		t.Errorf("current incarnation must hold the fence, got %v", err)
	}

	if err := st.StopIncarnation(c, sc, computer.ID, inc2.ID); err != nil {
		t.Fatalf("stop: %v", err)
	}
	// Destroy is terminal.
	if _, err := st.SetComputerStatus(c, sc, computer.ID, compute.ComputerDestroyed); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if _, err := st.SetComputerStatus(c, sc, computer.ID, compute.ComputerActive); err == nil || contracts.Code(err) != contracts.ErrConflict {
		t.Errorf("reviving a destroyed computer must be CONFLICT, got %v", err)
	}
}

func TestStoreActivityLeases(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc := scope()
	computer := mustCreate(t, st, sc, compute.ClassC, compute.ProfileEphemeral, "")

	n, err := st.ActiveLeases(c, sc, computer.ID)
	if err != nil || n != 0 {
		t.Fatalf("fresh computer active leases = %d, %v (want 0)", n, err)
	}

	// Two non-exclusive command leases coexist.
	if _, err := st.AcquireLease(c, sc, computer.ID, compute.LeaseCommand, "h1", time.Minute); err != nil {
		t.Fatalf("lease cmd h1: %v", err)
	}
	if _, err := st.AcquireLease(c, sc, computer.ID, compute.LeaseCommand, "h2", time.Minute); err != nil {
		t.Fatalf("lease cmd h2: %v", err)
	}
	if n, _ := st.ActiveLeases(c, sc, computer.ID); n != 2 {
		t.Errorf("active leases = %d, want 2", n)
	}

	// A writer lease is exclusive against a different holder.
	if _, err := st.AcquireLease(c, sc, computer.ID, compute.LeaseWriter, "w1", time.Minute); err != nil {
		t.Fatalf("writer w1: %v", err)
	}
	if _, err := st.AcquireLease(c, sc, computer.ID, compute.LeaseWriter, "w2", time.Minute); err == nil || contracts.Code(err) != contracts.ErrConflict {
		t.Errorf("second concurrent writer must CONFLICT, got %v", err)
	}
	// Re-acquiring the same writer lease (same holder) just extends it.
	if _, err := st.AcquireLease(c, sc, computer.ID, compute.LeaseWriter, "w1", time.Hour); err != nil {
		t.Errorf("same-holder writer re-acquire must extend, got %v", err)
	}

	// Release makes the computer eligible to sleep again.
	for _, k := range []compute.ActivityLeaseKind{compute.LeaseCommand, compute.LeaseCommand} {
		_ = k
	}
	if err := st.ReleaseLease(c, sc, computer.ID, compute.LeaseCommand, "h1"); err != nil {
		t.Fatalf("release h1: %v", err)
	}
	if err := st.ReleaseLease(c, sc, computer.ID, compute.LeaseCommand, "h2"); err != nil {
		t.Fatalf("release h2: %v", err)
	}
	if err := st.ReleaseLease(c, sc, computer.ID, compute.LeaseWriter, "w1"); err != nil {
		t.Fatalf("release writer: %v", err)
	}
	if n, _ := st.ActiveLeases(c, sc, computer.ID); n != 0 {
		t.Errorf("after releasing all leases active = %d, want 0", n)
	}

	// Out-of-range TTL is rejected.
	if _, err := st.AcquireLease(c, sc, computer.ID, compute.LeaseCommand, "h9", time.Millisecond); err == nil || contracts.Code(err) != contracts.ErrInvalid {
		t.Errorf("sub-second lease TTL must be INVALID, got %v", err)
	}
}
