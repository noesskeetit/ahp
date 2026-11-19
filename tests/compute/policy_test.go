package compute_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestPolicyClassDefaultsExact(t *testing.T) {
	cases := []struct {
		class compute.Class
		want  compute.Quota
	}{
		{compute.ClassS, compute.Quota{MaxCPUMillisPerSec: 8000, MaxMemoryMB: 16384, MaxPIDs: 65536, MaxPTYs: 64, MaxOpenFiles: 1048576, MaxStorageMB: 204800, MaxSessionTTL: 24 * time.Hour}},
		{compute.ClassC, compute.Quota{MaxCPUMillisPerSec: 4000, MaxMemoryMB: 8192, MaxPIDs: 32768, MaxPTYs: 64, MaxOpenFiles: 262144, MaxStorageMB: 51200, MaxSessionTTL: 4 * time.Hour}},
		{compute.ClassX, compute.Quota{MaxCPUMillisPerSec: 2000, MaxMemoryMB: 4096, MaxPIDs: 8192, MaxPTYs: 64, MaxOpenFiles: 65536, MaxStorageMB: 10240, MaxSessionTTL: time.Hour}},
	}
	for _, tc := range cases {
		if got := compute.ClassDefaults(tc.class); got != tc.want {
			t.Errorf("ClassDefaults(%s) = %+v, want %+v", tc.class, got, tc.want)
		}
	}
	// PTY budget is per-workspace and the same across classes (SBX-03).
	for _, c := range []compute.Class{compute.ClassS, compute.ClassC, compute.ClassX} {
		if q := compute.ClassDefaults(c); q.MaxPTYs != compute.MaxPTYsPerWorkspace {
			t.Errorf("ClassDefaults(%s).MaxPTYs = %d, want %d", c, q.MaxPTYs, compute.MaxPTYsPerWorkspace)
		}
	}
}

func TestPolicyResolveQuotaOnlyTightens(t *testing.T) {
	def := compute.ClassDefaults(compute.ClassC)
	// A zero request resolves to the class default.
	if got := compute.ResolveQuota(compute.ClassC, compute.Quota{}); got != def {
		t.Errorf("ResolveQuota(C, zero) = %+v, want default %+v", got, def)
	}
	// A smaller request narrows; a larger request must NOT widen.
	narrow := compute.Quota{MaxCPUMillisPerSec: 1000, MaxMemoryMB: 1024}
	got := compute.ResolveQuota(compute.ClassC, narrow)
	if got.MaxCPUMillisPerSec != 1000 || got.MaxMemoryMB != 1024 {
		t.Errorf("narrow not applied: %+v", got)
	}
	if got.MaxPIDs != def.MaxPIDs {
		t.Errorf("unspecified PID limit should stay default: %+v", got)
	}
	wide := compute.Quota{MaxCPUMillisPerSec: def.MaxCPUMillisPerSec * 10, MaxMemoryMB: def.MaxMemoryMB * 10}
	got = compute.ResolveQuota(compute.ClassC, wide)
	if got.MaxCPUMillisPerSec != def.MaxCPUMillisPerSec || got.MaxMemoryMB != def.MaxMemoryMB {
		t.Errorf("request must not widen past class default: %+v", got)
	}
	// TTL longer than default is clamped to default.
	got = compute.ResolveQuota(compute.ClassC, compute.Quota{MaxSessionTTL: 100 * time.Hour})
	if got.MaxSessionTTL != def.MaxSessionTTL {
		t.Errorf("TTL not clamped to default: %v", got.MaxSessionTTL)
	}
}

func TestPolicyResolveQuotaXHardCeilings(t *testing.T) {
	// Even an absurdly wide request against X is clamped to X ceilings.
	wide := compute.Quota{MaxCPUMillisPerSec: 1_000_000, MaxMemoryMB: 1_000_000, MaxPIDs: 1_000_000, MaxOpenFiles: 1_000_000, MaxStorageMB: 1_000_000, MaxSessionTTL: 72 * time.Hour}
	got := compute.ResolveQuota(compute.ClassX, wide)
	want := compute.ClassDefaults(compute.ClassX)
	if got != want {
		t.Errorf("X clamped quota = %+v, want ceilings %+v", got, want)
	}
}

func TestPolicyEgress(t *testing.T) {
	x := compute.EgressPolicy(compute.ClassX)
	if len(x.AllowedHosts) != 0 || len(x.PrivateHosts) != 0 {
		t.Errorf("class X must be deny-all, got %+v", x)
	}
	c := compute.EgressPolicy(compute.ClassC)
	if len(c.AllowedHosts) == 0 {
		t.Errorf("class C should carry an allow-list, got %+v", c)
	}
	if c.PrivateHosts != nil {
		t.Errorf("class C must not expose internal PrivateHosts, got %v", c.PrivateHosts)
	}
	s := compute.EgressPolicy(compute.ClassS)
	// S declares an (empty-but-initialised) PrivateHosts capability, distinct from C.
	if s.PrivateHosts == nil {
		t.Errorf("class S PrivateHosts should be an initialised slice, got nil")
	}
	if !reflect.DeepEqual(s.AllowedHosts, c.AllowedHosts) {
		t.Errorf("S and C share the public allow-list; got S=%v C=%v", s.AllowedHosts, c.AllowedHosts)
	}
}

func TestPolicyJailRoots(t *testing.T) {
	base := "/mnt/ahp"
	if got := compute.JailRoots(compute.ProfilePersonal, base, "ada", "ws1"); !reflect.DeepEqual(got, []string{"/mnt/ahp/home/ada"}) {
		t.Errorf("personal jail = %v", got)
	}
	if got := compute.JailRoots(compute.ProfileProject, base, "ada", "ws1"); !reflect.DeepEqual(got, []string{"/mnt/ahp/workspace/ws1"}) {
		t.Errorf("project jail = %v", got)
	}
	if got := compute.JailRoots(compute.ProfileEphemeral, base, "ada", "ws1"); !reflect.DeepEqual(got, []string{"/mnt/ahp/scratch"}) {
		t.Errorf("ephemeral jail = %v", got)
	}
}

func TestPolicyComputerAccess(t *testing.T) {
	// personal is only ever class S.
	if err := compute.CheckComputerAccess(compute.ClassS, compute.ProfilePersonal, ""); err != nil {
		t.Errorf("personal on S must be allowed, got %v", err)
	}
	for _, c := range []compute.Class{compute.ClassC, compute.ClassX} {
		err := compute.CheckComputerAccess(c, compute.ProfilePersonal, "")
		if err == nil || contracts.Code(err) != contracts.ErrForbidden {
			t.Errorf("personal on %s must be FORBIDDEN, got %v", c, err)
		}
	}
	// X may never bind a realm.
	if err := compute.CheckComputerAccess(compute.ClassX, compute.ProfileEphemeral, "realm-1"); err == nil || contracts.Code(err) != contracts.ErrForbidden {
		t.Errorf("X with realm must be FORBIDDEN, got %v", err)
	}
	// S/C with a realm and non-personal profile is fine.
	if err := compute.CheckComputerAccess(compute.ClassS, compute.ProfileProject, "realm-1"); err != nil {
		t.Errorf("S project+realm must be allowed, got %v", err)
	}
}

func TestPolicyPTYAndProcessBudget(t *testing.T) {
	q := compute.ClassDefaults(compute.ClassX)
	if err := compute.CheckPTYBudget(q, int(q.MaxPTYs)-1); err != nil {
		t.Errorf("opening PTY at limit-1 must succeed, got %v", err)
	}
	if err := compute.CheckPTYBudget(q, int(q.MaxPTYs)); err == nil || contracts.Code(err) != contracts.ErrConflict {
		t.Errorf("opening the %dth+1 PTY must CONFLICT, got %v", q.MaxPTYs, err)
	}
	if err := compute.CheckProcessBudget(q); err != nil {
		t.Errorf("X has a PID budget, process spawn must succeed, got %v", err)
	}
	if err := compute.CheckProcessBudget(compute.Quota{MaxPIDs: 0}); err == nil || contracts.Code(err) != contracts.ErrForbidden {
		t.Errorf("zero PID budget must FORBID spawning, got %v", err)
	}
}

func TestPolicyPersonalRequiresTrustedClass(t *testing.T) {
	if !compute.PersonalRequiresTrustedClass(compute.ProfilePersonal, compute.ClassS) {
		t.Error("personal/S must satisfy the trusted-class requirement")
	}
	if compute.PersonalRequiresTrustedClass(compute.ProfilePersonal, compute.ClassC) {
		t.Error("personal/C must violate the trusted-class requirement")
	}
	// Non-personal profiles are unconstrained by class.
	if !compute.PersonalRequiresTrustedClass(compute.ProfileEphemeral, compute.ClassX) {
		t.Error("ephemeral/X should be trivially satisfied")
	}
}
