package routing

// Legs for the declared per-copy quota (PDF rev3.2 :1620-1630 "квоты провайдеров"
// belong to Model Gateway; :323 "квоты RPM/TPM, попытки"; :2495 quotas per tenant
// AND per class, tenant-scoped, not global).
//
// The point of declaring a copy's bound in the same committed file that names the
// copy is auditability: a limit that exists only in a database someone populated
// from a terminal is invisible to whoever reads the routing table. These legs keep
// the declaration honest in both directions -- a config that declares an unbounded
// "limit" must fail to load, because 0 in this plane means unlimited.

import (
	"strings"
	"testing"
)

const twoCopiesWithQuota = `{
  "version": 1,
  "providers": [
    {"name": "modelrun", "class": "standard", "audience": {"kind": "external"},
     "secret_ref": "ahp-hive/execution/qwen", "models": ["model-under-test"],
     "quota": {"rpm": 20, "tpm": 120000, "window_seconds": 60}},
    {"name": "modelrun-in-tenant", "class": "restricted", "audience": {"kind": "tenant"},
     "secret_ref": "ahp-hive/execution/qwen", "models": ["model-under-test"],
     "quota": {"rpm": 10, "tpm": 60000}}
  ],
  "routes": [{"name": "default", "when": {"tenants": ["*"]}, "then": {"provider": "modelrun"}}]
}`

func TestDeclaredQuotaIsReadablePerCopy(t *testing.T) {
	tbl, err := Parse([]byte(twoCopiesWithQuota))
	if err != nil {
		t.Fatalf("a valid config with per-copy quota failed to load: %v", err)
	}
	declared := tbl.DeclaredQuotas()
	if len(declared) != 2 {
		t.Fatalf("declared quota entries = %d, want 2 (one per copy): %+v", len(declared), declared)
	}
	byProvider := map[string]DeclaredQuota{}
	for _, q := range declared {
		byProvider[q.Provider] = q
	}
	external, ok := byProvider["modelrun"]
	if !ok || external.RPM != 20 || external.TPM != 120000 || external.WindowSeconds != 60 {
		t.Fatalf("modelrun declared quota = %+v, want rpm 20 / tpm 120000 / window 60", external)
	}
	// The second copy states no window: the quota plane's own default applies, and
	// the reader must not invent one that disagrees with the database.
	inTenant, ok := byProvider["modelrun-in-tenant"]
	if !ok || inTenant.RPM != 10 || inTenant.TPM != 60000 || inTenant.WindowSeconds != 60 {
		t.Fatalf("modelrun-in-tenant declared quota = %+v, want rpm 10 / tpm 60000 / window defaulted to 60", inTenant)
	}
}

// A committed config is a promise. "rpm: 0" in this plane means unbounded, so
// writing it into a file whose whole purpose is to state bounds has to be a load
// failure, not a number that quietly disables the gate for that copy.
func TestDeclaredQuotaMustBeFinite(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"zero rpm", `{"name": "p1", "audience": {"kind": "tenant"}, "secret_ref": "ahp-hive/execution/qwen", "models": ["m1"], "quota": {"rpm": 0, "tpm": 1000}}`},
		{"negative rpm", `{"name": "p1", "audience": {"kind": "tenant"}, "secret_ref": "ahp-hive/execution/qwen", "models": ["m1"], "quota": {"rpm": -5, "tpm": 1000}}`},
		{"zero tpm", `{"name": "p1", "audience": {"kind": "tenant"}, "secret_ref": "ahp-hive/execution/qwen", "models": ["m1"], "quota": {"rpm": 10, "tpm": 0}}`},
		{"window beyond the column check", `{"name": "p1", "audience": {"kind": "tenant"}, "secret_ref": "ahp-hive/execution/qwen", "models": ["m1"], "quota": {"rpm": 10, "tpm": 1000, "window_seconds": 90000}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := `{"version": 1, "providers": [` + tc.body + `], "routes": [{"name": "d", "when": {"tenants": ["*"]}, "then": {"provider": "p1"}}]}`
			_, err := Parse([]byte(raw))
			if err == nil {
				t.Fatalf("config with %s loaded: a declared limit that means unbounded must not load", tc.name)
			}
			if !strings.Contains(err.Error(), "p1") {
				t.Errorf("refusal does not name the provider at fault: %v", err)
			}
		})
	}
}

// Absence is not a pass: a copy without a quota block publishes nothing, and the
// audit side (scripts/quotactl) is what turns that into a non-zero exit. This leg
// pins the reader's side of that contract so sync cannot publish a number nobody
// declared.
func TestCopyWithoutQuotaDeclaresNothing(t *testing.T) {
	raw := `{"version": 1, "providers": [{"name": "p1", "audience": {"kind": "tenant"}, "secret_ref": "ahp-hive/execution/qwen", "models": ["m1"]}], "routes": [{"name": "d", "when": {"tenants": ["*"]}, "then": {"provider": "p1"}}]}`
	tbl, err := Parse([]byte(raw))
	if err != nil {
		t.Fatalf("a provider with no quota block must still load (declaring a bound is policy, not schema): %v", err)
	}
	if got := tbl.DeclaredQuotas(); len(got) != 0 {
		t.Fatalf("DeclaredQuotas = %+v, want none: nothing may be published for a copy that declared nothing", got)
	}
}
