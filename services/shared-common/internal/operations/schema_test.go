package operations

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestValidateSchema(t *testing.T) {
	valid := []string{
		"ahp_tools",
		"ahp_a",
		"ahp_core_effects",
		"ahp_" + strings.Repeat("a", 41), // 4 + 41 = 45 chars, at the bound
	}
	for _, s := range valid {
		if err := validateSchema(s); err != nil {
			t.Errorf("validateSchema(%q) = %v, want nil", s, err)
		}
	}
	invalid := []string{
		"",
		"tools",
		"ahp_",
		"ahp_Tools",
		"ahp_1tools",
		"ahp-tools",
		"ahp_tools; DROP TABLE users",
		`ahp_tools"`,
		"ahp_tools space",
		"public",
		"ahp_tools" + strings.Repeat("a", 41), // 46 chars, over the bound
		"AHP_tools",
		" ahp_tools",
		"ahp_tools\n",
	}
	for _, s := range invalid {
		if err := validateSchema(s); err == nil {
			t.Errorf("validateSchema(%q) = nil, want error", s)
		}
	}
}

func TestDDLContainsQuotedSchemaAndIsIdempotentShape(t *testing.T) {
	ddl := ddl("ahp_tools")
	for _, want := range []string{
		`CREATE SCHEMA IF NOT EXISTS "ahp_tools"`,
		`CREATE TABLE IF NOT EXISTS "ahp_tools".operations`,
		`CREATE UNIQUE INDEX IF NOT EXISTS operations_active_guard_uidx`,
		`PRIMARY KEY (tenant, deployment, key)`,
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("DDL missing %q", want)
		}
	}
	for _, forbidden := range []string{"DROP ", "TRUNCATE ", "ALTER "} {
		if strings.Contains(strings.ToUpper(ddl), forbidden) {
			t.Errorf("DDL must never contain %q", forbidden)
		}
	}
}

func TestValidateRequestBounds(t *testing.T) {
	base := Request{
		Key:       "k1",
		Kind:      "tool.call",
		Target:    "tool:echo",
		Arguments: json.RawMessage(`{"a":1}`),
	}
	if err := validateRequest(base); err != nil {
		t.Fatalf("baseline request rejected: %v", err)
	}

	cases := []struct {
		name  string
		mut   func(*Request)
		valid bool
	}{
		{"empty key", func(r *Request) { r.Key = "" }, false},
		{"long key", func(r *Request) { r.Key = strings.Repeat("k", maxKeyLen+1) }, false},
		{"empty kind", func(r *Request) { r.Kind = "" }, false},
		{"long kind", func(r *Request) { r.Kind = strings.Repeat("k", maxKindLen+1) }, false},
		{"empty target", func(r *Request) { r.Target = "" }, false},
		{"long target", func(r *Request) { r.Target = strings.Repeat("t", maxTargetLen+1) }, false},
		{"long guard", func(r *Request) { r.Guard = strings.Repeat("g", maxGuardLen+1) }, false},
		{"invalid json", func(r *Request) { r.Arguments = json.RawMessage(`{`) }, false},
		// Duplicate keys are syntactically valid JSON but are rejected by
		// the canonicaliser, so Accept refuses them up front.
		{"duplicate keys", func(r *Request) { r.Arguments = json.RawMessage(`{"a":1,"a":2}`) }, false},
		{"oversized args", func(r *Request) { r.Arguments = json.RawMessage(`"` + strings.Repeat("x", maxArgumentsLen) + `"`) }, false},
		{"no arguments ok", func(r *Request) { r.Arguments = nil }, true},
	}
	for _, tc := range cases {
		r := base
		tc.mut(&r)
		err := validateRequest(r)
		if tc.valid && err != nil {
			t.Errorf("%s: unexpected error %v", tc.name, err)
		}
		if !tc.valid && err == nil {
			t.Errorf("%s: expected error", tc.name)
		}
	}
}

func TestFingerprintIsCanonical(t *testing.T) {
	// Key order and whitespace inside the arguments must not change the
	// fingerprint: it is taken over the canonical JSON form.
	a := Request{Key: "k", Kind: "tool.call", Target: "t", Arguments: json.RawMessage(`{"b":2,"a":1}`)}
	b := Request{Key: "k", Kind: "tool.call", Target: "t", Arguments: json.RawMessage("{\n  \"a\" : 1,  \"b\": 2 }")}
	da, err := fingerprint(a)
	if err != nil {
		t.Fatal(err)
	}
	db, err := fingerprint(b)
	if err != nil {
		t.Fatal(err)
	}
	if da != db {
		t.Fatalf("canonical arguments produced different fingerprints: %s vs %s", da, db)
	}

	c := a
	c.Guard = "acct:1"
	if dc, err := fingerprint(c); err != nil || dc == da {
		t.Fatalf("guard must participate in the fingerprint (got %s, err %v)", dc, err)
	}
	e := a
	e.Arguments = json.RawMessage(`{"a":1,"b":3}`)
	if de, err := fingerprint(e); err != nil || de == da {
		t.Fatalf("argument values must participate in the fingerprint (got %s, err %v)", de, err)
	}
}

func TestFingerprintKeepsExactNumbers(t *testing.T) {
	// Regression: arguments must be digested as raw canonical JSON. Going
	// through float64 would collapse these two distinct integers.
	base := Request{Key: "k", Kind: "tool.call", Target: "t"}
	a := base
	a.Arguments = json.RawMessage(`{"amount":9007199254740992}`)
	b := base
	b.Arguments = json.RawMessage(`{"amount":9007199254740993}`)
	da, err := fingerprint(a)
	if err != nil {
		t.Fatal(err)
	}
	db, err := fingerprint(b)
	if err != nil {
		t.Fatal(err)
	}
	if da == db {
		t.Fatal("adjacent int64 magnitudes must not share a fingerprint")
	}
	// Equivalent spellings of the same value keep the same fingerprint:
	// the canonicaliser rewrites 1.50e2 to 150, so both forms digest
	// identically.
	c := base
	c.Arguments = json.RawMessage(`{"amount":1.50e2}`)
	d1, err := fingerprint(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Arguments = json.RawMessage(`{"amount":150}`)
	d2, err := fingerprint(c)
	if err != nil {
		t.Fatal(err)
	}
	if d1 != d2 {
		t.Fatalf("1.50e2 and 150 must fingerprint identically: %s vs %s", d1, d2)
	}
}

func TestFingerprintRejectsDuplicateKeys(t *testing.T) {
	// Regression: duplicate object keys must fail fingerprinting (and thus
	// Accept) instead of silently digesting the last value.
	r := Request{Key: "k", Kind: "tool.call", Target: "t", Arguments: json.RawMessage(`{"a":1,"a":2}`)}
	if _, err := fingerprint(r); err == nil {
		t.Fatal("duplicate JSON keys must be rejected")
	} else if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("duplicate keys: got %v, want INVALID_ARGUMENT", err)
	}
}
