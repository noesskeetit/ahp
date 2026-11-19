package contracts

import (
	"encoding/base32"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestScopeValidateAcceptsSafeIdentifiers(t *testing.T) {
	for _, scope := range []Scope{
		{Tenant: "acme", Deployment: "agents-space", Subject: "user.01"},
		{Tenant: "t-1", Deployment: "deploy_2", Subject: "svc:runner:7"},
		{Tenant: "ops@corp", Deployment: "b", Subject: "c"},
		{Tenant: strings.Repeat("a", 128), Deployment: "b", Subject: "c"},
	} {
		if err := scope.Validate(); err != nil {
			t.Errorf("Validate(%+v) = %v, want nil", scope, err)
		}
	}
}

func TestScopeValidateRejectsMissingOversizedAndUnsafe(t *testing.T) {
	long := strings.Repeat("a", 129)
	unsafe := []string{"", " ", "a b", "a\tb", "a\nb", "a/b", "a\\b", "a\x00b", "a.b\x01", "aé", "a;b", "a*b", "a?b"}
	mutations := []func(s *Scope, bad string){
		func(s *Scope, bad string) { s.Tenant = bad },
		func(s *Scope, bad string) { s.Deployment = bad },
		func(s *Scope, bad string) { s.Subject = bad },
	}
	for _, bad := range unsafe {
		for i, mutate := range mutations {
			scope := Scope{Tenant: "acme", Deployment: "agents", Subject: "user.1"}
			mutate(&scope, bad)
			if err := scope.Validate(); err == nil {
				t.Errorf("field %d: Validate accepted %q", i, bad)
			} else if Code(err) != ErrInvalid {
				t.Errorf("field %d value %q: code = %q, want %q", i, bad, Code(err), ErrInvalid)
			}
		}
	}
	for _, oversized := range []Scope{
		{Tenant: long, Deployment: "d", Subject: "s"},
		{Tenant: "t", Deployment: long, Subject: "s"},
		{Tenant: "t", Deployment: "d", Subject: long},
	} {
		if err := oversized.Validate(); err == nil {
			t.Errorf("Validate accepted an oversized field: %+v", oversized)
		}
	}
}

func TestNewIDUniqueAndDecodable(t *testing.T) {
	seen := make(map[string]struct{}, 4096)
	for i := 0; i < 4096; i++ {
		id := NewID()
		if id == "" {
			t.Fatal("NewID returned empty id")
		}
		if strings.ContainsAny(id, " /\\\x00") {
			t.Fatalf("NewID returned unsafe id %q", id)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("NewID repeated id %q", id)
		}
		seen[id] = struct{}{}
	}
	// The random component must carry full 128-bit strength.
	id := NewID()
	parts := strings.Split(id, "_")
	if len(parts) != 2 {
		t.Fatalf("unexpected id layout %q", id)
	}
	raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(strings.ToUpper(parts[1]))
	if err != nil {
		t.Fatalf("id %q random part is not base32: %v", id, err)
	}
	if len(raw) != 16 {
		t.Fatalf("random part is %d bytes, want 16", len(raw))
	}
}

func TestDigestStableAcrossKeyOrderAndWhitespace(t *testing.T) {
	a := `{"b":1,"a":{"z":[1,2],"y":"s"},"c":true}`
	b := "{\n  \"c\" : true,\n  \"a\": { \"y\": \"s\", \"z\": [ 1, 2 ] },\n  \"b\": 1\n}"
	da, err := Digest(json.RawMessage(a))
	if err != nil {
		t.Fatalf("Digest(a) = %v", err)
	}
	db, err := Digest(json.RawMessage(b))
	if err != nil {
		t.Fatalf("Digest(b) = %v", err)
	}
	if da != db {
		t.Fatalf("digest differs for equivalent objects: %s != %s", da, db)
	}

	// Decoded values (maps) and raw JSON must agree.
	var decoded any
	if err := json.Unmarshal([]byte(a), &decoded); err != nil {
		t.Fatal(err)
	}
	dd, err := Digest(decoded)
	if err != nil {
		t.Fatalf("Digest(decoded) = %v", err)
	}
	if dd != da {
		t.Fatalf("decoded value digest %s != raw digest %s", dd, da)
	}

	// Structs and maps with identical content agree.
	type inner struct {
		Y string `json:"y"`
		Z []int  `json:"z"`
	}
	type outer struct {
		A inner `json:"a"`
		B int   `json:"b"`
		C bool  `json:"c"`
	}
	ds, err := Digest(outer{A: inner{Y: "s", Z: []int{1, 2}}, B: 1, C: true})
	if err != nil {
		t.Fatalf("Digest(struct) = %v", err)
	}
	if ds != da {
		t.Fatalf("struct digest %s != raw digest %s", ds, da)
	}
}

func TestDigestPreservesNumberValues(t *testing.T) {
	equal := [][2]string{
		{`{"v":1}`, `{"v":1.0}`},
		{`{"v":1e1}`, `{"v":10}`},
		{`{"v":100}`, `{"v":1e2}`},
		{`{"v":1.50}`, `{"v":1.5}`},
		{`{"v":1e5}`, `{"v":100000}`},
		{`{"v":0.0001}`, `{"v":1e-4}`},
		{`{"v":-0}`, `{"v":0}`},
		{`{"v":1e-005}`, `{"v":1e-5}`},
		{`{"v":123456789012345678901234567890}`, `{"v":1.2345678901234567890123456789e29}`},
		{`{"v":0.1}`, `{"v":0.100000000000000000000000000000}`},
	}
	for _, pair := range equal {
		da, err := Digest(json.RawMessage(pair[0]))
		if err != nil {
			t.Fatalf("Digest(%s) = %v", pair[0], err)
		}
		db, err := Digest(json.RawMessage(pair[1]))
		if err != nil {
			t.Fatalf("Digest(%s) = %v", pair[1], err)
		}
		if da != db {
			t.Errorf("Digest(%s) = %s, Digest(%s) = %s, want equal", pair[0], da, pair[1], db)
		}
	}

	distinct := []string{`{"v":1}`, `{"v":1.5}`, `{"v":10}`, `{"v":-1}`, `{"v":0.1}`, `{"v":1e-31}`}
	seen := make(map[string]string, len(distinct))
	for _, c := range distinct {
		d, err := Digest(json.RawMessage(c))
		if err != nil {
			t.Fatalf("Digest(%s) = %v", c, err)
		}
		if prev, ok := seen[d]; ok {
			t.Errorf("Digest(%s) collides with Digest(%s)", c, prev)
		}
		seen[d] = c
	}
}

// TestDigestNumbersAreExact guards the different-arguments protection: no two
// distinct numeric values may share a digest because of rounding.
func TestDigestNumbersAreExact(t *testing.T) {
	dZero, err := Digest(json.RawMessage(`{"v":0}`))
	if err != nil {
		t.Fatal(err)
	}
	// A value far below float64 or 30-digit precision must not become zero.
	for _, tiny := range []string{`{"v":1e-31}`, `{"v":1e-300}`, `{"v":1e-1000}`} {
		d, err := Digest(json.RawMessage(tiny))
		if err != nil {
			t.Fatalf("Digest(%s) = %v", tiny, err)
		}
		if d == dZero {
			t.Errorf("Digest(%s) collides with Digest(0)", tiny)
		}
	}
	// Two decimals differing only beyond the 30th digit must differ.
	a := `{"v":0.1000000000000000000000000000001}`
	b := `{"v":0.1000000000000000000000000000002}`
	da, err := Digest(json.RawMessage(a))
	if err != nil {
		t.Fatal(err)
	}
	db, err := Digest(json.RawMessage(b))
	if err != nil {
		t.Fatal(err)
	}
	if da == db {
		t.Error("values differing beyond 30 digits digest identically")
	}
	// Long integers keep every digit.
	dSmall, err := Digest(json.RawMessage(`{"v":123456789012345678901234567890}`))
	if err != nil {
		t.Fatal(err)
	}
	dBig, err := Digest(json.RawMessage(`{"v":123456789012345678901234567891}`))
	if err != nil {
		t.Fatal(err)
	}
	if dSmall == dBig {
		t.Error("long integers differing in the last digit digest identically")
	}
	// Exponents outside the documented bound are rejected, not rounded.
	for _, out := range []string{`{"v":1e1001}`, `{"v":1e-1001}`} {
		if _, err := Digest(json.RawMessage(out)); err == nil {
			t.Errorf("Digest accepted an out-of-range exponent %s", out)
		} else if Code(err) != ErrInvalid {
			t.Errorf("Digest(%s): code = %q, want %q", out, Code(err), ErrInvalid)
		}
	}
	// Boundary exponents inside the bound are accepted exactly.
	for _, in := range []string{`{"v":1e1000}`, `{"v":1e-1000}`} {
		if _, err := Digest(json.RawMessage(in)); err != nil {
			t.Errorf("Digest(%s) = %v, want acceptance", in, err)
		}
	}
	// json.Number must be a strict JSON number, not a general rational.
	for _, bad := range []string{"1/2", "0x10", "1e", "", " 1", "1 ", "NaN", "Inf", "+1", "1.2.3"} {
		if _, err := Digest(json.Number(bad)); err == nil {
			t.Errorf("Digest accepted json.Number(%q)", bad)
		}
	}
	// float64 canonicalisation agrees with the equivalent json.Number.
	for _, pair := range [][2]any{
		{0.1, json.Number("0.1")},
		{1e-31, json.Number("1e-31")},
		{1e21, json.Number("1e21")},
		{1.5, json.Number("1.5")},
		{-0.0, json.Number("0")},
		{3.0, json.Number("3")},
	} {
		df, err := Digest(pair[0])
		if err != nil {
			t.Fatalf("Digest(%v) = %v", pair[0], err)
		}
		dn, err := Digest(pair[1])
		if err != nil {
			t.Fatalf("Digest(%v) = %v", pair[1], err)
		}
		if df != dn {
			t.Errorf("float64 %v digest %s != json.Number %v digest %s", pair[0], df, pair[1], dn)
		}
	}
}

func TestDigestRejectsInvalidInput(t *testing.T) {
	rejected := []string{
		`{"a":1,"a":2}`,
		`{"a":NaN}`,
		`{"a":Infinity}`,
		`{"a":-Infinity}`,
		`{"a":1} trailing`,
		`   `,
		`{"a":01}`,
		`{"a":+1}`,
		`{"a":.5}`,
		`{"a":1.}`,
		`{"a":1,`,
		`{"a":}`,
	}
	for _, raw := range rejected {
		if _, err := Digest(json.RawMessage(raw)); err == nil {
			t.Errorf("Digest accepted invalid raw JSON %q", raw)
		} else if Code(err) != ErrInvalid {
			t.Errorf("Digest(%q): code = %q, want %q", raw, Code(err), ErrInvalid)
		}
	}

	if _, err := Digest(complex(1, 2)); err == nil {
		t.Error("Digest accepted a complex number")
	}
	if _, err := Digest(func() {}); err == nil {
		t.Error("Digest accepted a function")
	}
	if _, err := Digest(make(chan int)); err == nil {
		t.Error("Digest accepted a channel")
	}
	if _, err := Digest(mathNaN()); err == nil {
		t.Error("Digest accepted a non-finite float64")
	}
	if _, err := Digest(mathInf(1)); err == nil {
		t.Error("Digest accepted +Inf")
	}
	if _, err := Digest(unsupported{}); err == nil {
		t.Error("Digest accepted an unsupported value")
	}
}

func TestDigestRejectsDuplicateKeys(t *testing.T) {
	cases := []string{
		`{"a":1,"a":2}`,
		`{"a":{"b":1,"b":2}}`,
		`[{"k":1,"k":2}]`,
		`[{"k":1},{"k":2},{"j":3,"j":4}]`,
	}
	for _, raw := range cases {
		if _, err := Digest(json.RawMessage(raw)); err == nil {
			t.Errorf("Digest accepted duplicate object keys in %s", raw)
		}
	}
	// Repeating the same key in distinct objects is valid JSON.
	if _, err := Digest(json.RawMessage(`[{"k":1},{"k":2},{"k":2}]`)); err != nil {
		t.Errorf("Digest rejected valid JSON with repeated keys in distinct objects: %v", err)
	}
	// Duplicate keys hidden inside embedded raw JSON are rejected too, before
	// encoding/json would normalise them away.
	if _, err := Digest(rawCarrier{Payload: json.RawMessage(`{"x":1,"x":2}`)}); err == nil {
		t.Error("Digest accepted duplicate keys inside embedded raw JSON")
	}
	// And inside a raw array element of a struct field.
	if _, err := Digest(rawCarrier{Payload: json.RawMessage(`[{"y":1,"y":2}]`)}); err == nil {
		t.Error("Digest accepted duplicate keys inside an embedded raw array")
	}
}

func TestDigestIsDeterministicAndPrefixed(t *testing.T) {
	v := map[string]any{"z": []any{1, "two", nil, true}, "a": map[string]any{"k": "v"}}
	first, err := Digest(v)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		next, err := Digest(v)
		if err != nil {
			t.Fatal(err)
		}
		if next != first {
			t.Fatalf("digest not deterministic: %s != %s", next, first)
		}
	}
	if !strings.HasPrefix(first, DigestPrefix) || len(first) != len(DigestPrefix)+64 {
		t.Fatalf("unexpected digest format %q", first)
	}
}

func TestDigestNilAndScalars(t *testing.T) {
	for _, v := range []any{nil, "s", true, 1, int64(-3), uint64(4), json.Number("1.20"), 3.5, []byte("raw")} {
		if _, err := Digest(v); err != nil {
			t.Errorf("Digest(%#v) = %v", v, err)
		}
	}
	if dn, err := Digest(nil); err != nil {
		t.Fatal(err)
	} else if dn != mustDigest(t, "null") {
		t.Errorf("nil digest %s, want digest of JSON null", dn)
	}
	if d, err := Digest(json.Number("1.20")); err != nil {
		t.Fatal(err)
	} else if d != mustDigest(t, "1.2") {
		t.Errorf("json.Number digest %s does not match normalised 1.2", d)
	}
	// Nil pointers, maps and slices follow encoding/json: null, {} and [].
	var np *Scope
	var nm map[string]any
	var ns []any
	if d, err := Digest(np); err != nil {
		t.Errorf("Digest(nil *Scope) = %v", err)
	} else if d != mustDigest(t, "null") {
		t.Errorf("Digest(nil *Scope) = %s, want null digest", d)
	}
	if d, err := Digest(nm); err != nil {
		t.Errorf("Digest(nil map) = %v", err)
	} else if d != mustDigest(t, "{}") {
		t.Errorf("Digest(nil map) = %s, want empty-object digest", d)
	}
	if d, err := Digest(ns); err != nil {
		t.Errorf("Digest(nil slice) = %v", err)
	} else if d != mustDigest(t, "[]") {
		t.Errorf("Digest(nil slice) = %s, want empty-array digest", d)
	}
	// A type with only a String method: encoding/json ignores String and
	// marshals its exported field, so the canonical form is {"Value":"x"}.
	if d, err := Digest(stringerOnly{Value: "x"}); err != nil {
		t.Errorf("Digest(stringer) = %v", err)
	} else if d != mustDigest(t, `{"Value":"x"}`) {
		t.Errorf("Digest(stringer) = %s, want %s", d, mustDigest(t, `{"Value":"x"}`))
	}
}

func TestDigestDistinguishesContent(t *testing.T) {
	base := map[string]any{"a": "1", "b": []any{1, 2}}
	d0, err := Digest(base)
	if err != nil {
		t.Fatal(err)
	}
	variants := []map[string]any{
		{"a": "1", "b": []any{1, 2}, "c": nil},
		{"a": "1", "b": []any{2, 1}},
		{"a": "1", "b": []any{}},
		{"a": "11", "b": []any{1, 2}},
	}
	for i, v := range variants {
		d, err := Digest(v)
		if err != nil {
			t.Fatalf("variant %d: %v", i, err)
		}
		if d == d0 {
			t.Errorf("variant %d %+v digests identical to base", i, v)
		}
	}
}

func mustDigest(t *testing.T, raw string) string {
	t.Helper()
	d, err := Digest(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Digest(%s) = %v", raw, err)
	}
	return d
}

func TestOperationKeyDeterministicAndScoped(t *testing.T) {
	scope := Scope{Tenant: "acme", Deployment: "agents", Subject: "user.1"}
	k1 := OperationKey(scope, "run-1", "node.a", "call-1")
	k2 := OperationKey(scope, "run-1", "node.a", "call-1")
	if k1 != k2 {
		t.Fatalf("OperationKey not deterministic: %s != %s", k1, k2)
	}
	if len(k1) != 64 || strings.ContainsAny(k1, " /\\\x00") {
		t.Fatalf("unexpected operation key %q", k1)
	}

	other := Scope{Tenant: "acme", Deployment: "agents", Subject: "user.2"}
	if OperationKey(other, "run-1", "node.a", "call-1") == k1 {
		t.Fatal("operation key ignores scope")
	}
	for _, mutate := range [][3]string{
		{"run-2", "node.a", "call-1"},
		{"run-1", "node.b", "call-1"},
		{"run-1", "node.a", "call-2"},
	} {
		if OperationKey(scope, mutate[0], mutate[1], mutate[2]) == k1 {
			t.Fatalf("operation key ignores field change %v", mutate)
		}
	}
}

func TestOperationKeyRespectsFieldBoundaries(t *testing.T) {
	scope := Scope{Tenant: "acme", Deployment: "agents", Subject: "user.1"}
	// Without length framing these two would hash the same byte sequence.
	a := OperationKey(scope, "run", "node", "xcall")
	b := OperationKey(scope, "run", "nodex", "call")
	if a == b {
		t.Fatal("operation key concatenates fields without framing")
	}
	c := OperationKey(scope, "run\x00node\x00call", "", "")
	if a == c || b == c {
		t.Fatal("operation key does not separate separators from content")
	}
	// Empty fields must still produce a stable, distinct key.
	if OperationKey(scope, "", "", "") == OperationKey(scope, "", "", "x") {
		t.Fatal("operation key ignores the trailing field")
	}
	if OperationKey(scope, "", "x", "") == OperationKey(scope, "x", "", "") {
		t.Fatal("operation key ignores field position")
	}
}

func TestErrorCodesAndHelpers(t *testing.T) {
	want := map[string]string{
		ErrNotFound:   "NOT_FOUND",
		ErrConflict:   "CONFLICT",
		ErrStaleEpoch: "STALE_EPOCH",
		ErrForbidden:  "FORBIDDEN",
		ErrInvalid:    "INVALID_ARGUMENT",
		ErrKeyReuse:   "KEY_REUSE_MISMATCH",
		ErrLeaseLost:  "LEASE_LOST",
	}
	for code, literal := range want {
		if code != literal {
			t.Errorf("code constant mismatch: %q != %q", code, literal)
		}
		err := NewError(code, "boom")
		var e *Error
		if !errors.As(err, &e) {
			t.Fatalf("NewError(%s) is not a *Error", code)
		}
		if e.Code != code || e.Message != "boom" {
			t.Errorf("error = %+v", e)
		}
		if Code(err) != code {
			t.Errorf("Code = %q, want %q", Code(err), code)
		}
		if !strings.Contains(e.Error(), code) {
			t.Errorf("Error() = %q, want it to contain the code", e.Error())
		}
		data, mErr := json.Marshal(e)
		if mErr != nil {
			t.Fatal(mErr)
		}
		var back Error
		if uErr := json.Unmarshal(data, &back); uErr != nil {
			t.Fatal(uErr)
		}
		if back.Code != code || back.Message != "boom" {
			t.Errorf("JSON round trip = %+v", back)
		}
	}
	if Code(errors.New("plain")) != "" {
		t.Error("Code of a plain error must be empty")
	}
	if Code(fmt.Errorf("wrapped: %w", NewError(ErrConflict, "x"))) != ErrConflict {
		t.Error("Code must unwrap")
	}
	if Code(nil) != "" {
		t.Error("Code(nil) must be empty")
	}
}
