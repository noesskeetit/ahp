package atrest

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	testTenant  = "tenant-a"
	testPurpose = "state.payload"
	// plaintextSentinel is embedded in every test payload so tests can assert it
	// never appears in sealed bytes.
	plaintextSentinel = "TOP-PLAINTEXT-MARKER-9f31"
)

func mustSeal(t *testing.T, c *Codec, tenant, purpose, pt string) []byte {
	t.Helper()
	env, err := c.Seal(context.Background(), tenant, purpose, []byte(pt))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return env
}

func mustOpen(t *testing.T, c *Codec, tenant, purpose string, env []byte) string {
	t.Helper()
	pt, err := c.Open(context.Background(), tenant, purpose, env)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return string(pt)
}

func newTestCodec(t *testing.T, tenants ...string) (*Codec, *SyntheticKeyManager) {
	t.Helper()
	auth := NewSyntheticKeyManager()
	ctx := context.Background()
	for _, ten := range tenants {
		if _, err := auth.Provision(ctx, ten, "root", nil); err != nil {
			t.Fatalf("provision %s: %v", ten, err)
		}
	}
	c, err := NewCodec(auth)
	if err != nil {
		t.Fatal(err)
	}
	return c, auth
}

func TestSealOpenRoundtrip(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	payload := "value=" + plaintextSentinel
	env := mustSeal(t, c, testTenant, testPurpose, payload)
	if got := mustOpen(t, c, testTenant, testPurpose, env); got != payload {
		t.Fatalf("roundtrip mismatch: %q", got)
	}
}

// Ciphertext must not carry the plaintext: a TLS success or a synthetic KMS
// reply proves nothing about bytes at rest, so we assert on the bytes directly.
func TestCiphertextDiffersFromPlaintext(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	env := mustSeal(t, c, testTenant, testPurpose, "payload "+plaintextSentinel)
	if bytes.Contains(env, []byte(plaintextSentinel)) {
		t.Fatal("sealed bytes still contain the plaintext marker")
	}
	if bytes.Equal(env, []byte("payload "+plaintextSentinel)) {
		t.Fatal("sealed bytes equal plaintext")
	}
}

func TestSealIsRandomized(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	pt := "value=" + plaintextSentinel
	a := mustSeal(t, c, testTenant, testPurpose, pt)
	b := mustSeal(t, c, testTenant, testPurpose, pt)
	if bytes.Equal(a, b) {
		t.Fatal("two seals of the same plaintext produced identical bytes")
	}
	for _, env := range [][]byte{a, b} {
		if got := mustOpen(t, c, testTenant, testPurpose, env); got != pt {
			t.Fatalf("open randomized: %q", got)
		}
	}
}

func TestWrongTenantRejected(t *testing.T) {
	c, _ := newTestCodec(t, testTenant, "tenant-b")
	env := mustSeal(t, c, testTenant, testPurpose, "x")
	if _, err := c.Open(context.Background(), "tenant-b", testPurpose, env); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("want ErrTenantMismatch, got %v", err)
	}
}

// A different master key under the same (tenant, key id, version) must not open
// the envelope: this is the wrong-key negative at the primitive level.
func TestWrongKeyRejected(t *testing.T) {
	m1, err := randomTenantKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	m2, err := randomTenantKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	a1 := NewSyntheticKeyManager()
	if _, err := a1.Provision(ctx, testTenant, "root", m1); err != nil {
		t.Fatal(err)
	}
	c1, _ := NewCodec(a1)
	env := mustSeal(t, c1, testTenant, testPurpose, "x")

	a2 := NewSyntheticKeyManager()
	if _, err := a2.Provision(ctx, testTenant, "root", m2); err != nil {
		t.Fatal(err)
	}
	c2, _ := NewCodec(a2)
	if _, err := c2.Open(ctx, testTenant, testPurpose, env); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed for wrong key, got %v", err)
	}
}

func TestWrongPurposeRejected(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	env := mustSeal(t, c, testTenant, "artifact.bytes", "x")
	if _, err := c.Open(context.Background(), testTenant, "memory.fact", env); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed for purpose swap, got %v", err)
	}
}

func TestTamperRejected(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	env := mustSeal(t, c, testTenant, testPurpose, "value "+plaintextSentinel)
	// Flip a bit near the end of the ciphertext payload.
	bad := append([]byte(nil), env...)
	bad[len(bad)-1] ^= 0x01
	if _, err := c.Open(context.Background(), testTenant, testPurpose, bad); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed on payload tamper, got %v", err)
	}
}

func TestTamperWrappedDEKRejected(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	env := mustSeal(t, c, testTenant, testPurpose, "x")
	e, err := openEnvelope(env)
	if err != nil {
		t.Fatal(err)
	}
	e.dekWrap[len(e.dekWrap)-1] ^= 0x02
	raw, err := e.sealBytes()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(context.Background(), testTenant, testPurpose, raw); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("want ErrAuthFailed on DEK-wrap tamper, got %v", err)
	}
}

// Rotating the tenant key must not destroy access to previously sealed data:
// the old version becomes readable-only, new seals use the new version.
func TestRotationReadsPriorVersions(t *testing.T) {
	auth := NewSyntheticKeyManager()
	ctx := context.Background()
	if _, err := auth.Provision(ctx, testTenant, "root", nil); err != nil {
		t.Fatal(err)
	}
	c, _ := NewCodec(auth)
	oldEnv := mustSeal(t, c, testTenant, testPurpose, "before-rotation")
	if _, err := auth.Rotate(ctx, testTenant, "root", nil); err != nil {
		t.Fatal(err)
	}
	// Old ciphertext must still open (old version demoted to rotated).
	if got := mustOpen(t, c, testTenant, testPurpose, oldEnv); got != "before-rotation" {
		t.Fatalf("old env after rotation: %q", got)
	}
	// New seals must use version 2.
	newEnv := mustSeal(t, c, testTenant, testPurpose, "after-rotation")
	e, err := openEnvelope(newEnv)
	if err != nil {
		t.Fatal(err)
	}
	if e.keyVersion != 2 {
		t.Fatalf("expected new seals on version 2, got %d", e.keyVersion)
	}
	if got := mustOpen(t, c, testTenant, testPurpose, newEnv); got != "after-rotation" {
		t.Fatalf("new env: %q", got)
	}
}

func TestDisabledKeyRefused(t *testing.T) {
	auth := NewSyntheticKeyManager()
	ctx := context.Background()
	mk, _ := auth.Provision(ctx, testTenant, "root", nil)
	c, _ := NewCodec(auth)
	env := mustSeal(t, c, testTenant, testPurpose, "x")
	if err := auth.SetStatus(ctx, testTenant, "root", mk.Version, StatusDisabled); err != nil {
		t.Fatal(err)
	}
	// Opening prior data under a disabled key must refuse.
	if _, err := c.Open(ctx, testTenant, testPurpose, env); !errors.Is(err, ErrKeyDisabled) {
		t.Fatalf("want ErrKeyDisabled, got %v", err)
	}
	// Sealing must fail closed too (no active key).
	if _, err := c.Seal(ctx, testTenant, testPurpose, []byte("y")); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("want ErrUnknownTenant (no active key) on Seal, got %v", err)
	}
	// Re-enabling restores access.
	if err := auth.SetStatus(ctx, testTenant, "root", mk.Version, StatusActive); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, testTenant, testPurpose, env); err != nil {
		t.Fatalf("after re-enable, Open failed: %v", err)
	}
}

func TestRevokedKeyRefused(t *testing.T) {
	auth := NewSyntheticKeyManager()
	ctx := context.Background()
	mk, _ := auth.Provision(ctx, testTenant, "root", nil)
	c, _ := NewCodec(auth)
	env := mustSeal(t, c, testTenant, testPurpose, "x")
	if err := auth.SetStatus(ctx, testTenant, "root", mk.Version, StatusRevoked); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Open(ctx, testTenant, testPurpose, env); !errors.Is(err, ErrKeyRevoked) {
		t.Fatalf("want ErrKeyRevoked, got %v", err)
	}
	// A revoked key can never be reactivated.
	if err := auth.SetStatus(ctx, testTenant, "root", mk.Version, StatusActive); !errors.Is(err, ErrKeyRevoked) {
		t.Fatalf("reactivating a revoked key must fail, got %v", err)
	}
}

func TestUnknownTenantFailsClosed(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	if _, err := c.Seal(context.Background(), "tenant-ghost", testPurpose, []byte("x")); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("want ErrUnknownTenant on Seal, got %v", err)
	}
	env := mustSeal(t, c, testTenant, testPurpose, "x")
	if _, err := c.Open(context.Background(), "tenant-ghost", testPurpose, env); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("want ErrTenantMismatch, got %v", err)
	}
}

func TestEnvelopeMalformedRejected(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	env := mustSeal(t, c, testTenant, testPurpose, "x")
	// Truncation and a bogus magic must both fail closed before any key lookup.
	if _, err := c.Open(context.Background(), testTenant, testPurpose, env[:len(env)/2]); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed on truncation, got %v", err)
	}
	junk := append([]byte("XXXXXXXX"), env...)
	if _, err := c.Open(context.Background(), testTenant, testPurpose, junk); !errors.Is(err, ErrMalformed) {
		t.Fatalf("want ErrMalformed on bad magic, got %v", err)
	}
}

// A BYOK profile (Tier 1) with the same tenant-supplied material across two
// authorities must interoperate: the key is the customer's, not per-process.
func TestByokInteropAcrossAuthorities(t *testing.T) {
	ctx := context.Background()
	byok, err := randomTenantKeyMaterial()
	if err != nil {
		t.Fatal(err)
	}
	hexKey := hex.EncodeToString(byok)
	dir := t.TempDir()
	byokFile := filepath.Join(dir, "byok")
	if err := os.WriteFile(byokFile, []byte(testTenant+"="+hexKey+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvByokSecretFile, byokFile)
	t.Setenv(EnvRootSecretFile, "")

	a1, err := LoadAuthorityFromEnv(ctx, []string{testTenant})
	if err != nil {
		t.Fatal(err)
	}
	c1, _ := NewCodec(a1)
	env := mustSeal(t, c1, testTenant, testPurpose, "value "+plaintextSentinel)

	a2, err := LoadAuthorityFromEnv(ctx, []string{testTenant})
	if err != nil {
		t.Fatal(err)
	}
	c2, _ := NewCodec(a2)
	if _, err := c2.Open(ctx, testTenant, testPurpose, env); err != nil {
		t.Fatalf("BYOK interop failed across authorities: %v", err)
	}
	// Operator-root profile for the same tenant must NOT open the BYOK ciphertext.
	rootFile := filepath.Join(dir, "root")
	root, _ := randomTenantKeyMaterial()
	if err := os.WriteFile(rootFile, []byte(hex.EncodeToString(root)), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvByokSecretFile, "")
	t.Setenv(EnvRootSecretFile, rootFile)
	a3, err := LoadAuthorityFromEnv(ctx, []string{testTenant})
	if err != nil {
		t.Fatal(err)
	}
	c3, _ := NewCodec(a3)
	if _, err := c3.Open(ctx, testTenant, testPurpose, env); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("unrelated operator key opened BYOK ciphertext: %v", err)
	}
}

func TestLoadAuthorityRejectsShortKeys(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	byokFile := filepath.Join(dir, "byok")
	if err := os.WriteFile(byokFile, []byte(testTenant+"=deadbeef\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvByokSecretFile, byokFile)
	_, err := LoadAuthorityFromEnv(ctx, []string{testTenant})
	if err == nil || !strings.Contains(err.Error(), ErrKeyLength.Error()) {
		t.Fatalf("want key-length error, got %v", err)
	}
}

func TestCodecRejectsNilAuthority(t *testing.T) {
	if _, err := NewCodec(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("want ErrInvalidArgument, got %v", err)
	}
}

// TestLargePayloadRoundtrip guards the envelope serialization against a length
// prefix that cannot hold a real record: the field length must cover a payload
// far larger than 64 KiB (the earlier 2-byte prefix rejected these silently).
func TestLargePayloadRoundtrip(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	payload := bytes.Repeat([]byte("state-blob-"), 200_000) // ~2.2 MiB, > 64 KiB
	env := mustSeal(t, c, testTenant, "state.blob", string(payload))
	got := mustOpen(t, c, testTenant, "state.blob", env)
	if got != string(payload) {
		t.Fatalf("large payload roundtrip length mismatch: got %d want %d", len(got), len(payload))
	}
	if !IsEnvelope(env) {
		t.Fatal("large sealed bytes must be a valid envelope")
	}
}
