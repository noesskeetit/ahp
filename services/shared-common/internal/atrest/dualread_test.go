package atrest

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestIsEnvelopeAndTransparentPassthrough(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	legacy := []byte("legacy row " + plaintextSentinel)
	if IsEnvelope(legacy) {
		t.Fatal("plaintext must not be detected as an envelope")
	}
	// A reader with encryption enabled must pass a legacy plaintext row through.
	pt, sealed, err := c.OpenTransparent(context.Background(), testTenant, testPurpose, legacy)
	if err != nil || sealed || string(pt) != string(legacy) {
		t.Fatalf("passthrough wrong: sealed=%v err=%v pt=%q", sealed, err, pt)
	}
	env := mustSeal(t, c, testTenant, testPurpose, "new row")
	if !IsEnvelope(env) {
		t.Fatal("sealed bytes must be detected as an envelope")
	}
	pt, sealed, err = c.OpenTransparent(context.Background(), testTenant, testPurpose, env)
	if err != nil || !sealed || string(pt) != "new row" {
		t.Fatalf("sealed open wrong: sealed=%v err=%v pt=%q", sealed, err, pt)
	}
}

func TestTransparentGenuineEnvelopeStillFailsClosed(t *testing.T) {
	c, _ := newTestCodec(t, testTenant, "tenant-b")
	env := mustSeal(t, c, testTenant, testPurpose, "x")
	tampered := append([]byte(nil), env...)
	tampered[len(tampered)-1] ^= 0x01
	if _, _, err := c.OpenTransparent(context.Background(), testTenant, testPurpose, tampered); !errors.Is(err, ErrAuthFailed) {
		t.Fatalf("tampered envelope must fail closed, got %v", err)
	}
}

// benchSizes are the four payload sizes required for the acceptance overhead
// table (1 KiB, 64 KiB, 1 MiB, 32 MiB).
var benchSizes = []struct {
	name string
	size int
}{
	{"1KiB", 1 << 10},
	{"64KiB", 64 << 10},
	{"1MiB", 1 << 20},
	{"32MiB", 32 << 20},
}

func BenchmarkSeal(b *testing.B) {
	for _, s := range benchSizes {
		s := s
		b.Run(s.name, func(b *testing.B) { benchSeal(b, s.size) })
	}
}

func BenchmarkOpen(b *testing.B) {
	for _, s := range benchSizes {
		s := s
		b.Run(s.name, func(b *testing.B) { benchOpen(b, s.size) })
	}
}

func benchAuth(b *testing.B) (*Codec, *SyntheticKeyManager) {
	auth := NewSyntheticKeyManager()
	if _, err := auth.Provision(context.Background(), testTenant, "root", nil); err != nil {
		b.Fatal(err)
	}
	c, err := NewCodec(auth)
	if err != nil {
		b.Fatal(err)
	}
	return c, auth
}

func benchSeal(b *testing.B, size int) {
	c, _ := benchAuth(b)
	payload := make([]byte, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	var out []byte
	for i := 0; i < b.N; i++ {
		var err error
		out, err = c.Seal(context.Background(), testTenant, "bench", payload)
		if err != nil {
			b.Fatal(err)
		}
	}
	_ = out
}

func benchOpen(b *testing.B, size int) {
	c, _ := benchAuth(b)
	payload := make([]byte, size)
	env, err := c.Seal(context.Background(), testTenant, "bench", payload)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Open(context.Background(), testTenant, "bench", env); err != nil {
			b.Fatal(err)
		}
	}
}

// TestSealSizeOverhead records the deterministic byte growth of an envelope so
// the report can state size growth precisely: it is constant per record plus the
// tenant/key id lengths, not proportional to the payload.
func TestSealSizeOverhead(t *testing.T) {
	c, _ := newTestCodec(t, testTenant)
	payload := bytes.Repeat([]byte("x"), 4096)
	env, err := c.Seal(context.Background(), testTenant, "artifact.bytes", payload)
	if err != nil {
		t.Fatal(err)
	}
	grew := len(env) - len(payload)
	e, err := openEnvelope(env)
	if err != nil {
		t.Fatal(err)
	}
	// overhead = framing + ids + nonces + wrapped DEK + GCM tag
	// Each writeBytes/writeString field carries a 4-byte length prefix.
	want := 6 + 1 + (4 + len(testTenant)) + (4 + len(e.keyID)) + 4 + (4 + nonceBytes) + (4 + dekBytes + 16) + (4 + nonceBytes) + (4 + 16)
	if grew != want {
		t.Fatalf("overhead mismatch: grew=%d want=%d", grew, want)
	}
	t.Logf("envelope overhead for %d-byte payload (tenant=%q,key=%q): +%d bytes", len(payload), testTenant, e.keyID, grew)
}
