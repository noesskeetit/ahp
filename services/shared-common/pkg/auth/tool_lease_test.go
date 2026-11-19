package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestToolLeaseIsBoundToSessionEpochAndTask(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewSigner("state-authority", "state-key", priv)
	if err != nil {
		t.Fatal(err)
	}
	verifier := NewVerifier("state-authority", map[string]ed25519.PublicKey{"state-key": pub})
	worker := Claims{Kind: KindTask, Tenant: "tenant", Deployment: "dep", Subject: "user",
		Task: "worker-1", Context: "conversation-1", Grant: "grant-1", Root: "root-1"}
	expires := time.Now().Add(time.Minute).UTC().Truncate(time.Microsecond)
	token, err := signer.SignToolLease(worker, "session-1", 7, expires)
	if err != nil {
		t.Fatal(err)
	}
	got, err := verifier.VerifyToolLease(token, worker, "session-1", 7)
	if err != nil || !got.Equal(expires) {
		t.Fatalf("verified expiry %v, error %v", got, err)
	}
	for _, test := range []struct {
		name string
		edit func(*Claims, *string, *int64)
	}{
		{"tenant", func(c *Claims, _ *string, _ *int64) { c.Tenant = "other" }},
		{"deployment", func(c *Claims, _ *string, _ *int64) { c.Deployment = "other" }},
		{"subject", func(c *Claims, _ *string, _ *int64) { c.Subject = "other" }},
		{"task", func(c *Claims, _ *string, _ *int64) { c.Task = "worker-2" }},
		{"context", func(c *Claims, _ *string, _ *int64) { c.Context = "conversation-2" }},
		{"grant", func(c *Claims, _ *string, _ *int64) { c.Grant = "grant-2" }},
		{"root", func(c *Claims, _ *string, _ *int64) { c.Root = "root-2" }},
		{"kind", func(c *Claims, _ *string, _ *int64) { c.Kind = KindHuman }},
		{"session", func(_ *Claims, s *string, _ *int64) { *s = "session-2" }},
		{"epoch", func(_ *Claims, _ *string, e *int64) { *e = 8 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			caller, session, epoch := worker, "session-1", int64(7)
			test.edit(&caller, &session, &epoch)
			if _, err := verifier.VerifyToolLease(token, caller, session, epoch); contracts.Code(err) != contracts.ErrForbidden {
				t.Fatalf("changed %s was not rejected: %v", test.name, err)
			}
		})
	}
	payloadEnd := strings.LastIndexByte(token, '.')
	replacement := "A"
	if token[payloadEnd+1] == 'A' {
		replacement = "B"
	}
	tampered := token[:payloadEnd+1] + replacement + token[payloadEnd+2:]
	if _, err := verifier.VerifyToolLease(tampered, worker, "session-1", 7); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("altered signature was not rejected: %v", err)
	}
	if _, err := verifier.Verify(token, "ahp", KindTask, WorkloadID(worker.Scope())); err == nil {
		t.Fatal("tool lease accepted as a task token")
	}
}
