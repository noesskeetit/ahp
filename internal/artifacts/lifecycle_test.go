package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestDerivationReadiness(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	body := "raw-bytes-for-derivations"
	ref, _ := e.uploadSeal(t, "k-deriv", body)

	// No extraction exists for this opaque binary: content is ready, text is not.
	rd, err := e.store.Readiness(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("Readiness: %v", err)
	}
	if !rd.Sealed || !rd.ContentReady || rd.Ready || len(rd.Derivations) != 0 {
		t.Fatalf("fresh sealed artifact must expose content without claiming text readiness: %+v", rd)
	}

	// A pending derivation flips readiness off.
	if err := e.store.MarkDerivation(ctx, e.scope, ref.ID,
		Derivation{Kind: "thumbnail", Status: "pending"}); err != nil {
		t.Fatalf("MarkDerivation pending: %v", err)
	}
	rd, err = e.store.Readiness(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("Readiness pending: %v", err)
	}
	if rd.Ready {
		t.Fatal("artifact with pending derivation must not be ready")
	}

	// A failed derivation keeps it not-ready too.
	if err := e.store.MarkDerivation(ctx, e.scope, ref.ID,
		Derivation{Kind: "transcript", Status: "failed", Detail: "engine down"}); err != nil {
		t.Fatalf("MarkDerivation failed: %v", err)
	}
	rd, _ = e.store.Readiness(ctx, e.scope, ref.ID)
	if rd.Ready || len(rd.Derivations) != 2 {
		t.Fatalf("expected two derivations and not ready: %+v", rd)
	}

	// Generic derivations cannot fabricate the required ingestion stages.
	sum := sha256.Sum256([]byte("thumb"))
	if err := e.store.MarkDerivation(ctx, e.scope, ref.ID, Derivation{
		Kind: "thumbnail", Status: "ready", SHA256: hex.EncodeToString(sum[:]), Size: 3,
	}); err != nil {
		t.Fatalf("MarkDerivation thumb ready: %v", err)
	}
	if err := e.store.MarkDerivation(ctx, e.scope, ref.ID,
		Derivation{Kind: "transcript", Status: "ready"}); err != nil {
		t.Fatalf("MarkDerivation transcript ready: %v", err)
	}
	rd, _ = e.store.Readiness(ctx, e.scope, ref.ID)
	if rd.Ready {
		t.Fatalf("generic stages must not bypass extraction: %+v", rd)
	}

	// Invalid derivation status is rejected before touching the store.
	if err := e.store.MarkDerivation(ctx, e.scope, ref.ID,
		Derivation{Kind: "x", Status: "weird"}); err == nil {
		t.Fatal("expected invalid status rejection")
	}
}

func TestLifecycleForeignDenied(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ref, _ := e.uploadSeal(t, "k-lc-foreign", "secret-lifecycle")
	e.foreign.Tenant = e.scope.Tenant
	// A foreign subject cannot even read lifecycle metadata of a private artifact.
	if _, err := e.store.GetLifecycle(ctx, e.foreign, ref.ID); err == nil {
		t.Fatal("foreign subject must not read lifecycle")
	}
	if err := e.store.SetLegalHold(ctx, e.foreign, ref.ID, true); err == nil {
		t.Fatal("foreign subject must not set legal hold")
	}
}

func TestRetentionLegalHoldPurge(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Legal hold blocks deletion outright.
	held, _ := e.uploadSeal(t, "k-hold", "cannot-delete-me")
	if err := e.store.SetLegalHold(ctx, e.scope, held.ID, true); err != nil {
		t.Fatalf("SetLegalHold: %v", err)
	}
	lc, err := e.store.GetLifecycle(ctx, e.scope, held.ID)
	if err != nil || !lc.LegalHold {
		t.Fatalf("expected legal hold set: %+v err=%v", lc, err)
	}
	// Deletion is refused while the hold is set.
	if err := e.store.Delete(ctx, e.scope, held.ID, "dk"); err == nil {
		t.Fatal("Delete under legal hold must fail")
	}
	if _, err := e.store.Get(ctx, e.scope, held.ID); err != nil {
		t.Fatalf("held artifact must remain readable: %v", err)
	}
	// Releasing the hold allows the tombstone again.
	if err := e.store.SetLegalHold(ctx, e.scope, held.ID, false); err != nil {
		t.Fatalf("SetLegalHold off: %v", err)
	}
	if err := e.store.Delete(ctx, e.scope, held.ID, "dk"); err != nil {
		t.Fatalf("Delete after releasing hold: %v", err)
	}

	// A second artifact exercises the retention + purge path.
	ref, _ := e.uploadSeal(t, "k-purge", "purge-me-after-retention")
	future := time.Now().UTC().Add(time.Hour)
	if err := e.store.SetRetention(ctx, e.scope, ref.ID, &future); err != nil {
		t.Fatalf("SetRetention future: %v", err)
	}
	if err := e.store.Delete(ctx, e.scope, ref.ID, "dk2"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := e.store.Get(ctx, e.scope, ref.ID); err == nil {
		t.Fatal("tombstoned artifact must be gone from reads")
	}
	// Retention floor has not elapsed.
	if err := e.store.Purge(ctx, e.scope, ref.ID); err == nil {
		t.Fatal("purge before retention floor must fail")
	}
	// Legal hold blocks purge even after the floor.
	past := time.Now().UTC().Add(-time.Hour)
	if err := e.store.SetRetention(ctx, e.scope, ref.ID, &past); err != nil {
		t.Fatalf("SetRetention past: %v", err)
	}
	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, true); err != nil {
		t.Fatalf("SetLegalHold: %v", err)
	}
	if err := e.store.Purge(ctx, e.scope, ref.ID); err == nil {
		t.Fatal("purge under legal hold must fail")
	}
	// Release hold, then purge succeeds and destroys the row.
	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, false); err != nil {
		t.Fatalf("SetLegalHold off: %v", err)
	}
	if err := e.store.Purge(ctx, e.scope, ref.ID); err != nil {
		t.Fatalf("Purge: %v", err)
	}
	if _, err := e.store.GetLifecycle(ctx, e.scope, ref.ID); err == nil {
		t.Fatal("purged artifact lifecycle must be gone")
	}
}
