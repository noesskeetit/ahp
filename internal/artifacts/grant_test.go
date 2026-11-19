package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// reasonIs pins the pair a client branches on: the contract code decides the
// HTTP class, the reason names the domain event. Acceptance measures these two
// fields, never message text.
func reasonIs(t *testing.T, err error, wantCode, wantReason string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want %s/%s, got nil", wantCode, wantReason)
	}
	if got := contracts.Code(err); got != wantCode {
		t.Fatalf("code = %q (err %v), want %s", got, err, wantCode)
	}
	if got := contracts.ReasonOf(err); got != wantReason {
		t.Fatalf("reason = %q (err %v), want %s", got, err, wantReason)
	}
}

// uploadSealLabeled is uploadSeal with an explicit label set: the ceiling tests
// need an artifact whose labels are narrower than another subject's read.
func uploadSealLabeled(t *testing.T, e *env, key, body string, set labels.Set) ArtifactRef {
	t.Helper()
	sum := sha256.Sum256([]byte(body))
	up, err := e.store.BeginUpload(context.Background(), e.scope, key, UploadRequest{
		Name: "g.bin", MIME: "application/octet-stream",
		SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)), Labels: set,
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	ref, err := e.store.Seal(context.Background(), e.scope, up.ID)
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return ref
}

// ageGrant moves one grant past its expiry in the database, because a test that
// slept out a real TTL would be slow and a test that faked the clock would not
// have measured the SQL predicate that decides expiry.
func ageGrant(t *testing.T, digest string) {
	t.Helper()
	pool := isolatedPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	// Both ends move: the table CHECKs that a grant window points forward, and
	// that invariant is worth keeping, so an aged grant is "issued two hours
	// ago, expired a second ago" rather than a row with a backwards window.
	if _, err := pool.Exec(ctx, `
UPDATE ahp_artifacts.download_grants
   SET issued_at = now() - interval '2 hours', expires_at = now() - interval '1 second'
 WHERE digest = $1`, digest); err != nil {
		t.Fatalf("age grant: %v", err)
	}
}

func grantDigestOf(t *testing.T, token string) string {
	t.Helper()
	digest, err := grantDigest(token)
	if err != nil {
		t.Fatalf("grantDigest of an issued token: %v", err)
	}
	return digest
}

// TestDownloadGrantStreamsExactVersionOnce is the positive control of the
// contract: the bytes that come back are the sealed version, and a one-time
// grant answers a second consumption with a machine-readable refusal.
func TestDownloadGrantStreamsExactVersionOnce(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "grant exact version bytes\n"
	ref := uploadSealLabeled(t, e, "k-"+t.Name(), body, labels.Set{})

	grant, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID, GrantRequest{Version: ref.Version})
	if err != nil {
		t.Fatalf("MintDownloadGrant: %v", err)
	}
	if grant.ArtifactID != ref.ID || grant.Version != ref.Version || grant.SHA256 != ref.SHA256 {
		t.Fatalf("grant pins %+v, want artifact %s version %s sha %s",
			grant, ref.ID, ref.Version, ref.SHA256)
	}
	if !grant.OneTime || grant.UsesMax != 1 {
		t.Fatalf("default grant is not one-time: %+v", grant)
	}
	if !grant.ExpiresAt.After(grant.IssuedAt) || grant.ExpiresAt.Sub(grant.IssuedAt) > MaxGrantTTL {
		t.Fatalf("grant lifetime = %v..%v, want inside the ceiling", grant.IssuedAt, grant.ExpiresAt)
	}

	rc, got, err := e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 0, 0)
	if err != nil {
		t.Fatalf("OpenGrantedRange: %v", err)
	}
	if got.ID != ref.ID || got.Version != ref.Version || got.SHA256 != ref.SHA256 {
		t.Fatalf("granted ref = %+v, want the pinned version", got)
	}
	readAll(t, rc, body)

	_, _, err = e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 0, 0)
	reasonIs(t, err, contracts.ErrForbidden, ReasonGrantUsed)
}

// TestDownloadGrantHonoursRange: a granted read is a proxy pipe, so Range must
// behave exactly as it does on Download, and a refusal must not spend the use.
func TestDownloadGrantHonoursRange(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "0123456789abcdefghij\n"
	ref := uploadSealLabeled(t, e, "k-"+t.Name(), body, labels.Set{})
	grant, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID, GrantRequest{Version: ref.Version, Uses: 2})
	if err != nil {
		t.Fatalf("MintDownloadGrant: %v", err)
	}
	rc, _, err := e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 4, 6)
	if err != nil {
		t.Fatalf("ranged OpenGrantedRange: %v", err)
	}
	readAll(t, rc, "456789")

	// Past the end: 416-shaped error, and the second use is still available.
	if _, _, err := e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, ref.Size+10, 0); err == nil {
		t.Fatal("a range past the end must not stream")
	} else if !errors.Is(err, ErrRangeNotSatisfiable) {
		t.Fatalf("err = %v, want ErrRangeNotSatisfiable", err)
	}
	rc, _, err = e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 0, 0)
	if err != nil {
		t.Fatalf("use after a refused range: %v", err)
	}
	readAll(t, rc, body)

	_, _, err = e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 0, 0)
	reasonIs(t, err, contracts.ErrForbidden, ReasonGrantUsed)
}

// TestDownloadGrantRefusesUnpinnedVersion is the "точная версия" half of the
// contract: a grant never means "whatever is current".
func TestDownloadGrantRefusesUnpinnedVersion(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ref := uploadSealLabeled(t, e, "k-"+t.Name(), "pinned\n", labels.Set{})

	for _, tc := range []struct{ name, version string }{
		{"empty version", ""},
		{"latest keyword", "latest"},
		{"LATEST keyword", "LATEST"},
		{"a version that is not sealed here", "00000000-0000-0000-0000-000000000000"},
	} {
		_, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID, GrantRequest{Version: tc.version})
		if err == nil {
			t.Fatalf("%s: mint succeeded, want refusal", tc.name)
		}
		want := ReasonGrantVersionRequired
		switch tc.name {
		case "a version that is not sealed here":
			want = ReasonGrantVersionMismatch
		}
		code := contracts.ErrInvalid
		if want == ReasonGrantVersionMismatch {
			code = contracts.ErrForbidden
		}
		reasonIs(t, err, code, want)
	}
}

// TestDownloadGrantRecipientIsTheCaller is negative leg (a) of the contract: a
// grant moves bytes to a NAMED subject, so naming somebody else is refused with
// a machine-readable reason, and a grant issued to one subject cannot be
// redeemed by another.
func TestDownloadGrantRecipientIsTheCaller(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "recipient bound\n"
	ref := uploadSealLabeled(t, e, "k-"+t.Name(), body, labels.Set{Subjects: []string{"alice"}})

	if _, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID,
		GrantRequest{Version: ref.Version, Recipient: "someone-else"}); err == nil {
		t.Fatal("a grant to another subject must be refused")
	} else {
		reasonIs(t, err, contracts.ErrForbidden, ReasonGrantRecipientMismatch)
	}

	grant, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID, GrantRequest{Version: ref.Version})
	if err != nil {
		t.Fatalf("MintDownloadGrant: %v", err)
	}
	// The same artifact, same tenant, another subject: the label ceiling and the
	// recipient binding both refuse it, and neither answer is a 200.
	if _, err := e.store.MintDownloadGrant(ctx, e.foreign, ref.ID,
		GrantRequest{Version: ref.Version}); err == nil {
		t.Fatal("another subject may not mint a grant for an artifact private to alice")
	} else if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign mint code = %q, want FORBIDDEN", contracts.Code(err))
	}
	if _, _, err := e.store.OpenGrantedRange(ctx, e.foreign, grant.Grant, 0, 0); err == nil {
		t.Fatal("another subject may not redeem alice's grant")
	} else {
		reasonIs(t, err, contracts.ErrForbidden, ReasonGrantRecipientMismatch)
	}
}

// TestDownloadGrantLifecycleRefusals is negative leg (d): rights are evaluated
// at issue AND at consumption, so a tombstone or a legal hold stops the download
// even when the grant was issued before it.
func TestDownloadGrantLifecycleRefusals(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "lifecycle gated\n"
	ref := uploadSealLabeled(t, e, "k-"+t.Name(), body, labels.Set{})

	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, true); err != nil {
		t.Fatalf("SetLegalHold: %v", err)
	}
	_, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID, GrantRequest{Version: ref.Version})
	reasonIs(t, err, contracts.ErrForbidden, ReasonArtifactLegalHold)
	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, false); err != nil {
		t.Fatalf("SetLegalHold off: %v", err)
	}

	grant, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID, GrantRequest{Version: ref.Version})
	if err != nil {
		t.Fatalf("MintDownloadGrant: %v", err)
	}
	// A hold landed after issuance: the consumption re-checks and refuses.
	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, true); err != nil {
		t.Fatalf("SetLegalHold on: %v", err)
	}
	_, _, err = e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 0, 0)
	reasonIs(t, err, contracts.ErrForbidden, ReasonArtifactLegalHold)
	if err := e.store.SetLegalHold(ctx, e.scope, ref.ID, false); err != nil {
		t.Fatalf("SetLegalHold off: %v", err)
	}

	// A tombstone answers like every other read of a deleted artifact, and a
	// grant issued before the deletion stops working with it.
	if err := e.store.Delete(ctx, e.scope, ref.ID, "k-delete-"+t.Name()); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, _, err = e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 0, 0)
	reasonIs(t, err, contracts.ErrNotFound, ReasonArtifactDeleted)
	if _, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID,
		GrantRequest{Version: ref.Version}); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("mint on a tombstone code = %q (err %v), want NOT_FOUND", contracts.Code(err), err)
	}
}

// TestDownloadGrantExpiresIsDistinctFromUsed is negative leg (b): an expired and
// a spent grant are two different machine-readable answers, and a malformed or
// unknown token says nothing more than "not valid".
func TestDownloadGrantExpiresIsDistinctFromUsed(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	body := "expiry is measurable\n"
	ref := uploadSealLabeled(t, e, "k-"+t.Name(), body, labels.Set{})
	grant, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID,
		GrantRequest{Version: ref.Version, TTL: MinGrantTTL})
	if err != nil {
		t.Fatalf("MintDownloadGrant: %v", err)
	}
	ageGrant(t, grantDigestOf(t, grant.Grant))
	_, _, err = e.store.OpenGrantedRange(ctx, e.scope, grant.Grant, 0, 0)
	reasonIs(t, err, contracts.ErrForbidden, ReasonGrantExpired)

	for _, bad := range []string{"", "not-a-grant", grant.Grant[:len(grant.Grant)-2]} {
		_, _, err := e.store.OpenGrantedRange(ctx, e.scope, bad, 0, 0)
		reasonIs(t, err, contracts.ErrForbidden, ReasonGrantUnknown)
	}
}

// TestDownloadGrantTTLIsClamped: the ceiling is a policy of this service, not a
// caller preference, so an over-long request is cut rather than honoured.
func TestDownloadGrantTTLIsClamped(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ref := uploadSealLabeled(t, e, "k-"+t.Name(), "ttl\n", labels.Set{})

	long, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID,
		GrantRequest{Version: ref.Version, TTL: 30 * 24 * time.Hour})
	if err != nil {
		t.Fatalf("MintDownloadGrant: %v", err)
	}
	if got := long.ExpiresAt.Sub(long.IssuedAt); got != MaxGrantTTL {
		t.Fatalf("lifetime = %v, want the ceiling %v", got, MaxGrantTTL)
	}
	if _, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID,
		GrantRequest{Version: ref.Version, TTL: time.Millisecond}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("a sub-second grant code = %q (err %v), want INVALID_ARGUMENT", contracts.Code(err), err)
	}
	if _, err := e.store.MintDownloadGrant(ctx, e.scope, ref.ID,
		GrantRequest{Version: ref.Version, Uses: MaxGrantUses + 1}); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("an over-counted grant code = %q (err %v), want INVALID_ARGUMENT", contracts.Code(err), err)
	}
}
