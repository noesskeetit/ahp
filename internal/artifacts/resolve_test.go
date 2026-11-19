package artifacts

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The contract under test is the one the human edge needs and the PDF allows:
// "download by digest" is Resolve + the version-exact download, so a lookup must
// answer id + exact sealed version and nothing else. The negative half matters more
// than the positive one: a resolver that distinguishes "no such digest" from "yours
// to see" turns a read-only route into an existence oracle over the whole tenant.

func resolveDigestOf(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func randomDigest() string {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

func TestResolveByDigestReturnsExactSealedVersion(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body := "goodall resolve: the bytes a run frame named"
	ref, _ := e.uploadSeal(t, "k-resolve-"+t.Name(), body)

	got, err := e.store.ResolveByDigest(ctx, e.scope, resolveDigestOf(body))
	if err != nil {
		t.Fatalf("ResolveByDigest: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("want exactly 1 version, got %d", len(got))
	}
	v := got[0]
	if v.ArtifactID != ref.ID || v.Version != ref.Version {
		t.Fatalf("resolve must name the same id+version a read does: got %s/%s want %s/%s",
			v.ArtifactID, v.Version, ref.ID, ref.Version)
	}
	if v.SHA256 != resolveDigestOf(body) || v.Size != int64(len(body)) {
		t.Fatalf("resolve disagrees with the seal: sha=%s size=%d", v.SHA256, v.Size)
	}
	if v.Name != "a.bin" || v.MIME != "application/octet-stream" {
		t.Fatalf("resolve lost the display fields a download card needs: name=%q mime=%q", v.Name, v.MIME)
	}

	// Out-of-band proof that the version Resolve named is a real sealed object
	// version, checked against MinIO rather than against the store's own answer.
	found := false
	for obj := range e.client.ListObjects(ctx, e.bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			t.Fatalf("ListObjects: %v", obj.Err)
		}
		if obj.VersionID == ref.Version {
			found = true
		}
	}
	if !found {
		t.Fatalf("resolved version %s is not an object version in the bucket", ref.Version)
	}
	rc, _, err := e.store.Open(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	readAll(t, rc, body)
}

func TestResolveAnswerCarriesNoContentBearingFields(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body := "goodall resolve shape"
	e.uploadSeal(t, "k-resolve-shape-"+t.Name(), body)

	got, err := e.store.ResolveByDigest(ctx, e.scope, resolveDigestOf(body))
	if err != nil {
		t.Fatalf("ResolveByDigest: %v", err)
	}
	raw, err := json.Marshal(got)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var items []map[string]any
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("want 1 item, got %d", len(items))
	}
	allowed := map[string]bool{"artifact_id": true, "version": true, "sha256": true,
		"size": true, "name": true, "mime": true}
	for k := range items[0] {
		if !allowed[k] {
			t.Fatalf("resolve returned a field no download needs: %q", k)
		}
	}
	for _, banned := range []string{"object", "bucket", "label", "task", "tenant", "subject", "prompt", "content"} {
		if strings.Contains(strings.ToLower(string(raw)), banned) {
			t.Fatalf("resolve answer mentions %q: %s", banned, raw)
		}
	}
}

// The oracle test: the same digest, admitted and not admitted, must be
// indistinguishable - same code, same message, same everything.
func TestResolveNotAdmittedIsIndistinguishableFromUnknown(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body := "goodall resolve oracle"
	// Seed the row while the policy admits, then withdraw admission and ask again.
	ref, _ := e.uploadSeal(t, "k-oracle-seed-"+t.Name(), body)
	digest := resolveDigestOf(body)

	e.allow.Store(false)
	_, errDenied := e.store.ResolveByDigest(ctx, e.scope, digest)
	_, errUnknown := e.store.ResolveByDigest(ctx, e.scope, randomDigest())
	codeIs(t, errDenied, contracts.ErrNotFound)
	codeIs(t, errUnknown, contracts.ErrNotFound)
	if errDenied.Error() != errUnknown.Error() {
		t.Fatalf("resolver distinguishes hidden from absent: %q vs %q", errDenied, errUnknown)
	}
	// And it was rights, not an empty fixture: re-admit and the digest resolves.
	e.allow.Store(true)
	if got, err := e.store.ResolveByDigest(ctx, e.scope, digest); err != nil || len(got) != 1 || got[0].ArtifactID != ref.ID {
		t.Fatalf("after re-admitting the same digest must resolve: %d results, err=%v", len(got), err)
	}
}

// A second tenant must not be able to feel the first tenant's digests at all.
func TestResolveIsTenantScoped(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body := "goodall resolve cross-tenant"
	e.uploadSeal(t, "k-xt-"+t.Name(), body)
	other := contracts.Scope{Tenant: "t-not-" + e.scope.Tenant[2:], Deployment: "d1", Subject: "carol"}
	got, err := e.store.ResolveByDigest(ctx, other, resolveDigestOf(body))
	if err == nil {
		t.Fatalf("cross-tenant resolve succeeded with %d results: existence leak across tenants", len(got))
	}
	codeIs(t, err, contracts.ErrNotFound)
}

func TestResolveRefusesMalformedQuestions(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, tc := range []struct {
		name   string
		digest string
		reason string
	}{
		{"empty", "", ReasonDigestRequired},
		{"too short", "0123abcd", ""},
		{"uppercase", strings.Repeat("A", 64), ""},
		{"non hex", strings.Repeat("z", 64), ""},
		{"path traversal", "../../etc/passwd", ""},
		{"sql shaped", "' OR 1=1 --", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.store.ResolveByDigest(ctx, e.scope, tc.digest)
			codeIs(t, err, contracts.ErrInvalid)
			if tc.reason != "" {
				if got := contracts.ReasonOf(err); got != tc.reason {
					t.Fatalf("want reason %q, got %q", tc.reason, got)
				}
			}
		})
	}
}

// Deleting the conversation must not leave the digest resolvable: the tombstone wins.
func TestResolveFollowsTombstone(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	body := "goodall resolve tombstone"
	key := "k-tomb-" + t.Name()
	ref, up := e.uploadSeal(t, key, body)
	digest := resolveDigestOf(body)

	if got, err := e.store.ResolveByDigest(ctx, e.scope, digest); err != nil || len(got) != 1 {
		t.Fatalf("resolve before delete: %d results, err=%v", len(got), err)
	}
	if err := e.store.Delete(ctx, e.scope, ref.ID, up.Object); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := e.store.ResolveByDigest(ctx, e.scope, digest)
	codeIs(t, err, contracts.ErrNotFound)
	if err.Error() != contracts.NewError(contracts.ErrNotFound, "artifact not found").Error() {
		t.Fatalf("tombstoned answer differs from absent answer: %q", err)
	}
}
