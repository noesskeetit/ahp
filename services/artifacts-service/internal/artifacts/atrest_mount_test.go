package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
)

// mountCipher installs a SYNTHETIC dynamic authority over the store's own
// client and segment size, so a test drives the actual mounted put/get path
// rather than calling the seam directly.
func mountCipher(e *env, segSize int64) {
	e.store.cipher = objectcipher.New(mustCodec(), e.client, true).WithSegmentSize(segSize)
}

func mustCodec() *atrest.Codec {
	auth := atrest.NewDynamicAuthority(bytes.Repeat([]byte("r"), 32)) // 32-byte root for deriveKEK
	codec, err := atrest.NewCodec(auth)
	if err != nil {
		panic(err)
	}
	return codec
}

func rawVersion(t *testing.T, e *env, object, version string) []byte {
	t.Helper()
	rc, err := e.client.GetObject(context.Background(), e.bucket, object, minio.GetObjectOptions{VersionID: version})
	if err != nil {
		t.Fatalf("raw get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("raw read: %v", err)
	}
	return b
}

// Envelope-on-write (byte-level regression): with the mount on, the bytes that
// survive in MinIO after Seal are at-rest envelopes, never the plaintext, for
// both a single-segment and a multi-segment object.
func TestAtrestMountEnvelopesStoredBytes(t *testing.T) {
	e := setup(t)
	mountCipher(e, 64<<10)
	ctx := context.Background()

	small := strings.Repeat("s", 3<<10)
	refS, upS := e.uploadSeal(t, "mount-small", small)
	rawS := rawVersion(t, e, upS.Object, refS.Version)
	if string(rawS) == small {
		t.Fatal("stored bytes are plaintext (single-segment)")
	}
	if !atrest.IsEnvelope(rawS) {
		t.Fatal("single-segment stored bytes are not an envelope")
	}

	large := strings.Repeat("L", 300<<10)
	refL, upL := e.uploadSeal(t, "mount-large", large)
	rawL := rawVersion(t, e, upL.Object, refL.Version)
	if bytes.Contains(rawL, []byte("LLLLLLLL")) {
		t.Fatal("multi-segment stored bytes contain plaintext run")
	}
	if !atrest.IsEnvelope(rawL) {
		t.Fatal("multi-segment stored bytes are not envelope-backed")
	}

	rc, _, err := e.store.Open(ctx, e.scope, refL.ID)
	if err != nil {
		t.Fatalf("open large: %v", err)
	}
	readAll(t, rc, large)

	rng, _, err := e.store.OpenRange(ctx, e.scope, refL.ID, 100<<10, 40<<10)
	if err != nil {
		t.Fatalf("open range: %v", err)
	}
	readAll(t, rng, large[100<<10:140<<10])
}

// Dual-read: legacy plaintext objects (written before the mount) open and
// range-read through a mounted store; the mount never invents a second copy.
func TestAtrestMountDualReadPlaintext(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	body := strings.Repeat("legacy-plaintext-", 40)
	ref, up := e.uploadSeal(t, "legacy-obj", body) // cipher nil: plaintext stored
	if string(rawVersion(t, e, up.Object, ref.Version)) != body {
		t.Fatal("baseline: plaintext not stored as written")
	}
	mountCipher(e, 64<<10) // now mount is ON
	rc, _, err := e.store.Open(ctx, e.scope, ref.ID)
	if err != nil {
		t.Fatalf("open legacy under mount: %v", err)
	}
	readAll(t, rc, body)
	rng, _, err := e.store.OpenRange(ctx, e.scope, ref.ID, 5, 10)
	if err != nil {
		t.Fatalf("range legacy under mount: %v", err)
	}
	readAll(t, rng, body[5:15])
}

// No plaintext version survives a sealed upload: every version id under the
// object key is an envelope (the plaintext candidate version was retired).
func TestAtrestMountNoPlaintextAfterSeal(t *testing.T) {
	e := setup(t)
	mountCipher(e, 64<<10)
	body := strings.Repeat("secret-", 5000)
	ref, up := e.uploadSeal(t, "mount-retire", body)
	ctx := context.Background()
	for obj := range e.client.ListObjects(ctx, e.bucket, minio.ListObjectsOptions{Prefix: up.Object, Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			t.Fatalf("list: %v", obj.Err)
		}
		if obj.Key != up.Object {
			continue
		}
		raw := rawVersion(t, e, obj.Key, obj.VersionID)
		if string(raw) == body {
			t.Fatalf("plaintext version %s survived seal", obj.VersionID)
		}
		if !atrest.IsEnvelope(raw) {
			t.Fatalf("version %s is neither plaintext nor envelope", obj.VersionID)
		}
	}
	if ref.Version == "" {
		t.Fatal("sealed ref missing version")
	}
}

// Dedup: the sealed bytes land on the SAME object path the upload reserved;
// seal-transform never creates a second object key.
func TestAtrestMountSealKeepsSameObject(t *testing.T) {
	e := setup(t)
	mountCipher(e, 64<<10)
	body := strings.Repeat("same-path-", 200)
	_, up := e.uploadSeal(t, "mount-samepath", body)
	keys := map[string]bool{}
	for obj := range e.client.ListObjects(context.Background(), e.bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("list: %v", obj.Err)
		}
		keys[obj.Key] = true
	}
	if len(keys) != 1 || !keys[up.Object] {
		t.Fatalf("seal wrote a second object: %v", keys)
	}
}

// Fail-closed: with no provisionable key authority the mounted Seal refuses and
// the artifact stays unsealed - plaintext is never bound behind an ON profile.
func TestAtrestMountFailClosedWithoutKey(t *testing.T) {
	e := setup(t)
	// SyntheticKeyManager with no Provisioned tenant: Current(tenant) errors.
	empty, cerr := atrest.NewCodec(atrest.NewSyntheticKeyManager())
	if cerr != nil {
		t.Fatalf("codec: %v", cerr)
	}
	e.store.cipher = objectcipher.New(empty, e.client, true).WithSegmentSize(64 << 10)
	body := "cannot-seal-me"
	sum := sha256.Sum256([]byte(body))
	up, err := e.store.BeginUpload(context.Background(), e.scope, "mount-failclosed", UploadRequest{
		Name: "a.bin", MIME: "application/octet-stream", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body)),
	})
	if err != nil {
		t.Fatalf("BeginUpload: %v", err)
	}
	put(t, up.PresignedURL, body)
	if _, err := e.store.Seal(context.Background(), e.scope, up.ID); err == nil {
		t.Fatal("Seal must fail closed without a key authority")
	}
}
