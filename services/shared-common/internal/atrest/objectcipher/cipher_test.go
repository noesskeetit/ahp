//go:build ahp_data

package objectcipher

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	tenant   = "artifacts-tenant"
	sentinel = "ARTIFACT-PLAINTEXT-MARKER-5b7d"
)

// newBucket spins up a uniquely named bucket against the real lane MinIO and
// cleans it up; no neighbor bucket is touched.
func newBucket(t testing.TB) (*minio.Client, string) {
	t.Helper()
	endpoint := os.Getenv("AHP_S3_ENDPOINT")
	access := os.Getenv("AHP_MINIO_ROOT_USER")
	secret := os.Getenv("AHP_MINIO_ROOT_PASSWORD")
	if endpoint == "" || access == "" || secret == "" {
		t.Fatal("AHP_S3_ENDPOINT/AHP_MINIO_ROOT_USER/AHP_MINIO_ROOT_PASSWORD must be set")
	}
	host, secure := hostOf(endpoint)
	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(access, secret, ""),
		Region: os.Getenv("AHP_S3_REGION"),
		Secure: secure,
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	var rnd [4]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	bucket := "ahp-atrest-oc-" + hex.EncodeToString(rnd[:])
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: os.Getenv("AHP_S3_REGION")}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	t.Cleanup(func() {
		cctx, cc := context.WithTimeout(context.Background(), 30*time.Second)
		defer cc()
		for o := range client.ListObjects(cctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			_ = client.RemoveObject(cctx, bucket, o.Key, minio.RemoveObjectOptions{VersionID: o.VersionID})
		}
		_ = client.RemoveBucket(cctx, bucket)
	})
	return client, bucket
}

func hostOf(raw string) (string, bool) {
	for _, p := range []string{"https://", "http://"} {
		if len(raw) >= len(p) && raw[:len(p)] == p {
			rest := raw[len(p):]
			for i := 0; i < len(rest); i++ {
				if rest[i] == '/' {
					rest = rest[:i]
					break
				}
			}
			return rest, p == "https://"
		}
	}
	return raw, false
}

func cipherFor(t testing.TB, client *minio.Client, enabled bool) *Cipher {
	t.Helper()
	auth := atrest.NewSyntheticKeyManager()
	if _, err := auth.Provision(context.Background(), tenant, "root", nil); err != nil {
		t.Fatal(err)
	}
	codec, err := atrest.NewCodec(auth)
	if err != nil {
		t.Fatal(err)
	}
	return New(codec, client, enabled)
}

func rawBytes(t testing.TB, client *minio.Client, bucket, object string) []byte {
	t.Helper()
	obj, err := client.GetObject(context.Background(), bucket, object, minio.GetObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Close()
	b, err := io.ReadAll(obj)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Proves "no silent enable": with the profile OFF the object stored in MinIO is
// the plaintext, byte-for-byte, with no envelope — identical to today's store.
func TestDisabledPutIsPlainBytes(t *testing.T) {
	client, bucket := newBucket(t)
	c := cipherFor(t, client, false)
	body := []byte("artifact body " + sentinel)
	if _, err := c.PutObject(context.Background(), tenant, bucket, "a/obj", body, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	stored := rawBytes(t, client, bucket, "a/obj")
	if !bytes.Equal(stored, body) {
		t.Fatal("disabled profile did not store plaintext verbatim")
	}
	if atrest.IsEnvelope(stored) {
		t.Fatal("disabled profile wrote an envelope (silent enable)")
	}
	got, err := c.GetObject(context.Background(), tenant, bucket, "a/obj", 0, 0)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("disabled read: %q err=%v", got, err)
	}
}

func TestEnabledRoundtripPreservesSHAIdentity(t *testing.T) {
	client, bucket := newBucket(t)
	c := cipherFor(t, client, true)
	body := []byte("artifact body " + sentinel)
	if _, err := c.PutObject(context.Background(), tenant, bucket, "a/obj", body, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	stored := rawBytes(t, client, bucket, "a/obj")
	if bytes.Contains(stored, []byte(sentinel)) {
		t.Fatal("stored object bytes leak plaintext marker")
	}
	if !atrest.IsEnvelope(stored) {
		t.Fatal("enabled profile did not seal the object bytes")
	}
	got, err := c.GetObject(context.Background(), tenant, bucket, "a/obj", 0, 0)
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("enabled read: err=%v", err)
	}
	// The plaintext sha is what dedup/seal identity uses; it must be unchanged.
	want := sha256.Sum256(body)
	gotSHA := sha256.Sum256(got)
	if want != gotSHA {
		t.Fatal("plaintext sha identity changed after sealing")
	}
}

// Enabling encryption must never break readers of objects written beforehand.
func TestEnabledReadsLegacyPlaintext(t *testing.T) {
	client, bucket := newBucket(t)
	legacy := []byte("legacy object written before encryption " + sentinel)
	if _, err := client.PutObject(context.Background(), bucket, "a/legacy", bytes.NewReader(legacy), int64(len(legacy)), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	c := cipherFor(t, client, true)
	got, err := c.GetObject(context.Background(), tenant, bucket, "a/legacy", 0, 0)
	if err != nil || !bytes.Equal(got, legacy) {
		t.Fatalf("enabled reader failed to pass through legacy plaintext: err=%v", err)
	}
	// Range over a legacy plaintext object is honored too.
	got, err = c.GetObject(context.Background(), tenant, bucket, "a/legacy", 6, 6)
	if err != nil || string(got) != string(legacy[6:12]) {
		t.Fatalf("legacy range: %q err=%v", got, err)
	}
}

func TestRangeEquivalenceDisabledVsEnabled(t *testing.T) {
	client, bucket := newBucket(t)
	body := []byte("0123456789ABCDEFGHIJ" + sentinel)
	off, cnt := int64(3), int64(10)
	want := string(body[off : off+cnt])

	cOff := cipherFor(t, client, false)
	if _, err := cOff.PutObject(context.Background(), tenant, bucket, "off", body, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	cOn := cipherFor(t, client, true)
	if _, err := cOn.PutObject(context.Background(), tenant, bucket, "on", body, minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	offGot, _ := cOff.GetObject(context.Background(), tenant, bucket, "off", off, cnt)
	onGot, _ := cOn.GetObject(context.Background(), tenant, bucket, "on", off, cnt)
	if string(offGot) != want || string(onGot) != want {
		t.Fatalf("range mismatch: off=%q on=%q want=%q", offGot, onGot, want)
	}
}

func TestWrongTenantRejected(t *testing.T) {
	client, bucket := newBucket(t)
	c := cipherFor(t, client, true)
	if _, err := c.PutObject(context.Background(), tenant, bucket, "a/obj", []byte("secret "+sentinel), minio.PutObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetObject(context.Background(), "other-tenant", bucket, "a/obj", 0, 0); !errors.Is(err, atrest.ErrTenantMismatch) {
		t.Fatalf("want ErrTenantMismatch, got %v", err)
	}
}

func TestEnabledFromEnvDefaultsOff(t *testing.T) {
	t.Setenv(EnvArtifactEncryption, "")
	if EnabledFromEnv() {
		t.Fatal("unset toggle must be OFF")
	}
	t.Setenv(EnvArtifactEncryption, "true")
	if !EnabledFromEnv() {
		t.Fatal("true must enable")
	}
	t.Setenv(EnvArtifactEncryption, "maybe")
	if EnabledFromEnv() {
		t.Fatal("non-truthy value must stay OFF")
	}
}

func BenchmarkObjectPutGet1MiBDisabled(b *testing.B) { benchPutGet(b, 1<<20, false) }
func BenchmarkObjectPutGet1MiBEnabled(b *testing.B)  { benchPutGet(b, 1<<20, true) }
func BenchmarkObjectPutGet100MiBDisabled(b *testing.B) {
	if testing.Short() {
		b.Skip("skip 100 MiB in -short")
	}
	benchPutGet(b, 100<<20, false)
}
func BenchmarkObjectPutGet100MiBEnabled(b *testing.B) {
	if testing.Short() {
		b.Skip("skip 100 MiB in -short")
	}
	benchPutGet(b, 100<<20, true)
}

func benchPutGet(b *testing.B, size int, enabled bool) {
	client, bucket := newBucket(b)
	c := cipherFor(b, client, enabled)
	body := make([]byte, size)
	ctx := context.Background()
	object := "bench/obj"
	b.SetBytes(int64(size))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.PutObject(ctx, tenant, bucket, object, body, minio.PutObjectOptions{}); err != nil {
			b.Fatal(err)
		}
		if _, err := c.GetObject(ctx, tenant, bucket, object, 0, 0); err != nil {
			b.Fatal(err)
		}
	}
}
