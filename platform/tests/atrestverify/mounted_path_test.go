package atrestverify_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
)

// L1 - after Seal the bytes that live in the bucket are not the plaintext.
//
// The claim under test is byte-level, so it is measured at the bucket: read one exact object
// version directly with the object client (no product code in the read path) and compare digests.
// A flag in a config file or an "enabled" field in an artifact is not evidence of encryption.
func TestL1StoredBytesAreNotPlaintext(t *testing.T) {
	l := startLeg(t, "L1_envelope_on_write_bytes")
	sizes := []struct {
		name string
		n    int
	}{
		{"1KiB", 1 << 10},
		{"64KiB", 64 << 10},
		{"1MiB+1", int(objectcipher.DefaultSegmentSize) + 1}, // forces the multi-segment format
		{"4MiB", 4 << 20},
	}
	results := map[string]any{}
	for _, s := range sizes {
		plain := bodyOf(s.n)
		v := newEnv(t, true, 0) // product default segment size
		ref, up := v.uploadSeal(t, v.store, "l1-"+s.name, string(plain))
		raw := v.rawGetVersion(t, up.Object, ref.Version)
		if len(raw) == 0 {
			t.Fatalf("%s: stored object is empty", s.name)
		}
		if bytes.Equal(raw, plain) {
			t.Fatalf("%s: stored bytes are the plaintext", s.name)
		}
		if shaHex(raw) == shaHex(plain) {
			t.Fatalf("%s: stored digest equals the plaintext digest", s.name)
		}
		if bytes.Contains(raw, plain[:min(len(plain), 4096)]) {
			t.Fatalf("%s: stored bytes contain a plaintext run", s.name)
		}
		if !atrest.IsEnvelope(raw) {
			t.Fatalf("%s: stored head is not an envelope (no envelope header at offset 0)", s.name)
		}
		got := readAllBytes(t, mustOpen(t, v.store, v, ref.ID))
		if !bytes.Equal(got, plain) {
			t.Fatalf("%s: Open did not return the plaintext (%d of %d bytes)", s.name, len(got), len(plain))
		}
		results[s.name] = map[string]any{
			"plain_len": len(plain), "stored_len": len(raw),
			"overhead_bytes": len(raw) - len(plain),
			"plain_sha256":   shaHex(plain), "stored_sha256": shaHex(raw),
			"envelope_header_at_offset0": true, "open_roundtrip_exact": true,
			"pg_sha256_equals_plaintext": ref.SHA256 == shaHex(plain),
			"segmented":                  int64(len(plain)) > objectcipher.DefaultSegmentSize,
		}
		if ref.SHA256 != shaHex(plain) {
			t.Fatalf("%s: PG identity is not the plaintext digest", s.name)
		}
	}
	l.set("sizes", results)
	l.set("level", "store (BeginUpload/Seal/Open) + direct bucket read of the sealed version")
	l.set("header_evidence", "atrest.IsEnvelope over the first bytes of the stored object, not a config flag")
}

// L2a - dual-read: an object sealed while the mount was OFF still opens after it is ON.
//
// This is the migration property: turning at-rest encryption on must not orphan the bytes that
// are already in the bucket. Same bucket, same tenant, same PG row, mount flipped between write
// and read - and the read handle is a second Store built through the exported option.
func TestL2aLegacyPlaintextObjectStillOpens(t *testing.T) {
	l := startLeg(t, "L2a_dual_read_legacy_plaintext")
	plain := bodyOf(200 << 10)
	v := newEnv(t, false, 0)
	ref, up := v.uploadSeal(t, v.offStore, "l2a-legacy", string(plain))
	raw := v.rawGetVersion(t, up.Object, ref.Version)
	if !bytes.Equal(raw, plain) {
		t.Fatalf("with the mount OFF the stored bytes must be the plaintext")
	}
	l.set("stored_bytes", "plaintext (sha256 equal)")
	l.set("stored_sha256", shaHex(raw))

	if v.onStore == nil {
		t.Fatal("harness did not build the mounted handle")
	}
	got := readAllBytes(t, mustOpen(t, v.onStore, v, ref.ID))
	if !bytes.Equal(got, plain) {
		t.Fatalf("legacy plaintext object did not open through the mounted handle")
	}
	l.set("opened_through_mounted_handle", true)
	l.set("mechanism", "atrest.IsEnvelope=false at read time -> native plaintext path (OpenTransparent)")
}

// L2b - dual-read: a v1 single-envelope object opens and ranges correctly.
//
// A body at or below one segment is stored as one v1 envelope, whose documented mechanism limit
// is "range = full read + slice". The leg pins that the limit is a performance property and not a
// correctness one: the bytes must still be exactly right.
func TestL2bV1EnvelopeObjectOpensAndRanges(t *testing.T) {
	l := startLeg(t, "L2b_dual_read_v1_envelope")
	plain := bodyOf(64 << 10)
	v := newEnv(t, true, 0)
	ref, up := v.uploadSeal(t, v.store, "l2b-envelope", string(plain))
	raw := v.rawGetVersion(t, up.Object, ref.Version)
	if !atrest.IsEnvelope(raw) {
		t.Fatalf("expected a v1 envelope head")
	}
	if int64(len(plain)) > objectcipher.DefaultSegmentSize {
		t.Fatal("fixture is larger than the segment size, this leg tests the single-envelope path")
	}
	if got := readAllBytes(t, mustOpen(t, v.store, v, ref.ID)); !bytes.Equal(got, plain) {
		t.Fatalf("v1 envelope did not open to the plaintext")
	}
	rc, _, err := v.store.OpenRange(contextOr(t), v.scope, ref.ID, 1000, 4096)
	if err != nil {
		t.Fatalf("OpenRange on a v1 envelope: %v", err)
	}
	got := readAllBytes(t, rc)
	if !bytes.Equal(got, plain[1000:1000+4096]) {
		t.Fatalf("v1 envelope range returned wrong bytes")
	}
	l.set("stored_len", len(raw))
	l.set("stored_sha256", shaHex(raw))
	l.set("range_offset", 1000)
	l.set("range_count", 4096)
	l.set("range_exact", true)
	l.set("mechanism", "v1 envelope: documented full-read + slice, verified byte-exact")
}

// L2c - a corrupted segment must fail closed, never splice.
//
// The dangerous failure mode of a segmented reader is answering with a mix of verified and
// unverified segments, or with the neighbouring segment's bytes. This leg writes one object with
// the product's own exported seam (objectcipher.PutObjectStream), flips one byte inside the second
// segment's ciphertext, and demands that every read shape that touches that segment refuses.
// Range-level: the range NOT touching the broken segment must still work (granularity), and the
// range touching it must refuse.
func TestL2cCorruptedSegmentFailsClosed(t *testing.T) {
	l := startLeg(t, "L2c_corrupted_segment_fails_closed")
	client, tenant, cipher, bucket := ownCipher(t, 0)
	plain := bodyOf(3 * 1 << 20) // three product-default segments
	object := "verify/l2c-" + runIdentValue()
	info, err := cipher.PutObjectStream(contextOr(t), tenant, bucket, object, bytes.NewReader(plain), int64(len(plain)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("PutObjectStream: %v", err)
	}
	version := info.VersionID
	raw := rawBucket(t, client, bucket, object, version)
	if bytes.Equal(raw, plain) {
		t.Fatal("the seam stored plaintext")
	}
	if len(raw) <= (2<<20)/2 {
		t.Fatalf("object is not as large as expected: %d bytes", len(raw))
	}
	broken := append([]byte(nil), raw...)
	target := int64(len(broken) / 2) // inside the second segment's ciphertext
	broken[target] ^= 0xFF
	corruptObject := "verify/l2c-corrupt-" + runIdentValue()
	corruptVersion := putBucket(t, client, bucket, corruptObject, broken)

	// Whole-object read must never answer successfully. Refusing at Open is one honest shape;
	// streaming the verified prefix and then refusing at the broken segment is another. What is NOT
	// acceptable is a nil-error complete answer, because a caller cannot tell a spliced body from a
	// whole one after the fact.
	stage := "open"
	delivered := 0
	if rc, err := cipher.OpenReader(contextOr(t), tenant, bucket, corruptObject, corruptVersion); err == nil {
		stage = "read"
		var readErr error
		delivered, readErr = readAllAllowError2(rc)
		rc.Close()
		if readErr == nil {
			t.Fatalf("a corrupted segment produced a successful whole-object read of %d bytes", delivered)
		}
		if !bytes.Equal(deliveredPrefix(delivered, plain), plain[:delivered]) {
			t.Fatalf("the bytes delivered before the refusal are not the plaintext prefix: a splice was served")
		}
	}
	l.set("whole_object_refused_at", stage)
	l.set("bytes_delivered_before_refusal", delivered)
	// Range touching the broken segment must refuse.
	if _, err := cipher.GetRange(contextOr(t), tenant, bucket, corruptObject, corruptVersion, target, 4096); err == nil {
		t.Fatalf("GetRange across the corrupted segment returned bytes instead of refusing")
	}
	// A range inside the first, untouched segment must still open: fail closed is per segment,
	// not an excuse to lose the whole object.
	if got, err := cipher.GetRange(contextOr(t), tenant, bucket, corruptObject, corruptVersion, 0, 4096); err != nil {
		t.Fatalf("an untouched segment must still read: %v", err)
	} else if !bytes.Equal(got, plain[:4096]) {
		t.Fatalf("untouched segment returned wrong bytes")
	}
	// And a clean object of the same size reads whole and exactly, so the refusal above cannot be
	// explained by the fixture being broken.
	if got, err := cipher.GetRange(contextOr(t), tenant, bucket, object, version, 0, int64(len(plain))); err != nil {
		t.Fatalf("clean object range read: %v", err)
	} else if !bytes.Equal(got, plain) {
		t.Fatalf("clean object did not return the full plaintext")
	}
	l.set("corrupt_offset_in_ciphertext", target)
	l.set("segment_size", objectcipher.DefaultSegmentSize)
	l.set("open_reader_refused", true)
	l.set("range_across_corruption_refused", true)
	l.set("untouched_segment_still_reads", true)
	l.set("clean_control_exact", true)
	l.set("level", "objectcipher exported seam (PutObjectStream/GetRange/OpenReader)")
}

// L2e - a damaged stored object must never be answered as a shorter SUCCESS on the product path.
//
// Root cause this leg pins: the segmented reader recognises its own format by a footer magic
// (objectcipher/segmented.go: loadManifest returns errNotSegmented when the magic is gone), and on
// errNotSegmented both OpenReader and GetRange fall back to "read the object as one envelope, then
// slice". For a segmented object whose footer was destroyed the head of the object IS a valid
// envelope - of the FIRST segment only - so the fallback decrypts one segment and reports it as the
// whole answer. The seam cannot know the intended length; the durable artifact row can, and the
// product documents that ahp_artifacts.artifacts.size is the PLAINTEXT length (seal.go: "the bound
// sha256/size stay the plaintext digest/length by design"). The read path therefore has to treat
// that attestation as binding: fewer plaintext bytes than the row attests is a refusal, not a 200.
//
// Fault injection is storage-layer and is stated as such: the sealed version is replaced by a copy
// with the footer wiped and the artifact row is repointed at that version - which is what silent
// object-store damage or a bucket-level attacker produces. PG's attested content fields (sha256,
// size) are left exactly as Seal wrote them.
func TestL2eDamagedStoredObjectIsNotSilentlyAccepted(t *testing.T) {
	l := startLeg(t, "L2e_damaged_object_not_silently_accepted")
	plain := bodyOf(3 << 20)
	v := newEnv(t, true, 0)
	ref, up := v.uploadSeal(t, v.store, "l2e-damaged", string(plain))
	good := v.rawGetVersion(t, up.Object, ref.Version)
	if !atrest.IsEnvelope(good) {
		t.Fatalf("fixture: expected a segmented envelope, the stored head is not an envelope")
	}
	// Control: while the bytes are intact the same artifact reads whole and exactly.
	if got := readAllBytes(t, mustOpen(t, v.store, v, ref.ID)); !bytes.Equal(got, plain) {
		t.Fatalf("control read of the intact artifact did not return the plaintext")
	}

	damaged := append([]byte(nil), good...)
	for i := len(damaged) - 8; i < len(damaged); i++ {
		damaged[i] = 0x00 // wipe the segment-manifest footer magic
	}
	damagedVersion := v.rawPut(t, up.Object, damaged)
	repointArtifactVersion(t, v, ref.ID, damagedVersion)
	l.set("injection", "footer wiped in a new object version; artifact row repointed at it (PG sha256/size untouched)")
	l.set("attested_size_bytes", len(plain))

	// (1) Whole read: refuse at Open or refuse during the read - never a short success.
	short, shortErr := readAllowError(t, v.store, v, ref.ID)
	l.set("open_read_error", fmt.Sprint(shortErr))
	l.set("open_read_bytes", len(short))
	if shortErr == nil {
		t.Errorf("silent truncation: Open answered %d of %d attested bytes with no error", len(short), len(plain))
	} else if !bytes.Equal(short, plain[:len(short)]) {
		t.Errorf("the refusal delivered bytes that are not the plaintext prefix (splice): %d bytes", len(short))
	}

	// (2) A public range past the surviving first segment must refuse, not answer empty.
	if rc, _, err := v.store.OpenRange(contextOr(t), v.scope, ref.ID, (2<<20)+100, 64); err == nil {
		got := readAllBytes(t, rc)
		t.Errorf("OpenRange past the surviving segment answered %d bytes with no error instead of refusing", len(got))
	} else {
		l.set("range_past_surviving_segment_refused", fmt.Sprint(err))
	}

	// (3) A range INSIDE the surviving first segment stays readable, so the refusals above are
	// damage-scoped rather than the read path being broken.
	if rc, _, err := v.store.OpenRange(contextOr(t), v.scope, ref.ID, 0, 4096); err != nil {
		t.Errorf("an intact leading segment must still read: %v", err)
	} else {
		got := readAllBytes(t, rc)
		if !bytes.Equal(got, plain[:4096]) {
			t.Errorf("the intact leading segment returned wrong bytes")
		}
		l.set("intact_leading_segment_reads", true)
	}
	l.set("expected_policy", "delivered plaintext shorter than the durable size attestation = refusal")
	l.set("mechanism", "loadManifest errNotSegmented -> OpenTransparent fallback answers one segment as the whole object")
}

// L3 - OpenRange across a segment boundary returns exactly the right bytes.
//
// Offsets are chosen relative to the PRODUCT default segment size (1 MiB), because that is the
// configuration the deployment actually runs with, not a test-chosen small one: a boundary bug
// hidden behind a 64 KiB test segment is still a bug in production.
func TestL3RangeAcrossSegmentBoundary(t *testing.T) {
	l := startLeg(t, "L3_range_across_segment_boundary")
	seg := objectcipher.DefaultSegmentSize
	body := bodyOf(int(2*seg + 12345))
	v := newEnv(t, true, 0)
	ref, up := v.uploadSeal(t, v.store, "l3-boundary", string(body))
	raw := v.rawGetVersion(t, up.Object, ref.Version)
	l.set("plain_len", len(body))
	l.set("stored_len", len(raw))
	l.set("segment_size", seg)
	l.set("segments_expected", 3)

	cases := []struct {
		name          string
		offset, count int64
	}{
		{"head", 0, 16},
		{"across_first_boundary_back", seg - 8, 16},
		{"exactly_at_boundary", seg, 16},
		{"one_byte_either_side", seg - 1, 2},
		{"across_second_boundary", 2*seg - 4, 8},
		{"whole_segment_1", seg, seg},
		{"into_tail", 2*seg + 5, 1000},
		{"to_end_from_boundary", seg + 1, 0},
		{"whole_object", 0, int64(len(body))},
	}
	results := map[string]any{}
	for _, c := range cases {
		rc, _, err := v.store.OpenRange(contextOr(t), v.scope, ref.ID, c.offset, c.count)
		if err != nil {
			t.Fatalf("%s: OpenRange(%d,%d): %v", c.name, c.offset, c.count, err)
		}
		got := readAllBytes(t, rc)
		end := int64(len(body))
		if c.count > 0 && c.offset+c.count < end {
			end = c.offset + c.count
		}
		want := body[c.offset:end]
		if !bytes.Equal(got, want) {
			t.Fatalf("%s: range(%d,%d) returned %d bytes that do not match the plaintext slice (%d bytes)",
				c.name, c.offset, c.count, len(got), len(want))
		}
		results[c.name] = map[string]any{"offset": c.offset, "count": c.count, "returned": len(got), "exact": true}
	}
	// Past the end must be a refusal, not an empty success.
	if _, _, err := v.store.OpenRange(contextOr(t), v.scope, ref.ID, int64(len(body))+1, 16); err == nil {
		t.Fatal("OpenRange past the end returned success instead of a refusal")
	} else {
		l.set("past_end_refused", fmt.Sprintf("%T", err))
	}
	l.set("cases", results)
	l.set("level", "store.OpenRange with the product default segment size")
}

// L5 - OFF must be byte-for-byte what the pre-mount product produced.
//
// "Inert when disabled" is a claim about bytes, so it is tested as bytes: with no cipher mounted,
// the stored object equals the plaintext exactly, PG holds the plaintext digest and size, and the
// read path returns the same stream it always did. Anything less would let a mount change the
// OFF behaviour and still be called inert.
func TestL5OffModeIsByteIdentical(t *testing.T) {
	l := startLeg(t, "L5_off_mode_byte_identical")
	for _, s := range []struct {
		name string
		n    int
	}{{"1KiB", 1 << 10}, {"3MiB", 3 << 20}} {
		plain := bodyOf(s.n)
		v := newEnv(t, false, 0)
		ref, up := v.uploadSeal(t, v.offStore, "l5-"+s.name, string(plain))
		raw := v.rawGetVersion(t, up.Object, ref.Version)
		if !bytes.Equal(raw, plain) {
			t.Fatalf("%s: OFF mode changed the stored bytes (%d vs %d)", s.name, len(raw), len(plain))
		}
		if ref.SHA256 != shaHex(plain) || ref.Size != int64(len(plain)) {
			t.Fatalf("%s: OFF mode PG identity changed: sha=%v size=%v", s.name, ref.SHA256 == shaHex(plain), ref.Size)
		}
		if got := readAllBytes(t, mustOpen(t, v.offStore, v, ref.ID)); !bytes.Equal(got, plain) {
			t.Fatalf("%s: OFF mode read mismatch", s.name)
		}
		l.set(s.name, map[string]any{
			"stored_equals_plaintext": true, "stored_len": len(raw),
			"pg_sha256_equals_plaintext": true, "pg_size_equals_plaintext": true,
		})
	}
	l.set("note", "no cipher is mounted: Store.cipherOn() is false, so every put/get is the native client call")
}

// L6 - encryption must not move the dedup / seal identity.
//
// PG is documented to keep the PLAINTEXT digest and size, so that idempotency, dedup and extraction
// caching keep working across the mount. Two uploads of the same bytes under different idempotency
// keys must therefore agree in PG while their stored envelopes differ (per-object nonce/DEK), and a
// repeated BeginUpload under the same key must return the same PG identity rather than a new row.
func TestL6DedupAndSealIdentityUnderEncryption(t *testing.T) {
	l := startLeg(t, "L6_dedup_seal_identity_under_encryption")
	plain := bodyOf(2<<20 + 777)
	v := newEnv(t, true, 0)
	refA, upA := v.uploadSeal(t, v.store, "l6-a", string(plain))
	refB, upB := v.uploadSeal(t, v.store, "l6-b", string(plain))
	rawA := v.rawGetVersion(t, upA.Object, refA.Version)
	rawB := v.rawGetVersion(t, upB.Object, refB.Version)
	if refA.SHA256 != shaHex(plain) || refB.SHA256 != shaHex(plain) {
		t.Fatalf("PG identity moved away from the plaintext digest: %s / %s", refA.SHA256, refB.SHA256)
	}
	if refA.SHA256 != refB.SHA256 {
		t.Fatal("two identical plaintexts produced different PG digests")
	}
	if refA.Size != refB.Size || refA.Size != int64(len(plain)) {
		t.Fatalf("PG size is not the plaintext size: %d vs %d vs %d", refA.Size, refB.Size, len(plain))
	}
	if refA.ID == refB.ID {
		t.Fatal("two different idempotency keys collapsed into one artifact id")
	}
	if bytes.Equal(rawA, rawB) {
		t.Fatal("identical plaintexts produced identical ciphertext (no per-object nonce/DEK)")
	}
	if bytes.Equal(rawA, plain) || bytes.Equal(rawB, plain) {
		t.Fatal("one of the stored objects is plaintext")
	}
	if !atrest.IsEnvelope(rawA) || !atrest.IsEnvelope(rawB) {
		t.Fatal("stored heads are not envelopes")
	}
	// Same key twice: the PG identity is the idempotency answer, and it must not change.
	upAgain, err := v.store.BeginUpload(contextOr(t), v.scope, "l6-a", uploadRequestFor(plain))
	if err != nil {
		t.Fatalf("repeat BeginUpload under the same key: %v", err)
	}
	if upAgain.ID != refA.ID {
		t.Fatalf("idempotent BeginUpload returned a different PG identity: %s vs %s", upAgain.ID, refA.ID)
	}
	l.set("pg_sha256", refA.SHA256)
	l.set("pg_size", refA.Size)
	l.set("pg_digest_equal_across_uploads", true)
	l.set("stored_bytes_differ_across_uploads", true)
	l.set("stored_len_a", len(rawA))
	l.set("stored_len_b", len(rawB))
	l.set("idempotent_same_id", true)
	l.set("encryption_overhead_bytes", len(rawA)-len(plain))
}

// --- helpers used by the legs above ---------------------------------------------------------

func uploadRequestFor(plain []byte) artifacts.UploadRequest {
	return artifacts.UploadRequest{Name: "verify.bin", MIME: "application/octet-stream",
		SHA256: shaHex(plain), Size: int64(len(plain))}
}

func contextOr(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

func mustOpen(t *testing.T, st *artifacts.Store, v *venv, id string) io.ReadCloser {
	t.Helper()
	reader, _, err := st.Open(contextOr(t), v.scope, id)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return reader
}

// ownCipher builds a cipher over this run's bucket with its OWN key authority, so a leg can write
// and then damage an object without needing access to unexported store fields.
func ownCipher(t *testing.T, segmentSize int64) (*minio.Client, string, *objectcipher.Cipher, string) {
	t.Helper()
	requireLive(t)
	client := minioClient(t)
	bucket := "go-atverify-" + runIdentValue() + "own"
	ctx := contextOr(t)
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("make bucket: %v", err)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for obj := range client.ListObjects(c, bucket, minio.ListObjectsOptions{Recursive: true, WithVersions: true}) {
			if obj.Err != nil {
				break
			}
			_ = client.RemoveObject(c, bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID})
		}
		_ = client.RemoveBucket(c, bucket)
	})
	ciph := objectcipher.New(mustCodec(t), client, true)
	if segmentSize > 0 {
		ciph = ciph.WithSegmentSize(segmentSize)
	}
	var rnd [4]byte
	_, _ = rand.Read(rnd[:])
	return client, "own-" + runIdentValue(), ciph, bucket
}

func rawBucket(t *testing.T, client *minio.Client, bucket, object, version string) []byte {
	t.Helper()
	rc, err := client.GetObject(contextOr(t), bucket, object, minio.GetObjectOptions{VersionID: version})
	if err != nil {
		t.Fatalf("raw get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		t.Fatalf("raw read: %v", err)
	}
	return buf.Bytes()
}

func putBucket(t *testing.T, client *minio.Client, bucket, object string, body []byte) string {
	t.Helper()
	info, err := client.PutObject(contextOr(t), bucket, object, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("raw put: %v", err)
	}
	return info.VersionID
}

func readAllAllowError2(rc io.ReadCloser) (int, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(rc)
	return buf.Len(), err
}

// deliveredPrefix returns the prefix the reader actually served, for the splice comparison.
func deliveredPrefix(n int, plain []byte) []byte { return plain[:n] }

// readAllowError reads a product Open handle and reports the delivered bytes plus the read error.
func readAllowError(t *testing.T, st *artifacts.Store, v *venv, id string) ([]byte, error) {
	t.Helper()
	rc, _, err := st.Open(contextOr(t), v.scope, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	var buf bytes.Buffer
	_, rerr := buf.ReadFrom(rc)
	return buf.Bytes(), rerr
}

// repointArtifactVersion is storage-layer fault injection: it points the artifact row at another
// version of the same object. The attested content fields (sha256, size) stay as Seal wrote them.
func repointArtifactVersion(t *testing.T, v *venv, id, version string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tag, err := v.pool.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET version = $1 WHERE id = $2`, version, id)
	if err != nil {
		t.Fatalf("storage fault injection failed: %v", err)
	}
	if tag.RowsAffected() != 1 {
		t.Fatalf("storage fault injection affected %d rows, expected 1", tag.RowsAffected())
	}
}
