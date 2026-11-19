package atrestverify_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/ahp-platform/ahp/internal/atrest/objectcipher"
)

// Splice legs (the class the pre-registered review protocol names as the only meaningful integrity
// test of the segmented format).
//
// The data layer of the envelope binds tenant, keyID, purpose, keyVersion and the DEK wrap, but NOT
// the object name, the segment index or the segment offset (internal/atrest/codec.go). Two whole,
// valid segments of the same tenant and purpose are therefore interchangeable as far as AES-GCM is
// concerned: corrupting bytes proves only that GCM works and proves nothing about the format's
// actual claim. Everything that makes swapping, reordering, truncating or splicing fail closed is
// the per-segment plaintext sha256 in the sealed manifest (objectcipher/segmented.go openSegment).
// These legs therefore substitute WHOLE VALID SEGMENTS and require the refusal to be the manifest
// mismatch, never an authentication failure - a refusal of the wrong kind would mean the leg tested
// the wrong defence.
//
// Segments are assembled through the exported codec (atrest.Codec.Seal) in the documented layout
// [segments][manifest envelope][4B manifest length][8B footer magic]. A control read of the
// unmutated object runs first in every leg: if the reader did not accept the constructed object as
// its own format, the mutations below would prove nothing and the leg fails on the control.
const (
	segFooterMagicConst = "AHPSEGF1"
	spliceSegSize       = int64(64 << 10) // reader-visible minimum, keeps the fixture small and multi-segment
	spliceSegments      = 3
)

type spliceEntry struct {
	Off      int64  `json:"off"`
	Len      int64  `json:"len"`
	PlainLen int64  `json:"plain_len"`
	SHA256   string `json:"sha256"`
}

type spliceManifest struct {
	SegSize   int64         `json:"seg_size"`
	PlainSize int64         `json:"plain_size"`
	Segs      []spliceEntry `json:"segs"`
}

type spliceObject struct {
	plain     []byte
	segments  [][]byte // sealed ciphertext of each segment, in order
	envelopes []byte   // segments concatenated
	object    []byte   // segments + manifest envelope + 4B length + footer magic
}

// buildSpliceObject seals plaintext chunks with the same codec/tenant/purpose the cipher under test
// uses, so the result is indistinguishable from a product-written segmented object to the reader.
func buildSpliceObject(t *testing.T, codec *atrest.Codec, tenant string, plain []byte, segSize int64) *spliceObject {
	t.Helper()
	ctx := contextOr(t)
	o := &spliceObject{plain: plain}
	for start := int64(0); start < int64(len(plain)); start += segSize {
		end := start + segSize
		if end > int64(len(plain)) {
			end = int64(len(plain))
		}
		sealed, err := codec.Seal(ctx, tenant, objectcipher.DefaultPurpose, plain[start:end])
		if err != nil {
			t.Fatalf("seal segment: %v", err)
		}
		o.segments = append(o.segments, sealed)
		o.envelopes = append(o.envelopes, sealed...)
	}
	man := spliceManifest{SegSize: segSize, PlainSize: int64(len(plain))}
	for i, seg := range o.segments {
		start := int64(i) * segSize
		end := start + segSize
		if end > int64(len(plain)) {
			end = int64(len(plain))
		}
		sum := sha256.Sum256(plain[start:end])
		man.Segs = append(man.Segs, spliceEntry{
			Off: cipherOffset(o.segments, i), Len: int64(len(seg)), PlainLen: end - start,
			SHA256: hex.EncodeToString(sum[:]),
		})
	}
	mraw, err := json.Marshal(&man)
	if err != nil {
		t.Fatalf("manifest marshal: %v", err)
	}
	menv, err := codec.Seal(ctx, tenant, objectcipher.DefaultPurpose, mraw)
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	foot := make([]byte, 4+len(segFooterMagicConst))
	binary.BigEndian.PutUint32(foot[:4], uint32(len(menv)))
	copy(foot[4:], segFooterMagicConst)
	o.object = append(append(append([]byte(nil), o.envelopes...), menv...), foot...)
	return o
}

// spliceFixture gives one leg a private bucket, tenant, codec and cipher.
type spliceFixture struct {
	client *minio.Client
	bucket string
	tenant string
	codec  *atrest.Codec
	cipher *objectcipher.Cipher
}

func newSpliceFixture(t *testing.T) *spliceFixture {
	t.Helper()
	requireLive(t)
	client := minioClient(t)
	bucket := "go-atverify-spl-" + runIdentValue() + ident()[:4]
	ctx := contextOr(t)
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatalf("splice bucket: %v", err)
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
	codec := mustCodecOnce(t)
	return &spliceFixture{
		client: client, bucket: bucket, tenant: "spl-" + runIdentValue(),
		codec: codec, cipher: objectcipher.New(codec, client, true).WithSegmentSize(spliceSegSize),
	}
}

func (f *spliceFixture) put(t *testing.T, object string, body []byte) string {
	t.Helper()
	info, err := f.client.PutObject(contextOr(t), f.bucket, object, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{})
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	return info.VersionID
}

// readWhole opens an object through the cipher and reports bytes plus the read error.
func (f *spliceFixture) readWhole(t *testing.T, object, version string) ([]byte, error) {
	t.Helper()
	rc, err := f.cipher.OpenReader(contextOr(t), f.tenant, f.bucket, object, version)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()
	var buf bytes.Buffer
	_, rerr := buf.ReadFrom(rc)
	return buf.Bytes(), rerr
}

// control demands that the unmutated constructed object reads back exactly. Without this the
// mutations below would be uninterpretable.
func (f *spliceFixture) control(t *testing.T, name string, o *spliceObject) string {
	t.Helper()
	version := f.put(t, name, o.object)
	got, err := f.readWhole(t, name, version)
	if err != nil || !bytes.Equal(got, o.plain) {
		t.Fatalf("control: the constructed segmented object did not read back exactly (err=%v, %d of %d bytes); "+
			"the fixture would not prove anything about the format", err, len(got), len(o.plain))
	}
	if rng, err := f.cipher.GetRange(contextOr(t), f.tenant, f.bucket, name, version, spliceSegSize, 16); err != nil ||
		!bytes.Equal(rng, o.plain[spliceSegSize:spliceSegSize+16]) {
		t.Fatalf("control: range read of the constructed object is wrong (err=%v, %d bytes)", err, len(rng))
	}
	return version
}

// classifyError names which defence produced a refusal. Only the manifest mismatch proves the
// property under test; an authentication failure would mean the mutation damaged ciphertext.
func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	text := err.Error()
	switch {
	case strings.Contains(text, "digest mismatch"):
		return "manifest_plaintext_sha256"
	case strings.Contains(text, "length mismatch"):
		return "manifest_length"
	case strings.Contains(text, "malformed"):
		return "manifest_malformed"
	case strings.Contains(text, "authentication failed"):
		return "gcm_authentication"
	}
	return "other: " + text
}

// TestL9aSwapWholeSegments - two valid segments of one object exchanged must refuse.
func TestL9aSwapWholeSegments(t *testing.T) {
	l := startLeg(t, "L9a_swap_whole_segments_refuses")
	f := newSpliceFixture(t)
	plain := bodyOf(int(spliceSegSize) * spliceSegments)
	o := buildSpliceObject(t, f.codec, f.tenant, plain, spliceSegSize)
	if len(o.segments) != spliceSegments {
		t.Fatalf("fixture produced %d segments, expected %d", len(o.segments), spliceSegments)
	}
	f.control(t, "splice/l9a-control", o)
	l.set("control_read_exact", true)
	l.set("segment_count", len(o.segments))

	swapped := append([]byte(nil), o.object...)
	a, b := o.segments[0], o.segments[1]
	// Segment 0 occupies [0,len0), segment 1 occupies [len0,len0+len1) - equal lengths here.
	copy(swapped[0:len(a)], b)
	copy(swapped[len(a):len(a)+len(b)], a)
	version := f.put(t, "splice/l9a-swapped", swapped)

	whole, werr := f.readWhole(t, "splice/l9a-swapped", version)
	l.set("whole_read_error", fmt.Sprint(werr))
	l.set("whole_read_class", classifyError(werr))
	l.set("whole_read_bytes", len(whole))
	if werr == nil {
		t.Errorf("swapped whole segments were served as a valid object (%d bytes)", len(whole))
	} else if classifyError(werr) == "gcm_authentication" {
		t.Errorf("the refusal came from GCM, not from the manifest: the leg would prove nothing about splice defence")
	} else if classifyError(werr) != "manifest_plaintext_sha256" {
		t.Errorf("expected the manifest plaintext-sha256 refusal, got %s", classifyError(werr))
	}

	rng, rerr := f.cipher.GetRange(contextOr(t), f.tenant, f.bucket, "splice/l9a-swapped", version, 0, 16)
	l.set("range_error_class", classifyError(rerr))
	l.set("range_bytes", len(rng))
	if rerr == nil {
		t.Errorf("a range inside the substituted segment was served (%d bytes) instead of refusing", len(rng))
	}

	// Proof that both envelopes are individually valid: each decrypts on its own. GCM is satisfied
	// by the swap, which is exactly why only the manifest can catch it.
	// len(a) is the CIPHERTEXT envelope length; the plaintext of a full segment is segSize bytes.
	if p0, err := f.codec.Open(contextOr(t), f.tenant, objectcipher.DefaultPurpose, a); err != nil || !bytes.Equal(p0, plain[:spliceSegSize]) {
		t.Fatalf("fixture: segment 0 envelope is not validly decryptable on its own (err=%v)", err)
	}
	if p1, err := f.codec.Open(contextOr(t), f.tenant, objectcipher.DefaultPurpose, b); err != nil || !bytes.Equal(p1, plain[spliceSegSize:2*spliceSegSize]) {
		t.Fatalf("fixture: segment 1 envelope is not validly decryptable on its own (err=%v)", err)
	}
	l.set("both_envelopes_individually_valid", true)
	l.set("defence_under_test", "manifest per-segment plaintext sha256 (segmented.go openSegment)")
}

// TestL9bTransplantSegmentAcrossObjects - a valid segment moved from object B into object A (same
// tenant, same codec, same purpose) must refuse. This is the shape GCM cannot see by construction.
func TestL9bTransplantSegmentAcrossObjects(t *testing.T) {
	l := startLeg(t, "L9b_transplant_segment_between_objects")
	f := newSpliceFixture(t)
	plainA := bodyOf(int(spliceSegSize) * spliceSegments)
	plainB := append([]byte(nil), plainA...)
	for i := spliceSegSize; i < 2*spliceSegSize; i++ {
		plainB[i] ^= 0xA5 // segment 1 differs; the transplant comes from here
	}
	a := buildSpliceObject(t, f.codec, f.tenant, plainA, spliceSegSize)
	b := buildSpliceObject(t, f.codec, f.tenant, plainB, spliceSegSize)
	f.control(t, "splice/l9b-controlA", a)
	f.control(t, "splice/l9b-controlB", b)
	l.set("controls_read_exact", true)

	mixed := append([]byte(nil), a.object...)
	off := int64(0)
	for i := 0; i < 1; i++ {
		off += int64(len(a.segments[i]))
	}
	copy(mixed[off:off+int64(len(a.segments[1]))], b.segments[1])
	version := f.put(t, "splice/l9b-mixed", mixed)

	whole, werr := f.readWhole(t, "splice/l9b-mixed", version)
	l.set("whole_read_error_class", classifyError(werr))
	l.set("whole_read_bytes", len(whole))
	if werr == nil {
		t.Errorf("a segment transplanted from another object was served as valid (%d bytes)", len(whole))
	} else if classifyError(werr) != "manifest_plaintext_sha256" {
		t.Errorf("expected the manifest plaintext-sha256 refusal, got %s", classifyError(werr))
	}

	rng, rerr := f.cipher.GetRange(contextOr(t), f.tenant, f.bucket, "splice/l9b-mixed", version, spliceSegSize, 16)
	l.set("range_error_class", classifyError(rerr))
	if rerr == nil {
		t.Errorf("range over the transplanted segment served %d bytes instead of refusing", len(rng))
	}
	if pB, err := f.codec.Open(contextOr(t), f.tenant, objectcipher.DefaultPurpose, b.segments[1]); err != nil ||
		!bytes.Equal(pB, plainB[spliceSegSize:2*spliceSegSize]) {
		t.Fatalf("fixture: the transplanted envelope is not validly decryptable (err=%v)", err)
	}
	l.set("transplanted_envelope_valid_on_its_own", true)
}

// TestL9cRemoveWholeSegment - a segment deleted from the middle, with manifest and footer left
// intact, must refuse through manifest validation and must not deliver any plaintext.
func TestL9cRemoveWholeSegment(t *testing.T) {
	l := startLeg(t, "L9c_remove_whole_segment_refuses")
	f := newSpliceFixture(t)
	plain := bodyOf(int(spliceSegSize) * spliceSegments)
	o := buildSpliceObject(t, f.codec, f.tenant, plain, spliceSegSize)
	f.control(t, "splice/l9c-control", o)
	l.set("control_read_exact", true)

	start := int64(len(o.segments[0]))
	end := start + int64(len(o.segments[1]))
	cut := append([]byte(nil), o.object[:start]...)
	cut = append(cut, o.object[end:]...)
	version := f.put(t, "splice/l9c-cut", cut)

	whole, werr := f.readWhole(t, "splice/l9c-cut", version)
	l.set("whole_read_error_class", classifyError(werr))
	l.set("whole_read_bytes", len(whole))
	if werr == nil {
		t.Errorf("removing a whole segment produced a successful read of %d bytes", len(whole))
	}
	if len(whole) != 0 {
		t.Errorf("plaintext was released from a damaged segmented object: %d bytes", len(whole))
	}
	if _, err := f.cipher.GetRange(contextOr(t), f.tenant, f.bucket, "splice/l9c-cut", version, 0, 16); err == nil {
		t.Error("a range of an object with a missing segment was served instead of refusing")
	} else {
		l.set("range_error_class", classifyError(err))
	}
	l.set("note", "manifest contiguity/size validation refuses before any plaintext: segmented.go loadManifest")
}

// mustCodecOnce keeps one key authority per test process so segments constructed here and segments
// read by the cipher are the same cryptographic objects.
func mustCodecOnce(t *testing.T) *atrest.Codec {
	t.Helper()
	if sharedCodec != nil {
		return sharedCodec
	}
	root := make([]byte, 32)
	if _, err := rand.Read(root); err != nil {
		t.Fatalf("root material: %v", err)
	}
	codec, err := atrest.NewCodec(atrest.NewDynamicAuthority(root))
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	sharedCodec = codec
	return codec
}

var sharedCodec *atrest.Codec

// cipherOffset is the ciphertext offset of segment i: segments are stored back to back.
func cipherOffset(segments [][]byte, i int) int64 {
	var n int64
	for j := 0; j < i; j++ {
		n += int64(len(segments[j]))
	}
	return n
}
