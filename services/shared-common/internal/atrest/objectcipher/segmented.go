package objectcipher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/minio/minio-go/v7"
)

// Segmented object format (R6): sealed artifacts above one segment are stored
// as a stream of independent envelope segments plus an authenticated manifest
// trailer, so range reads fetch only the ciphertext segments overlapping the
// requested plaintext range, and full reads decrypt segment by segment with a
// bounded working set (one segment of ciphertext + one of plaintext) instead
// of materialising the whole object (peak ~2x).
//
// Layout: [segments...][manifest envelope][4B manifest length][8B footer magic]
// The manifest binds, through the tenant/purpose-bound AAD of its own envelope,
// the exact ciphertext offset/length and the plaintext sha256 of every segment,
// so swapping, reordering, truncating or splicing segments between objects of
// the same tenant fails closed before any plaintext is released.
//
// The format is owned by this seam and does NOT change the crypto contract:
// every segment and the manifest are plain atrest envelopes (same AES-256-GCM,
// HKDF, AAD binding, fail-closed semantics), and the v1 whole-object envelope
// and legacy plaintext objects remain readable through the same entry points.
const (
	segMagic        = "AHPSEGV1"
	segFooterMagic  = "AHPSEGF1"
	segFooterSize   = 4 + 8
	sealPartSizeMin = uint64(16 << 20)
	// EnvSegmentSize optionally overrides DefaultSegmentSize (bytes, >= 64KiB).
	// It only affects the shape of NEW sealed objects; readers self-detect.
	EnvSegmentSize = "AHP_ATREST_ARTIFACTS_SEGMENT_BYTES"
)

// DefaultSegmentSize bounds plaintext bytes per sealed segment.
const DefaultSegmentSize = int64(1 << 20)

// ErrMalformedSegmented marks a segmented object whose trailer or manifest
// does not parse; callers must treat it as corruption, never as plaintext.
var ErrMalformedSegmented = errors.New("objectcipher: malformed segmented object")

type segEntry struct {
	Off      int64  `json:"off"`
	Len      int64  `json:"len"`
	PlainLen int64  `json:"plain_len"`
	SHA256   string `json:"sha256"`
}

type segManifest struct {
	SegSize   int64      `json:"seg_size"`
	PlainSize int64      `json:"plain_size"`
	Segs      []segEntry `json:"segs"`
}

func segmentSizeFromEnv() int64 {
	raw := os.Getenv(EnvSegmentSize)
	if raw == "" {
		return DefaultSegmentSize
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 64<<10 {
		return DefaultSegmentSize
	}
	return n
}

// WithSegmentSize overrides the segment size (tests and operator tuning).
// Values below 64 KiB fall back to the default.
func (c *Cipher) WithSegmentSize(n int64) *Cipher {
	if n >= 64<<10 {
		c.segSize = n
	}
	return c
}

// PutObjectStream stores body under object, sealing on the way through when
// enabled. Disabled it is the verbatim pass-through write. When enabled,
// objects up to one segment use the v1 whole-object envelope; larger objects
// use the segmented format so neither write nor later reads need the whole
// plaintext in memory at once beyond one segment.
func (c *Cipher) PutObjectStream(ctx context.Context, tenant, bucket, object string, body io.Reader, size int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	if !c.enabled {
		return c.client.PutObject(ctx, bucket, object, body, size, opts)
	}
	if tenant == "" {
		return minio.UploadInfo{}, atrest.ErrInvalidArgument
	}
	seg := c.segSize
	if seg > 0 && size >= 0 && size <= seg {
		plain, err := io.ReadAll(body)
		if err != nil {
			return minio.UploadInfo{}, err
		}
		sealed, err := c.codec.Seal(ctx, tenant, c.purpose, plain)
		if err != nil {
			return minio.UploadInfo{}, err
		}
		return c.client.PutObject(ctx, bucket, object, bytes.NewReader(sealed), int64(len(sealed)), opts)
	}
	if seg <= 0 {
		seg = DefaultSegmentSize
	}
	pr, pw := io.Pipe()
	opts.PartSize = sealPartSizeMin
	type res struct {
		info minio.UploadInfo
		err  error
	}
	done := make(chan res, 1)
	go func() {
		info, err := c.client.PutObject(ctx, bucket, object, pr, -1, opts)
		done <- res{info, err}
	}()
	werr := func() error {
		buf := make([]byte, seg)
		var man segManifest
		man.SegSize = seg
		var off, plainOff int64
		for {
			n, rerr := io.ReadFull(body, buf)
			if n > 0 {
				sum := sha256.Sum256(buf[:n])
				sealed, serr := c.codec.Seal(ctx, tenant, c.purpose, buf[:n])
				if serr != nil {
					return serr
				}
				if _, werr := pw.Write(sealed); werr != nil {
					return werr
				}
				man.Segs = append(man.Segs, segEntry{Off: off, Len: int64(len(sealed)), PlainLen: int64(n), SHA256: hex.EncodeToString(sum[:])})
				off += int64(len(sealed))
				plainOff += int64(n)
			}
			if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
				break
			}
			if rerr != nil {
				return rerr
			}
		}
		man.PlainSize = plainOff
		mraw, err := json.Marshal(&man)
		if err != nil {
			return err
		}
		msealed, err := c.codec.Seal(ctx, tenant, c.purpose, mraw)
		if err != nil {
			return err
		}
		var foot [segFooterSize]byte
		binary.BigEndian.PutUint32(foot[:4], uint32(len(msealed)))
		copy(foot[4:], segFooterMagic)
		if _, err := pw.Write(msealed); err != nil {
			return err
		}
		if _, err := pw.Write(foot[:]); err != nil {
			return err
		}
		return nil
	}()
	if werr != nil {
		_ = pw.CloseWithError(werr)
		r := <-done
		return minio.UploadInfo{}, errors.Join(werr, r.err)
	}
	_ = pw.Close()
	r := <-done
	return r.info, r.err
}

// GetRange reads a plaintext range of one object version. Disabled it is the
// native byte-range read. Enabled: segmented objects fetch only the segments
// covering the range; v1 envelopes and legacy plaintext fall back to a full
// read plus slice (documented mechanism limit of the v1 format).
func (c *Cipher) GetRange(ctx context.Context, tenant, bucket, object, versionID string, offset, count int64) ([]byte, error) {
	if !c.enabled {
		return c.nativeRangeVersioned(ctx, bucket, object, versionID, offset, count)
	}
	if tenant == "" {
		return nil, atrest.ErrInvalidArgument
	}
	man, err := c.loadManifest(ctx, tenant, bucket, object, versionID)
	if err == errNotSegmented {
		raw, err := c.getRawVersioned(ctx, bucket, object, versionID)
		if err != nil {
			return nil, err
		}
		plain, _, err := c.codec.OpenTransparent(ctx, tenant, c.purpose, raw)
		if err != nil {
			return nil, err
		}
		return slice(plain, offset, count)
	}
	if err != nil {
		return nil, err
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= man.PlainSize {
		return []byte(nil), nil
	}
	end := man.PlainSize
	if count > 0 && offset+count < end {
		end = offset + count
	}
	var out []byte
	first := offset / man.SegSize
	last := (end - 1) / man.SegSize
	for i := first; i <= last; i++ {
		plain, err := c.openSegment(ctx, tenant, bucket, object, versionID, man, i)
		if err != nil {
			return nil, err
		}
		lo := max64(offset-i*man.SegSize, 0)
		hi := min64(end-i*man.SegSize, int64(len(plain)))
		if lo < hi {
			out = append(out, plain[lo:hi]...)
		}
	}
	return out, nil
}

// OpenReader returns a streaming plaintext reader over one object version.
// Disabled it is the native object stream. Segmented objects are decrypted
// segment by segment on demand; v1 envelopes and legacy plaintext are read
// whole (peak ~2x of object size only for the v1/legacy formats).
func (c *Cipher) OpenReader(ctx context.Context, tenant, bucket, object, versionID string) (io.ReadCloser, error) {
	if !c.enabled {
		return c.client.GetObject(ctx, bucket, object, minio.GetObjectOptions{VersionID: versionID})
	}
	if tenant == "" {
		return nil, atrest.ErrInvalidArgument
	}
	man, err := c.loadManifest(ctx, tenant, bucket, object, versionID)
	if err == errNotSegmented {
		raw, err := c.getRawVersioned(ctx, bucket, object, versionID)
		if err != nil {
			return nil, err
		}
		plain, _, err := c.codec.OpenTransparent(ctx, tenant, c.purpose, raw)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(plain)), nil
	}
	if err != nil {
		return nil, err
	}
	return &segReader{c: c, ctx: ctx, tenant: tenant, bucket: bucket, object: object, versionID: versionID, man: man}, nil
}

var errNotSegmented = errors.New("objectcipher: object is not segmented")

func (c *Cipher) loadManifest(ctx context.Context, tenant, bucket, object, versionID string) (*segManifest, error) {
	info, err := c.client.StatObject(ctx, bucket, object, minio.StatObjectOptions{VersionID: versionID})
	if err != nil {
		return nil, err
	}
	if info.Size < segFooterSize+1 {
		return nil, errNotSegmented
	}
	foot, err := c.getBytes(ctx, bucket, object, versionID, info.Size-segFooterSize, info.Size)
	if err != nil {
		return nil, err
	}
	if string(foot[4:]) != segFooterMagic {
		return nil, errNotSegmented
	}
	mlen := int64(binary.BigEndian.Uint32(foot[:4]))
	if mlen <= 0 || mlen+segFooterSize > info.Size {
		return nil, ErrMalformedSegmented
	}
	mraw, err := c.getBytes(ctx, bucket, object, versionID, info.Size-segFooterSize-mlen, info.Size-segFooterSize)
	if err != nil {
		return nil, err
	}
	plain, _, err := c.codec.OpenTransparent(ctx, tenant, c.purpose, mraw)
	if err != nil {
		return nil, err
	}
	var man segManifest
	if err := json.Unmarshal(plain, &man); err != nil {
		return nil, ErrMalformedSegmented
	}
	if man.SegSize <= 0 || len(man.Segs) == 0 {
		return nil, ErrMalformedSegmented
	}
	var lastEnd, plainSum int64
	for _, s := range man.Segs {
		if s.Off != lastEnd || s.Len <= 0 || s.PlainLen <= 0 {
			return nil, ErrMalformedSegmented
		}
		if _, derr := hex.DecodeString(s.SHA256); derr != nil || len(s.SHA256) != 64 {
			return nil, ErrMalformedSegmented
		}
		lastEnd += s.Len
		plainSum += s.PlainLen
	}
	if man.PlainSize != plainSum {
		return nil, ErrMalformedSegmented
	}
	if info.Size < lastEnd+segFooterSize+mlen {
		return nil, ErrMalformedSegmented
	}
	return &man, nil
}

// openSegment fetches, opens and integrity-checks one sealed segment.
func (c *Cipher) openSegment(ctx context.Context, tenant, bucket, object, versionID string, man *segManifest, idx int64) ([]byte, error) {
	if idx < 0 || idx >= int64(len(man.Segs)) {
		return nil, ErrMalformedSegmented
	}
	s := man.Segs[idx]
	raw, err := c.getBytes(ctx, bucket, object, versionID, s.Off, s.Off+s.Len)
	if err != nil {
		return nil, err
	}
	plain, _, err := c.codec.OpenTransparent(ctx, tenant, c.purpose, raw)
	if err != nil {
		return nil, err
	}
	if int64(len(plain)) != s.PlainLen {
		return nil, fmt.Errorf("%w: segment %d length mismatch", ErrMalformedSegmented, idx)
	}
	sum := sha256.Sum256(plain)
	if hex.EncodeToString(sum[:]) != s.SHA256 {
		return nil, fmt.Errorf("%w: segment %d digest mismatch", ErrMalformedSegmented, idx)
	}
	return plain, nil
}

func (c *Cipher) getBytes(ctx context.Context, bucket, object, versionID string, start, end int64) ([]byte, error) {
	var opts minio.GetObjectOptions
	opts.VersionID = versionID
	if err := opts.SetRange(start, end-1); err != nil {
		return nil, err
	}
	obj, err := c.client.GetObject(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

func (c *Cipher) getRawVersioned(ctx context.Context, bucket, object, versionID string) ([]byte, error) {
	obj, err := c.client.GetObject(ctx, bucket, object, minio.GetObjectOptions{VersionID: versionID})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

func (c *Cipher) nativeRangeVersioned(ctx context.Context, bucket, object, versionID string, offset, count int64) ([]byte, error) {
	var opts minio.GetObjectOptions
	opts.VersionID = versionID
	if offset > 0 || count > 0 {
		if offset < 0 {
			offset = 0
		}
		var end int64
		if count > 0 {
			end = offset + count - 1
		}
		if err := opts.SetRange(offset, end); err != nil {
			return nil, err
		}
	}
	obj, err := c.client.GetObject(ctx, bucket, object, opts)
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

// segReader streams plaintext of a segmented object, decrypting at most one
// segment ahead of the consumer's demand.
type segReader struct {
	c                      *Cipher
	ctx                    context.Context
	tenant, bucket, object string
	versionID              string
	man                    *segManifest
	idx                    int64
	buf                    []byte
	done                   bool
}

func (r *segReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.buf) == 0 {
		if r.done {
			return 0, io.EOF
		}
		plain, err := r.c.openSegment(r.ctx, r.tenant, r.bucket, r.object, r.versionID, r.man, r.idx)
		if err != nil {
			return 0, err
		}
		r.buf = plain
		r.idx++
		if r.idx >= int64(len(r.man.Segs)) {
			r.done = true
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *segReader) Close() error { return nil }

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
