// Package objectcipher wires atrest envelope encryption to an S3/MinIO object
// byte boundary. It is the mechanism-ready seam the Artifacts plane adopts: it
// seals object bytes on write and transparently opens them on read, while
// leaving indexes, digests and metadata untouched.
//
// Safety contract:
//   - Encryption is operator-controlled and defaults OFF. When OFF the package
//     is inert: PutObject is a plain object write and GetObject is a native
//     byte-range read, byte-for-byte identical to an unencrypted store. There
//     is no silent enable.
//   - When ON, only new writes are sealed; reads still open sealed objects and
//     pass through legacy/plaintext objects unchanged, so enabling never breaks
//     readers of previously stored bytes.
//   - The plaintext sha256/size of an artifact are unchanged by sealing the
//     object bytes, so dedup, seal identity and range semantics are preserved.
package objectcipher

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"

	"github.com/ahp-platform/ahp/internal/atrest"
	"github.com/minio/minio-go/v7"
)

// EnvArtifactEncryption is the operator toggle for artifact-object encryption.
// Only 1/true/on/yes enable it; anything else (including unset) leaves the
// profile OFF so behavior is identical to the pre-encryption store.
const EnvArtifactEncryption = "AHP_ATREST_ARTIFACTS_ENABLED"

// DefaultPurpose is the domain tag sealed into the AAD for artifact bytes, so
// an artifact ciphertext can never be opened as another owner's payload.
const DefaultPurpose = "artifact.bytes"

// Cipher seals and opens artifact object bytes via an atrest Codec.
type Cipher struct {
	codec   *atrest.Codec
	client  *minio.Client
	enabled bool
	purpose string
	segSize int64
}

// New builds a Cipher. codec/client must be non-nil. enabled controls sealing of
// new writes; when false every method is a pass-through to the object store.
func New(codec *atrest.Codec, client *minio.Client, enabled bool) *Cipher {
	return &Cipher{codec: codec, client: client, enabled: enabled, purpose: DefaultPurpose, segSize: segmentSizeFromEnv()}
}

// WithPurpose overrides the AAD domain tag.
func (c *Cipher) WithPurpose(p string) *Cipher { c.purpose = p; return c }

// Enabled reports whether new writes are sealed.
func (c *Cipher) Enabled() bool { return c.enabled }

// EnabledFromEnv resolves the operator toggle from EnvArtifactEncryption.
// Anything but an explicit truthy value returns false (fail toward OFF).
func EnabledFromEnv() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(EnvArtifactEncryption))) {
	case "1", "true", "on", "yes":
		return true
	default:
		return false
	}
}

// PutObject writes object bytes. Sealed when enabled, verbatim otherwise. It
// returns the number of bytes actually stored (== len(body) when disabled).
func (c *Cipher) PutObject(ctx context.Context, tenant, bucket, object string, body []byte, opts minio.PutObjectOptions) (int64, error) {
	payload := body
	if c.enabled {
		if tenant == "" {
			return 0, atrest.ErrInvalidArgument
		}
		sealed, err := c.codec.Seal(ctx, tenant, c.purpose, body)
		if err != nil {
			return 0, err
		}
		payload = sealed
	}
	info, err := c.client.PutObject(ctx, bucket, object, bytes.NewReader(payload), int64(len(payload)), opts)
	if err != nil {
		return 0, err
	}
	return info.Size, nil
}

// GetObject reads object bytes and returns plaintext. When disabled it is a
// native byte-range read (identical to the unencrypted store). When enabled it
// reads the object and transparently opens a sealed envelope, passing a legacy
// plaintext object through unchanged; range is applied to the recovered
// plaintext so byte-range download semantics are preserved for both cases.
//
// offset<0 or count<0 are treated as a full read. count==0 with offset>=0 means
// "from offset to the end".
func (c *Cipher) GetObject(ctx context.Context, tenant, bucket, object string, offset, count int64) ([]byte, error) {
	if !c.enabled {
		return c.nativeRange(ctx, bucket, object, offset, count)
	}
	if tenant == "" {
		return nil, atrest.ErrInvalidArgument
	}
	// Read the whole object, then recover plaintext and slice. A sealed record
	// authenticates (fail-closed); a plaintext record is returned as-is.
	raw, err := c.getRaw(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	plain, _, err := c.codec.OpenTransparent(ctx, tenant, c.purpose, raw)
	if err != nil {
		return nil, err
	}
	return slice(plain, offset, count)
}

func (c *Cipher) nativeRange(ctx context.Context, bucket, object string, offset, count int64) ([]byte, error) {
	var opts minio.GetObjectOptions
	if offset > 0 || count > 0 {
		if offset < 0 {
			offset = 0
		}
		end := int64(0) // 0 => open-ended "bytes=offset-"
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

func (c *Cipher) getRaw(ctx context.Context, bucket, object string) ([]byte, error) {
	obj, err := c.client.GetObject(ctx, bucket, object, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(obj)
}

// slice returns plain[offset:offset+count] with the same clamping rules as an
// object range, so callers see identical bytes whether the object was sealed or
// plaintext.
func slice(plain []byte, offset, count int64) ([]byte, error) {
	if offset < 0 {
		offset = 0
	}
	if offset >= int64(len(plain)) {
		return []byte(nil), nil
	}
	end := int64(len(plain))
	if count > 0 {
		if e := offset + count; e < end {
			end = e
		}
	}
	return plain[offset:end], nil
}

// FromEnv builds the mounted cipher for a product binary from the operator
// profile. It returns (nil, nil) when the toggle is OFF, so callers pass a nil
// cipher through the inert path with zero behavior change. When ON it fails
// closed at start-up if no durable key material is configured.
func FromEnv(ctx context.Context, client *minio.Client) (*Cipher, error) {
	if !EnabledFromEnv() {
		return nil, nil
	}
	auth, err := atrest.LoadDynamicAuthorityFromEnv(ctx)
	if err != nil {
		return nil, err
	}
	codec, err := atrest.NewCodec(auth)
	if err != nil {
		return nil, err
	}
	return New(codec, client, true), nil
}
