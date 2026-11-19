package artifacts

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
)

// At-rest mount (R6): the object byte seam is wired into the actual put/get
// points of the artifact plane. Semantics that must not change, do not:
//   - PG keeps PLAINTEXT sha256/size; dedup, seal identity, extraction cache
//     and idempotency keep operating on plaintext digests.
//   - The client contract is untouched: uploads stay presigned PUT (bytes land
//     as plaintext, then Seal transforms the verified version into a sealed
//     version of the SAME object path - never a second object), downloads
//     stay server-proxied (Open/OpenRange) and decrypt transparently.
//   - OFF (no cipher or cipher disabled) is inert: every path below falls
//     through to the exact same minio client calls as before.
func (s *Store) cipherOn(tenant string) bool {
	return s.cipher != nil && s.cipher.Enabled() && tenant != ""
}

// sealTransform turns the verified plaintext version into a sealed version of
// the same object and returns the version id to bind. Fail-closed: without a
// working key authority the artifact stays unsealed, it is never bound as
// plaintext behind an enabled profile.
func (s *Store) sealTransform(ctx context.Context, tenant, object, versionID string, size int64) (string, error) {
	src, err := s.client.GetObject(ctx, s.bucket, object, minio.GetObjectOptions{VersionID: versionID})
	if err != nil {
		return "", storageError("open object for seal transform", err)
	}
	defer src.Close()
	info, err := s.cipher.PutObjectStream(ctx, tenant, s.bucket, object, src, size, minio.PutObjectOptions{})
	if err != nil {
		return "", storageError("seal transform refused", err)
	}
	if info.VersionID != "" {
		return info.VersionID, nil
	}
	// Unversioned buckets (non-product stands) read back the newest version.
	return "", nil
}

// plaintextReader delivers the plaintext bytes of one exact object version,
// decrypting transparently when the mount is on and returning the native
// stream otherwise.
func (s *Store) plaintextReader(ctx context.Context, tenant, object, versionID string) (io.ReadCloser, error) {
	if s.cipherOn(tenant) {
		return s.cipher.OpenReader(ctx, tenant, s.bucket, object, versionID)
	}
	return s.client.GetObject(ctx, s.bucket, object, minio.GetObjectOptions{VersionID: versionID})
}
