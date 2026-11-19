package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/minio/minio-go/v7"
)

// PutBoundBytes is the owner-side upload path for opaque service output. It
// uses the same staged metadata, access checks and sealing as the HTTP API.
func (s *Store) PutBoundBytes(ctx context.Context, scope contracts.Scope, binding Binding, key string, data []byte) (ArtifactRef, error) {
	if err := requireBindingValue("context id", binding.ContextID); err != nil {
		return ArtifactRef{}, err
	}
	digest := sha256.Sum256(data)
	request := UploadRequest{
		Name: "tool-result.bin", MIME: "application/octet-stream",
		Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]),
	}
	upload, err := s.BeginUploadWithBinding(ctx, scope, binding, key, request)
	if err != nil {
		return ArtifactRef{}, err
	}
	if ref, err := s.Get(ctx, scope, upload.ID); err == nil {
		return ref, nil
	} else if code := contracts.Code(err); code != contracts.ErrConflict && code != contracts.ErrNotFound {
		return ArtifactRef{}, err
	}
	if _, err := s.client.PutObject(ctx, s.bucket, upload.Object, bytes.NewReader(data), request.Size,
		minio.PutObjectOptions{ContentType: request.MIME}); err != nil {
		return ArtifactRef{}, storageError("upload opaque service output", err)
	}
	return s.Seal(ctx, scope, upload.ID)
}
