package statejournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
)

const (
	maxStagedBlobBytes = 4 << 20
	s3BlobRoot         = "ahp-journal/v1/"
	unversionedBlob    = "s3"
)

// S3Blobs keeps immutable, session-scoped payloads in a private bucket. The
// existing journal rows remain the authority for publication and retention.
type S3Blobs struct {
	client *minio.Client
	bucket string
}

func NewWithS3(ctx context.Context, pool *pgxpool.Pool, client *minio.Client, bucket string) (*Store, error) {
	if pool == nil || client == nil || bucket == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "journal S3 storage requires a pool, client and bucket")
	}
	exists, err := client.BucketExists(ctx, bucket)
	if err != nil || !exists {
		return nil, contracts.NewError(contracts.ErrUnavailable, "journal S3 bucket is unavailable")
	}
	return &Store{pool: pool, s3: &S3Blobs{client: client, bucket: bucket}}, nil
}

func (s *Store) StageBlob(ctx context.Context, scope contracts.Scope, session string, lease Lease, data []byte) (BlobRef, error) {
	var ref BlobRef
	if s.s3 == nil {
		return ref, contracts.NewError(contracts.ErrUnavailable, "direct journal blob storage is disabled")
	}
	if !validID(session) || len(data) <= maxInlineBytes || len(data) > maxStagedBlobBytes {
		return ref, contracts.NewError(contracts.ErrInvalid, "blob must exceed 256 KiB and be at most 4 MiB")
	}
	if err := scope.Validate(); err != nil {
		return ref, err
	}
	if err := s.markBlobStage(ctx, scope, session, lease); err != nil {
		return ref, err
	}
	upload, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	ref, err := s.s3.stage(upload, scope.Tenant, session, data)
	if err != nil {
		return BlobRef{}, err
	}
	if err := s.markBlobStage(ctx, scope, session, lease); err != nil {
		return BlobRef{}, err
	}
	return ref, nil
}

// A durable staging mark protects reused immutable objects even when their
// S3 LastModified is older than the collector's cutoff. It also serializes
// staging with collection without holding a connection during the upload.
func (s *Store) markBlobStage(ctx context.Context, scope contracts.Scope, session string, lease Lease) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE ahp_journal.sessions SET blob_stage_at=clock_timestamp()
		WHERE tenant_id=$1 AND deployment_id=$2 AND subject_id=$3 AND session_id=$4
		  AND lease_owner=$5 AND lease_epoch=$6 AND lease_expires_at>clock_timestamp() AND status<>'DELETED'`,
		scope.Tenant, scope.Deployment, scope.Subject, session, lease.Worker, lease.Epoch)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrStaleEpoch, "session lease has expired or been superseded")
	}
	return err
}

func (s *Store) ReadBlob(ctx context.Context, scope contracts.Scope, session string, ref BlobRef) ([]byte, error) {
	if s.s3 == nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "direct journal blob storage is disabled")
	}
	if _, err := s.GetTurnStatus(ctx, scope, session); err != nil {
		return nil, err
	}
	return s.s3.read(ctx, scope.Tenant, session, ref)
}

func (b *S3Blobs) stage(ctx context.Context, tenant, session string, data []byte) (BlobRef, error) {
	digest := sha256.Sum256(data)
	hash := hex.EncodeToString(digest[:])
	ref := BlobRef{ArtifactID: hash, SHA256: hash, SizeBytes: int64(len(data))}
	key := s3BlobPrefix(tenant, session) + hash
	info, err := b.client.StatObject(ctx, b.bucket, key, minio.StatObjectOptions{})
	if err == nil {
		return stagedRef(ref, info)
	}
	if minio.ToErrorResponse(err).Code != "NoSuchKey" {
		return BlobRef{}, s3Unavailable()
	}
	opts := minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true,
		UserMetadata: map[string]string{"Sha256": hash}}
	opts.SetMatchETagExcept("*")
	upload, err := b.client.PutObject(ctx, b.bucket, key, bytes.NewReader(data), ref.SizeBytes, opts)
	if err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "PreconditionFailed", "ConditionalRequestConflict":
			info, err := b.client.StatObject(ctx, b.bucket, key, minio.StatObjectOptions{})
			if err == nil {
				return stagedRef(ref, info)
			}
		}
		return BlobRef{}, s3Unavailable()
	}
	ref.Version = s3Version(upload.VersionID)
	return ref, nil
}

func stagedRef(ref BlobRef, info minio.ObjectInfo) (BlobRef, error) {
	if info.Size != ref.SizeBytes || info.Metadata.Get("X-Amz-Meta-Sha256") != ref.SHA256 {
		return BlobRef{}, badS3Ref()
	}
	ref.Version = s3Version(info.VersionID)
	return ref, nil
}

func s3Version(version string) string {
	if version == "" {
		return unversionedBlob
	}
	return version
}

func s3ObjectVersion(ref BlobRef) string {
	if ref.Version == unversionedBlob {
		return ""
	}
	return ref.Version
}

func (b *S3Blobs) objectKey(tenant, session string, ref BlobRef) (string, error) {
	if !validPayload(Payload{Blob: &ref}) || ref.ArtifactID != ref.SHA256 || ref.SizeBytes > maxStagedBlobBytes ||
		strings.ToLower(ref.SHA256) != ref.SHA256 {
		return "", badS3Ref()
	}
	return s3BlobPrefix(tenant, session) + ref.SHA256, nil
}

func (b *S3Blobs) verify(ctx context.Context, tenant, session string, ref BlobRef) error {
	key, err := b.objectKey(tenant, session, ref)
	if err != nil {
		return err
	}
	info, err := b.client.StatObject(ctx, b.bucket, key, minio.StatObjectOptions{VersionID: s3ObjectVersion(ref)})
	if err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "NoSuchKey", "NoSuchVersion":
			return contracts.NewError(contracts.ErrNotFound, "journal blob not found")
		default:
			return s3Unavailable()
		}
	}
	if info.Size != ref.SizeBytes || s3Version(info.VersionID) != ref.Version || info.Metadata.Get("X-Amz-Meta-Sha256") != ref.SHA256 {
		return badS3Ref()
	}
	return nil
}

func (b *S3Blobs) read(ctx context.Context, tenant, session string, ref BlobRef) ([]byte, error) {
	key, err := b.objectKey(tenant, session, ref)
	if err != nil {
		return nil, err
	}
	object, err := b.client.GetObject(ctx, b.bucket, key, minio.GetObjectOptions{VersionID: s3ObjectVersion(ref)})
	if err != nil {
		return nil, s3Unavailable()
	}
	defer object.Close()
	data, err := io.ReadAll(io.LimitReader(object, ref.SizeBytes+1))
	if err != nil {
		switch minio.ToErrorResponse(err).Code {
		case "NoSuchKey", "NoSuchVersion":
			return nil, contracts.NewError(contracts.ErrNotFound, "journal blob not found")
		default:
			return nil, s3Unavailable()
		}
	}
	digest := sha256.Sum256(data)
	if int64(len(data)) != ref.SizeBytes || hex.EncodeToString(digest[:]) != ref.SHA256 {
		return nil, badS3Ref()
	}
	return data, nil
}

func s3BlobPrefix(tenant, session string) string {
	return s3BlobRoot + base64.RawURLEncoding.EncodeToString([]byte(tenant)) + "/" +
		base64.RawURLEncoding.EncodeToString([]byte(session)) + "/"
}

func (b *S3Blobs) retireSession(ctx context.Context, tenant, session string) error {
	prefix := s3BlobPrefix(tenant, session)
	for obj := range b.client.ListObjects(ctx, b.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true, WithVersions: true}) {
		if obj.Err != nil {
			return s3Unavailable()
		}
		if err := b.client.RemoveObject(ctx, b.bucket, obj.Key, minio.RemoveObjectOptions{VersionID: obj.VersionID}); err != nil {
			return s3Unavailable()
		}
	}
	return nil
}

// SweepOrphanBlobs removes unpublished uploads after their staging deadline.
// The row lock is shared with stage and commit. A live lease or any journal
// reference protects the object from collection.
func (s *Store) SweepOrphanBlobs(ctx context.Context, tenant string, cutoff time.Time) error {
	if s.s3 == nil || !validID(tenant) || cutoff.IsZero() {
		return contracts.NewError(contracts.ErrInvalid, "invalid journal S3 sweep")
	}
	prefix := s3BlobRoot + base64.RawURLEncoding.EncodeToString([]byte(tenant)) + "/"
	options := minio.ListObjectsOptions{Prefix: prefix, Recursive: true}
	for obj := range s.s3.client.ListObjects(ctx, s.s3.bucket, options) {
		if obj.Err != nil {
			return s3Unavailable()
		}
		if obj.LastModified.After(cutoff) {
			continue
		}
		sessionPart, digest, ok := strings.Cut(strings.TrimPrefix(obj.Key, prefix), "/")
		sessionBytes, err := base64.RawURLEncoding.DecodeString(sessionPart)
		if err != nil || !ok || !validID(string(sessionBytes)) || len(digest) != sha256.Size*2 {
			continue
		}
		if err := s.sweepOrphanBlob(ctx, tenant, string(sessionBytes), digest, obj.Key, cutoff); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) sweepOrphanBlob(ctx context.Context, tenant, session, digest, key string, cutoff time.Time) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var protected bool
		err := tx.QueryRow(ctx, `
			SELECT coalesce(lease_expires_at>clock_timestamp(),false)
			    OR coalesce(blob_stage_at>$3,false)
			    OR (status='DELETED' AND retention_swept_at IS NULL) OR legal_hold
			FROM ahp_journal.sessions WHERE tenant_id=$1 AND session_id=$2
			FOR UPDATE SKIP LOCKED`, tenant, session, cutoff).Scan(&protected)
		if errors.Is(err, pgx.ErrNoRows) || (err == nil && protected) {
			return nil
		}
		if err != nil {
			return err
		}
		err = tx.QueryRow(ctx, `SELECT
			EXISTS (SELECT FROM ahp_journal.session_events WHERE tenant_id=$1 AND session_id=$2 AND blob_ref->>'artifact_id'=$3) OR
			EXISTS (SELECT FROM ahp_journal.checkpoints WHERE tenant_id=$1 AND session_id=$2 AND blob_ref->>'artifact_id'=$3) OR
			EXISTS (SELECT FROM ahp_journal.checkpoint_writes WHERE tenant_id=$1 AND session_id=$2 AND blob_ref->>'artifact_id'=$3) OR
			EXISTS (SELECT FROM ahp_journal.tool_intents WHERE tenant_id=$1 AND session_id=$2 AND response_blob_ref->>'artifact_id'=$3) OR
			EXISTS (SELECT FROM ahp_journal.scoped_kv WHERE tenant_id=$1 AND scope_type='session' AND scope_owner=$2 AND blob_ref->>'artifact_id'=$3)`,
			tenant, session, digest).Scan(&protected)
		if err != nil || protected {
			return err
		}
		info, err := s.s3.client.StatObject(ctx, s.s3.bucket, key, minio.StatObjectOptions{})
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return nil
		}
		if err != nil {
			return s3Unavailable()
		}
		if info.LastModified.After(cutoff) {
			return nil
		}
		if err := s.s3.client.RemoveObject(ctx, s.s3.bucket, key, minio.RemoveObjectOptions{VersionID: info.VersionID}); err != nil {
			return s3Unavailable()
		}
		return nil
	})
}

func badS3Ref() error {
	return contracts.NewError(contracts.ErrInvalid, "invalid journal S3 blob descriptor")
}

func s3Unavailable() error {
	return contracts.NewError(contracts.ErrUnavailable, "journal S3 operation failed")
}
