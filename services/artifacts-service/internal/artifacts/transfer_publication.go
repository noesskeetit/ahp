package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

type transferRowReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Only CopyFromTransferGrant may publish a transfer target. A recipient must
// not bypass its source authority check by calling the ordinary Seal route.
// Call this again in Seal's bind transaction: the upload may become transfer-
// owned while an earlier ordinary Seal is reading its object bytes.
func rejectTransferSeal(ctx context.Context, db transferRowReader, id string) error {
	var owned bool
	if err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_artifacts.checkpoint_transfer_operations WHERE target_id=$1)`, id).Scan(&owned); err != nil {
		return storageError("check transfer publication owner", err)
	}
	if owned {
		return transferDenied()
	}
	return nil
}

// verifyTransferCandidate checks copied bytes but does not publish them. The
// ordinary read gates still see an unsealed row until the caller's final TX
// binds this version and its completed operation receipt together.
func (s *Store) verifyTransferCandidate(ctx context.Context, tx pgx.Tx, target TransferIdentity, id string, expected ArtifactRef) (ArtifactRef, bool, error) {
	if err := checkArtifactWritable(ctx, tx, target.Scope, id); err != nil {
		if contracts.Code(err) == contracts.ErrNotFound {
			return ArtifactRef{}, false, transferUnknown()
		}
		return ArtifactRef{}, false, err
	}
	var ref ArtifactRef
	var object, wantedSHA string
	var wantedSize int64
	var rawLabels []byte
	var deleted bool
	err := tx.QueryRow(ctx, `SELECT id,tenant,deployment,subject,context_id,task_id,name,mime,COALESCE(version,''),COALESCE(sha256,''),COALESCE(size,0),labels,object,expected_sha,expected_size,deleted FROM ahp_artifacts.artifacts WHERE id=$1 FOR UPDATE`, id).Scan(
		&ref.ID, &ref.Tenant, &ref.Deployment, &ref.Subject, &ref.ContextID, &ref.TaskID, &ref.Name, &ref.MIME, &ref.Version, &ref.SHA256, &ref.Size, &rawLabels, &object, &wantedSHA, &wantedSize, &deleted)
	if err != nil || deleted {
		return ArtifactRef{}, false, transferUnknown()
	}
	if ref.Tenant != target.Scope.Tenant || ref.Deployment != target.Scope.Deployment || ref.Subject != target.Scope.Subject || ref.ContextID != target.Binding.ContextID || ref.TaskID != target.Binding.TaskID || ref.MIME != "application/octet-stream" || wantedSHA != expected.SHA256 || wantedSize != expected.Size {
		return ArtifactRef{}, false, transferDenied()
	}
	if err := json.Unmarshal(rawLabels, &ref.Labels); err != nil {
		return ArtifactRef{}, false, transferUnknown()
	}
	sealed := ref.Version != ""
	if sealed && (ref.SHA256 != wantedSHA || ref.Size != wantedSize) {
		return ArtifactRef{}, false, transferUnknown()
	}
	info, err := s.client.StatObject(ctx, s.bucket, object, minio.StatObjectOptions{VersionID: ref.Version})
	// With the at-rest mount on, durable bytes are ciphertext and stat size is
	// the sealed length; end-to-end integrity is still proven below by hashing
	// the decrypted stream against the plaintext digest.
	if err != nil || info.VersionID == "" || (!s.cipherOn(ref.Tenant) && info.Size != wantedSize) {
		return ArtifactRef{}, false, transferUnknown()
	}
	if sealed && info.VersionID != ref.Version {
		return ArtifactRef{}, false, transferUnknown()
	}
	obj, err := s.plaintextReader(ctx, ref.Tenant, object, info.VersionID)
	if err != nil {
		return ArtifactRef{}, false, transferUnknown()
	}
	defer obj.Close()
	hash := sha256.New()
	n, err := io.Copy(hash, io.LimitReader(obj, wantedSize+1))
	if err != nil || n != wantedSize || hex.EncodeToString(hash.Sum(nil)) != wantedSHA {
		return ArtifactRef{}, false, transferUnknown()
	}
	ref.Version, ref.SHA256, ref.Size = info.VersionID, wantedSHA, wantedSize
	return ref, sealed, nil
}

func publishTransfer(ctx context.Context, tx pgx.Tx, operation string, ref ArtifactRef, sealed bool) error {
	if !sealed {
		doc, err := json.Marshal(ref.Labels)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE ahp_artifacts.artifacts SET version=$2,sha256=$3,size=$4,labels=$5,sealed_at=now(),ingestion_state='unsupported',ingestion_detail='unsupported_media_type' WHERE id=$1 AND version IS NULL AND NOT deleted`, ref.ID, ref.Version, ref.SHA256, ref.Size, doc)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return contracts.NewError(contracts.ErrConflict, "transfer publication changed")
		}
	}
	_, err := tx.Exec(ctx, `UPDATE ahp_artifacts.checkpoint_transfer_operations SET status='complete' WHERE operation=$1`, operation)
	return err
}
