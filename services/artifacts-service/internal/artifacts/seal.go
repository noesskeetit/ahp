package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Seal verifies the uploaded bytes against the declared metadata and binds
// the artifact to one exact, immutable object version.
//
// The flow is: authorize, load metadata, stat the latest object version,
// GET that exact version, stream it through SHA-256 and compare both the
// digest and the length. Only then does a single conditional UPDATE bind
// version, hash, size and labels. A repeated seal returns the original ref
// unchanged; a later overwrite of the object path can never change what an
// already-sealed id serves, because the id is pinned to its VersionID.
func (s *Store) Seal(ctx context.Context, scope contracts.Scope, id string) (ArtifactRef, error) {
	if err := scope.Validate(); err != nil {
		return ArtifactRef{}, err
	}
	if err := validateID(id); err != nil {
		return ArtifactRef{}, err
	}

	var (
		object        string
		expectedSHA   string
		expectedSize  int64
		tenant        string
		deleted       bool
		sealedVersion string
		sealedSHA     string
		contextID     string
		labelsDoc     []byte
	)
	err := s.pool.QueryRow(ctx, `
SELECT object, expected_sha, expected_size, tenant, deleted,
       COALESCE(version, ''), COALESCE(sha256, ''), labels, context_id
FROM ahp_artifacts.artifacts WHERE id = $1`, id).
		Scan(&object, &expectedSHA, &expectedSize, &tenant, &deleted,
			&sealedVersion, &sealedSHA, &labelsDoc, &contextID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ArtifactRef{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	case err != nil:
		return ArtifactRef{}, storageError("read artifact for seal", err)
	}
	if deleted || tenant != scope.Tenant {
		return ArtifactRef{}, contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if err := s.authorize(ctx, scope, "seal", "artifact:"+id); err != nil {
		return ArtifactRef{}, err
	}

	if err := s.checkReadScope(ctx, ArtifactRef{ID: id, ContextID: contextID}); err != nil {
		return ArtifactRef{}, err
	}
	if err := rejectTransferSeal(ctx, s.pool, id); err != nil {
		return ArtifactRef{}, err
	}

	// Already sealed: replay the original immutable ref, never re-verify or
	// re-bind against whatever bytes live at the path now.
	if sealedVersion != "" && sealedSHA != "" {
		ref, err := s.get(ctx, scope, id, false)
		if err != nil {
			return ArtifactRef{}, err
		}
		return s.processSealed(ctx, scope, ref)
	}

	// Latest object version is the seal candidate.
	info, err := s.client.StatObject(ctx, s.bucket, object, minio.StatObjectOptions{})
	if err != nil {
		return ArtifactRef{}, storageError("stat object", err)
	}
	versionID := info.VersionID

	// Read back that exact version and verify bytes end to end.
	obj, err := s.client.GetObject(ctx, s.bucket, object,
		minio.GetObjectOptions{VersionID: versionID})
	if err != nil {
		return ArtifactRef{}, storageError("open object version", err)
	}
	defer func() { _ = obj.Close() }()

	sum := sha256.New()
	if info.Size != expectedSize {
		return ArtifactRef{}, contracts.NewError(contracts.ErrConflict, "uploaded size mismatch")
	}
	n, err := io.Copy(sum, io.LimitReader(obj, expectedSize+1))
	if err != nil {
		return ArtifactRef{}, storageError("verify object bytes", err)
	}
	actual := hex.EncodeToString(sum.Sum(nil))
	if n != expectedSize || actual != expectedSHA {
		return ArtifactRef{}, contracts.NewError(contracts.ErrConflict,
			"uploaded bytes do not match the declared sha256/size")
	}
	// At-rest mount: with an enabled operator profile the verified plaintext
	// version is transformed into a sealed version of the SAME object before
	// the bind, so every sealed artifact's durable bytes are envelope
	// ciphertext under the tenant key. Fail-closed: a missing/broken key
	// authority leaves the artifact unsealed, never bound as plaintext.
	// The bound sha256/size stay the plaintext digest/length by design:
	// dedup, seal identity and range semantics are plaintext-based.
	plainVersionID := versionID
	if s.cipherOn(tenant) {
		versionID, err = s.sealTransform(ctx, tenant, object, versionID, expectedSize)
		if err != nil {
			return ArtifactRef{}, err
		}
	}

	// Labels are frozen at seal time from the stored (already merged) set.
	var frozen labels.Set
	if len(labelsDoc) > 0 {
		if err := json.Unmarshal(labelsDoc, &frozen); err != nil {
			return ArtifactRef{}, contracts.NewError(contracts.ErrConflict, "stored labels are unreadable")
		}
	}
	grantLabels, err := s.callerLabels(ctx, scope)
	if err != nil {
		return ArtifactRef{}, err
	}
	frozen = labels.Merge(frozen, grantLabels)
	labelsDoc, err = json.Marshal(frozen)
	if err != nil {
		return ArtifactRef{}, err
	}
	// Admission merges the frozen labels with the caller's current grant
	// labels and enforces any workspace membership, so a seal cannot bind
	// bytes to a visibility the caller no longer holds.
	if _, err := s.admit(ctx, scope, frozen); err != nil {
		return ArtifactRef{}, err
	}

	var bound bool
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := rejectTransferSeal(ctx, tx, id); err != nil {
			return err
		}
		if err := checkArtifactWritable(ctx, tx, scope, id); err != nil {
			return err
		}
		// The conditional UPDATE is the atomic bind: only one caller can
		// move a row from unsealed to sealed, and it can never be rewritten.
		tag, err := tx.Exec(ctx, `
UPDATE ahp_artifacts.artifacts
SET version = $1, sha256 = $2, size = $3, sealed_at = now(), labels = $5::jsonb
WHERE id = $4 AND version IS NULL AND NOT deleted`,
			versionID, actual, n, id, labelsDoc)
		if err != nil {
			return storageError("bind artifact version", err)
		}
		bound = tag.RowsAffected() == 1
		return nil
	})
	if err != nil {
		return ArtifactRef{}, err
	}
	if !bound {
		// Lost the race (or a delete landed): the winner's binding stands.
		ref, err := s.get(ctx, scope, id, false)
		if err != nil {
			return ArtifactRef{}, err
		}
		return s.processSealed(ctx, scope, ref)
	}

	ref, err := s.get(ctx, scope, id, false)
	if err != nil {
		return ArtifactRef{}, err
	}
	ref.Labels = frozen
	// At-rest mount: once the sealed version is the bound one, retire the
	// plaintext candidate version. Best effort (GC would collect it anyway),
	// but it keeps the durable window for plaintext as small as possible:
	// after Seal the only version of a mounted artifact's object is the
	// sealed one.
	if plainVersionID != "" && plainVersionID != versionID {
		_ = s.client.RemoveObject(ctx, s.bucket, object, minio.RemoveObjectOptions{VersionID: plainVersionID})
	}
	return s.processSealed(ctx, scope, ref)
}
