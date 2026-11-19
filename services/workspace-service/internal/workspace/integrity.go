package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// IntegrityReport is the result of verifying one revision's objects against
// the live object store.
type IntegrityReport struct {
	RevisionID string           `json:"revision_id"`
	OK         bool             `json:"ok"`
	Files      []FileIntegrity  `json:"files"`
}

// FileIntegrity records the per-file verification outcome.
type FileIntegrity struct {
	Path      string `json:"path"`
	ArtifactID string `json:"artifact_id"`
	Expected   string `json:"expected_sha256"`
	Actual     string `json:"actual_sha256"`
	Match      bool   `json:"match"`
}

// VerifyRevisionIntegrity reads every artifact object backing the given
// revision from the object store and verifies its SHA-256 matches the digest
// recorded in the revision's file set. This is the object-store-level
// immutability proof: it does not trust the metadata index, it hashes the
// bytes as they exist in MinIO right now.
func (s *Store) VerifyRevisionIntegrity(ctx context.Context, scope contracts.Scope, workspace, rev string) (IntegrityReport, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return IntegrityReport{}, err
	}
	r, err := s.resolveRevision(ctx, w.ID, rev)
	if err != nil {
		return IntegrityReport{}, err
	}
	files, err := queryFiles(ctx, s.pool, `
SELECT path, artifact_id, mode, digest
  FROM ahp_workspace.files WHERE workspace_id=$1 AND rev_id=$2 ORDER BY path`, w.ID, r.ID)
	if err != nil {
		return IntegrityReport{}, err
	}
	report := IntegrityReport{RevisionID: r.ID, OK: true}
	for _, f := range files {
		fi := FileIntegrity{Path: f.Path, ArtifactID: f.ArtifactID, Expected: f.Digest}
		actual, err := s.artifactSHA256(ctx, scope, f.ArtifactID)
		if err != nil {
			fi.Match = false
			fi.Actual = ""
			report.OK = false
			report.Files = append(report.Files, fi)
			continue
		}
		fi.Actual = actual
		fi.Match = actual == f.Digest
		if !fi.Match {
			report.OK = false
		}
		report.Files = append(report.Files, fi)
	}
	return report, nil
}

// artifactSHA256 reads the full object from the object store and returns its
// SHA-256 hex digest. It bypasses the artifact metadata (which is what we are
// verifying) and hashes the bytes directly.
func (s *Store) artifactSHA256(ctx context.Context, scope contracts.Scope, artifactID string) (string, error) {
	rc, _, err := s.artifacts.OpenRange(ctx, scope, artifactID, 0, 0)
	if err != nil {
		return "", fmt.Errorf("workspace: open artifact for integrity check: %w", err)
	}
	defer rc.Close()
	h := sha256.New()
	if _, err := io.Copy(h, rc); err != nil {
		return "", fmt.Errorf("workspace: read artifact for integrity check: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// BranchInfo is one named branch (e.g. a conflict branch) and its target revision.
type BranchInfo struct {
	Name    string   `json:"name"`
	RevID   string   `json:"rev_id"`
	Seq     int64    `json:"seq"`
	Digest  string   `json:"digest"`
	Author  string   `json:"author"`
	Created string   `json:"created_at"`
}

// ListConflictBranches returns all conflict branches for a workspace. Conflict
// branches are preserved for conscious merging; they are never auto-resolved
// or silently lost.
func (s *Store) ListConflictBranches(ctx context.Context, scope contracts.Scope, workspace string) ([]BranchInfo, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT b.name, b.rev_id, r.seq, r.digest, r.author_subject, r.created_at
  FROM ahp_workspace.branches b
  JOIN ahp_workspace.revisions r ON r.id = b.rev_id
 WHERE b.workspace_id=$1 AND b.name LIKE 'conflicts/%'
 ORDER BY r.created_at DESC`, w.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BranchInfo
	for rows.Next() {
		var b BranchInfo
		var createdAt time.Time
		if err := rows.Scan(&b.Name, &b.RevID, &b.Seq, &b.Digest, &b.Author, &createdAt); err != nil {
			return nil, err
		}
		b.Created = createdAt.Format(time.RFC3339)
		out = append(out, b)
	}
	return out, rows.Err()
}

// ErrIntegrityViolation is returned when a revision's objects no longer match
// their recorded digests.
var ErrIntegrityViolation = contracts.NewError(contracts.ErrConflict, "workspace: revision integrity violation detected")
