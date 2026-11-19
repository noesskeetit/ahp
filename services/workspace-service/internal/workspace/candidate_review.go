package workspace

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"path"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// CandidateCheck reports a stored check without exposing arbitrary runner output.
type CandidateCheck struct {
	Name          string `json:"name"`
	Status        string `json:"status"`
	TargetDigest  string `json:"target_digest"`
	Trusted       bool   `json:"trusted"`
	RunnerTask    string `json:"runner_task,omitempty"`
	RunnerSubject string `json:"runner_subject,omitempty"`
}

// CandidateReview is a digest-bound snapshot of a candidate and its checks.
type CandidateReview struct {
	Candidate      Candidate        `json:"candidate"`
	Files          []File           `json:"files"`
	Checks         []CandidateCheck `json:"checks"`
	OwnedArtifacts []string         `json:"owned_artifacts"`
}

// CandidateDetails checks both project membership and each source artifact's
// current read policy. Project membership cannot reveal a private draft source.
func (s *Store) CandidateDetails(ctx context.Context, scope contracts.Scope, id string) (CandidateReview, error) {
	c, err := s.getCandidate(ctx, scope, id, "read")
	if err != nil {
		return CandidateReview{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return CandidateReview{}, err
	}
	defer tx.Rollback(ctx)
	var digest, state string
	if err = tx.QueryRow(ctx, "SELECT digest,state FROM ahp_workspace.candidates WHERE id=$1 FOR SHARE", id).Scan(&digest, &state); err != nil {
		return CandidateReview{}, err
	}
	if digest != c.Digest || state != c.State {
		return CandidateReview{}, contracts.NewError(contracts.ErrConflict, "candidate changed; reload it")
	}
	files, err := queryFiles(ctx, tx, "SELECT path,artifact_id,mode,digest FROM ahp_workspace.candidate_files WHERE candidate_id=$1 ORDER BY path", id)
	if err != nil {
		return CandidateReview{}, err
	}
	rows, err := tx.Query(ctx, "SELECT name,status,target_digest,trusted,runner_task,runner_subject FROM ahp_workspace.checks WHERE candidate_id=$1 ORDER BY name", id)
	if err != nil {
		return CandidateReview{}, err
	}
	checks := []CandidateCheck{}
	for rows.Next() {
		var v CandidateCheck
		if err = rows.Scan(&v.Name, &v.Status, &v.TargetDigest, &v.Trusted, &v.RunnerTask, &v.RunnerSubject); err != nil {
			rows.Close()
			return CandidateReview{}, err
		}
		checks = append(checks, v)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return CandidateReview{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return CandidateReview{}, err
	}
	// No pool lease is held while the artifact reader may consult Identity.
	owned := []string{}
	for _, f := range files {
		ref, readErr := s.artifacts.Get(ctx, scope, f.ArtifactID)
		if readErr != nil {
			return CandidateReview{}, readErr
		}
		if ref.Subject == scope.Subject {
			owned = append(owned, ref.ID)
		}
	}
	if _, err = s.getCandidate(ctx, scope, id, "read"); err != nil {
		return CandidateReview{}, err
	}
	return CandidateReview{Candidate: c, Files: files, Checks: checks, OwnedArtifacts: owned}, nil
}

// VerifyCandidate executes byte integrity checks on sealed sources. It never
// changes checks owned by CI/test runners and never accepts a browser-supplied
// verdict. A concurrent edit invalidates this result before it can be recorded.
func (s *Store) VerifyCandidate(ctx context.Context, scope contracts.Scope, id string) (CandidateReview, error) {
	if _, err := s.getCandidate(ctx, scope, id, "candidate:check"); err != nil {
		return CandidateReview{}, err
	}
	d, err := s.CandidateDetails(ctx, scope, id)
	if err != nil {
		return CandidateReview{}, err
	}
	if d.Candidate.State != "pending" {
		return CandidateReview{}, contracts.NewError(contracts.ErrConflict, "candidate is not pending")
	}
	const maxFiles = 256
	const maxBytes int64 = 256 << 20
	if len(d.Files) > maxFiles {
		return CandidateReview{}, contracts.NewError(contracts.ErrInvalid, "integrity check supports at most 256 files")
	}
	var total int64
	for _, f := range d.Files {
		if f.Mode&0111 != 0 || !documentFile(f.Path) {
			return CandidateReview{}, contracts.NewError(contracts.ErrConflict, "source code and executable candidates require checks from a trusted test runner; the built-in integrity check supports documents and data only")
		}
		ref, err := s.artifacts.Get(ctx, scope, f.ArtifactID)
		if err != nil {
			return CandidateReview{}, err
		}
		if ref.SHA256 != strings.TrimPrefix(f.Digest, "sha256:") {
			return CandidateReview{}, contracts.NewError(contracts.ErrConflict, "candidate file digest differs from its sealed artifact")
		}
		if ref.Size <= 0 || ref.Size > maxBytes-total {
			return CandidateReview{}, contracts.NewError(contracts.ErrInvalid, "integrity check exceeds 256 MiB")
		}
		h := sha256.New()
		var n int64
		if ref.Size > 0 {
			r, actual, err := s.artifacts.OpenRange(ctx, scope, f.ArtifactID, 0, 0)
			if err != nil {
				return CandidateReview{}, err
			}
			n, err = io.Copy(h, io.LimitReader(r, ref.Size+1))
			closeErr := r.Close()
			if err != nil || closeErr != nil || n != ref.Size || actual.Version != ref.Version {
				return CandidateReview{}, contracts.NewError(contracts.ErrConflict, "sealed file could not be verified")
			}
		}
		if hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
			return CandidateReview{}, contracts.NewError(contracts.ErrConflict, "sealed file checksum mismatch")
		}
		total += n
	}
	if _, err = s.getCandidate(ctx, scope, id, "candidate:check"); err != nil {
		return CandidateReview{}, err
	}
	tx, err := s.beginWritable(ctx, scope)
	if err != nil {
		return CandidateReview{}, err
	}
	defer tx.Rollback(ctx)
	var digest, state string
	if err = tx.QueryRow(ctx, "SELECT digest,state FROM ahp_workspace.candidates WHERE id=$1 FOR UPDATE", id).Scan(&digest, &state); err != nil {
		return CandidateReview{}, err
	}
	if digest != d.Candidate.Digest || state != "pending" {
		return CandidateReview{}, contracts.NewError(contracts.ErrConflict, "candidate changed during integrity check; retry")
	}
	_, err = tx.Exec(ctx, `INSERT INTO ahp_workspace.checks(id,candidate_id,name,status,target_digest,detail)
 VALUES($1,$2,'sealed-byte-integrity','passed',$3,'Server verified sealed source SHA-256 and size; not a code test')
 ON CONFLICT(candidate_id,name) DO UPDATE SET status='passed',target_digest=EXCLUDED.target_digest,detail=EXCLUDED.detail`, contracts.NewID(), id, digest)
	if err != nil {
		return CandidateReview{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return CandidateReview{}, err
	}
	return s.CandidateDetails(ctx, scope, id)
}

// documentFile bounds the built-in human checker; it is not a substitute for CI.
func documentFile(name string) bool {
	switch strings.ToLower(path.Ext(name)) {
	case ".txt", ".md", ".csv", ".tsv", ".json", ".jsonl", ".yaml", ".yml", ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".webp":
		return true
	}
	return false
}
