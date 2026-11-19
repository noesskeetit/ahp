package workspace

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// filesDigest is the canonical content digest of a candidate file set. The set
// is sorted by path first so the digest is order-independent for callers while
// remaining stable for the same content.
func filesDigest(files []File) (string, error) {
	cp := append([]File(nil), files...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Path < cp[j].Path })
	seen := make(map[string]struct{}, len(cp))
	for _, f := range cp {
		if _, dup := seen[f.Path]; dup {
			return "", contracts.NewError(contracts.ErrInvalid, "duplicate path "+f.Path)
		}
		seen[f.Path] = struct{}{}
	}
	return contracts.Digest(cp)
}

// validateFiles checks each path and rejects duplicate paths before any write.
func validateFiles(files []File) error {
	seen := make(map[string]struct{}, len(files))
	for _, f := range files {
		if err := validatePath(f.Path); err != nil {
			return err
		}
		if f.ArtifactID == "" {
			return contracts.NewError(contracts.ErrInvalid, "file "+f.Path+" has no artifact")
		}
		if _, dup := seen[f.Path]; dup {
			return contracts.NewError(contracts.ErrInvalid, "duplicate path "+f.Path)
		}
		seen[f.Path] = struct{}{}
	}
	return nil
}

// CreateCandidate opens a copy-on-write working set against an immutable base
// revision. baseRev == "" starts from an empty set; otherwise the base revision
// file set is copied into the candidate. History is never mutated.
func (s *Store) CreateCandidate(ctx context.Context, scope contracts.Scope, workspace, baseRev string) (Candidate, error) {
	w, err := s.requireMember(ctx, scope, workspace, "candidate:create")
	if err != nil {
		return Candidate{}, err
	}
	base := ""
	if baseRev != "" {
		rev, err := s.resolveRevision(ctx, w.ID, baseRev)
		if err != nil {
			return Candidate{}, err
		}
		base = rev.ID
	}
	cand := Candidate{
		ID: contracts.NewID(), WorkspaceID: w.ID, BaseRev: base,
		HeadAtCreate: w.HeadRev, State: "pending", CreatedAt: time.Now().UTC(),
	}
	tx, err := s.beginWritable(ctx, scope)
	if err != nil {
		return Candidate{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_workspace.candidates
		   (id, workspace_id, base_rev, head_at_create, digest, state)
		 VALUES ($1,$2,$3,$4,$5,'pending')`,
		cand.ID, cand.WorkspaceID, cand.BaseRev, cand.HeadAtCreate, ""); err != nil {
		return Candidate{}, err
	}
	if base != "" {
		if _, err := tx.Exec(ctx, `
INSERT INTO ahp_workspace.candidate_files (candidate_id, path, artifact_id, mode, digest)
SELECT $1, path, artifact_id, mode, digest
  FROM ahp_workspace.files WHERE workspace_id=$2 AND rev_id=$3`,
			cand.ID, w.ID, base); err != nil {
			return Candidate{}, err
		}
	}
	digest, err := s.candidateDigestTx(ctx, tx, cand.ID)
	if err != nil {
		return Candidate{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_workspace.candidates SET digest=$2 WHERE id=$1`, cand.ID, digest); err != nil {
		return Candidate{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Candidate{}, err
	}
	cand.Digest = digest
	return cand, nil
}

// UpdateCandidateFiles replaces a candidate's file set and clears its checks,
// forcing re-validation. A candidate that is no longer pending cannot change.
func (s *Store) UpdateCandidateFiles(ctx context.Context, scope contracts.Scope, candidateID string, files []File) (Candidate, error) {
	if err := validateFiles(files); err != nil {
		return Candidate{}, err
	}
	cand, err := s.getCandidate(ctx, scope, candidateID, "candidate:write")
	if err != nil {
		return Candidate{}, err
	}
	if cand.State != "pending" {
		return Candidate{}, contracts.NewError(contracts.ErrConflict, "candidate is no longer mutable")
	}
	tx, err := s.beginWritable(ctx, scope)
	if err != nil {
		return Candidate{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx,
		`DELETE FROM ahp_workspace.candidate_files WHERE candidate_id=$1`, candidateID); err != nil {
		return Candidate{}, err
	}
	for _, f := range files {
		if _, err := tx.Exec(ctx,
			`INSERT INTO ahp_workspace.candidate_files (candidate_id, path, artifact_id, mode, digest)
			 VALUES ($1,$2,$3,$4,$5)`,
			candidateID, f.Path, f.ArtifactID, f.Mode, f.Digest); err != nil {
			return Candidate{}, err
		}
	}
	// Any prior checks targeted the old content; drop them so a merge cannot
	// reuse a stale green check.
	if _, err := tx.Exec(ctx,
		`DELETE FROM ahp_workspace.checks WHERE candidate_id=$1`, candidateID); err != nil {
		return Candidate{}, err
	}
	digest, err := s.candidateDigestTx(ctx, tx, candidateID)
	if err != nil {
		return Candidate{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_workspace.candidates SET digest=$2 WHERE id=$1`, candidateID, digest); err != nil {
		return Candidate{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Candidate{}, err
	}
	cand.Digest = digest
	return cand, nil
}

// SetCheck records the result of a named validation for a candidate. The check
// is bound to the candidate's current digest: if the file set changes, the
// check no longer applies (UpdateCandidateFiles deletes checks anyway).
func (s *Store) SetCheck(ctx context.Context, scope contracts.Scope, candidateID, name, status, detail string) error {
	cand, err := s.getCandidate(ctx, scope, candidateID, "candidate:check")
	if err != nil {
		return err
	}
	if name == "" {
		return contracts.NewError(contracts.ErrInvalid, "check name is required")
	}
	switch status {
	case "pending", "passed", "failed":
	default:
		return contracts.NewError(contracts.ErrInvalid, "check status must be pending, passed or failed")
	}
	if cand.State != "pending" {
		return contracts.NewError(contracts.ErrConflict, "candidate is no longer pending")
	}
	_, err = s.writeExec(ctx, scope, `
INSERT INTO ahp_workspace.checks (id, candidate_id, name, status, target_digest, detail)
VALUES ($1,$2,$3,$4,$5,$6)
ON CONFLICT (candidate_id, name) DO UPDATE
   SET status = EXCLUDED.status, target_digest = EXCLUDED.target_digest, detail = EXCLUDED.detail`,
		contracts.NewID(), candidateID, name, status, cand.Digest, detail)
	return err
}

// RejectCandidate closes a pending candidate without merging it.
func (s *Store) RejectCandidate(ctx context.Context, scope contracts.Scope, candidateID string) (Candidate, error) {
	cand, err := s.getCandidate(ctx, scope, candidateID, "candidate:reject")
	if err != nil {
		return Candidate{}, err
	}
	if cand.State != "pending" {
		return Candidate{}, contracts.NewError(contracts.ErrConflict, "candidate is no longer pending")
	}
	if _, err := s.writeExec(ctx, scope,
		`UPDATE ahp_workspace.candidates SET state='rejected' WHERE id=$1 AND state='pending'`,
		candidateID); err != nil {
		return Candidate{}, err
	}
	cand.State = "rejected"
	return cand, nil
}

// GetCandidate returns one candidate visible to the caller.
func (s *Store) GetCandidate(ctx context.Context, scope contracts.Scope, candidateID string) (Candidate, error) {
	return s.getCandidate(ctx, scope, candidateID, "read")
}

// ListCandidates returns candidates for a workspace in the given states
// (empty means all). Newest first.
func (s *Store) ListCandidates(ctx context.Context, scope contracts.Scope, workspace string, states []string) ([]Candidate, error) {
	w, err := s.requireMember(ctx, scope, workspace, "candidate:list")
	if err != nil {
		return nil, err
	}
	if len(states) == 0 {
		states = []string{"pending", "merged", "rejected", "conflict"}
	}
	rows, err := s.pool.Query(ctx,
		candidateSelect+`
 WHERE c.workspace_id=$1 AND c.state = ANY($2)
 ORDER BY c.created_at DESC`, w.ID, states)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanCandidates(rows)
}

// getCandidate loads a candidate after verifying the caller is a member of its
// workspace.
func (s *Store) getCandidate(ctx context.Context, scope contracts.Scope, candidateID, action string) (Candidate, error) {
	if candidateID == "" {
		return Candidate{}, contracts.NewError(contracts.ErrInvalid, "candidate id is required")
	}
	var c Candidate
	err := s.pool.QueryRow(ctx,
		candidateSelect+`
 WHERE c.id=$1 AND w.tenant=$2`,
		candidateID, scope.Tenant).Scan(
		&c.ID, &c.WorkspaceID, &c.BaseRev, &c.HeadAtCreate, &c.Digest,
		&c.State, &c.MergedRev, &c.ConflictBranch, &c.CreatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Candidate{}, contracts.NewError(contracts.ErrNotFound, "candidate not found")
	case err != nil:
		return Candidate{}, err
	}
	if err := s.authorize(ctx, scope, action, "workspace:"+c.WorkspaceID); err != nil {
		return Candidate{}, err
	}
	if err := s.authorizeMember(ctx, scope, c.WorkspaceID, action); err != nil {
		return Candidate{}, err
	}
	return c, nil
}

const candidateSelect = `
SELECT c.id, c.workspace_id, c.base_rev, c.head_at_create, c.digest,
       c.state, c.merged_rev, c.conflict_branch, c.created_at
  FROM ahp_workspace.candidates c
  JOIN ahp_workspace.workspaces w ON w.id = c.workspace_id`

func scanCandidates(rows pgx.Rows) ([]Candidate, error) {
	var out []Candidate
	for rows.Next() {
		var c Candidate
		if err := rows.Scan(&c.ID, &c.WorkspaceID, &c.BaseRev, &c.HeadAtCreate,
			&c.Digest, &c.State, &c.MergedRev, &c.ConflictBranch, &c.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// candidateDigestTx recomputes a candidate's digest from its stored files.
func (s *Store) candidateDigestTx(ctx context.Context, tx pgx.Tx, candidateID string) (string, error) {
	files, err := queryFiles(ctx, tx, `
SELECT path, artifact_id, mode, digest
  FROM ahp_workspace.candidate_files WHERE candidate_id=$1 ORDER BY path`, candidateID)
	if err != nil {
		return "", err
	}
	return filesDigest(files)
}
