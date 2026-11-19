package workspace

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// Publish attempts to merge a candidate into the workspace head under a
// compare-and-swap on expectedHead plus a non-overlap recheck. It never
// mutates history: on success a new head revision is appended, and on conflict
// the candidate snapshot is recorded on a conflict branch while head stays
// put. Every referenced artifact must be publishable to the workspace under
// the merging caller's current rights, otherwise the merge is refused with
// FORBIDDEN and no revision is written.
func (s *Store) Publish(ctx context.Context, scope contracts.Scope, candidateID, expectedHead string) (Revision, error) {
	if candidateID == "" {
		return Revision{}, contracts.NewError(contracts.ErrInvalid, "candidate id is required")
	}
	if err := scope.Validate(); err != nil {
		return Revision{}, err
	}
	tx, err := s.beginWritable(ctx, scope)
	if err != nil {
		return Revision{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		cand     Candidate
		wsTenant string
		wsName   string
		headRev  string
	)
	err = tx.QueryRow(ctx,
		candidateSelect+` WHERE c.id=$1 AND w.tenant=$2 FOR UPDATE OF c`,
		candidateID, scope.Tenant).Scan(
		&cand.ID, &cand.WorkspaceID, &cand.BaseRev, &cand.HeadAtCreate, &cand.Digest,
		&cand.State, &cand.MergedRev, &cand.ConflictBranch, &cand.CreatedAt)
	switch {
	case err == pgx.ErrNoRows:
		return Revision{}, contracts.NewError(contracts.ErrNotFound, "candidate not found")
	case err != nil:
		return Revision{}, err
	}
	if err := s.authorize(ctx, scope, "publish", "workspace:"+cand.WorkspaceID); err != nil {
		return Revision{}, err
	}
	if err := s.authorizeMember(ctx, scope, cand.WorkspaceID, "publish"); err != nil {
		return Revision{}, err
	}
	if cand.State != "pending" {
		return Revision{}, contracts.NewError(contracts.ErrConflict, "candidate is no longer pending")
	}

	if err := tx.QueryRow(ctx,
		`SELECT tenant, name, COALESCE(head_rev, '')
		   FROM ahp_workspace.workspaces WHERE id=$1 FOR UPDATE`,
		cand.WorkspaceID).Scan(&wsTenant, &wsName, &headRev); err != nil {
		return Revision{}, err
	}

	// Recheck 1: content digest must still match what the caller last saw.
	currentDigest, err := s.candidateDigestTx(ctx, tx, candidateID)
	if err != nil {
		return Revision{}, err
	}
	if currentDigest != cand.Digest {
		return Revision{}, contracts.NewError(contracts.ErrConflict, "candidate changed, recheck required")
	}

	files, err := queryFiles(ctx, tx, `
SELECT path, artifact_id, mode, digest
  FROM ahp_workspace.candidate_files WHERE candidate_id=$1 ORDER BY path`, candidateID)
	if err != nil {
		return Revision{}, err
	}

	// Recheck 2: every recorded check must be green and bound to the current
	// candidate digest.
	if err := s.checkChecks(ctx, tx, candidateID, currentDigest); err != nil {
		return Revision{}, err
	}

	// Recheck 2b: for code/executable candidates every check must additionally
	// carry trusted CI provenance (§3.7: проверки before CommitPublish; the
	// audit: a browser flag or the author's own verdict is not a build).
	if err := s.checkTrustedCI(ctx, tx, candidateID, files); err != nil {
		return Revision{}, err
	}

	headMatches := headRev == expectedHead
	overlaps, err := s.overlapsChanged(ctx, tx, cand.WorkspaceID, cand.BaseRev, headRev, files)
	if err != nil {
		return Revision{}, err
	}

	// Rights gate: refuse (FORBIDDEN, no revision) if any artifact may not enter
	// the workspace under the caller's current rights.
	ids := make([]string, 0, len(files))
	for _, f := range files {
		ids = append(ids, f.ArtifactID)
	}
	sink := labels.Audience{Kind: labels.KindWorkspace, Workspace: wsName}
	if err := s.artifacts.CheckPublication(ctx, scope, ids, sink); err != nil {
		return Revision{}, err
	}

	if !headMatches || overlaps {
		return s.recordConflict(ctx, tx, scope, cand, headRev, files, !headMatches, overlaps)
	}
	return s.merge(ctx, tx, scope, cand, headRev, files, currentDigest)
}

// merge appends a new head revision and moves head_rev to it.
func (s *Store) merge(ctx context.Context, tx pgx.Tx, scope contracts.Scope, cand Candidate, headRev string, files []File, digest string) (Revision, error) {
	rev, err := s.appendRevision(ctx, tx, cand.WorkspaceID, headRev, digest, files, scope.Subject)
	if err != nil {
		return Revision{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_workspace.workspaces SET head_rev=$2 WHERE id=$1`,
		cand.WorkspaceID, rev.ID); err != nil {
		return Revision{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_workspace.candidates SET state='merged', merged_rev=$2 WHERE id=$1`,
		cand.ID, rev.ID); err != nil {
		return Revision{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Revision{}, err
	}
	return rev, nil
}

// recordConflict preserves the candidate snapshot on a conflict branch without
// moving head, then reports the conflict.
func (s *Store) recordConflict(ctx context.Context, tx pgx.Tx, scope contracts.Scope, cand Candidate, headRev string, files []File, headMoved, overlaps bool) (Revision, error) {
	rev, err := s.appendRevision(ctx, tx, cand.WorkspaceID, headRev, cand.Digest, files, scope.Subject)
	if err != nil {
		return Revision{}, err
	}
	branch := "conflicts/" + cand.ID
	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_workspace.branches (workspace_id, name, rev_id) VALUES ($1,$2,$3)
		 ON CONFLICT (workspace_id, name) DO UPDATE SET rev_id = EXCLUDED.rev_id`,
		cand.WorkspaceID, branch, rev.ID); err != nil {
		return Revision{}, err
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_workspace.candidates SET state='conflict', conflict_branch=$2 WHERE id=$1`,
		cand.ID, branch); err != nil {
		return Revision{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Revision{}, err
	}
	reason := "candidate overlaps changed paths"
	if headMoved {
		reason = "head moved past expected_head"
	} else if overlaps {
		reason = "candidate overlaps changed paths"
	}
	return rev, contracts.NewError(contracts.ErrConflict, "publish conflict: "+reason)
}

// appendRevision writes a new immutable revision and its file set, returning
// the revision. Callers decide whether to move head_rev.
func (s *Store) appendRevision(ctx context.Context, tx pgx.Tx, workspaceID, parent, digest string, files []File, author string) (Revision, error) {
	var seq int64
	if err := tx.QueryRow(ctx,
		`SELECT COALESCE(MAX(seq), 0) + 1 FROM ahp_workspace.revisions WHERE workspace_id=$1`,
		workspaceID).Scan(&seq); err != nil {
		return Revision{}, err
	}
	rev := Revision{
		ID: contracts.NewID(), WorkspaceID: workspaceID, Seq: seq,
		ParentRev: parent, Digest: digest, Author: author, CreatedAt: time.Now().UTC(),
	}
	parentArg := any(nil)
	if parent != "" {
		parentArg = parent
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_workspace.revisions (id, workspace_id, seq, parent_rev, digest, author_subject)
		 VALUES ($1,$2,$3,$4,$5,$6)`,
		rev.ID, workspaceID, seq, parentArg, digest, author); err != nil {
		return Revision{}, err
	}
	for _, f := range files {
		if _, err := tx.Exec(ctx,
			`INSERT INTO ahp_workspace.files (workspace_id, rev_id, path, artifact_id, mode, digest)
			 VALUES ($1,$2,$3,$4,$5,$6)`,
			workspaceID, rev.ID, f.Path, f.ArtifactID, f.Mode, f.Digest); err != nil {
			return Revision{}, err
		}
	}
	return rev, nil
}

// checkChecks verifies that every recorded check is green and targeted the
// candidate's current digest.
func (s *Store) checkChecks(ctx context.Context, tx pgx.Tx, candidateID, digest string) error {
	var total, stale int
	if err := tx.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE status <> 'passed' OR target_digest <> $2)
		   FROM ahp_workspace.checks WHERE candidate_id=$1`,
		candidateID, digest).Scan(&total, &stale); err != nil {
		return err
	}
	if total == 0 {
		return contracts.NewError(contracts.ErrConflict, "candidate has no recorded checks")
	}
	if stale > 0 {
		return contracts.NewError(contracts.ErrConflict, "candidate has failed or stale checks")
	}
	return nil
}

// overlapsChanged reports whether any candidate path was changed on head since
// the candidate's base. A base of "" is treated as an empty set, so any path
// that now exists on head counts as changed.
func (s *Store) overlapsChanged(ctx context.Context, tx pgx.Tx, workspaceID, baseRev, headRev string, files []File) (bool, error) {
	if headRev == "" {
		return false, nil
	}
	changed, err := s.changedPaths(ctx, tx, workspaceID, baseRev, headRev)
	if err != nil {
		return false, err
	}
	for _, f := range files {
		if _, ok := changed[f.Path]; ok {
			return true, nil
		}
	}
	return false, nil
}

// changedPaths returns the set of paths whose content differs between baseRev
// and headRev (present-and-different, or present on only one side).
func (s *Store) changedPaths(ctx context.Context, tx pgx.Tx, workspaceID, baseRev, headRev string) (map[string]struct{}, error) {
	base, err := s.revContent(ctx, tx, workspaceID, baseRev)
	if err != nil {
		return nil, err
	}
	head, err := s.revContent(ctx, tx, workspaceID, headRev)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{})
	for p, d := range head {
		if bd, ok := base[p]; !ok || bd != d {
			out[p] = struct{}{}
		}
	}
	for p, d := range base {
		if _, ok := head[p]; !ok {
			_ = d
			out[p] = struct{}{}
		}
	}
	return out, nil
}

// revContent maps path -> (artifact_id||digest) for a revision. An empty revID
// yields an empty map (the "no base" case).
func (s *Store) revContent(ctx context.Context, tx pgx.Tx, workspaceID, revID string) (map[string]string, error) {
	out := map[string]string{}
	if revID == "" {
		return out, nil
	}
	rows, err := tx.Query(ctx,
		`SELECT path, artifact_id || '@' || digest
		   FROM ahp_workspace.files WHERE workspace_id=$1 AND rev_id=$2`,
		workspaceID, revID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var p, v string
		if err := rows.Scan(&p, &v); err != nil {
			return nil, err
		}
		out[p] = v
	}
	return out, rows.Err()
}
