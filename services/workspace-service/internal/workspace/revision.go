package workspace

import (
	"context"
	"errors"
	"io"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// GetHead returns the current head revision id of a workspace, or "" when the
// workspace has never had a revision merged.
func (s *Store) GetHead(ctx context.Context, scope contracts.Scope, workspace string) (string, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return "", err
	}
	return w.HeadRev, nil
}

// GetRevision returns one immutable revision identified by id or numeric seq.
func (s *Store) GetRevision(ctx context.Context, scope contracts.Scope, workspace, rev string) (Revision, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return Revision{}, err
	}
	return s.resolveRevision(ctx, w.ID, rev)
}

// ListRevisions returns a workspace's revisions, newest sequence first.
func (s *Store) ListRevisions(ctx context.Context, scope contracts.Scope, workspace string) ([]Revision, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx,
		revisionSelect+` WHERE r.workspace_id=$1 ORDER BY r.seq DESC`, w.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Revision
	for rows.Next() {
		var r Revision
		if err := rows.Scan(&r.ID, &r.WorkspaceID, &r.Seq, &r.ParentRev,
			&r.Digest, &r.Author, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListFiles returns the file set of an immutable revision.
func (s *Store) ListFiles(ctx context.Context, scope contracts.Scope, workspace, rev string) ([]File, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return nil, err
	}
	r, err := s.resolveRevision(ctx, w.ID, rev)
	if err != nil {
		return nil, err
	}
	return queryFiles(ctx, s.pool, `
SELECT path, artifact_id, mode, digest
  FROM ahp_workspace.files WHERE workspace_id=$1 AND rev_id=$2 ORDER BY path`, w.ID, r.ID)
}

// ReadFile proxies a byte range of the artifact backing a path in a revision.
// The reader's current rights are re-checked by the artifact store on every
// call, so a fact whose labels tightened since the commit can no longer be
// streamed. offset/count follow OpenRange semantics (count==0 means to end).
func (s *Store) ReadFile(ctx context.Context, scope contracts.Scope, workspace, rev, path string, offset, count int64) (io.ReadCloser, File, int64, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return nil, File{}, 0, err
	}
	r, err := s.resolveRevision(ctx, w.ID, rev)
	if err != nil {
		return nil, File{}, 0, err
	}
	var f File
	err = s.pool.QueryRow(ctx,
		`SELECT path, artifact_id, mode, digest
		   FROM ahp_workspace.files
		  WHERE workspace_id=$1 AND rev_id=$2 AND path=$3`,
		w.ID, r.ID, path).Scan(&f.Path, &f.ArtifactID, &f.Mode, &f.Digest)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, File{}, 0, contracts.NewError(contracts.ErrNotFound, "file not found")
	case err != nil:
		return nil, File{}, 0, err
	}
	rc, ref, err := s.artifacts.OpenRange(ctx, scope, f.ArtifactID, offset, count)
	if err != nil {
		return nil, File{}, 0, err
	}
	// Trust the sealed artifact identity over any stale digest on the row.
	f.Digest = ref.SHA256
	return rc, f, ref.Size, nil
}

// Pin marks a revision as pinned by the caller so it cannot be purged.
func (s *Store) Pin(ctx context.Context, scope contracts.Scope, workspace, rev string) error {
	w, err := s.requireMember(ctx, scope, workspace, "write")
	if err != nil {
		return err
	}
	r, err := s.resolveRevision(ctx, w.ID, rev)
	if err != nil {
		return err
	}
	_, err = s.writeExec(ctx, scope,
		`INSERT INTO ahp_workspace.pins (workspace_id, rev_id, subject) VALUES ($1,$2,$3)
		 ON CONFLICT DO NOTHING`, w.ID, r.ID, scope.Subject)
	return err
}

// Unpin removes the caller's pin from a revision.
func (s *Store) Unpin(ctx context.Context, scope contracts.Scope, workspace, rev string) error {
	w, err := s.requireMember(ctx, scope, workspace, "write")
	if err != nil {
		return err
	}
	r, err := s.resolveRevision(ctx, w.ID, rev)
	if err != nil {
		return err
	}
	_, err = s.writeExec(ctx, scope,
		`DELETE FROM ahp_workspace.pins WHERE workspace_id=$1 AND rev_id=$2 AND subject=$3`,
		w.ID, r.ID, scope.Subject)
	return err
}

// GetBranch returns the revision a named branch (e.g. a conflict branch) points
// at.
func (s *Store) GetBranch(ctx context.Context, scope contracts.Scope, workspace, branch string) (Revision, error) {
	w, err := s.requireMember(ctx, scope, workspace, "read")
	if err != nil {
		return Revision{}, err
	}
	var revID string
	err = s.pool.QueryRow(ctx,
		`SELECT rev_id FROM ahp_workspace.branches WHERE workspace_id=$1 AND name=$2`,
		w.ID, branch).Scan(&revID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Revision{}, contracts.NewError(contracts.ErrNotFound, "branch not found")
	case err != nil:
		return Revision{}, err
	}
	return s.resolveRevision(ctx, w.ID, revID)
}

// PurgeRevision deletes an immutable revision that is not head, not pinned,
// has no children and is not the base of a live working copy. It never
// rewrites history that is still reachable: the PDF forbids deleting a
// revision while at least one working copy uses it (§7 с.48). A
// conflict-branch tip with no working copy left is still purgeable — that is
// how an abandoned conflict is discarded — while the artifact collector keeps
// branch bytes alive through the reference scan for as long as the branch
// row exists.
func (s *Store) PurgeRevision(ctx context.Context, scope contracts.Scope, workspace, rev string) error {
	w, err := s.requireMember(ctx, scope, workspace, "purge")
	if err != nil {
		return err
	}
	r, err := s.resolveRevision(ctx, w.ID, rev)
	if err != nil {
		return err
	}
	if w.HeadRev == r.ID {
		return contracts.NewError(contracts.ErrConflict, "cannot purge the head revision")
	}
	var pins, children int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_workspace.pins WHERE workspace_id=$1 AND rev_id=$2`,
		w.ID, r.ID).Scan(&pins); err != nil {
		return err
	}
	if pins > 0 {
		return contracts.NewError(contracts.ErrConflict, "revision is pinned")
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_workspace.revisions WHERE workspace_id=$1 AND parent_rev=$2`,
		w.ID, r.ID).Scan(&children); err != nil {
		return err
	}
	if children > 0 {
		return contracts.NewError(contracts.ErrConflict, "revision has children")
	}
	var workingCopies int
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_workspace.candidates
		  WHERE workspace_id=$1 AND base_rev=$2 AND state IN ('pending','conflict')`,
		w.ID, r.ID).Scan(&workingCopies); err != nil {
		return err
	}
	if workingCopies > 0 {
		return contracts.NewError(contracts.ErrConflict, "revision is in use by a working copy")
	}
	_, err = s.writeExec(ctx, scope,
		`DELETE FROM ahp_workspace.revisions WHERE workspace_id=$1 AND id=$2`, w.ID, r.ID)
	return err
}
