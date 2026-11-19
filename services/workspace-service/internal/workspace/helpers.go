package workspace

import (
	"context"
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx so file reads can run
// either standalone or inside a transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// queryFiles runs a query returning (path, artifact_id, mode, digest) columns.
func queryFiles(ctx context.Context, q querier, sql string, args ...any) ([]File, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []File
	for rows.Next() {
		var f File
		if err := rows.Scan(&f.Path, &f.ArtifactID, &f.Mode, &f.Digest); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// resolveRevision resolves a revision by id or by numeric seq within a known
// workspace. Empty idOrSeq with a head is handled by callers.
func (s *Store) resolveRevision(ctx context.Context, workspaceID, idOrSeq string) (Revision, error) {
	var r Revision
	var q string
	var arg any
	if n, err := strconv.ParseInt(idOrSeq, 10, 64); err == nil {
		q = revisionSelect + ` WHERE r.workspace_id=$1 AND r.seq=$2`
		arg = n
	} else {
		q = revisionSelect + ` WHERE r.workspace_id=$1 AND r.id=$2`
		arg = idOrSeq
	}
	err := s.pool.QueryRow(ctx, q, workspaceID, arg).Scan(
		&r.ID, &r.WorkspaceID, &r.Seq, &r.ParentRev, &r.Digest, &r.Author, &r.CreatedAt)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Revision{}, contracts.NewError(contracts.ErrNotFound, "revision not found")
	case err != nil:
		return Revision{}, err
	}
	return r, nil
}

const revisionSelect = `
SELECT r.id, r.workspace_id, r.seq, COALESCE(r.parent_rev, ''), r.digest,
       r.author_subject, r.created_at
  FROM ahp_workspace.revisions r`
