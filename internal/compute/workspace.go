package compute

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ErrWorkspaceConflict is Compute's own code for a copy-on-write commit whose
// expected head does not match the live head. It is deliberately distinct from
// a generic conflict so the API surfaces "your base is stale, a conflict branch
// was opened" instead of a silent overwrite.
const ErrWorkspaceConflict = "WORKSPACE_CONFLICT"

func workspaceConflict(msg string) error { return contracts.NewError(ErrWorkspaceConflict, msg) }

const envRevisionSelect = `id, computer_id, revision, spec, status, parent_id, error, created_at`
const workspaceRevisionSelect = `id, computer_id, revision, base_rev, digest, branch, created_at`

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

// RecordSnapshot stores a durable reference to a point-in-time snapshot whose
// bytes and digest were produced by a backend (a filesystem snapshot for the
// directory kind, a guest memory image for the memory / SBX-08 warm-boot kind).
// The store never reads guest bytes; it records the reference.
func (s *Store) RecordSnapshot(ctx context.Context, scope contracts.Scope, computerID string, kind SnapshotKind, path, digest string, sizeBytes int64) (Snapshot, error) {
	if kind != SnapshotDirectory && kind != SnapshotMemory {
		return Snapshot{}, invalid("snapshot kind must be directory or memory")
	}
	if err := validIdentifier(digest, "snapshot digest"); err != nil {
		return Snapshot{}, err
	}
	if sizeBytes < 0 {
		return Snapshot{}, invalid("snapshot size must not be negative")
	}
	id := contracts.NewID()
	var out Snapshot
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.snapshots (id, computer_id, kind, path, digest, size_bytes)
			VALUES ($1,$2,$3,$4,$5,$6)
			RETURNING id, computer_id, kind, path, digest, size_bytes, created_at`,
			id, computerID, string(kind), path, digest, sizeBytes).
			Scan(&out.ID, &out.ComputerID, &out.Kind, &out.Path, &out.Digest, &out.SizeBytes, &out.CreatedAt)
	})
	if err != nil {
		return Snapshot{}, err
	}
	out.CreatedAt = out.CreatedAt.UTC()
	return out, nil
}

// ---------------------------------------------------------------------------
// Environment / package revisions with rollback
// ---------------------------------------------------------------------------

// EnsureEnvironment guarantees that a Computer has at least a base environment
// revision and returns the current applied head. Replaying it is idempotent.
func (s *Store) EnsureEnvironment(ctx context.Context, scope contracts.Scope, computerID, spec string) (EnvRevision, error) {
	if len(spec) > maxSpecBytes {
		return EnvRevision{}, invalid("environment spec exceeds 64 KiB")
	}
	var out EnvRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		head, err := s.appliedEnvRevision(ctx, tx, computerID)
		if err == nil {
			out = head
			return nil
		}
		if contracts.Code(err) != string(contracts.ErrNotFound) {
			return err
		}
		rev, err := s.insertEnvRevision(ctx, tx, computerID, 1, spec, "applied", "")
		out = rev
		return err
	})
	return out, err
}

// BeginEnvironmentInstall opens an exclusive environment operation: it takes the
// writer lease on the working tree (rejecting concurrent writers with CONFLICT)
// and records a pending revision whose parent is the current applied head. The
// backend then installs the spec. The caller must settle it with
// FinishEnvironmentInstall; a failure rolls the head back to the parent.
func (s *Store) BeginEnvironmentInstall(ctx context.Context, scope contracts.Scope, computerID, spec, holder string) (EnvRevision, error) {
	if err := validIdentifier(holder, "writer holder"); err != nil {
		return EnvRevision{}, err
	}
	if len(spec) > maxSpecBytes {
		return EnvRevision{}, invalid("environment spec exceeds 64 KiB")
	}
	var out EnvRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		if err := s.claimWriterLease(ctx, tx, computerID, holder); err != nil {
			return err
		}
		parent := ""
		var next int64 = 1
		if head, err := s.appliedEnvRevision(ctx, tx, computerID); err == nil {
			parent = head.ID
		} else if contracts.Code(err) != string(contracts.ErrNotFound) {
			return err
		}
		// The next revision number is monotonically unique per Computer even
		// across rolled-back attempts, so numbering follows the max, not the
		// applied head.
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(max(revision), 0) + 1
			FROM ahp_compute.env_revisions WHERE computer_id = $1`,
			computerID).Scan(&next); err != nil {
			return err
		}
		rev, err := s.insertEnvRevision(ctx, tx, computerID, next, spec, "pending", parent)
		out = rev
		return err
	})
	return out, err
}

// FinishEnvironmentInstall settles a pending revision. On success it becomes the
// applied head. On failure the revision is marked rolled_back with its error and
// the applied head stays the parent — the caller observes a rollback, never a
// half-applied environment. The writer lease is always released.
func (s *Store) FinishEnvironmentInstall(ctx context.Context, scope contracts.Scope, computerID, revisionID, holder string, ok bool, errMsg string) (EnvRevision, error) {
	var out EnvRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		status := "applied"
		if !ok {
			status = "rolled_back"
		}
		rev, err := scanEnvRevision(tx.QueryRow(ctx, `
			UPDATE ahp_compute.env_revisions
			SET status = $2, error = $3
			WHERE id = $1 AND computer_id = $4 AND status = 'pending'
			RETURNING `+envRevisionSelect,
			revisionID, status, errMsg, computerID))
		if err != nil {
			return err
		}
		out = rev
		_, err = tx.Exec(ctx, `
			DELETE FROM ahp_compute.activity_leases
			WHERE computer_id = $1 AND kind = 'writer' AND holder = $2`, computerID, holder)
		return err
	})
	return out, err
}

// CurrentEnvironment returns the applied environment head, or NOT_FOUND if none.
func (s *Store) CurrentEnvironment(ctx context.Context, scope contracts.Scope, computerID string) (EnvRevision, error) {
	var out EnvRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		rev, err := s.appliedEnvRevision(ctx, tx, computerID)
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("environment revision")
		}
		out = rev
		return err
	})
	return out, err
}

// ---------------------------------------------------------------------------
// Copy-on-write workspace commits (project Computers)
// ---------------------------------------------------------------------------

// CommitWorkspace records a copy-on-write delta for a project Computer using
// optimistic concurrency: expectedHead must equal the live head. On a match a
// new main-branch revision is created. On a mismatch no overwrite happens — a
// conflict branch is opened and WORKSPACE_CONFLICT is returned carrying it, so
// the caller reconciles instead of losing a concurrent change.
func (s *Store) CommitWorkspace(ctx context.Context, scope contracts.Scope, computerID string, expectedHead int64, digest string) (WorkspaceRevision, error) {
	if err := validIdentifier(digest, "workspace digest"); err != nil {
		return WorkspaceRevision{}, err
	}
	if expectedHead < 0 {
		return WorkspaceRevision{}, invalid("expected head must not be negative")
	}
	var out WorkspaceRevision
	var conflict error
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		c, err := s.getComputer(ctx, tx, scope, computerID)
		if err != nil {
			return err
		}
		if c.Profile != ProfileProject {
			return contracts.NewError(contracts.ErrForbidden,
				"copy-on-write commits apply only to project computers")
		}
		var head int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(max(revision), 0) FROM ahp_compute.workspace_revisions
			WHERE computer_id = $1 AND branch = 'main'`, computerID).Scan(&head); err != nil {
			return err
		}
		if head != expectedHead {
			branch := "conflict-" + contracts.NewID()
			rev := WorkspaceRevision{
				ID: contracts.NewID(), ComputerID: computerID,
				Revision: head + 1, BaseRev: head, Digest: digest, Branch: branch,
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO ahp_compute.workspace_revisions (id, computer_id, revision, base_rev, digest, branch)
				VALUES ($1,$2,$3,$4,$5,$6)`,
				rev.ID, computerID, rev.Revision, rev.BaseRev, digest, branch); err != nil {
				return err
			}
			out = rev
			conflict = workspaceConflict(fmt.Sprintf(
				"workspace head moved to %d; opened conflict branch %s", head, branch))
			return conflict
		}
		created, err := scanWorkspaceRevision(tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.workspace_revisions (id, computer_id, revision, base_rev, digest, branch)
			VALUES ($1,$2,$3,$4,$5,'main')
			RETURNING `+workspaceRevisionSelect,
			contracts.NewID(), computerID, expectedHead+1, expectedHead, digest))
		out = created
		return err
	})
	if conflict != nil {
		return out, conflict
	}
	return out, err
}

// WorkspaceHead returns the current main-branch workspace revision, or a
// zero-revision sentinel when nothing has been committed yet.
func (s *Store) WorkspaceHead(ctx context.Context, scope contracts.Scope, computerID string) (WorkspaceRevision, error) {
	var out WorkspaceRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `SELECT `+workspaceRevisionSelect+`
			FROM ahp_compute.workspace_revisions
			WHERE computer_id = $1 AND branch = 'main'
			ORDER BY revision DESC LIMIT 1`, computerID)
		rev, err := scanWorkspaceRevision(row)
		if contracts.Code(err) == string(contracts.ErrNotFound) {
			out = WorkspaceRevision{ComputerID: computerID, Revision: 0, Branch: "main"}
			return nil
		}
		out = rev
		return err
	})
	return out, err
}

// ---------------------------------------------------------------------------
// internal helpers
// ---------------------------------------------------------------------------

func (s *Store) claimWriterLease(ctx context.Context, tx pgx.Tx, computerID, holder string) error {
	var busy bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM ahp_compute.activity_leases
			WHERE computer_id = $1 AND kind = 'writer' AND expires > clock_timestamp() AND holder <> $2)`,
		computerID, holder).Scan(&busy); err != nil {
		return err
	}
	if busy {
		return contracts.NewError(contracts.ErrConflict,
			"the working tree is held by another writer lease")
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO ahp_compute.activity_leases (id, computer_id, kind, holder, expires)
		VALUES ($1,$2,'writer',$3, clock_timestamp() + interval '15 minutes')
		ON CONFLICT (computer_id, kind, holder)
		DO UPDATE SET expires = clock_timestamp() + interval '15 minutes'`,
		contracts.NewID(), computerID, holder)
	return err
}

func (s *Store) appliedEnvRevision(ctx context.Context, tx pgx.Tx, computerID string) (EnvRevision, error) {
	return scanEnvRevision(tx.QueryRow(ctx, `SELECT `+envRevisionSelect+`
		FROM ahp_compute.env_revisions
		WHERE computer_id = $1 AND status = 'applied'
		ORDER BY revision DESC LIMIT 1`, computerID))
}

func (s *Store) insertEnvRevision(ctx context.Context, tx pgx.Tx, computerID string, revision int64, spec, status, parent string) (EnvRevision, error) {
	return scanEnvRevision(tx.QueryRow(ctx, `
		INSERT INTO ahp_compute.env_revisions (id, computer_id, revision, spec, status, parent_id)
		VALUES ($1,$2,$3,$4,$5,$6)
		RETURNING `+envRevisionSelect,
		contracts.NewID(), computerID, revision, spec, status, parent))
}

func scanEnvRevision(row scanner) (EnvRevision, error) {
	var r EnvRevision
	err := row.Scan(&r.ID, &r.ComputerID, &r.Revision, &r.Spec, &r.Status,
		&r.ParentID, &r.ErrorMsg, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return EnvRevision{}, notFound("environment revision")
	}
	if err != nil {
		return EnvRevision{}, fmt.Errorf("compute: scan env revision: %w", err)
	}
	r.CreatedAt = r.CreatedAt.UTC()
	return r, nil
}

func scanWorkspaceRevision(row scanner) (WorkspaceRevision, error) {
	var r WorkspaceRevision
	err := row.Scan(&r.ID, &r.ComputerID, &r.Revision, &r.BaseRev, &r.Digest, &r.Branch, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkspaceRevision{}, notFound("workspace revision")
	}
	if err != nil {
		return WorkspaceRevision{}, fmt.Errorf("compute: scan workspace revision: %w", err)
	}
	r.CreatedAt = r.CreatedAt.UTC()
	return r, nil
}

// EnvRevisionByID returns one environment revision of the Computer.
func (s *Store) EnvRevisionByID(ctx context.Context, scope contracts.Scope, computerID, revisionID string) (EnvRevision, error) {
	var out EnvRevision
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getComputer(ctx, tx, scope, computerID); err != nil {
			return err
		}
		rev, err := scanEnvRevision(tx.QueryRow(ctx, `
			SELECT `+envRevisionSelect+` FROM ahp_compute.env_revisions
			WHERE id = $1 AND computer_id = $2`, revisionID, computerID))
		out = rev
		return err
	})
	return out, err
}
