package compute

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// drainInterval renders a drain window as an SQL interval so the deadline is
// computed by the clock that later judges it. Computing it in Go
// (time.Now().Add(window)) and comparing it in SQL (drain_deadline <= now())
// asks the application clock and the database clock to agree: with a zero window
// the deadline then sits at the app's "now", and a server whose clock is a
// fraction of a millisecond behind reports an immediately expired drain as still
// draining. One clock makes an expired drain expired, everywhere, always.
func drainInterval(window time.Duration) string {
	if window < 0 {
		window = 0
	}
	return fmt.Sprintf("%d microseconds", window.Microseconds())
}

// PublishVersion registers a new image version for an agent. If there is an
// existing active version, it transitions to drain with the given window.
// The new version becomes active immediately. The digest is the image sha256.
func (s *Store) PublishVersion(ctx context.Context, scope contracts.Scope, agent, digest string, stateCompat bool, drainWindow time.Duration) (ImageVersion, error) {
	if agent == "" {
		return ImageVersion{}, invalid("compute: agent required for version publish")
	}
	if digest == "" {
		return ImageVersion{}, invalid("compute: digest required for version publish")
	}
	var newVer ImageVersion
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Drain the current active version if one exists.
		var curID, curStatus string
		err := tx.QueryRow(ctx, `
			SELECT id, status FROM ahp_compute.image_versions
			WHERE tenant=$1 AND deployment=$2 AND agent=$3 AND status='active'
			LIMIT 1`,
			scope.Tenant, scope.Deployment, agent,
		).Scan(&curID, &curStatus)
		if err == nil {
			_, err = tx.Exec(ctx, `
				UPDATE ahp_compute.image_versions
				SET status='drain', drain_deadline=now() + $2::interval, updated_at=now()
				WHERE id=$1`, curID, drainInterval(drainWindow),
			)
			if err != nil {
				return fmt.Errorf("compute: drain previous version: %w", err)
			}
		} else if err != pgx.ErrNoRows {
			return fmt.Errorf("compute: query active version: %w", err)
		}

		// Insert the new version as active.
		verID := fmt.Sprintf("ver-%s-%s-%d", agent, digest[7:13], time.Now().UnixNano())
		err = tx.QueryRow(ctx, `
			INSERT INTO ahp_compute.image_versions
				(id, tenant, deployment, agent, digest, status, state_compat)
			VALUES ($1,$2,$3,$4,$5,'active',$6)
			RETURNING id, tenant, deployment, agent, digest, status,
				drain_deadline, state_compat, published_at, created_at, updated_at`,
			verID, scope.Tenant, scope.Deployment, agent, digest, stateCompat,
		).Scan(&newVer.ID, &newVer.Scope.Tenant, &newVer.Scope.Deployment, &newVer.Agent,
			&newVer.Digest, &newVer.Status, &newVer.DrainDeadline, &newVer.StateCompat,
			&newVer.PublishedAt, &newVer.CreatedAt, &newVer.UpdatedAt)
		if err != nil {
			return fmt.Errorf("compute: insert version: %w", err)
		}
		return nil
	})
	if err != nil {
		return ImageVersion{}, err
	}
	return newVer, nil
}

// GetActiveVersion returns the currently active version for an agent.
func (s *Store) GetActiveVersion(ctx context.Context, scope contracts.Scope, agent string) (ImageVersion, error) {
	var v ImageVersion
	var rawDrain *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant, deployment, agent, digest, status,
			drain_deadline, state_compat, published_at, created_at, updated_at
		FROM ahp_compute.image_versions
		WHERE tenant=$1 AND deployment=$2 AND agent=$3 AND status='active'
		LIMIT 1`,
		scope.Tenant, scope.Deployment, agent,
	).Scan(&v.ID, &v.Scope.Tenant, &v.Scope.Deployment, &v.Agent,
		&v.Digest, &v.Status, &rawDrain, &v.StateCompat,
		&v.PublishedAt, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return ImageVersion{}, notFound("active image version")
		}
		return ImageVersion{}, err
	}
	v.DrainDeadline = rawDrain
	return v, nil
}

// GetVersionByDigest returns a specific version by its image digest.
func (s *Store) GetVersionByDigest(ctx context.Context, scope contracts.Scope, agent, digest string) (ImageVersion, error) {
	var v ImageVersion
	var rawDrain *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, tenant, deployment, agent, digest, status,
			drain_deadline, state_compat, published_at, created_at, updated_at
		FROM ahp_compute.image_versions
		WHERE tenant=$1 AND deployment=$2 AND agent=$3 AND digest=$4
		LIMIT 1`,
		scope.Tenant, scope.Deployment, agent, digest,
	).Scan(&v.ID, &v.Scope.Tenant, &v.Scope.Deployment, &v.Agent,
		&v.Digest, &v.Status, &rawDrain, &v.StateCompat,
		&v.PublishedAt, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return ImageVersion{}, notFound("image version")
		}
		return ImageVersion{}, err
	}
	v.DrainDeadline = rawDrain
	return v, nil
}

// DrainVersion explicitly transitions a version to drain with the given window.
// If the version is already active, it becomes drain. New tasks will route to
// the (next) active version.
func (s *Store) DrainVersion(ctx context.Context, scope contracts.Scope, agent, digest string, window time.Duration) (ImageVersion, error) {
	var v ImageVersion
	var rawDrain *time.Time
	err := s.pool.QueryRow(ctx, `
		UPDATE ahp_compute.image_versions
		SET status='drain', drain_deadline=now() + $5::interval, updated_at=now()
		WHERE tenant=$1 AND deployment=$2 AND agent=$3 AND digest=$4
			AND status IN ('active','drain')
		RETURNING id, tenant, deployment, agent, digest, status,
			drain_deadline, state_compat, published_at, created_at, updated_at`,
		scope.Tenant, scope.Deployment, agent, digest, drainInterval(window),
	).Scan(&v.ID, &v.Scope.Tenant, &v.Scope.Deployment, &v.Agent,
		&v.Digest, &v.Status, &rawDrain, &v.StateCompat,
		&v.PublishedAt, &v.CreatedAt, &v.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return ImageVersion{}, notFound("drainable image version")
		}
		return ImageVersion{}, err
	}
	v.DrainDeadline = rawDrain
	return v, nil
}

// RollbackVersion marks the current active version as rolled_back and
// re-activates the most recent previous non-rolled-back version. New tasks
// will route to the re-activated version.
func (s *Store) RollbackVersion(ctx context.Context, scope contracts.Scope, agent string) (ImageVersion, error) {
	var reactivated ImageVersion
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Mark current active as rolled_back.
		tag, err := tx.Exec(ctx, `
			UPDATE ahp_compute.image_versions
			SET status='rolled_back', updated_at=now()
			WHERE tenant=$1 AND deployment=$2 AND agent=$3 AND status='active'`,
			scope.Tenant, scope.Deployment, agent,
		)
		if err != nil {
			return fmt.Errorf("compute: mark rolled_back: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return notFound("active version to rollback")
		}

		// Find the most recent non-rolled-back, non-retired version to reactivate.
		err = tx.QueryRow(ctx, `
			UPDATE ahp_compute.image_versions
			SET status='active', drain_deadline=NULL, updated_at=now()
			WHERE id = (
				SELECT id FROM ahp_compute.image_versions
				WHERE tenant=$1 AND deployment=$2 AND agent=$3
					AND status IN ('drain','retired')
				ORDER BY published_at DESC
				LIMIT 1
			)
			RETURNING id, tenant, deployment, agent, digest, status,
				drain_deadline, state_compat, published_at, created_at, updated_at`,
			scope.Tenant, scope.Deployment, agent,
		).Scan(&reactivated.ID, &reactivated.Scope.Tenant, &reactivated.Scope.Deployment,
			&reactivated.Agent, &reactivated.Digest, &reactivated.Status,
			&reactivated.DrainDeadline, &reactivated.StateCompat,
			&reactivated.PublishedAt, &reactivated.CreatedAt, &reactivated.UpdatedAt)
		if err != nil {
			if err == pgx.ErrNoRows {
				return notFound("previous version to rollback to")
			}
			return fmt.Errorf("compute: reactivate previous: %w", err)
		}
		return nil
	})
	if err != nil {
		return ImageVersion{}, err
	}
	return reactivated, nil
}

// CheckDrainExpiry checks if a draining version's deadline has passed. If so,
// it transitions to retired. Returns true if the version was retired.
func (s *Store) CheckDrainExpiry(ctx context.Context, scope contracts.Scope, agent, digest string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE ahp_compute.image_versions
		SET status='retired', updated_at=now()
		WHERE tenant=$1 AND deployment=$2 AND agent=$3 AND digest=$4
			AND status='drain' AND drain_deadline IS NOT NULL
			AND drain_deadline <= now()`,
		scope.Tenant, scope.Deployment, agent, digest,
	)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// PinComputerToVersion sets the version_digest on a computer, recording which
// image version it was created under.
func (s *Store) PinComputerToVersion(ctx context.Context, scope contracts.Scope, computerID, digest string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE ahp_compute.computers
		SET version_digest=$5, updated_at=now()
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND id=$4`,
		scope.Tenant, scope.Deployment, scope.Subject, computerID, digest,
	)
	if err != nil {
		return fmt.Errorf("compute: pin version: %w", err)
	}
	return nil
}

// CheckStateCompat verifies that a state revision's schema_version is
// forward-compatible with the target version's expected schema. Forward
// compat means: a state written at schema_version N is readable by a runtime
// expecting schema_version M when N <= M.
func CheckStateCompat(stateSchemaVersion, targetSchemaVersion int) bool {
	return stateSchemaVersion <= targetSchemaVersion
}

// --- B135: §9.3 Four Situations ---

// DrainExpiryResult is the outcome of resolving a drain expiry for a task.
type DrainExpiryResult struct {
	Migrated bool   // true if the task was migrated to the new version
	Failed   bool   // true if the task was failed (no compat)
	Reason   string // human-readable reason for failure (when Failed=true)
	Target   string // the version digest the task was migrated to (when Migrated=true)
}

// ResolveDrainExpiry handles situation 2: the drain window has expired.
// If the version's state_compat is true, the task is migrated to the active
// version. Otherwise the task is failed with a clear reason suggesting the
// user start over.
func (s *Store) ResolveDrainExpiry(ctx context.Context, scope contracts.Scope, agent, computerID, oldDigest string) (DrainExpiryResult, error) {
	// Get the old version to check state_compat.
	oldVer, err := s.GetVersionByDigest(ctx, scope, agent, oldDigest)
	if err != nil {
		return DrainExpiryResult{}, err
	}

	// Get the new active version.
	newVer, err := s.GetActiveVersion(ctx, scope, agent)
	if err != nil {
		return DrainExpiryResult{}, err
	}

	if !oldVer.StateCompat {
		return DrainExpiryResult{
			Failed: true,
			Reason: "state incompatible: old version does not declare state.compat; start a new task",
		}, nil
	}

	// Migrate: update the computer's version pin to the new version.
	if err := s.PinComputerToVersion(ctx, scope, computerID, newVer.Digest); err != nil {
		return DrainExpiryResult{}, err
	}
	return DrainExpiryResult{
		Migrated: true,
		Target:   newVer.Digest,
	}, nil
}

// RouteNewTask determines which version a new task should be routed to.
// It always returns the active version (situation 1: new tasks go to the
// current active; situation 3: after rollback, new tasks go to the
// reactivated version; situation 4: new task after agent update goes to
// the new version).
func (s *Store) RouteNewTask(ctx context.Context, scope contracts.Scope, agent string) (ImageVersion, error) {
	return s.GetActiveVersion(ctx, scope, agent)
}

// ThreadCompatResult is the outcome of checking whether an existing thread
// can continue on the new version (situation 4).
type ThreadCompatResult struct {
	Continue   bool   // true if the old thread can continue on the new version
	NewContext bool   // true if a new context must be opened
	Reason     string // explanation when NewContext=true
}

// CheckThreadCompat handles situation 4: agent updated mid-Dot-chat.
// The old thread continues only if state.compat allows it; otherwise Dot
// opens a new context.
func CheckThreadCompat(oldStateCompat bool, oldSchemaVersion, newSchemaVersion int) ThreadCompatResult {
	if !oldStateCompat {
		return ThreadCompatResult{
			NewContext: true,
			Reason:     "old version does not declare state.compat; opening new context",
		}
	}
	if !CheckStateCompat(oldSchemaVersion, newSchemaVersion) {
		return ThreadCompatResult{
			NewContext: true,
			Reason:     fmt.Sprintf("schema version mismatch: state v%d not forward-compatible with target v%d; opening new context", oldSchemaVersion, newSchemaVersion),
		}
	}
	return ThreadCompatResult{Continue: true}
}
