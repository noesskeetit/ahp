package registry

import (
	"context"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Deployment is one entry of an installation's rollout history.
type Deployment struct {
	ID           string    `json:"id"`
	Tenant       string    `json:"tenant"`
	Installation string    `json:"installation_id"`
	AgentID      string    `json:"agent_id"`
	Environment  string    `json:"environment"`
	VersionID    string    `json:"version_id"`
	Digest       string    `json:"digest"`
	Action       string    `json:"action"`
	ReleaseID    string    `json:"release_id,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// recordDeployment appends one rollout fact inside the caller's transaction, so
// a version pointer and its history can never disagree.
func recordDeployment(ctx context.Context, tx pgx.Tx, tenant, installationID, agentID, environment string, version Version, action, releaseID string) error {
	const q = `
INSERT INTO ahp_registry.deployments
    (id, tenant, installation_id, agent_id, environment, version_id, digest, action, release_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	if _, err := tx.Exec(ctx, q, contracts.NewID(), tenant, installationID, agentID,
		environment, version.ID, version.Digest, action, releaseID); err != nil {
		return storageError("record deployment", err)
	}
	return nil
}

// ListDeployments returns the rollout history of one installation of the caller
// tenant, newest first.
func (s *Store) ListDeployments(ctx context.Context, tenant, installationID string, limit int) ([]Deployment, error) {
	if err := requireID("tenant", tenant); err != nil {
		return nil, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
SELECT id, tenant, installation_id, agent_id, environment, version_id, digest, action, release_id, created_at
FROM ahp_registry.deployments
WHERE tenant = $1 AND installation_id = $2
ORDER BY created_at DESC, id LIMIT $3`, tenant, installationID, limit)
	if err != nil {
		return nil, storageError("list deployments", err)
	}
	defer rows.Close()
	out := make([]Deployment, 0, 8)
	for rows.Next() {
		var d Deployment
		if err := rows.Scan(&d.ID, &d.Tenant, &d.Installation, &d.AgentID, &d.Environment,
			&d.VersionID, &d.Digest, &d.Action, &d.ReleaseID, &d.CreatedAt); err != nil {
			return nil, storageError("scan deployment", err)
		}
		d.CreatedAt = d.CreatedAt.UTC()
		out = append(out, d)
	}
	return out, rows.Err()
}

// Rollback moves an installation back to the previous version it actually ran.
//
// The rule is literal (PDF rev 3.2, c. 42 "Версии"): "Откат переключает на
// предыдущий digest новые задачи; задачи, начатые новой версией, на ней и
// остаются." Only the installation's version pointer moves; task pins are never
// rewritten, so a task keeps the version it started on. The previous version is
// taken from the recorded rollout history, so a rollback target is a registry
// fact rather than a client assertion.
func (s *Store) Rollback(ctx context.Context, tenant, installationID string) (Installation, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Installation{}, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return Installation{}, err
	}

	var out Installation
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkTenantWritable(ctx, tx, tenant); err != nil {
			return err
		}
		inst, err := lockInstallation(ctx, tx, tenant, installationID)
		if err != nil {
			return err
		}
		target, err := previousVersionOf(ctx, tx, tenant, installationID, inst.VersionID)
		if err != nil {
			return err
		}
		version, err := lockVersion(ctx, tx, target)
		if err != nil {
			return err
		}
		if version.AgentID != inst.AgentID {
			return contracts.NewError(contracts.ErrConflict, "rollback target belongs to a different agent")
		}
		if err := requireGrantedSubset(version.Manifest.Capabilities, inst.Capabilities); err != nil {
			return contracts.NewError(contracts.ErrConflict,
				"granted capabilities exceed the rollback target ceiling: "+err.Error())
		}
		if err := requireInstallationAllowed(version.Manifest, tenant); err != nil {
			return err
		}
		// A rollback is a rollout in the other direction, so it carries the same
		// supply-chain obligation as a promotion: the target's image must still
		// have a verified, unexpired admission and -- in stage/prod -- currently
		// passing evaluation, security and restart-chaos verdicts. Otherwise
		// "roll back" would be a way to deploy an image the gate had already
		// refused.
		if err := s.requireDeployAdmission(ctx, tx, version, inst.Environment); err != nil {
			return err
		}
		previous, err := getVersion(ctx, tx, inst.VersionID)
		if err != nil {
			return err
		}
		const q = `
UPDATE ahp_registry.installations SET version_id = $1, updated_at = now()
WHERE tenant = $2 AND id = $3`
		if _, err := tx.Exec(ctx, q, target, tenant, installationID); err != nil {
			return storageError("rollback installation", err)
		}
		if err := recordDeployment(ctx, tx, tenant, installationID, inst.AgentID,
			inst.Environment, version, "rollback", ""); err != nil {
			return err
		}
		if derr := recordRollout(ctx, tx, tenant, installationID, inst.AgentID,
			inst.Environment, previous, version, "rollback"); derr != nil {
			return derr
		}
		inst.VersionID = target
		out = inst.Installation
		return nil
	})
	if err != nil {
		return Installation{}, err
	}
	return out, nil
}

// previousVersionOf finds the most recent distinct version an installation was
// deployed on before the current one.
func previousVersionOf(ctx context.Context, tx pgx.Tx, tenant, installationID, current string) (string, error) {
	const q = `
SELECT version_id FROM ahp_registry.deployments
WHERE tenant = $1 AND installation_id = $2 AND version_id <> $3
ORDER BY created_at DESC, id DESC LIMIT 1`
	var target string
	err := tx.QueryRow(ctx, q, tenant, installationID, current).Scan(&target)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", contracts.NewError(contracts.ErrNotFound,
				"no previous version to roll back to: this installation never changed version")
		}
		return "", storageError("read deployment history", err)
	}
	return target, nil
}

// PreviousVersion reports the version a rollback would return to, without
// changing anything.
func (s *Store) PreviousVersion(ctx context.Context, tenant, installationID string) (Version, error) {
	if err := requireID("tenant", tenant); err != nil {
		return Version{}, err
	}
	if err := requireID("installation id", installationID); err != nil {
		return Version{}, err
	}
	var current, target string
	err := s.pool.QueryRow(ctx, `SELECT version_id FROM ahp_registry.installations WHERE tenant = $1 AND id = $2`,
		tenant, installationID).Scan(&current)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Version{}, contracts.NewError(contracts.ErrNotFound, "installation not found")
		}
		return Version{}, storageError("read installation", err)
	}
	row := s.pool.QueryRow(ctx, `
SELECT version_id FROM ahp_registry.deployments
WHERE tenant = $1 AND installation_id = $2 AND version_id <> $3
ORDER BY created_at DESC, id DESC LIMIT 1`, tenant, installationID, current)
	if err := row.Scan(&target); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Version{}, contracts.NewError(contracts.ErrNotFound, "no previous version")
		}
		return Version{}, storageError("read deployment history", err)
	}
	return s.GetVersion(ctx, target)
}
