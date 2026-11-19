package artifacts

import (
	"context"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const subjectFenceSchema = `CREATE TABLE IF NOT EXISTS ahp_artifacts.erased_subjects (
 tenant text NOT NULL, subject text NOT NULL, fenced_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant,subject)
);
CREATE TABLE IF NOT EXISTS ahp_artifacts.erased_tenants (tenant text PRIMARY KEY,fenced_at timestamptz NOT NULL DEFAULT now());`

// FenceSubject prevents registration, sealing and restoration of this owner's
// artifacts across deployments. It drains admitted metadata write transactions;
// issued S3 URLs expire independently and cannot make a tombstone readable.
func (s *Store) FenceSubject(ctx context.Context, scope contracts.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := s.authorize(ctx, scope, "delete", "artifacts:subject"); err != nil {
		return err
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, subjectFenceKey(scope)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO ahp_artifacts.erased_subjects(tenant,subject) VALUES($1,$2) ON CONFLICT DO NOTHING`, scope.Tenant, scope.Subject)
		return err
	})
}
func subjectFenceKey(scope contracts.Scope) string {
	return "artifacts-erasure/" + scope.Tenant + "/" + scope.Subject
}
func checkSubjectWritable(ctx context.Context, tx pgx.Tx, scope contracts.Scope) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, subjectFenceKey(scope)); err != nil {
		return err
	}
	var erased bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_artifacts.erased_subjects WHERE tenant=$1 AND subject=$2) OR EXISTS(SELECT 1 FROM ahp_artifacts.erased_tenants WHERE tenant=$1)`, scope.Tenant, scope.Subject).Scan(&erased); err != nil {
		return err
	}
	if erased {
		return contracts.NewError(contracts.ErrForbidden, "artifacts: subject erasure forbids further writes")
	}
	return nil
}

// Content changes guard both the actor and the immutable stored owner. Read
// operations on a shared retained artifact keep their existing admission rules.
func checkArtifactWritable(ctx context.Context, tx pgx.Tx, scope contracts.Scope, id string) error {
	var owner contracts.Scope
	err := tx.QueryRow(ctx, `SELECT tenant,deployment,subject FROM ahp_artifacts.artifacts WHERE id=$1`, id).Scan(&owner.Tenant, &owner.Deployment, &owner.Subject)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	if err != nil {
		return err
	}
	if owner.Tenant != scope.Tenant {
		return contracts.NewError(contracts.ErrNotFound, "artifact not found")
	}
	first, second := scope, owner
	if first.Subject > second.Subject {
		first, second = second, first
	}
	if err = checkSubjectWritable(ctx, tx, first); err != nil {
		return err
	}
	if first.Subject != second.Subject {
		return checkSubjectWritable(ctx, tx, second)
	}
	return nil
}
