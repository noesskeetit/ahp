package state

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const subjectFenceSchema = `CREATE TABLE IF NOT EXISTS ahp_state.erased_subjects (
 tenant text NOT NULL, subject text NOT NULL, fenced_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant,subject)
);
CREATE TABLE IF NOT EXISTS ahp_state.erased_tenants (tenant text PRIMARY KEY,fenced_at timestamptz NOT NULL DEFAULT now());`

// FenceSubject is a trusted control-plane barrier across every deployment.
// It waits for admitted write transactions to finish before committing the
// durable tombstone. Discovery after this call sees all preceding writes.
func (s *Store) FenceSubject(ctx context.Context, scope contracts.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, subjectFenceKey(scope)); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO ahp_state.erased_subjects(tenant,subject) VALUES($1,$2) ON CONFLICT DO NOTHING`, scope.Tenant, scope.Subject)
		return err
	})
	if err == nil {
		// Isolate any experimental volatile stream tail for this subject so no
		// not-yet-durable content survives the fence in memory. Live reads already
		// mask the erased thread through GetThread; this also bounds memory.
		s.dropTailsForScope(scope.Tenant, scope.Subject)
	}
	return err
}
func subjectFenceKey(scope contracts.Scope) string {
	return "state-erasure/" + scope.Tenant + "/" + scope.Subject
}

const erasedSubjectQuery = `SELECT EXISTS(SELECT 1 FROM ahp_state.erased_subjects WHERE tenant=$1 AND subject=$2) OR EXISTS(SELECT 1 FROM ahp_state.erased_tenants WHERE tenant=$1)`

// Keep these as THREE ordered commands, not a combined SELECT/CTE: the
// tombstone read must acquire a new Read Committed snapshot after a lock wait.
func queueSubjectWritable(batch *pgx.Batch, scope contracts.Scope) {
	batch.Queue(`SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant))
	batch.Queue(`SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, subjectFenceKey(scope))
	batch.Queue(erasedSubjectQuery, scope.Tenant, scope.Subject)
}

// Every content writer holds the shared lock through COMMIT. The separate
// SELECT sees a fence committed while the lock acquisition was waiting.
func checkSubjectWritable(ctx context.Context, tx pgx.Tx, scope contracts.Scope) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, subjectFenceKey(scope)); err != nil {
		return err
	}
	return denyErasedSubject(ctx, tx, scope)
}
func denyErasedSubject(ctx context.Context, tx pgx.Tx, scope contracts.Scope) error {
	var erased bool
	if err := tx.QueryRow(ctx, erasedSubjectQuery, scope.Tenant, scope.Subject).Scan(&erased); err != nil {
		return err
	}
	if erased {
		return contracts.NewError(contracts.ErrForbidden, "state: subject erasure forbids further writes")
	}
	return nil
}

// Claim already locked its candidate run. Never block on the shared subject
// lock while holding that row: another admitted writer may need the same run.
func trySubjectWritableForClaim(ctx context.Context, tx pgx.Tx, scope contracts.Scope) (bool, error) {
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)).Scan(&locked); err != nil {
		return false, err
	}
	if !locked {
		return false, nil
	}
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock_shared(hashtextextended($1,0))`, subjectFenceKey(scope)).Scan(&locked); err != nil {
		return false, err
	}
	if !locked {
		return false, nil
	}
	return true, denyErasedSubject(ctx, tx, scope)
}
