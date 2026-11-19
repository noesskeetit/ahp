package operations

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func (s *Store) subjectFenceKey(tenant, subject string) string {
	return s.schema + "/subject-erasure/" + tenant + "/" + subject
}

// CheckSubjectWritable follows the tenant lock and precedes effect/content row
// locks. Reconciliation intentionally remains permitted behind this barrier.
func (s *Store) CheckSubjectWritable(ctx context.Context, tx pgx.Tx, tenant, subject string) error {
	if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, s.subjectFenceKey(tenant, subject)); e != nil {
		return e
	}
	var erased bool
	if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+quoteIdentifier(s.schema)+`.erased_subjects WHERE tenant=$1 AND subject=$2)`, tenant, subject).Scan(&erased); e != nil {
		return e
	}
	if erased {
		return contracts.NewError(contracts.ErrForbidden, "operations: subject erased")
	}
	return nil
}

// FenceSubject serializes against admission and dispatch across all deployments.
// A dispatched effect stays UNKNOWN until its owner proves the actual outcome.
func (s *Store) FenceSubject(ctx context.Context, tenant, subject string) error {
	if e := (contracts.Scope{Tenant: tenant, Subject: subject, Deployment: "control"}).Validate(); e != nil {
		return e
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.subjectFenceKey(tenant, subject)); e != nil {
			return e
		}
		q := quoteIdentifier(s.schema)
		if _, e := tx.Exec(ctx, `INSERT INTO `+q+`.erased_subjects VALUES($1,$2) ON CONFLICT DO NOTHING`, tenant, subject); e != nil {
			return e
		}
		_, e := tx.Exec(ctx, `UPDATE `+q+`.operations SET status=CASE WHEN status='ACCEPTED' THEN 'FENCED' ELSE 'SIDE_EFFECT_UNKNOWN' END,terminal=(status='ACCEPTED'),version=version+1,updated_at=now() WHERE tenant=$1 AND subject=$2 AND status IN ('ACCEPTED','DISPATCHED')`, tenant, subject)
		return e
	})
}

// DeleteSubject removes settled metadata only, retaining the permanent barrier.
func (s *Store) DeleteSubject(ctx context.Context, tenant, subject string) error {
	return s.DeleteSubjectChecked(ctx, tenant, subject, nil)
}

// DeleteSubjectChecked commits the owner's local content erasure and terminal
// receipt removal atomically. erase must only use the supplied transaction.
func (s *Store) DeleteSubjectChecked(ctx context.Context, tenant, subject string, erase func(context.Context, pgx.Tx) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.subjectFenceKey(tenant, subject)); e != nil {
			return e
		}
		q := quoteIdentifier(s.schema)
		var fenced, pending bool
		if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+q+`.erased_subjects WHERE tenant=$1 AND subject=$2),EXISTS(SELECT 1 FROM `+q+`.operations WHERE tenant=$1 AND subject=$2 AND NOT terminal)`, tenant, subject).Scan(&fenced, &pending); e != nil {
			return e
		}
		if !fenced || pending {
			return contracts.NewReasoned(contracts.ErrUnavailable, "subject_effects_pending", "operations: subject fence or effect outcome pending")
		}
		if erase != nil {
			if e := erase(ctx, tx); e != nil {
				return e
			}
		}
		_, e := tx.Exec(ctx, `DELETE FROM `+q+`.operations WHERE tenant=$1 AND subject=$2`, tenant, subject)
		return e
	})
}
