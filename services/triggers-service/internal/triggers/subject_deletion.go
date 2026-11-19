package triggers

import (
	"context"
	"github.com/jackc/pgx/v5"
)

// FenceSubject stops sources and fences in-flight delivery effects durably.
func (s *Service) FenceSubject(ctx context.Context, tenant, subject string) error {
	if err := s.ledger.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, "UPDATE "+quoteIdent(s.schema)+".triggers SET status='REVOKED',next_fire_at=NULL,version=version+1,updated_at=now() WHERE tenant=$1 AND subject=$2 AND status<>'REVOKED'", tenant, subject)
	return err
}

// DeleteSubject removes schedules/webhook receipts only after delivery settles.
func (s *Service) DeleteSubject(ctx context.Context, tenant, subject string) error {
	if err := s.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	return s.ledger.DeleteSubjectChecked(ctx, tenant, subject, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "DELETE FROM "+quoteIdent(s.schema)+".triggers WHERE tenant=$1 AND subject=$2", tenant, subject)
		return err
	})
}
