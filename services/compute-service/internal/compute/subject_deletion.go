package compute

import (
	"context"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

func (s *Store) checkOwnerWritable(ctx context.Context, tx pgx.Tx, scope contracts.Scope) error {
	if err := s.checkTenantWritable(ctx, tx, scope.Tenant); err != nil {
		return err
	}
	ledger, err := operations.New(s.pool, "ahp_compute")
	if err != nil {
		return err
	}
	return ledger.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject)
}

// FenceSubject permanently closes admission but leaves dispatched outcomes
// unsettled; backend watchers cancel cooperatively under the same owner scope.
func (s *Store) FenceSubject(ctx context.Context, tenant, subject string) error {
	if err := (contracts.Scope{Tenant: tenant, Subject: subject, Deployment: "control"}).Validate(); err != nil {
		return err
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "ahp_compute/subject-erasure/"+tenant+"/"+subject); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, "INSERT INTO ahp_compute.erased_subjects VALUES($1,$2) ON CONFLICT DO NOTHING", tenant, subject); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, "UPDATE ahp_compute.commands SET cancel_requested=true,status=CASE WHEN status='accepted' THEN 'cancelled' ELSE status END,updated_at=clock_timestamp() WHERE tenant=$1 AND subject=$2 AND status IN ('accepted','dispatched','side_effect_unknown')", tenant, subject)
		return err
	})
}

func (s *Store) subjectPending(ctx context.Context, tx pgx.Tx, tenant, subject string) error {
	var fenced, pending bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_compute.erased_subjects WHERE tenant=$1 AND subject=$2),
 EXISTS(SELECT 1 FROM ahp_compute.commands WHERE tenant=$1 AND subject=$2 AND status IN ('accepted','dispatched','side_effect_unknown')) OR
 EXISTS(SELECT 1 FROM ahp_compute.env_revisions e JOIN ahp_compute.computers c ON c.id=e.computer_id WHERE c.tenant=$1 AND c.subject=$2 AND e.status='pending') OR
 EXISTS(SELECT 1 FROM ahp_compute.incarnations i JOIN ahp_compute.computers c ON c.id=i.computer_id WHERE c.tenant=$1 AND c.subject=$2 AND i.status<>'stopped') OR
 EXISTS(SELECT 1 FROM ahp_compute.activity_leases l JOIN ahp_compute.computers c ON c.id=l.computer_id WHERE c.tenant=$1 AND c.subject=$2 AND l.expires>clock_timestamp())`, tenant, subject).Scan(&fenced, &pending)
	if err != nil {
		return err
	}
	if !fenced || pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "subject_compute_pending", "compute: active execution or outcome pending")
	}
	return nil
}

// DeleteSubject without a runner can erase metadata-only ephemeral Computers.
// Filesystem-bearing resources require the runner's validated cleanup path.
func (s *Store) DeleteSubject(ctx context.Context, tenant, subject string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", "ahp_compute/subject-erasure/"+tenant+"/"+subject); err != nil {
			return err
		}
		if err := s.subjectPending(ctx, tx, tenant, subject); err != nil {
			return err
		}
		var files bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_compute.snapshots s JOIN ahp_compute.computers c ON c.id=s.computer_id WHERE c.tenant=$1 AND c.subject=$2) OR EXISTS(SELECT 1 FROM ahp_compute.computers WHERE tenant=$1 AND subject=$2 AND profile<>'ephemeral')`, tenant, subject).Scan(&files); err != nil {
			return err
		}
		if files {
			return contracts.NewError(contracts.ErrUnavailable, "compute: filesystem cleanup owner required")
		}
		_, err := tx.Exec(ctx, "DELETE FROM ahp_compute.computers WHERE tenant=$1 AND subject=$2", tenant, subject)
		return err
	})
}

func (r *Runner) FenceSubject(ctx context.Context, tenant, subject string) error {
	return r.store.FenceSubject(ctx, tenant, subject)
}
func (r *Runner) DeleteSubject(ctx context.Context, tenant, subject string) error {
	return r.deleteOwner(ctx, tenant, subject)
}
