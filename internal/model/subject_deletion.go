package model

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// FenceSubject blocks every Model admission before discovering generations or
// copies. It requests cancellation but never invents a zero provider bill.
func (s *Service) FenceSubject(ctx context.Context, tenant, subject string) error {
	if s.ledger == nil {
		return contracts.NewError(contracts.ErrUnavailable, "model: effect owner unavailable")
	}
	if err := s.ledger.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	if s.files != nil {
		if err := s.files.FenceSubject(ctx, tenant, subject); err != nil {
			return err
		}
	}
	rows, err := s.gens.pool.Query(ctx, `SELECT id,tenant,deployment,subject FROM ahp_model.generations WHERE tenant=$1 AND subject=$2 AND status IN ('PENDING','RUNNING','STREAMING')`, tenant, subject)
	if err != nil {
		return err
	}
	type target struct {
		id    string
		scope contracts.Scope
	}
	var targets []target
	for rows.Next() {
		var t target
		if err = rows.Scan(&t.id, &t.scope.Tenant, &t.scope.Deployment, &t.scope.Subject); err != nil {
			rows.Close()
			return err
		}
		targets = append(targets, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, t := range targets {
		if _, err = s.Cancel(ctx, t.scope, t.id); err != nil {
			return err
		}
	}
	return nil
}

// DeleteSubject uses terminal receipts to recover an interrupted settlement,
// then erases Model content across deployments. Unknown effects and unsettled
// reservations keep the control operation pending; permanent fences remain.
func (s *Service) DeleteSubject(ctx context.Context, tenant, subject string) error {
	if err := s.FenceSubject(ctx, tenant, subject); err != nil {
		return err
	}
	if err := s.DeleteSubjectFiles(ctx, tenant, subject); err != nil {
		return err
	}
	if err := s.recoverErasureGenerations(ctx, tenant, subject); err != nil {
		return err
	}
	var err error
	// Check accounting before erasing receipts needed by settlement recovery.
	var pending bool
	if err = s.gens.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_model.budget_reservations r JOIN ahp_model.budgets b ON b.id=r.budget_id WHERE b.tenant=$1 AND b.subject=$2 AND NOT r.settled)`, tenant, subject).Scan(&pending); err != nil {
		return err
	}
	if pending {
		return contracts.NewReasoned(contracts.ErrUnavailable, "subject_model_pending", "model: reservation outcome pending")
	}
	return s.ledger.DeleteSubjectChecked(ctx, tenant, subject, func(ctx context.Context, tx pgx.Tx) error {
		var quotaExists bool
		if e := tx.QueryRow(ctx, `SELECT to_regclass('ahp_model.quota_admits') IS NOT NULL`).Scan(&quotaExists); e != nil {
			return e
		}
		if quotaExists {
			if _, e := tx.Exec(ctx, `DELETE FROM ahp_model.quota_admits q USING ahp_model.generations g WHERE g.tenant=$1 AND g.subject=$2 AND q.tenant=g.tenant AND q.op_key=g.key`, tenant, subject); e != nil {
				return e
			}
		}
		for _, q := range []string{
			`DELETE FROM ahp_model.budget_task_aliases WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_model.budget_reservations WHERE budget_id IN (SELECT id FROM ahp_model.budgets WHERE tenant=$1 AND subject=$2)`,
			`DELETE FROM ahp_model.generations WHERE tenant=$1 AND subject=$2`,
			`DELETE FROM ahp_model.budgets WHERE tenant=$1 AND subject=$2`,
		} {
			if _, e := tx.Exec(ctx, q, tenant, subject); e != nil {
				return e
			}
		}
		return nil
	})
}

// recoverErasureGenerations adopts existing provider receipts without another
// inference call. An empty subject means the entire fenced tenant.
func (s *Service) recoverErasureGenerations(ctx context.Context, tenant, subject string) error {
	rows, err := s.gens.pool.Query(ctx, `SELECT `+genCols+` FROM ahp_model.generations WHERE tenant=$1 AND ($2='' OR subject=$2)`, tenant, subject)
	if err != nil {
		return err
	}
	var generations []Generation
	for rows.Next() {
		g, e := s.gens.scanGen(rows)
		if e != nil {
			rows.Close()
			return e
		}
		generations = append(generations, g)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, g := range generations {
		op, e := s.ledger.Get(ctx, g.Scope, g.EffectKey)
		if e != nil {
			return e
		}
		switch op.Status {
		case operations.StatusSideEffectUnknown, operations.StatusDispatched, operations.StatusAccepted:
			return contracts.NewReasoned(contracts.ErrUnavailable, "subject_model_pending", "model: provider effect unresolved")
		case operations.StatusSucceeded, operations.StatusFailed:
			var result resultPayload
			if json.Unmarshal(op.Result, &result) != nil || !terminal(result.Status) {
				return contracts.NewError(contracts.ErrUnavailable, "model: invalid provider receipt")
			}
			if _, e = s.settleAndFinish(ctx, g.Scope, g, result.Status, result.Error, result.Usage, op.Status == operations.StatusFailed, result); e != nil {
				return e
			}
		case operations.StatusFenced:
			// This journal fact proves dispatch never happened.
			if e = s.cancelUnsentReservation(ctx, g.Scope, g); e != nil {
				return e
			}
		}
	}
	return nil
}
