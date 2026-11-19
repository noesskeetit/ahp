package triggers

import (
	"context"
	"errors"
	"fmt"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// Permanent authority loss is the class of DelegationIssuer failure that means
// "this trigger may never fire again", as opposed to "this attempt failed".
//
// The product rule (PDF rev 3.2, c. 48 "Фоновые и запланированные задачи" and
// c. 54 "9.6. Отмена и отзыв") is that a dismissed subject or a revoked
// delegation closes FUTURE runs: "увольнение сотрудника закрывает будущие
// запуски", "Грант отозван: перевыпуска токена нет, Triggers остановлены".
// A trigger whose delegation can no longer be honoured must therefore be
// stopped, not retried forever; retrying would both spin the worker and keep
// knocking on a door Identity & Policy already closed.
//
// Only authority verdicts are permanent. Transport failures, timeouts and
// INTERNAL/UNAVAILABLE answers are deliberately NOT permanent: the delegation
// may still be live and the next tick must try again.
func permanentAuthorityLoss(err error) bool {
	if err == nil {
		return false
	}
	switch contracts.Code(err) {
	case contracts.ErrNotFound, contracts.ErrForbidden, contracts.ErrInvalid:
		return true
	default:
	}
	// A wrapped identity sentinel without a contract code is not a verdict.
	return errors.Is(err, errDelegationUnknown)
}

// errDelegationUnknown lets an issuer signal "no such delegation reference"
// without building a contracts error.
var errDelegationUnknown = errors.New("triggers: delegation reference unknown")

// stop marks a trigger REVOKED with a machine-readable reason. It is a
// monotonic revocation, independent of the admission counter. A concurrent
// admission must not turn confirmed authority loss into a stale-CAS no-op.
// A trigger already out of ACTIVE is left exactly as it is.
func (s *Service) stop(ctx context.Context, trig Trigger, reason string) error {
	q := fmt.Sprintf(`UPDATE %s.triggers
		SET status = 'REVOKED', next_fire_at = NULL, version = version + 1, updated_at = now()
		WHERE trigger_id = $1 AND tenant = $2 AND deployment = $3
		  AND status = 'ACTIVE'`,
		quoteIdent(s.schema))
	if _, err := s.pool.Exec(ctx, q, trig.TriggerID, trig.Tenant, trig.Deployment); err != nil {
		return fmt.Errorf("triggers: stop trigger: %w", err)
	}
	return nil
}

// stopIfDelegationGone inspects an IssueToken failure and stops the trigger
// when the failure is an authority verdict. It reports whether the trigger was
// stopped. The ledger slot is settled by the caller.
func (s *Service) stopIfDelegationGone(ctx context.Context, trig Trigger, issueErr error) bool {
	if !permanentAuthorityLoss(issueErr) {
		return false
	}
	// Best effort: a failed stop leaves the trigger ACTIVE, which is safe
	// (the next attempt hits the same verdict) but must not mask the verdict.
	if err := s.stop(ctx, trig, "delegation_revoked"); err != nil {
		return false
	}
	return true
}

// RevokeBySubject stops every ACTIVE trigger of one subject in one tenant,
// across deployments, and returns how many triggers were stopped.
//
// This is the hook Identity & Policy's cascade calls when a subject is
// dismissed or deleted (PDF rev 3.2, c. 53-54: "гранты отозваны, триггеры
// остановлены"). It is idempotent: a second call after the cascade stops
// nothing and returns 0.
func (s *Service) RevokeBySubject(ctx context.Context, tenant, subject string) (int, error) {
	return s.stopWhere(ctx, "subject", tenant, subject)
}

// RevokeByDelegation stops every ACTIVE trigger bound to one delegation
// reference in one tenant and returns how many triggers were stopped.
//
// A delegation reference is what outlives a token: when Identity & Policy
// revokes the delegation tree, every trigger that would have re-issued a token
// from it must stop at once (PDF rev 3.2, c. 48: "Triggers хранят ссылку на
// делегирование"; c. 54: "Triggers остановлены"). Idempotent.
func (s *Service) RevokeByDelegation(ctx context.Context, tenant, delegationRef string) (int, error) {
	return s.stopWhere(ctx, "delegation_ref", tenant, delegationRef)
}

// stopWhere is the shared cascade: ACTIVE rows only, scoped by tenant so a
// caller can never stop another tenant's triggers.
func (s *Service) stopWhere(ctx context.Context, column, tenant, value string) (int, error) {
	if tenant == "" || value == "" {
		return 0, contracts.NewError(contracts.ErrInvalid, "triggers: tenant and selector are required")
	}
	var col string
	switch column {
	case "subject":
		col = "subject"
	case "delegation_ref":
		col = "delegation_ref"
	default:
		return 0, contracts.NewError(contracts.ErrInvalid, "triggers: unknown cascade selector")
	}
	q := fmt.Sprintf(`UPDATE %s.triggers
		SET status = 'REVOKED', next_fire_at = NULL, version = version + 1, updated_at = now()
		WHERE tenant = $1 AND %s = $2 AND status = 'ACTIVE'
		RETURNING trigger_id`, quoteIdent(s.schema), col)
	rows, err := s.pool.Query(ctx, q, tenant, value)
	if err != nil {
		return 0, fmt.Errorf("triggers: cascade revoke: %w", err)
	}
	defer rows.Close()
	stopped := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return stopped, fmt.Errorf("triggers: cascade revoke scan: %w", err)
		}
		stopped++
	}
	if err := rows.Err(); err != nil {
		return stopped, fmt.Errorf("triggers: cascade revoke rows: %w", err)
	}
	return stopped, nil
}
