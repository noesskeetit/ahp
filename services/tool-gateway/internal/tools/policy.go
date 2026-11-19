package tools

import (
	"context"
	"errors"
	"fmt"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DBPolicySource is the durable PolicySource: it reads the operator grant table
// and the shared approvals table in the tool plane's own schema. Both signals
// are default-deny by absence -- a missing row, a missing table or a query
// error all resolve to "not allowed", never to "allow".
type DBPolicySource struct {
	ledger *operations.Store

	pool   *pgxpool.Pool
	schema string
}

// newDBPolicySource binds the source to the pool and (already schemaRe-guarded)
// schema the Service was constructed with.
func newDBPolicySource(pool *pgxpool.Pool, schema string) *DBPolicySource {
	return &DBPolicySource{pool: pool, schema: schema}
}

// policyDDL is executed by Service.applyDDL so the operator grant table and its
// audit companion exist before any Guardian decision runs.
func (s *DBPolicySource) policyDDL() []string {
	return []string{
		// Operator standing grants at (tenant, connection, tool) granularity.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.tool_policy (
			tenant      text        NOT NULL,
			connection  text        NOT NULL,
			tool        text        NOT NULL,
			created_by  text        NOT NULL,
			created_at  timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant, connection, tool)
		)`, s.schema),
		// Append-only audit of every operator grant change.
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.tool_policy_audit (
			id           bigserial   PRIMARY KEY,
			tenant       text        NOT NULL,
			connection   text        NOT NULL,
			tool         text        NOT NULL,
			action       text        NOT NULL,
			changed_by   text        NOT NULL,
			changed_at   timestamptz NOT NULL DEFAULT now()
		)`, s.schema),
	}
}

// ApprovalExists reports a human-approved row for (tenant, digest, tool). It
// reuses the approvals table Service.Approve binds, so the same human sign-off
// is the durable evidence the Guardian trusts -- there is no second gate.
func (s *DBPolicySource) ApprovalExists(ctx context.Context, scope contracts.Scope, digest, tool string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	var ok bool
	// Only an UNSPENT, unexpired signature counts as durable evidence: a
	// consumed approval authorises nothing after its single effect (P1 audit).
	q := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 FROM %s.approvals
			WHERE tenant = $1 AND digest = $2 AND tool = $3
			  AND approved_by IS NOT NULL
			  AND consumed_at IS NULL
			  AND (expires_at IS NULL OR expires_at > now())
		)`, s.schema)
	if err := s.pool.QueryRow(ctx, q, scope.Tenant, digest, tool).Scan(&ok); err != nil {
		return false, fmt.Errorf("tools: policy approval lookup: %w", err)
	}
	return ok, nil
}

// OperatorPolicyAllows reports a standing operator grant for
// (tenant, connection, tool). A missing table or row is a deny, never an allow.
func (s *DBPolicySource) OperatorPolicyAllows(ctx context.Context, scope contracts.Scope, connection, tool string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	var ok bool
	q := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 FROM %s.tool_policy
			WHERE tenant = $1 AND connection = $2 AND tool = $3
		)`, s.schema)
	err := s.pool.QueryRow(ctx, q, scope.Tenant, connection, tool).Scan(&ok)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		// The table is created by Service.applyDDL; a hard error here must fail
		// closed to deny rather than surface as an unhandled allow.
		return false, fmt.Errorf("tools: policy operator lookup: %w", err)
	}
	return ok, nil
}

// GrantOperatorPolicy records an operator standing grant and writes an audit
// row in the same transaction. It is the only writer of tool_policy and is
// intended for operator/tooling use, not exposed on the task-facing HTTP plane.
func (s *DBPolicySource) GrantOperatorPolicy(ctx context.Context, scope contracts.Scope, connection, tool, changedBy string) error {
	if s == nil || s.pool == nil {
		return contracts.NewError(contracts.ErrInvalid, "tools: policy source not configured")
	}
	if changedBy == "" {
		return contracts.NewError(contracts.ErrInvalid, "tools: policy change requires an actor")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if s.ledger == nil {
			return contracts.NewError(contracts.ErrUnavailable, "tools: tenant fence unavailable")
		}
		if err := s.ledger.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := s.ledger.CheckSubjectWritable(ctx, tx, scope.Tenant, changedBy); err != nil {
			return err
		}
		i := fmt.Sprintf(`INSERT INTO %s.tool_policy (tenant, connection, tool, created_by)
			VALUES ($1,$2,$3,$4)
			ON CONFLICT (tenant, connection, tool) DO NOTHING`, s.schema)
		if _, err := tx.Exec(ctx, i, scope.Tenant, connection, tool, changedBy); err != nil {
			return fmt.Errorf("tools: grant policy: %w", err)
		}
		a := fmt.Sprintf(`INSERT INTO %s.tool_policy_audit (tenant, connection, tool, action, changed_by)
			VALUES ($1,$2,$3,'grant',$4)`, s.schema)
		if _, err := tx.Exec(ctx, a, scope.Tenant, connection, tool, changedBy); err != nil {
			return fmt.Errorf("tools: grant policy audit: %w", err)
		}
		return nil
	})
}

// RevokeOperatorPolicy removes a standing grant and writes an audit row.
func (s *DBPolicySource) RevokeOperatorPolicy(ctx context.Context, scope contracts.Scope, connection, tool, changedBy string) error {
	if s == nil || s.pool == nil {
		return contracts.NewError(contracts.ErrInvalid, "tools: policy source not configured")
	}
	if changedBy == "" {
		return contracts.NewError(contracts.ErrInvalid, "tools: policy change requires an actor")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if s.ledger == nil {
			return contracts.NewError(contracts.ErrUnavailable, "tools: tenant fence unavailable")
		}
		if err := s.ledger.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := s.ledger.CheckSubjectWritable(ctx, tx, scope.Tenant, changedBy); err != nil {
			return err
		}
		d := fmt.Sprintf(`DELETE FROM %s.tool_policy WHERE tenant=$1 AND connection=$2 AND tool=$3`, s.schema)
		if _, err := tx.Exec(ctx, d, scope.Tenant, connection, tool); err != nil {
			return fmt.Errorf("tools: revoke policy: %w", err)
		}
		a := fmt.Sprintf(`INSERT INTO %s.tool_policy_audit (tenant, connection, tool, action, changed_by)
			VALUES ($1,$2,$3,'revoke',$4)`, s.schema)
		if _, err := tx.Exec(ctx, a, scope.Tenant, connection, tool, changedBy); err != nil {
			return fmt.Errorf("tools: revoke policy audit: %w", err)
		}
		return nil
	})
}
