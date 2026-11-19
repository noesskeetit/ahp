package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/ahp-platform/ahp/internal/artifacts"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// ToolLease is established by the HTTP edge from a verified State API proof
// and an authenticated task principal.
type ToolLease struct {
	SessionID string
	ContextID string
	Epoch     int64
	ExpiresAt time.Time
}

type FencedResult struct {
	Status  string      `json:"status"`
	Receipt *ResultView `json:"receipt,omitempty"`
}

func (s *Service) MigrateToolLeases(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+s.schema+`.gateway_session_epochs (
			tenant_id text NOT NULL,
			session_id text NOT NULL,
			deployment_id text NOT NULL,
			subject_id text NOT NULL,
			context_id text NOT NULL,
			min_allowed_epoch bigint NOT NULL CHECK (min_allowed_epoch > 0),
			PRIMARY KEY (tenant_id, session_id)
		);
		CREATE TABLE IF NOT EXISTS `+s.schema+`.gateway_operations (
			tenant_id text NOT NULL,
			session_id text NOT NULL,
			operation_key text NOT NULL,
			created_epoch bigint NOT NULL CHECK (created_epoch > 0),
			request_digest text,
			status text NOT NULL CHECK (
				(status='IN_FLIGHT' AND request_digest IS NOT NULL)
				OR (status='FENCED_ABORTED' AND request_digest IS NULL)
			),
			updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
			PRIMARY KEY (tenant_id, session_id, operation_key),
			FOREIGN KEY (tenant_id, session_id) REFERENCES `+s.schema+`.gateway_session_epochs
		);`)
	return err
}

// advanceToolEpoch serializes dispatch and recovery on the same Gateway row.
// The Gateway clock checks the signed deadline inside this transaction.
func (s *Service) advanceToolEpoch(ctx context.Context, tx pgx.Tx, scope contracts.Scope, lease ToolLease) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO `+s.schema+`.gateway_session_epochs AS active
			(tenant_id, session_id, deployment_id, subject_id, context_id, min_allowed_epoch)
		SELECT $1,$2,$3,$4,$5,$6 WHERE $7::timestamptz > clock_timestamp()
		ON CONFLICT (tenant_id, session_id) DO UPDATE
			SET min_allowed_epoch=EXCLUDED.min_allowed_epoch
		WHERE active.deployment_id=EXCLUDED.deployment_id
		  AND active.subject_id=EXCLUDED.subject_id AND active.context_id=EXCLUDED.context_id
		  AND active.min_allowed_epoch <= EXCLUDED.min_allowed_epoch
		  AND $7::timestamptz > clock_timestamp()`,
		scope.Tenant, lease.SessionID, scope.Deployment, scope.Subject, lease.ContextID, lease.Epoch, lease.ExpiresAt)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrStaleEpoch, "tools: expired or superseded session lease")
	}
	return err
}

// takeToolEpoch is used for transactions that do not already run inside the
// ledger's write barriers.
func (s *Service) takeToolEpoch(ctx context.Context, tx pgx.Tx, scope contracts.Scope, lease ToolLease) error {
	if err := s.ledger.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
		return err
	}
	if err := s.ledger.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
		return err
	}
	return s.advanceToolEpoch(ctx, tx, scope, lease)
}

func leasedOperationKey(lease ToolLease, key string) (string, error) {
	if key == "" || len(key) > 128 || lease.SessionID == "" || lease.ContextID == "" || lease.Epoch <= 0 {
		return "", contracts.NewError(contracts.ErrInvalid, "tools: invalid leased operation")
	}
	digest := sha256.Sum256([]byte(lease.SessionID + "\x00" + key))
	return "leased:" + hex.EncodeToString(digest[:]), nil
}

// CallWithToolLease and FenceAndLookup share the Gateway's admission lock.
// A marker indicating IN_FLIGHT and the ledger dispatch right commit together.
func (s *Service) CallWithToolLease(ctx context.Context, scope contracts.Scope, key, tool string, args json.RawMessage, prov CallProvenance, lease ToolLease) (operations.Operation, error) {
	internalKey, err := leasedOperationKey(lease, key)
	if err != nil {
		return operations.Operation{}, err
	}
	digest, err := ApprovalDigest(tool, args)
	if err != nil {
		return operations.Operation{}, err
	}
	if err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.takeToolEpoch(ctx, tx, scope, lease); err != nil {
			return err
		}
		var recorded string
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(request_digest, '') FROM `+s.schema+`.gateway_operations
			WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
			scope.Tenant, lease.SessionID, key).Scan(&recorded)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err == nil && recorded != "" && recorded != digest {
			return contracts.NewError(contracts.ErrKeyReuse, "tools: operation key was used with a different request")
		}
		return err
	}); err != nil {
		return operations.Operation{}, err
	}
	check := func(ctx context.Context, tx pgx.Tx) error {
		if err := s.advanceToolEpoch(ctx, tx, scope, lease); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO `+s.schema+`.gateway_operations AS current
				(tenant_id, session_id, operation_key, created_epoch, request_digest, status)
			VALUES ($1,$2,$3,$4,$5,'IN_FLIGHT')
			ON CONFLICT (tenant_id, session_id, operation_key) DO UPDATE SET
				created_epoch=EXCLUDED.created_epoch, request_digest=EXCLUDED.request_digest,
				status='IN_FLIGHT', updated_at=clock_timestamp()
			WHERE current.status='FENCED_ABORTED'`, scope.Tenant, lease.SessionID, key, lease.Epoch, digest)
		return err
	}
	op, err := s.call(ctx, scope, internalKey, tool, args, prov, check,
		artifacts.Binding{ContextID: lease.SessionID, TaskID: prov.TaskID})
	if op.Key != "" {
		op.Key = key
	}
	return op, err
}

// FenceAndLookup raises the epoch and classifies the effect from the same
// admission transaction. Result bytes remain in the existing Gateway ledger.
func (s *Service) FenceAndLookup(ctx context.Context, scope contracts.Scope, key string, lease ToolLease) (FencedResult, error) {
	internalKey, err := leasedOperationKey(lease, key)
	if err != nil {
		return FencedResult{}, err
	}
	var stored string
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.takeToolEpoch(ctx, tx, scope, lease); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO `+s.schema+`.gateway_operations
				(tenant_id, session_id, operation_key, created_epoch, status)
			VALUES ($1,$2,$3,$4,'FENCED_ABORTED')
			ON CONFLICT (tenant_id, session_id, operation_key) DO NOTHING`, scope.Tenant, lease.SessionID, key, lease.Epoch)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 1 {
			stored = "FENCED_ABORTED"
			return nil
		}
		return tx.QueryRow(ctx, `
			SELECT status FROM `+s.schema+`.gateway_operations
			WHERE tenant_id=$1 AND session_id=$2 AND operation_key=$3`,
			scope.Tenant, lease.SessionID, key).Scan(&stored)
	})
	if err != nil {
		return FencedResult{}, err
	}
	if stored == "FENCED_ABORTED" {
		return FencedResult{Status: "SAFE_TO_RETRY"}, nil
	}
	op, err := s.ledger.Get(ctx, scope, internalKey)
	if err != nil {
		return FencedResult{}, err
	}
	switch op.Status {
	case operations.StatusSucceeded:
		result, err := s.GetResult(ctx, scope, ResultReader{ContextID: lease.ContextID}, internalKey)
		if err != nil {
			return FencedResult{}, err
		}
		result.Key = key
		return FencedResult{Status: "COMPLETED", Receipt: &result}, nil
	case operations.StatusSideEffectUnknown:
		return FencedResult{Status: "OUTCOME_UNKNOWN"}, nil
	case operations.StatusDispatched:
		return FencedResult{Status: "IN_FLIGHT"}, nil
	default:
		// A durable dispatch grant exists. A failure or fence remains unsafe
		// to repeat unless reconciled by the owner of the external operation.
		return FencedResult{Status: "OUTCOME_UNKNOWN"}, nil
	}
}
