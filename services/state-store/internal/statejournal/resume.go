package statejournal

import (
	"context"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

type ResumeState = journal.ResumeState

func (s *Store) InspectSession(ctx context.Context, scope contracts.Scope, session string) (ResumeState, error) {
	var state ResumeState
	if err := scope.Validate(); err != nil {
		return state, err
	}
	if !validID(session) {
		return state, contracts.NewError(contracts.ErrInvalid, "invalid session identity")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return state, err
	}
	defer tx.Rollback(ctx)
	state.SessionID = session
	err = tx.QueryRow(ctx, `
		SELECT harness_type,format_version,status,head_seq,head_checkpoint_id,leaf_entry_id,append_parent_id,parent_session_id
		FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND session_id=$2 AND deployment_id=$3 AND subject_id=$4 AND status<>'DELETED'`,
		scope.Tenant, session, scope.Deployment, scope.Subject).Scan(&state.Harness, &state.FormatVersion, &state.Status,
		&state.HeadSeq, &state.HeadCheckpoint, &state.Leaf, &state.AppendParent, &state.ParentSessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ResumeState{}, contracts.NewError(contracts.ErrNotFound, "session not found")
	}
	if err != nil {
		return ResumeState{}, err
	}
	rows, err := tx.Query(ctx, `
		SELECT call_id,status,coalesce(gateway_operation_id,''),tool_name,args_digest
		FROM ahp_journal.tool_intents WHERE tenant_id=$1 AND session_id=$2
		  AND status IN ('PREPARED','DISPATCHED') ORDER BY call_id`, scope.Tenant, session)
	if err != nil {
		return ResumeState{}, err
	}
	state.Unresolved, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (ToolIntent, error) {
		var tool ToolIntent
		err := row.Scan(&tool.CallID, &tool.Status, &tool.GatewayOperationID, &tool.ToolName, &tool.ArgsDigest)
		return tool, err
	})
	if err != nil {
		return ResumeState{}, err
	}
	return state, tx.Commit(ctx)
}
