package statejournal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

type StandaloneToolRequest = journal.StandaloneToolRequest
type StandaloneToolDecision = journal.StandaloneToolDecision

// BeginStandaloneTool durably reserves the one permitted external dispatch.
// After a lease changes, an unfinished external call is always unknown,
// including a PREPARED record whose old worker never recorded dispatch.
func (s *Store) BeginStandaloneTool(ctx context.Context, scope contracts.Scope, session string, lease Lease, request StandaloneToolRequest) (StandaloneToolDecision, error) {
	var decision StandaloneToolDecision
	if err := scope.Validate(); err != nil {
		return decision, err
	}
	if !validID(session) || !validID(request.CallID) || !validID(request.ToolName) || request.ArgsDigest == (SHA256Digest{}) {
		return decision, contracts.NewError(contracts.ErrInvalid, "invalid standalone tool identity")
	}
	decision.CallID = request.CallID
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := lockSession(ctx, tx, scope, session, lease); err != nil {
			return err
		}
		var (
			epoch    *int64
			tool     string
			args     []byte
			response Payload
		)
		err := tx.QueryRow(ctx, `
			SELECT status, coalesce(gateway_operation_id,''), prepared_epoch, tool_name,
			       args_digest, response_payload, response_blob_ref
			FROM ahp_journal.tool_intents WHERE tenant_id=$1 AND session_id=$2 AND call_id=$3`,
			scope.Tenant, session, request.CallID).Scan(&decision.Status, &decision.GatewayOperationID,
			&epoch, &tool, &args, &response.Inline, &response.Blob)
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrNotFound, "standalone tool requires a prepared intent")
		}
		if err != nil {
			return err
		}
		if tool != request.ToolName || !bytes.Equal(args, request.ArgsDigest[:]) {
			return contracts.NewError(contracts.ErrKeyReuse, "tool arguments do not match the prepared intent")
		}
		switch decision.Status {
		case IntentCompleted:
			decision.Result = &response
			return nil
		case IntentUnknown:
			return nil
		}
		if decision.GatewayOperationID == "" {
			hash := sha256.Sum256([]byte(request.CallID))
			decision.GatewayOperationID = "standalone-" + hex.EncodeToString(hash[:])
		}
		current := epoch != nil && *epoch == lease.Epoch
		if request.AckLost || !current {
			decision.Status = IntentUnknown
		} else if decision.Status == IntentPrepared {
			decision.Status = IntentDispatched
			decision.CanExecute = true
		} else {
			return nil
		}
		_, err = tx.Exec(ctx, `UPDATE ahp_journal.tool_intents
			SET status=$4,gateway_operation_id=$5 WHERE tenant_id=$1 AND session_id=$2 AND call_id=$3`,
			scope.Tenant, session, request.CallID, decision.Status, decision.GatewayOperationID)
		return err
	})
	if err != nil {
		return StandaloneToolDecision{}, err
	}
	return decision, nil
}
