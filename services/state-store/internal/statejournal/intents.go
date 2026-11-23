package statejournal

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5"
)

const (
	IntentPrepared         = journal.IntentPrepared
	IntentDispatched       = journal.IntentDispatched
	IntentAwaitingApproval = journal.IntentAwaitingApproval
	IntentAwaitingAuth     = journal.IntentAwaitingAuth
	IntentCompleted        = journal.IntentCompleted
	IntentRefused          = journal.IntentRefused
	IntentUnknown          = journal.IntentUnknown
)

type IntentTransition = journal.IntentTransition

type ToolIntent = journal.ToolIntent

func (s *Store) GetToolIntent(ctx context.Context, scope contracts.Scope, session, callID string) (ToolIntent, error) {
	if err := scope.Validate(); err != nil {
		return ToolIntent{}, err
	}
	if !validID(session) || !validID(callID) {
		return ToolIntent{}, contracts.NewError(contracts.ErrInvalid, "invalid tool intent identity")
	}
	result := ToolIntent{IntentTransition: IntentTransition{CallID: callID}}
	var payload Payload
	err := s.pool.QueryRow(ctx, `
		SELECT i.status, coalesce(i.gateway_operation_id,''), i.tool_name, i.args_digest, i.idempotency_key,
		       i.response_payload, i.response_blob_ref
		FROM ahp_journal.tool_intents i JOIN ahp_journal.sessions t USING (tenant_id, session_id)
		WHERE i.tenant_id=$1 AND i.session_id=$2 AND i.call_id=$3
		  AND t.deployment_id=$4 AND t.subject_id=$5 AND t.status <> 'DELETED'`,
		scope.Tenant, session, callID, scope.Deployment, scope.Subject).
		Scan(&result.Status, &result.GatewayOperationID, &result.ToolName, &result.ArgsDigest, &result.IdempotencyKey,
			&payload.Inline, &payload.Blob)
	if errors.Is(err, pgx.ErrNoRows) {
		err = contracts.NewError(contracts.ErrNotFound, "tool intent not found")
	}
	if err != nil {
		return ToolIntent{}, err
	}
	if payload.Inline != nil || payload.Blob != nil {
		result.Result = &payload
	}
	return result, nil
}

type refusalDescription struct {
	Reason string `json:"reason"`
	Tool   string `json:"tool"`
}

func (s *Store) MutateToolIntent(ctx context.Context, scope contracts.Scope, session string, lease Lease, transition IntentTransition) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := validateIntentTransition(transition); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, turn, err := lockSessionState(ctx, tx, scope, session, lease)
	if err != nil {
		return err
	}
	if err := turn.validate(transition.TurnSeq, nil); err != nil {
		return err
	}
	if requiresJournalOutcome(transition.Status) {
		return contracts.NewError(contracts.ErrInvalid, "tool outcome requires an atomic journal commit")
	}
	if transition.Result != nil {
		if err := s.pinBlobs(ctx, tx, scope, session, addBlob(nil, *transition.Result)); err != nil {
			return err
		}
	}
	if err := mutateIntent(ctx, tx, scope.Tenant, session, turn.Mode == journal.ExecutionVMManaged, transition); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func validateIntentTransition(transition IntentTransition) error {
	if !validID(transition.CallID) || !validID(transition.GatewayOperationID) {
		return contracts.NewError(contracts.ErrInvalid, "invalid tool intent identity")
	}
	switch transition.Status {
	case IntentDispatched, IntentUnknown, IntentAwaitingApproval, IntentAwaitingAuth:
		if transition.Result != nil {
			return contracts.NewError(contracts.ErrInvalid, "tool transition does not accept a result")
		}
	case IntentCompleted:
		if transition.Result == nil || !validPayload(*transition.Result) {
			return contracts.NewError(contracts.ErrInvalid, "completed tools require a valid result")
		}
	case IntentRefused:
		var refusal refusalDescription
		if transition.Result == nil || transition.Result.Blob != nil || !validPayload(*transition.Result) ||
			json.Unmarshal(transition.Result.Inline, &refusal) != nil || !validID(refusal.Tool) ||
			(refusal.Reason != "POLICY_DENIED" && refusal.Reason != "APPROVAL_DECLINED") {
			return contracts.NewError(contracts.ErrInvalid, "refused tools require a reason and tool in an inline JSON result")
		}
	default:
		return contracts.NewError(contracts.ErrInvalid, "invalid tool intent transition")
	}
	return nil
}

func awaitsToolInput(status string) bool {
	return status == IntentAwaitingApproval || status == IntentAwaitingAuth
}

func requiresJournalOutcome(status string) bool {
	return awaitsToolInput(status) || status == IntentRefused
}

func validateOutcomes(outcomes []IntentTransition) error {
	if len(outcomes) > 256 {
		return contracts.NewError(contracts.ErrInvalid, "tool outcome batch exceeds 256 items")
	}
	for _, outcome := range outcomes {
		if outcome.Status != IntentCompleted && outcome.Status != IntentUnknown && !requiresJournalOutcome(outcome.Status) {
			return contracts.NewError(contracts.ErrInvalid, "invalid journal tool outcome")
		}
		if err := validateIntentTransition(outcome); err != nil {
			return err
		}
	}
	return nil
}

func commitOutcomes(ctx context.Context, tx pgx.Tx, tenant, session string, vmManaged bool, outcomes []IntentTransition) error {
	for _, outcome := range outcomes {
		if err := mutateIntent(ctx, tx, tenant, session, vmManaged, outcome); err != nil {
			return err
		}
	}
	return nil
}

func mutateIntent(ctx context.Context, tx pgx.Tx, tenant, session string, vmManaged bool, transition IntentTransition) error {
	result := Payload{}
	if transition.Result != nil {
		result = *transition.Result
	}
	args := []any{tenant, session, transition.CallID, transition.Status, transition.GatewayOperationID,
		result.Inline, blobValue(result)}
	var previous, operationID, idempotencyKey, toolName string
	var same bool
	err := tx.QueryRow(ctx, `
		SELECT status, coalesce(gateway_operation_id,''), idempotency_key, tool_name, status=$4 AND coalesce(gateway_operation_id,'')=$5
		  AND response_payload IS NOT DISTINCT FROM $6::bytea
		  AND response_blob_ref IS NOT DISTINCT FROM $7::jsonb
		FROM ahp_journal.tool_intents
		WHERE tenant_id=$1 AND session_id=$2 AND call_id=$3`, args...).Scan(&previous, &operationID, &idempotencyKey, &toolName, &same)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.NewError(contracts.ErrNotFound, "tool intent not found")
	}
	if err != nil {
		return err
	}
	if operationID != "" && operationID != transition.GatewayOperationID {
		return contracts.NewError(contracts.ErrKeyReuse, "tool operation identity changed")
	}
	newStatus := requiresJournalOutcome(previous) || requiresJournalOutcome(transition.Status)
	// Standalone admission assigned its own key in older VM generations.
	// Continue only an already recorded standalone identity with legacy statuses.
	standalone := operationID != "" && operationID == standaloneOperationID(transition.CallID)
	if transition.GatewayOperationID != idempotencyKey && (newStatus || (vmManaged && !standalone)) {
		return contracts.NewError(contracts.ErrKeyReuse, "tool operation does not match the prepared idempotency key")
	}
	if same {
		return nil
	}
	if previous == IntentUnknown {
		return contracts.NewReasoned(contracts.ErrConflict, IntentUnknown, "tool result requires reconciliation")
	}
	if !allowedIntentTransition(previous, transition.Status) {
		return contracts.NewError(contracts.ErrConflict, "tool intent transition is not allowed")
	}
	if transition.Status == IntentRefused {
		var refusal refusalDescription
		if err := json.Unmarshal(result.Inline, &refusal); err != nil || refusal.Tool != toolName {
			return contracts.NewError(contracts.ErrKeyReuse, "refusal tool does not match the prepared intent")
		}
	}
	_, err = tx.Exec(ctx, `
		UPDATE ahp_journal.tool_intents
		SET status=$4, gateway_operation_id=$5, response_payload=$6, response_blob_ref=$7
		WHERE tenant_id=$1 AND session_id=$2 AND call_id=$3`, args...)
	return err
}

func allowedIntentTransition(previous, next string) bool {
	switch next {
	case IntentDispatched:
		return previous == IntentPrepared || awaitsToolInput(previous)
	case IntentAwaitingApproval, IntentAwaitingAuth:
		return previous == IntentPrepared || previous == IntentDispatched
	case IntentRefused:
		return previous == IntentPrepared || previous == IntentDispatched || awaitsToolInput(previous)
	case IntentCompleted, IntentUnknown:
		return previous == IntentDispatched
	default:
		return false
	}
}
