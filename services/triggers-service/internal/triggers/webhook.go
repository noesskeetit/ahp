package triggers

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// WebhookMaxBody is enforced both at HTTP ingress and by library callers.
const WebhookMaxBody = 1 << 20

func webhookOperationKey(scope contracts.Scope, triggerID, messageID string) string {
	b, _ := json.Marshal([]string{scope.Tenant, scope.Deployment, scope.Subject, triggerID, messageID})
	digest := sha256.Sum256(b)
	return "webhook:" + hex.EncodeToString(digest[:])
}

// ReceiveWebhook verifies HMAC before any delivery reservation or effect ledger
// write. An identical delivery resumes its durable sequence; changing any raw
// body byte under that ID is a key-reuse error, even after the trigger exhausts.
func (s *Service) ReceiveWebhook(ctx context.Context, scope contracts.Scope, triggerID, messageID, signature string, body []byte) (operations.Operation, error) {
	if err := scope.Validate(); err != nil {
		return operations.Operation{}, err
	}
	if triggerID == "" || messageID == "" || len(messageID) > 256 || strings.ContainsAny(messageID, "\r\n") || len(body) > WebhookMaxBody {
		return operations.Operation{}, contracts.NewError(contracts.ErrInvalid, "triggers: invalid webhook delivery ID or body size")
	}
	trig, err := s.Get(ctx, scope, triggerID)
	if err != nil {
		return operations.Operation{}, err
	}
	if trig.Kind != KindWebhook {
		return operations.Operation{}, contracts.NewError(contracts.ErrInvalid, "triggers: trigger is not a webhook")
	}
	secret, err := s.cfg.Secrets.Resolve(ctx, scope, trig.SecretRef)
	if err != nil || len(secret) == 0 {
		return operations.Operation{}, contracts.NewError(contracts.ErrForbidden, "triggers: webhook secret unresolved")
	}
	if !verifyWebhookSignature(secret, canonicalWebhookMessage(triggerID, messageID, body), signature) {
		return operations.Operation{}, contracts.NewError(contracts.ErrForbidden, "triggers: webhook signature invalid")
	}
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	key := webhookOperationKey(scope, triggerID, messageID)
	delivery, exists, err := s.webhookDelivery(ctx, trig, messageID)
	if err != nil {
		return operations.Operation{}, err
	}
	if exists {
		if delivery.digest != digest {
			return operations.Operation{}, contracts.NewError(contracts.ErrKeyReuse, "triggers: message_id reused with a different delivery body")
		}
		if existing, gerr := s.ledger.Get(ctx, scope, key); gerr == nil && existing.Status != operations.StatusAccepted {
			return existing, nil
		} else if gerr != nil && contracts.Code(gerr) != contracts.ErrNotFound {
			return operations.Operation{}, gerr
		}
	} else {
		// Old library-only deliveries did not bind the body. They cannot be
		// safely replayed under the new contract or treated as fresh work.
		oldKey := fmt.Sprintf("trigger:%s:wh:%s", triggerID, messageID)
		if len(oldKey) <= 128 {
			if _, gerr := s.ledger.Get(ctx, scope, oldKey); gerr == nil {
				return operations.Operation{}, contracts.NewError(contracts.ErrConflict, "triggers: legacy delivery has no authenticated body fingerprint")
			} else if contracts.Code(gerr) != contracts.ErrNotFound {
				return operations.Operation{}, gerr
			}
		}
	}
	if trig.Status == StatusRevoked {
		return operations.Operation{}, contracts.NewError(contracts.ErrForbidden, "triggers: webhook revoked")
	}
	if !trig.Delegated {
		active, aerr := s.cfg.Tasks.IsTaskActive(ctx, scope, trig.ParentTask)
		if aerr != nil {
			return operations.Operation{}, fmt.Errorf("triggers: parent liveness: %w", aerr)
		}
		if !active {
			return operations.Operation{}, contracts.NewError(contracts.ErrForbidden, "triggers: parent task inactive")
		}
	}
	token, expires, err := s.cfg.Issuer.IssueToken(ctx, scope, trig.DelegationRef, s.cfg.FireTTL)
	if err != nil {
		s.stopIfDelegationGone(ctx, trig, err)
		return operations.Operation{}, err
	}
	if token == "" || !expires.After(s.cfg.Clock()) {
		return operations.Operation{}, contracts.NewError(contracts.ErrForbidden, "triggers: unusable delegated task credential")
	}
	delivery, err = s.admitWebhook(ctx, trig, messageID, digest)
	if err != nil {
		return operations.Operation{}, err
	}
	args, _ := json.Marshal(map[string]any{"sequence": delivery.sequence, "target": trig.Target, "delegated": trig.Delegated, "body_sha256": digest, "configuration": rawOrEmpty(trig.Payload)})
	op, err := s.ledger.Accept(ctx, scope, operations.Request{Key: key, Kind: EffectKind, Target: trig.Target, Arguments: args})
	if err != nil {
		// A concurrent identical delivery may settle after our initial lookup.
		// Admission already bound this message ID to the exact body; a sealed
		// ledger key is its durable receipt, never permission to dispatch again.
		if contracts.Code(err) == contracts.ErrConflict {
			return s.ledger.Get(ctx, scope, key)
		}
		return operations.Operation{}, err
	}
	if op.Status != operations.StatusAccepted {
		return op, nil
	}
	// Admission and Accept can wait behind other transactions. Re-check live
	// authority at the send boundary so an old minted token cannot carry a
	// delivery past a revocation that happened while it was still unsent.
	token, expires, err = s.webhookDispatchCredential(ctx, trig)
	if err != nil {
		if permanentAuthorityLoss(err) {
			settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if _, fenceErr := s.ledger.Fence(settle, scope, key); fenceErr != nil {
				return op, fmt.Errorf("triggers: revoked delivery fence: %w", fenceErr)
			}
		}
		return op, err
	}
	if _, err = s.ledger.DispatchChecked(ctx, scope, key, func(ctx context.Context, tx pgx.Tx) error {
		var status string
		err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT status FROM %s.triggers WHERE trigger_id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4 FOR UPDATE`, quoteIdent(s.schema)), triggerID, scope.Tenant, scope.Deployment, scope.Subject).Scan(&status)
		if err != nil {
			return err
		}
		if status == StatusRevoked {
			return contracts.NewError(contracts.ErrForbidden, "triggers: webhook revoked before dispatch")
		}
		return nil
	}); err != nil {
		if errors.Is(err, operations.ErrAlreadyDispatched) {
			return s.ledger.Get(ctx, scope, key)
		}
		if contracts.Code(err) == contracts.ErrForbidden {
			settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			if _, fenceErr := s.ledger.Fence(settle, scope, key); fenceErr != nil {
				return op, fenceErr
			}
		}
		return operations.Operation{}, err
	}
	event := map[string]any{"message_id": messageID}
	if json.Valid(body) {
		event["body"] = json.RawMessage(body)
	} else {
		event["body_base64"] = base64.StdEncoding.EncodeToString(body)
	}
	payload, _ := json.Marshal(map[string]any{"configuration": rawOrEmpty(trig.Payload), "webhook": event})
	res, fireErr := s.cfg.Dispatcher.Fire(ctx, scope, FireRequest{TriggerID: triggerID, Sequence: delivery.sequence, Token: token, ExpiresAt: expires, Target: trig.Target, Kind: KindWebhook, Payload: payload, Delegated: trig.Delegated})
	// The request can outlive the caller connection. Settle a known outcome on a
	// bounded detached context; if that write fails, DISPATCHED stays reserved.
	settle, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if fireErr != nil {
		if errors.Is(fireErr, ErrDispatchRefused) {
			return s.ledger.Complete(settle, scope, key, json.RawMessage(`{"error":"dispatch_refused"}`), true)
		}
		op, markErr := s.ledger.MarkUnknown(settle, scope, key)
		if markErr != nil {
			return op, fmt.Errorf("triggers: unknown delivery settlement: %w", markErr)
		}
		return op, fmt.Errorf("triggers: fire outcome unknown: %w", fireErr)
	}
	result, err := json.Marshal(res)
	if err != nil {
		_, _ = s.ledger.MarkUnknown(settle, scope, key)
		return op, err
	}
	return s.ledger.Complete(settle, scope, key, result, false)
}

func (s *Service) webhookDispatchCredential(ctx context.Context, trig Trigger) (string, time.Time, error) {
	token, expires, err := s.cfg.Issuer.IssueToken(ctx, trig.scope(), trig.DelegationRef, s.cfg.FireTTL)
	if err != nil {
		s.stopIfDelegationGone(ctx, trig, err)
		return "", time.Time{}, err
	}
	if token == "" || !expires.After(s.cfg.Clock()) {
		return "", time.Time{}, contracts.NewError(contracts.ErrForbidden, "triggers: unusable delegated task credential")
	}
	current, err := s.Get(ctx, trig.scope(), trig.TriggerID)
	if err != nil {
		return "", time.Time{}, err
	}
	if current.Status == StatusRevoked {
		return "", time.Time{}, contracts.NewError(contracts.ErrForbidden, "triggers: webhook revoked")
	}
	if !current.Delegated {
		active, err := s.cfg.Tasks.IsTaskActive(ctx, current.scope(), current.ParentTask)
		if err != nil {
			return "", time.Time{}, err
		}
		if !active {
			return "", time.Time{}, contracts.NewError(contracts.ErrForbidden, "triggers: parent task inactive")
		}
	}
	return token, expires, nil
}
