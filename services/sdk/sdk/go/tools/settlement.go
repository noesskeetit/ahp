package tools

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	state "github.com/ahp-platform/ahp/sdk/go/state/journal"
)

// LeasedCall identifies an intent already committed to State. Arguments must
// have exactly the bytes hashed when the intent was prepared.
type LeasedCall struct {
	Scope     contracts.Scope
	CallID    string
	Key       string
	Tool      string
	Arguments json.RawMessage
}

// SettleLeasedCall recovers a committed intent or makes one permitted Gateway
// attempt. It waits for an in-flight operation only while the turn is live.
// A canceled attempt may reconcile once; an unresolved intent stays DISPATCHED.
func (c *Client) SettleLeasedCall(ctx context.Context, store *state.Client, lease *state.VMLease, call LeasedCall) (journal.ToolIntent, error) {
	if store == nil || lease == nil || ctx == nil || call.CallID == "" || call.Key == "" || call.Tool == "" || !json.Valid(call.Arguments) {
		return journal.ToolIntent{}, invalid("state client, lease and prepared tool call are required")
	}
	if err := call.Scope.Validate(); err != nil {
		return journal.ToolIntent{}, err
	}
	turn := lease.Current()
	currentLease := func() (state.ClaimedTurn, error) {
		current := lease.Current()
		if current.SessionID != turn.SessionID || current.Epoch != turn.Epoch || current.InputSeq != turn.InputSeq || current.Worker != turn.Worker ||
			!time.Now().Before(current.ExpiresAt) {
			return state.ClaimedTurn{}, contracts.NewError(contracts.ErrStaleEpoch, "tool settlement no longer owns this turn")
		}
		return current, nil
	}
	proof := func() (LeaseProof, error) {
		current, err := currentLease()
		if err != nil {
			return LeaseProof{}, err
		}
		p := LeaseProof{current.SessionID, current.Epoch, current.ToolLeaseProof}
		return p, p.validate()
	}
	if _, err := proof(); err != nil {
		return journal.ToolIntent{}, err
	}
	intent, err := store.GetToolIntent(ctx, turn.SessionID, call.CallID)
	if err != nil {
		return journal.ToolIntent{}, err
	}
	digest := sha256.Sum256(call.Arguments)
	if intent.CallID != call.CallID || intent.IdempotencyKey != call.Key || intent.ToolName != call.Tool ||
		!bytes.Equal(intent.ArgsDigest, digest[:]) || (intent.GatewayOperationID != "" && intent.GatewayOperationID != call.Key) {
		return journal.ToolIntent{}, contracts.NewError(contracts.ErrKeyReuse, "tool call does not match the prepared intent")
	}
	switch intent.Status {
	case journal.IntentCompleted, journal.IntentRefused, journal.IntentUnknown:
		return intent, nil
	case journal.IntentPrepared, journal.IntentDispatched, journal.IntentAwaitingApproval, journal.IntentAwaitingAuth:
	default:
		return intent, unknown("unexpected durable tool intent status")
	}

	dispatch := func(ctx context.Context) error {
		if intent.Status == journal.IntentDispatched {
			return nil
		}
		transition := journal.IntentTransition{CallID: call.CallID, TurnSeq: turn.InputSeq, Status: journal.IntentDispatched, GatewayOperationID: call.Key}
		if err := store.MutateToolIntent(ctx, turn.SessionID, turn.Epoch, transition); err != nil {
			return err
		}
		intent.IntentTransition = transition
		return nil
	}
	save := func(ctx context.Context, status, reason string, payload *journal.Payload) (journal.ToolIntent, error) {
		if _, err := proof(); err != nil {
			return intent, err
		}
		if err := dispatch(ctx); err != nil {
			return intent, err
		}
		// The event identity is stable across competing writers. A waiting
		// gate can be reached again in a later turn after a human replies.
		identity := call.CallID + "\x00" + status
		if status == journal.IntentAwaitingApproval || status == journal.IntentAwaitingAuth {
			identity += "\x00" + strconv.FormatInt(turn.InputSeq, 10)
		}
		eventKey := sha256.Sum256([]byte(identity))
		key := "tool-settle:" + hex.EncodeToString(eventKey[:])
		event, err := json.Marshal(map[string]string{"call_id": call.CallID, "tool": call.Tool, "status": status, "reason": reason})
		if err != nil {
			return intent, err
		}
		transition := journal.IntentTransition{CallID: call.CallID, TurnSeq: turn.InputSeq, Status: status, GatewayOperationID: call.Key, Result: payload}
		req := journal.AppendRequest{
			TurnSeq: turn.InputSeq, Key: key,
			Events:       []journal.EventInput{{ID: key, Type: "tool.outcome", Role: "tool", Codec: "json", Payload: journal.Payload{Inline: event}}},
			ToolOutcomes: []journal.IntentTransition{transition},
		}
		if status == journal.IntentAwaitingApproval || status == journal.IntentAwaitingAuth {
			req.FinishTurn = "WAITING_INPUT"
		}
		receipt, committed, err := appendToolOutcome(ctx, store, turn, req, transition)
		if err != nil {
			return intent, err
		}
		if req.FinishTurn != "" {
			lease.ApplyFinish(turn.InputSeq, receipt)
		}
		if committed != nil {
			return *committed, nil
		}
		intent.IntentTransition = transition
		return intent, nil
	}
	lookup := func(ctx context.Context) (FencedLookup, error) {
		current, err := proof()
		if err != nil {
			return FencedLookup{}, err
		}
		return c.FenceAndLookup(ctx, call.Key, current)
	}
	settle := func(ctx context.Context, result FencedLookup) (journal.ToolIntent, error) {
		switch result.Status {
		case LookupCompleted:
			r := result.Receipt
			if r.Target != call.Tool || r.Provenance.GrantContext != journal.VMSessionContextID(call.Scope, turn.SessionID) ||
				!ownerDigestMatches(r.Digest, call.Tool, call.Arguments) {
				return intent, unknown("Gateway receipt does not match the prepared operation")
			}
			payload, err := journalResult(*r)
			if err != nil {
				return intent, err
			}
			return save(ctx, journal.IntentCompleted, "", &payload)
		case LookupOutcomeUnknown:
			return save(ctx, journal.IntentUnknown, LookupOutcomeUnknown, nil)
		case LookupInFlight:
			if err := dispatch(ctx); err != nil {
				return intent, err
			}
			return intent, nil
		default:
			// A detached cleanup can see SAFE_TO_RETRY. Only a subsequent
			// live invocation with ownership may authorize dispatch.
			return intent, nil
		}
	}

	settleLive := func(result FencedLookup) (journal.ToolIntent, error) {
		if result.Status != LookupInFlight {
			return settle(ctx, result)
		}
		if err := dispatch(ctx); err != nil {
			return intent, err
		}
		const maxBackoff = 200 * time.Millisecond
		backoff := 10 * time.Millisecond
		for result.Status == LookupInFlight {
			current, err := currentLease()
			if err != nil {
				return intent, err
			}
			pause := time.NewTimer(min(backoff, time.Until(current.ExpiresAt)))
			select {
			case <-ctx.Done():
				pause.Stop()
				return intent, ctx.Err()
			case <-pause.C:
			}
			if err := ctx.Err(); err != nil {
				return intent, err
			}
			current, err = currentLease() // A heartbeat can renew the proof during backoff.
			if err != nil {
				return intent, err
			}
			lookupCtx, cancel := context.WithDeadline(ctx, current.ExpiresAt)
			next, err := lookup(lookupCtx)
			deadline := lookupCtx.Err()
			cancel()
			if ctx.Err() != nil {
				return intent, ctx.Err()
			}
			if _, leaseErr := currentLease(); leaseErr != nil {
				return intent, leaseErr
			}
			backoff = min(backoff*2, maxBackoff)
			if err != nil {
				if deadline == context.DeadlineExceeded {
					// A renewal kept the turn live beyond this lookup's deadline.
					continue
				}
				return intent, err
			}
			result = next
		}
		if result.Status == LookupSafeToRetry {
			return intent, unknown("Gateway reported SAFE_TO_RETRY for an in-flight operation")
		}
		return settle(ctx, result)
	}

	result, err := lookup(ctx)
	if err != nil {
		return intent, err
	}
	if result.Status != LookupSafeToRetry {
		return settleLive(result)
	}
	if err := dispatch(ctx); err != nil {
		return intent, err
	}
	current, err := proof()
	if err != nil {
		return intent, err
	}
	_, callErr := c.CallWithLease(ctx, call.Key, call.Tool, call.Arguments, current)
	if callErr != nil && ctx.Err() == nil {
		var api *Error
		if !errors.As(callErr, &api) {
			return intent, callErr
		}
		switch {
		case api.Status == http.StatusConflict && api.Code == contracts.ErrConflict && api.Reason == "APPROVAL_REQUIRED":
			return save(ctx, journal.IntentAwaitingApproval, api.Reason, nil)
		case api.Status == http.StatusConflict && api.Code == "AUTH_REQUIRED" && api.Reason == "AUTH_REQUIRED":
			return save(ctx, journal.IntentAwaitingAuth, api.Reason, nil)
		case api.Status == http.StatusConflict && api.Code == contracts.ErrConflict && api.Reason == "APPROVAL_DECLINED",
			api.Status == http.StatusForbidden && api.Code == contracts.ErrForbidden && api.Reason == "POLICY_DENIED":
			body, err := json.Marshal(map[string]string{"reason": api.Reason, "tool": call.Tool})
			if err != nil {
				return intent, err
			}
			return save(ctx, journal.IntentRefused, api.Reason, &journal.Payload{Inline: body})
		case !api.OutcomeUnknown:
			return intent, callErr
		}
	}

	// The request might have crossed the external-effect boundary even when
	// its transport was canceled. The detached path may only reconcile.
	if ctx.Err() == nil {
		result, err = lookup(ctx)
		if err == nil {
			if result.Status == LookupInFlight {
				return settleLive(result)
			}
			settled, settleErr := settle(ctx, result)
			if settleErr == nil || ctx.Err() == nil {
				return settled, settleErr
			}
		} else if ctx.Err() == nil {
			return intent, err
		}
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2500*time.Millisecond)
	defer cancel()
	result, err = lookup(cleanup)
	if err != nil {
		return intent, err
	}
	return settle(cleanup, result)
}

// An admission can advance the head between inspection and commit. Retry only
// the State append, retaining the immutable outcome and event key.
func appendToolOutcome(ctx context.Context, store *state.Client, turn state.ClaimedTurn, request journal.AppendRequest, transition journal.IntentTransition) (journal.AppendReceipt, *journal.ToolIntent, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		head, err := store.InspectSession(ctx, turn.SessionID)
		if err != nil {
			return journal.AppendReceipt{}, nil, err
		}
		if attempt > 0 && head.HeadSeq <= request.ExpectedSeq {
			break
		}
		request.ExpectedSeq = head.HeadSeq
		receipt, err := store.AppendEvents(ctx, turn.SessionID, turn.Epoch, request)
		if err == nil {
			return receipt, nil, nil
		}
		if committed, readErr := store.GetToolIntent(ctx, turn.SessionID, transition.CallID); readErr == nil &&
			committed.Status == transition.Status && committed.GatewayOperationID == transition.GatewayOperationID {
			return journal.AppendReceipt{}, &committed, nil
		}
		lastErr = err
		if contracts.Code(err) != contracts.ErrConflict {
			break
		}
	}
	return journal.AppendReceipt{}, nil, lastErr
}

func ownerDigestMatches(digest, tool string, args json.RawMessage) bool {
	var decoded any
	if json.Unmarshal(args, &decoded) != nil {
		return false
	}
	approval, err := contracts.Digest(map[string]any{"tool": tool, "arguments": decoded})
	if err != nil {
		return false
	}
	for _, guard := range []string{"", approval} {
		d, err := contracts.Digest(map[string]any{"kind": "tool.call", "target": tool, "guard": guard, "arguments": args})
		if err == nil && d == digest {
			return true
		}
	}
	return false
}

func journalResult(r ToolResult) (journal.Payload, error) {
	if r.Storage == StorageArtifact {
		body, err := json.Marshal(struct {
			Storage string       `json:"result_storage"`
			Ref     *ArtifactRef `json:"artifact_ref"`
		}{r.Storage, r.Ref})
		if err != nil {
			return journal.Payload{}, err
		}
		return journal.Payload{Inline: body}, nil
	}
	if len(r.Payload) > 256<<10 {
		return journal.Payload{}, unknown("oversized Gateway receipt requires an artifact reference")
	}
	return journal.Payload{Inline: r.Payload}, nil
}
