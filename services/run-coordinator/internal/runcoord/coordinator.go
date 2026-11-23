package runcoord

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/ahp-platform/ahp/pkg/transport"
)

type State interface {
	EnqueueTurn(context.Context, journal.EnqueueRequest) (journal.EnqueueReceipt, error)
	GetTurnStatus(context.Context, string) (journal.TurnStatus, error)
	InspectSession(context.Context, string) (journal.ResumeState, error)
	GetEvents(context.Context, string, int64, int, string) (journal.EventPage, error)
	CancelSession(context.Context, string, int64) (journal.TurnStatus, error)
	RequeueFencedVMTurn(context.Context, string, journal.FenceRequeueRequest) error
}

type GlobalState interface {
	ListGlobalRecoverableVMPage(context.Context, string, *journal.VMReconcileCursor) (journal.VMReconcilePage, error)
}

type Execution struct {
	Scope         contracts.Scope `json:"scope"`
	SessionID     string          `json:"session_id"`
	Harness       string          `json:"harness_type,omitempty"`
	LaunchTurnSeq int64           `json:"launch_turn_seq,omitempty"`
	Generation    int64           `json:"generation"`
	ExecutionID   string          `json:"execution_id"`
}

type VMDispatcher interface {
	EnsureRunning(context.Context, Execution) error
	Fence(context.Context, Execution) error
}

type SessionSignal = journal.SessionSignal

type SignalBus interface {
	Subscribe(SessionSignal) (<-chan struct{}, func())
}

type Coordinator struct {
	AccessPolicy AccessPolicy
	StateFor     func(contracts.Scope) State
	GlobalState  GlobalState
	Dispatcher   VMDispatcher
	Signals      SignalBus
}

func (c *Coordinator) Reconcile(ctx context.Context) error {
	return errors.Join(c.reconcileStatus(ctx, "QUEUED"), c.reconcileStatus(ctx, "ACTIVE"))
}

func (c *Coordinator) reconcileStatus(ctx context.Context, status string) error {
	if c.GlobalState == nil || c.Dispatcher == nil {
		return errors.New("VM reconciliation requires global State and a dispatcher")
	}
	var cursor *journal.VMReconcileCursor
	var firstErr error
	var failures int
	for ctx.Err() == nil {
		page, err := c.GlobalState.ListGlobalRecoverableVMPage(ctx, status, cursor)
		if err != nil {
			return errors.Join(firstErr, err)
		}
		for _, session := range page.Sessions {
			if ctx.Err() != nil {
				break
			}
			if err := c.reconcileTurn(ctx, session); err != nil {
				failures++
				if firstErr == nil {
					firstErr = fmt.Errorf("reconcile %s: %w", session.SessionID, err)
				}
			}
		}
		if page.Next == nil {
			break
		}
		if cursor != nil && *page.Next == *cursor {
			return errors.Join(firstErr, errors.New("VM reconcile cursor did not advance"))
		}
		cursor = page.Next
	}
	if firstErr != nil {
		return fmt.Errorf("%d %s VM reconciliations failed: %w", failures, status, firstErr)
	}
	return ctx.Err()
}

func (c *Coordinator) reconcileTurn(ctx context.Context, s journal.VMSession) error {
	if err := s.Scope.Validate(); err != nil {
		return fmt.Errorf("VM reconcile returned an invalid scope: %w", err)
	}
	execution := Execution{Scope: s.Scope, SessionID: s.SessionID, Harness: s.Harness,
		Generation: s.Epoch, ExecutionID: s.ExecutionID}
	if s.Status == "ACTIVE" {
		if s.ExecutionID == "" {
			return errors.New("active VM has no recorded execution owner")
		}
		state := c.stateFor(s.Scope)
		if state == nil {
			return errors.New("VM reconciliation has no State client for the session scope")
		}
		// The physical owner survives a warm followup.
		if err := c.Dispatcher.Fence(ctx, execution); err != nil {
			return err
		}
		if err := state.RequeueFencedVMTurn(ctx, s.SessionID, journal.FenceRequeueRequest{
			TurnSeq: s.TurnSeq, Epoch: s.Epoch, ExecutionID: s.ExecutionID,
		}); err != nil {
			if contracts.Code(err) == contracts.ErrConflict {
				return nil // another replica changed or recovered the same turn
			}
			return err
		}
	} else if s.Status != "QUEUED" {
		return fmt.Errorf("unexpected reconcile status %s", s.Status)
	}
	execution.Generation++
	execution.LaunchTurnSeq = s.TurnSeq
	execution.ExecutionID = journal.DeterministicExecutionID(s.Scope, s.SessionID, s.TurnSeq, execution.Generation)
	return c.Dispatcher.EnsureRunning(ctx, execution)
}

func (c *Coordinator) RunReconcile(ctx context.Context, interval time.Duration) {
	var sweeps sync.WaitGroup
	for _, status := range []string{"QUEUED", "ACTIVE"} {
		sweeps.Go(func() {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				if err := c.reconcileStatus(ctx, status); err != nil && ctx.Err() == nil {
					slog.Warn("run coordinator reconcile will retry", "status", status, "error", err)
				}
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		})
	}
	sweeps.Wait()
}

func (c *Coordinator) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/agents/{agent}/sessions/{action}", c.action)
	mux.HandleFunc("GET /v1/agents/{agent}/sessions/{session}", c.withSession(
		func(w http.ResponseWriter, r *http.Request, state State, session SessionSignal, conversation string) {
			c.status(w, r, state, session.SessionID, conversation)
		}))
	mux.HandleFunc("GET /v1/agents/{agent}/sessions/{session}/stream", c.withSession(
		func(w http.ResponseWriter, r *http.Request, state State, session SessionSignal, _ string) {
			c.stream(w, r, state, session)
		}))
	return transport.Workload()(mux)
}

func (c *Coordinator) stateFor(scope contracts.Scope) State {
	if c.StateFor == nil {
		return nil
	}
	return c.StateFor(scope)
}

func (c *Coordinator) withSession(next func(http.ResponseWriter, *http.Request, State, SessionSignal, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		scope, err := c.AccessPolicy.authorize(r, r.PathValue("agent"))
		if err != nil {
			transport.WriteError(w, r, err)
			return
		}
		conversation := r.PathValue("session")
		if !validID(conversation) {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "invalid conversation ID"))
			return
		}
		state := c.stateFor(scope)
		if state == nil {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrUnavailable, "coordinator State client is unavailable"))
			return
		}
		session := SessionSignal{Scope: scope, SessionID: journal.ScopedSessionID(scope, conversation)}
		next(w, r, state, session, conversation)
	}
}

// Validate the public key before hashing it into a State session ID.
func validID(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for _, char := range s {
		if char <= ' ' || char == 127 {
			return false
		}
	}
	return true
}

func (c *Coordinator) action(w http.ResponseWriter, r *http.Request) {
	action := r.PathValue("action")
	i := strings.LastIndexByte(action, ':')
	if i < 0 {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown session action"))
		return
	}
	r.SetPathValue("session", action[:i])
	switch action[i+1:] {
	case "send":
		c.withSession(c.send)(w, r)
	case "cancel":
		c.withSession(c.cancel)(w, r)
	default:
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown session action"))
	}
}

func (c *Coordinator) send(w http.ResponseWriter, r *http.Request, state State, session SessionSignal, conversation string) {
	var request journal.EnqueueRequest
	if err := transport.ReadJSON(w, r, &request, transport.DefaultMaxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	request.SessionID, request.ExecutionMode = session.SessionID, journal.ExecutionVMManaged
	// Default delivery queues behind an active turn and starts when idle.
	// An explicit "turn" retains the legacy one-active-turn conflict.
	if request.DeliveryMode == "" || request.DeliveryMode == "auto" {
		request.DeliveryMode = journal.DeliveryFollowup
	}
	receipt, err := state.EnqueueTurn(r.Context(), request)
	if err == nil && c.Dispatcher != nil && !receipt.Replayed &&
		receipt.AcceptedMode == journal.DeliveryTurn && receipt.Status == "QUEUED" {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		err := c.reconcileTurn(ctx, journal.VMSession{
			Scope: session.Scope, SessionID: session.SessionID, Harness: request.Harness,
			TurnSeq: receipt.HeadSeq, Epoch: receipt.Epoch, Status: receipt.Status,
		})
		cancel()
		if err != nil {
			slog.Warn("initial VM dispatch will be retried", "session", session.SessionID, "error", err)
		}
	}
	receipt.SessionID = conversation
	writeActionResult(w, r, struct {
		journal.EnqueueReceipt
		ConversationID string `json:"conversation_id"`
	}{receipt, conversation}, err)
}

func (c *Coordinator) cancel(w http.ResponseWriter, r *http.Request, state State, session SessionSignal, conversation string) {
	var request struct {
		TurnSeq int64 `json:"turn_seq,omitempty"`
	}
	if err := transport.ReadJSON(w, r, &request, transport.DefaultMaxBodyBytes); err != nil {
		transport.WriteError(w, r, err)
		return
	}
	status, err := state.CancelSession(r.Context(), session.SessionID, request.TurnSeq)
	writeActionResult(w, r, struct {
		journal.TurnStatus
		ConversationID string `json:"conversation_id"`
	}{status, conversation}, err)
}

func (c *Coordinator) status(w http.ResponseWriter, r *http.Request, state State, session, conversation string) {
	status, err := state.GetTurnStatus(r.Context(), session)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	resume, err := state.InspectSession(r.Context(), session)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, struct {
		journal.TurnStatus
		ConversationID string               `json:"conversation_id"`
		Unresolved     []journal.ToolIntent `json:"unresolved_intents,omitempty"`
	}{status, conversation, resume.Unresolved})
}

func writeActionResult(w http.ResponseWriter, r *http.Request, result any, err error) {
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusAccepted, result)
}
