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
	ListRecoverableVMPage(context.Context, string, *journal.VMReconcileCursor) (journal.VMReconcilePage, error)
	RequeueFencedVMTurn(context.Context, string, journal.FenceRequeueRequest) error
}

type Execution struct {
	Scope       contracts.Scope `json:"scope"`
	SessionID   string          `json:"session_id"`
	Harness     string          `json:"harness_type,omitempty"`
	TurnSeq     int64           `json:"turn_seq"`
	Generation  int64           `json:"generation"`
	ExecutionID string          `json:"execution_id"`
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
	Scope      contracts.Scope
	State      State
	Dispatcher VMDispatcher
	Signals    SignalBus
}

func (c *Coordinator) Reconcile(ctx context.Context) error {
	return errors.Join(c.reconcileStatus(ctx, "QUEUED"), c.reconcileStatus(ctx, "ACTIVE"))
}

func (c *Coordinator) reconcileStatus(ctx context.Context, status string) error {
	var cursor *journal.VMReconcileCursor
	var firstErr error
	var failures int
	for ctx.Err() == nil {
		page, err := c.State.ListRecoverableVMPage(ctx, status, cursor)
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
	if s.Scope != c.Scope {
		return contracts.NewError(contracts.ErrForbidden, "VM reconcile returned a different scope")
	}
	execution := Execution{Scope: s.Scope, SessionID: s.SessionID, TurnSeq: s.TurnSeq,
		Harness: s.Harness, Generation: s.Epoch, ExecutionID: s.ExecutionID}
	if s.Status == "ACTIVE" {
		if s.ExecutionID == "" {
			return errors.New("active VM has no recorded execution owner")
		}
		// The physical owner survives a warm followup. Recomputing it from the
		// current turn would address a different VM.
		if err := c.Dispatcher.Fence(ctx, execution); err != nil {
			return err
		}
		if err := c.State.RequeueFencedVMTurn(ctx, s.SessionID, journal.FenceRequeueRequest{
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
	mux.HandleFunc("GET /v1/agents/{agent}/sessions/{session}", c.status)
	mux.HandleFunc("GET /v1/agents/{agent}/sessions/{session}/stream", func(w http.ResponseWriter, r *http.Request) {
		c.stream(w, r, r.PathValue("session"))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		caller, ok := transport.FromContext(r.Context())
		if !ok || caller.Scope != c.Scope {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "coordinator scope does not match caller"))
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func (c *Coordinator) action(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("agent") != c.Scope.Deployment {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "agent is outside coordinator scope"))
		return
	}
	session, action, ok := strings.Cut(r.PathValue("action"), ":")
	if !ok || session == "" {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown session action"))
		return
	}
	switch action {
	case "send":
		var request journal.EnqueueRequest
		if err := transport.ReadJSON(w, r, &request, transport.DefaultMaxBodyBytes); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		request.SessionID, request.ExecutionMode = session, journal.ExecutionVMManaged
		// Default delivery queues behind an active turn and starts when idle.
		// An explicit "turn" retains the legacy one-active-turn conflict.
		if request.DeliveryMode == "" || request.DeliveryMode == "auto" {
			request.DeliveryMode = journal.DeliveryFollowup
		}
		receipt, err := c.State.EnqueueTurn(r.Context(), request)
		if err == nil && c.Dispatcher != nil && !receipt.Replayed &&
			receipt.AcceptedMode == journal.DeliveryTurn && receipt.Status == "QUEUED" {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			err := c.reconcileTurn(ctx, journal.VMSession{
				Scope: c.Scope, SessionID: session, Harness: request.Harness,
				TurnSeq: receipt.HeadSeq, Epoch: receipt.Epoch, Status: receipt.Status,
			})
			cancel()
			if err != nil {
				slog.Warn("initial VM dispatch will be retried", "session", session, "error", err)
			}
		}
		writeActionResult(w, r, receipt, err)
	case "cancel":
		var request struct {
			TurnSeq int64 `json:"turn_seq,omitempty"`
		}
		if err := transport.ReadJSON(w, r, &request, transport.DefaultMaxBodyBytes); err != nil {
			transport.WriteError(w, r, err)
			return
		}
		status, err := c.State.CancelSession(r.Context(), session, request.TurnSeq)
		writeActionResult(w, r, status, err)
	default:
		transport.WriteError(w, r, contracts.NewError(contracts.ErrNotFound, "unknown session action"))
	}
}

func (c *Coordinator) status(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("agent") != c.Scope.Deployment {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "agent is outside coordinator scope"))
		return
	}
	status, err := c.State.GetTurnStatus(r.Context(), r.PathValue("session"))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	state, err := c.State.InspectSession(r.Context(), r.PathValue("session"))
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, struct {
		journal.TurnStatus
		Unresolved []journal.ToolIntent `json:"unresolved_intents,omitempty"`
	}{status, state.Unresolved})
}

func writeActionResult(w http.ResponseWriter, r *http.Request, result any, err error) {
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusAccepted, result)
}
