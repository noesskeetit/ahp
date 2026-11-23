package journal

import (
	"context"
	"net/url"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func (c *Client) ClaimVMTurn(ctx context.Context, session string, req VMTurnClaimRequest) (ClaimedTurn, error) {
	var turn ClaimedTurn
	err := c.http.Call(ctx, sessionPath(session)+"/vm:claim", req, &turn, true)
	return turn, err
}

func (c *Client) RequeueFencedVMTurn(ctx context.Context, session string, req FenceRequeueRequest) error {
	return c.http.Call(ctx, sessionPath(session)+"/vm:requeue-fenced", req, nil, true)
}

func (c *Client) CancelSession(ctx context.Context, session string, turnSeq int64) (TurnStatus, error) {
	var status TurnStatus
	req := struct {
		TurnSeq int64 `json:"turn_seq,omitempty"`
	}{turnSeq}
	err := c.http.Call(ctx, sessionPath(session)+"/vm:cancel", req, &status, true)
	return status, err
}

func (c *Client) ListRecoverableVMPage(ctx context.Context, status string, after *journal.VMReconcileCursor) (journal.VMReconcilePage, error) {
	var page journal.VMReconcilePage
	query := url.Values{"status": {status}}
	if after != nil {
		query.Set("after", after.At.Format(time.RFC3339Nano))
		query.Set("after_session", after.SessionID)
	}
	err := c.get(ctx, "/internal/v1/state/vm-reconcile", query, &page)
	return page, err
}

// VMLease tracks the current turn and signed lease as responses arrive. Use
// the same tracker for heartbeat and finish responses from one execution.
type VMLease struct {
	mu   sync.Mutex
	turn ClaimedTurn
}

func NewVMLease(turn ClaimedTurn) *VMLease {
	return &VMLease{turn: turn}
}

func (l *VMLease) Current() ClaimedTurn {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.turn
}

// ApplyHeartbeat returns only control for the current turn. Proofs from a
// later deadline may also renew a lease while the finish response is in flight.
func (l *VMLease) ApplyHeartbeat(renewed SessionLease) *TurnControlDelta {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.turn.InputSeq == 0 || l.turn.Worker != renewed.Worker || l.turn.Epoch != renewed.Epoch ||
		renewed.ExpiresAt.Before(l.turn.ExpiresAt) {
		return nil
	}
	if renewed.ExpiresAt.After(l.turn.ExpiresAt) || renewed.ToolLeaseProof != "" {
		l.turn.Lease = renewed.Lease
		l.turn.ToolLeaseProof = renewed.ToolLeaseProof
	}
	if renewed.Control != nil && renewed.Control.TurnSeq == l.turn.InputSeq {
		return renewed.Control
	}
	return nil
}

// ApplyFinish uses a successful finish receipt for the specified turn. It
// returns true when the same execution owns a promoted followup.
func (l *VMLease) ApplyFinish(finishedTurnSeq int64, receipt AppendReceipt) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if finishedTurnSeq == 0 || finishedTurnSeq != l.turn.InputSeq {
		return false
	}
	if receipt.Control == nil || receipt.Control.NextTurn == nil {
		l.turn = ClaimedTurn{}
		return false
	}
	next := ClaimedTurn{TurnLease: *receipt.Control.NextTurn, ToolLeaseProof: receipt.Control.ToolLeaseProof}
	if next.SessionID != l.turn.SessionID || next.Worker != l.turn.Worker || next.Epoch != l.turn.Epoch ||
		next.InputSeq <= finishedTurnSeq || receipt.Control.TurnSeq != next.InputSeq {
		return false
	}
	if next.ExpiresAt.Before(l.turn.ExpiresAt) || (next.ExpiresAt.Equal(l.turn.ExpiresAt) && next.ToolLeaseProof == "") {
		next.Lease, next.ToolLeaseProof = l.turn.Lease, l.turn.ToolLeaseProof
	}
	l.turn = next
	return true
}

// RunVMHeartbeatWithControl updates the caller's VM lease tracker on every
// heartbeat, including responses that have no control delta.
func (c *Client) RunVMHeartbeatWithControl(ctx context.Context, lease *VMLease, ttl time.Duration, onControl func(TurnControlDelta)) (context.Context, context.CancelFunc) {
	current := lease.Current()
	return c.runHeartbeat(ctx, current.SessionID, current.Epoch, ttl, func(renewed SessionLease) {
		if control := lease.ApplyHeartbeat(renewed); control != nil && onControl != nil {
			onControl(*control)
		}
	})
}
