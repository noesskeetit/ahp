package journal

import (
	"context"
	"net/url"
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
