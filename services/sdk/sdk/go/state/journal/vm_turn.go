package journal

import "context"

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

func (c *Client) ListRecoverableVMSessions(ctx context.Context) ([]VMSession, error) {
	var sessions []VMSession
	err := c.get(ctx, "/internal/v1/state/vm-reconcile", nil, &sessions)
	return sessions, err
}
