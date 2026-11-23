package journal

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// FinishClaimedTurn atomically records a queued turn's terminal result and
// releases the claim. Callers should retry a lost acknowledgement with the
// same turn, expectedSeq and output. Prepare a large JSON output with
// EncodePayload before calling this method. The append can then be replayed
// unchanged after release, without attempting to stage a blob on a stale lease.
func (c *Client) FinishClaimedTurn(ctx context.Context, turn ClaimedTurn, expectedSeq int64, output Payload, waitingForInput bool) (AppendReceipt, error) {
	return c.finishTurn(ctx, turn, output, waitingForInput, "turn.completed", AppendRequest{ExpectedSeq: expectedSeq})
}

// FinishVMTurn commits the assistant output with any steer acknowledgement.
// State records the terminal event and returns any activated followup in Control.
// Retry a lost acknowledgement with the same arguments, including a previously
// prepared blob reference for large output.
func (c *Client) FinishVMTurn(ctx context.Context, turn ClaimedTurn, expectedSeq int64, output Payload, steerAck *SteerAck, waitingForInput bool) (AppendReceipt, error) {
	req := AppendRequest{TurnSeq: turn.InputSeq, SteerAck: steerAck, ExpectedSeq: expectedSeq}
	return c.finishTurn(ctx, turn, output, waitingForInput, "turn.output", req)
}

func (c *Client) finishTurn(ctx context.Context, turn ClaimedTurn, output Payload, waitingForInput bool, eventType string, req AppendRequest) (AppendReceipt, error) {
	if turn.SessionID == "" || turn.Epoch <= 0 || turn.InputSeq <= 0 || req.ExpectedSeq < turn.InputSeq ||
		(output.Blob == nil && !json.Valid(output.Inline)) ||
		(output.Blob != nil && output.Inline != nil) {
		return AppendReceipt{}, contracts.NewError(contracts.ErrInvalid, "invalid queued turn completion")
	}
	req.FinishTurn = "IDLE"
	if waitingForInput {
		req.FinishTurn = "WAITING_INPUT"
	}
	req.Key = eventType + ":" + strconv.FormatInt(turn.InputSeq, 10)
	req.Events = []EventInput{{ID: req.Key, Type: eventType, Role: "assistant", Codec: "json", Payload: output}}
	return c.AppendEvents(ctx, turn.SessionID, turn.Epoch, req)
}
