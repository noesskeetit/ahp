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
	if turn.SessionID == "" || turn.Epoch <= 0 || turn.InputSeq <= 0 || expectedSeq < turn.InputSeq ||
		(output.Blob == nil && !json.Valid(output.Inline)) ||
		(output.Blob != nil && output.Inline != nil) {
		return AppendReceipt{}, contracts.NewError(contracts.ErrInvalid, "invalid queued turn completion")
	}
	status := "IDLE"
	if waitingForInput {
		status = "WAITING_INPUT"
	}
	key := "turn.completed:" + strconv.FormatInt(turn.InputSeq, 10)
	req := AppendRequest{
		Key: key, ExpectedSeq: expectedSeq, FinishTurn: status,
		Events: []EventInput{{ID: key, Type: "turn.completed", Role: "assistant", Codec: "json", Payload: output}},
	}
	return c.AppendEvents(ctx, turn.SessionID, turn.Epoch, req)
}
