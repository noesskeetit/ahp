package space

import (
	"encoding/json"
	"strings"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// CloseReasonCanceled is the one terminal fact the execution plane's dictionary
// has no word for: a run the user stopped never produced a turn verdict at all, so
// classifying it as model_finished would be false and classifying it as error would
// blame the model for a user decision.
//
// The spelling is taken from this plane's OWN run-status enum (state.RunCanceled),
// so no synonym for an existing verdict is introduced -- the value set of
// close_reason grows by one word on the run side only, and that word is already the
// platform's name for this exact fact. Flagged for the director rather than decided
// silently, because it is the single place where the two ends of an A->B chain do not
// share a value: a task on the execution plane is never "canceled" by close_reason,
// it is canceled by its lease and its subtree.
const CloseReasonCanceled = "canceled"

// CloseReasonCancelRequested names the terminal fact "this run ended because a cancel
// was journaled against it and the lease holder honoured it".
//
// It is distinct from CloseReasonCanceled on purpose. Canceled is written by the party
// that owns the lease and the decision (CancelTurn closing a parked run, or a cancel this
// same process issued). cancel_requested is written by the HOLDER of a run that somebody
// else asked to stop: the status is still canceled - the user's decision is what ended the
// run - but the reason says which mechanism ended it, so an operator reading a stuck-then-
// stopped run can tell a direct cancel from a cancel that had to wait for the holder's next
// checkpoint. Neither value claims anything about the effect of the work already recorded:
// the accepted effects of the run stay in the transcript unchanged.
const CloseReasonCancelRequested = "cancel_requested"

// runCloseReason names why this run's turn ended, in the vocabulary the execution
// plane already journals (gateway.MetadataCloseReason), so a verifier comparing the
// two ends of a delegation chain compares the same word against the same word.
//
// The classification is not reimplemented here: gateway.ClassifyCloseReason is the
// single classifier this product has, and it is what produced the value a reader
// sees on the child task. Re-deriving it in this package would create a second
// dictionary with the same names and slightly different rules -- the exact failure
// mode the requirement forbids ("один словарь значений, без новых синонимов").
//
// status is the verdict the coordinator already committed to; a non-terminal status
// (the fenced "unknown" path) states nothing at all, because a run whose verdict is
// genuinely unknown must not be handed a confident word.
func runCloseReason(status string, coreErr, durableErr error, stepCause string,
	sawStepError bool, finalText string, modelTurns, budget int) string {
	switch status {
	case CloseReasonCanceled:
		return CloseReasonCanceled
	case "done", "failed":
	default:
		return ""
	}
	errText := stepCause
	if durableErr != nil {
		// A durable-stream hole is the cause that ended this run even when Core
		// returned cleanly: the run cannot be completed because its transcript
		// cannot be written.
		if errText != "" {
			errText += " "
		}
		errText += durableErr.Error()
	}
	joined := coreErr
	if joined == nil {
		joined = durableErr
	}
	return gateway.ClassifyCloseReason(joined, errText, sawStepError, finalText, modelTurns, budget)
}

// stepContentText projects one step frame's content onto the text a reader would
// call the model's answer. The classifier only needs to know whether the model said
// anything at all, so an undecodable frame degrades to its raw bytes (non-empty)
// rather than to the empty string, which would be read as "the model said nothing".
func stepContentText(content json.RawMessage) string {
	if len(content) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(content, &s); err == nil {
		return s
	}
	var frame map[string]any
	if err := json.Unmarshal(content, &frame); err == nil {
		for _, key := range []string{"text", "content", "answer", "message"} {
			if v, ok := frame[key].(string); ok && strings.TrimSpace(v) != "" {
				return v
			}
		}
	}
	return string(content)
}
