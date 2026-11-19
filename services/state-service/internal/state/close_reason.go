package state

import "github.com/ahp-platform/ahp/pkg/contracts"

// RunPayloadCloseReason is the key this plane records the terminal turn verdict
// under, inside the run's EXISTING payload document (runs.payload, jsonb).
//
// The word is deliberately identical to the one the execution plane journals on a
// task (internal/gateway: MetadataCloseReason), because the point of the key is
// that an auditor can compare the two ends of an A->B chain and read the same
// vocabulary in both. The literal is repeated here -- rather than importing the
// execution plane from the state plane, which would invert the layering -- for the
// same reason adapters/emil repeats the parent-task key it produces: a producer
// names the key it writes.
const RunPayloadCloseReason = "close_reason"

// runEndReasonClause is the policy sentence every door of State says when a caller tries
// to end a run without saying why. One constant, so a client that talks to Release and
// to PutBatch cannot meet two wordings of one rule.
const runEndReasonClause = "may only end a run with a stated reason (close_reason and/or failure_cause)"

// runEndReasonPolicy is the single decision about whether an ending run owes a stated
// reason. Both doors ask it - Release and the run_status half of PutBatch - and only
// disagree about WHICH terminal statuses their contracts cover, a difference written at
// the call site rather than hidden in a message. The refusal is INVALID_ARGUMENT with the
// same clause; `door` names the entry point so an operator reading one log line knows
// which gate answered.
//
// PDF rev3.2 §2.5 makes an ended run owe its reason durably; DEF-BATCH-1 wrote the rule
// into the batch door, DEF-RELEASE-REASON-1 extends the same sentence to Release for the
// statuses this contract covers (failed, canceled). Done is deliberately not one of them
// on the Release door: the verdict that finished the run is recorded by the transcript,
// and 40+ existing honest callers rely on releasing a completed run without restating it.
func runEndReasonPolicy(door, status, closeReason, cause string, covered ...string) error {
	if closeReason != "" || cause != "" {
		return nil
	}
	for _, s := range covered {
		if status == s {
			return contracts.NewError(contracts.ErrInvalid, door+" "+runEndReasonClause)
		}
	}
	return nil
}

// RunPayloadFailureCause is the key under which a caller that is NOT the execution
// plane records why IT ended a run, inside the same existing payload document.
//
// It exists because a plane-level refusal (a preflight check that failed before any
// model call) knows a cause the execution vocabulary has no word for, and the
// dictionary key above must keep carrying exactly that vocabulary so the A->B audit
// comparison stays intact. One payload, two honest keys: close_reason says which
// execution word closed the run, failure_cause quotes the refusing plane's own
// sentence verbatim. A run that records neither keeps its payload byte for byte.
const RunPayloadFailureCause = "failure_cause"
