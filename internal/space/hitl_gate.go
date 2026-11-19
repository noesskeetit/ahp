package space

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// hitl_gate.go — reading a HITL gate out of the only place it has ever existed.
//
// Space keeps no gate table, deliberately: SubmitHITL records the gate as the thread-inbox event key
// "hitl:<id>" (kind hitl_request) and parks the run on it, and RespondHITL records the decision as key
// "hitl_response:<id>" (kind hitl_response). Those two events ARE the record, so a gate's state is read
// from them and nowhere else - the rule turn_history.go already applies to a turn's status and to its
// failure cause, applied to an approval: ask the durable plane instead of a memory a restart took away.
// Without this read a gate exists only in the answer of the one call that created it, so an approval
// card that reloads after a runner restart cannot ask whether it is still waiting or what was decided,
// while the thread's inbox says both.
//
// Two limits belong to the published State contract, not to this file, so they are stated instead of
// being worked around:
//   - ReadInbox returns UNACKED events only (internal/state/events.go). A gate whose request event a
//     consumer already acknowledged is therefore not readable here, and this read says what it found
//     rather than claiming the gate never existed.
//   - an inbox event carries no created_at, so RespondedAt stays empty. Taking an answer instant from
//     this process's clock at read time would invent data and move on every refresh - the argument
//     turn_history.go makes about UpdatedAt, applied to an approval.

const (
	hitlRequestKeyPrefix  = "hitl:"
	hitlResponseKeyPrefix = "hitl_response:"

	// hitlWakeKeySuffix names the event that RE-SIGNALS a gate's answer to a run whose park missed
	// it. It is a distinct key on purpose: the gate's own answer keeps its key and stays the single
	// decision event a reader matches, while this one exists only to be inserted - which is the one
	// thing State's published wake rule reacts to.
	hitlWakeKeySuffix = ":wake"
)

// hitlGateRecord is what one thread's durable inbox says about one gate.
type hitlGateRecord struct {
	requested bool
	answered  bool
	gate      HITLRequest // the gate as its own request event states it
	approved  bool
	response  json.RawMessage
}

// sameDecision reports whether an incoming answer IS the decision this gate already holds. The
// comparison covers the verdict and the response bytes, because those are the bytes State dedups on:
// a second answer with the same verdict but different commentary is a different payload under one key,
// which State refuses, so it cannot honestly be reported here as a decision that was recorded.
func (rec *hitlGateRecord) sameDecision(req HITLRespondRequest) bool {
	return rec.approved == req.Approved && jsonEquivalent(rec.response, req.Response)
}

// decisionStatus is the gate's lifecycle word for the decision its record holds. A record with no
// decision has no status here: "pending" is stated by the reader that checked for one, not by this.
func (rec *hitlGateRecord) decisionStatus() HITLStatus {
	if rec.approved {
		return HITLApproved
	}
	return HITLRejected
}

// jsonEquivalent compares two JSON values by meaning, not by the bytes a client happened to send.
func jsonEquivalent(a, b json.RawMessage) bool {
	return bytes.Equal(compactJSON(a), compactJSON(b))
}

func compactJSON(raw json.RawMessage) []byte {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil // no commentary and absent commentary are the same answer
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return bytes.TrimSpace(raw) // unreadable: compare what arrived, byte for byte
	}
	return buf.Bytes()
}

func hitlDecisionWord(approved bool) string {
	if approved {
		return "approved"
	}
	return "rejected"
}

// errHITLInboxScanCapped makes a truncated inbox scan an ERROR rather than an empty answer: a thread
// longer than the bound proves nothing about what its inbox holds, and "this gate was never answered"
// built on a truncated read is a claim about durable data this read never verified.
var errHITLInboxScanCapped = errors.New("space: gate scan reached its bound before the thread's inbox was drained")

// readHITLGate pages the thread's inbox (bounded, under the CALLER's own scope-bound task token) and
// collects what it says about one gate. Matching is on the event KEY, which is the writer's own
// identity for the gate - not on the payload, and not on the kind, which every HITL answer shares and
// which is therefore no evidence about WHICH gate an answer belongs to.
func (c *Coordinator) readHITLGate(ctx context.Context, token, threadID, hitlID string) (*hitlGateRecord, error) {
	const page = 200
	const maxScan = 20000
	rec := &hitlGateRecord{}
	requestKey, responseKey := hitlRequestKeyPrefix+hitlID, hitlResponseKeyPrefix+hitlID
	scanned := 0
	after := int64(0)
	for {
		evs, err := c.state.ReadInbox(ctx, token, threadID, after, page)
		if err != nil {
			return nil, err // an unreadable inbox proves nothing about this gate, either way
		}
		if len(evs) == 0 {
			return rec, nil
		}
		for _, ev := range evs {
			scanned++
			after = ev.Sequence
			switch ev.Key {
			case requestKey:
				rec.requested = true
				if err := json.Unmarshal(ev.Payload, &rec.gate); err != nil {
					// Durable data in a shape Space cannot interpret is this service's fault, not the
					// caller's: the detail is logged and the caller gets the honest 500 the transport
					// gives an unclassified error. There is no contract code for "State's own record is
					// unreadable", and inventing one would dress a server fault up as a client error.
					slog.Warn("space: HITL gate request event is unreadable",
						"hitl_id", hitlID, "thread_id", threadID, "err", err.Error())
					return nil, fmt.Errorf("space: HITL gate %s request event does not carry a readable gate", hitlID)
				}
				rec.gate.ID = hitlID
			case responseKey:
				rec.answered = true
				var decided struct {
					Approved bool            `json:"approved"`
					Response json.RawMessage `json:"response,omitempty"`
				}
				if err := json.Unmarshal(ev.Payload, &decided); err != nil {
					slog.Warn("space: HITL gate response event is unreadable",
						"hitl_id", hitlID, "thread_id", threadID, "err", err.Error())
					return nil, fmt.Errorf("space: HITL gate %s response event does not carry a readable decision", hitlID)
				}
				rec.approved = decided.Approved
				rec.response = decided.Response
			}
		}
		if rec.requested && rec.answered {
			return rec, nil
		}
		if len(evs) < page {
			return rec, nil // drained the thread's unacked inbox
		}
		if scanned >= maxScan {
			return nil, errHITLInboxScanCapped
		}
	}
}

// GetHITL reads one gate as the thread's durable record states it. A gate nobody submitted is NOT_FOUND
// rather than an empty invented one; a gate whose request event has been acknowledged by a consumer
// reports the decision alone when the response is still readable, and NOT_FOUND with what was actually
// checked when neither is.
func (c *Coordinator) GetHITL(ctx context.Context, token, threadID, hitlID string) (*HITLRequest, error) {
	if strings.TrimSpace(threadID) == "" || strings.TrimSpace(hitlID) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "space: thread_id and hitl_id required")
	}
	rec, err := c.readHITLGate(ctx, token, threadID, hitlID)
	if err != nil {
		return nil, err
	}
	if !rec.requested && !rec.answered {
		return nil, contracts.NewError(contracts.ErrNotFound,
			"space: this thread's durable inbox carries no HITL gate "+hitlID+
				" (an acknowledged event is not readable through the published ReadInbox contract, so this states "+
				"what was found and not that the gate never existed)")
	}
	gate := rec.gate
	gate.ID = hitlID
	if gate.TurnID != "" {
		run, err := c.state.GetRun(ctx, token, gate.TurnID)
		if err != nil {
			return nil, err
		}
		if run.Status == "canceled" {
			gate.Status = HITLCanceled
			return &gate, nil
		}
	}
	if gate.Status == "" {
		gate.Status = HITLPending
	}
	if rec.answered {
		gate.Status = rec.decisionStatus()
		gate.Response = rec.response
	}
	return &gate, nil
}

// checkHITLAnswer is the gate's answer-time identity check, run BEFORE anything is written.
//
// It refuses the one thing the durable record cannot hold: a second, different decision for a gate that
// already has one. State keys the response event by the gate and refuses a different payload under that
// key, but a storage-layer verdict is not an answer about a gate: on the wire Space relayed State's
// KEY_REUSE_MISMATCH flattened to INVALID_ARGUMENT, and against a plane that does not check the digest
// at all it answered {"ok":true} to a decision the record contradicts. Neither shape tells the caller
// what the gate decided, which is the question an approval card is asking. A repeat of the SAME decision
// is not a re-decision: one human choice arriving twice (a retried click, a retried request) stays
// accepted, writes nothing, and produces no second wake - State's own dedup skips its wake on a repeat,
// so writing again would buy nothing an early success does not already mean.
//
// An unreadable inbox is NOT treated as "unanswered": refusing a human decision because a read failed
// would strand a run whose gate is fine, so the failure is said out loud and the answer proceeds as it
// always did. This check can therefore only ever refuse a decision the record positively contradicts.
func (c *Coordinator) checkHITLAnswer(ctx context.Context, token, threadID string, req HITLRespondRequest) error {
	rec, err := c.readHITLGate(ctx, token, threadID, req.HITLID)
	if err != nil {
		slog.Warn("space: HITL answer could not be checked against the durable gate record",
			"hitl_id", req.HITLID, "thread_id", threadID, "err", err.Error())
		return nil
	}
	if rec.requested && rec.gate.TurnID != "" {
		run, err := c.state.GetRun(ctx, token, rec.gate.TurnID)
		if err != nil {
			return err
		}
		if run.Status == "canceled" {
			return contracts.NewError(contracts.ErrConflict, "space: canceled turn cannot accept a HITL response")
		}
	}
	if !rec.answered {
		return nil
	}
	if rec.sameDecision(req) {
		return nil
	}
	return contracts.NewError(contracts.ErrConflict,
		"space: HITL gate "+req.HITLID+" was already answered ("+hitlDecisionWord(rec.approved)+
			") and this answer is not that decision: a gate keeps the first decision it was given")
}

// releaseRunIfGateAnswered closes the window SubmitHITL cannot close by ordering itself.
//
// SubmitHITL records the gate first and parks the run second, and State's wake happens only while an
// inbox event is being INSERTED (internal/state/events.go), while Suspend parks unconditionally
// (internal/state/store.go). An answer that lands between those two calls is therefore inserted while
// the run is still running - it wakes nothing - and the park that follows waits for a signal that has
// already been and gone. With no deadline on the gate the run stays parked forever with its answer
// unacknowledged one call away, and no error was ever returned to anyone.
//
// Reordering to park-first does not fix this, it only moves the permanent stall somewhere worse: if
// the gate event then fails to be written, the run is parked on a gate nobody was ever told about, and
// Space has no way back - there is no unsuspend operation, and Release needs the lease that Suspend
// has already cleared. So the durable record is written first (nothing is ever lost) and a missed wake
// is repaired FROM the record.
//
// The repair re-signals a decision and invents nothing. It acts only on positive evidence - a decision
// this thread's own inbox holds, read under the caller's token - and never on an absence, which is the
// asymmetry that keeps it inside what Space is allowed to know (unlike "no such gate", which the
// published ReadInbox cannot prove: acknowledged events are invisible to it). The wake it uses is the
// platform's published rule, the same one the real answer relies on: inserting an event of the kind a
// waiting run waits for releases it. Under a distinct key, so the gate's answer event stays unique and
// the reader that matches that key still finds exactly one decision - this event is a doorbell, not a
// verdict, and says so in its payload.
//
// Two residuals are stated rather than hidden. If a consumer acknowledged the answer inside the window,
// this read cannot see it and the run stays parked: that is the acked-inclusive inbox read asked for on
// the forum, not something Space can substitute. And if the caller's token may not read the thread's
// inbox, the caller of SubmitHITL is told nothing false - the gate and the park are durable and stay as
// they are, and the missed repair is logged.
func (c *Coordinator) releaseRunIfGateAnswered(ctx context.Context, token, runID, threadID, hitlID string) (*hitlGateRecord, error) {
	rec, err := c.readHITLGate(ctx, token, threadID, hitlID)
	if err != nil {
		return nil, err
	}
	if !rec.answered {
		return nil, nil // nothing to repair, and nothing here may be inferred from that alone
	}
	run, err := c.state.GetRun(ctx, token, runID)
	if err != nil {
		return rec, err
	}
	if run.Status != runStatusWaiting || run.WaitFor != hitlWaitFor {
		// The run is not parked on this gate: either the wake arrived after all or something already
		// released it. Either way a second signal would be a wake this thread never asked for.
		return rec, nil
	}
	payload, err := json.Marshal(map[string]any{
		"hitl_id":     hitlID,
		"approved":    rec.approved,
		"response":    rec.response,
		"resignal_of": hitlResponseKeyPrefix + hitlID,
	})
	if err != nil {
		return rec, err
	}
	if _, err := c.state.Enqueue(ctx, token, threadID, hitlResponseKeyPrefix+hitlID+hitlWakeKeySuffix, hitlWaitFor, payload); err != nil {
		return rec, err
	}
	return rec, nil
}
