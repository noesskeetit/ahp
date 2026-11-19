package harness

import (
	"bytes"
	"encoding/json"
	"strings"
)

// DEF-PROF-1 accounting. The transport plane (did the HTTP call succeed) and
// the terminal plane (how did the platform generation actually end) are counted
// apart and named apart, because collapsing them let a phase with a FAILED
// generation and three never-resolved streams report "errors=0" (evidence
// profiling-llm-20261109T161709Z-y9f8mc.json: requests=12, errors=0,
// COMPLETED=8, FAILED=1, "streamed"=3).
//
// Invariants this file guarantees:
//   - logical_errors = requests - terminal_completed, and logical_breakdown
//     sums exactly to logical_errors;
//   - "streamed" is never terminal: only the durable generation row read after
//     the stream closed may carry a request into the completed count;
//   - verdict is FAIL whenever terminal_failed>0, terminal_blocked>0 or
//     logical_errors>0 -- an honest zero is the only way to a PASS.

// IsTerminalGenerationStatus reports whether a platform generation state is
// final. PENDING/RUNNING/STREAMING are in-flight: reading one of these as a
// phase outcome would silently retire a request whose generation can still
// end badly.
func IsTerminalGenerationStatus(status string) bool {
	switch status {
	case "COMPLETED", "FAILED", "CANCELED", "BLOCKED":
		return true
	}
	return false
}

// StreamTerminalStatus extracts the durable final status from a consumed
// StreamModel response body. The gateway's terminal frame carries the
// persisted generation row -- that row, not the fact that bytes streamed, is
// the terminal outcome of a stream request (DEF-PROF-1: "streamed" is not a
// status). An error frame is a FAILED generation; a stream that closed
// without either frame resolved nothing and returns "".
func StreamTerminalStatus(stream []byte) string {
	status := ""
	for _, line := range bytes.Split(stream, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var frame struct {
			Type       string `json:"type"`
			Generation struct {
				Status string `json:"status"`
			} `json:"generation"`
		}
		if json.Unmarshal(line, &frame) != nil {
			continue
		}
		switch frame.Type {
		case "terminal":
			status = frame.Generation.Status
		case "error":
			status = "FAILED"
		}
	}
	return status
}

// StreamOutcome extracts the durable terminal row from a consumed StreamModel
// response: status, the error the platform persisted with it and the length of
// the output text. Like StreamTerminalStatus, "bytes arrived" is never an
// outcome; only the row in the terminal frame is.
func StreamOutcome(stream []byte) Outcome {
	oc := Outcome{}
	for _, line := range bytes.Split(stream, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var frame struct {
			Type       string `json:"type"`
			Error      string `json:"message"`
			Generation struct {
				Status string `json:"status"`
				Error  string `json:"error"`
				Output string `json:"output"`
			} `json:"generation"`
		}
		if json.Unmarshal(line, &frame) != nil {
			continue
		}
		switch frame.Type {
		case "terminal":
			oc.Status = frame.Generation.Status
			oc.Error = frame.Generation.Error
			n := len(frame.Generation.Output)
			oc.OutputLen = &n
		case "error":
			oc.Status = "FAILED"
			if oc.Error == "" {
				oc.Error = frame.Error
			}
		}
	}
	return oc
}

// PhaseAccounting is the per-phase verdict block embedded in the harness
// phase reports: transport plane and terminal plane, counted apart.
type PhaseAccounting struct {
	// FailureCauses keys durable failure causes by a stable token (e.g.
	// "provider_empty_completion"), so a FAILED count is never an autopsy
	// waiting to happen (DEF-PROF-2). It stays separate from
	// logical_breakdown: causes explain failures, the breakdown still sums to
	// logical_errors.
	FailureCauses map[string]int `json:"failure_causes,omitempty"`
	// CompletedEmptyOutput counts generations the platform called COMPLETED
	// while carrying no text: counted, never folded into "worked".
	CompletedEmptyOutput int            `json:"completed_empty_output"`
	Requests             int            `json:"requests"`
	TransportErrors      int            `json:"transport_errors"`
	TerminalCompleted    int            `json:"terminal_completed"`
	TerminalFailed       int            `json:"terminal_failed"`
	TerminalBlocked      int            `json:"terminal_blocked"`
	TerminalUnknown      int            `json:"terminal_unknown"`
	LogicalErrors        int            `json:"logical_errors"`
	LogicalBreakdown     map[string]int `json:"logical_breakdown"`
	Verdict              string         `json:"verdict"`
	VerdictReasons       []string       `json:"verdict_reasons,omitempty"`
}

// AccountPhase folds one phase's transport result and per-request durable
// terminal statuses into the verdict block. terminalStatuses must hold the
// durable final status of every request whose generation row was read --
// including stream requests, whose status must come from the terminal frame or
// a subsequent durable read, never from a local "streamed" marker. Requests
// with no entry there are accounted as no_terminal_status (unknown): an
// outcome nobody read is not a success.
func AccountPhaseOutcomes(requests, transportErrors int, outcomes []Outcome) PhaseAccounting {
	a := PhaseAccounting{Requests: requests, TransportErrors: transportErrors, LogicalBreakdown: map[string]int{}, FailureCauses: map[string]int{}}
	add := func(reason string) { a.LogicalBreakdown[reason]++ }
	for _, oc := range outcomes {
		status := oc.Status
		cause := CauseToken(oc.Error)
		switch status {
		case "COMPLETED":
			a.TerminalCompleted++
			// "done" with no text is not a working answer; it is reported,
			// never counted toward anything that reads as success.
			if oc.OutputLen != nil && *oc.OutputLen == 0 {
				a.CompletedEmptyOutput++
			}
		case "FAILED":
			a.TerminalFailed++
			if cause != "" {
				a.FailureCauses[cause]++
				add("generation_failed/" + cause)
			} else {
				// No fabricated cause: a failure whose durable error was never
				// read stays explicitly cause-less.
				a.FailureCauses[causeUnavailable]++
				add("generation_failed")
			}
		case "BLOCKED":
			a.TerminalBlocked++
			add("generation_blocked")
		case "CANCELED":
			a.TerminalUnknown++
			add("canceled")
		case "", "streamed":
			// "streamed" is transport progress, not a durable outcome.
			a.TerminalUnknown++
			add("no_terminal_status")
		case "PENDING", "RUNNING", "STREAMING":
			a.TerminalUnknown++
			add("non_terminal_status:" + status)
		default:
			a.TerminalUnknown++
			add("unrecognized_status:" + status)
		}
	}
	switch {
	case len(outcomes) > requests:
		// Bookkeeping mismatch: more statuses than requests means the harness
		// itself lost track; that is a FAIL, never a silent completion.
		a.LogicalBreakdown["harness_bookkeeping_mismatch"] += len(outcomes) - requests
		a.TerminalUnknown += len(outcomes) - requests
	case len(outcomes) < requests:
		missing := requests - len(outcomes)
		a.TerminalUnknown += missing
		a.LogicalBreakdown["no_terminal_status"] += missing
	}
	a.LogicalErrors = a.Requests - a.TerminalCompleted
	if a.LogicalErrors < 0 {
		a.LogicalErrors = 0
	}
	a.Verdict = "PASS"
	if a.TerminalFailed > 0 {
		a.VerdictReasons = append(a.VerdictReasons, "terminal_failed>0")
	}
	if a.TerminalBlocked > 0 {
		a.VerdictReasons = append(a.VerdictReasons, "terminal_blocked>0")
	}
	if a.LogicalErrors > 0 {
		a.VerdictReasons = append(a.VerdictReasons, "logical_errors>0")
	}
	if a.CompletedEmptyOutput > 0 {
		a.VerdictReasons = append(a.VerdictReasons, "completed_empty_output>0")
	}
	if len(a.VerdictReasons) > 0 {
		a.Verdict = "FAIL"
	}
	return a
}

// causeUnavailable is the honest placeholder for a failure whose durable error
// string never reached the harness. It must never be replaced by a guessed
// cause: naming a cause that was not observed is exactly the fabrication this
// accounting exists to prevent.
const causeUnavailable = "cause_unavailable"

// Outcome is one request's durable terminal outcome together with the cause the
// platform persisted next to it. Bare status strings cannot express WHY a
// generation failed, and the platform does store the cause (ahp_model
// .generations.error, e.g. "provider_empty_completion: the provider ended the
// turn with no text and no tool calls (finish=length)"); dropping it turned
// every failed phase into a manual database autopsy (DEF-PROF-2, RED
// TestAccountPhaseCannotNameDurableCause, evidence
// dot-e2e-runtime/20261109/cause_attribution_red_test.go.txt).
type Outcome struct {
	Status string `json:"status"`
	// Error is the durable error text as the platform recorded it.
	Error string `json:"error,omitempty"`
	// OutputLen is the length of the durable output text; nil means the output
	// was never read (never assume zero).
	OutputLen *int `json:"output_len,omitempty"`
}

// OutcomesFromStatuses adapts callers that only have bare statuses. Causes
// stay unknown -- which is recorded as cause_unavailable, not invented.
func OutcomesFromStatuses(statuses []string) []Outcome {
	out := make([]Outcome, 0, len(statuses))
	for _, st := range statuses {
		out = append(out, Outcome{Status: st})
	}
	return out
}

// AccountPhase is the status-only form of the phase verdict. Prefer
// AccountPhaseOutcomes: without causes it can say how many generations failed
// but never why.
func AccountPhase(requests, transportErrors int, terminalStatuses []string) PhaseAccounting {
	return AccountPhaseOutcomes(requests, transportErrors, OutcomesFromStatuses(terminalStatuses))
}

// CauseToken reduces a durable error string to a stable, groupable token
// ("provider_empty_completion: the provider ended..." -> "provider_empty_completion").
// The token is derived from observed text only.
func CauseToken(errText string) string {
	s := strings.TrimSpace(errText)
	if s == "" {
		return ""
	}
	if i := strings.Index(s, ":"); i >= 0 {
		s = s[:i]
	}
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(s)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			b.WriteRune(r)
		case r == ' ' || r == '-' || r == '.' || r == '/':
			b.WriteRune('_')
		}
	}
	tok := strings.Trim(b.String(), "_")
	if len(tok) > 64 {
		tok = tok[:64]
	}
	if tok == "" {
		return "unparseable_cause"
	}
	return tok
}

// AggregateAccounting folds phases into the run-level verdict. A run is PASS
// only when every phase is PASS.
func AggregateAccounting(phases []PhaseAccounting) PhaseAccounting {
	total := PhaseAccounting{LogicalBreakdown: map[string]int{}, Verdict: "PASS"}
	for _, p := range phases {
		total.Requests += p.Requests
		total.TransportErrors += p.TransportErrors
		total.TerminalCompleted += p.TerminalCompleted
		total.TerminalFailed += p.TerminalFailed
		total.TerminalBlocked += p.TerminalBlocked
		total.TerminalUnknown += p.TerminalUnknown
		for k, v := range p.LogicalBreakdown {
			total.LogicalBreakdown[k] += v
		}
		if total.FailureCauses == nil {
			total.FailureCauses = map[string]int{}
		}
		for k, v := range p.FailureCauses {
			total.FailureCauses[k] += v
		}
		total.CompletedEmptyOutput += p.CompletedEmptyOutput
		if p.Verdict != "PASS" {
			total.Verdict = "FAIL"
			total.VerdictReasons = append(total.VerdictReasons, p.VerdictReasons...)
		}
	}
	total.LogicalErrors = total.Requests - total.TerminalCompleted
	if total.LogicalErrors < 0 {
		total.LogicalErrors = 0
	}
	return total
}
