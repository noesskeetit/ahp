package executor

import (
	"log/slog"
	"regexp"
	"strings"
)

// The turn-failure contract has two halves and both were missing.
//
// The A2A status message told the caller "executor: model/tool turn failed" and
// nothing else, while the executor had ALREADY computed the reason (errText from
// the failed step, or the run error). The operator running the deployment saw
// the same sentence the client saw -- which cannot distinguish "the object store
// answered StorageFull" from "the model refused": the only way to find out was to
// rebuild the binary with an extra log line. That is the defect this file closes.
//
// What is deliberately NOT done: the cause is never claimed to be a verdict about
// the outside world, and it is never published raw. Provider and storage errors
// quote request URLs, DSNs and bearer tokens back inside their own messages, so
// every span leaving this process goes through redactSecrets first, and the
// contract-visible text stays inside the 512-byte bound the status message has
// always been truncated to.
const (
	// maxCauseLen bounds the cause portion of the terminal reason. The status
	// text itself is truncated at 512 bytes elsewhere (truncate in executor.go);
	// this bound keeps the canonical sentence plus a useful cause inside it.
	maxCauseLen = 400
	// truncatedMark and redactedMark are literal so a test and an operator
	// grepping the journal can branch on them.
	truncatedMark = " [truncated]"
	redactedMark  = "[redacted]"
)

var (
	// credentialKeyRe matches key=value / key: value for the names that are
	// never safe to journal. The key stays (it tells the operator WHICH secret
	// the upstream quoted), the value goes.
	credentialKeyRe = regexp.MustCompile(`(?i)(token|password|passwd|secret|api[_-]?key|access[_-]?key|secret[_-]?key|authorization|credential)s?(\s*[=:]\s*)([^\s,;]+)`)
	// bearerRe matches an Authorization-style scheme plus its value even when it
	// appears mid-sentence, which is how http clients echo it on failure.
	bearerRe = regexp.MustCompile(`(?i)(bearer|basic|digest)\s+[A-Za-z0-9._~+/=-]{6,}`)
	// userinfoRe matches credentials embedded in a URL, including the form a
	// storage client quotes back on a PUT failure.
	userinfoRe = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+:[^\s/@]+@`)
	// longOpaqueRe is the last line of defence: a long opaque run is a key, an
	// opaque or a signature far more often than it is prose.
	longOpaqueRe = regexp.MustCompile(`[A-Za-z0-9+/=_-]{24,}`)
)

// redactSecrets removes credential-shaped spans from an upstream error. It is
// intentionally conservative about prose: short words survive, opaque runs do
// not. It is not a general PII filter -- it never pretends to be one.
func redactSecrets(s string) string {
	s = credentialKeyRe.ReplaceAllString(s, "$1$2"+redactedMark)
	s = bearerRe.ReplaceAllString(s, "$1 "+redactedMark)
	s = userinfoRe.ReplaceAllString(s, "$1"+redactedMark+"@")
	s = longOpaqueRe.ReplaceAllString(s, redactedMark)
	return s
}

// failureCause composes the terminal reason: the canonical sentence first (so
// anything that matches on it keeps working), then the sanitized cause when one
// was actually computed. No cause means the sentence stays byte-identical to what
// this process has always emitted -- an absent cause is never invented.
func failureCause(sentence string, runErr error, errText string) string {
	joined := strings.TrimSpace(errText)
	if runErr != nil {
		if joined != "" {
			joined += "; " + runErr.Error()
		} else {
			joined = runErr.Error()
		}
	}
	if joined == "" {
		return sentence
	}
	cause := redactSecrets(joined)
	if budget := maxCauseLen - len(truncatedMark); len(cause) > budget {
		cause = cause[:budget] + truncatedMark
	}
	return sentence + ": " + cause
}

// journalTurnFailure writes the operator-facing record of the same failure. It
// names the task and carries the sanitized cause, because the task row and the
// a2a status are bounded surfaces while the journal is where an operator actually
// looks. It says nothing about whether an effect happened -- effect truth lives
// in the operation ledger, and this line must not contradict it.
func journalTurnFailure(taskID string, runErr error, errText string) {
	cause := redactSecrets(strings.TrimSpace(errText))
	if len(cause) > maxCauseLen {
		cause = cause[:maxCauseLen] + truncatedMark
	}
	slog.Warn("executor: turn failed before completing",
		"task_id", taskID,
		"cause", cause,
		"run_error", redactSecrets(errString(runErr)),
		"cause_source", errTextSource(errText, runErr))
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// errTextSource names which plane produced the cause, so an operator can route
// the failure (tool plane vs model plane vs run loop) without reading code.
func errTextSource(errText string, runErr error) string {
	switch {
	case strings.TrimSpace(errText) != "" && runErr != nil:
		return "step_and_run"
	case runErr != nil:
		return "run"
	default:
		return "step"
	}
}
