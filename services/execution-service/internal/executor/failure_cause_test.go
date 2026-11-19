package executor

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// DEF(operator visibility): when a model/tool step fails the task ends with
// close_reason=error and the message "executor: model/tool turn failed", and the
// cause the executor already computed (errText / runErr) reaches nobody: not the
// contract-visible status message, not the journal. An operator then cannot tell
// "object store full" from "model refused" without a debug build. These tests pin
// the two halves of the fix: the cause is journalled, and a bounded, redacted
// form of it travels in the terminal status text.

func captureLogs(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	return &buf
}

func TestFailureCauseCarriesTheComputedReason(t *testing.T) {
	cause := failureCause("executor: model/tool turn failed",
		nil, "artifact store: put object: Storage reached its minimum free drive threshold")
	if !strings.Contains(cause, "minimum free drive threshold") {
		t.Fatalf("terminal reason lost the cause, got %q", cause)
	}
	if !strings.HasPrefix(cause, "executor: model/tool turn failed") {
		t.Fatalf("the canonical sentence must stay first, got %q", cause)
	}
}

func TestFailureCauseStaysWithinTheContractBound(t *testing.T) {
	long := strings.Repeat("quota window exhausted for tenant ", 40)
	cause := failureCause("executor: model/tool turn failed", errors.New(long), "")
	if !strings.Contains(cause, "quota window exhausted") {
		t.Fatalf("prose must survive redaction, got %.120q", cause)
	}
	if len(cause) > 512 {
		t.Fatalf("a2a status text is bounded at 512, got %d bytes", len(cause))
	}
	if !strings.HasSuffix(cause, truncatedMark) {
		t.Fatalf("a truncated cause must say so, got %q", cause[len(cause)-40:])
	}
}

func TestFailureCauseRedactsCredentials(t *testing.T) {
	secret := "AKIAIOSFODNN7EXAMPLEsupersecretvalue0123456789"
	cause := failureCause("executor: model/tool turn failed", nil,
		"put https://user:hunter2@minio.internal:9000/bucket: Authorization: Bearer "+secret)
	if strings.Contains(cause, secret) || strings.Contains(cause, "hunter2") {
		t.Fatalf("the cause leaked a credential: %q", cause)
	}
	if !strings.Contains(cause, redactedMark) {
		t.Fatalf("a redacted span must be visible as redacted, got %q", cause)
	}
}

func TestFailureCauseWithoutAnyCauseStaysCanonical(t *testing.T) {
	if got := failureCause("executor: model/tool turn failed", nil, ""); got != "executor: model/tool turn failed" {
		t.Fatalf("no cause must not invent one, got %q", got)
	}
}

func TestTurnFailureIsJournalledWithTheSanitizedCause(t *testing.T) {
	buf := captureLogs(t)
	secret := "token=z0mgV3rTPfxQabcdef0123456789ABCDEF"
	journalTurnFailure("task-1", nil, "vault refused: "+secret)
	out := buf.String()
	if !strings.Contains(out, "task-1") {
		t.Fatalf("the journal line must name the task, got %q", out)
	}
	if strings.Contains(out, "z0mgV3rTPfxQabcdef") {
		t.Fatalf("the journal line leaked a credential: %q", out)
	}
	if !strings.Contains(out, "vault refused") {
		t.Fatalf("the journal line must carry the cause, got %q", out)
	}
}
