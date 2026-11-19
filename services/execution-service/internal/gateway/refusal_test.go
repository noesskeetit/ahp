package gateway

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestSanitizeDetailKeepsGuardTextButRedactsValues pins the projection rule: the
// static part of a cause line stays readable for the operator, while values a
// lower layer quoted back -- the classic way request content escapes into a log
// -- do not travel.
func TestSanitizeDetailKeepsGuardTextButRedactsValues(t *testing.T) {
	cases := []struct {
		name    string
		in      error
		want    string
		notWant []string
	}{
		{
			name: "static guard line survives intact",
			in:   contracts.NewError(contracts.ErrInvalid, "gateway: task kind required"),
			want: "INVALID_ARGUMENT: gateway: task kind required",
		},
		{
			name:    "single-quoted value is redacted",
			in:      errors.New(`pq: value too long for type character varying(64): 'SECRET-PROMPT-TEXT'`),
			notWant: []string{"SECRET-PROMPT-TEXT"},
		},
		{
			name:    "double-quoted value is redacted",
			in:      errors.New(`driver error on "another-secret-marker" while binding`),
			notWant: []string{"another-secret-marker"},
		},
		{
			name:    "unterminated quote redacts the remainder",
			in:      errors.New(`echo of unterminated 'LEAKED-ARGUMENTS-AND-ARTIFACTS`),
			notWant: []string{"LEAKED-ARGUMENTS-AND-ARTIFACTS"},
		},
		{
			name:    "oversized token is truncated",
			in:      errors.New("echoed:" + strings.Repeat("x", 500)),
			notWant: []string{strings.Repeat("x", 100)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeDetail(tc.in)
			if got == "" {
				t.Fatal("sanitised detail is empty, operator would learn nothing")
			}
			if tc.want != "" && got != tc.want {
				t.Fatalf("detail = %q want %q", got, tc.want)
			}
			for _, bad := range tc.notWant {
				if strings.Contains(got, bad) {
					t.Fatalf("detail leaked %q: %q", bad, got)
				}
			}
			if n := len([]rune(got)); n > refusalDetailMaxRunes+1 {
				t.Fatalf("detail length = %d, want <= %d (+1 for the truncation marker)", n, refusalDetailMaxRunes)
			}
		})
	}
}

// TestSanitizeDetailCannotForgeJournalLines proves a cause line cannot inject a
// second journal line or a terminal escape: control characters are separators.
func TestSanitizeDetailCannotForgeJournalLines(t *testing.T) {
	forge := "real refusal\n{\"level\":\"ERROR\",\"msg\":\"gateway: forged operator line\"}\x1b[2Jam here"
	got := sanitizeDetail(errors.New(forge))
	if strings.ContainsAny(got, "\n\r\x1b") {
		t.Fatalf("detail still carries control characters: %q", got)
	}
	if strings.Contains(got, "\n") {
		t.Fatalf("detail can break the journal line: %q", got)
	}
}

// TestRefusalCodePrefersContractThenProtocol checks that the two families of
// refusal stay separately visible: a contract code never degrades into the
// protocol name, and a protocol error never reports an empty code.
func TestRefusalCodePrefersContractThenProtocol(t *testing.T) {
	if got := refusalCode(contracts.NewError(contracts.ErrKeyReuse, "x")); got != contracts.ErrKeyReuse {
		t.Fatalf("contract code = %q want %q", got, contracts.ErrKeyReuse)
	}
	if got := refusalCode(a2a.NewError(a2a.ErrInvalidParams, "gateway: request rejected")); got != "INVALID_PARAMS" {
		t.Fatalf("protocol code = %q want INVALID_PARAMS", got)
	}
	if got := refusalCode(fmt.Errorf("store: %w", a2a.ErrTaskNotFound)); got != "TASK_NOT_FOUND" {
		t.Fatalf("bare protocol sentinel = %q want TASK_NOT_FOUND", got)
	}
	if got := refusalCode(errors.New("bare failure")); got != "UNCLASSIFIED" {
		t.Fatalf("unclassified code = %q want UNCLASSIFIED", got)
	}
	if got := refusalCode(nil); got != "" {
		t.Fatalf("nil code = %q want empty", got)
	}
}

// TestRefusalReasonNamesTheGuard checks the precedence that makes two refusals
// of one transport class distinguishable: the guard that decided wins, the
// surfacing stage is the fallback, and an unexplained refusal says so.
func TestRefusalReasonNamesTheGuard(t *testing.T) {
	annotated := contracts.NewReasoned(contracts.ErrKeyReuse, ReasonKeyReuseMismatch, "gateway: message id reused with a different payload")
	if got := refusalReason("accept_task", annotated); got != ReasonKeyReuseMismatch {
		t.Fatalf("reason = %q want the deciding guard %q", got, ReasonKeyReuseMismatch)
	}
	if got := refusalReason("accept_task", errors.New("bare")); got != "accept_task" {
		t.Fatalf("fallback reason = %q want the stage", got)
	}
	if got := refusalReason("", errors.New("bare")); got != ReasonUnclassified {
		t.Fatalf("unexplained reason = %q want %q", got, ReasonUnclassified)
	}
	// A wrapped annotation still names its guard: propagation through fmt.Errorf
	// must not flatten the reason.
	wrapped := errors.New("outer: " + annotated.Error())
	_ = wrapped
	if got := refusalReason("stage", errorChain(annotated)); got != ReasonKeyReuseMismatch {
		t.Fatalf("wrapped reason = %q want %q", got, ReasonKeyReuseMismatch)
	}
}

// errorChain wraps err twice the way the layers below the handler do.
func errorChain(err error) error { return errors.Join(errors.New("outer"), err) }

// TestClassifyRefusalLeavesClientAnswerUntouched is the boundary rule: building
// the operator projection must not change the protocol error the caller sees.
func TestClassifyRefusalLeavesClientAnswerUntouched(t *testing.T) {
	for _, err := range []error{
		contracts.NewReasoned(contracts.ErrKeyReuse, ReasonKeyReuseMismatch, "gateway: message id reused with a different payload"),
		contracts.NewReasoned(contracts.ErrInvalid, ReasonContentEmpty, "gateway: message needs at least one part"),
		contracts.NewError(contracts.ErrForbidden, "gateway: context is not owned by this caller"),
		a2a.NewError(a2a.ErrTaskNotFound, "gateway: not found"),
		errors.New("bare storage failure"),
	} {
		before := mapError(err)
		r := classifyRefusal(MethodSendMessage, "accept_task", err)
		after := mapError(err)
		if before.Error() != after.Error() {
			t.Fatalf("projection changed the client answer: %q -> %q", before, after)
		}
		if strings.Contains(after.Error(), r.Reason) {
			t.Fatalf("client answer carries the operator reason %q: %q", r.Reason, after)
		}
	}
}
