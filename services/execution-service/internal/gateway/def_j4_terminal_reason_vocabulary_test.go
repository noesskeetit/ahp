package gateway_test

import (
	"testing"

	"github.com/ahp-platform/ahp/internal/gateway"
)

// This leg is in its own file on purpose: it pins spellings that exist only with the fix, so
// keeping it beside the behavioural legs would make those legs fail to COMPILE on the unfixed
// base and the RED could not show the defect as a missing field -- the difference between
// "the invariant is violated" and "a constant is missing" is the whole evidence.
// TestDefJ4TerminalReasonVocabulary pins the spellings, for the operator and for the A->B
// comparison rather than for its own sake: the reason field is read across planes, and
// "canceled", "subject_deleted", "tenant_deleted" and "grant_revoked" are words the platform
// already uses elsewhere (state.RunCanceled, space.CloseReasonCanceled, memory's codes,
// identity's own refusal). Nothing here invents a synonym for an existing verdict.
func TestDefJ4TerminalReasonVocabularyIsFixed(t *testing.T) {
	cases := []struct{ what, got, want string }{
		{"a revoked grant", gateway.CloseReasonGrantRevoked, "grant_revoked"},
		{"a refused authority", gateway.CloseReasonAuthorityRefused, "authority_refused"},
		{"a plain model/policy error", gateway.CloseReasonError, "error"},
		{"a lost transport", gateway.CloseReasonProviderTransport, "provider_transport"},
		{"an exhausted attempt ceiling", gateway.CloseReasonAttemptCeiling, "attempt_ceiling_exceeded"},
		{"a honoured cancel", gateway.CloseReasonCanceled, "canceled"},
		{"a subject erasure", gateway.CloseReasonSubjectDeleted, "subject_deleted"},
		{"a tenant erasure", gateway.CloseReasonTenantDeleted, "tenant_deleted"},
		{"a transition nobody reported", gateway.CloseReasonUnreported, "executor_unreported"},
	}
	seen := map[string]string{}
	for _, c := range cases {
		if c.got != c.want {
			t.Fatalf("%s journals %q, want %q: the value of this key is read by another plane, so the spelling is part of the contract", c.what, c.got, c.want)
		}
		if other, clash := seen[c.got]; clash {
			t.Fatalf("two different facts (%s, %s) share the word %q: the dictionary would say nothing", other, c.what, c.got)
		}
		seen[c.got] = c.what
	}
}
