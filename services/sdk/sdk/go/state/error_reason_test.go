package state

import (
	"net/http"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestStateErrorReasonPreservesClassAndStaysBounded(t *testing.T) {
	for _, tc := range []struct {
		name, body, reason string
	}{
		{"key-reuse", `{"code":"INVALID_ARGUMENT","reason":"KEY_REUSE_MISMATCH","message":"different body"}`, contracts.ErrKeyReuse},
		{"lowercase-owner-reason", `{"code":"INVALID_ARGUMENT","reason":"tenant_state_effects_pending","message":"reconcile"}`, "tenant_state_effects_pending"},
		{"unknown-field", `{"code":"INVALID_ARGUMENT","reason":"KEY_REUSE_MISMATCH","message":"private-sentinel","credential":"not-allowed"}`, ""},
		{"invalid-reason", `{"code":"INVALID_ARGUMENT","reason":"key\nreuse","message":"private-sentinel"}`, ""},
		{"oversized-reason", `{"code":"INVALID_ARGUMENT","reason":"` + strings.Repeat("A", 65) + `","message":"private-sentinel"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := statusError(http.StatusBadRequest, []byte(tc.body))
			if contracts.Code(err) != contracts.ErrInvalid || contracts.ReasonOf(err) != tc.reason {
				t.Fatalf("State refusal lost class/reason: code=%s reason=%s error=%v", contracts.Code(err), contracts.ReasonOf(err), err)
			}
			if tc.reason == "" && strings.Contains(err.Error(), "private-sentinel") {
				t.Fatal("malformed owner error leaked its body")
			}
		})
	}
}
