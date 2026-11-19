package main

import (
	"fmt"
	"strings"
	"testing"
)

// A product auth error that echoes the rejected bearer must never reach the log
// or the tailed platform log. The marker stands in for a live token: the RED this
// pins is "the body was interpolated into the error" (the pre-fix
// fmt.Errorf("login http %d: %s", ...)); the fix renders status + code + length.
func TestAuthFailureNeverEchoesBody(t *testing.T) {
	const marker = "SECRET-BEARER-MARKER-xyz"
	cases := []struct {
		op     string
		status int
		body   string
	}{
		{"login", 401, fmt.Sprintf(`{"error":"bad credentials echoed=%s","code":"auth_invalid"}`, marker)},
		{"exchange", 403, fmt.Sprintf(`plaintext echo of %s with no json`, marker)},
	}
	for _, c := range cases {
		err := authFailure(c.op, c.status, []byte(c.body))
		s := err.Error()
		if strings.Contains(s, marker) {
			t.Fatalf("%s: auth failure leaked the response body into the error: %q", c.op, s)
		}
		if !strings.Contains(s, fmt.Sprintf("%d", c.status)) {
			t.Fatalf("%s: error must name the HTTP status for diagnosis: %q", c.op, s)
		}
		if !strings.Contains(s, "bytes") {
			t.Fatalf("%s: error must report the body length so a truncation is visible: %q", c.op, s)
		}
	}
}

// Losing the whole response must not also lose the machine-readable reason: the
// product "code" is a short diagnostic token, kept (bounded) so a real 403 is
// still explainable without echoing the body.
func TestAuthFailureKeepsProductCode(t *testing.T) {
	err := authFailure("exchange", 403, []byte(`{"code":"exchange_denied","message":"do not print this message"}`))
	if !strings.Contains(err.Error(), "exchange_denied") {
		t.Fatalf("auth failure should surface the product code; got %q", err.Error())
	}
	if strings.Contains(err.Error(), "do not print this message") {
		t.Fatalf("auth failure must not echo the message field; got %q", err.Error())
	}
}
