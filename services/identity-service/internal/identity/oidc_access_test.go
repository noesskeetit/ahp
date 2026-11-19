package identity

import (
	"testing"
	"time"
)

func TestOIDCRequiresPublicAccessAssertion(t *testing.T) {
	now := time.Now()
	a := &oidcAuthenticator{cfg: OIDCConfig{Issuer: "https://issuer.example", ClientID: "ahp-public"}, now: func() time.Time { return now }}
	for _, tc := range []struct {
		name, use, kind, task string
		ok                    bool
	}{
		{"access", "access", "", "", true},
		{"human-access", "access", "human", "", true},
		{"missing-purpose", "", "", "", false},
		{"refresh", "refresh", "", "", false},
		{"task", "access", "task", "task-1", false},
		{"hidden-task", "access", "", "task-1", false},
		{"exchange", "exchange", "exchange", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claims := map[string]any{"iss": a.cfg.Issuer, "aud": a.cfg.ClientID, "exp": float64(now.Add(time.Minute).Unix()), "sub": "alice", "tenant": "tenant", "token_use": tc.use}
			if tc.kind != "" {
				claims["token_kind"] = tc.kind
			}
			if tc.task != "" {
				claims["task"] = tc.task
			}
			err := a.validateClaims(claims)
			if (err == nil) != tc.ok {
				t.Fatalf("accepted=%t want=%t", err == nil, tc.ok)
			}
		})
	}
}
