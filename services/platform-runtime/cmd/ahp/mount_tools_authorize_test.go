package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/tools/toolsapi"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// stubHumans is the durable human-permission read under test: found/not-found
// are distinguishable internally and must NOT be distinguishable on the wire.
type stubHumans struct {
	users    map[string]identity.BrowserUser
	sessions map[string]identity.BrowserSession
}

func (s stubHumans) BrowserTokenActive(_ context.Context, jti string) (identity.BrowserSession, error) {
	session, ok := s.sessions[jti]
	if !ok {
		return identity.BrowserSession{}, errors.New("inactive session")
	}
	return session, nil
}

func (s stubHumans) BrowserUserBySubject(_ context.Context, tenant, subject string) (identity.BrowserUser, error) {
	u, ok := s.users[tenant+"|"+subject]
	if !ok {
		return identity.BrowserUser{}, errors.New("identity: browser user not found")
	}
	return u, nil
}

// TestToolsHumanAuthorizeUniformDeny pins the refusal contract of the approvals
// plane (director 2026-11-02, foreign-principal leg): every denial -- wrong
// capability, non-human bearer, unknown subject, subject without
// tools:approve -- must be byte-identical to the caller and must not echo any
// identifier, while the real reason is audited server-side.
func TestToolsHumanAuthorizeUniformDeny(t *testing.T) {
	var logBuf bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logBuf, nil))
	humans := stubHumans{sessions: map[string]identity.BrowserSession{"live": {Tenant: "t1", Subject: "u-permitted"}, "plain": {Tenant: "t1", Subject: "u-plain"}}, users: map[string]identity.BrowserUser{
		"t1|u-permitted": {UserID: "1", Tenant: "t1", Subject: "u-permitted", Permissions: []string{"tools", "tools:approve"}},
		"t1|u-plain":     {UserID: "2", Tenant: "t1", Subject: "u-plain", Permissions: []string{"tools"}},
	}}
	authz := toolsHumanAuthorize(humans, log)

	if err := authz(context.Background(), auth.Claims{Kind: auth.KindHuman,
		Tenant: "t1", Subject: "u-permitted", JTI: "live"}, toolsapi.ApproveCapability); err != nil {
		t.Fatalf("permitted human must pass, got %v", err)
	}

	denials := []struct {
		name  string
		claim auth.Claims
		cap   string
	}{
		{"missing JTI", auth.Claims{Kind: auth.KindHuman, Tenant: "t1", Subject: "u-permitted"}, toolsapi.ConnectCapability},
		{"inactive JTI", auth.Claims{Kind: auth.KindHuman, Tenant: "t1", Subject: "u-permitted", JTI: "retired"}, toolsapi.ConnectCapability},
		{"mismatched subject", auth.Claims{Kind: auth.KindHuman, Tenant: "t1", Subject: "u-permitted", JTI: "plain"}, toolsapi.ConnectCapability},
		{"mismatched tenant", auth.Claims{Kind: auth.KindHuman, Tenant: "other", Subject: "u-permitted", JTI: "live"}, toolsapi.ConnectCapability},
		{"unknown subject", auth.Claims{Kind: auth.KindHuman, Tenant: "t1", Subject: "u-nobody"}, toolsapi.ApproveCapability},
		{"no capability", auth.Claims{Kind: auth.KindHuman, Tenant: "t1", Subject: "u-plain", JTI: "plain"}, toolsapi.ApproveCapability},
		{"task bearer", auth.Claims{Kind: auth.KindTask, Tenant: "t1", Subject: "svc-worker"}, toolsapi.ApproveCapability},
		{"other capability", auth.Claims{Kind: auth.KindHuman, Tenant: "t1", Subject: "u-plain"}, "tools"},
	}
	var first string
	for i, d := range denials {
		err := authz(context.Background(), d.claim, d.cap)
		if err == nil {
			t.Fatalf("%s: expected a refusal", d.name)
		}
		msg := err.Error()
		if !strings.HasPrefix(msg, "FORBIDDEN") {
			t.Fatalf("%s: refusal is not FORBIDDEN: %s", d.name, msg)
		}
		// The capability NAME is not scanned literally: "tools" is a substring of
		// the plane's own error prefix. The sensitive name is tools:approve.
		for _, leak := range []string{"t1", d.claim.Subject, "tools:approve"} {
			if leak != "" && strings.Contains(msg, leak) {
				t.Fatalf("%s: refusal leaked %q: %s", d.name, leak, msg)
			}
		}
		if i == 0 {
			first = msg
			continue
		}
		if msg != first {
			t.Fatalf("%s: refusal differs (%q vs %q) -- the plane would be an existence oracle", d.name, msg, first)
		}
	}
	for _, want := range []string{"approvals plane refused", "u-plain", "approver lacks capability"} {
		if !strings.Contains(logBuf.String(), want) {
			t.Fatalf("audit log must record %q for the operator; got: %s", want, logBuf.String())
		}
	}
}
