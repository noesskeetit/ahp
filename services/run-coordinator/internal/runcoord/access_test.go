package runcoord

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func coordinatorAuth(t *testing.T) (*auth.Signer, *auth.Verifier) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("coordinator-tests", "test-key", priv)
	if err != nil {
		t.Fatal(err)
	}
	return signer, auth.NewVerifier("coordinator-tests", map[string]ed25519.PublicKey{"test-key": pub})
}

func signedCaller(t *testing.T, signer *auth.Signer, claims auth.Claims) string {
	t.Helper()
	token, err := signer.Sign(claims, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func taskCaller(scope contracts.Scope, capability string) auth.Claims {
	return auth.Claims{Kind: auth.KindTask, Audience: "ahp", Tenant: scope.Tenant,
		Deployment: scope.Deployment, Subject: scope.Subject, Task: "coord-test", Root: "root",
		Context: "ctx", Grant: "grant", Capabilities: []string{capability}}
}

func humanCaller(tenant, subject string) auth.Claims {
	return auth.Claims{Kind: auth.KindHuman, Audience: "ahp-browser", Tenant: tenant, Subject: subject}
}

// This fixture injects the outcome of certificate verification. The handler
// still derives the workload through transport.Workload and verifies the JWT.
func setVerifiedPeer(r *http.Request, peer contracts.Scope) {
	workload, err := url.Parse(auth.WorkloadID(peer))
	if err != nil {
		panic(err)
	}
	cert := &x509.Certificate{URIs: []*url.URL{workload}}
	r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}}
}

func coordinatorRequest(ctx context.Context, handler http.Handler, method, path, body, token string, peer contracts.Scope) *httptest.ResponseRecorder {
	r := httptest.NewRequestWithContext(ctx, method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Tenant-Id", "untrusted-tenant")
	r.Header.Set("X-Subject-Id", "untrusted-subject")
	if peer != (contracts.Scope{}) {
		setVerifiedPeer(r, peer)
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestAgentAccessPolicyPrecedesStateAccess(t *testing.T) {
	signer, verifier := coordinatorAuth(t)
	target := contracts.Scope{Tenant: "tenant-a", Deployment: "agent-1", Subject: "alice"}
	userEdge := contracts.Scope{Tenant: "tenant-a", Deployment: "user-edge"}
	outsider := contracts.Scope{Tenant: "tenant-b", Deployment: "user-edge"}
	platformEdge := contracts.Scope{Tenant: "platform", Deployment: "edge"}
	broker := contracts.Scope{Tenant: "tenant-a", Deployment: "broker", Subject: "alice"}
	otherBroker := contracts.Scope{Tenant: "tenant-b", Deployment: "broker", Subject: "alice"}
	rogue := contracts.Scope{Tenant: "tenant-a", Deployment: "rogue", Subject: "alice"}
	human := humanCaller(target.Tenant, target.Subject)
	for _, tc := range []struct {
		name        string
		peer        contracts.Scope
		claims      auth.Claims
		noPolicy    bool
		policyDeny  bool
		policyCalls int
		want        int
	}{
		{name: "direct task", peer: target, claims: taskCaller(target, "run.coordinate"), policyCalls: 1, want: http.StatusAccepted},
		{name: "permitted task broker", peer: broker, claims: taskCaller(broker, "run.coordinate"), policyCalls: 1, want: http.StatusAccepted},
		{name: "broker denied by agent access", peer: broker, claims: taskCaller(broker, "run.coordinate"), policyDeny: true, policyCalls: 1, want: http.StatusForbidden},
		{name: "broker allowlist is tenant bound", peer: otherBroker, claims: taskCaller(otherBroker, "run.coordinate"), want: http.StatusForbidden},
		{name: "non-broker workload", peer: rogue, claims: taskCaller(rogue, "run.coordinate"), want: http.StatusForbidden},
		{name: "task proof is bound to peer", peer: rogue, claims: taskCaller(broker, "run.coordinate"), want: http.StatusForbidden},
		{name: "task needs run.coordinate", peer: target, claims: taskCaller(target, "state.coordinate"), want: http.StatusForbidden},
		{name: "permitted tenant edge", peer: userEdge, claims: human, policyCalls: 1, want: http.StatusAccepted},
		{name: "permitted platform edge", peer: platformEdge, claims: human, policyCalls: 1, want: http.StatusAccepted},
		{name: "human denied by agent access", peer: userEdge, claims: human, policyDeny: true, policyCalls: 1, want: http.StatusForbidden},
		{name: "human across a tenant edge", peer: outsider, claims: human, want: http.StatusForbidden},
		{name: "human through unapproved workload", peer: rogue, claims: human, want: http.StatusForbidden},
		{name: "human without mtls", claims: human, want: http.StatusForbidden},
		{name: "missing policy fails closed", peer: userEdge, claims: human, noPolicy: true, want: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var policyCalls, stateCalls int
			policy := AccessPolicy{Verifier: verifier, UserAudience: "ahp-browser",
				PlatformEdgeWorkloadIDs: []string{auth.WorkloadID(platformEdge)},
				UserEdgeDeployments:     []string{"user-edge"}, BrokerWorkloadIDs: []string{auth.WorkloadID(broker)}}
			if !tc.noPolicy {
				policy.AuthorizeAgent = func(_ context.Context, principal CallerPrincipal, scope contracts.Scope) error {
					policyCalls++
					if scope != target || principal.Claims.Kind != tc.claims.Kind || auth.WorkloadID(principal.Peer) != auth.WorkloadID(tc.peer) {
						t.Fatalf("got caller %+v and target %+v", principal, scope)
					}
					if tc.policyDeny {
						return errors.New("agent access denied")
					}
					return nil
				}
			}
			c := &Coordinator{AccessPolicy: policy, StateFor: func(scope contracts.Scope) State {
				stateCalls++
				if scope != target {
					t.Fatalf("state scope %+v, want %+v", scope, target)
				}
				return coldAdmissionState{}
			}}
			w := coordinatorRequest(t.Context(), c.Handler(), http.MethodPost, "/v1/agents/agent-1/sessions/chat-1:send",
				`{"session_id":"foreign-session","harness_type":"go","format_version":1,"key":"test","input":{"inline":"e30="}}`,
				signedCaller(t, signer, tc.claims), tc.peer)
			wantStateCalls := 0
			if tc.want == http.StatusAccepted {
				wantStateCalls = 1
			}
			if w.Code != tc.want || policyCalls != tc.policyCalls || stateCalls != wantStateCalls {
				t.Fatalf("HTTP %d (%s), policy called %d times, state called %d times", w.Code, w.Body, policyCalls, stateCalls)
			}
		})
	}
}

func TestConfiguredAgentAccessRequiresSubjectAndDelegatedBroker(t *testing.T) {
	policy, err := ReadAgentAccess(strings.NewReader(`{"agents":[{"tenant":"tenant-a","deployment":"agent-1","subjects":["alice"],"brokers":["broker"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	target := contracts.Scope{Tenant: "tenant-a", Deployment: "agent-1", Subject: "alice"}
	broker := contracts.Scope{Tenant: "tenant-a", Deployment: "broker", Subject: "alice"}
	otherBroker := contracts.Scope{Tenant: "tenant-a", Deployment: "second-broker", Subject: "alice"}
	for _, tc := range []struct {
		claims auth.Claims
		scope  contracts.Scope
		allow  bool
	}{
		{humanCaller(target.Tenant, target.Subject), target, true},
		{taskCaller(target, "run.coordinate"), target, true},
		{taskCaller(broker, "run.coordinate"), target, true},
		{taskCaller(otherBroker, "run.coordinate"), target, false},
		{humanCaller(target.Tenant, "bob"), contracts.Scope{Tenant: target.Tenant, Deployment: target.Deployment, Subject: "bob"}, false},
		{humanCaller(target.Tenant, target.Subject), contracts.Scope{Tenant: target.Tenant, Deployment: "absent", Subject: target.Subject}, false},
	} {
		err := policy(t.Context(), CallerPrincipal{Claims: tc.claims, Peer: tc.claims.Scope()}, tc.scope)
		if (err == nil) != tc.allow {
			t.Fatalf("target %+v caller %+v got %v, allow = %t", tc.scope, tc.claims, err, tc.allow)
		}
	}
}

func TestCoordinatorRejectsInvalidConversationBeforeState(t *testing.T) {
	signer, verifier := coordinatorAuth(t)
	c := Coordinator{
		AccessPolicy: AccessPolicy{Verifier: verifier, AuthorizeAgent: func(context.Context, CallerPrincipal, contracts.Scope) error { return nil }},
		StateFor: func(contracts.Scope) State {
			t.Fatal("invalid conversation reached State")
			return nil
		},
	}
	scope := contracts.Scope{Tenant: "tenant-a", Deployment: "agent-1", Subject: "alice"}
	token := signedCaller(t, signer, taskCaller(scope, "run.coordinate"))
	for _, path := range []string{"/v1/agents/agent-1/sessions/chat%20room", "/v1/agents/agent-1/sessions/" + strings.Repeat("a", 257) + ":cancel", "/v1/agents/agent-1/sessions/bad%00name/stream"} {
		method := http.MethodGet
		if strings.HasSuffix(path, ":cancel") {
			method = http.MethodPost
		}
		if w := coordinatorRequest(t.Context(), c.Handler(), method, path, "{}", token, scope); w.Code != http.StatusBadRequest {
			t.Fatalf("invalid conversation at %q: HTTP %d (%s)", path, w.Code, w.Body)
		}
	}
}

func testAllowAgent(context.Context, CallerPrincipal, contracts.Scope) error { return nil }
