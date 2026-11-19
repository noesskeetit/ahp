package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// testUserAudience is the audience identity puts on the credential a browser
// holds. The data plane never sees it as a task audience, which is exactly why
// mounting a user face has to be an explicit decision instead of a side effect.
const testUserAudience = "ahp-browser"

// TestUserCredentialIsRefusedOnATaskFace pins the measured starting point: on a
// plain Task face a browser bearer dies on the audience, so a route that
// forwards the user bearer untouched reaches a plane that cannot verify it.
func TestUserCredentialIsRefusedOnATaskFace(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		Task(verifier, testAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, testUserAudience, "t1", "alice"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "audience") {
		t.Fatalf("body = %s, want the audience refusal the lane measured", body)
	}
}

// TestTaskOrUserAcceptsUserCredentialOverTheEdgesCertificate is the ruling's
// shape: the edge presents its own client certificate, the user bearer is not
// rewritten, and the plane derives the boundary from both halves -- tenant and
// subject from the token, deployment from the verified workload, because a
// browser is not a deployment and no header is allowed to say otherwise.
func TestTaskOrUserAcceptsUserCredentialOverTheEdgesCertificate(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, testUserAudience, "t1", "alice"))
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s, want 200", resp.StatusCode, body)
	}
	var scope contracts.Scope
	if err := json.NewDecoder(resp.Body).Decode(&scope); err != nil {
		t.Fatalf("decode scope: %v", err)
	}
	if scope != (contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "alice"}) {
		t.Fatalf("scope = %+v, want tenant t1 from the token, deployment d1 from the certificate, subject alice", scope)
	}
}

// TestTaskOrUserStillAcceptsTaskTokens keeps every existing workload leg green:
// adding the user path must not disturb the credential they already present.
func TestTaskOrUserStillAcceptsTaskTokens(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s, want 200", resp.StatusCode, body)
	}
}

// TestTaskOrUserKeepsTheWorkloadBindingForTaskTokens is the control the fleet
// will re-measure: a task token is bound to the deployment that minted it, so
// presenting it over another deployment's certificate stays a refusal.
func TestTaskOrUserKeepsTheWorkloadBindingForTaskTokens(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, "spiffe://ahp/tenant/t1/deployment/d2"),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signTask(t, signer, testAudience, "t1", "d1", "task-1"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

// TestTaskOrUserRefusesAUserCredentialFromAnotherTenant stops a stolen browser
// bearer from being pushed through a second tenant's edge.
func TestTaskOrUserRefusesAUserCredentialFromAnotherTenant(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, testUserAudience, "evil", "mallory"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

// TestTaskOrUserRefusesAUserCredentialWithoutTheEdgeCertificate: the plane keeps
// demanding a verified workload on the connection, so it can never become a
// plane a browser talks to directly.
func TestTaskOrUserRefusesAUserCredentialWithoutTheEdgeCertificate(t *testing.T) {
	_, verifier := newSignerVerifier(t)
	srv := httptest.NewServer(TaskOrUser(verifier, testAudience, testUserAudience, "cap:run",
		allowAll)(okHandler()))
	t.Cleanup(srv.Close)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/run", nil)
	req.Header.Set("Authorization", "Bearer whatever")
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

// TestTaskOrUserRefusesAUserCredentialForAnotherAudience: switching on the
// credential kind must not turn into "any human token from any deployment".
func TestTaskOrUserRefusesAUserCredentialForAnotherAudience(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, "some-other-audience", "t1", "alice"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

// TestTaskOrUserWithoutAUserAudienceIsTheTaskFaceExactly: an unwired plane keeps
// today's answer, including the error text the existing legs assert on.
func TestTaskOrUserWithoutAUserAudienceIsTheTaskFaceExactly(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, "", "cap:run", allowAll)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, testUserAudience, "t1", "alice"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "audience") {
		t.Fatalf("body = %s, want the task-face refusal to stay byte-stable", body)
	}
}

// TestTaskOrUserNilAuthorizeFailsClosed: no callback means no request may reach
// the handler, whichever credential was presented.
func TestTaskOrUserNilAuthorizeFailsClosed(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", nil)(okHandler()))
	for _, tok := range []string{
		signHuman(t, signer, testUserAudience, "t1", "alice"),
		signTask(t, signer, testAudience, "t1", "d1", "task-1"),
	} {
		resp := do(t, srv, tok)
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want 403", resp.StatusCode)
		}
	}
}

// TestTaskOrUserHandsTheAuthorizeCallbackTheFilledScope: the policy that names
// which edges may carry user credentials reads claims.Deployment, so the
// callback has to see the deployment the certificate proved.
func TestTaskOrUserHandsTheAuthorizeCallbackTheFilledScope(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	var seen auth.Claims
	var seenCapability string
	authorize := func(_ context.Context, c auth.Claims, capability string) error {
		seen, seenCapability = c, capability
		return nil
	}
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", authorize)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, testUserAudience, "t1", "alice"))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if seenCapability != "cap:run" {
		t.Fatalf("capability = %q, want cap:run", seenCapability)
	}
	if seen.Kind != auth.KindHuman || seen.Deployment != "d1" || seen.Tenant != "t1" {
		t.Fatalf("claims = kind %q deployment %q tenant %q, want human/d1/t1",
			seen.Kind, seen.Deployment, seen.Tenant)
	}
	if err := seen.Scope().Validate(); err != nil {
		t.Fatalf("the callback must receive a valid scope: %v", err)
	}
}

// TestTaskOrUserRefusalFromAuthorizeStopsTheRequest: a policy deny is a denial,
// not a fallthrough to the other credential branch.
func TestTaskOrUserRefusalFromAuthorizeStopsTheRequest(t *testing.T) {
	ca := newCA(t)
	signer, verifier := newSignerVerifier(t)
	deny := func(context.Context, auth.Claims, string) error {
		return contracts.NewError(contracts.ErrForbidden, "this edge may not carry user credentials")
	}
	srv := newMTLSServer(t, ca, ca.clientCert(t, testURI),
		TaskOrUser(verifier, testAudience, testUserAudience, "cap:run", deny)(okHandler()))
	resp := do(t, srv, signHuman(t, signer, testUserAudience, "t1", "alice"))
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "this edge may not carry user credentials") {
		t.Fatalf("body = %s, want the policy's own refusal", body)
	}
}
