package stateint

// Negative isolation and revocation cases required by acceptance point 6. Every
// check is observed against the live mTLS server and real Postgres: scope is
// derived solely from the verified certificate plus a bound task token, bodies
// carry no identity, and a revoked grant stops every task-route call.

import (
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestIsolationCrossTenantCannotReadThread proves tenant/deployment isolation at
// the API boundary. A fully valid FOREIGN principal (foreign workload cert +
// foreign-bound task token, both mutually consistent so the cert/token
// cross-check passes and the signature verifies) reading a thread owned by
// another scope receives NOT_FOUND — the store never surfaces a cross-scope
// row, and the response is indistinguishable from a genuinely missing thread.
// The SAME foreign principal can still create and read its OWN thread, which
// proves the result is scoping, not a blanket denial.
func TestIsolationCrossTenantCannotReadThread(t *testing.T) {
	e := newEnv(t)
	own := newTaskThread(e, t) // lives in e.tenant/e.deployment

	fTenant := "f-" + contracts.NewID()
	fDep := "fd-" + contracts.NewID()
	// Foreign-scoped token signed by this env's key: signature is valid, claims
	// are foreign. Paired with a foreign workload cert the cross-check passes.
	fToken := e.signTaskScoped(t, fTenant, fDep, testSubject, "task-foreign")

	// Foreign principal reading e's thread id -> 404 NOT_FOUND, no leak.
	status, out := e.rawWithCert(t, fTenant, fDep, "GetThread", fToken,
		jsonBody(map[string]any{"thread_id": own.ID}))
	if status != 404 {
		t.Fatalf("cross-scope GetThread status=%d body=%v, want 404", status, out)
	}
	if code, _ := out["code"].(string); code != "NOT_FOUND" {
		t.Fatalf("cross-scope GetThread code=%v, want NOT_FOUND", out["code"])
	}

	// Positive control: the same foreign principal creates its OWN thread -> 200.
	status, created := e.rawWithCert(t, fTenant, fDep, "CreateThread", fToken,
		jsonBody(map[string]any{"context_id": "ctx-" + contracts.NewID(), "key": "k-" + contracts.NewID()}))
	if status != 200 {
		t.Fatalf("foreign CreateThread status=%d body=%v, want 200", status, created)
	}
	if id, _ := created["id"].(string); id == "" {
		t.Fatalf("foreign CreateThread returned no id: %v", created)
	} else if id == own.ID {
		t.Fatalf("foreign create collided with owner thread id")
	}
}

// TestIsolationCertTokenScopeMismatch proves the certificate and the bearer
// token are cryptographically bound: presenting a foreign cert with a home-scope
// token, or a home cert with a foreign token, is rejected before any handler
// runs. This blocks token theft across workloads.
func TestIsolationCertTokenScopeMismatch(t *testing.T) {
	e := newEnv(t)
	own := newTaskThread(e, t)

	fTenant := "f-" + contracts.NewID()
	fDep := "fd-" + contracts.NewID()
	homeToken := e.signTask(t, "task-home")
	fToken := e.signTaskScoped(t, fTenant, fDep, testSubject, "task-foreign")

	// Foreign cert + home token -> scope cross-check fails -> 403 FORBIDDEN.
	status, out := e.rawWithCert(t, fTenant, fDep, "GetThread", homeToken,
		jsonBody(map[string]any{"thread_id": own.ID}))
	if status != 403 {
		t.Fatalf("foreign-cert/home-token status=%d body=%v, want 403", status, out)
	}
	if code, _ := out["code"].(string); code != "FORBIDDEN" {
		t.Fatalf("foreign-cert/home-token code=%v, want FORBIDDEN", out["code"])
	}

	// Home cert + foreign token -> workload binding fails at verification -> 403.
	status, out = e.rawWithCert(t, e.tenant, e.deployment, "GetThread", fToken,
		jsonBody(map[string]any{"thread_id": own.ID}))
	if status != 403 {
		t.Fatalf("home-cert/foreign-token status=%d body=%v, want 403", status, out)
	}
}

// TestIsolationMissingBearerToken proves a task route refuses to run on a valid
// certificate alone: without a bearer task token the request is FORBIDDEN at the
// middleware, before the body is even read.
func TestIsolationMissingBearerToken(t *testing.T) {
	e := newEnv(t)
	own := newTaskThread(e, t)

	status, out := e.rawWithCert(t, e.tenant, e.deployment, "GetThread", "",
		jsonBody(map[string]any{"thread_id": own.ID}))
	if status != 403 {
		t.Fatalf("missing-bearer status=%d body=%v, want 403", status, out)
	}
	if code, _ := out["code"].(string); code != "FORBIDDEN" {
		t.Fatalf("missing-bearer code=%v, want FORBIDDEN", out["code"])
	}
}

// TestIsolationRevocationBlocksEveryCall proves the authorize callback is a live
// gate consulted on every task-route request: once the grant is revoked, both
// lease-mutating (Heartbeat) and read (GetThread) operations become FORBIDDEN,
// even though the presented tokens are still otherwise well-formed and unexpired.
func TestIsolationRevocationBlocksEveryCall(t *testing.T) {
	e := newEnv(t)
	lp := e.claimFirst(t, "w-revoke") // installs a live lease token

	// Sanity: the lease works before revocation.
	if _, err := e.client.Heartbeat(e.ctx, lp.fence, 60000); err != nil {
		t.Fatalf("pre-revocation Heartbeat: %v", err)
	}

	e.revoked.Store(true)

	_, hbErr := e.client.Heartbeat(e.ctx, lp.fence, 60000)
	mustCode(t, hbErr, contracts.ErrForbidden, "Heartbeat after revocation")

	_, gtErr := e.client.GetThread(e.ctx, lp.thread.ID)
	mustCode(t, gtErr, contracts.ErrForbidden, "GetThread after revocation")
}

// TestIsolationEmptyTokenFailsClosed proves the SDK refuses to send an
// unauthenticated request at all: an empty bearer token yields FORBIDDEN on the
// client side before any HTTP round trip.
func TestIsolationEmptyTokenFailsClosed(t *testing.T) {
	e := newEnv(t)
	own := newTaskThread(e, t)

	e.setToken("")
	_, err := e.client.GetThread(e.ctx, own.ID)
	mustCode(t, err, contracts.ErrForbidden, "GetThread with empty token")
}
