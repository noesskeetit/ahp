package stateapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestScopeBodyFieldsRejected proves the scope can never come from the body:
// every scope-ish field is an unknown field and strict JSON rejects it.
func TestScopeBodyFieldsRejected(t *testing.T) {
	e := newEnv(t)
	task := "task-" + contracts.NewID()
	tok := e.signTask(t, task)

	bodies := []string{
		`{"context_id":"c1","key":"k1","scope":{"tenant":"x","deployment":"y","subject":"z"}}`,
		`{"context_id":"c1","key":"k1","tenant":"x"}`,
		`{"context_id":"c1","key":"k1","deployment":"y"}`,
		`{"context_id":"c1","key":"k1","subject":"z"}`,
		`{"context_id":"c1","key":"k1","deployment_id":"y"}`,
	}
	for _, b := range bodies {
		status, body := e.call(t, "CreateThread", tok, b)
		mustStatus(t, status, http.StatusBadRequest, body)
	}

	// ClaimRun accepts no subject either.
	for _, b := range []string{
		`{"worker":"w1","subject":"someone"}`,
		`{"worker":"w1","scope":{"tenant":"x"}}`,
	} {
		status, body := e.call(t, "ClaimRun", "", b)
		mustStatus(t, status, http.StatusBadRequest, body)
	}

	// The clean body still works and lands in the certificate/token scope.
	status, th := e.call(t, "CreateThread", tok, `{"context_id":"ctx-scope","key":"k-scope"}`)
	mustStatus(t, status, http.StatusOK, th)
	sc, _ := th["scope"].(map[string]any)
	if sc["tenant"] != e.tenant || sc["deployment"] != e.deployment ||
		sc["subject"] != testSubject || sc["subject"] == task {
		t.Fatalf("scope leaked from body: %v", sc)
	}
}

// TestRevokedAuthorizeDeniesExistingData proves a revoked grant stops every
// task route even for data the subject already owns, and that a revoked
// issuer makes ClaimRun release its lease instead of handing out a
// tokenless run.
func TestRevokedAuthorizeDeniesExistingData(t *testing.T) {
	e := newEnv(t)
	task := "task-" + contracts.NewID()
	tok := e.signTask(t, task)

	status, th := e.call(t, "CreateThread", tok, `{"context_id":"ctx-rev","key":"k-rev"}`)
	mustStatus(t, status, http.StatusOK, th)
	threadID, _ := th["id"].(string)
	status, run := e.call(t, "CreateRun", tok, `{"thread_id":"`+threadID+
		`","key":"k-run","payload":{"a":1}}`)
	mustStatus(t, status, http.StatusOK, run)
	runID, _ := run["id"].(string)

	// Revoke the grant: the still-valid token now reaches nothing.
	e.revoked.Store(true)
	for _, call := range []struct{ op, body string }{
		{"GetThread", `{"thread_id":"` + threadID + `"}`},
		{"GetRun", `{"run_id":"` + runID + `"}`},
		{"ListRuns", `{"thread_id":"` + threadID + `"}`},
		{"CreateThread", `{"context_id":"ctx-rev","key":"k-rev"}`},
	} {
		status, body := e.call(t, call.op, tok, call.body)
		mustStatus(t, status, http.StatusForbidden, body)
	}

	// Claim with a denied issuer: no tokenless run may surface and the
	// lease must be gone (released as canceled, a terminal status).
	status, claimed := e.call(t, "ClaimRun", "", `{"worker":"w1","ttl_ms":60000}`)
	mustStatus(t, status, http.StatusForbidden, claimed)
	if tok, ok := claimed["task_token"].(string); ok && tok != "" {
		t.Fatal("denied claim leaked a token")
	}
	if l, ok := claimed["lease"]; ok && l != nil {
		t.Fatal("denied claim leaked a lease")
	}
	e.revoked.Store(false)

	// Canceled is terminal: the run is never handed out again.
	status, claimed = e.call(t, "ClaimRun", "", `{"worker":"w1","ttl_ms":60000}`)
	mustStatus(t, status, http.StatusOK, claimed)
	if claimed["lease"] != nil {
		t.Fatalf("canceled run was reclaimed: %v", claimed)
	}
	status, gotRun := e.call(t, "GetRun", tok, `{"run_id":"`+runID+`"}`)
	mustStatus(t, status, http.StatusOK, gotRun)
	if gotRun["status"] != "canceled" {
		t.Fatalf("run status after denied claim = %v, want canceled", gotRun["status"])
	}
}

// TestForeignTenantCannotProbe proves isolation: a second workload
// (different tenant/deployment certificate) sees NOT_FOUND, never the data.
func TestForeignTenantCannotProbe(t *testing.T) {
	e := newEnv(t)
	task := "task-" + contracts.NewID()
	tok := e.signTask(t, task)
	status, th := e.call(t, "CreateThread", tok, `{"context_id":"ctx-iso","key":"k-iso"}`)
	mustStatus(t, status, http.StatusOK, th)
	threadID, _ := th["id"].(string)

	// A foreign workload with its own valid token.
	foreign := newEnv(t)
	foreignTask := "task-" + contracts.NewID()
	foreignTok := foreign.signTask(t, foreignTask)
	status, body := foreign.call(t, "GetThread", foreignTok, `{"thread_id":"`+threadID+`"}`)
	mustStatus(t, status, http.StatusNotFound, body)

	// And the foreign deployment's claim never returns our run.
	status, claimed := foreign.call(t, "ClaimRun", "", `{"worker":"w1","ttl_ms":60000}`)
	mustStatus(t, status, http.StatusOK, claimed)
	if claimed["lease"] != nil {
		t.Fatalf("foreign deployment claimed a foreign run: %v", claimed)
	}
	if tok, ok := claimed["task_token"].(string); ok && tok != "" {
		t.Fatalf("empty claim leaked a token: %v", claimed)
	}
}

// TestNoTokenRejected proves no public/human token is accepted on task
// routes: only a bound task token passes.
func TestNoTokenRejected(t *testing.T) {
	e := newEnv(t)
	human, err := e.signer.Sign(auth.Claims{
		Audience: testAudience, Kind: auth.KindHuman,
		Tenant: e.tenant, Subject: testSubject,
	}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{human, "garbage.token.value", ""} {
		status, body := e.call(t, "GetThread", tok, `{"thread_id":"x"}`)
		mustStatus(t, status, http.StatusForbidden, body)
	}
	wrongAud, err := e.signer.Sign(auth.Claims{
		Audience: "other", Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment,
		Subject: testSubject, Task: "task-x", Root: "r", Context: "c", Grant: "g",
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, body := e.call(t, "GetThread", wrongAud, `{"thread_id":"x"}`)
	mustStatus(t, status, http.StatusForbidden, body)
}
