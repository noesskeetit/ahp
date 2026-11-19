package toolsapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestAuthAndMethod: the task routes require a verified task bearer, and only
// POST is served (a GET on a known operation is a 404).
func TestAuthAndMethod(t *testing.T) {
	e := newEnv(t)
	// No bearer: the task gate refuses (never 200).
	if status, _ := e.call(t, "ConnectionState", "", `{}`); status == 200 {
		t.Fatal("unauthenticated ConnectionState accepted")
	}
	// GET is not served on any operation.
	if got := e.getStatus(t, "Call"); got != http.StatusNotFound {
		t.Fatalf("GET /v1/tools/Call status=%d, want 404", got)
	}
}

// TestTenantIsolation: an effect journalled under one tenant is invisible to a
// foreign tenant/workload on the same front and database.
func TestTenantIsolation(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	if status, _ := e.call(t, "Call", tok, `{"key":"k-iso","tool":"echo","arguments":{}}`); status != 200 {
		t.Fatalf("seed call failed: status %d", status)
	}

	foreignTenant := "tF" + randomHex(t, 6)
	foreignDep := "dF" + randomHex(t, 6)
	foreignCert := e.ca.clientCert(t, workloadURI(foreignTenant, foreignDep))
	foreignTok, err := e.signer.Sign(auth.Claims{
		Audience: tAudience, Kind: auth.KindTask,
		Tenant: foreignTenant, Deployment: foreignDep, Subject: "uF",
		Task: "task-F", Root: "run-F", Context: "ctx-F", Grant: "gF", Depth: 0,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	status, body := e.doRaw(t, foreignCert, "Get", foreignTok, `{"key":"k-iso"}`)
	if status != http.StatusNotFound || contracts.Code(codeOfErr(body)) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant Get = status %d body %v, want 404 NOT_FOUND", status, body)
	}
}

// TestApproveHumanOnly exercises the full loop: a gated call is refused without
// approval; a task token can never approve; a human token can; then it succeeds.
func TestApproveHumanOnly(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	args := `{"action":"send"}`

	// (1) No approval yet -> refusal (CONFLICT).
	status, body := e.call(t, "Call", tok, `{"key":"k-g-1","tool":"gated","arguments":`+args+`}`)
	mustStatus(t, status, http.StatusConflict, body)

	// (2) The agent's follow-up step: create a pending approval request.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	requestID, digest, err := e.svc.CreateApprovalRequest(ctx,
		contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject},
		"k-g-2", "gated", json.RawMessage(args))
	if err != nil {
		t.Fatalf("CreateApprovalRequest: %v", err)
	}

	// (3) A task token must NOT approve.
	if status, _ := e.call(t, "Approve", tok, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`); status == http.StatusOK {
		t.Fatal("a task token approved a human approval -- human-only invariant broken")
	}

	// (4) A human token approves.
	human := e.signHuman(t)
	status, body = e.call(t, "Approve", human, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`)
	mustStatus(t, status, http.StatusOK, body)
	if body["approved"] != true {
		t.Fatalf("approve body = %v, want approved=true", body)
	}

	// (5) Now the call succeeds (fresh key, same canonical digest).
	status, body = e.call(t, "Call", tok, `{"key":"k-g-2","tool":"gated","arguments":`+args+`}`)
	mustStatus(t, status, http.StatusOK, body)
	if body["status"] != operations.StatusSucceeded {
		t.Fatalf("gated call after approval = %v, want SUCCEEDED", body)
	}
}

// TestNoContentInResponsesOrJournal: a call carrying a sentinel prompt/argument
// must never leak it into the HTTP response or any persisted journal column.
func TestNoContentInResponsesOrJournal(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	marker := "SENTINEL_PROMPT_" + randomHex(t, 8)
	args := `{"prompt":"` + marker + `","answer":"` + marker + `"}`
	status, body := e.call(t, "Call", tok, `{"key":"k-nc","tool":"echo","arguments":`+args+`}`)
	mustStatus(t, status, http.StatusOK, body)

	if _, ok := body["result"]; ok {
		t.Fatalf("response carries a 'result' field: %v", body)
	}
	requireNoContent(t, body, marker)
	if e.schemaContainsMarker(t, marker) {
		t.Fatalf("the %s schema persisted the content marker", e.schema)
	}
}

// --- helpers ---------------------------------------------------------------

func (e *env) getStatus(t *testing.T, op string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, e.srv.URL+"/v1/tools/"+op, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := e.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

func codeOfErr(body map[string]any) error {
	if c, ok := body["code"].(string); ok {
		return &contracts.Error{Code: c}
	}
	return nil
}

func (e *env) schemaContainsMarker(t *testing.T, marker string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	q := fmt.Sprintf(`
		SELECT count(*) FROM (
			SELECT (tenant||' '||deployment||' '||subject||' '||tool||' '||decision||' '||reason||' '||class||' '||digest) AS t
			  FROM %[1]s.guardian_log WHERE tenant=$1
			UNION ALL
			SELECT (key||' '||kind||' '||target||' '||fingerprint||' '||status||' '||coalesce(result::text,'')) FROM %[1]s.operations WHERE tenant=$1
		) x WHERE x.t LIKE '%%' || $2 || '%%'`, e.schema)
	var n int
	if err := e.pool.QueryRow(ctx, q, e.tenant, marker).Scan(&n); err != nil {
		t.Fatalf("scan journal for marker: %v", err)
	}
	return n > 0
}
