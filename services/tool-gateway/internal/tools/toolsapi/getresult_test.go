package toolsapi

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// These tests drive the mounted face over real mTLS with a real PostgreSQL
// ledger and a real MCP dispatch against the loopback fixture in
// fixtures_test.go. That fixture is a FAKE protocol server: it proves the
// gateway's own behaviour, not the behaviour of a real external provider.

func signTaskCtx(t *testing.T, e *env, contextID string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: tAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject,
		Task: "task-1", Root: "run-1", Context: contextID, Grant: "grant-1", Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// (10) The end-to-end owner path: dispatch through Call, then read the raw MCP
// result over GetResult with the SAME verified context. Reading must not send
// the effect again.
func TestGetResultRouteOwnerReadsOwnResult(t *testing.T) {
	e := newEnv(t)
	tok := signTaskCtx(t, e, "ctx-1")
	args := json.RawMessage(`{"v":1}`)
	e.allowDigest(t, "write", args)
	status, body := e.call(t, "Call", tok, `{"key":"k-api-1","tool":"write","arguments":`+string(args)+`}`)
	mustStatus(t, status, 200, body)
	if body["status"] != "SUCCEEDED" {
		t.Fatalf("seed call status = %v", body["status"])
	}
	before := e.fixture.effCount

	status, body = e.call(t, "GetResult", tok, `{"key":"k-api-1"}`)
	mustStatus(t, status, 200, body)
	if body["result_storage"] != "inline" || body["status"] != "SUCCEEDED" {
		t.Fatalf("GetResult body = %v", body)
	}
	raw, _ := json.Marshal(body["payload"])
	if !strings.Contains(string(raw), "written") {
		t.Fatalf("payload lost the MCP result: %s", raw)
	}
	prov, _ := json.Marshal(body["provenance"])
	if !strings.Contains(string(prov), "ctx-1") || !strings.Contains(string(prov), "tools-result-v1") {
		t.Fatalf("provenance = %s, want the dispatch-time labels", prov)
	}
	if strings.Contains(string(raw), "Bearer") {
		t.Fatalf("payload leaked a credential: %s", raw)
	}
	if e.fixture.effCount != before {
		t.Fatalf("GetResult re-dispatched: effects %d -> %d", before, e.fixture.effCount)
	}
	// The content-free journal route stays content-free.
	status, journal := e.call(t, "Get", tok, `{"key":"k-api-1"}`)
	mustStatus(t, status, 200, journal)
	for _, k := range []string{"payload", "provenance", "result"} {
		if _, ok := journal[k]; ok {
			t.Fatalf("Get leaked %q: %v", k, journal)
		}
	}
}

// (11) Same human, same tenant, different context: the read is refused, because
// the taint that paid for the effect belongs to the producing context.
func TestGetResultRouteSameSubjectDifferentContextDenied(t *testing.T) {
	e := newEnv(t)
	if status, body := e.call(t, "Call", signTaskCtx(t, e, "ctx-1"), `{"key":"k-api-2","tool":"echo","arguments":{}}`); status != 200 {
		t.Fatalf("seed call: %d %v", status, body)
	}
	status, body := e.call(t, "GetResult", signTaskCtx(t, e, "ctx-2"), `{"key":"k-api-2"}`)
	if status != 403 || contracts.Code(codeOfErr(body)) != contracts.ErrForbidden {
		t.Fatalf("sibling-context read = %d %v, want 403 FORBIDDEN", status, body)
	}
	// The owning context still reads it: the refusal is the binding, not a
	// blanket outage.
	if status, body := e.call(t, "GetResult", signTaskCtx(t, e, "ctx-1"), `{"key":"k-api-2"}`); status != 200 {
		t.Fatalf("owner read after a refused sibling = %d %v, want 200", status, body)
	}
}

// (12) A foreign workload cannot read it either, and never learns it exists.
func TestGetResultRouteForeignWorkloadDenied(t *testing.T) {
	e := newEnv(t)
	if status, body := e.call(t, "Call", e.signTask(t), `{"key":"k-api-3","tool":"echo","arguments":{}}`); status != 200 {
		t.Fatalf("seed call: %d %v", status, body)
	}
	foreignTenant := "tF" + randomHex(t, 6)
	foreignDep := "dF" + randomHex(t, 6)
	cert := e.ca.clientCert(t, workloadURI(foreignTenant, foreignDep))
	foreignTok, err := e.signer.Sign(auth.Claims{
		Audience: tAudience, Kind: auth.KindTask,
		Tenant: foreignTenant, Deployment: foreignDep, Subject: "uF",
		Task: "task-F", Root: "run-F", Context: "ctx-F", Grant: "gF", Depth: 0,
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if status, body := e.doRaw(t, cert, "GetResult", foreignTok, `{"key":"k-api-3"}`); status != 404 {
		t.Fatalf("foreign read = %d %v, want 404", status, body)
	}
}

// (13) GetResult is a task-facing owner route: a human bearer is not a task
// authority over effects.
func TestGetResultRouteRejectsHumanBearer(t *testing.T) {
	e := newEnv(t)
	if status, body := e.call(t, "Call", e.signTask(t), `{"key":"k-api-4","tool":"echo","arguments":{}}`); status != 200 {
		t.Fatalf("seed call: %d %v", status, body)
	}
	if status, body := e.call(t, "GetResult", e.signHuman(t), `{"key":"k-api-4"}`); status != 403 {
		t.Fatalf("human bearer = %d %v, want 403", status, body)
	}
}
