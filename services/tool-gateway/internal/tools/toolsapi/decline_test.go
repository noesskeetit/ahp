package toolsapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// (DH1) The employee-facing loop over the mounted face: a gated call is refused,
// the request shows up in the human worklist, a DECLINE lands there, the effect
// stays unexecuted, a retry is refused as declined, and the queue no longer
// offers the answered request.
func TestDeclineRouteRefusesTheEffect(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	human := e.signHuman(t)
	args := `{"action":"send"}`

	status, body := e.call(t, "Call", tok, `{"key":"k-d-1","tool":"gated","arguments":`+args+`}`)
	mustStatus(t, status, http.StatusConflict, body)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject}
	requestID, digest, err := e.svc.CreateApprovalRequest(ctx, scope, "k-d-1", "gated", json.RawMessage(args))
	if err != nil {
		t.Fatalf("CreateApprovalRequest: %v", err)
	}
	if list := worklist(t, e, human); !hasRequest(list, requestID) {
		t.Fatalf("the refused request is missing from the human worklist: %v", list)
	}

	status, body = e.call(t, "Decline", human, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`)
	mustStatus(t, status, http.StatusOK, body)
	if body["declined"] != true || body["declined_by"] != tHumanSubj {
		t.Fatalf("decline body = %v, want declined=true declined_by=%s", body, tHumanSubj)
	}

	status, body = e.call(t, "Call", tok, `{"key":"k-d-1","tool":"gated","arguments":`+args+`}`)
	if status != http.StatusConflict {
		t.Fatalf("retry after decline = %d %v, want 409", status, body)
	}
	if msg, _ := body["message"].(string); !containsDeclined(msg) {
		t.Fatalf("retry must name the decline, got %v", body)
	}
	// Nothing executed and the answer is not re-askable.
	if _, err := e.svc.Get(ctx, scope, "k-d-1"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("a declined call must not be journalled: err=%v", err)
	}
	// The refusal itself is on the record: a deny row naming the employee, in the
	// same journal the guardian writes, with no allow row for this operation.
	if n := e.journalRows(t, "deny", "human-decline", digest); n != 1 {
		t.Fatalf("decline audit rows = %d, want exactly 1", n)
	}
	if n := e.journalRows(t, "allow", "%", digest); n != 0 {
		t.Fatalf("a declined operation must have no allow row, got %d", n)
	}
	if list := worklist(t, e, human); hasRequest(list, requestID) {
		t.Fatalf("a declined request must leave the worklist: %v", list)
	}
	status, body = e.call(t, "Approve", human, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`)
	if status != http.StatusConflict {
		t.Fatalf("approve after decline = %d %v, want 409", status, body)
	}
}

// (DH2) Declining is human-only: a task bearer cannot answer its own HITL
// request (the whole point of the PDF rule that an agent cannot say "yes"/"no"
// for the employee).
func TestDeclineRouteRejectsTaskBearer(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	requestID, digest, err := e.svc.CreateApprovalRequest(ctx,
		contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: tSubject},
		"k-d-2", "gated", json.RawMessage(`{"action":"send"}`))
	if err != nil {
		t.Fatalf("CreateApprovalRequest: %v", err)
	}
	status, body := e.call(t, "Decline", tok, `{"request_id":"`+requestID+`","digest":"`+digest+`","tool":"gated"}`)
	if status == http.StatusOK {
		t.Fatalf("a task token declined a human approval: %v", body)
	}
	if status != http.StatusForbidden {
		t.Fatalf("task bearer decline = %d %v, want 403", status, body)
	}
	// And the request is still pending for the human.
	if list := worklist(t, e, e.signHuman(t)); !hasRequest(list, requestID) {
		t.Fatalf("the request must stay pending after a refused decline: %v", list)
	}
}

func worklist(t *testing.T, e *env, humanTok string) []any {
	t.Helper()
	status, body := e.call(t, "Approvals", humanTok, `{}`)
	mustStatus(t, status, http.StatusOK, body)
	list, _ := body["approvals"].([]any)
	return list
}

func hasRequest(list []any, requestID string) bool {
	for _, item := range list {
		m, _ := item.(map[string]any)
		if m["request_id"] == requestID {
			return true
		}
	}
	return false
}

func containsDeclined(msg string) bool {
	return strings.Contains(strings.ToLower(msg), "declined")
}

// journalRows counts this tenant's guardian audit rows by decision, class and
// operation digest, attributed to the answering employee. Content-free: the
// journal holds identifiers and a digest, never arguments.
func (e *env) journalRows(t *testing.T, decision, class, digest string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	q := fmt.Sprintf(`
		SELECT count(*) FROM %[1]s.guardian_log
	 WHERE tenant=$1 AND decision=$2 AND class LIKE $3 AND digest=$4 AND subject=$5`, e.schema)
	var n int
	if err := e.pool.QueryRow(ctx, q, e.tenant, decision, class, digest, tHumanSubj).Scan(&n); err != nil {
		t.Fatalf("scan guardian_log: %v", err)
	}
	return n
}
