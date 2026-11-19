package dote2e_test

// PDF rev3.2 L942 says a held side effect waits for the employee's decision.
// The cancel leg of that decision: when the employee does not sign, the effect
// must never execute - not on retry, not by digest confusion. The platform has
// no employee-facing Decline route today (recorded as GAP-DOTE2E-7), so the
// honest cancel proof is: the retry stays held, a wrong digest is refused, the
// connector store shows no effect, and the approval row stays unapproved in PG.

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHitlCancelNeverExecutesTheEffect(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-cancel-"+stamp,
		"dot-cancel-ctx-"+stamp, []string{"tools", "state"})
	checks := map[string]any{}

	// (a) The side effect is raised and held.
	callKey := task + "-never"
	arguments := map[string]any{"note": "This note must never be written " + stamp}
	err := stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": callKey,
		"tool": "record_note", "arguments": arguments}, token, nil)
	if status(err) != 409 || !strings.Contains(strings.ToLower(errText(err)), "approval") {
		checks["side_effect_held"] = map[string]any{"status": "FAIL", "error": truncate(errText(err), 160)}
		stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
		t.Fatalf("held effect missing: %v", errText(err))
	}
	checks["side_effect_held"] = map[string]any{"status": "PASS", "http": 409}

	// (b) There is no Decline/Cancel route for the employee on the approvals
	// face; the absence is recorded, not hidden.
	var declined map[string]any
	errDecline := stand.call(stand.HTTP, "/u-ahp/v1/tools/Decline", map[string]any{"request_id": "x"}, human, &declined)
	checks["employee_decline_route"] = map[string]any{"status": "GAP-DOTE2E-7",
		"http": status(errDecline), "code": errorCode(errDecline),
		"note": "no employee-facing Decline on the Tool GW approvals face; cancel is expressed by never signing"}

	// (c) Retry without signing: the same held decision, no execution.
	err = stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": callKey,
		"tool": "record_note", "arguments": arguments}, token, nil)
	if status(err) != 409 {
		checks["retry_still_held"] = map[string]any{"status": "FAIL", "http": status(err),
			"error": truncate(errText(err), 160)}
		stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
		t.Fatalf("an unsigned retry executed the effect: %v", errText(err))
	}
	checks["retry_still_held"] = map[string]any{"status": "PASS", "http": 409}

	// (d) The employee finds the request and tries to sign a digest that is not
	// the held one: that must be refused.
	var worklist struct {
		Approvals []map[string]any `json:"approvals"`
	}
	var pending map[string]any
	for attempt := 0; attempt < 10 && pending == nil; attempt++ {
		if err := stand.call(stand.HTTP, "/u-ahp/v1/tools/Approvals", map[string]any{"limit": 50}, human, &worklist); err != nil {
			stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
			t.Fatalf("employee worklist: %v", errText(err))
		}
		for _, item := range worklist.Approvals {
			if item["tool"] == "record_note" {
				pending = item
			}
		}
		if pending == nil {
			time.Sleep(400 * time.Millisecond)
		}
	}
	if pending == nil {
		stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
		t.Fatal("the held request never reached the employee worklist")
	}
	err = stand.call(stand.HTTP, "/u-ahp/v1/tools/Approve", map[string]any{
		"request_id": field(pending, "request_id", "id"), "digest": "0000000000000000",
		"tool": "record_note"}, human, nil)
	checks["wrong_digest_refused"] = map[string]any{"status": "PASS", "http": status(err), "code": errorCode(err)}
	if err == nil {
		checks["wrong_digest_refused"].(map[string]any)["status"] = "FAIL"
		stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
		t.Fatal("a wrong digest was approvable")
	}

	// (e) The connector store shows no effect for this stamp.
	store, _ := os.ReadFile(filepath.Join(stand.Dir, "notes", "notes.jsonl"))
	checks["no_effect_in_connector_store"] = map[string]any{"status": "PASS",
		"stamp_absent": !strings.Contains(string(store), stamp)}
	if strings.Contains(string(store), stamp) {
		checks["no_effect_in_connector_store"].(map[string]any)["status"] = "FAIL"
		stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
		t.Fatal("the unsigned effect reached the connector store")
	}

	// (f) In PG the approval row exists and remains unapproved/ unconsumed.
	var approvedBy *string
	var consumed bool
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT approved_by, consumed_at IS NOT NULL FROM ahp_tools_all.approvals
		  WHERE tenant=$1 AND tool='record_note' ORDER BY created_at DESC LIMIT 1`,
		stand.Scope["tenant"]).Scan(&approvedBy, &consumed)
	if err != nil {
		checks["approval_row_unapproved"] = map[string]any{"status": "FAIL", "error": err.Error()}
		stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
		t.Fatalf("approval row probe: %v", err)
	}
	checks["approval_row_unapproved"] = map[string]any{"status": "PASS",
		"approved": approvedBy != nil, "consumed": consumed}
	if approvedBy != nil || consumed {
		checks["approval_row_unapproved"].(map[string]any)["status"] = "FAIL"
		stand.record(t, "hitl-cancel-no-effect", "FAIL", checks)
		t.Fatal("an unsigned approval became approved/consumed in PG")
	}
	stand.record(t, "hitl-cancel-no-effect", "PASS", checks)
}
