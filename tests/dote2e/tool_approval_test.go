package dote2e_test

// PDF rev3.2 L942: the approval of a side-effecting tool call is negotiated with
// the EMPLOYEE directly, by the Tool Gateway - not with the agent that asked for
// it. The scenario runs that whole exchange over the real HTTPS/mTLS edge:
//
//	a task token tries a side-effecting tool   -> refused, a pending request appears
//	the employee sees the request and signs it  -> approved
//	the same task retries the same key          -> the effect happens once
//
// The notes connector is the real MCP server the lane stand started (the lane's
// own ahp-note-mcp process); nothing in this scenario is stubbed. Only the human
// credential may sign, and the tenant admin of the same tenant - who lacks
// tools:approve - may not.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestToolApprovalIsSignedByTheEmployee(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "dot-hitl-"+stamp,
		"dot-hitl-ctx-"+stamp, []string{"tools", "state"})
	checks := map[string]any{}

	// (a) The agent's side effect stops at the gateway.
	callKey := task + "-record"
	// record_note is the lane connector's side-effecting tool; its own schema names
	// the field "note" (the MCP server refuses anything else).
	arguments := map[string]any{"note": "Dot asked the employee to approve this note " + stamp}
	err := stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": callKey,
		"tool": "record_note", "arguments": arguments}, token, nil)
	approvalRaised := err != nil && status(err) == 409 &&
		strings.Contains(strings.ToLower(errText(err)), "approval")
	held := map[string]any{"status": "PASS", "http": status(err),
		"code": errorCode(err), "message": truncate(errText(err), 160)}
	checks["side_effect_held_for_human"] = held
	if !approvalRaised {
		held["status"] = "FAIL"
		stand.record(t, "tool-approval-hitl", "FAIL", checks)
		t.Fatalf("a side-effecting call did not stop for a human decision: %v", errText(err))
	}

	// (b) The employee's own worklist holds that request. The machine credential
	// may not read it: the approvals plane is human-only.
	var taskWorklist struct {
		Approvals []map[string]any `json:"approvals"`
	}
	errTask := stand.call(stand.HTTP, "/v1/tools/Approvals", map[string]any{"limit": 20}, token, &taskWorklist)
	taskDenied := map[string]any{"status": "PASS", "http": status(errTask), "code": errorCode(errTask)}
	checks["approvals_plane_task_denied"] = taskDenied
	if errTask == nil {
		taskDenied["status"] = "FAIL"
		taskDenied["count"] = len(taskWorklist.Approvals)
		stand.record(t, "tool-approval-hitl", "FAIL", checks)
		t.Fatal("a task token read the human approvals worklist")
	}

	var worklist struct {
		Approvals []map[string]any `json:"approvals"`
	}
	var pending map[string]any
	found := false
	// The request is registered by the refusal above, so the worklist is polled
	// briefly rather than assumed to be instantly visible.
	for attempt := 0; attempt < 10 && !found; attempt++ {
		if err := stand.call(stand.HTTP, "/u-ahp/v1/tools/Approvals", map[string]any{"limit": 50}, human, &worklist); err != nil {
			checks["employee_worklist"] = map[string]any{"status": "FAIL", "http": status(err),
				"code": errorCode(err), "message": truncate(errText(err), 200)}
			stand.record(t, "tool-approval-hitl", "FAIL", checks)
			t.Fatalf("the employee could not read their own approval worklist: %v", errText(err))
		}
		for _, item := range worklist.Approvals {
			// Match this scenario's own held call by its operation key, not by
			// tool name: other lane scenarios legitimately leave record_note
			// requests pending in the same tenant worklist.
			opKey, _ := item["operation_key"].(string)
			if item["tool"] == "record_note" && strings.Contains(opKey, callKey) {
				pending = item
				found = true
				break
			}
		}
		if !found {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if !found {
		checks["employee_worklist"] = map[string]any{"status": "FAIL", "pending": len(worklist.Approvals)}
		stand.record(t, "tool-approval-hitl", "FAIL", checks)
		t.Fatal("no pending approval for the held side effect reached the employee")
	}
	checks["employee_worklist"] = map[string]any{"status": "PASS", "pending": len(worklist.Approvals),
		"request_fields": sortedKeys(pending), "route": "/u-ahp/v1/tools/Approvals"}

	// (c) Someone else in the same tenant, without tools:approve, cannot sign it.
	adminHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	err = stand.call(stand.HTTP, "/u-ahp/v1/tools/Approve", map[string]any{
		"request_id": field(pending, "request_id", "id"), "digest": field(pending, "digest"),
		"tool": "record_note"}, adminHuman, nil)
	unauthorised := map[string]any{"status": "PASS", "http": status(err), "code": errorCode(err)}
	checks["unauthorised_approver_refused"] = unauthorised
	if err == nil {
		unauthorised["status"] = "FAIL"
		unauthorised["note"] = "a principal without tools:approve signed a human approval"
		stand.record(t, "tool-approval-hitl", "FAIL", checks)
		t.Fatal("a principal without tools:approve signed a human approval")
	}

	// (d) The employee signs it.
	var approved struct {
		Approved   bool   `json:"approved"`
		ApprovedBy string `json:"approved_by"`
		Digest     string `json:"digest"`
	}
	signed := map[string]any{"status": "PASS", "approved": approved.Approved}
	checks["employee_signed"] = signed
	if err := stand.call(stand.HTTP, "/u-ahp/v1/tools/Approve", map[string]any{
		"request_id": field(pending, "request_id", "id"), "digest": field(pending, "digest"),
		"tool": "record_note"}, human, &approved); err != nil {
		signed["status"] = "FAIL"
		signed["http"] = status(err)
		signed["code"] = errorCode(err)
		signed["message"] = truncate(errText(err), 200)
		stand.record(t, "tool-approval-hitl", "FAIL", checks)
		t.Fatalf("the employee could not approve their own held action: %v", errText(err))
	}
	signed["digest_matches_request"] = approved.Digest == field(pending, "digest")

	// (e) The retried call now performs the effect exactly once, in the notes
	// store of the lane stand, and the durable approval journal shows the
	// signature as consumed.
	var operation struct {
		Status string `json:"status"`
		Digest string `json:"digest"`
		Key    string `json:"key"`
	}
	retryErr := stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": callKey,
		"tool": "record_note", "arguments": arguments}, token, &operation)
	effect := map[string]any{"status": "PASS", "operation_status": operation.Status, "operation_key": operation.Key,
		"error": truncate(errText(retryErr), 200)}
	checks["effect_after_approval"] = effect
	if retryErr != nil || operation.Status != "SUCCEEDED" {
		effect["status"] = "FAIL"
		stand.record(t, "tool-approval-hitl", "FAIL", checks)
		t.Fatalf("the approved call did not complete: status=%q err=%v", operation.Status, errText(retryErr))
	}

	// What the machine face answers for a tool call is a content-free operation
	// view (internal/tools/toolsapi view()): the bytes of a tool result reach the
	// model inside a run, not through this face. So the durable proof of the
	// effect is the connector's own store: the lane's ahp-note-mcp keeps its notes
	// in notes/notes.jsonl of the instance directory.
	var listed map[string]any
	contentFree := map[string]any{"status": "PASS"}
	if err := stand.call(stand.HTTP, "/v1/tools/Call", map[string]any{"key": task + "-list",
		"tool": "list_notes", "arguments": map[string]any{}}, token, &listed); err != nil {
		contentFree["status"] = "PARTIAL"
		contentFree["error"] = truncate(errText(err), 200)
	} else {
		observed, _ := json.Marshal(listed)
		contentFree["operation_fields"] = sortedKeys(listed)
		contentFree["carries_tool_output"] = strings.Contains(string(observed), stamp)
	}
	checks["machine_call_reply_shape"] = contentFree

	visible := map[string]any{"status": "PASS"}
	store, err := os.ReadFile(filepath.Join(stand.Dir, "notes", "notes.jsonl"))
	if err != nil {
		visible["status"] = "FAIL"
		visible["error"] = truncate(err.Error(), 160)
	} else {
		visible["connector_store_bytes"] = len(store)
		visible["approved_note_written"] = strings.Contains(string(store), stamp)
		if !visible["approved_note_written"].(bool) {
			visible["status"] = "FAIL"
		}
	}
	checks["effect_visible_in_connector"] = visible

	var consumed bool
	var approvals int
	_ = stand.Pool.QueryRow(context.Background(),
		`SELECT count(*), coalesce(bool_and(consumed_at IS NOT NULL), false)
		   FROM ahp_tools_all.approvals WHERE tenant=$1 AND tool='record_note'`,
		stand.Scope["tenant"]).Scan(&approvals, &consumed)
	checks["approval_journal"] = map[string]any{"status": map[bool]string{true: "PASS", false: "PARTIAL"}[approvals > 0],
		"rows": approvals, "signature_consumed": consumed,
		"note": "the journal is the Tool GW's own ledger of who signed which canonical digest"}

	for _, name := range []string{"effect_visible_in_connector"} {
		if value, ok := checks[name].(map[string]any); ok && value["status"] == "FAIL" {
			stand.record(t, "tool-approval-hitl", "FAIL", checks)
			t.Fatalf("%s: %v", name, value)
		}
	}
	stand.record(t, "tool-approval-hitl", "PASS", checks)
}

func field(item map[string]any, names ...string) string {
	for _, name := range names {
		if value, ok := item[name].(string); ok && value != "" {
			return value
		}
	}
	return ""
}

func sortedKeys(item map[string]any) []string {
	keys := make([]string, 0, len(item))
	for key := range item {
		keys = append(keys, key)
	}
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return keys
}
