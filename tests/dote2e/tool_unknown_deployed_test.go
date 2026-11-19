package dote2e_test

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// External effect is the real note connector's fsynced ledger. A bounded
// transport proxy discards only its first successful reply, not the effect.
func TestToolUnknownGuardSurvivesBackendRestartAndNewTask(t *testing.T) {
	control := os.Getenv("AHP_DOT_E2E_LOST_ACK_CONTROL")
	if control == "" || os.Getenv("AHP_DOT_E2E_ALLOW_RESTART") != "1" {
		t.Skip("SKIP_EXPLICIT[tag=dot_e2e_optin] reason=lost_ack_probe_not_enabled missing=AHP_DOT_E2E_LOST_ACK_CONTROL,AHP_DOT_E2E_ALLOW_RESTART -- no external-effect/restart proof without own configured transport probe")
	}
	raw, err := os.ReadFile(control)
	if err != nil {
		t.Fatal(err)
	}
	var probe struct {
		Marker  string `json:"marker"`
		Receipt string `json:"receipt"`
		Result  string `json:"result"`
	}
	if err = json.Unmarshal(raw, &probe); err != nil || !strings.HasPrefix(probe.Marker, "ahp-lost-ack-") {
		t.Fatal("own transport probe required")
	}
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	suffix := time.Now().UTC().Format("150405.000000")
	token, task := stand.taskGrantContext(t, stand.HTTP, human, "unknown-"+suffix, "unknown-ctx-"+suffix, []string{"tools", "state"})
	key := task + "-note"
	args := map[string]any{"note": probe.Marker}
	request := func(key string) map[string]any {
		return map[string]any{"key": key, "tool": "record_note", "arguments": args}
	}
	approve := func(key string) {
		t.Helper()
		var list struct {
			Approvals []map[string]any `json:"approvals"`
		}
		if e := stand.call(stand.HTTP, "/u-ahp/v1/tools/Approvals", map[string]any{"limit": 50}, human, &list); e != nil {
			t.Fatal(e)
		}
		for _, item := range list.Approvals {
			if item["operation_key"] == key {
				if e := stand.call(stand.HTTP, "/u-ahp/v1/tools/Approve", map[string]any{"request_id": field(item, "request_id", "id"), "digest": field(item, "digest"), "tool": "record_note"}, human, nil); e != nil {
					t.Fatal(e)
				}
				return
			}
		}
		t.Fatal("own approval absent")
	}
	count := func() int {
		t.Helper()
		data, e := os.ReadFile(filepath.Join(stand.Dir, "notes/notes.jsonl"))
		if e != nil {
			t.Fatal(e)
		}
		n := 0
		for _, line := range strings.Split(string(data), "\n") {
			var note struct {
				Text string `json:"text"`
			}
			if json.Unmarshal([]byte(line), &note) == nil && note.Text == probe.Marker {
				n++
			}
		}
		return n
	}
	if e := stand.call(stand.HTTP, "/v1/tools/Call", request(key), token, nil); status(e) != 409 {
		t.Fatalf("human gate missing: %v", e)
	}
	approve(key)
	var first struct {
		Status string `json:"status"`
	}
	if e := stand.call(stand.HTTP, "/v1/tools/Call", request(key), token, &first); e != nil || first.Status != "SIDE_EFFECT_UNKNOWN" {
		t.Fatalf("committed lost reply must be unknown: status=%s error=%v", first.Status, e)
	}
	if n := count(); n != 1 {
		t.Fatalf("external effect count after lost reply=%d", n)
	}
	var capture struct {
		DropCount      int `json:"drop_count"`
		ForwardedCalls int `json:"forwarded_calls"`
		UpstreamStatus int `json:"upstream_status"`
	}
	data, e := os.ReadFile(probe.Receipt)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &capture); e != nil || capture.DropCount != 1 || capture.ForwardedCalls != 1 || capture.UpstreamStatus != 200 {
		t.Fatal("lost ACK was not independently observed")
	}
	if _, e = restartLaneRole(stand, "backend"); e != nil {
		t.Fatal("owned backend restart failed")
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		req, _ := http.NewRequest(http.MethodGet, stand.Origin+"/healthz", nil)
		res, e := stand.HTTP.Do(req)
		if e == nil {
			res.Body.Close()
			if res.StatusCode == 200 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("restarted backend unavailable")
		}
		time.Sleep(200 * time.Millisecond)
	}
	var stored struct {
		Status string `json:"status"`
	}
	if e = stand.call(stand.HTTP, "/v1/tools/Get", map[string]any{"key": key}, token, &stored); e != nil || stored.Status != "SIDE_EFFECT_UNKNOWN" {
		t.Fatal("durable UNKNOWN lost across restart", e, stored.Status)
	}
	var replay struct {
		Status string `json:"status"`
	}
	replayErr := stand.call(stand.HTTP, "/v1/tools/Call", request(key), token, &replay)
	if replayErr != nil || replay.Status != "SIDE_EFFECT_UNKNOWN" {
		t.Errorf("same-key replay hid durable uncertainty: status=%s http=%d error=%v", replay.Status, status(replayErr), replayErr)
	}
	nextToken, nextTask := stand.taskGrantContext(t, stand.HTTP, human, "unknown-next-"+suffix, "unknown-next-ctx-"+suffix, []string{"tools", "state"})
	nextKey := nextTask + "-note"
	if e = stand.call(stand.HTTP, "/v1/tools/Call", request(nextKey), nextToken, nil); status(e) != 409 {
		t.Fatal("new intent bypassed approval", e)
	}
	approve(nextKey)
	guardErr := stand.call(stand.HTTP, "/v1/tools/Call", request(nextKey), nextToken, nil)
	if status(guardErr) != 409 || !strings.Contains(errText(guardErr), "guard") {
		t.Errorf("new task/key bypassed unknown guard: %v", guardErr)
	}
	n := count()
	if n != 1 {
		t.Errorf("duplicate business effect after recovery: %d", n)
	}
	data, e = os.ReadFile(probe.Receipt)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(data, &capture); e != nil || capture.ForwardedCalls != 1 {
		t.Error("recovery sent another mutation to connector")
	}
	result := map[string]any{"status": map[bool]string{true: "failed", false: "passed"}[t.Failed()], "marker": probe.Marker, "external_effect_count": n, "first_status": first.Status, "recovered_status": stored.Status, "same_key_replay_status": replay.Status, "same_key_replay_http": status(replayErr), "new_task_guard_http": status(guardErr), "new_task_guard_code": errorCode(guardErr), "new_task_distinct": nextTask != task, "new_context_distinct": true, "backend_process_restarted": true, "fault_proxy": capture, "real_model_used": false, "external_owner": "real fsynced MCP notes service; controlled note only", "reconciled": false, "guard_left_held": true}
	output, _ := json.MarshalIndent(result, "", "  ")
	if e = os.WriteFile(probe.Result, output, 0600); e != nil {
		t.Fatal(e)
	}
}
