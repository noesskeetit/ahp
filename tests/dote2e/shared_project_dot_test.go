package dote2e_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	modelclient "github.com/ahp-platform/ahp/sdk/go/model"
	"github.com/ahp-platform/ahp/sdk/go/sharedmemory"
)

// Actual Dot/model/Memory/Identity/Workspace/State. Data is a new synthetic
// corpus; no existing personal bank is copied into a shared project.
func TestDotSharedProjectMemoryAndRevocation(t *testing.T) {
	stand := loadStand(t)
	controlPath := os.Getenv("AHP_DOT_E2E_SHARED_MEMORY_CONTROL")
	if controlPath == "" || os.Getenv("AHP_DOT_E2E_ALLOW_RESTART") != "1" {
		t.Skip("SKIP_ENV[tag=dot_e2e_layout] reason=shared_project_restart_control_not_supplied missing=AHP_DOT_E2E_SHARED_MEMORY_CONTROL,AHP_DOT_E2E_ALLOW_RESTART")
	}
	var control struct {
		CuratorUsername string `json:"curator_username"`
		CuratorPassword string `json:"curator_password"`
		CuratorSubject  string `json:"curator_subject"`
		MemberSubject   string `json:"member_subject"`
	}
	raw, err := os.ReadFile(controlPath)
	if err != nil || json.Unmarshal(raw, &control) != nil || control.MemberSubject == "" || control.CuratorSubject == "" {
		t.Fatal("private shared-project control is invalid")
	}
	checks := map[string]any{}
	defer func() {
		verdict := "PASS"
		if t.Failed() {
			verdict = "FAIL"
			checks["test_failure"] = map[string]any{"status": "FAIL"}
		}
		stand.record(t, "dot-shared-project-memory", verdict, checks)
	}()
	stamp := strconv.FormatInt(time.Now().UnixNano(), 10)
	ownerHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	memberHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	curatorHuman := stand.humanToken(t, stand.HTTP, control.CuratorUsername, control.CuratorPassword)
	ownerToken, _ := stand.taskGrant(t, stand.HTTP, ownerHuman, "shared-project-owner-"+stamp, []string{"workspace", "state", "memory"})
	adminToken, _ := stand.taskGrant(t, stand.HTTP, memberHuman, "shared-project-config-"+stamp, []string{"registry", "registry:admin", "state", "model"})
	var previousConfig json.RawMessage
	var previousRevision int64
	if err = stand.Pool.QueryRow(t.Context(), `SELECT revision,data FROM ahp_registry.configs WHERE tenant=$1 AND agent_id=$2 ORDER BY revision DESC LIMIT 1`, stand.Scope["tenant"], dotAgentID).Scan(&previousRevision, &previousConfig); err != nil {
		t.Fatal("read own current AgentConfig")
	}
	instructions := "Shared memory acceptance " + stamp + ". Use the employee's requested memory tools exactly once when asked; project writes are proposals pending review. For a release-code question, return only the code from retrieved current-project memory."
	publishDotAgentConfig(t, stand, "lane-qwen", []string{"memory_search", "memory_remember", "memory_correct"}, instructions, 8)
	defer func() {
		var revision int64
		var current json.RawMessage
		if err := stand.Pool.QueryRow(context.Background(), `SELECT revision,data FROM ahp_registry.configs WHERE tenant=$1 AND agent_id=$2 ORDER BY revision DESC LIMIT 1`, stand.Scope["tenant"], dotAgentID).Scan(&revision, &current); err != nil {
			t.Error("read AgentConfig for restoration")
			return
		}
		var data map[string]any
		if json.Unmarshal(current, &data) != nil {
			t.Error("invalid current AgentConfig")
			return
		}
		if data["instructions"] != instructions {
			checks["config_restore"] = map[string]any{"status": "PASS", "concurrent_external_change_preserved": true}
			return
		}
		if err := stand.call(stand.HTTP, "/v1/registry/PutConfig", map[string]any{"agent_id": dotAgentID, "expected_revision": revision, "data": previousConfig}, adminToken, nil); err != nil {
			t.Error("restore own previous AgentConfig refused")
			return
		}
		checks["config_restore"] = map[string]any{"status": "PASS", "previous_data_restored": true, "previous_revision": previousRevision}
	}()
	projects := make([]string, 2)
	codes := []string{"SHARED-A-" + stamp, "SHARED-B-" + stamp}
	records := make([]string, 2)
	for index := range projects {
		var project struct {
			ID string `json:"id"`
		}
		if err = stand.call(stand.HTTP, "/v1/workspace/Create", map[string]any{"workspace": "dot-shared-" + strconv.Itoa(index) + "-" + stamp}, ownerToken, &project); err != nil || project.ID == "" {
			t.Fatal("create own shared workspace")
		}
		projects[index] = project.ID
		for _, subject := range []string{control.MemberSubject, control.CuratorSubject} {
			if err = stand.call(stand.HTTP, "/v1/workspace/AddMember", map[string]any{"workspace": project.ID, "subject": subject}, ownerToken, nil); err != nil {
				t.Fatal("add own test project member")
			}
			defer func(project, subject string) {
				_ = stand.call(stand.HTTP, "/v1/workspace/RemoveMember", map[string]any{"workspace": project, "subject": subject}, ownerToken, nil)
			}(project.ID, subject)
		}
		seedToken, _ := stand.taskGrant(t, stand.HTTP, curatorHuman, "shared-seed-"+strconv.Itoa(index)+"-"+stamp, []string{"memory", "memory:review", "workspace", "state"})
		client, e := sharedmemory.New(sharedmemory.Config{BaseURL: stand.Origin, HTTPClient: stand.HTTP, TokenSource: func(context.Context) (string, error) { return seedToken, nil }})
		if e != nil {
			t.Fatal(e)
		}
		p, e := client.Propose(t.Context(), "seed-"+strconv.Itoa(index)+"-"+stamp, sharedmemory.ProposeRequest{Kind: sharedmemory.Workspace, Workspace: project.ID, Text: "The release code of this project is " + codes[index] + ".", SourceKind: "user_statement", Evidence: []sharedmemory.Evidence{{SourceKind: "user_statement", Detail: "Synthetic project-specific acceptance code"}}})
		if e != nil {
			t.Fatal("seed own shared fact")
		}
		if _, e = client.Review(t.Context(), "seed-review-"+strconv.Itoa(index)+"-"+stamp, p.Memory.ID, "approve"); e != nil {
			t.Fatal("curator activation refused")
		}
		records[index] = p.Memory.ID
	}
	type acceptedTurn struct {
		TurnID   string `json:"turn_id"`
		ThreadID string `json:"thread_id"`
		Profile  string `json:"project_memory_profile"`
	}
	contextID := "shared-project-dialog-" + stamp
	start := func(key, prompt string, previous *acceptedTurn) acceptedTurn {
		t.Helper()
		body := map[string]any{"key": key + "-" + stamp, "context_id": contextID, "input": map[string]string{"role": "user", "content": prompt}, "max_steps": 8, "project_id": projects[0]}
		if previous == nil {
			body["project_memory_profile"] = "shared"
		} else {
			body["thread_id"] = previous.ThreadID
		}
		var turn acceptedTurn
		if err := stand.call(stand.HTTP, "/v1/space/AcceptTurn", body, memberHuman, &turn); err != nil {
			t.Fatalf("shared project AcceptTurn: %v", err)
		}
		if turn.TurnID == "" || turn.Profile != "shared" {
			t.Fatal("shared project profile missing from accepted turn")
		}
		return turn
	}
	finish := func(turn acceptedTurn) (string, []string) {
		t.Helper()
		deadline := time.Now().Add(180 * time.Second)
		var state struct {
			Status  string `json:"status"`
			Failure string `json:"failure_reason"`
			Profile string `json:"project_memory_profile"`
			Project string `json:"project_id"`
		}
		for {
			if err := stand.call(stand.HTTP, "/v1/space/GetTurn", map[string]string{"turn_id": turn.TurnID}, memberHuman, &state); err != nil {
				t.Fatal("read shared turn")
			}
			if state.Status == "done" {
				break
			}
			if state.Status == "failed" || state.Status == "canceled" || time.Now().After(deadline) {
				t.Fatalf("shared turn status=%s reason=%s", state.Status, state.Failure)
			}
			time.Sleep(time.Second)
		}
		if state.Profile != "shared" || state.Project != projects[0] {
			t.Fatal("durable shared binding lost")
		}
		var ids []string
		var task string
		var generations, usage int
		if err := stand.Pool.QueryRow(t.Context(), `SELECT count(g.id),coalesce(sum(g.usage_total),0),array_agg(g.id ORDER BY g.created_at,g.id),min(r.task_id) FROM ahp_state.runs r JOIN ahp_state.threads t ON t.id=r.thread_id JOIN ahp_model.generations g ON g.tenant=t.tenant AND g.task=r.task_id WHERE r.id=$1 AND t.tenant=$2`, turn.TurnID, stand.Scope["tenant"]).Scan(&generations, &usage, &ids, &task); err != nil || generations < 1 || usage < 1 {
			t.Fatalf("actual model generation proof missing: generations=%d usage=%d err=%v", generations, usage, err)
		}
		// Model output is task-bound. Use the PDF workload-only renewal route
		// for that exact own run; an unrelated root grant must remain refused.
		var credential struct {
			Token string `json:"token"`
		}
		if err := stand.call(stand.HTTP, "/v1/identity/tasks/"+url.PathEscape(task)+"/issue-token", map[string]any{}, "", &credential); err != nil || credential.Token == "" {
			t.Fatal("reissue exact own run token for model verification")
		}
		modelReader, err := modelclient.New(modelclient.Config{BaseURL: stand.Origin, HTTPClient: stand.HTTP, TokenSource: func(context.Context) (string, error) { return credential.Token, nil }})
		if err != nil {
			t.Fatal(err)
		}
		var output strings.Builder
		for _, id := range ids {
			g, err := modelReader.Get(t.Context(), id)
			if err != nil {
				t.Fatalf("canonical model output read failed: %v", err)
			}
			output.WriteString(g.Output)
			output.WriteByte('\n')
		}
		var stream struct {
			Events []struct {
				Kind string `json:"kind"`
			} `json:"events"`
		}
		if err := stand.call(stand.HTTP, "/v1/space/ReadStream", map[string]any{"thread_id": turn.ThreadID, "after": 0}, memberHuman, &stream); err != nil {
			t.Fatal("read durable shared transcript")
		}
		kinds := make([]string, 0, len(stream.Events))
		terminal := false
		for _, e := range stream.Events {
			kinds = append(kinds, e.Kind)
			terminal = terminal || e.Kind == "done" || e.Kind == "status"
		}
		if !terminal {
			t.Fatal("shared turn has no terminal stream event")
		}
		return output.String(), kinds
	}
	first := start("shared-recall", "What is the release code recorded in this project's memory? Return the code only.", nil)
	output, kinds := finish(first)
	if !strings.Contains(output, codes[0]) || strings.Contains(output, codes[1]) {
		t.Fatal("Dot did not retrieve only its selected project's code")
	}
	digest := sha256.Sum256([]byte(output))
	checks["selected_project_real_model"] = map[string]any{"status": "PASS", "turn": first.TurnID, "thread": first.ThreadID, "selected_workspace": projects[0], "excluded_workspace": projects[1], "selected_code_in_output": true, "other_code_absent": true, "output_sha256": hex.EncodeToString(digest[:]), "stream_kinds": kinds}
	if _, err = restartLaneRole(stand, "runner"); err != nil {
		t.Fatal("restart exact owned runner")
	}
	ready := false
	for deadline := time.Now().Add(45 * time.Second); time.Now().Before(deadline); time.Sleep(time.Second) {
		if e := stand.call(stand.HTTP, "/v1/space/GetTurn", map[string]string{"turn_id": first.TurnID}, memberHuman, nil); e == nil {
			ready = true
			break
		}
	}
	if !ready {
		t.Fatal("restarted runner unavailable")
	}
	cold := start("shared-cold-recall", "What is our project's release code? Return only the stored code.", &first)
	output, kinds = finish(cold)
	if !strings.Contains(output, codes[0]) || strings.Contains(output, codes[1]) {
		t.Fatal("cold shared recall crossed project scope")
	}
	checks["profile_restored_after_runner_restart"] = map[string]any{"status": "PASS", "profile_inherited_without_request_override": true, "turn": cold.TurnID, "stream_kinds": kinds}
	proposalText := "Synthetic shared proposal " + stamp
	written := start("shared-write", `Call memory_remember exactly once with target project and text "`+proposalText+`". Do not approve it. Then say that review is required.`, &first)
	_, kinds = finish(written)
	var proposalID, proposalState string
	var review bool
	var proposalCount int
	if err = stand.Pool.QueryRow(t.Context(), `SELECT id,state,review_required,count(*) OVER () FROM ahp_memory.memories WHERE tenant=$1 AND workspace=$2 AND text=$3 AND provenance->>'run_id'=$4`, stand.Scope["tenant"], projects[0], proposalText, written.TurnID).Scan(&proposalID, &proposalState, &review, &proposalCount); err != nil || proposalState != "proposed" || !review || proposalCount != 1 {
		t.Fatal("Dot did not persist an explicit reviewable project proposal")
	}
	checks["runtime_proposal"] = map[string]any{"status": "PASS", "record": proposalID, "state": proposalState, "review_required": review, "turn": written.TurnID, "stream_kinds": kinds}
	correctedText := "The release code of this project is SHARED-CORRECTION-" + stamp + "."
	corrected := start("shared-correct", `Call memory_correct exactly once with scope project, memory_ref "ahp-shared-memory://`+records[0]+`", text "`+correctedText+`", and reason "synthetic acceptance correction". Do not approve it. Then say review is required.`, &first)
	_, kinds = finish(corrected)
	var supersedes string
	if err = stand.Pool.QueryRow(t.Context(), `SELECT id,state,review_required,supersedes,count(*) OVER () FROM ahp_memory.memories WHERE tenant=$1 AND workspace=$2 AND text=$3 AND provenance->>'run_id'=$4`, stand.Scope["tenant"], projects[0], correctedText, corrected.TurnID).Scan(&proposalID, &proposalState, &review, &supersedes, &proposalCount); err != nil || proposalState != "proposed" || !review || supersedes != records[0] || proposalCount != 1 {
		t.Fatal("Dot did not persist scoped correction as a proposal")
	}
	var originalState string
	if err = stand.Pool.QueryRow(t.Context(), `SELECT state FROM ahp_memory.memories WHERE id=$1 AND tenant=$2`, records[0], stand.Scope["tenant"]).Scan(&originalState); err != nil || originalState != "active" {
		t.Fatal("unreviewed correction changed the active source")
	}
	checks["runtime_correction"] = map[string]any{"status": "PASS", "record": proposalID, "source": supersedes, "source_still_active": true, "turn": corrected.TurnID, "stream_kinds": kinds}
	if err = stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{"key": "switch-profile-" + stamp, "context_id": contextID, "thread_id": first.ThreadID, "project_id": projects[0], "project_memory_profile": "native", "input": "switch"}, memberHuman, nil); status(err) != 409 {
		t.Fatalf("existing conversation silently changed profile: %v", err)
	}
	if err = stand.call(stand.HTTP, "/v1/workspace/RemoveMember", map[string]any{"workspace": projects[0], "subject": control.MemberSubject}, ownerToken, nil); err != nil {
		t.Fatal("revoke own project member")
	}
	var before, after int
	if err = stand.Pool.QueryRow(t.Context(), `SELECT count(*) FROM ahp_state.runs WHERE thread_id=$1`, first.ThreadID).Scan(&before); err != nil {
		t.Fatal(err)
	}
	err = stand.call(stand.HTTP, "/v1/space/AcceptTurn", map[string]any{"key": "revoked-project-" + stamp, "context_id": contextID, "thread_id": first.ThreadID, "input": "Continue this project."}, memberHuman, nil)
	if status(err) != 403 && status(err) != 404 {
		t.Fatalf("old human token continued a revoked project: %v", err)
	}
	if err = stand.Pool.QueryRow(t.Context(), `SELECT count(*) FROM ahp_state.runs WHERE thread_id=$1`, first.ThreadID).Scan(&after); err != nil || after != before {
		t.Fatal("revoked project created a new run")
	}
	checks["revoked_existing_conversation"] = map[string]any{"status": "PASS", "same_human_token": true, "no_new_run": true, "profile_switch_refused": true}
}
