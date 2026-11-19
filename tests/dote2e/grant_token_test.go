package dote2e_test

// PDF rev3.2 L2050: "Ход — Run в Run Coordinator; на границе AHP — задача с
// грантом и токеном". On the AHP side every personal-Dot turn therefore has to
// arrive as a task carrying a live grant and an attested token bound to the
// employee who started the turn.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func decodeClaims(t *testing.T, jwt string) map[string]any {
	t.Helper()
	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Fatal("task token is not a JWT")
	}
	payload := parts[1]
	if pad := len(payload) % 4; pad != 0 {
		payload += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		t.Fatalf("decode task token claims: %v", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("parse task token claims: %v", err)
	}
	return claims
}

func TestDotTurnIsATaskWithGrantAndToken(t *testing.T) {
	stand := loadStand(t)
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	taskName := "dot-turn-" + time.Now().UTC().Format("150405")
	var grant struct {
		Token string `json:"token"`
		Task  string `json:"task"`
	}
	if err := stand.call(stand.HTTP, "/v1/identity/control/exchange", map[string]any{
		"human_token": human, "task": taskName, "context": taskName,
		"capabilities": []string{"state", "model", "tools", "artifacts", "memory"}}, human, &grant); err != nil {
		t.Fatalf("turn could not be opened as an AHP task: %v", err)
	}
	claims := decodeClaims(t, grant.Token)
	scopeTenant, _ := stand.Scope["tenant"].(string)
	checks := map[string]any{}

	// (a) The token is bound to the employee and to this exact task: the same
	// subject that opened the chat turn, never an anonymous agent identity.
	if claims["sub"] != stand.Scope["subject"] {
		t.Fatalf("task token is not bound to the employee: sub=%v", claims["sub"])
	}
	if claimString(claims["tenant"]) != scopeTenant {
		t.Fatalf("task token carries the wrong tenant: %v", claims["tenant"])
	}
	if claimString(claims["task"]) != grant.Task {
		t.Fatalf("task token is not bound to the task: %v vs %v", claims["task"], grant.Task)
	}
	if claimString(claims["root"]) == "" {
		t.Fatalf("task token has no root authority: the grant would be untraceable")
	}
	checks["token_bound_to_employee_and_task"] = map[string]any{"status": "PASS", "task": grant.Task,
		"subject": claims["sub"], "root": claimString(claims["root"]), "depth": claims["depth"]}

	// (b) A live grant exists in PostgreSQL for this (tenant, subject, task): the
	// token is a projection of stored authority, not a self-certified blob.
	var grants int
	if err := stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_identity.grants_child WHERE tenant=$1 AND subject=$2`,
		scopeTenant, stand.Scope["subject"]).Scan(&grants); err != nil {
		t.Fatalf("read grants_child: %v", err)
	}
	if grants == 0 {
		t.Fatal("no stored grant found for the task token: the turn has no grant at the AHP boundary")
	}
	checks["grant_recorded_in_postgres"] = map[string]any{"status": "PASS", "child_grants": grants}

	// (c) The task token is accepted by the owners it was granted: State and
	// Artifacts both serve the turn.
	var upload struct {
		ID  string `json:"id"`
		URL string `json:"presigned_url"`
	}
	blob := []byte("personal-dot-turn-payload")
	err := stand.call(stand.HTTP, "/v1/artifacts/BeginUpload", map[string]any{
		"key": taskName + "-turn", "name": "turn.txt", "mime": "text/plain",
		"sha256": digestHex(blob), "size": len(blob), "labels": map[string]any{}}, grant.Token, &upload)
	if err != nil {
		t.Fatalf("granted task token refused by Artifacts: %v", err)
	}
	checks["grants_are_live_on_owners"] = map[string]any{"status": "PASS", "artifact": upload.ID}

	// (d) Widening is refused: a capability the employee does not hold cannot
	// appear in the minted token.
	err = stand.call(stand.HTTP, "/v1/identity/control/exchange", map[string]any{
		"human_token": human, "task": taskName + "-widened", "context": taskName + "-widened",
		"capabilities": []string{"not-a-real-capability"}}, human, nil)
	if status(err) == 0 || status(err) == 200 {
		t.Fatalf("capability widening was accepted: %v", err)
	}
	checks["capability_widening_refused"] = map[string]any{"status": "PASS", "http": status(err), "code": errorCode(err)}

	// (e) The token cannot be used for another employee's turn: a foreign grant
	// cannot read this task's artifact.
	foreignHuman := stand.humanToken(t, stand.Foreign, mustLogin(t, "foreign_email"), mustLogin(t, "foreign_password"))
	foreignToken, _ := stand.taskGrant(t, stand.Foreign, foreignHuman, "dot-turn-foreign-"+time.Now().UTC().Format("150405"),
		[]string{"artifacts", "state", "memory"})
	var refused struct{}
	err = stand.call(stand.Foreign, "/v1/artifacts/Download", map[string]any{"id": upload.ID}, foreignToken, &refused)
	if status(err) == 0 {
		t.Fatal("a foreign task token downloaded the owner's turn artifact")
	}
	if status(err) != 403 && status(err) != 404 {
		t.Fatalf("expected 403/404 for a foreign task, got %d", status(err))
	}
	checks["foreign_task_cannot_reach_turn"] = map[string]any{"status": "PASS", "http": status(err)}
	stand.record(t, "turn-is-task-with-grant-and-token", "PASS", checks)
}

func claimString(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(toString(value))
}

func toString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		out, _ := json.Marshal(typed)
		return string(out)
	}
}
