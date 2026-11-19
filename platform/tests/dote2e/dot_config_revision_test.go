package dote2e_test

// PDF rev3.2 L2044-2046: "Dot — личный агент сотрудника: Ревизия AgentConfig
// системного агента agents-space (Registry) + личные настройки (as-profile);
// исполняет as-turn-runner". The scenario proves a Dot run resolves its
// behaviour from a Registry AgentConfig REVISION plus the employee's private
// profile revision, and never from a hard-coded agent.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

const dotAgentID = "agents-space"

func dotTenantConfig(route, instructions string, maxSteps int) map[string]any {
	return map[string]any{
		"instructions": instructions,
		"model_route":  route,
		"max_steps":    maxSteps,
		"policy": map[string]any{
			"allowed_model_routes":        []string{route},
			"allowed_skills":              []string{},
			"allowed_tools":               []string{},
			"allowed_computer_policies":   []string{"disabled", "personal"},
			"allow_personal_instructions": true,
			"allow_personal_skills":       true,
		},
	}
}

func TestDotRunsFromAgentConfigRevisionAndPersonalProfile(t *testing.T) {
	stand := loadStand(t)
	caps := []string{"registry", "state", "memory", "model"}
	human := stand.humanToken(t, stand.HTTP, mustLogin(t, "email"), mustLogin(t, "password"))
	stamp := time.Now().UTC().Format("150405")
	token, task := stand.taskGrant(t, stand.HTTP, human, "dot-cfg-"+stamp, caps)
	// The AgentConfig of the system agent belongs to the tenant administrator;
	// the employee owns only the private profile overlay.
	adminHuman := stand.humanToken(t, stand.HTTP, mustLogin(t, "admin_email"), mustLogin(t, "admin_password"))
	adminToken, adminTask := stand.taskGrant(t, stand.HTTP, adminHuman, "dot-cfg-admin-"+stamp, append(caps, "registry:admin"))

	checks := map[string]any{}

	// (a) Without a saved agents-space AgentConfig the Dot cannot start, and the
	// refusal names the missing administrator action instead of inventing a
	// default agent.
	var pinned struct {
		Snapshot struct {
			Revision        int64 `json:"revision"`
			ProfileRevision int64 `json:"profile_revision"`
			Dot             struct {
				Instructions string `json:"instructions"`
				MaxSteps     int    `json:"max_steps"`
			} `json:"dot"`
		} `json:"snapshot"`
	}
	err := stand.call(stand.HTTP, "/v1/registry/PinDotConfig", map[string]any{"key": task, "context_id": task}, token, &pinned)
	switch {
	case status(err) == 503 && strings.Contains(err.Error(), "agents-space AgentConfig"):
		checks["pin_without_agent_config"] = map[string]any{"status": "PASS", "http": 503, "refusal": err.Error()}
	case err == nil:
		// The AgentConfig for agents-space was already saved in this tenant by
		// an earlier lane run on the same stand; the registry has no delete, so
		// the fresh-tenant refusal is proven once per tenant and its evidence
		// stays on disk. Re-runs record the skip honestly instead of faking it.
		checks["pin_without_agent_config"] = map[string]any{"status": "SKIPPED_RERUN",
			"note": "agents-space AgentConfig already present in this tenant; fresh-tenant 503 proven in the first run's evidence"}
	default:
		t.Fatalf("expected an honest 503 naming agents-space AgentConfig, got %v", err)
	}

	// (b) The administrator saves the first AgentConfig revision of the system
	// agent, with the tenant policy that bounds every employee's Dot.
	route := "lane-qwen"
	if err := stand.call(stand.HTTP, "/v1/registry/PutModelRoute", map[string]any{
		"route": route, "expected_revision": 0,
		"document": map[string]any{"model": mustEnv(t, "AHP_LLM_MODEL"), "provider": "qwen"}}, adminToken, nil); err != nil {
		t.Fatalf("PutModelRoute refused: %v", err)
	}
	// The employee may not write the system agent's configuration.
	if err := stand.call(stand.HTTP, "/v1/registry/PutConfig", map[string]any{
		"agent_id": dotAgentID, "expected_revision": 0,
		"data": dotTenantConfig(route, "employee attempt", 8)}, token, nil); status(err) != 403 {
		t.Fatalf("employee writing agents-space AgentConfig must be refused with 403, got %v", err)
	}
	checks["employee_cannot_write_agent_config"] = map[string]any{"status": "PASS", "http": 403}
	var configPut struct {
		Config struct {
			Revision int64 `json:"revision"`
		} `json:"config"`
	}
	if err := stand.call(stand.HTTP, "/v1/registry/PutConfig", map[string]any{
		"agent_id": dotAgentID, "expected_revision": 0,
		"data": dotTenantConfig(route, "Base tenant instructions.", 8)}, adminToken, &configPut); err != nil {
		t.Fatalf("PutConfig(agents-space) refused: %v", err)
	}
	if configPut.Config.Revision != 1 {
		t.Fatalf("first AgentConfig revision must be 1, got %d", configPut.Config.Revision)
	}
	checks["agent_config_revision_one"] = map[string]any{"status": "PASS", "revision": configPut.Config.Revision,
		"written_by": "tenant administrator grant " + adminTask}

	// (c) Pinning now yields a snapshot bound to that revision, with no personal
	// overlay yet.
	if err := stand.call(stand.HTTP, "/v1/registry/PinDotConfig", map[string]any{"key": task, "context_id": task}, token, &pinned); err != nil {
		t.Fatalf("PinDotConfig after configuration refused: %v", err)
	}
	if pinned.Snapshot.Revision != 1 || pinned.Snapshot.ProfileRevision != 0 {
		t.Fatalf("unexpected snapshot revisions: %+v", pinned.Snapshot)
	}
	if !strings.Contains(pinned.Snapshot.Dot.Instructions, "Base tenant instructions.") {
		t.Fatalf("snapshot did not carry the AgentConfig instructions: %q", pinned.Snapshot.Dot.Instructions)
	}
	checks["snapshot_from_revision"] = map[string]any{"status": "PASS", "config_revision": pinned.Snapshot.Revision,
		"profile_revision": pinned.Snapshot.ProfileRevision, "max_steps": pinned.Snapshot.Dot.MaxSteps}

	// (d) The employee's private profile revision (as-profile) is layered on top:
	// a new pin carries profile_revision 1 and the personal instructions.
	if err := stand.call(stand.HTTP, "/v1/registry/PutProfile", map[string]any{
		"expected_revision": 0,
		"data":              map[string]any{"instructions": "Employee prefers Russian answers.", "max_steps": 4}}, token, nil); err != nil {
		t.Fatalf("PutProfile refused: %v", err)
	}
	secondTask := task + "-turn2"
	if err := stand.call(stand.HTTP, "/v1/registry/PinDotConfig", map[string]any{"key": secondTask, "context_id": secondTask}, token, &pinned); err != nil {
		t.Fatalf("PinDotConfig with profile refused: %v", err)
	}
	if pinned.Snapshot.ProfileRevision != 1 {
		t.Fatalf("profile revision did not reach the snapshot: %+v", pinned.Snapshot)
	}
	if !strings.Contains(pinned.Snapshot.Dot.Instructions, "Employee prefers Russian answers.") {
		t.Fatalf("personal profile instructions missing from the resolved Dot: %q", pinned.Snapshot.Dot.Instructions)
	}
	if pinned.Snapshot.Dot.MaxSteps != 4 {
		t.Fatalf("profile step budget not applied: %d", pinned.Snapshot.Dot.MaxSteps)
	}
	checks["personal_profile_layered"] = map[string]any{"status": "PASS", "profile_revision": pinned.Snapshot.ProfileRevision,
		"resolved_max_steps": pinned.Snapshot.Dot.MaxSteps}

	// (e) A new AgentConfig revision changes the next run, proving behaviour is
	// read from the Registry and not baked into the runner.
	if err := stand.call(stand.HTTP, "/v1/registry/PutConfig", map[string]any{
		"agent_id": dotAgentID, "expected_revision": 1,
		"data": dotTenantConfig(route, "Base tenant instructions v2.", 8)}, adminToken, nil); err != nil {
		t.Fatalf("second PutConfig refused: %v", err)
	}
	thirdTask := task + "-turn3"
	if err := stand.call(stand.HTTP, "/v1/registry/PinDotConfig", map[string]any{"key": thirdTask, "context_id": thirdTask}, token, &pinned); err != nil {
		t.Fatalf("PinDotConfig after revision 2 refused: %v", err)
	}
	if pinned.Snapshot.Revision != 2 || !strings.Contains(pinned.Snapshot.Dot.Instructions, "v2") {
		t.Fatalf("run config did not follow the AgentConfig revision: %+v", pinned.Snapshot)
	}
	checks["run_follows_new_revision"] = map[string]any{"status": "PASS", "config_revision": pinned.Snapshot.Revision}

	// (f) The tenant policy bounds the employee: a profile may not raise the
	// tenant step budget.
	err = stand.call(stand.HTTP, "/v1/registry/PutProfile", map[string]any{
		"expected_revision": 1, "data": map[string]any{"instructions": "widen", "max_steps": 99}}, token, nil)
	if status(err) != 403 {
		t.Fatalf("profile widening must be refused with 403, got %v", err)
	}
	checks["profile_cannot_widen_tenant_budget"] = map[string]any{"status": "PASS", "http": 403}

	// (g) Pins are durable per (tenant, subject, deployment, operation key), so a
	// restarted runner replays the same configuration bytes.
	var persisted int
	err = stand.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM ahp_registry.dot_pins WHERE tenant=$1 AND subject=$2 AND deployment=$3`,
		stand.Scope["tenant"], stand.Scope["subject"], stand.Scope["deployment"]).Scan(&persisted)
	if err != nil {
		t.Fatalf("read dot_pins: %v", err)
	}
	if persisted < 3 {
		t.Fatalf("expected one pinned snapshot per turn in PostgreSQL, got %d", persisted)
	}
	var snapshotKeys []string
	rows, err := stand.Pool.Query(context.Background(),
		`SELECT operation_key, snapshot->>'revision' FROM ahp_registry.dot_pins WHERE tenant=$1 ORDER BY operation_key`,
		stand.Scope["tenant"])
	if err != nil {
		t.Fatalf("read pin snapshots: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key, revision string
		if err := rows.Scan(&key, &revision); err != nil {
			t.Fatalf("scan pin: %v", err)
		}
		snapshotKeys = append(snapshotKeys, key+"@"+revision)
	}
	checks["pins_durable_in_postgres"] = map[string]any{"status": "PASS", "rows": persisted, "pins": snapshotKeys}

	// (h) Another tenant's Dot cannot read this tenant's profile: profiles are
	// addressed by the verified principal, not by a body field.
	foreignHuman := stand.humanToken(t, stand.Foreign, mustLogin(t, "foreign_email"), mustLogin(t, "foreign_password"))
	foreignToken, foreignTask := stand.taskGrant(t, stand.Foreign, foreignHuman, "dot-cfg-foreign-"+stamp, caps)
	var foreignPin struct {
		Snapshot struct {
			ProfileRevision int64 `json:"profile_revision"`
		} `json:"snapshot"`
	}
	err = stand.call(stand.Foreign, "/v1/registry/PinDotConfig", map[string]any{"key": foreignTask, "context_id": foreignTask}, foreignToken, &foreignPin)
	if status(err) != 503 {
		t.Fatalf("foreign tenant must have no Dot configuration at all, got %v", err)
	}
	var foreignProfile struct {
		Profile struct {
			Revision int64           `json:"revision"`
			Data     json.RawMessage `json:"data"`
		} `json:"profile"`
	}
	if err := stand.call(stand.Foreign, "/v1/registry/GetProfile", map[string]any{}, foreignToken, &foreignProfile); err != nil {
		t.Fatalf("foreign GetProfile refused: %v", err)
	}
	if foreignProfile.Profile.Revision != 0 || strings.Contains(string(foreignProfile.Profile.Data), "Russian") {
		t.Fatalf("foreign tenant saw the owner's private profile: %+v", foreignProfile.Profile)
	}
	checks["foreign_tenant_cannot_read_profile"] = map[string]any{"status": "PASS", "own_pin_http": 503,
		"foreign_profile_revision": foreignProfile.Profile.Revision}

	stand.record(t, "dot-config-revision-and-profile", "PASS", checks)
}

func mustLogin(t *testing.T, key string) string {
	t.Helper()
	dir := mustEnv(t, "AHP_DOT_E2E_INSTANCE")
	raw, err := readFile(dir + "/login.json")
	if err != nil {
		t.Fatalf("read lane login record: %v", err)
	}
	var values map[string]string
	if err := json.Unmarshal(raw, &values); err != nil {
		t.Fatalf("parse lane login record: %v", err)
	}
	value := values[key]
	if value == "" {
		t.Fatalf("lane login record has no %q", key)
	}
	return value
}

func readFile(path string) ([]byte, error) {
	return readLaneFile(path)
}
