//go:build live

package emil

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestLiveEmilDelegation exercises the Emil adapter against a REAL Emil
// core-agent process over the real A2A wire. It is gated behind the `live`
// build tag so the default `go test ./...` run stays hermetic. Run with:
//
//	EMIL_BASE_URL=http://127.0.0.1:18000 go test -tags live ./adapters/emil/ -count=1
//
// This test is FAIL-CLOSED: if EMIL_BASE_URL is unset it fails (does not skip).
// In the isolated AHP acceptance environment Emil is backed by the native
// Qwen model and its builtin tools (incl. core_artifact_save) are active, so a
// content-producing prompt must reach state=completed and must record a real
// core_artifact_save tool-hop in provenance. Anything less is a real failure,
// not a skip.
func TestLiveEmilDelegation(t *testing.T) {
	base := os.Getenv("EMIL_BASE_URL")
	if base == "" {
		t.Fatal("EMIL_BASE_URL unset: live Emil test is fail-closed (no skip). Point it at the real core-agent, e.g. http://127.0.0.1:18000")
	}

	adapter, err := New(Config{BaseURL: base, CardCacheTTL: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const marker = "AHP-LIVE-EMIL-PROBE"
	prompt := "Use the core_artifact_save tool to save a text artifact named " +
		"live-probe.txt whose content is exactly " + marker +
		". Then reply with one short sentence confirming the save."

	res, prov, err := adapter.Delegate(ctx, prompt)
	if err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	if prov == nil {
		t.Fatal("nil provenance")
	}

	if prov.AgentName != "core-agent" {
		t.Errorf("agent name = %q, want core-agent", prov.AgentName)
	}
	if prov.AgentVersion == "" {
		t.Errorf("agent version empty")
	}
	if len(prov.AgentCardSHA256) != 64 {
		t.Errorf("agent card sha256 = %q, want 64 hex chars", prov.AgentCardSHA256)
	}
	if prov.TaskID == "" || prov.ContextID == "" {
		t.Errorf("missing ids: task=%q context=%q", prov.TaskID, prov.ContextID)
	}

	// The real Qwen-backed Emil must complete.
	if prov.TaskState != StateCompleted {
		t.Fatalf("task state = %q, want completed (Emil must be backed by a working model); err=%v", prov.TaskState, err)
	}
	if err != nil {
		t.Errorf("completed state returned error: %v", err)
	}
	if strings.TrimSpace(res.Text) == "" {
		t.Errorf("completed task produced empty reply text")
	}

	// Prove a REAL tool-hop happened: the agent must have invoked
	// core_artifact_save (not merely streamed a generated reply).
	var sawArtifactSave bool
	for _, tc := range prov.ToolCalls {
		if tc == "core_artifact_save" {
			sawArtifactSave = true
		}
	}
	if !sawArtifactSave {
		t.Errorf("no core_artifact_save tool-hop observed; tool_calls=%v (artifact tool must be active in the isolated AHP env)", prov.ToolCalls)
	}
	t.Logf("Emil live OK: task=%s context=%s state=%s tool_calls=%v reply=%q",
		prov.TaskID, prov.ContextID, prov.TaskState, prov.ToolCalls, truncateStr(res.Text, 120))

	if prov.AuthPresent || prov.AuthType != "none" {
		t.Errorf("unexpected auth: present=%v type=%q", prov.AuthPresent, prov.AuthType)
	}

	task, _, gerr := adapter.client.GetTask(ctx, prov.TaskID)
	if gerr != nil {
		t.Fatalf("GetTask(%s): %v", prov.TaskID, gerr)
	}
	if task.ID != prov.TaskID {
		t.Errorf("GetTask id = %q, want %q", task.ID, prov.TaskID)
	}
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
