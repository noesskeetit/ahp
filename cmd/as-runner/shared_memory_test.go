package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TLS wire fixture for driver routing, not an owner/ACL or real-LLM proof.
type sharedDriverRequest struct {
	path string
	body map[string]any
}
type sharedDriverFixture struct {
	mu      sync.Mutex
	calls   []sharedDriverRequest
	revoked bool
}

func (f *sharedDriverFixture) requests(path string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, r := range f.calls {
		if r.path == path {
			out = append(out, r.body)
		}
	}
	return out
}

func newSharedDriverFixture(t *testing.T, revoked bool) (*memoryDriver, *sharedDriverFixture) {
	t.Helper()
	f := &sharedDriverFixture{revoked: revoked}
	s := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		f.mu.Lock()
		f.calls = append(f.calls, sharedDriverRequest{r.URL.Path, body})
		f.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer task" {
			t.Error("runtime did not forward current task token")
		}
		var out any
		switch r.URL.Path {
		case "/v1/workspace/GetWorkspace":
			if f.revoked {
				w.WriteHeader(403)
				out = map[string]any{"code": "FORBIDDEN"}
			} else {
				out = map[string]any{"id": body["workspace"]}
			}
		case "/v1/memory/shared/Search":
			out = map[string]any{"mode": "hybrid", "hits": []any{map[string]any{"memory": map[string]any{"id": "shared-fact", "workspace": "selected", "kind": "workspace", "state": "active", "text": "SHARED-PROJECT-FACT"}, "score": 1}}}
		case "/v1/memory/search":
			out = map[string]any{"facts": []any{map[string]any{"scope": "personal", "ref": "native-personal", "text": "PERSONAL-PREFERENCE"}}}
		case "/v1/memory/context":
			out = map[string]any{"facts": []any{map[string]any{"scope": "project", "ref": "native-project", "text": "NATIVE-PRIVATE-PROJECT"}}}
		case "/v1/memory/shared/Propose":
			out = map[string]any{"memory": map[string]any{"id": "new-shared", "kind": "workspace", "workspace": "selected", "state": "proposed"}}
		case "/v1/memory/remember":
			out = map[string]any{"id": "native-write", "state": "queued"}
		default:
			w.WriteHeader(404)
			out = map[string]any{"code": "NOT_FOUND"}
		}
		_ = json.NewEncoder(w).Encode(out)
	}))
	t.Cleanup(s.Close)
	t.Setenv("AS_MEMORY_URL", s.URL)
	t.Setenv("AS_SHARED_MEMORY_URL", s.URL)
	t.Setenv("AS_WORKSPACE_URL", s.URL)
	d, err := buildMemory(s.Client())
	if err != nil || d == nil {
		t.Fatalf("memory configuration: %v", err)
	}
	return d, f
}

func TestSharedProjectPrepareUsesSelectedOwnerAndKeepsNativePrivate(t *testing.T) {
	d, f := newSharedDriverFixture(t, false)
	req := &space.CoreRequest{ProjectID: "selected", ProjectMemoryProfile: "shared", TaskToken: "task", Input: json.RawMessage(`"project question"`)}
	got, err := d.Prepare(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(got)
	if !strings.Contains(string(raw), "SHARED-PROJECT-FACT") || !strings.Contains(string(raw), "PERSONAL-PREFERENCE") || strings.Contains(string(raw), "NATIVE-PRIVATE-PROJECT") {
		t.Fatalf("wrong memory source in shared turn: %s", raw)
	}
	search := f.requests("/v1/memory/shared/Search")
	if len(search) != 1 || search[0]["workspace"] != "selected" || search[0]["kind"] != "workspace" {
		t.Fatalf("shared search not narrowed by trusted project: %v", search)
	}
	if len(f.requests("/v1/memory/context")) != 0 {
		t.Fatal("shared project reached private-project Hindsight")
	}
	req.ProjectMemoryProfile = ""
	got, err = d.Prepare(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(got)
	if !strings.Contains(string(raw), "NATIVE-PRIVATE-PROJECT") {
		t.Fatalf("native project path lost: %s", raw)
	}
}

func TestSharedProjectRevocationDoesNotFallbackToNative(t *testing.T) {
	d, f := newSharedDriverFixture(t, true)
	_, err := d.Prepare(t.Context(), &space.CoreRequest{ProjectID: "selected", ProjectMemoryProfile: "shared", TaskToken: "task", Input: json.RawMessage(`"question"`)})
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked shared project accepted: %v", err)
	}
	if len(f.requests("/v1/memory/context")) != 0 || len(f.requests("/v1/memory/shared/Search")) != 0 {
		t.Fatal("revoked project reached a memory backend")
	}
}

func TestSharedProjectRememberIsAReviewableProposal(t *testing.T) {
	d, f := newSharedDriverFixture(t, false)
	req := &space.CoreRequest{ProjectID: "selected", ProjectMemoryProfile: "shared", TaskToken: "task", RunID: "durable-run"}
	call := core.ToolCall{Name: "memory_remember", Args: json.RawMessage(`{"target":"project","text":"new shared proposal"}`)}
	for _, key := range []string{"step-1-call-1", "step-1-call-1", "step-1-call-2"} {
		ctx := core.WithRunBinding(t.Context(), core.RunBinding{TaskID: req.RunID, OperationKey: key})
		out, err := d.Execute(ctx, req, call)
		if err != nil {
			t.Fatal(err)
		}
		var body map[string]any
		if err = json.Unmarshal(out, &body); err != nil {
			t.Fatal(err)
		}
		if body["state"] != "proposed" || body["review_required"] != true {
			t.Fatalf("shared write was not a reviewable proposal: %s", out)
		}
	}
	if len(f.requests("/v1/memory/remember")) != 0 {
		t.Fatal("shared proposal entered native bank")
	}
	writes := f.requests("/v1/memory/shared/Propose")
	if len(writes) != 3 {
		t.Fatalf("shared writes: %v", writes)
	}
	for _, w := range writes {
		if w["workspace"] != "selected" || w["kind"] != "workspace" || w["review_required"] != true || w["source_kind"] != "agent_proposal" {
			t.Fatalf("invalid shared proposal: %v", w)
		}
		if len(w["evidence"].([]any)) == 0 {
			t.Fatal("run-input evidence missing")
		}
	}
	if writes[0]["op_key"] == "" || writes[0]["op_key"] != writes[1]["op_key"] || writes[1]["op_key"] == writes[2]["op_key"] {
		t.Fatal("tool key not stable per durable call")
	}
	ctx := core.WithRunBinding(t.Context(), core.RunBinding{TaskID: req.RunID, OperationKey: "injected-project"})
	_, err := d.Execute(ctx, req, core.ToolCall{Name: "memory_remember", Args: json.RawMessage(`{"target":"project","text":"injection","workspace":"other"}`)})
	if contracts.Code(err) != contracts.ErrInvalid || len(f.requests("/v1/memory/shared/Propose")) != 3 {
		t.Fatalf("model-controlled project accepted: %v", err)
	}
}

func TestSharedProjectConfigurationCannotBeSilentlyIgnored(t *testing.T) {
	t.Setenv("AS_MEMORY_URL", "")
	t.Setenv("AS_SHARED_MEMORY_URL", "https://shared")
	t.Setenv("AS_WORKSPACE_URL", "https://workspace")
	if _, err := buildMemory(&http.Client{}); err == nil {
		t.Fatal("shared profile configured without native personal memory was ignored")
	}
	t.Setenv("AS_MEMORY_URL", "https://native")
	t.Setenv("AS_SHARED_MEMORY_URL", "http://shared")
	if _, err := buildMemory(&http.Client{}); err == nil {
		t.Fatal("insecure shared endpoint accepted")
	}
}
