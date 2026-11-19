package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/internal/memory/longterm/sdk"
)

type backend struct {
	mu     sync.Mutex
	facts  map[string][]memory.NativeFact
	states map[string]string
	calls  int
}

func (b *backend) Configure(context.Context, memory.Bank) error { return nil }
func (b *backend) Retain(_ context.Context, bank memory.Bank, j memory.Job) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	b.states[j.ID] = "completed"
	b.facts[bank.ID] = append(b.facts[bank.ID], memory.NativeFact{ID: "1f35d949-7728-44a6-a699-a71010805d3a", Text: j.Content, Type: "world"})
	return nil
}
func (b *backend) Status(_ context.Context, bank, id string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if state, ok := b.states[id]; ok {
		return state, nil
	}
	return "not_found", nil
}
func (b *backend) Cancel(context.Context, string, string) error { return nil }
func (b *backend) Recall(_ context.Context, bank memory.Bank, _ string, _ int) ([]memory.NativeFact, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.facts[bank.ID], nil
}
func (b *backend) Correct(context.Context, memory.Bank, memory.Job) error { return nil }
func (b *backend) DeleteBank(_ context.Context, id string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.facts, id)
	return nil
}

type handlerTransport struct{ handler http.Handler }

func (t handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.handler.ServeHTTP(rec, r)
	response := rec.Result()
	response.Request = r
	return response, nil
}

type harness struct {
	s       *memory.Service
	b       *backend
	handler http.Handler
	secret  string
	owner   string
}

func setup(t *testing.T) *harness {
	t.Helper()
	secret := "01234567890123456789012345678901"
	access := devAccess{Token: secret}
	b := &backend{facts: map[string][]memory.NativeFact{}, states: map[string]string{}}
	s := memory.New(memory.NewMemoryStore(), b, access)
	s.Config.Poll = time.Nanosecond
	id, err := access.Authorize(context.Background(), secret+".runtime", memory.AccessRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{s, b, New(s), secret, id.Owner()}
}
func (h *harness) client(kind string) *sdk.Client {
	c := sdk.New("http://memory.test", func(context.Context) (string, error) { return h.secret + "." + kind, nil })
	c.HTTP.Transport = handlerTransport{h.handler}
	return c
}
func (h *harness) request(t *testing.T, method, path, kind, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if kind != "" {
		r.Header.Set("Authorization", "Bearer "+h.secret+"."+kind)
	}
	r.Header.Set("Idempotency-Key", key)
	rec := httptest.NewRecorder()
	h.handler.ServeHTTP(rec, r)
	return rec
}
func (h *harness) work(t *testing.T) {
	t.Helper()
	for n := 0; n < 4; n++ {
		if err := h.s.WorkOwner(context.Background(), h.owner); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSDKRememberSearchAndOperationFlow(t *testing.T) {
	h := setup(t)
	c := h.client("agent")
	req := memory.RememberRequest{ProjectID: "alpha", Target: memory.Project, Text: "Use gRPC", SourceRefs: []string{"user-message-1"}}
	op, err := c.Remember(context.Background(), "remember-1", req)
	if err != nil || op.State != "queued" {
		t.Fatal(op, err)
	}
	again, err := c.Remember(context.Background(), "remember-1", req)
	if err != nil || again.ID != op.ID {
		t.Fatal(again, err)
	}
	h.work(t)
	state, err := c.Operation(context.Background(), op.ID, "alpha", memory.Project)
	if err != nil || state.State != "succeeded" {
		t.Fatal(state, err)
	}
	facts, err := c.Search(context.Background(), memory.SearchRequest{ProjectID: "alpha", Scope: "project", Query: "protocol"})
	if err != nil || len(facts.Facts) != 1 || facts.Facts[0].Text != "Use gRPC" {
		t.Fatal(facts, err)
	}
	other, err := c.Search(context.Background(), memory.SearchRequest{ProjectID: "beta", Scope: "project", Query: "protocol"})
	if err != nil || len(other.Facts) != 0 {
		t.Fatal(other, err)
	}
	_, err = c.Operation(context.Background(), op.ID, "beta", memory.Project)
	if err == nil {
		t.Fatal("wrong project operation disclosed")
	}
	if h.b.calls != 1 {
		t.Fatal("duplicate retain")
	}
}
func TestRuntimeEventPreservesSourceTextAndScope(t *testing.T) {
	h := setup(t)
	h.s.Config.ProjectBatch = 1
	c := h.client("runtime")
	req := memory.TurnRequest{ProjectID: "alpha", SessionID: "s", Sequence: 1, Sanitized: true, Sources: []memory.Source{{Ref: "finding", Role: "assistant", Text: "verified discovery", At: time.Now().UTC(), AllowedScopes: []memory.Scope{memory.Project}}}}
	if err := c.TurnCompleted(context.Background(), "turn-1", req); err != nil {
		t.Fatal(err)
	}
	h.work(t)
	facts, err := c.Search(context.Background(), memory.SearchRequest{ProjectID: "alpha", Scope: "project", Query: "discovery"})
	if err != nil || len(facts.Facts) != 1 || !strings.Contains(facts.Facts[0].Text, "verified discovery") {
		t.Fatal(facts, err)
	}
	if h.b.calls != 1 {
		t.Fatal("project-only source was sent elsewhere")
	}
}
func TestAgentCannotManageSettingsOrForgeRuntimeEvents(t *testing.T) {
	h := setup(t)
	for _, req := range []struct{ method, path, body string }{{"PATCH", "/v1/memory/settings", `{"personal_enabled":false}`}, {"POST", "/v1/memory/clear", `{"scope":"personal"}`}, {"POST", "/internal/v1/turns", `{"project_id":"a","session_id":"s","sequence":1,"sanitized":true,"sources":[]}`}} {
		rec := h.request(t, req.method, req.path, "agent", "key", req.body)
		if rec.Code != 403 {
			t.Fatal(rec.Code, rec.Body.String())
		}
	}
}
func TestAPIRejectsMissingAuthArbitraryBanksAndMultipleJSONObjects(t *testing.T) {
	h := setup(t)
	if rec := h.request(t, "POST", "/v1/memory/search", "", "", `{"query":"x","scope":"personal"}`); rec.Code != 401 {
		t.Fatal(rec.Code)
	}
	for _, body := range []string{`{"target":"personal","text":"x","source_refs":["x"],"bank_id":"foreign"}`, `{"target":"personal","text":"x","source_refs":["x"]} {}`} {
		rec := h.request(t, "POST", "/v1/memory/remember", "agent", "key", body)
		if rec.Code != 400 {
			t.Fatal(rec.Code, rec.Body.String())
		}
	}
}
func TestConsoleCanDisableWithoutDeleting(t *testing.T) {
	h := setup(t)
	rec := h.request(t, "PATCH", "/v1/memory/settings", "console", "disable", `{"personal_enabled":false}`)
	if rec.Code != 200 {
		t.Fatal(rec.Code, rec.Body.String())
	}
	_, err := h.client("agent").Remember(context.Background(), "disabled", memory.RememberRequest{Target: memory.Personal, Text: "x", SourceRefs: []string{"x"}})
	if err == nil {
		t.Fatal("write to disabled memory accepted")
	}
	rec = h.request(t, "GET", "/v1/memory/settings", "console", "", "")
	if rec.Code != 200 || !bytes.Contains(rec.Body.Bytes(), []byte(`"personal_enabled":false`)) {
		t.Fatal(rec.Code, rec.Body.String())
	}
}
func TestReadinessAndLivenessDoNotRequireUserToken(t *testing.T) {
	h := setup(t)
	for _, path := range []string{"/livez", "/readyz"} {
		rec := h.request(t, "GET", path, "", "", "")
		if rec.Code != 200 {
			t.Fatal(rec.Code)
		}
	}
}
func TestToolDispatchBindsProjectAndSourceReferences(t *testing.T) {
	h := setup(t)
	c := h.client("agent")
	raw := json.RawMessage(`{"text":"current project decision","target":"project","project_id":"attacker"}`)
	out, err := c.Invoke(context.Background(), "memory_remember", raw, "tool-call", "alpha", []string{"runtime-source"})
	if err != nil {
		t.Fatal(err)
	}
	if out.(memory.Operation).State != "queued" {
		t.Fatal(out)
	}
	h.work(t)
	facts, err := c.Search(context.Background(), memory.SearchRequest{ProjectID: "alpha", Scope: "project", Query: "decision"})
	if err != nil || len(facts.Facts) != 1 {
		t.Fatal(facts, err)
	}
}
