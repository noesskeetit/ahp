package space_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// recExchange records the SERVER-DERIVED authority task id the Space edge asked to exchange
// for, so a test can prove the exchange authority is derived from the verified principal and
// NEVER from a caller-chosen body/path value (the deriveTaskID regression root removed).
type recExchange struct {
	mu    sync.Mutex
	tasks []string
}

func (e *recExchange) Exchange(_ context.Context, h space.HumanPrincipal, task string) (string, string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tasks = append(e.tasks, task)
	return "TASK:" + h.Subject, task, nil
}

func (e *recExchange) last() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.tasks) == 0 {
		return ""
	}
	return e.tasks[len(e.tasks)-1]
}

// newAuthorityEdge builds the production HTTP edge: authPerSubject injects the verified
// principal, ExchangeMiddleware performs the human->task exchange with a recording exchanger,
// and the handler runs with RequireScopedTask (the browser human JWT cannot reach State).
func newAuthorityEdge(t *testing.T, ex space.TaskExchanger) (*httptest.Server, *recExchange) {
	t.Helper()
	ms := newMockState()
	msrv := ms.Server()
	t.Cleanup(msrv.Close)
	sc := space.NewStateClient(msrv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w-auth"})
	base := space.ExchangeMiddleware(ex, authPerSubject)
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: base, RequireScopedTask: true})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs, ex.(*recExchange)
}

// TestExchangeAuthorityIsServerDerived proves that even a LEGITIMATE body field (context_id)
// and attacker-chosen identifiers never become the exchange authority: the exchanged task is
// the server-derived "space-authority:<subject>:<deployment>:<session>", scoped to the verified
// subject from the auth layer, regardless of anything in the body.
func TestExchangeAuthorityIsServerDerived(t *testing.T) {
	ex := &recExchange{}
	hs, rec := newAuthorityEdge(t, ex)
	body := map[string]any{
		"key": "k1", "context_id": "CTX-ATTACK", "input": "hi",
	}
	code, b := postSpaceAs(t, hs.URL, "AcceptTurn", body, "s1")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn %d: %s", code, b)
	}
	got := rec.last()
	if got != "space-authority:s1:d1:" {
		t.Fatalf("exchange authority must be server-derived from the verified subject, got %q", got)
	}
	if strings.Contains(got, "CTX-ATTACK") {
		t.Fatalf("body context_id leaked into the exchange authority: %q", got)
	}
}

// TestUnknownAuthorityFieldsRejected proves the caller cannot inject tenant/subject/task via
// the body at all: strict decode rejects unknown fields before the handler runs (defense in
// depth behind the server-derived authority).
func TestUnknownAuthorityFieldsRejected(t *testing.T) {
	ex := &recExchange{}
	hs, _ := newAuthorityEdge(t, ex)
	body := map[string]any{"key": "k1", "context_id": "c1", "input": "x", "subject": "victim", "task": "TASK-ATTACK"}
	code, b := postSpaceAs(t, hs.URL, "AcceptTurn", body, "s1")
	if code != http.StatusBadRequest {
		t.Fatalf("unknown authority fields must be rejected by strict decode (400), got %d: %s", code, b)
	}
}

// TestListChatsEmptyBodyAuthorized proves the first-screen read that has NO body fields still
// succeeds: the authority task is non-empty by construction, so ListChats {} is not refused
// with an empty-task error.
func TestListChatsEmptyBodyAuthorized(t *testing.T) {
	ex := &recExchange{}
	hs, rec := newAuthorityEdge(t, ex)
	code, b := postSpaceAs(t, hs.URL, "ListChats", map[string]any{}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListChats {} must be authorized (200), got %d: %s", code, b)
	}
	if got := rec.last(); !strings.HasPrefix(got, "space-authority:") {
		t.Fatalf("ListChats must carry a non-empty server authority, got %q", got)
	}
}

// TestOversizedBodyRejected proves FIX3: a body over the 4 MiB cap is answered 413 and is never
// truncated into the strict decoder (which could otherwise accept a valid JSON prefix and hide
// smuggled trailing bytes).
func TestOversizedBodyRejected(t *testing.T) {
	ex := &recExchange{}
	hs, _ := newAuthorityEdge(t, ex)
	// Build a valid JSON prefix padded well past the 4 MiB limit.
	pad := strings.Repeat("a", (4<<20)+1024)
	payload, _ := json.Marshal(map[string]any{"key": "k", "context_id": "c", "input": pad})
	req, _ := http.NewRequest(http.MethodPost, hs.URL+"/v1/space/AcceptTurn", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Test-Subject", "s1")
	req.Header.Set("Authorization", "Bearer tok-s1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("oversized post: %v", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized body must be 413, got %d: %s", resp.StatusCode, data)
	}
	if !strings.Contains(string(data), "BODY_TOO_LARGE") {
		t.Fatalf("expected BODY_TOO_LARGE code, got %s", data)
	}
}
