package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// spyExchanger is a test double for the server->server Identity task exchange. It
// records the human subject it was asked to exchange for and returns a distinct
// task token, so a test can prove the Space edge used the EXCHANGED token rather
// than the browser's human JWT.
type spyExchanger struct {
	mu       sync.Mutex
	Subjects []string
	TTL      string
}

func (s *spyExchanger) Exchange(_ context.Context, h space.HumanPrincipal, _ string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Subjects = append(s.Subjects, h.Subject)
	tok := "TASK:" + h.Subject
	// issuedTask echoes a server-shaped authority id, mirroring Identity returning the
	// attenuated task the token is bound to.
	return tok, "space-authority:" + h.Subject + ":d1:", nil
}

// newExchangeSpace builds the real Space HTTP edge: auth injects the verified human
// principal (authPerSubject), ExchangeMiddleware performs the human->task exchange,
// and the handler runs with RequireScopedTask so the raw browser bearer can never
// be used as the State task token.
func newExchangeSpace(t *testing.T, ex space.TaskExchanger, requireScoped bool) (*httptest.Server, *spyExchanger) {
	t.Helper()
	ms := newMockState()
	msrv := ms.Server()
	t.Cleanup(msrv.Close)
	sc := space.NewStateClient(msrv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w-ex"})
	base := space.ExchangeMiddleware(ex, authPerSubject)
	if ex == nil {
		base = authPerSubject // no exchange: only valid with a trusted service bearer
	}
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: base, RequireScopedTask: requireScoped})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs, nil
}

// TestScopedTaskRequiredRejectsHumanJWT proves the production gate: with
// RequireScopedTask on and NO exchange performed, a browser human JWT forwarded as
// the bearer is REJECTED (never used to reach State). This is the boundary the
// frontend contract relies on: the UI's human token cannot talk to State directly.
func TestScopedTaskRequiredRejectsHumanJWT(t *testing.T) {
	hs, _ := newExchangeSpace(t, nil, true) // RequireScopedTask, but auth only injects principal (no exchange)
	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key": "ex-1", "context_id": "ctx-ex-1", "input": map[string]any{"role": "user", "content": "hi"},
	}, "alice")
	if code != http.StatusForbidden {
		t.Fatalf("human JWT with RequireScopedTask and no exchange = %d, want 403 (body=%s)", code, body)
	}
}

// TestExchangeMiddlewareHumanToTask proves that with the exchange wired, a browser
// human JWT is exchanged server-side for a scoped task token and the turn succeeds,
// and that the exchanger received the VERIFIED subject (not a browser-chosen one).
func TestExchangeMiddlewareHumanToTask(t *testing.T) {
	spy := &spyExchanger{}
	hs, _ := newExchangeSpace(t, spy, true)
	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key": "ex-2", "context_id": "ctx-ex-2", "input": map[string]any{"role": "user", "content": "hi"},
	}, "carol")
	if code != 200 {
		t.Fatalf("accept via exchange = %d, want 200 (body=%s)", code, body)
	}
	if spy.subjectCount("carol") != 1 {
		t.Fatalf("exchanger not invoked with verified subject carol: %+v", spy.snapshot())
	}
}

func (s *spyExchanger) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.Subjects...)
}
func (s *spyExchanger) subjectCount(sub string) int {
	n := 0
	for _, x := range s.snapshot() {
		if x == sub {
			n++
		}
	}
	return n
}

// TestExchangeMiddlewareNilExchangerFailClosed proves a human request that reached
// Space with a principal but no configured exchanger is refused (human JWT must not
// reach State), rather than silently forwarding the human bearer.
func TestExchangeMiddlewareNilExchangerFailClosed(t *testing.T) {
	ms := newMockState()
	msrv := ms.Server()
	t.Cleanup(msrv.Close)
	sc := space.NewStateClient(msrv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w-nil"})
	// ExchangeMiddleware wrapping authPerSubject, but exchanger is nil.
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: space.ExchangeMiddleware(nil, authPerSubject)})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{
		"key": "ex-3", "context_id": "ctx-ex-3", "input": map[string]any{"role": "user", "content": "hi"},
	}, "dave")
	if code != http.StatusForbidden {
		t.Fatalf("nil exchanger human request = %d, want 403 fail-closed (body=%s)", code, body)
	}
}

// TestIdentityGrantExchangerContract proves Space's CLIENT speaks the AUTHORITATIVE
// Identity exchange contract (decision 2026-10-30-human-task-exchange-wire-contract):
//
//	POST {human_token, task, context?, capabilities?} (STRICT: unknown fields => 400)
//	  ->  {token, task}
//
// and that Space does NOT send grant/subject/tenant/deployment/session_id/ttl_seconds
// and does NOT read the legacy "task_token". This is a wire-conformance test of OUR
// client against a faithful strict stub of the ACCEPTED Identity scheme — it does NOT
// import the foreign identityapi package (not in base 012a849) and is NOT the joint
// real-handler/PG/mTLS run (that is IV's composed-checkout gate).
func TestIdentityGrantExchangerContract(t *testing.T) {
	var gotPath string
	var gotAuth string
	// Stub implements EXACTLY the accepted scheme with strict decode.
	idSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var body struct {
			HumanToken   string   `json:"human_token"`
			Task         string   `json:"task"`
			Context      string   `json:"context"`
			Capabilities []string `json:"capabilities"`
		}
		if err := dec.Decode(&body); err != nil {
			http.Error(w, `{"error":"strict decode rejected"}`, http.StatusBadRequest)
			return
		}
		if body.HumanToken == "" || body.Task == "" {
			http.Error(w, `{"error":"human_token and task required"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"token":"scoped-task-token","task":"` + body.Task + `"}`))
	}))
	t.Cleanup(idSrv.Close)

	ex, err := space.NewIdentityGrantExchanger(space.GrantExchangerConfig{
		IdentityExchangeURL: idSrv.URL, // base URL; client must append /v1/identity/control/exchange
		Client:              idSrv.Client(),
	})
	if err != nil {
		t.Fatalf("NewIdentityGrantExchanger: %v", err)
	}
	tok, issuedTask, err := ex.Exchange(context.Background(),
		space.HumanPrincipal{Subject: "ada", HumanToken: "HUMAN-JWT-abc"}, "thread-77")
	if err != nil {
		t.Fatalf("Exchange: %v", err)
	}
	if tok != "scoped-task-token" {
		t.Fatalf("Exchange token = %q, want scoped-task-token (client must read the response field \"token\", not task_token)", tok)
	}
	// The issued task is the ATTENUATED task the token is bound to (from the response),
	// which Space uses as the Tier-2 delegate parent — never a broad/browser value.
	if issuedTask != "thread-77" {
		t.Fatalf("Exchange issuedTask = %q, want the server-issued task echoed in the response", issuedTask)
	}
	if gotPath != space.IdentityExchangePath {
		t.Fatalf("exchange path = %q, want %q", gotPath, space.IdentityExchangePath)
	}
	// Space authenticates as its OWN workload (mTLS), never by replaying a caller
	// bearer into the Authorization header.
	if gotAuth != "" {
		t.Fatalf("Space->Identity exchange leaked a caller bearer Authorization header: %q", gotAuth)
	}
	// Negative: legacy/mis-shaped response with only "task_token" (no "token") =>
	// the client must fail CLOSED, never forward a human JWT or return a stale token.
	legacy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"task_token":"OLD","expires_in":300}`))
	}))
	t.Cleanup(legacy.Close)
	lx, _ := space.NewIdentityGrantExchanger(space.GrantExchangerConfig{
		IdentityExchangeURL: legacy.URL, Client: legacy.Client(),
	})
	if _, _, err := lx.Exchange(context.Background(),
		space.HumanPrincipal{Subject: "ada", HumanToken: "H"}, "thread-77"); err == nil {
		t.Fatalf("Exchange on legacy task_token-only response = nil error, want fail-closed")
	}

	// Negative: server 403 => fail-closed.
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, `{"error":"forbidden"}`, http.StatusForbidden)
	}))
	t.Cleanup(bad.Close)
	bex, _ := space.NewIdentityGrantExchanger(space.GrantExchangerConfig{
		IdentityExchangeURL: bad.URL, Client: bad.Client(),
	})
	if _, _, err := bex.Exchange(context.Background(),
		space.HumanPrincipal{Subject: "ada", HumanToken: "H"}, "thread-77"); err == nil {
		t.Fatalf("Exchange on HTTP 403 = nil error, want fail-closed")
	}

	// Negative: a client-chosen scope smuggled into the body must be rejected by the
	// strict server. Here we assert that IF the body carried a forbidden field the
	// strict decode would 400 (i.e. the accepted scheme is strict), by calling the
	// stub directly with such a body.
	rawForbidden := `{"human_token":"H","task":"t","subject":"evil","tenant":"T","deployment":"D"}`
	resp, err := http.Post(idSrv.URL+space.IdentityExchangePath, "application/json", strings.NewReader(rawForbidden))
	if err != nil {
		t.Fatalf("post forbidden body: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("forbidden-field body to strict Identity = %d, want 400", resp.StatusCode)
	}

	_ = transport.WithPrincipal
}
