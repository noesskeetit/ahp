package space_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// grantRec is a recording RunGrantIssuer double. It records the ordered delegate/cancel
// events and the exact arguments Space passed, so a test can prove the Tier-2 contract:
// Delegate is given the ingress authority's PARENT task and a server-owned CHILD task, it
// returns a DISTINCT child token, and Cancel is called with the grant's OWN scope + child task.
type grantRec struct {
	mu           sync.Mutex
	events       *[]string
	parent       string
	child        string
	childToken   string
	delegateN    int
	cancelN      int
	cancelToken  string
	cancelTask   string
	ctxClaim     string
	delegateFail bool
	cancelFail   bool
}

func (g *grantRec) event(s string) {
	if g.events != nil {
		*g.events = append(*g.events, s)
	}
}

func (g *grantRec) Delegate(_ context.Context, ingress, parent string, spec space.RunGrantSpec) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.delegateFail {
		return "", errors.New("delegate boom")
	}
	g.delegateN++
	g.parent = parent
	g.child = spec.Task
	g.ctxClaim = spec.Context
	g.event("delegate")
	return g.childToken, nil
}

func (g *grantRec) Cancel(_ context.Context, childToken, childTask string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.cancelN++
	g.cancelToken = childToken
	g.cancelTask = childTask
	if g.cancelFail {
		return errors.New("cancel boom")
	}
	return nil
}

// stateRec is a minimal State recorder that records the bearer + body each op was presented
// with, so a test can prove CreateRun was presented the CHILD token (not the ingress token)
// and that the durable task id is the child task.
type stateRec struct {
	events             *[]string
	createThreadBearer string
	createRunBearer    string
	createRunTaskID    string
	cancelBearer       string
	runStatus          string
}

func (s *stateRec) event(e string) {
	if s.events != nil {
		*s.events = append(*s.events, e)
	}
}

func writeStateJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func newStateRecorder(t *testing.T, events *[]string) (*httptest.Server, *stateRec) {
	t.Helper()
	sr := &stateRec{events: events, runStatus: "pending"}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/state/CreateThread", func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		sr.createThreadBearer = r.Header.Get("Authorization")
		cid, _ := b["context_id"].(string)
		writeStateJSON(w, map[string]any{"id": "thr-1", "context_id": cid})
	})
	mux.HandleFunc("/v1/state/GetThread", func(w http.ResponseWriter, r *http.Request) {
		// Continuing a conversation is how a browser sends the second message of a Dot
		// thread; the recorded thread keeps a context id distinct from its own id, so a
		// grant minted on this path is still testing the two different identities.
		writeStateJSON(w, map[string]any{"id": "thr-1", "context_id": "c1"})
	})
	mux.HandleFunc("/v1/state/CreateRun", func(w http.ResponseWriter, r *http.Request) {
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		sr.createRunBearer = r.Header.Get("Authorization")
		sr.createRunTaskID, _ = b["task_id"].(string)
		sr.event("create_run")
		writeStateJSON(w, map[string]any{"id": "run-1", "thread_id": "thr-1", "task_id": b["task_id"], "status": "pending", "sequence": 1})
	})
	mux.HandleFunc("/v1/state/GetRun", func(w http.ResponseWriter, r *http.Request) {
		writeStateJSON(w, map[string]any{"id": "run-1", "thread_id": "thr-1", "task_id": sr.createRunTaskID, "status": sr.runStatus})
	})
	mux.HandleFunc("/v1/state/CancelWaitingRun", func(w http.ResponseWriter, r *http.Request) {
		sr.cancelBearer = r.Header.Get("Authorization")
		sr.runStatus = "canceled"
		writeStateJSON(w, map[string]any{"ok": true})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, sr
}

// TestIdentityDelegateIssuerAtomic pins the FROZEN E1 delegate contract:
// POST /v1/identity/roots/{parent}/delegate bearer=<parent token> -> 201 {grant, token} in ONE
// call. The client must (a) present the parent token as bearer, (b) send ttl as a duration
// STRING and the server-owned child task in body.task, (c) reject a 200 (non-created) answer,
// (d) reject a 201 that carries no token, and (e) reject a child whose grant.task diverges from
// the requested id (guards against an over-privileged parent redirecting the run).
func TestIdentityDelegateIssuerAtomic(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	var delBody map[string]any
	var delAuth string
	var status = http.StatusCreated
	var body = `{"grant":{"task":"child-1"},"token":"CHILD-TOKEN-XYZ"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if !strings.HasSuffix(p, "/delegate") {
			mu.Lock()
			calls = append(calls, "OTHER:"+p)
			mu.Unlock()
			http.Error(w, "unexpected "+p, http.StatusBadRequest) // no separate mint allowed
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&delBody)
		delAuth = r.Header.Get("Authorization")
		mu.Lock()
		calls = append(calls, "delegate")
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	issuer, err := space.NewIdentityDelegateIssuer(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}
	tok, err := issuer.Delegate(context.Background(), "PARENT-TOKEN", "parent-task", space.RunGrantSpec{
		Task: "child-1", Context: "thr-1", Capabilities: []string{"state:write"}, TTL: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if tok != "CHILD-TOKEN-XYZ" {
		t.Fatalf("expected atomically minted child token, got %q", tok)
	}
	mu.Lock()
	got := append([]string(nil), calls...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "delegate" {
		t.Fatalf("E1 must be ONE delegate call (no separate mint), got %v", got)
	}
	if !strings.HasPrefix(delAuth, "Bearer PARENT-TOKEN") {
		t.Fatalf("delegate must present the parent (attenuated) token as bearer, got %q", delAuth)
	}
	if ttl, ok := delBody["ttl"].(string); !ok || ttl == "" {
		t.Fatalf("delegate ttl must be a duration string, got %#v", delBody["ttl"])
	}
	if delBody["task"] != "child-1" {
		t.Fatalf("delegate body.task must be the server-owned child id, got %#v", delBody["task"])
	}

	// (c) a 200 (not Created) answer fails closed.
	status, body = http.StatusOK, `{"grant":{"task":"child-1"},"token":"X"}`
	if _, err := issuer.Delegate(context.Background(), "P", "parent-task", space.RunGrantSpec{Task: "child-1", TTL: time.Minute}); err == nil {
		t.Fatal("expected reject of a non-201 delegate")
	}
	// (d) a 201 with no token fails closed.
	status, body = http.StatusCreated, `{"grant":{"task":"child-1"}}`
	if _, err := issuer.Delegate(context.Background(), "P", "parent-task", space.RunGrantSpec{Task: "child-1", TTL: time.Minute}); err == nil {
		t.Fatal("expected reject of a delegate with no child token")
	}
	// (e) grant.task diverging from the requested child id fails closed.
	status, body = http.StatusCreated, `{"grant":{"task":"OTHER"},"token":"X"}`
	if _, err := issuer.Delegate(context.Background(), "P", "parent-task", space.RunGrantSpec{Task: "child-1", TTL: time.Minute}); err == nil {
		t.Fatal("expected reject when the created child diverges from the requested task")
	}
}

// TestIdentityDelegateIssuerCancelTaskBound pins cancel to a TASK-BOUND control:
// POST /v1/identity/roots/{childTask}/cancel bearer=<child token> -> 2xx ok; a non-2xx answer
// fails closed (a revoked grant must not outlive a cancel that was rejected). The operator
// control/cancel-subtree route (cert allowlist) must NEVER be used by Space.
func TestIdentityDelegateIssuerCancelTaskBound(t *testing.T) {
	var gotAuth string
	gotPath := ""
	failNext := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if failNext {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	issuer, err := space.NewIdentityDelegateIssuer(srv.URL, srv.Client())
	if err != nil {
		t.Fatalf("issuer: %v", err)
	}
	if err := issuer.Cancel(context.Background(), "CHILD-TOKEN", "rtask-s1:k1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if gotPath != "/v1/identity/roots/rtask-s1:k1/cancel" {
		t.Fatalf("cancel must be the task-bound roots/{child}/cancel route, got %q", gotPath)
	}
	if gotAuth != "Bearer CHILD-TOKEN" {
		t.Fatalf("cancel must present the run's OWN child token as bearer, got %q", gotAuth)
	}
	// Empty bearer or empty task fails closed before any network call.
	if err := issuer.Cancel(context.Background(), "", "rtask-s1:k1"); err == nil {
		t.Fatal("expected fail-closed when no child token bearer is available")
	}
	if err := issuer.Cancel(context.Background(), "CHILD-TOKEN", ""); err == nil {
		t.Fatal("expected fail-closed when no durable child task ref is available")
	}
	// A rejected cancel (403) must fail closed (a live grant must not outlive a failed cancel).
	failNext = true
	if err := issuer.Cancel(context.Background(), "CHILD-TOKEN", "rtask-s1:k1"); err == nil {
		t.Fatal("expected fail-closed when the task-bound cancel is rejected (403)")
	}
}

// TestAcceptTurnChildGrantBeforeCreateRun proves the Tier-2 ORDER and binding: the child
// Delegate runs BEFORE CreateRun, CreateRun is PRESENTED the child token (State then binds
// Run.TaskID to it), the durable task id is the server-owned child id, and the ingress token
// is never presented to CreateRun.
func TestAcceptTurnChildGrantBeforeCreateRun(t *testing.T) {
	events := []string{}
	srv, sr := newStateRecorder(t, &events)
	sc := space.NewStateClient(srv.URL, nil)
	rec := &grantRec{events: &events, childToken: "CHILD-TOK"}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "w",
		RunGrantIssuer: rec, RequireRunGrant: true, RunGrantTTL: time.Minute,
	})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	ctx := space.WithAuthorityTask(context.Background(), "space-authority:s1:d1:")
	resp, err := coord.AcceptTurn(ctx, "INGRESS-TOK", scope, space.AcceptTurnRequest{
		Key: "k1", ContextID: "c1", Input: json.RawMessage(`"hi"`), MaxSteps: 5,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	// ORDER: delegate strictly precedes create_run.
	if len(events) < 2 || events[0] != "delegate" || events[1] != "create_run" {
		t.Fatalf("expected delegate before create_run, got %v", events)
	}
	// PARENT is the ingress authority task; CHILD is server-owned idempotent.
	if rec.parent != "space-authority:s1:d1:" {
		t.Fatalf("delegate parent must be the ingress authority task, got %q", rec.parent)
	}
	if rec.child != "rtask-s1:k1" {
		t.Fatalf("child task id must be server-owned rtask-<subject>:<key>, got %q", rec.child)
	}
	// CreateRun PRESENTED the child token, not the ingress token; task id is the child.
	if sr.createRunBearer != "Bearer CHILD-TOK" {
		t.Fatalf("CreateRun must present the child grant token, got %q", sr.createRunBearer)
	}
	if strings.Contains(sr.createRunBearer, "INGRESS") {
		t.Fatalf("ingress token must never be presented to CreateRun: %q", sr.createRunBearer)
	}
	if sr.createRunTaskID != "rtask-s1:k1" {
		t.Fatalf("durable Run.TaskID must be the child task, got %q", sr.createRunTaskID)
	}
	// The in-memory turn carries the child grant (transient) + child task ref (durable).
	snap, ok := coord.TurnSnapshot(resp.TurnID)
	if !ok {
		t.Fatal("turn snapshot missing")
	}
	if snap.RunGrant != "CHILD-TOK" || snap.RunTask != "rtask-s1:k1" {
		t.Fatalf("turn must carry child grant + child task ref, got grant=%q task=%q", snap.RunGrant, snap.RunTask)
	}
	// RunGrant must NEVER serialize (it is a transient credential).
	b, _ := json.Marshal(snap)
	if strings.Contains(string(b), "CHILD-TOK") || strings.Contains(string(b), "run_grant") {
		t.Fatalf("RunGrant must not serialize into any Turn payload: %s", b)
	}
}

// TestRunGrantFailClosed proves Tier-2 never downgrades to the ingress authority: no issuer,
// no authority task, or a failed Delegate all refuse the turn, and a failed Delegate creates
// NO run (no durable ingress-scoped run is left behind).
func TestRunGrantFailClosed(t *testing.T) {
	events := []string{}
	srv, _ := newStateRecorder(t, &events)
	sc := space.NewStateClient(srv.URL, nil)
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	req := space.AcceptTurnRequest{Key: "k", ContextID: "c", Input: json.RawMessage(`"x"`)}
	ctx := space.WithAuthorityTask(context.Background(), "space-authority:s1:d1:")

	// (a) RequireRunGrant with no issuer -> refuse.
	c1 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, RequireRunGrant: true})
	if _, err := c1.AcceptTurn(ctx, "tok", scope, req); err == nil {
		t.Fatal("expected fail-closed when a run grant is required but no issuer is configured")
	}

	// (b) authority task absent from the request context -> refuse (never mint from nothing).
	rec := &grantRec{childToken: "C"}
	c2 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, RunGrantIssuer: rec, RequireRunGrant: true})
	if _, err := c2.AcceptTurn(context.Background(), "tok", scope, req); err == nil {
		t.Fatal("expected fail-closed when the ingress authority task is missing")
	}

	// (c) Delegate fails -> AcceptTurn fails and NO run is created (no durable ingress run).
	n := len(events)
	rec2 := &grantRec{childToken: "C", delegateFail: true}
	c3 := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, RunGrantIssuer: rec2, RequireRunGrant: true})
	if _, err := c3.AcceptTurn(ctx, "tok", scope, req); err == nil {
		t.Fatal("expected fail-closed when the delegate fails")
	}
	if len(events) != n {
		t.Fatalf("a failed delegate must not create a run, but events grew: %v", events[n:])
	}
}

// TestCancelTurnRevokesGrantScope proves Cancel revokes THIS run's grant by its durable child
// task ref under the grant's OWN scope, and a revoke error is surfaced (never a false ok).
func TestCancelTurnRevokesGrantScope(t *testing.T) {
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	req := space.AcceptTurnRequest{Key: "k1", ContextID: "c1", Input: json.RawMessage(`"x"`)}
	ctx := space.WithAuthorityTask(context.Background(), "space-authority:s1:d1:")

	events := []string{}
	srv, state := newStateRecorder(t, &events)
	rec := &grantRec{events: &events, childToken: "CHILD-TOK"}
	c := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv.URL, nil), Core: &stubCore{}, RunGrantIssuer: rec, RequireRunGrant: true,
	})
	acc, err := c.AcceptTurn(ctx, "tok", scope, req)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if err := c.CancelTurn(context.Background(), acc.TurnID, erunIngressTok); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if rec.cancelN != 1 || rec.cancelTask != "rtask-s1:k1" {
		t.Fatalf("cancel must revoke the run's child grant by its durable child task ref, got n=%d task=%q", rec.cancelN, rec.cancelTask)
	}
	if rec.cancelToken != "CHILD-TOK" {
		t.Fatalf("cancel must present the run's OWN child token as bearer, got %q", rec.cancelToken)
	}
	if state.cancelBearer != "Bearer CHILD-TOK" || state.runStatus != "canceled" {
		t.Fatal("pending State run was not closed using its conversation credential")
	}

	// A revoke error must be surfaced as a failure, not a false ok.
	events2 := []string{}
	srv2, _ := newStateRecorder(t, &events2)
	rec2 := &grantRec{events: &events2, childToken: "CHILD-TOK", cancelFail: true}
	c2 := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: space.NewStateClient(srv2.URL, nil), Core: &stubCore{}, RunGrantIssuer: rec2, RequireRunGrant: true,
	})
	acc2, err := c2.AcceptTurn(ctx, "tok", scope, req)
	if err != nil {
		t.Fatalf("accept2: %v", err)
	}
	if err := c2.CancelTurn(context.Background(), acc2.TurnID, erunIngressTok); err == nil {
		t.Fatal("a failed grant revoke must surface as an error, never a false 200")
	}
}

// TestRunGrantAbsentInHTTPOutput drives the real HTTP edge (exchange middleware + handler) and
// asserts the Tier-2 run-grant token never reaches the browser via ListTurns.
func TestRunGrantAbsentInHTTPOutput(t *testing.T) {
	ms := newMockState()
	msrv := ms.Server()
	t.Cleanup(msrv.Close)
	sc := space.NewStateClient(msrv.URL, nil)
	const secret = "SECRET-GRANT-TOKEN-DO-NOT-LEAK"
	rec := &grantRec{childToken: secret}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, RunGrantIssuer: rec, RequireRunGrant: true, RunGrantTTL: time.Minute,
	})
	base := space.ExchangeMiddleware(&spyExchanger{}, authPerSubject)
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: base, RequireScopedTask: true})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)

	code, body := postSpaceAs(t, hs.URL, "AcceptTurn", map[string]any{"key": "kk", "context_id": "cc", "input": "hi", "max_steps": 3}, "s1")
	if code != http.StatusOK {
		t.Fatalf("AcceptTurn %d: %s", code, body)
	}
	// A child grant WAS minted and used.
	if rec.delegateN != 1 || rec.child != "rtask-s1:kk" {
		t.Fatalf("expected one child grant for the run, got n=%d child=%q", rec.delegateN, rec.child)
	}
	code, body = postSpaceAs(t, hs.URL, "ListTurns", map[string]any{}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns %d: %s", code, body)
	}
	if strings.Contains(string(body), secret) {
		t.Fatalf("Tier-2 run grant token leaked into ListTurns output: %s", body)
	}
	if strings.Contains(string(body), "run_grant") {
		t.Fatalf("run_grant field must not appear in HTTP output: %s", body)
	}
	// The durable, non-secret child task ref MAY appear (it is a resource id), proving the
	// run is child-grant-scoped rather than ingress-scoped.
	if !strings.Contains(string(body), "rtask-s1:kk") {
		t.Logf("note: run_task ref not surfaced in ListTurns (acceptable): %s", body)
	}
}

// DEF-DOTE2E-1. The run grant's context claim has exactly one honest source: the
// conversation the run operates in, which is what State stores as threads.context_id.
//
// That is not a preference. CoreRequest documents the same value ("the same value a task
// token carries as its context claim. Whatever a run writes is recorded under it, so the
// platform can find that data by conversation and delete it with the conversation"),
// cmd/as-runner's scoped core guard compares a request's ContextID against the claim, and
// the artifact plane binds every row a run writes to that claim so the data can be found
// and deleted with the conversation. Minting the grant with thread.ID instead made every
// browser Dot turn fail before the model with "Core request differs from verified task
// authority" - and, on the path where a request carries no ContextID, silently rebound the
// run's artifacts to a thread id, i.e. data that outlives the conversation it belongs to.
//
// The fixture keeps the two ids distinct on purpose (thread "thr-1", context "c1"): a test
// whose thread id equals its context id cannot tell these two sources apart, which is how
// the defect survived a green suite.
func TestAcceptTurnRunGrantCarriesTheConversationContextNotTheThreadID(t *testing.T) {
	events := []string{}
	srv, sr := newStateRecorder(t, &events)
	sc := space.NewStateClient(srv.URL, nil)
	rec := &grantRec{events: &events, childToken: "CHILD-TOK"}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: sc, Core: &stubCore{}, WorkerID: "w",
		RunGrantIssuer: rec, RequireRunGrant: true, RunGrantTTL: time.Minute,
	})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	ctx := space.WithAuthorityTask(context.Background(), "space-authority:s1:d1:")
	if _, err := coord.AcceptTurn(ctx, "INGRESS-TOK", scope, space.AcceptTurnRequest{
		Key: "k1", ContextID: "c1", Input: json.RawMessage(`"hi"`), MaxSteps: 5,
	}); err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if rec.ctxClaim != "c1" {
		t.Fatalf("the run grant was minted over context %q, want the conversation \"c1\" "+
			"(threads.context_id) - the value Core and the artifact plane bind on; the run row is %q",
			rec.ctxClaim, sr.createRunTaskID)
	}
	if rec.ctxClaim == "thr-1" {
		t.Fatal("the grant claims the THREAD id; a thread is not a conversation")
	}
	// A thread continued by id must keep claiming its own conversation, not the id of the
	// second thread that happens to be continuing it.
	if _, err := coord.AcceptTurn(ctx, "INGRESS-TOK", scope, space.AcceptTurnRequest{
		Key: "k2", ThreadID: "thr-1", Input: json.RawMessage(`"again"`), MaxSteps: 5,
	}); err != nil {
		t.Fatalf("AcceptTurn continue: %v", err)
	}
	if rec.ctxClaim != "c1" {
		t.Fatalf("a continued thread changed the claimed context to %q, want \"c1\"", rec.ctxClaim)
	}
}
