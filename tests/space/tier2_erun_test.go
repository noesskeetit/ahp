package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Frozen Tier-2 E-run contract (decisions/2026-10-30T145405Z-tier2-authority-cancel-frozen-contract.md):
//
//	runTok = claim.TaskToken ALWAYS (warm AND cold): State reissues a fresh token bound to the
//	durable Run.TaskID (the CHILD run task) at ClaimRun. Space never re-delegates/re-mints at
//	dispatch time (a cold turn has no ingress task at all), and never falls back to the ingress
//	authority. Under RequireRunGrant, executeTurn fails closed unless the durable Run.TaskID is a
//	CHILD run task. Turn.Scope comes from the LEASE (the run's owner), never from whoever called
//	DispatchTurn over HTTP.
//
// The double below records the Authorization header of every State op so "which principal did the
// run actually present" is directly observable, and it is reset between the accept phase (where the
// ingress authority legitimately appears) and the dispatch phase (where it must not).
const (
	erunClaimToken = "RUN-CHILD-CLAIM-TOKEN"
	erunIngressTok = "INGRESS-AUTHORITY-TOKEN"
	erunCallerTok  = "CALLER-B-TOKEN"
	erunChildTask  = "rtask-owner-a:k-erun"
	erunAuthTask   = "space-authority:owner-a:d1"
)

type erunState struct {
	mu      sync.Mutex
	runTask string
	bearers map[string][]string
	appends int

	// ops is the ORDERED log of every op this double saw (bearers is unordered per op, so
	// "did the durable terminal frame reach State BEFORE the run was closed?" is only
	// answerable from a sequence). Recorded for every op, filled from the request body.
	ops []erunOp

	// refuseAppends reproduces what real State does the moment a run's Identity grant is
	// revoked: every further write with that credential is refused.
	refuseAppends bool

	releaseCh chan string
	// releaseReasonCh records the same Release calls WITH the DEF-A6 terminal reason
	// that travelled in the body, so "which close_reason did this run hand State" is
	// observable without changing what releaseCh means for every existing test.
	releaseReasonCh chan erunRelease
	srv             *httptest.Server
}

// erunRelease is one Release call this double saw: the status the run closed with
// and the reason that came with it (empty when the caller stated none).
type erunRelease struct {
	Status      string
	CloseReason string
}

// erunOp is one recorded State call: the op name, the bearer that presented it, and the
// stream coordinates (key/kind/payload) when the op carried them.
type erunOp struct {
	Index   int
	Op      string
	Bearer  string
	Key     string
	Kind    string
	Payload string
}

func newERunState(t *testing.T, runTask string) *erunState {
	t.Helper()
	m := &erunState{
		runTask:         runTask,
		bearers:         map[string][]string{},
		releaseCh:       make(chan string, 4),
		releaseReasonCh: make(chan erunRelease, 8),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/state/", func(w http.ResponseWriter, r *http.Request) {
		op := r.URL.Path[len("/v1/state/"):]
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		m.mu.Lock()
		m.bearers[op] = append(m.bearers[op], r.Header.Get("Authorization"))
		rec := erunOp{Op: op, Bearer: r.Header.Get("Authorization")}
		rec.Index = len(m.ops)
		if k, ok := body["key"].(string); ok {
			rec.Key = k
		}
		if k, ok := body["kind"].(string); ok {
			rec.Kind = k
		}
		if p, ok := body["payload"]; ok {
			if raw, merr := json.Marshal(p); merr == nil {
				rec.Payload = string(raw)
			}
		}
		m.ops = append(m.ops, rec)
		m.mu.Unlock()
		write := func(v any) {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(v)
		}
		switch op {
		case "CreateThread":
			write(map[string]any{"id": "thr-erun-1", "context_id": "ctx-erun"})
		case "CreateRun":
			write(map[string]any{"id": "run-erun-1", "thread_id": "thr-erun-1", "task_id": body["task_id"], "status": "pending", "sequence": 1})
		case "ClaimRun":
			m.mu.Lock()
			task := m.runTask
			m.mu.Unlock()
			payload, _ := json.Marshal(map[string]any{"type": "turn", "input": "hi", "max_steps": 3, "run_task": task})
			write(map[string]any{
				"task_token": erunClaimToken,
				"lease": map[string]any{
					"thread": map[string]any{"id": "thr-erun-1"},
					"run":    map[string]any{"id": "run-erun-1", "thread_id": "thr-erun-1", "task_id": task, "status": "pending", "payload": json.RawMessage(payload)},
					// Real State returns the RUN OWNER's scope on ClaimRun; that is the lease scope
					// the cold turn must adopt (never the dispatch caller's scope).
					"scope":      map[string]any{"tenant": "t1", "deployment": "d1", "subject": "owner-a"},
					"worker":     "w1",
					"epoch":      9,
					"expires_at": time.Now().Add(time.Minute).UTC(),
				},
			})
		case "ListCheckpoints":
			write(map[string]any{"checkpoints": []any{}})
		case "ReadStream":
			page := map[string]any{"events": []any{}}
			if _, bounded := body["max_response_bytes"]; bounded {
				page["has_more"] = false
				page["next_after"] = body["after"]
			}
			write(page)
		case "AppendStream":
			m.mu.Lock()
			refuse := m.refuseAppends
			m.appends++
			n := m.appends
			m.mu.Unlock()
			if refuse {
				w.WriteHeader(http.StatusForbidden)
				write(map[string]any{"code": "FORBIDDEN",
					"message": "identity: the grant named by the token does not exist in its scope"})
				return
			}
			write(map[string]any{"sequence": n})
		case "Heartbeat":
			write(map[string]any{})
		case "Release":
			status := "unknown"
			if s, ok := body["status"].(string); ok {
				status = s
			}
			reason, _ := body["close_reason"].(string)
			select {
			case m.releaseCh <- status:
			default:
			}
			select {
			case m.releaseReasonCh <- erunRelease{Status: status, CloseReason: reason}:
			default:
			}
			write(map[string]any{})
		default:
			write(map[string]any{})
		}
	})
	m.srv = httptest.NewServer(mux)
	t.Cleanup(m.srv.Close)
	return m
}

// reset drops recorded bearers/appends so subsequent assertions only see the dispatch phase.
func (m *erunState) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.bearers = map[string][]string{}
	m.ops = nil
	m.appends = 0
}

// streamFrames returns every durable AppendStream this double saw, in arrival order.
func (m *erunState) streamFrames() []erunOp {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []erunOp
	for _, o := range m.ops {
		if o.Op == "AppendStream" {
			out = append(out, o)
		}
	}
	return out
}

// opPosition returns the index of the first recorded call of op, or -1 when it never happened.
func (m *erunState) opPosition(op string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, o := range m.ops {
		if o.Op == op {
			return i
		}
	}
	return -1
}

func (m *erunState) opBearers(op string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.bearers[op]))
	out = append(out, m.bearers[op]...)
	return out
}

func (m *erunState) totalAppends() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.appends
}

// forbidToken asserts no RUN op presented the given token as bearer. ClaimRun is deliberately
// excluded: it is the trigger boundary, so by construction it carries whatever token the
// dispatcher holds. E-run constrains the identity of the run AFTER the claim (Core, stream
// writes, heartbeat, release), which is everything recorded here except ClaimRun.
func (m *erunState) forbidToken(t *testing.T, token, why string) {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	for op, list := range m.bearers {
		if op == "ClaimRun" {
			continue
		}
		for _, b := range list {
			if b == "Bearer "+token {
				t.Fatalf("%s: op %s presented bearer %q", why, op, b)
			}
		}
	}
}

func (m *erunState) requireBearer(t *testing.T, op, token, why string) {
	t.Helper()
	list := m.opBearers(op)
	if len(list) == 0 {
		t.Fatalf("%s: op %s was never called", why, op)
	}
	for _, b := range list {
		if b != "Bearer "+token {
			t.Fatalf("%s: op %s presented %q, want the claim token", why, op, b)
		}
	}
}

func (m *erunState) waitRelease(t *testing.T, d time.Duration) (string, bool) {
	t.Helper()
	select {
	case s := <-m.releaseCh:
		return s, true
	case <-time.After(d):
		return "", false
	}
}

// erunCore records the CoreRequest it was handed, so the test can prove which token/scope the
// model+tool loop would actually run under.
type erunCore struct {
	mu    sync.Mutex
	calls int
	token string
	scope contracts.Scope
}

func (c *erunCore) Run(ctx context.Context, req *space.CoreRequest, results chan<- space.StepResult) error {
	c.mu.Lock()
	c.calls++
	c.token = req.TaskToken
	c.scope = req.Scope
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return nil
}

func (c *erunCore) snapshot() (int, string, contracts.Scope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls, c.token, c.scope
}

func newERunCoordinator(f *erunState, core space.CoreRunner, issuer space.RunGrantIssuer) *space.Coordinator {
	return space.NewCoordinator(space.CoordinatorConfig{
		StateClient:     space.NewStateClient(f.srv.URL, nil),
		Core:            core,
		WorkerID:        "w1",
		RunGrantIssuer:  issuer,
		RequireRunGrant: true,
		RunGrantTTL:     time.Minute,
	})
}

// TestERunWarmDispatchPresentsClaimToken pins the warm leg of E-run: after AcceptTurn minted the
// child grant, dispatch must present State's reissued claim token for the run's durable writes and
// for Core, must NOT re-delegate, and must never reuse the ingress authority.
func TestERunWarmDispatchPresentsClaimToken(t *testing.T) {
	f := newERunState(t, erunChildTask)
	issuer := &grantRec{childToken: "MINTED-CHILD-TOK"}
	core := &erunCore{}
	c := newERunCoordinator(f, core, issuer)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	ctx := space.WithAuthorityTask(context.Background(), erunAuthTask)
	if _, err := c.AcceptTurn(ctx, erunIngressTok, owner, space.AcceptTurnRequest{
		Key: "k-erun", ContextID: "ctx-erun", Input: json.RawMessage(`"hi"`), MaxSteps: 3,
	}); err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if issuer.delegateN != 1 {
		t.Fatalf("exactly one child grant is minted at accept, got %d", issuer.delegateN)
	}
	f.reset()

	if err := c.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("warm DispatchTurn: %v", err)
	}
	if status, ok := f.waitRelease(t, 10*time.Second); !ok || status != "done" {
		t.Fatalf("warm run did not complete cleanly: status=%q ok=%v", status, ok)
	}

	calls, token, scope := core.snapshot()
	if calls != 1 {
		t.Fatalf("warm: Core must run exactly once, got %d", calls)
	}
	if token != erunClaimToken {
		t.Fatalf("warm: Core must run under claim.TaskToken, got %q", token)
	}
	if scope.Subject != "owner-a" {
		t.Fatalf("warm: Core scope must be the turn owner, got %+v", scope)
	}
	if issuer.delegateN != 1 {
		t.Fatalf("dispatch must NOT re-delegate/re-mint (grants minted=%d)", issuer.delegateN)
	}
	f.requireBearer(t, "AppendStream", erunClaimToken, "warm: stream writes")
	f.requireBearer(t, "Release", erunClaimToken, "warm: release")
	f.forbidToken(t, erunIngressTok, "SP-3 warm E-run: ingress authority must never be presented by the run")
}

// TestERunColdDispatchUsesClaimTokenAndLeaseScope pins the cold leg: a fresh process (empty turn
// table) must (a) run under claim.TaskToken, (b) never re-delegate (a cold turn has no ingress task),
// (c) bind the turn/Core scope to the LEASE owner, not to the caller that triggered dispatch.
func TestERunColdDispatchUsesClaimTokenAndLeaseScope(t *testing.T) {
	f := newERunState(t, erunChildTask)
	f.reset()

	callerB := contracts.Scope{Tenant: "t9", Deployment: "d9", Subject: "user-eve"}
	issuer2 := &grantRec{childToken: "MUST-NOT-BE-MINTED"}
	core := &erunCore{}
	c := newERunCoordinator(f, core, issuer2)

	if err := c.DispatchTurn(context.Background(), erunCallerTok, callerB); err != nil {
		t.Fatalf("cold DispatchTurn(caller B): %v", err)
	}
	if status, ok := f.waitRelease(t, 10*time.Second); !ok || status != "done" {
		t.Fatalf("cold run did not complete cleanly: status=%q ok=%v", status, ok)
	}

	calls, token, scope := core.snapshot()
	if calls != 1 {
		t.Fatalf("cold: Core must run exactly once, got %d", calls)
	}
	if token != erunClaimToken {
		t.Fatalf("cold: Core must run under claim.TaskToken (never a re-mint), got %q", token)
	}
	if scope.Subject != "owner-a" {
		t.Fatalf("cold: Turn scope must come from the lease owner, got %+v", scope)
	}
	if issuer2.delegateN != 0 {
		t.Fatalf("cold dispatch must never re-delegate a grant (minted=%d)", issuer2.delegateN)
	}
	snap, ok := c.TurnSnapshot("run-erun-1")
	if !ok {
		t.Fatal("cold turn was not rehydrated")
	}
	if snap.Scope.Subject == "user-eve" || snap.Scope.Subject != "owner-a" {
		t.Fatalf("cold rehydrate must bind the lease owner, got %+v", snap.Scope)
	}
	f.requireBearer(t, "AppendStream", erunClaimToken, "cold: stream writes")
	f.requireBearer(t, "Release", erunClaimToken, "cold: release")
	f.forbidToken(t, erunCallerTok, "SP-3 cold E-run: the dispatch caller's token must never reach State")
}

// TestERunRequireGrantRejectsAuthorityRootedRun is the fail-closed leg of E-run: when the durable
// Run.TaskID is the broad ingress authority (the Tier-2 boundary was bypassed at CreateRun), the run
// must fail BEFORE Core with zero durable writes. The same harness then runs the positive control:
// with a CHILD Run.TaskID the very same coordinator does reach Core, so the negative is non-vacuous.
func TestERunRequireGrantRejectsAuthorityRootedRun(t *testing.T) {
	f := newERunState(t, erunAuthTask) // NOT a child run task -> boundary bypass
	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}

	core := &erunCore{}
	c := newERunCoordinator(f, core, &grantRec{childToken: "MINTED-CHILD-TOK"})
	if err := c.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	status, ok := f.waitRelease(t, 10*time.Second)
	if !ok || status != "failed" {
		t.Fatalf("an authority-rooted run must fail closed, got status=%q ok=%v", status, ok)
	}
	if calls, _, _ := core.snapshot(); calls != 0 {
		t.Fatalf("SP-3: Core must not run when Run.TaskID is the ingress authority, calls=%d", calls)
	}
	if got := f.totalAppends(); got != 0 {
		t.Fatalf("a refused run must write nothing to the durable stream, appends=%d", got)
	}
	f.requireBearer(t, "Release", erunClaimToken, "refused run must still free the lease with its own claim token")
	f.forbidToken(t, erunIngressTok, "SP-3: a refused run must not fall back to the ingress authority")

	// Positive control on the SAME harness: a CHILD Run.TaskID reaches Core.
	f2 := newERunState(t, erunChildTask)
	core2 := &erunCore{}
	c2 := newERunCoordinator(f2, core2, &grantRec{childToken: "MINTED-CHILD-TOK"})
	if err := c2.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("control DispatchTurn: %v", err)
	}
	if status, ok := f2.waitRelease(t, 10*time.Second); !ok || status != "done" {
		t.Fatalf("positive control must complete, got status=%q ok=%v", status, ok)
	}
	if calls, _, _ := core2.snapshot(); calls != 1 {
		t.Fatalf("positive control must reach Core exactly once, calls=%d", calls)
	}
}

// forbidAppends makes every later durable write with the run's credential fail, exactly as it
// does on the stand once CancelTurn has revoked the run's own grant.
func (m *erunState) forbidAppends() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refuseAppends = true
}

// drainReleases returns every terminal status already recorded, without blocking.
func drainReleases(ch chan string) []string {
	var out []string
	for {
		select {
		case v := <-ch:
			out = append(out, v)
		default:
			return out
		}
	}
}
