//go:build state_integration

package space_test

// The real-plane half of the built-in runner's mutation contract.
//
// core_mutation_journal_test.go proves the loop's RULES against a double that mirrors
// State. This file proves them where the rules are actually decided: the real State API
// over PostgreSQL, with a real minted run token, a real thread lease and the real
// intent fingerprint. Three properties, every one of them read back out of the real
// State rather than from the runner's own opinion:
//
//   - a run delivered twice applies its external effect ONCE, because the real
//     (run, node, call) row tells the second life what already happened;
//   - the same coordinate carrying a DIFFERENT body is refused with the real
//     KEY_REUSE_MISMATCH instead of quietly becoming a second mutation;
//   - an outcome whose write cannot reach the plane lands in a real `unknown` row, is
//     never auto-replayed, and answers from the journal only after a privileged
//     ReconcileIntent -- the published contract's wording, end to end.
//
// A flaky proxy (opFlakyProxy) sits between the runner and the real State and kills the
// connection for one op, so "it was never written" and "it was written and its ACK was
// lost" are the same observable event -- the only honest way to test a retry.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// opFlakyProxy forwards every State call to the real State with the env's workload
// certificate, except calls to /v1/state/<op> whose body carries statusIn: those die
// with the connection closed and nothing forwarded. statusIn == "" means every call to
// that op.
func opFlakyProxy(t *testing.T, e *cEnv, op, statusIn string, failures int32) *httptest.Server {
	t.Helper()
	var dropped int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("proxy read: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		drop := strings.HasSuffix(r.URL.Path, "/"+op) && atomic.LoadInt32(&dropped) < failures &&
			(statusIn == "" || strings.Contains(string(body), `"status":"`+statusIn+`"`))
		if drop {
			atomic.AddInt32(&dropped, 1)
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Errorf("proxy cannot hijack to drop a response")
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			conn, _, herr := hj.Hijack()
			if herr != nil {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			_ = conn.Close() // no response at all: the caller cannot tell what landed
			return
		}
		fwd, err := http.NewRequestWithContext(r.Context(), http.MethodPost, e.srv.URL+r.URL.Path, bytes.NewReader(body))
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		fwd.Header.Set("content-type", "application/json")
		if auth := r.Header.Get("Authorization"); auth != "" {
			fwd.Header.Set("Authorization", auth)
		}
		resp, err := e.srv.Client().Do(fwd)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(out)
	}))
	t.Cleanup(proxy.Close)
	return proxy
}

// realMutationEnv is one real run under one real lease, driven by the real Core loop.
type realMutationEnv struct {
	e      *cEnv
	runner *space.StateClient // what the RUNNER talks to (possibly through a proxy)
	token  string
	thread string
	runID  string
	worker string
	epoch  int64
	exec   *mutatingExecutor
}

func newRealMutationEnv(t *testing.T, e *cEnv, runner *space.StateClient) *realMutationEnv {
	t.Helper()
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(e.ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "mut-real")
	if err != nil {
		t.Fatalf("CreateThread over real State: %v", err)
	}
	run, err := e.client.CreateRun(e.ctx, human, th.ID, "req-"+contracts.NewID(), json.RawMessage(`{"type":"turn","input":"go"}`), "")
	if err != nil {
		t.Fatalf("CreateRun over real State: %v", err)
	}
	claim, err := e.client.ClaimRun(e.ctx, "", "w-real", 120*time.Second)
	if err != nil || claim.Lease == nil {
		t.Fatalf("ClaimRun over real State: %v", err)
	}
	if claim.TaskToken == "" {
		t.Fatalf("real ClaimRun minted no run token")
	}
	// State's ClaimRun hands out whichever run is claimable, so this must be proved
	// rather than assumed: the journal is keyed by (run, node, call), and a life that
	// leased a different run would leave the created run's rows unexamined.
	if claim.Lease.Run.ID != run.ID {
		t.Fatalf("the claim must lease the run this env created: created=%s claimed=%s",
			run.ID, claim.Lease.Run.ID)
	}
	return &realMutationEnv{
		e: e, runner: runner, token: claim.TaskToken, thread: th.ID, runID: claim.Lease.Run.ID,
		worker: claim.Lease.Worker, epoch: claim.Lease.Epoch, exec: &mutatingExecutor{},
	}
}

// life drives ONE LIFE of the real loop: ask for one effect, then finish. The model
// mints a fresh tool-call id per life, which is what a re-delivery really looks like.
func (m *realMutationEnv) life(t *testing.T, callID, args string) []string {
	t.Helper()
	reg := core.NewToolRegistry(m.exec)
	reg.Register(core.ToolDef{Name: "effect", Description: "an external effect"})
	rt := core.NewRuntime(core.RuntimeConfig{
		State: m.runner,
		Model: &oneShotEffectModel{callID: callID, args: args},
		Tools: reg,
	})
	results := make(chan space.StepResult, 64)
	var bodies []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sr := range results {
			if sr.StepType == space.StepTool || sr.StepType == space.StepError {
				bodies = append(bodies, string(sr.Content))
			}
		}
	}()
	req := &space.CoreRequest{
		TurnID: "turn-" + contracts.NewID(), ThreadID: m.thread,
		RunID: m.runID, Worker: m.worker, Epoch: m.epoch, TaskToken: m.token,
		Input: json.RawMessage(`"do the effect"`), MaxSteps: 4,
		Scope: contracts.Scope{Tenant: m.e.tenant, Deployment: m.e.deployment, Subject: cSubject},
	}
	runErr := rt.Run(m.e.ctx, req, results)
	// Run emits synchronously and does not close the caller's channel; the drain has
	// to be released before it can be waited on, and before any Fatalf can leave the
	// helper (internal/core/runtime_nil_tools_test.go:23-29 is the repo's pattern).
	close(results)
	<-done
	if runErr != nil {
		t.Fatalf("core.Run: %v", runErr)
	}
	return bodies
}

// row reads the mutation's journal row back out of the REAL State.
func (m *realMutationEnv) row(t *testing.T, call string) (status, digest string, result json.RawMessage) {
	t.Helper()
	intent, err := m.e.client.GetIntent(m.e.ctx, m.token, m.runID, "core", call)
	if err != nil {
		t.Fatalf("GetIntent over real State (call %q): %v", call, err)
	}
	return intent.Status, intent.Digest, intent.Result
}

func TestCoreMutationReplayDoesNotRepeatEffect_RealState(t *testing.T) {
	e := cNewEnv(t)
	m := newRealMutationEnv(t, e, e.client)
	const args = `{"file":"report.txt"}`

	first := m.life(t, "tc-life-1", args)
	if got := m.exec.calls.Load(); got != 1 {
		t.Fatalf("life 1 must apply the effect exactly once, got %d", got)
	}
	if len(first) == 0 || !strings.Contains(first[0], `"attempt":1`) {
		t.Fatalf("life 1 must emit the effect result, got %#v", first)
	}
	status, _, stored := m.row(t, "0")
	if status != "succeeded" {
		t.Fatalf("the real journal must close a completed mutation, got %q", status)
	}
	if !strings.Contains(string(stored), `"attempt":1`) {
		t.Fatalf("the real journal must hold the effect's result, got %s", stored)
	}

	// Same run, delivered again: a new model-side call id for the same mutation, and
	// the real row is what stops the second effect.
	second := m.life(t, "tc-life-2", args)
	if got := m.exec.calls.Load(); got != 1 {
		t.Fatalf("re-delivery repeated the external effect over the real State: executions=%d", got)
	}
	if len(second) == 0 || !strings.Contains(second[0], `"attempt":1`) {
		t.Fatalf("re-delivery must serve the result recorded in the real State, got %#v", second)
	}

	// A different body at the same coordinate IS a different mutation: the real
	// fingerprint refuses it and the runner must not perform it.
	third := m.life(t, "tc-life-3", `{"file":"other.txt"}`)
	if got := m.exec.calls.Load(); got != 1 {
		t.Fatalf("a reused mutation coordinate executed a second body: executions=%d", got)
	}
	var joined string
	for _, s := range third {
		joined += s
	}
	if !strings.Contains(joined, contracts.ErrKeyReuse) {
		t.Fatalf("the real KEY_REUSE refusal must reach the step stream, got %#v", third)
	}
}

func TestCoreMutationOutcomeLossBecomesUnknown_RealState(t *testing.T) {
	e := cNewEnv(t)
	proxy := opFlakyProxy(t, e, "UpdateIntent", "succeeded", 1<<20)
	m := newRealMutationEnv(t, e, space.NewStateClient(proxy.URL, nil))
	const args = `{"file":"report.txt"}`

	m.life(t, "tc-life-1", args)
	if got := m.exec.calls.Load(); got != 1 {
		t.Fatalf("the effect itself had to run exactly once, got %d", got)
	}
	status, digest, _ := m.row(t, "0")
	if status != "unknown" {
		t.Fatalf("an unconfirmable outcome must be UNKNOWN in the real journal, got %q", status)
	}

	// The next life must refuse to replay the real UNKNOWN.
	second := m.life(t, "tc-life-2", args)
	if got := m.exec.calls.Load(); got != 1 {
		t.Fatalf("the real State's UNKNOWN was auto-replayed: executions=%d", got)
	}
	var refused string
	for _, s := range second {
		refused += s
	}
	if !strings.Contains(refused, "unknown") {
		t.Fatalf("the refusal must name the ambiguity, got %#v", second)
	}

	// Only the privileged reconciliation may resolve it, and only then may a later
	// life answer that mutation from the journal.
	reconciled := json.RawMessage(`{"attempt":1,"from":"reconcile"}`)
	if _, err := e.client.ReconcileIntent(e.ctx, m.token,
		space.Fence{RunID: m.runID, Worker: m.worker, Epoch: m.epoch},
		"core", "0", "succeeded", "not-the-digest", reconciled); err == nil {
		t.Fatal("reconcile with a wrong fingerprint must be rejected")
	}
	if _, err := e.client.ReconcileIntent(e.ctx, m.token,
		space.Fence{RunID: m.runID, Worker: m.worker, Epoch: m.epoch},
		"core", "0", "succeeded", digest, reconciled); err != nil {
		t.Fatalf("ReconcileIntent over real State: %v", err)
	}
	third := m.life(t, "tc-life-3", args)
	if got := m.exec.calls.Load(); got != 1 {
		t.Fatalf("a reconciled mutation must never run again: executions=%d", got)
	}
	if len(third) == 0 || !strings.Contains(third[0], `"from":"reconcile"`) {
		t.Fatalf("a reconciled mutation must answer from the journal, got %#v", third)
	}
}
