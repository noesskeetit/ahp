//go:build e2e

// Package sdk_test — F2 crash-window integration gate.
//
// This is the integration proof the rev7 findings post required: a REAL
// PostgreSQL-backed intent crash window driven by a REAL Python LangGraph
// adapter process that is hard-killed (os._exit) AFTER the external side effect
// fires but BEFORE the intent is marked succeeded. On restart the SAME call must
// NOT re-run the effect (exactly-once external witness) and must surface a
// SideEffectUnknown so a reconciler can resolve it with external evidence.
//
// Why a test-only HTTP bridge: the product stateapi handler in this worktree
// does not yet route KV/intent/checkpoint operations over HTTP (state-owner
// scope; the boundary is asserted by TestE2EHTTPSdkLifecycle). The bridge is a
// test harness in our scope (tests/sdk) that exposes the SAME internal/state.Store
// intent ops over real HTTP against the SAME real PG, so the Python client's
// real HTTP code path is exercised end-to-end. The substance of the proof — a
// real PG intent that is "dispatched" (not "succeeded") across a real process
// crash, plus an exactly-once external witness — is NOT fixture-narrowed.
//
//	Run: AHP_TEST_DATABASE_URL=... AHP_F2_PYTHON=/path/to/python go test -tags e2e \
//	       ./tests/sdk/ -run TestF2 -v
package sdk_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// f2Store connects to the real acceptance PG and runs the idempotent ahp_state
// migrations. Isolated from the PG E2E tests by a distinct deployment tag.
func f2Store(t *testing.T) *state.Store {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		url = os.Getenv("DATABASE_URL")
	}
	if url == "" {
		t.Fatal("F2 requires AHP_TEST_DATABASE_URL (a real PostgreSQL); SKIP is not an accepted result")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("f2 pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("f2 ping: %v", err)
	}
	store := state.New(pool)
	for _, m := range []func(context.Context) error{
		store.Migrate, store.MigrateCheckpoints, store.MigrateIntents,
		store.MigrateEvents, store.MigrateKV,
	} {
		if err := m(context.Background()); err != nil {
			t.Fatalf("f2 migrate: %v", err)
		}
	}
	return store
}

// f2Scope returns a scope isolated into its OWN unique deployment (fresh per
// call). This matters: the other PG E2E tests share e2eScope.Deployment and can
// leave pending runs there, so a ClaimRun in a shared deployment could grab a
// non-F2 run. A per-run deployment guarantees ClaimRun only ever sees F2's own
// freshly created pending run.
func f2Scope() contracts.Scope {
	return contracts.Scope{
		Tenant:     "e2e-tenant",
		Deployment: "f2-deploy-" + contracts.NewID(),
		Subject:    "f2-subject",
	}
}

// intentReq mirrors the Python client's fence-scoped intent request bodies.
type intentReq struct {
	Fence  state.Fence     `json:"fence"`
	RunID  string          `json:"run_id"` // GetIntent sends run_id top-level (no fence)
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Args   json.RawMessage `json:"args"`
	Status string          `json:"status"`
	Digest string          `json:"digest"`
	Result json.RawMessage `json:"result"`
}

// f2Bridge exposes the real state.Store intent ops over real HTTP. It preserves
// the authoritative contract error CODES on the wire (matching what the
// server-side state layer returns), which the Python client branches on.
type f2Bridge struct {
	store *state.Store
	scope contracts.Scope
}

func (b *f2Bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const prefix = "/v1/state/"
	op := strings.TrimPrefix(r.URL.Path, prefix)
	var req intentReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 2<<20)).Decode(&req); err != nil {
		b.writeErr(w, contracts.NewError(contracts.ErrInvalid, "bridge: bad request body: "+err.Error()))
		return
	}
	var result any
	var err error
	switch op {
	case "RecordIntent":
		result, err = b.store.RecordIntent(r.Context(), b.scope, req.Fence.RunID, req.Fence.Worker,
			req.Fence.Epoch, req.Node, req.Call, req.Kind, req.Target, nonEmptyRaw(req.Args))
	case "UpdateIntent":
		result, err = b.store.UpdateIntent(r.Context(), b.scope, req.Fence.RunID, req.Fence.Worker,
			req.Fence.Epoch, req.Node, req.Call, req.Status, nonEmptyRaw(req.Result))
	case "GetIntent":
		result, err = b.store.GetIntent(r.Context(), b.scope, req.RunID, req.Node, req.Call)
	case "ReconcileIntent":
		result, err = b.store.ReconcileIntent(r.Context(), b.scope, req.Fence.RunID, req.Fence.Worker,
			req.Fence.Epoch, req.Node, req.Call, req.Status, req.Digest, nonEmptyRaw(req.Result))
	default:
		b.writeErr(w, contracts.NewError(contracts.ErrNotFound, "bridge: no route for op "+op))
		return
	}
	if err != nil {
		b.writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"intent": result})
}

// nonEmptyRaw normalizes the wire form the Python client actually sends: an
// omitted field decodes to a nil RawMessage, which the authoritative store would
// treat as an invalid/empty payload. Map a nil args/result to a valid JSON object
// (the client sends {} for empty args) so digests and result persistence match
// the SDK's own behaviour. An explicit null is passed through.
func nonEmptyRaw(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage(`{}`)
	}
	return raw
}

func (b *f2Bridge) writeErr(w http.ResponseWriter, err error) {
	code := contracts.Code(err)
	status := http.StatusInternalServerError
	switch code {
	case contracts.ErrNotFound:
		status = http.StatusNotFound
	case contracts.ErrInvalid, contracts.ErrKeyReuse:
		status = http.StatusBadRequest
	case contracts.ErrConflict, contracts.ErrStaleEpoch, contracts.ErrLeaseLost:
		status = http.StatusConflict
	case contracts.ErrForbidden:
		status = http.StatusForbidden
	}
	if code == "" {
		code = "INTERNAL"
	}
	body, _ := json.Marshal(contracts.Error{Code: code, Message: err.Error()})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

var witnessLine = regexp.MustCompile(`(?m)^f2-effect-`)

func countWitness(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		t.Fatalf("read witness: %v", err)
	}
	return len(witnessLine.FindAllIndex(data, -1))
}

// TestF2IntentCrashWindow proves exactly-once side effects across a real
// process crash at the intent crash window, against a real PG.
func TestF2IntentCrashWindow(t *testing.T) {
	store := f2Store(t)
	ctx := context.Background()

	// One fresh thread + run, then claim it to obtain a real fence.
	scope := f2Scope()
	thread, err := store.CreateThread(ctx, scope, "f2-ctx", "f2-key")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	worker := "f2-worker"
	// CreateRun's last string arg is a *key* (dedup hint); the run ID is
	// server-generated. Capture the real ID so the fence and assertions use it.
	run, err := store.CreateRun(ctx, scope, thread.ID, "task-f2", "f2-run-key", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := store.ClaimRun(ctx, scope.Tenant, scope.Deployment, worker, 5*time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun: %v (lease=%v)", err, lease)
	}
	if lease.Run.ID != run.ID {
		t.Fatalf("claimed run %q, want %q (scope isolation broken)", lease.Run.ID, run.ID)
	}
	runID := run.ID
	t.Cleanup(func() {
		_ = store.Release(context.Background(), scope, runID, worker, lease.Epoch, state.RunDone)
	})
	fence := state.Fence{RunID: lease.Run.ID, Worker: worker, Epoch: lease.Epoch}

	// Real HTTP bridge over the real store.
	srv := httptest.NewServer(&f2Bridge{store: store, scope: scope})
	defer srv.Close()

	py := os.Getenv("AHP_F2_PYTHON")
	if py == "" {
		t.Fatal("F2 requires AHP_F2_PYTHON (a Python with httpx+langgraph, e.g. the f2 venv); " +
			"not falling back to system python3")
	}
	agent := filepath.Join(repoRoot(t), "sdk", "python", "tests", "f2_agent.py")
	if _, err := os.Stat(agent); err != nil {
		t.Fatalf("F2 python agent not found: %v", err)
	}

	tmp := t.TempDir()
	witness := filepath.Join(tmp, "witness.txt")

	callID := "f2-call-" + contracts.NewID()

	spawn := func(mode string) (int, string, error) {
		cmd := exec.CommandContext(ctx, py, agent)
		cmd.Env = append(os.Environ(),
			"AHP_F2_BASE="+srv.URL,
			"AHP_F2_RUN_ID="+fence.RunID,
			"AHP_F2_WORKER="+fence.Worker,
			fmt.Sprintf("AHP_F2_EPOCH=%d", fence.Epoch),
			"AHP_F2_WITNESS="+witness,
			"AHP_F2_CALL_ID="+callID,
			"AHP_F2_MODE="+mode,
			"AHP_F2_PYTHON="+py,
			"AHP_F2_SDK_ROOT="+filepath.Join(repoRoot(t), "sdk", "python"),
		)
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		}
		return code, string(out), err
	}

	// --- PHASE A: crash mid-effect (after the external effect, before succeed) ---
	codeA, outA, errA := spawn("crash")
	if errA != nil && codeA == 0 {
		t.Fatalf("phase A should have hard-exited, got: %v\n%s", errA, outA)
	}
	if got := countWitness(t, witness); got != 1 {
		t.Fatalf("after crash: external witness lines = %d, want exactly 1 (effect fired once)\nagent:\n%s", got, outA)
	}

	// The authoritative crash-window signature: the intent is DISPATCHED in real
	// PG (effect fired, completion lost) — NOT succeeded, NOT recorded.
	after, err := store.GetIntent(ctx, scope, fence.RunID, "agent", callID)
	if err != nil {
		t.Fatalf("GetIntent after crash: %v", err)
	}
	if after.Status != state.IntentDispatched {
		t.Fatalf("after crash: intent status = %q, want dispatched (crash window between effect and succeed); agent:\n%s", after.Status, outA)
	}

	// --- PHASE B: restart the SAME call in a FRESH process ---
	// It must NOT re-run the effect and must surface SideEffectUnknown.
	codeB, outB, _ := spawn("recover")
	if codeB != 2 {
		t.Fatalf("phase B exit code = %d, want 2 (SideEffectUnknown raised); agent:\n%s", codeB, outB)
	}
	if got := countWitness(t, witness); got != 1 {
		t.Fatalf("after restart: external witness lines = %d, want still exactly 1 (no re-run); agent:\n%s", got, outB)
	}

	// Recover re-entered the crash window and moved dispatched -> unknown.
	final, err := store.GetIntent(ctx, scope, fence.RunID, "agent", callID)
	if err != nil {
		t.Fatalf("GetIntent after restart: %v", err)
	}
	if final.Status != state.IntentUnknown {
		t.Fatalf("after restart: intent status = %q, want unknown (recover surfaced SideEffectUnknown); agent:\n%s", final.Status, outB)
	}

	t.Logf("F2 crash window proven: effect ran exactly once across a real process crash; "+
		"intent %q -> %q -> %q; witness=%d",
		state.IntentRecorded, state.IntentDispatched, state.IntentUnknown, countWitness(t, witness))
}

// repoRoot resolves the repository root relative to the test's working directory
// (go test runs in the package directory: <root>/tests/sdk).
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Join(wd, "..", "..")
}
