package space_test

// core_mutation_journal_test.go -- block 8 (PDF rev 3.2): "повтор мутации" and
// "UNKNOWN между ходами" for the BUILT-IN runner.
//
// The published State contract is explicit (docs/state.md, internal/state/intents.go):
// an intent records an effect BEFORE it is attempted, a replay of the same key
// returns the ORIGINAL outcome instead of applying twice, and an UNKNOWN outcome is
// never auto-replayed -- it must be reconciled explicitly. The compute plane already
// obeys that shape (internal/compute.Exec returns mayDispatch=false once dispatched).
//
// The built-in Core loop (cmd/as-runner -> coordinator -> core.Runtime) is the one
// runner that journals its own tool effects, so these tests hold IT to the same rule.
// They drive the real production loop (core.NewRuntime with cfg.State set, exactly as
// cmd/as-runner/main.go wires it) against the State double, over a real lease, and ask
// the only question that matters for a mutation: after the same run is delivered
// again, did the external effect happen twice?

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// mutatingExecutor is an external effect with a memory: every EXECUTION stamps a
// fresh attempt number into its own result body, so "the effect ran twice" and
// "the first result was replayed" are distinguishable from the outside -- which is
// the whole point. A body-returning double that always answered identically could
// never tell those two apart.
type mutatingExecutor struct {
	calls atomic.Int64
}

func (e *mutatingExecutor) Execute(_ context.Context, name string, args json.RawMessage) (json.RawMessage, error) {
	if name != "effect" {
		return nil, fmt.Errorf("unknown tool %q", name)
	}
	n := e.calls.Add(1)
	return json.RawMessage(fmt.Sprintf(`{"tool":%q,"attempt":%d,"args":%s}`, name, n, string(args))), nil
}

// oneShotEffectModel asks for exactly one tool call, then finishes. The tool-call ID
// differs per life on purpose: that is what a re-delivery really looks like, because
// the model is asked again and mints a new call id for the same logical mutation.
type oneShotEffectModel struct {
	callID string
	args   string
	calls  atomic.Int64
}

func (m *oneShotEffectModel) Generate(_ context.Context, _ *core.ModelRequest) (*core.ModelResponse, error) {
	if m.calls.Add(1) == 1 {
		return &core.ModelResponse{
			Content:   json.RawMessage(`"applying the effect"`),
			ToolCalls: []core.ToolCall{{ID: m.callID, Name: "effect", Args: json.RawMessage(m.args)}},
		}, nil
	}
	return &core.ModelResponse{Content: json.RawMessage(`"finished"`), Stop: "stop"}, nil
}

type mutationEnv struct {
	ms     *mockState
	token  string
	runID  string
	worker string
	epoch  int64
	client *space.StateClient
}

func newMutationEnv(t *testing.T) *mutationEnv {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	client := space.NewStateClient(srv.URL, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	scope := contracts.Scope{Tenant: "t-mut", Deployment: "d-mut", Subject: "s-mut"}
	th, err := client.CreateThread(ctx, "tok", scope, "ctx-"+contracts.NewID(), "mut-key")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := client.CreateRun(ctx, "tok", th.ID, "req-"+contracts.NewID(), json.RawMessage(`{"input":"go"}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := client.ClaimRun(ctx, "tok", "w-mut", 60*time.Second)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if claim.Lease == nil || claim.TaskToken == "" {
		t.Fatalf("ClaimRun returned no lease/token: %+v", claim)
	}
	// The journal is keyed by (run, node, call), so every life of this env must be
	// working the SAME run the claim leased. If the claim ever handed out a different
	// run, these replay tests would be proving nothing about the run they created.
	if claim.Lease.Run.ID != run.ID {
		t.Fatalf("the claim must lease the run this env created: created=%s claimed=%s",
			run.ID, claim.Lease.Run.ID)
	}
	return &mutationEnv{ms: ms, token: claim.TaskToken, runID: claim.Lease.Run.ID,
		worker: claim.Lease.Worker, epoch: claim.Lease.Epoch, client: client}
}

// runLife drives ONE LIFE of the real Core loop under the env's lease and returns
// the bodies of the emitted tool steps.
func runLife(t *testing.T, env *mutationEnv, model core.ModelClient, exec core.ToolExecutor) []string {
	t.Helper()
	reg := core.NewToolRegistry(exec)
	reg.Register(core.ToolDef{Name: "effect", Description: "an external effect"})
	rt := core.NewRuntime(core.RuntimeConfig{Model: model, Tools: reg, State: env.client})
	results := make(chan space.StepResult, 64)
	var runErr error
	var toolBodies []string
	done := make(chan struct{})
	go func() {
		defer close(done)
		for sr := range results {
			if sr.StepType == space.StepTool || sr.StepType == space.StepError {
				toolBodies = append(toolBodies, string(sr.Content))
			}
		}
	}()
	req := &space.CoreRequest{
		TurnID: "turn-" + contracts.NewID(), ThreadID: "thread-" + contracts.NewID(),
		RunID: env.runID, Worker: env.worker, Epoch: env.epoch, TaskToken: env.token,
		Input: json.RawMessage(`"do the effect"`), MaxSteps: 4,
		Scope: contracts.Scope{Tenant: "t-mut", Deployment: "d-mut", Subject: "s-mut"},
	}
	runErr = rt.Run(context.Background(), req, results)
	// Run emits synchronously and does NOT close the caller's channel, so the drain
	// must be released explicitly; without this close, <-done blocks forever (the
	// repo's own pattern: internal/core/runtime_nil_tools_test.go:23-29).
	close(results)
	<-done
	if runErr != nil {
		t.Fatalf("core.Run: %v", runErr)
	}
	return toolBodies
}

// intentRows returns the journal rows the double holds for one run.
func (env *mutationEnv) rows() []intentItem {
	env.ms.mu.Lock()
	defer env.ms.mu.Unlock()
	var out []intentItem
	for _, it := range env.ms.intents {
		if it.RunID == env.runID {
			out = append(out, it)
		}
	}
	return out
}

// TestCoreMutationReplayDoesNotRepeatEffect is the block-8 mutation-replay gate.
//
// The same run is delivered twice (the second life mints a fresh model tool-call id
// for the SAME logical mutation, which is exactly what a cold re-delivery does). The
// effect must be applied ONCE: the journal is what tells the second life that this
// mutation already completed, and the recorded result -- not a fresh effect -- is
// what the model must see. This is the runner's share of "a restart never repeats
// the external effect"; the durable step stream already has the same property for
// steps (space.StepKey + digest).
func TestCoreMutationReplayDoesNotRepeatEffect(t *testing.T) {
	env := newMutationEnv(t)
	exec := &mutatingExecutor{}

	first := runLife(t, env, &oneShotEffectModel{callID: "tc-life-1", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("life 1 must apply the effect exactly once, got %d", got)
	}
	if len(first) == 0 || !strings.Contains(first[0], `"attempt":1`) {
		t.Fatalf("life 1 must emit the effect result, got %#v", first)
	}

	rows := env.rows()
	if len(rows) != 1 {
		t.Fatalf("one mutation must be one journal row, got %d rows: %+v", len(rows), rows)
	}
	if rows[0].Status != "succeeded" {
		t.Fatalf("a completed mutation must be closed in the journal, got status %q (row %+v)", rows[0].Status, rows[0])
	}
	if !strings.Contains(string(rows[0].Result), `"attempt":1`) {
		t.Fatalf("the journal must hold the effect's result, got %s", rows[0].Result)
	}

	// Second life of the SAME run: same mutation, new model-side call id.
	second := runLife(t, env, &oneShotEffectModel{callID: "tc-life-2", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("re-delivery repeated the external effect: executions=%d (journal rows=%+v)", got, env.rows())
	}
	if len(second) == 0 || !strings.Contains(second[0], `"attempt":1`) {
		t.Fatalf("re-delivery must serve the RECORDED result (attempt 1), got %#v", second)
	}
	if rows := env.rows(); len(rows) != 1 {
		t.Fatalf("a replayed mutation must not add journal rows, got %+v", rows)
	}
}

// TestCoreMutationRefusesEffectWhenJournalUnreachable proves the journal is a
// precondition of the effect, not a log line beside it. State refuses the record
// (a transport failure here is indistinguishable from a lost ACK -- the writer
// cannot know whether the row exists), so the runner cannot later tell "the effect
// was journaled and applied" from "the effect was applied blind". An effect the
// platform cannot reconcile is an effect it must not perform.
func TestCoreMutationRefusesEffectWhenJournalUnreachable(t *testing.T) {
	env := newMutationEnv(t)
	exec := &mutatingExecutor{}
	env.ms.armFailedIntent(-1, func(op, _, _, _ string) bool { return op == "RecordIntent" })

	steps := runLife(t, env, &oneShotEffectModel{callID: "tc-1", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 0 {
		t.Fatalf("an unjournalled mutation must not be executed, executions=%d", got)
	}
	var joined string
	for _, s := range steps {
		joined += s
	}
	if !strings.Contains(joined, "journal") {
		t.Fatalf("the refusal must be visible to the caller/step stream, got %#v", steps)
	}
}

// TestCoreMutationOutcomeLossBecomesUnknownAndIsNotReplayed is "UNKNOWN между
// ходами" at the exact place it is created: the effect happened, and the write that
// would have closed the journal row died. The row must land in `unknown` -- the only
// status that says "the world may have changed and nobody may guess" -- and the next
// life of the run must refuse to replay it instead of performing the effect again.
func TestCoreMutationOutcomeLossBecomesUnknownAndIsNotReplayed(t *testing.T) {
	env := newMutationEnv(t)
	exec := &mutatingExecutor{}
	env.ms.armFailedIntent(-1, func(op, _, _, status string) bool {
		return op == "UpdateIntent" && status == "succeeded"
	})

	runLife(t, env, &oneShotEffectModel{callID: "tc-1", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("the effect itself was never refused, executions=%d", got)
	}
	rows := env.rows()
	if len(rows) != 1 {
		t.Fatalf("one mutation must be one journal row, got %+v", rows)
	}
	if rows[0].Status != "unknown" {
		t.Fatalf("a mutation whose outcome could not be recorded must be UNKNOWN, got %q (row %+v)",
			rows[0].Status, rows[0])
	}

	env.ms.disarmIntentFaults()
	second := runLife(t, env, &oneShotEffectModel{callID: "tc-2", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("an UNKNOWN mutation was auto-replayed: executions=%d", got)
	}
	var joined string
	for _, s := range second {
		joined += s
	}
	if !strings.Contains(joined, "unknown") {
		t.Fatalf("the next life must report the ambiguity it refused to replay, got %#v", second)
	}
}

// TestCoreMutationOutcomeSurvivesALostACK is the other half of the same fault, and the
// reason the journal retries instead of panicking: the outcome write IS durable and
// only its ACK was lost. From the caller's side that is indistinguishable from a write
// that never happened (tests/space/mock_state_test.go arms both halves), so the writer
// must repeat the identical write -- after which the row reads `succeeded`, the turn
// closes normally, and the next life replays the result. Marking this UNKNOWN instead
// would strand a completed effect behind a reconciliation that has nothing to decide.
func TestCoreMutationOutcomeSurvivesALostACK(t *testing.T) {
	env := newMutationEnv(t)
	exec := &mutatingExecutor{}
	env.ms.armDroppedIntentACK(1, func(op, _, _, status string) bool {
		return op == "UpdateIntent" && status == "succeeded"
	})

	runLife(t, env, &oneShotEffectModel{callID: "tc-1", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("executions=%d", got)
	}
	rows := env.rows()
	if len(rows) != 1 || rows[0].Status != "succeeded" {
		t.Fatalf("a lost ACK must be repaired by repeating the identical write, got %+v", rows)
	}

	second := runLife(t, env, &oneShotEffectModel{callID: "tc-2", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("re-delivery repeated a completed effect: executions=%d", got)
	}
	if len(second) == 0 || !strings.Contains(second[0], `"attempt":1`) {
		t.Fatalf("re-delivery must replay the recorded result, got %#v", second)
	}
}

// TestCoreMutationRedeliveryWithDifferentBodyIsRefused: the same run coordinate may
// not silently become a DIFFERENT mutation. State answers KEY_REUSE_MISMATCH for
// that (the durable step stream has the same rule via StepKey), and the only safe
// answer for a side effect is to refuse it, loudly.
func TestCoreMutationRedeliveryWithDifferentBodyIsRefused(t *testing.T) {
	env := newMutationEnv(t)
	exec := &mutatingExecutor{}

	runLife(t, env, &oneShotEffectModel{callID: "tc-1", args: `{"file":"report.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("life 1 executions=%d", got)
	}
	second := runLife(t, env, &oneShotEffectModel{callID: "tc-2", args: `{"file":"other.txt"}`}, exec)
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("a reused mutation coordinate executed a second body: executions=%d", got)
	}
	var joined string
	for _, s := range second {
		joined += s
	}
	if !strings.Contains(joined, contracts.ErrKeyReuse) {
		t.Fatalf("the KEY_REUSE refusal must reach the step stream, got %#v", second)
	}
}

// TestCoreMutationJournalClosesThroughCoordinator is the composed production path
// (AcceptTurn -> DispatchTurn -> real Core loop) rather than a hand-built request:
// what a human chat turn leaves in State must be a CLOSED journal, so any later
// reader -- reconciler, orphan sweep, auditor -- can tell that the effect completed.
func TestCoreMutationJournalClosesThroughCoordinator(t *testing.T) {
	ms := newMockState()
	srv := ms.Server()
	defer srv.Close()
	sc := space.NewStateClient(srv.URL, nil)

	exec := &mutatingExecutor{}
	reg := core.NewToolRegistry(exec)
	reg.Register(core.ToolDef{Name: "effect", Description: "an external effect"})
	rt := core.NewRuntime(core.RuntimeConfig{Model: &oneShotEffectModel{callID: "tc-1", args: `{"file":"report.txt"}`}, Tools: reg, State: sc})
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: rt, WorkerID: "w-core"})

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	accept, err := coord.AcceptTurn(context.Background(), "token", scope, space.AcceptTurnRequest{
		Key: "mut-" + contracts.NewID(), ContextID: "ctx-" + contracts.NewID(),
		Input: json.RawMessage(`"do the effect"`), MaxSteps: 6,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := coord.DispatchTurn(context.Background(), "token", scope); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	deadline := time.Now().Add(8 * time.Second)
	var status space.TurnStatus
	for time.Now().Before(deadline) {
		if tn, ok := coord.TurnSnapshot(accept.TurnID); ok && tn.Status.IsTerminal() {
			status = tn.Status
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status != space.TurnDone {
		t.Fatalf("expected done, got %s", status)
	}
	if got := exec.calls.Load(); got != 1 {
		t.Fatalf("executions=%d", got)
	}

	ms.mu.Lock()
	var rows []intentItem
	for _, it := range ms.intents {
		if it.Target == "effect" {
			rows = append(rows, it)
		}
	}
	ms.mu.Unlock()
	if len(rows) != 1 {
		t.Fatalf("one mutation must be one journal row, got %+v", rows)
	}
	if rows[0].Status != "succeeded" {
		t.Fatalf("a completed turn must leave a CLOSED journal, got %q (%+v)", rows[0].Status, rows[0])
	}

	// The journal rows are also the audit trail: the key must be deterministic
	// (derived from the run's mutation index), never a model-minted id, or a
	// re-delivery could never find the row it must not replay.
	if _, err := fmt.Sscanf(rows[0].Call, "%d", new(int)); err != nil {
		t.Fatalf("mutation call key %q must be the run's deterministic mutation index, got: %v", rows[0].Call, err)
	}
}
