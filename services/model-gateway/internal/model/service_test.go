package model

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// ---- deterministic fakes ---------------------------------------------------

type fakeStream struct {
	deltas  []Delta
	usage   Usage
	idx     int
	errAt   int // emit err when idx==errAt (before reading delta); -1 disables
	err     error
	onDelta func(emitted int)
}

func (s *fakeStream) Next(ctx context.Context) (Delta, error) {
	select {
	case <-ctx.Done():
		return Delta{}, ctx.Err()
	default:
	}
	if s.errAt >= 0 && s.idx == s.errAt {
		s.idx++
		return Delta{}, s.err
	}
	if s.idx >= len(s.deltas) {
		return Delta{}, ErrStreamDone
	}
	d := s.deltas[s.idx]
	s.idx++
	if s.onDelta != nil {
		s.onDelta(s.idx)
	}
	return d, nil
}
func (s *fakeStream) Usage() Usage { return s.usage }
func (s *fakeStream) Close() error { return nil }

type fakeInference struct {
	mu     sync.Mutex
	calls  int
	model  string
	newStr func(req ChatRequest) (Stream, error)
}

func (f *fakeInference) Stream(ctx context.Context, req ChatRequest) (Stream, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.newStr(req)
}
func (f *fakeInference) Model() string { return f.model }
func (f *fakeInference) count() int    { f.mu.Lock(); defer f.mu.Unlock(); return f.calls }
func (f *fakeInference) streamText(deltas []string, usage Usage) {
	f.newStr = func(ChatRequest) (Stream, error) {
		fs := &fakeStream{errAt: -1, usage: usage}
		for _, d := range deltas {
			fs.deltas = append(fs.deltas, Delta{Text: d})
		}
		return fs, nil
	}
}

// ---- env helper ------------------------------------------------------------

type modelEnv struct {
	pool   *pgxpool.Pool
	gens   *GenerationStore
	budget *BudgetStore
	ledger *operations.Store
	scope  contracts.Scope
	u      string
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func newModelEnv(t *testing.T) *modelEnv {
	t.Helper()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)

	gens := NewGenerationStore(pool)
	if err := gens.Migrate(ctx); err != nil {
		t.Fatalf("gens migrate: %v", err)
	}
	budget := NewBudgetStore(pool)
	if err := budget.Migrate(ctx); err != nil {
		t.Fatalf("budget migrate: %v", err)
	}
	ledger, err := operations.New(pool, "ahp_model")
	if err != nil {
		t.Fatalf("operations.New: %v", err)
	}
	if err := ledger.Migrate(ctx); err != nil {
		t.Fatalf("ledger migrate: %v", err)
	}
	u := randHex(6)
	scope := contracts.Scope{Tenant: "t-" + u, Subject: "s-" + u, Deployment: "d-" + u}
	return &modelEnv{pool: pool, gens: gens, budget: budget, ledger: ledger, scope: scope, u: u}
}

func (e *modelEnv) service(inf Inference) *Service {
	return NewService(e.gens, e.budget, e.ledger, inf)
}

func chatReq(text string) ChatRequest {
	return ChatRequest{Messages: []Message{{Role: "user", Content: text}}}
}

// ---- tests -----------------------------------------------------------------

func TestModelSubmitIdempotencyAndKeyReuse(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"ok"}, Usage{TotalTokens: 3})
	svc := env.service(inf)

	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"), Model: "qwen-test"}
	g1, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit1: %v", err)
	}
	g2, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit replay: %v", err)
	}
	if g1.ID != g2.ID {
		t.Fatalf("replay not idempotent: %s vs %s", g1.ID, g2.ID)
	}
	if g1.Status != GenPending {
		t.Fatalf("status = %s, want PENDING", g1.Status)
	}
	// Same key, different canonical request => key reuse conflict.
	bad := args
	bad.Request = chatReq("totally different")
	if _, err := svc.Submit(ctx, bad); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("key reuse: want KEY_REUSE, got %v", err)
	}
}

func TestModelSubmitAdmissionBlocksOverBudget(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"x"}, Usage{TotalTokens: 1})
	svc := env.service(inf)

	root, err := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 5)
	if err != nil {
		t.Fatalf("root: %v", err)
	}
	args := SubmitArgs{
		Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 100,
	}
	if _, err := svc.Submit(ctx, args); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("over-budget submit: want CONFLICT, got %v", err)
	}
	// No orphan generation must exist: admission failed before persistence.
	_, err = env.gens.Get(ctx, env.scope, args.Key)
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("expected no generation after failed admission, got err=%v", err)
	}
	// No reservation was held either.
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Reserved != 0 {
		t.Fatalf("reservation leaked: reserved=%d", b.Reserved)
	}
}

func TestModelExecuteCompletesAndSettles(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	usage := Usage{PromptTokens: 4, CompletionTokens: 6, TotalTokens: 10}
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"Hel", "lo", " world"}, usage)
	svc := env.service(inf)

	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 20}
	g, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if done.Status != GenCompleted {
		t.Fatalf("status = %s, want COMPLETED", done.Status)
	}
	if done.Output != "Hello world" {
		t.Fatalf("output = %q", done.Output)
	}
	if done.Usage.TotalTokens != 10 {
		t.Fatalf("usage total = %d, want 10", done.Usage.TotalTokens)
	}
	// Budget: reserved 20, charged actual 10, released remainder.
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 10 || b.Reserved != 0 {
		t.Fatalf("budget spent=%d reserved=%d, want spent=10 reserved=0", b.Spent, b.Reserved)
	}
	// Ledger journalled the side effect as SUCCEEDED.
	op, err := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("ledger status = %s err=%v", op.Status, err)
	}
	// Re-execution is idempotent and must not call the provider again.
	before := inf.count()
	again, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil || again.Status != GenCompleted {
		t.Fatalf("re-execute: %v %+v", err, again)
	}
	if inf.count() != before {
		t.Fatalf("re-execute re-called provider: %d vs %d", inf.count(), before)
	}
}

func TestModelOverrunRecordsActualAndDeniesNewWork(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	// Provider reports more tokens than reserved. Accounting records the bill;
	// future admission must refuse the overdrawn task.
	usage := Usage{PromptTokens: 30, CompletionTokens: 40, TotalTokens: 70}
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"a", "b"}, usage)
	svc := env.service(inf)

	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 50)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 10}
	g, _ := svc.Submit(ctx, args)
	if _, err := svc.Execute(ctx, env.scope, g.ID); err != nil {
		t.Fatalf("execute: %v", err)
	}
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 70 {
		t.Fatalf("actual usage lost: spent=%d", b.Spent)
	}
	if _, err := env.budget.Reserve(ctx, env.scope, root.ID, "next", 1); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("overdrawn task admitted: %v", err)
	}
}

func TestModelCancelMidStream(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	usage := Usage{TotalTokens: 30}
	inf := &fakeInference{model: "qwen-test"}
	svc := env.service(inf)
	svc.checkN = 1 // check cancellation every delta

	var cancelAt = 3
	var genID string
	inf.newStr = func(ChatRequest) (Stream, error) {
		fs := &fakeStream{errAt: -1, usage: usage}
		for i := 0; i < 10; i++ {
			fs.deltas = append(fs.deltas, Delta{Text: fmt.Sprintf("d%d", i)})
		}
		fs.onDelta = func(emitted int) {
			if emitted == cancelAt {
				_, _ = env.gens.RequestCancel(context.Background(), env.scope, genID)
			}
		}
		return fs, nil
	}

	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"), Model: "qwen-test"}
	g, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	genID = g.ID
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if done.Status != GenCanceled {
		t.Fatalf("status = %s, want CANCELED", done.Status)
	}
	if done.Output == "" {
		t.Fatal("expected partial output before cancel")
	}
	if len(done.Output) >= len("d0d1d2d3d4d5d6d7d8d9") {
		t.Fatalf("stream was not cut short: %q", done.Output)
	}
	// Canceled mid-stream is a terminal failure for the ledger (completed=true).
	op, _ := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if op.Status != operations.StatusFailed {
		t.Fatalf("ledger status = %s, want FAILED", op.Status)
	}
}

func TestModelAmbiguousNotReplayedOnDispatchContention(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"should-not-run"}, Usage{TotalTokens: 5})
	svc := env.service(inf)

	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"), Model: "qwen-test"}
	g, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	// Simulate a crashed prior attempt that already grabbed the dispatch right
	// but never completed (status DISPATCHED).
	if _, err := env.ledger.Dispatch(ctx, env.scope, g.EffectKey); err != nil {
		t.Fatalf("pre-dispatch: %v", err)
	}
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	// The provider must NEVER be invoked (no automatic replay of an ambiguous
	// side effect), and the generation must fail as ambiguous.
	if inf.count() != 0 {
		t.Fatalf("ambiguous path re-called provider %d times", inf.count())
	}
	if done.Status != GenFailed || !strings.Contains(done.Error, "ambiguous") {
		t.Fatalf("status=%s error=%q, want FAILED/ambiguous", done.Status, done.Error)
	}
	op, _ := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("ledger status = %s, want SIDE_EFFECT_UNKNOWN", op.Status)
	}
}

func TestModelRecoveryUsesStoredSucceededResult(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"x"}, Usage{TotalTokens: 1})
	svc := env.service(inf)

	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 8}
	g, _ := svc.Submit(ctx, args)

	// Prior attempt dispatched AND completed in the ledger but the process died
	// before finalising the generation row. Recovery must reuse the stored
	// result, settle the budget, and never call the provider again.
	res, _ := json.Marshal(resultPayload{Status: GenCompleted, Usage: Usage{TotalTokens: 6}})
	if _, err := env.ledger.Dispatch(ctx, env.scope, g.EffectKey); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, err := env.ledger.Complete(ctx, env.scope, g.EffectKey, res, false); err != nil {
		t.Fatalf("complete: %v", err)
	}
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if inf.count() != 0 {
		t.Fatalf("recovery re-called provider %d times", inf.count())
	}
	if done.Status != GenCompleted || done.Usage.TotalTokens != 6 {
		t.Fatalf("recovered gen = %+v", done)
	}
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 6 {
		t.Fatalf("settled spent=%d, want 6", b.Spent)
	}
}

func TestModelTransportErrorDuringStreamIsAmbiguous(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.newStr = func(ChatRequest) (Stream, error) {
		return &fakeStream{
			deltas: []Delta{{Text: "partial"}},
			usage:  Usage{TotalTokens: 2},
			errAt:  1, // after one delta, fail with a transport error
			err:    &transportError{err: fmt.Errorf("connection reset")},
		}, nil
	}
	svc := env.service(inf)
	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 15}
	g, _ := svc.Submit(ctx, args)
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if done.Status != GenFailed || !strings.Contains(done.Error, "ambiguous") {
		t.Fatalf("status=%s error=%q, want FAILED/ambiguous", done.Status, done.Error)
	}
	// Ambiguous charges the full reservation (never a silent partial release).
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 15 {
		t.Fatalf("ambiguous settle spent=%d, want reserved 15", b.Spent)
	}
}

func TestModelCancelPendingGeneration(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"never"}, Usage{TotalTokens: 9})
	svc := env.service(inf)

	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"), Model: "qwen-test"}
	g, _ := svc.Submit(ctx, args)
	c, err := svc.Cancel(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if c.Status != GenCanceled {
		t.Fatalf("pending cancel status = %s, want CANCELED", c.Status)
	}
	// Execute of a canceled generation is a no-op that never calls the provider.
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute after cancel: %v", err)
	}
	if done.Status != GenCanceled || inf.count() != 0 {
		t.Fatalf("execute after cancel: status=%s calls=%d", done.Status, inf.count())
	}
}
