package model

import (
	"context"
	"os"
	"strings"
	"testing"
)

// DEF-A7-1. A live Qwen turn came back finish="length" with NO content and 128
// completion tokens charged. The gateway used to complete that as a success,
// because the streaming loop only ever looked at FinishReason=="tool_calls" and
// everything else fell through to "completed". An empty assistant message is not a
// completed turn: the run loop can neither act on it nor stop honestly on it, an
// artifact sealed from it is fabricated, and the operator gets no reason.
//
// PDF rev3.2 section 9 (corner cases) settles the shape: an abnormal end of a turn
// is "failed с причиной" (:3177, :3213), and every row there is a negative-test
// candidate. Deciding it inside Model Gateway is the ownership line :1626-1627 gives
// it -- "operations and attempts of generation". The refusal is terminal and
// charged: re-asking is the caller's bounded attempt, never a spin in here.
//
// These legs drive a fake Inference, so they are deterministic and touch no network;
// the live round-trip lives behind the `liveqwen` build tag (see live_qwen_test.go).

// requireModelDB keeps these legs from ever reading as green while uncounted: a
// missing DSN is a failure here, not a skip.
func requireModelDB(t *testing.T) {
	t.Helper()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("AHP_TEST_DATABASE_URL is not set: source the suite environment (an accounting leg that does not run proves nothing)")
	}
}

// finishStream yields text deltas (possibly none) and one terminal finish reason,
// with the usage the provider reported for the attempt.
func finishStream(f *fakeInference, texts []string, finish string, usage Usage) {
	f.newStr = func(ChatRequest) (Stream, error) {
		fs := &fakeStream{errAt: -1, usage: usage}
		for _, txt := range texts {
			fs.deltas = append(fs.deltas, Delta{Text: txt})
		}
		if finish != "" {
			fs.deltas = append(fs.deltas, Delta{FinishReason: finish})
		}
		return fs, nil
	}
}

const truncatedUsage = 138

func submitTruncatedTurn(t *testing.T, env *modelEnv, inf *fakeInference) (Generation, *Service) {
	t.Helper()
	ctx := context.Background()
	svc := env.service(inf)
	root, err := env.budget.Root(ctx, env.scope, "task-empty-"+env.u, "rb-"+env.u, 100_000)
	if err != nil {
		t.Fatalf("budget.Root: %v", err)
	}
	g, err := svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-empty-" + env.u, Key: "gen-" + env.u,
		Model: "qwen-test", Request: chatReq("say something"),
		BudgetID: root.ID, ReserveKey: "rk-" + env.u, ReserveAmount: 200,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	return done, svc
}

// TestEmptyCompletionAtLengthLimitFailsWithAReasonAndIsCharged is the measured case:
// no content, finish="length", tokens consumed. The verdict is FAILED with a named
// cause -- never a completion -- and the consumed tokens are charged, because they
// were spent whether or not anything usable came back.
func TestEmptyCompletionAtLengthLimitFailsWithAReasonAndIsCharged(t *testing.T) {
	requireModelDB(t)
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	finishStream(inf, nil, "length", Usage{PromptTokens: 10, CompletionTokens: 128, TotalTokens: truncatedUsage})

	done, _ := submitTruncatedTurn(t, env, inf)

	if done.Status == GenCompleted {
		t.Fatalf("an empty completion at the token limit was reported as %s: the caller would seal an artifact from nothing", done.Status)
	}
	if done.Status != GenFailed {
		t.Fatalf("status = %s, want %s", done.Status, GenFailed)
	}
	if !IsEmptyCompletionRefusal(done) {
		t.Fatalf("the refusal does not carry its cause: %q", done.Error)
	}
	if !strings.Contains(done.Error, "finish=length") {
		t.Fatalf("the refusal does not name the provider's stop reason: %q", done.Error)
	}
	if done.Usage.TotalTokens != truncatedUsage {
		t.Fatalf("usage = %+v, want the %d tokens the provider consumed", done.Usage, truncatedUsage)
	}

	ctx := context.Background()
	root, err := env.budget.Root(ctx, env.scope, "task-empty-"+env.u, "rb-"+env.u, 100_000)
	if err != nil {
		t.Fatalf("budget.Root: %v", err)
	}
	b, err := env.budget.Get(ctx, env.scope, root.ID)
	if err != nil {
		t.Fatalf("budget.Get: %v", err)
	}
	if b.Spent != truncatedUsage {
		t.Fatalf("tree spent = %d, want %d (the tokens were really consumed; hiding them is how cost stops matching usage)", b.Spent, truncatedUsage)
	}
	if b.Reserved != 0 {
		t.Fatalf("reserved = %d after a terminal refusal, want 0", b.Reserved)
	}
	if b.Spent > b.Limit {
		t.Fatalf("OVERDRAFT: spent=%d > limit=%d", b.Spent, b.Limit)
	}
	stored, err := env.gens.Get(ctx, env.scope, done.ID)
	if err != nil {
		t.Fatalf("gens.Get: %v", err)
	}
	if stored.Status != GenFailed || !strings.HasPrefix(stored.Error, EmptyCompletionCause) {
		t.Fatalf("stored generation = %s/%q, want failed with the empty-completion cause", stored.Status, stored.Error)
	}
	if strings.TrimSpace(stored.Output) != "" {
		t.Fatalf("a refused turn must not carry output, got %q", stored.Output)
	}
}

// TestEmptyCompletionWithoutAStatedFinishReasonNamesUnreported keeps the refusal
// honest when the provider says nothing: the operator sees "unreported" rather than
// an empty field, and the turn still ends failed.
func TestEmptyCompletionWithoutAStatedFinishReasonNamesUnreported(t *testing.T) {
	requireModelDB(t)
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	finishStream(inf, []string{"   "}, "stop", Usage{PromptTokens: 4, CompletionTokens: 1, TotalTokens: 5})

	done, _ := submitTruncatedTurn(t, env, inf)

	if done.Status != GenFailed || !IsEmptyCompletionRefusal(done) {
		t.Fatalf("whitespace-only output must end failed, got %s/%q", done.Status, done.Error)
	}
	if !strings.Contains(done.Error, "finish=stop") {
		t.Fatalf("the stated finish reason must survive into the cause: %q", done.Error)
	}
}

// TestTruncatedCompletionWithTextStillCompletes is the boundary of the fix: text
// that ran into the token limit is still the model's answer, and refusing it would
// turn a usable partial into an error. Only a turn with NOTHING to hand over is a
// refusal, so no accepted path changes behaviour.
func TestTruncatedCompletionWithTextStillCompletes(t *testing.T) {
	requireModelDB(t)
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	finishStream(inf, []string{"a partial answer that ran", " into the limit"}, "length",
		Usage{PromptTokens: 10, CompletionTokens: 128, TotalTokens: 138})

	done, _ := submitTruncatedTurn(t, env, inf)

	if done.Status != GenCompleted {
		t.Fatalf("status = %s (err=%q), want %s: text is text even when it is cut short", done.Status, done.Error, GenCompleted)
	}
	if !strings.Contains(done.Output, "ran into the limit") {
		t.Fatalf("the partial answer must be preserved, got %q", done.Output)
	}
	if done.Usage.TotalTokens != truncatedUsage {
		t.Fatalf("usage = %+v, want %d", done.Usage, truncatedUsage)
	}
}

// TestEmptyCompletionRefusalIsTerminalAndConvergesOnReplay: the verdict does not
// re-ask the provider and does not move the budget twice. A second Execute is the
// same terminal fact -- which is what makes the caller's retry bound meaningful
// rather than a slow overdraft.
func TestEmptyCompletionRefusalIsTerminalAndConvergesOnReplay(t *testing.T) {
	requireModelDB(t)
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	finishStream(inf, nil, "length", Usage{PromptTokens: 10, CompletionTokens: 128, TotalTokens: truncatedUsage})

	done, svc := submitTruncatedTurn(t, env, inf)
	before := inf.count()

	again, err := svc.Execute(context.Background(), env.scope, done.ID)
	if err != nil {
		t.Fatalf("replay execute: %v", err)
	}
	if again.Status != done.Status || again.Error != done.Error {
		t.Fatalf("replay changed the verdict: %s/%q vs %s/%q", again.Status, again.Error, done.Status, done.Error)
	}
	if after := inf.count(); after != before {
		t.Fatalf("provider called %d times after a terminal refusal, want %d: the refusal must not turn into a spin", after, before)
	}
	ctx := context.Background()
	root, err := env.budget.Root(ctx, env.scope, "task-empty-"+env.u, "rb-"+env.u, 100_000)
	if err != nil {
		t.Fatalf("budget.Root: %v", err)
	}
	b, err := env.budget.Get(ctx, env.scope, root.ID)
	if err != nil {
		t.Fatalf("budget.Get: %v", err)
	}
	if b.Spent != truncatedUsage {
		t.Fatalf("tree spent = %d after a replay, want %d (convergence: one attempt, one charge)", b.Spent, truncatedUsage)
	}
}
