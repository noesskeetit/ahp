package model

// A refused base_url must survive to the durable record with its own reason.
// Otherwise the only fact a verifier reads is provider_client_unavailable, which
// is true but useless: it does not say that the copy was misconfigured, nor which
// branch. Both legs run on the real budget and ledger.

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// copySelector hands out a copy client, and can be flipped to the misconfigured
// state to represent a route or secret edited after the generation was admitted.
type copySelector struct {
	ok         *fakeInference
	badBaseURL string
	bad        bool
}

func (c *copySelector) InferenceFor(ctx context.Context, _ string) (Inference, error) {
	if !c.bad {
		return c.ok, nil
	}
	_, err := NewQwenClient(ctx, secretstore.QwenSecret{APIKey: "sk-x-do-not-leak", BaseURL: c.badBaseURL, Model: "qwen-test"})
	return nil, err
}

// Admission must refuse a copy whose base_url is refused, before booking quota or
// budget: nothing is reserved, so nothing has to be returned.
func TestBaseURLMisconfiguredCopyIsRefusedAtAdmission(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	sel := &copySelector{ok: &fakeInference{model: "qwen-test"}, badBaseURL: "https://api.example.com/v1/chat/completions", bad: true}
	svc := env.service(&fakeInference{model: "qwen-test"}).WithProviderClients(sel)

	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 15, Provider: "copy-bad"}
	if _, err := svc.Submit(ctx, args); err == nil {
		t.Fatal("submit must refuse a misconfigured copy")
	} else if got := contracts.ReasonOf(err); got != ReasonBaseURLDuplicatedPath {
		t.Fatalf("admission reason=%q, want %q (err=%v)", got, ReasonBaseURLDuplicatedPath, err)
	}
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Reserved != 0 || b.Spent != 0 {
		t.Fatalf("a refused submission must book nothing: reserved=%d spent=%d", b.Reserved, b.Spent)
	}
}

// The harder case is the one production actually hits: the copy was fine at
// admission and is misconfigured at dispatch (a route or secret edited in between,
// or a restart that loaded a different route). The terminal record must carry the
// configuration reason, the ledger must show a definite FAILED, and the
// reservation must come back.
func TestBaseURLMisconfiguredCopyCarriesItsOwnReasonToTheRecord(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	sel := &copySelector{ok: &fakeInference{model: "qwen-test"}, badBaseURL: "https://api.example.com:99999/v1"}
	svc := env.service(&fakeInference{model: "qwen-test"}).WithProviderClients(sel)

	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 15, Provider: "copy-bad"}
	g, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	sel.bad = true
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if done.Status != GenFailed {
		t.Fatalf("status=%s, want FAILED", done.Status)
	}
	if !strings.Contains(done.Error, ReasonBaseURLPortInvalid) {
		t.Fatalf("the durable cause must be %q, got %q", ReasonBaseURLPortInvalid, done.Error)
	}
	if strings.Contains(done.Error, ReasonProviderClientUnavailable) {
		t.Fatalf("the generic reason must not mask the configuration cause: %q", done.Error)
	}
	if strings.Contains(done.Error, "ambiguous") {
		t.Fatalf("a configuration refusal must not read as ambiguous: %q", done.Error)
	}
	op, err := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if op.Status != operations.StatusFailed {
		t.Fatalf("ledger status=%s, want FAILED", op.Status)
	}
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 0 || b.Reserved != 0 {
		t.Fatalf("nothing was sent, so nothing may be held or charged: spent=%d reserved=%d", b.Spent, b.Reserved)
	}
}
