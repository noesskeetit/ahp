package model

// W-1: the egress cause class must be proven end to end -- real guarded client,
// real classification, real budget and ledger -- not only by handing a synthetic
// sentinel to the service. The D2 accounting legs inject preSendRefusalError at
// the inference seam, so a mutation in the client that stops classifying a guard
// denial as pre-send cannot redden them. These legs route the actual
// QwenClient through Service.Execute: the negative one reddens under that
// mutation, the positive one is the stage-2 shape (an internal provider whose
// reply is journalled and settled).

import (
	"context"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
)

// guardRefusalClient is a real guarded client whose destination resolves into
// private space and was never registered.
func guardRefusalClient(t *testing.T) *QwenClient {
	t.Helper()
	secret := secretstore.QwenSecret{APIKey: "sk-w1-do-not-leak", BaseURL: "http://provider.internal/v1", Model: "qwen-test"}
	res := fixedResolver{m: map[string][]string{"provider.internal": {"172.18.0.6"}}}
	dialed := ""
	c, err := newQwenClient(secret, netpolicy.WithResolver(res), netpolicy.WithDialer(recordDialer{dialed: &dialed}))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	if dialed != "" {
		t.Fatalf("the fixture must never dial, dialed %s", dialed)
	}
	return c
}

func TestW1RealGuardRefusalSettlesOnPG(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	svc := env.service(guardRefusalClient(t))

	root, _ := env.budget.Root(ctx, env.scope, "task1", "root-"+env.u, 100)
	args := SubmitArgs{Scope: env.scope, Task: "task1", Key: "gen-" + env.u, Request: chatReq("hi"),
		Model: "qwen-test", BudgetID: root.ID, ReserveKey: "res-" + env.u, ReserveAmount: 15}
	g, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	done, err := svc.Execute(ctx, env.scope, g.ID)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if done.Status != GenFailed {
		t.Fatalf("status=%s, want FAILED", done.Status)
	}
	if !strings.Contains(done.Error, ReasonModelEgressRefusedBeforeSend) {
		t.Fatalf("the durable cause must be %q, got %q", ReasonModelEgressRefusedBeforeSend, done.Error)
	}
	if strings.Contains(done.Error, "ambiguous") {
		t.Fatalf("a refusal before the first byte must not read as ambiguous: %q", done.Error)
	}
	op, err := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if op.Status != operations.StatusFailed {
		t.Fatalf("ledger status=%s, want FAILED (never SIDE_EFFECT_UNKNOWN)", op.Status)
	}
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 0 || b.Reserved != 0 {
		t.Fatalf("nothing was sent, so nothing may be held or charged: spent=%d reserved=%d", b.Spent, b.Reserved)
	}
}

// TestW1RealGuardRoundTripSettlesOnPG is the stage-2 shape end to end: a provider
// reached through its registered RFC1918 origin, answering over the guard, with
// the reply journalled and the actual usage settled.
func TestW1RealGuardRoundTripSettlesOnPG(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	p := newUnixProvider(t, "sk-w2-do-not-leak")
	secret := privateSecret()
	secret.Model = "qwen-test"
	c, err := newQwenClient(secret, netpolicy.WithDialer(netDialerFunc(p.dialSocket)))
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	svc := env.service(c)

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
		t.Fatalf("status=%s error=%q, want COMPLETED", done.Status, done.Error)
	}
	if done.Output != "internal reply" {
		t.Fatalf("output=%q, want the provider reply", done.Output)
	}
	if done.Usage.TotalTokens != 18 {
		t.Fatalf("usage total=%d, want the 18 tokens the provider reported", done.Usage.TotalTokens)
	}
	b, _ := env.budget.Get(ctx, env.scope, root.ID)
	if b.Spent != 18 || b.Reserved != 0 {
		t.Fatalf("budget spent=%d reserved=%d, want spent=18 reserved=0", b.Spent, b.Reserved)
	}
	op, err := env.ledger.Get(ctx, env.scope, g.EffectKey)
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("ledger status=%s err=%v, want SUCCEEDED", op.Status, err)
	}
	if got := p.dialed.Load(); got != "172.18.0.6:8000" {
		t.Fatalf("the guard must dial the registered origin, dialed %v", got)
	}
	if p.hits.Load() != 1 {
		t.Fatalf("the provider must have served exactly one request, saw %d", p.hits.Load())
	}
}
