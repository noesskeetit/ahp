package model

// The recorded provider is the contract, not a hint.
//
// Physical routing went wrong in this product once already: the decision named a
// provider and the transport ignored it. Per-copy clients fixed the dial; these
// legs close the other half -- that a generation admitted against one copy can
// never be SERVED by another, whatever the deployment happens to be configured
// with by the time it runs. That is the property a verifier needs to read
// "provider=X on this row" as evidence rather than as an annotation: the row
// chooses the socket, and a mismatch is refused rather than quietly re-attributed.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// recordedProvider reads the provider attribution straight off the ledger row --
// out of band, the way a verifier would.
func recordedProvider(t *testing.T, env *modelEnv, genID string) string {
	t.Helper()
	var provider string
	if err := env.pool.QueryRow(context.Background(),
		`SELECT provider FROM ahp_model.generations WHERE id=$1`, genID).Scan(&provider); err != nil {
		t.Fatalf("read the recorded provider: %v", err)
	}
	return provider
}

func pricedRequest() ChatRequest {
	return ChatRequest{Model: "model-under-test", Messages: []Message{{Role: "user", Content: "answer"}}}
}

// Leg 1: admission itself refuses a copy this deployment cannot serve. The
// check runs before any booking, so a refusal leaves nothing behind: no
// generation row, no quota window movement, and no dial of the copy that WAS
// configured -- substituting the available client is the defect this closes.
func TestGenerationIsNotAdmittedForACopyThisDeploymentCannotServe(t *testing.T) {
	ctx := context.Background()
	env, q, configuredCopy, otherCopy := quotaCopies(t)

	configured := &fakeInference{model: "model-under-test"}
	configured.streamText([]string{"must never be reached"}, Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2})
	svc := env.service(configured).WithQuota(q, configuredCopy)

	_, err := svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-immutable-1", Key: "im-gen-1",
		Request: pricedRequest(), Provider: otherCopy,
	})
	if err == nil {
		t.Fatal("a generation was admitted against a copy this deployment cannot serve: the provider claim is decorative")
	}
	if got := contracts.ReasonOf(err); got != ReasonProviderClientUnavailable {
		t.Fatalf("refusal reason = %q, want %q", got, ReasonProviderClientUnavailable)
	}
	if !strings.Contains(err.Error(), otherCopy) {
		t.Fatalf("the refusal must name the copy that cannot be served (%s), got: %v", otherCopy, err)
	}
	if configured.count() != 0 {
		t.Fatalf("the configured copy's client was dialled %d times for a request bound to another copy: substitution by the environment", configured.count())
	}
	var rows int
	if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_model.generations
 WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND key=$4`,
		env.scope.Tenant, env.scope.Subject, env.scope.Deployment, "im-gen-1").Scan(&rows); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if rows != 0 {
		t.Fatalf("a refused admission left %d generation rows: the refusal is an answer, not a partial write", rows)
	}
	if w := copyWindow(t, q, otherCopy, env.scope.Tenant); w.Requests != 0 {
		t.Fatalf("the unservable copy shows %d admitted requests: quota was booked for a request that was refused", w.Requests)
	}
}

// Leg 1b: post-admission immutability. A generation admitted while its copy was
// reachable, then executed by a deployment that no longer mounts that copy,
// refuses instead of running on the copy that happens to be available. The
// terminal record names the reason, and the recorded provider is not rewritten.
func TestAdmittedGenerationRefusesToRunOnAnotherCopyAfterAdmission(t *testing.T) {
	ctx := context.Background()
	env, q, configuredCopy, otherCopy := quotaCopies(t)

	configured := &fakeInference{model: "model-under-test"}
	configured.streamText([]string{"must never be reached"}, Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2})
	served := &fakeInference{model: "model-under-test"}
	served.streamText([]string{"served"}, Usage{PromptTokens: 3, CompletionTokens: 3, TotalTokens: 6})

	admitting := env.service(configured).
		WithQuota(q, configuredCopy).
		WithProviderClients(testProviderClients{otherCopy: served, configuredCopy: configured})
	gen, err := admitting.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-immutable-1b", Key: "im-gen-1b",
		Request: pricedRequest(), Provider: otherCopy,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if got := recordedProvider(t, env, gen.ID); got != otherCopy {
		t.Fatalf("recorded provider = %q, want the admitted copy %q", got, otherCopy)
	}
	bookedAtAdmission := copyWindow(t, q, otherCopy, env.scope.Tenant).Requests

	// The same ledger, a deployment that only has the environment's copy.
	strict := env.service(configured).WithQuota(q, configuredCopy)
	res, err := strict.Execute(ctx, env.scope, gen.ID)
	detail := res.Error
	if detail == "" {
		detail = fmt.Sprint(err)
	}
	if res.Status != GenFailed {
		t.Fatalf("status after executing a generation whose copy is not mounted = %s (%s), want %s: refusing by substitution would attribute spend to an upstream that never saw the request",
			res.Status, detail, GenFailed)
	}
	if !strings.Contains(detail, ReasonProviderClientUnavailable) || !strings.Contains(detail, otherCopy) {
		t.Fatalf("the terminal record must name the stable reason and the copy, got: %s", detail)
	}
	if served.count() != 0 || configured.count() != 0 {
		t.Fatalf("a refused generation dialled clients (recorded=%d configured=%d): nothing may be sent after the copy is found unservable", served.count(), configured.count())
	}
	if got := recordedProvider(t, env, gen.ID); got != otherCopy {
		t.Fatalf("a refused execution rewrote the provider to %q: history must not be edited into what was available", got)
	}
	if w := copyWindow(t, q, otherCopy, env.scope.Tenant); w.Requests != bookedAtAdmission {
		t.Fatalf("the refused attempt moved the quota window from %d to %d requests: no work, no spend", bookedAtAdmission, w.Requests)
	}
	if w := copyWindow(t, q, configuredCopy, env.scope.Tenant); w.Requests != 0 {
		t.Fatalf("the copy that was not chosen shows %d requests: attribution leaked", w.Requests)
	}
}

// Leg 2: with the right client mounted, the recorded copy wins over the
// configured one -- selection follows the ledger row, not the environment.
func TestRecordedProviderChoosesTheClientOverTheEnvironment(t *testing.T) {
	ctx := context.Background()
	env, q, configuredCopy, otherCopy := quotaCopies(t)

	configured := &fakeInference{model: "model-under-test"}
	configured.streamText([]string{"configured"}, Usage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10})
	served := &fakeInference{model: "model-under-test"}
	served.streamText([]string{"served"}, Usage{PromptTokens: 7, CompletionTokens: 3, TotalTokens: 10})

	svc := env.service(configured).
		WithQuota(q, configuredCopy).
		WithProviderClients(testProviderClients{otherCopy: served, configuredCopy: configured})

	gen, err := svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-immutable-2", Key: "im-gen-2",
		Request: pricedRequest(), Provider: otherCopy,
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if _, err := svc.Execute(ctx, env.scope, gen.ID); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if served.count() != 1 {
		t.Fatalf("the recorded copy's client was dialled %d times, want 1", served.count())
	}
	if configured.count() != 0 {
		t.Fatalf("the environment's copy was dialled %d times although the row names another: substitution", configured.count())
	}
	if got := recordedProvider(t, env, gen.ID); got != otherCopy {
		t.Fatalf("provider after the run = %q, want %q", got, otherCopy)
	}
}

// Leg 3: replaying the same operation key keeps the same attribution -- a retry
// cannot move a generation to a copy with more room.
func TestReplayKeepsTheRecordedProvider(t *testing.T) {
	ctx := context.Background()
	env, q, configuredCopy, otherCopy := quotaCopies(t)

	served := &fakeInference{model: "model-under-test"}
	served.streamText([]string{"served"}, Usage{PromptTokens: 2, CompletionTokens: 2, TotalTokens: 4})
	svc := env.service(served).
		WithQuota(q, configuredCopy).
		WithProviderClients(testProviderClients{otherCopy: served, configuredCopy: served})

	args := SubmitArgs{Scope: env.scope, Task: "task-immutable-3", Key: "im-gen-3", Request: pricedRequest(), Provider: otherCopy}
	first, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	replay, err := svc.Submit(ctx, args)
	if err != nil {
		t.Fatalf("replay submit: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("the same operation key produced two generations: %s then %s", first.ID, replay.ID)
	}
	if got := recordedProvider(t, env, replay.ID); got != otherCopy {
		t.Fatalf("the replay moved the attribution to %q, want %q: a retry may not shop for a copy with room", got, otherCopy)
	}
}
