package main

// emil_parent_attestation_test.go -- DEF-H10-15: the parent a delegated child
// declares must be the task the hop's OWN credential attests, not the State run id.
//
// Measured on VM2 (reports/mac-director/hegel-chain-e3177b5c.json): a real
// browser-initiated run whose model emitted emil.delegate was refused by the
// receiving deployment with reason parent_attestation_task_mismatch, because the
// hop declared task_id=20261101T224632.048158540Z_66gqbbwlik5jyczrfrel4jnksq (the
// State run id) while presenting a credential that attests
// rtask-s-rootuser:k-e317c1 (the identity grant task, == ahp_state.runs.task_id).
// internal/gateway/outbox.go already declares the grant task canonical for
// ahp_parent_task; these tests hold the producer to that rule.

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type parentHarness struct {
	att    runScopeAttester
	signer *auth.Signer
	aud    string
	real   contracts.Scope
	// runID is the STATE run id: what RunBinding.TaskID carries and what artifacts
	// and budget-tree rows are keyed by. It is a bookkeeping id, not a credential id.
	runID string
	// grantTask is the identity grant task the run's own credential carries, the
	// exact value ahp_state.runs.task_id stores for a browser run.
	grantTask string
}

func newParentHarness(t *testing.T) *parentHarness {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	signer, err := auth.NewSigner("h1015", "h1015-1", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	u := fmt.Sprint(time.Now().UnixNano())
	real := contracts.Scope{Tenant: "h1015" + u, Deployment: "runner-a", Subject: "s-rootuser"}
	aud := "ahp"
	return &parentHarness{
		att: runScopeAttester{verifier: auth.NewVerifier("h1015", map[string]ed25519.PublicKey{"h1015-1": pub}),
			taskAudience: aud, workload: auth.WorkloadID(real)},
		signer:    signer,
		aud:       aud,
		real:      real,
		runID:     "20261102T000000.000000000Z_" + u,
		grantTask: "rtask-s-rootuser:k-" + u,
	}
}

// runToken mints the run's own credential the way Identity does: task kind, bound
// to a task and a root, bound to THIS deployment's workload.
func (h *parentHarness) runToken(t *testing.T, task string, claims auth.Claims) string {
	t.Helper()
	if claims.Kind == "" {
		claims.Kind = auth.KindTask
	}
	if claims.Audience == "" {
		claims.Audience = h.aud
	}
	if claims.Subject == "" {
		claims.Subject = h.real.Subject
	}
	if claims.Tenant == "" {
		claims.Tenant = h.real.Tenant
	}
	if claims.Task == "" {
		claims.Task = task
	}
	if claims.Root == "" {
		claims.Root = "root-" + task
	}
	if claims.Context == "" {
		claims.Context = "ctx-" + task
	}
	if claims.Grant == "" {
		claims.Grant = "grant-" + task
	}
	if claims.Deployment == "" {
		claims.Deployment = h.real.Deployment
	}
	if len(claims.Actors) == 0 {
		claims.Actors = []string{h.real.Deployment}
	}
	tok, err := h.signer.Sign(claims, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return tok
}

// authorityFor rebuilds what internal/core hands a delegating tool mid-run: the
// carried scope (a browser turn carries NO deployment), the run's own credential,
// and the run binding whose TaskID is the State run id.
func (h *parentHarness) authorityFor(t *testing.T, bearer string) *emil.RunAuthority {
	t.Helper()
	carried := contracts.Scope{Tenant: h.real.Tenant, Subject: h.real.Subject}
	ctx := core.WithRunScope(t.Context(), carried, bearer)
	ctx = core.WithRunBinding(ctx, core.RunBinding{
		ContextID: "ctx-" + h.grantTask, TaskID: h.runID, MaxSteps: 3, MaxStepsStated: true,
	})
	return emilRunAuthority(ctx)
}

// TestDelegatedParentIsDeclaredAsTheAttestedGrantTask is the red test: before this
// fix the hop declared the State run id and the receiving plane refused with
// parent_attestation_task_mismatch. The declared parent must equal the task the
// presented credential attests, and the run id must still travel where it belongs.
func TestDelegatedParentIsDeclaredAsTheAttestedGrantTask(t *testing.T) {
	h := newParentHarness(t)
	prev := runAuthorityScope
	runAuthorityScope = h.att
	t.Cleanup(func() { runAuthorityScope = prev })

	ra := h.authorityFor(t, h.runToken(t, h.grantTask, auth.Claims{}))
	if ra == nil {
		t.Fatal("a run with its own credential must get an authority")
	}
	got, _ := ra.Labels[emil.MetaParentTask].(string)
	if got != h.grantTask {
		t.Fatalf("ahp_parent_task must be the credential's grant task %q, got %q -- a receiving plane attests the credential, so declaring %q is a guaranteed parent_attestation_task_mismatch",
			h.grantTask, got, h.runID)
	}
	if ra.TaskID != h.runID {
		t.Fatalf("the State run id must still name the run for bookkeeping, got %q", ra.TaskID)
	}
	if ra.DeclaredParent != h.grantTask {
		t.Fatalf("the hop must journal what it declared as parent, got %q", ra.DeclaredParent)
	}
}

// TestForeignRunCredentialDoesNotLendItsTaskIsTheAntiForgeryHalf: a credential
// signed for ANOTHER workload cannot make this run claim that task as its parent.
// The declaration falls back to the run's own id (announced, not silent), so the
// fix cannot be used to point a child at somebody else's lineage.
func TestForeignRunCredentialDoesNotLendItsTask(t *testing.T) {
	h := newParentHarness(t)
	prev := runAuthorityScope
	runAuthorityScope = h.att
	t.Cleanup(func() { runAuthorityScope = prev })

	foreign := h.runToken(t, "rtask-victim:k-stolen", auth.Claims{
		Deployment: "other-vc", Actors: []string{"other-vc"},
	})
	ra := h.authorityFor(t, foreign)
	got, _ := ra.Labels[emil.MetaParentTask].(string)
	if got != h.runID {
		t.Fatalf("a credential for another workload must not lend its task id; expected the run's own id %q, got %q", h.runID, got)
	}
}

// TestUnverifiableCredentialFallsBackAnnouncedly keeps a forged/garbage bearer from
// producing a parent claim at all: the run id is declared (the pre-fix behaviour,
// auditable in the WARN line) and never an attacker-chosen task.
func TestUnverifiableCredentialFallsBackAnnouncedly(t *testing.T) {
	h := newParentHarness(t)
	prev := runAuthorityScope
	runAuthorityScope = h.att
	t.Cleanup(func() { runAuthorityScope = prev })

	ra := h.authorityFor(t, "not-a-jwt")
	got, _ := ra.Labels[emil.MetaParentTask].(string)
	if got != h.runID {
		t.Fatalf("an unverifiable bearer must declare the run's own id, got %q", got)
	}
}

// TestAttesterWithoutPublishedKeysAttestsNoTask is the composition half: a runner
// that cannot verify (no published keys, or no pinned workload) must report an
// error rather than a task id, so the WARN path is what runs.
func TestAttesterWithoutPublishedKeysAttestsNoTask(t *testing.T) {
	h := newParentHarness(t)
	tok := h.runToken(t, h.grantTask, auth.Claims{})
	if _, err := (runScopeAttester{}).attestedTask(tok); err == nil {
		t.Fatal("an attester with no published keys must attest no task")
	}
	unpinned := runScopeAttester{verifier: h.att.verifier, taskAudience: h.att.taskAudience}
	if _, err := unpinned.attestedTask(tok); err == nil {
		t.Fatal("an attester with no pinned workload must attest no task")
	}
	if got, err := h.att.attestedTask(tok); err != nil || got != h.grantTask {
		t.Fatalf("the pinned attester must name %q, got %q (err %v)", h.grantTask, got, err)
	}
}
