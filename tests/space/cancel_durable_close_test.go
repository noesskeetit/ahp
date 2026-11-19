package space_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestCancelClosesTheRunWithTheCallersLiveCredential pins what cancel has to leave behind.
//
// Measured on the composed stand after the run credential became a real Identity grant token:
// CancelTurn revoked the grant (correct, and it is the security point of cancel), and the worker's
// next durable write then failed with the revoked credential. The coordinator read that failed
// write as a durability hole and stored the run as `failed`, so a user cancel looked like a crash,
// and the run row was never closed - which leaves a canceled run claimable by an orphan sweep.
//
// The rule this test enforces:
//   - the revoke still happens FIRST and with the run's own child token;
//   - the run row is then closed durably as `canceled` with the CALLER's still-live State token,
//     never with the credential cancel just killed;
//   - when the worker later discovers that its own writes are refused, the terminal status stays
//     `canceled` and no `failed` overwrite reaches State.
func TestCancelClosesTheRunWithTheCallersLiveCredential(t *testing.T) {
	f := newERunState(t, erunChildTask)
	issuer := &grantRec{childToken: "MINTED-CHILD-TOK"}
	core := &claimableCore{block: make(chan struct{})}
	c := newERunCoordinator(f, core, issuer)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	ctx := space.WithAuthorityTask(context.Background(), erunAuthTask)
	acc, err := c.AcceptTurn(ctx, erunIngressTok, owner, space.AcceptTurnRequest{
		Key: "k-cancel-durable", ContextID: "ctx-cancel-durable",
		Input: json.RawMessage(`"work until canceled"`), MaxSteps: 5,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := c.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && core.callCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if core.callCount() != 1 {
		t.Fatalf("Core never started (calls=%d)", core.callCount())
	}
	drainReleases(f.releaseCh)
	f.reset()

	if err := c.CancelTurn(context.Background(), acc.TurnID, erunCallerTok); err != nil {
		t.Fatalf("CancelTurn: %v", err)
	}

	// 1. the run's own grant was revoked, presented with the run's OWN live credential. Under the
	// frozen E-run seam that is the token State reissued for the durable child task at ClaimRun
	// (never the ingress authority, never the delegate token it replaced).
	issuer.mu.Lock()
	cancelN, cancelToken := issuer.cancelN, issuer.cancelToken
	issuer.mu.Unlock()
	if cancelN != 1 || cancelToken != erunClaimToken {
		t.Fatalf("cancel must revoke the run's own grant with the run's reissued child token %q, got n=%d bearer=%q",
			erunClaimToken, cancelN, cancelToken)
	}
	// 2. and the run row was closed as canceled, presented with the CALLER's token.
	select {
	case st := <-f.releaseCh:
		if st != "canceled" {
			t.Fatalf("cancel must close the durable run as canceled, got %q", st)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel never closed the durable run: an unclosed run stays claimable by an orphan sweep")
	}
	f.mu.Lock()
	bearers := append([]string(nil), f.bearers["Release"]...)
	f.mu.Unlock()
	if len(bearers) == 0 {
		t.Fatal("no Release reached State during cancel")
	}
	if last := bearers[len(bearers)-1]; last != "Bearer "+erunCallerTok {
		t.Fatalf("the durable close must be presented with the caller's live token, got %q", last)
	}
	if st, ok := c.TurnSnapshot(acc.TurnID); !ok || st.Status != space.TurnCanceled {
		t.Fatalf("turn after cancel = %v, want canceled", st.Status)
	}

	// 3. now let the blocked worker continue: its credential is revoked, so its writes are
	// refused. The terminal status must remain canceled and must not be rewritten as failed.
	f.forbidAppends()
	close(core.block)
	waitFor := time.Now().Add(8 * time.Second)
	for time.Now().Before(waitFor) && atomicActive(core) {
		time.Sleep(10 * time.Millisecond)
	}
	st, ok := c.TurnSnapshot(acc.TurnID)
	if !ok {
		t.Fatal("turn vanished after the worker gave up")
	}
	if st.Status != space.TurnCanceled {
		t.Fatalf("a canceled run whose credential died must stay canceled, got %v", st.Status)
	}
	drained := drainReleases(f.releaseCh)
	for _, s := range drained {
		if s == "failed" {
			t.Fatalf("a cancel must never be rewritten as a failure: State saw %q", s)
		}
	}
}

// TestCancelWithRejectedRevokeClosesNothing is the fail-closed counterpart: while Identity refuses
// the revoke, neither the in-memory turn nor the durable run may move.
func TestCancelWithRejectedRevokeClosesNothing(t *testing.T) {
	f := newERunState(t, erunChildTask)
	issuer := &grantRec{childToken: "MINTED-CHILD-TOK", cancelFail: true}
	core := &claimableCore{block: make(chan struct{})}
	c := newERunCoordinator(f, core, issuer)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	ctx := space.WithAuthorityTask(context.Background(), erunAuthTask)
	acc, err := c.AcceptTurn(ctx, erunIngressTok, owner, space.AcceptTurnRequest{
		Key: "k-cancel-noclose", ContextID: "ctx-cancel-noclose",
		Input: json.RawMessage(`"hold"`), MaxSteps: 3,
	})
	if err != nil {
		t.Fatalf("AcceptTurn: %v", err)
	}
	if err := c.DispatchTurn(context.Background(), erunIngressTok, owner); err != nil {
		t.Fatalf("DispatchTurn: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && core.callCount() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	drainReleases(f.releaseCh)
	f.reset()

	err = c.CancelTurn(context.Background(), acc.TurnID, erunCallerTok)
	if err == nil || !strings.Contains(err.Error(), "cancel") {
		t.Fatalf("a rejected revoke must surface as a cancel error, got %v", err)
	}
	if st, _ := c.TurnSnapshot(acc.TurnID); st.Status == space.TurnCanceled {
		t.Fatal("the turn may not be canceled while its grant is still live")
	}
	select {
	case st := <-f.releaseCh:
		t.Fatalf("nothing may be closed durably while the grant is live, saw %q", st)
	case <-time.After(300 * time.Millisecond):
	}
	f.mu.Lock()
	releases := len(f.bearers["Release"])
	f.mu.Unlock()
	if releases != 0 {
		t.Fatalf("a rejected revoke must not Release the run (%d calls)", releases)
	}
	close(core.block)
}
