package space_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestCancelTurnRejectedRevokeLeavesTurnUncanceled pins the DEFECT measured on the composed
// stand: when Identity refused to revoke the run's own grant, Space had ALREADY marked the
// turn canceled. The operator then saw a canceled turn whose credential was still live, and
// the 403 in the response contradicted the stored state. Under RequireRunGrant the revoke is
// mandatory, so a rejected revoke must change nothing at all.
func TestCancelTurnRejectedRevokeLeavesTurnUncanceled(t *testing.T) {
	f := newERunState(t, erunChildTask)
	issuer := &grantRec{childToken: "MINTED-CHILD-TOK", cancelFail: true}
	core := &claimableCore{block: make(chan struct{})}
	c := newERunCoordinator(f, core, issuer)

	owner := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "owner-a"}
	ctx := space.WithAuthorityTask(context.Background(), erunAuthTask)
	acc, err := c.AcceptTurn(ctx, erunIngressTok, owner, space.AcceptTurnRequest{
		Key: "k-cancel-fail", ContextID: "ctx-cancel-fail",
		Input: json.RawMessage(`"hold on"`), MaxSteps: 3,
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
	if st, _ := c.TurnSnapshot(acc.TurnID); st.Status != space.TurnRunning {
		t.Fatalf("status before cancel = %v, want running", st.Status)
	}

	err = c.CancelTurn(context.Background(), acc.TurnID, erunIngressTok)
	if err == nil {
		t.Fatal("cancel with a failing revoke returned nil; the run's grant is still live")
	}
	st, ok := c.TurnSnapshot(acc.TurnID)
	if !ok {
		t.Fatal("turn vanished from the snapshot")
	}
	if st.Status == space.TurnCanceled {
		t.Fatalf("cancel was REJECTED (%v) but the turn is stored as canceled", err)
	}
	if issuer.cancelN != 1 {
		t.Fatalf("revoke attempts = %d, want 1 (the attempt must still have been made)", issuer.cancelN)
	}
	if n := core.callCount(); n != 1 {
		t.Fatalf("Core executions = %d, want the run left untouched (1)", n)
	}

	// The same cancel succeeds once Identity accepts the revoke, and only then does the
	// turn become canceled and the execution stop.
	issuer.mu.Lock()
	issuer.cancelFail = false
	issuer.mu.Unlock()
	if err := c.CancelTurn(context.Background(), acc.TurnID, erunIngressTok); err != nil {
		t.Fatalf("cancel after the revoke succeeded: %v", err)
	}
	if st, _ := c.TurnSnapshot(acc.TurnID); st.Status != space.TurnCanceled {
		t.Fatalf("status after a successful revoke = %v, want canceled", st.Status)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && atomicActive(core) {
		time.Sleep(10 * time.Millisecond)
	}
	if atomicActive(core) {
		t.Fatal("the canceled run's execution is still active")
	}
	close(core.block)
}
