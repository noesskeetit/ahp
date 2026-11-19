package integration_test

import (
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B138 §9.6 Cancel and Revoke: 5 situations.
// §9.6 Отмена и отзыв: cancel in-flight / dispatched / terminal commands,
// cancel-then-complete interaction, and double-cancel idempotency.

// 1. Cancel an ACCEPTED command: closes immediately as CANCELLED.
func TestB138Sit1_CancelAccepted(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b138-s1-" + contracts.NewID(),
	}, "b138-s1-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Exec creates an ACCEPTED command.
	rec, mayDispatch, err := e.store.Exec(ctx(t), sc, comp.ID, "b138-s1-cmd", "echo hi", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !mayDispatch {
		t.Fatal("Exec: mayDispatch=false on fresh command")
	}
	if rec.Status != compute.CommandAccepted {
		t.Fatalf("after Exec: status=%s want accepted", rec.Status)
	}

	// Cancel: closes immediately.
	got, phase, err := e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if got.Status != compute.CommandCancelled {
		t.Fatalf("after Cancel: status=%s want cancelled", got.Status)
	}
	if phase != compute.CancelCancelled {
		t.Fatalf("phase=%q want %q", phase, compute.CancelCancelled)
	}
	t.Log("Sit1: cancel ACCEPTED → immediate CANCELLED (CancelCancelled)")
}

// 2. Cancel a DISPATCHED command: held for reconciliation (under_review).
func TestB138Sit2_CancelDispatched(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b138-s2-" + contracts.NewID(),
	}, "b138-s2-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b138-s2-cmd", "echo dispatch", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	// Dispatch: the command is now in flight.
	_, err = e.store.MarkDispatched(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}

	// Cancel a dispatched command: cannot confirm it hasn't run.
	got, phase, err := e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if phase != compute.CancelUnderReview {
		t.Fatalf("phase=%q want %q", phase, compute.CancelUnderReview)
	}
	// Status remains DISPATCHED (we can't change what the runner is doing).
	if got.Status != compute.CommandDispatched {
		t.Fatalf("status=%s want dispatched (held for review)", got.Status)
	}
	t.Log("Sit2: cancel DISPATCHED → under_review, status stays dispatched")
}

// 3. Cancel a terminal (SUCCEEDED) command: CONFLICT.
func TestB138Sit3_CancelTerminal(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b138-s3-" + contracts.NewID(),
	}, "b138-s3-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Drive to terminal: Exec → Dispatch → Complete.
	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b138-s3-cmd", "echo done", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	_, err = e.store.MarkDispatched(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	_, err = e.store.Complete(ctx(t), sc, comp.ID, rec.ID, 0, "done\n", "")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Cancel after terminal: must be rejected.
	_, _, err = e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if err == nil {
		t.Fatal("Cancel on SUCCEEDED: want error, got nil")
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("code=%s want CONFLICT", contracts.Code(err))
	}
	t.Log("Sit3: cancel SUCCEEDED → CONFLICT (settled command is immutable)")
}

// 4. Double cancel: first succeeds, second is CONFLICT.
func TestB138Sit4_DoubleCancel(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b138-s4-" + contracts.NewID(),
	}, "b138-s4-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b138-s4-cmd", "echo x", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}

	// First cancel: succeeds (ACCEPTED → CANCELLED).
	_, phase1, err := e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("first Cancel: %v", err)
	}
	if phase1 != compute.CancelCancelled {
		t.Fatalf("first phase=%q want cancelled", phase1)
	}

	// Second cancel: CONFLICT (already terminal).
	_, _, err = e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if err == nil {
		t.Fatal("second Cancel: want error, got nil")
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("second code=%s want CONFLICT", contracts.Code(err))
	}
	t.Log("Sit4: double cancel → first CANCELLED, second CONFLICT (idempotency guard)")
}

// 5. Cancel + Complete interaction: cancel_requested makes Complete settle CANCELLED.
func TestB138Sit5_CancelThenComplete(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b138-s5-" + contracts.NewID(),
	}, "b138-s5-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Dispatch a command.
	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b138-s5-cmd", "echo work", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	_, err = e.store.MarkDispatched(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}

	// Cancel while dispatched: sets cancel_requested=true.
	_, phase, err := e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if phase != compute.CancelUnderReview {
		t.Fatalf("phase=%q want under_review", phase)
	}

	// Runner completes with exit 0: but cancel_requested overrides → CANCELLED.
	done, err := e.store.Complete(ctx(t), sc, comp.ID, rec.ID, 0, "output\n", "")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if done.Status != compute.CommandCancelled {
		t.Fatalf("Complete after cancel: status=%s want cancelled (cancel_requested wins)", done.Status)
	}
	t.Log("Sit5: cancel REQUESTED then Complete(exit=0) → settles CANCELLED (revoke is authoritative)")
}
