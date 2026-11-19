package compute_test

import (
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func newLedgerComputer(t *testing.T, st *compute.Store) (contracts.Scope, string) {
	t.Helper()
	sc := scope()
	c := mustCreate(t, st, sc, compute.ClassS, compute.ProfileEphemeral, "")
	return sc, c.ID
}

// TestLedgerExecIdempotencyAndKeyReuse pins the ledger's idempotency contract:
// a replayed idempotency key returns the SAME record, is dispatchable only while
// the original is still ACCEPTED, and reusing the key with a different body is a
// hard KEY_REUSE rejection (no silent overwrite).
func TestLedgerExecIdempotencyAndKeyReuse(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc, cid := newLedgerComputer(t, st)
	key := "idem-" + contracts.NewID()

	first, may1, err := st.Exec(c, sc, cid, key, "echo hello", "/tmp")
	if err != nil {
		t.Fatalf("first Exec: %v", err)
	}
	if !may1 {
		t.Fatal("first Exec must be dispatchable")
	}
	if first.Status != compute.CommandAccepted {
		t.Fatalf("first status = %q, want accepted", first.Status)
	}

	replay, may2, err := st.Exec(c, sc, cid, key, "echo hello", "/tmp")
	if err != nil {
		t.Fatalf("replay Exec: %v", err)
	}
	if replay.ID != first.ID {
		t.Fatalf("replay returned a different command id: %q != %q", replay.ID, first.ID)
	}
	if !may2 {
		t.Fatal("replay of a still-accepted command must remain dispatchable")
	}

	// Reusing the same key with a different canonical body is rejected.
	if _, _, err := st.Exec(c, sc, cid, key, "echo different", "/tmp"); contracts.Code(err) != string(contracts.ErrKeyReuse) {
		t.Fatalf("key reuse err code = %v, want KEY_REUSE", err)
	}

	// Once dispatched, a replay is no longer dispatchable (must not double-run).
	if _, err := st.MarkDispatched(c, sc, cid, first.ID); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	if _, may3, err := st.Exec(c, sc, cid, key, "echo hello", "/tmp"); err != nil {
		t.Fatalf("replay after dispatch: %v", err)
	} else if may3 {
		t.Fatal("replay of a dispatched command must NOT be dispatchable")
	}
}

func TestLedgerComplete(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc, cid := newLedgerComputer(t, st)

	// exit 0 -> succeeded
	ok, _, err := st.Exec(c, sc, cid, "k-ok-"+contracts.NewID(), "true", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkDispatched(c, sc, cid, ok.ID); err != nil {
		t.Fatal(err)
	}
	rec, err := st.Complete(c, sc, cid, ok.ID, 0, "out", "")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if rec.Status != compute.CommandSucceeded || !rec.HasExit || rec.ExitCode != 0 {
		t.Fatalf("ok rec = %+v, want succeeded exit 0", rec)
	}
	// Idempotent replay of Complete returns the settled record unchanged.
	again, _ := st.Complete(c, sc, cid, ok.ID, 1, "more", "")
	if again.Status != compute.CommandSucceeded || again.ExitCode != 0 {
		t.Fatalf("Complete replay mutated record: %+v", again)
	}

	// non-zero exit -> failed
	bad, _, err := st.Exec(c, sc, cid, "k-bad-"+contracts.NewID(), "false", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkDispatched(c, sc, cid, bad.ID); err != nil {
		t.Fatal(err)
	}
	rec, err = st.Complete(c, sc, cid, bad.ID, 3, "", "boom")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != compute.CommandFailed || rec.ExitCode != 3 {
		t.Fatalf("bad rec = %+v, want failed exit 3", rec)
	}

	// Completing an ACCEPTED (never dispatched) command is a conflict.
	pending, _, err := st.Exec(c, sc, cid, "k-pend-"+contracts.NewID(), "sleep 1", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Complete(c, sc, cid, pending.ID, 0, "", ""); contracts.Code(err) != string(contracts.ErrConflict) {
		t.Fatalf("complete-before-dispatch err = %v, want CONFLICT", err)
	}
}

func TestLedgerCancel(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc, cid := newLedgerComputer(t, st)

	// ACCEPTED -> cancelled immediately.
	acc, _, err := st.Exec(c, sc, cid, "k-c1-"+contracts.NewID(), "echo", "")
	if err != nil {
		t.Fatal(err)
	}
	rec, phase, err := st.Cancel(c, sc, cid, acc.ID)
	if err != nil {
		t.Fatalf("Cancel accepted: %v", err)
	}
	if phase != compute.CancelCancelled || rec.Status != compute.CommandCancelled {
		t.Fatalf("cancel accepted: phase=%q status=%q, want cancelled/cancelled", phase, rec.Status)
	}
	// Cancelling a terminal command is a conflict.
	if _, _, err := st.Cancel(c, sc, cid, acc.ID); contracts.Code(err) != string(contracts.ErrConflict) {
		t.Fatalf("re-cancel err = %v, want CONFLICT", err)
	}

	// DISPATCHED -> under review; later completes as cancelled (flag authoritative).
	disp, _, err := st.Exec(c, sc, cid, "k-c2-"+contracts.NewID(), "echo", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkDispatched(c, sc, cid, disp.ID); err != nil {
		t.Fatal(err)
	}
	rec, phase, err = st.Cancel(c, sc, cid, disp.ID)
	if err != nil {
		t.Fatalf("Cancel dispatched: %v", err)
	}
	if phase != compute.CancelUnderReview || rec.Status != compute.CommandDispatched {
		t.Fatalf("cancel dispatched: phase=%q status=%q, want under_review/dispatched", phase, rec.Status)
	}
	done, err := st.Complete(c, sc, cid, disp.ID, 0, "", "")
	if err != nil {
		t.Fatalf("Complete after cancel: %v", err)
	}
	if done.Status != compute.CommandCancelled {
		t.Fatalf("post-cancel complete status = %q, want cancelled", done.Status)
	}
}

func TestLedgerRestartAndFence(t *testing.T) {
	st := newStore(t)
	c := ctx(t)
	sc, cid := newLedgerComputer(t, st)

	// Restart is legal only while ACCEPTED.
	acc, _, err := st.Exec(c, sc, cid, "k-r-"+contracts.NewID(), "echo", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Restart(c, sc, cid, acc.ID); err != nil {
		t.Fatalf("Restart accepted: %v", err)
	}
	disp, _, err := st.Exec(c, sc, cid, "k-r2-"+contracts.NewID(), "echo", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkDispatched(c, sc, cid, disp.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Restart(c, sc, cid, disp.ID); contracts.Code(err) != string(contracts.ErrConflict) {
		t.Fatalf("Restart dispatched err = %v, want CONFLICT", err)
	}

	// Fence of ACCEPTED -> fenced forever.
	fenceKey := "k-f-" + contracts.NewID()
	fa, _, err := st.Exec(c, sc, cid, fenceKey, "echo", "")
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st.Fence(c, sc, cid, fa.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != compute.CommandFenced {
		t.Fatalf("fence accepted status = %q, want fenced", rec.Status)
	}
	// A fenced key can never be dispatched.
	if _, may, err := st.Exec(c, sc, cid, fenceKey, "echo", ""); err != nil || may {
		t.Fatalf("replay of fenced key: may=%v err=%v, want false/nil", may, err)
	}

	// Fence of DISPATCHED -> side_effect_unknown (never claim ran or didn't).
	fd, _, err := st.Exec(c, sc, cid, "k-f2-"+contracts.NewID(), "echo", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkDispatched(c, sc, cid, fd.ID); err != nil {
		t.Fatal(err)
	}
	rec, err = st.Fence(c, sc, cid, fd.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != compute.CommandSideEffectUnknown {
		t.Fatalf("fence dispatched status = %q, want side_effect_unknown", rec.Status)
	}
}
