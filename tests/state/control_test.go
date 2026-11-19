package stateint

import (
	"bytes"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// TestControlLegalholdBlocksDelete proves the control-plane legalhold gate that
// retention and legal counsel depend on: a held thread cannot be deleted, the
// hold is auditable via its reason, removing the hold re-enables deletion, and
// the deletion cascades so the thread is gone (NOT_FOUND) afterwards. These run
// with a human admin credential; task credentials cannot administer legalholds.
func TestControlLegalholdBlocksDelete(t *testing.T) {
	e := newEnv(t)
	th := newTaskThread(e, t)
	// newTaskThread leaves a scope token (subject=user-ada) installed — the same
	// subject that owns the thread. Administrative writes use a separate credential.

	mustCode(t, e.client.SetLegalhold(e.ctx, th.ID, "litigation-hold"), contracts.ErrForbidden, "task cannot set legalhold")
	e.adminCall(t, "SetLegalhold", map[string]string{"thread_id": th.ID, "reason": "litigation-hold"}, nil)

	mustCode(t, e.client.DeleteThread(e.ctx, th.ID), contracts.ErrForbidden,
		"DeleteThread while under legalhold must be forbidden")

	// The thread must still be fully present while held.
	if _, err := e.client.GetThread(e.ctx, th.ID); err != nil {
		t.Fatalf("held thread disappeared: %v", err)
	}

	mustCode(t, e.client.RemoveLegalhold(e.ctx, th.ID), contracts.ErrForbidden, "task cannot remove legalhold")
	e.adminCall(t, "RemoveLegalhold", map[string]string{"thread_id": th.ID}, nil)
	requireNoErr(t, e.client.DeleteThread(e.ctx, th.ID), "DeleteThread after hold lifted")

	_, derr := e.client.GetThread(e.ctx, th.ID)
	mustCode(t, derr, contracts.ErrNotFound, "GetThread after cascade delete")
}

// TestControlDeleteSubjectSkipsHeld proves subject-level retention: DeleteSubject
// wipes every thread the subject owns EXCEPT those pinned by a legalhold, and it
// reports exactly how many it removed. This is the GDPR-style "right to be
// forgotten, minus lawful-hold" cascade the control plane must honour.
func TestControlDeleteSubjectSkipsHeld(t *testing.T) {
	e := newEnv(t)
	held := newTaskThread(e, t)
	free1 := newTaskThread(e, t)
	free2 := newTaskThread(e, t)

	e.adminCall(t, "SetLegalhold", map[string]string{"thread_id": held.ID, "reason": "audit-hold"}, nil)

	_, err := e.client.DeleteSubject(e.ctx)
	mustCode(t, err, contracts.ErrForbidden, "task cannot delete subject")
	var result struct {
		Deleted int64 `json:"deleted"`
	}
	e.adminCall(t, "DeleteSubject", map[string]any{}, &result)
	deleted := result.Deleted
	if deleted != 2 {
		t.Fatalf("DeleteSubject removed %d threads, want 2 (the two unheld)", deleted)
	}

	// The held thread survives the subject wipe; the unheld ones are gone.
	if _, err := e.client.GetThread(e.ctx, held.ID); err != nil {
		t.Fatalf("held thread must survive DeleteSubject: %v", err)
	}
	_, f1err := e.client.GetThread(e.ctx, free1.ID)
	mustCode(t, f1err, contracts.ErrNotFound, "free1 deleted")
	_, f2err := e.client.GetThread(e.ctx, free2.ID)
	mustCode(t, f2err, contracts.ErrNotFound, "free2 deleted")
}

// TestControlExportImportScopeCascade proves faithful, hook-free export/import
// into a DIFFERENT tenant/deployment scope. The import allocates a fresh thread
// id and re-materialises, byte-for-byte: every run, every checkpoint with its
// opaque blob bytes (verified through GetPart), every pending write (verified
// through GetWrites), the ordered stream, and the inbox — without any agent
// runtime hook or lease. The source thread is left byte-for-byte untouched.
// This is the portable-subject migration path.
func TestControlExportImportScopeCascade(t *testing.T) {
	src := newEnv(t)

	// Build a source thread under an active lease carrying a checkpoint with a
	// real opaque blob, a pending write, two ordered stream events and one inbox
	// message, so the snapshot must preserve every State subsystem.
	lp := src.claimFirst(t, "w-export")
	f := lp.fence
	secret := []byte{0x00, 0xca, 0xfe, 0xba, 0xbe, 0x01, 0xff}
	cpID := "cp-export-1"

	_, err := src.client.PutCheckpoint(src.ctx, f, cp(cpID, 1, secret), "cpk-"+contracts.NewID())
	requireNoErr(t, err, "PutCheckpoint")
	requireNoErr(t, src.client.PutWrites(src.ctx, f, "lg", cpID, []sdk.PendingWrite{
		{Task: "task-a", Index: 1, Channel: "messages", Data: []byte("partial-result")},
	}), "PutWrites")
	_, err = src.client.AppendStream(src.ctx, f, "evt-"+contracts.NewID(), "token",
		pj(map[string]any{"t": "hello"}))
	requireNoErr(t, err, "AppendStream")
	_, err = src.client.AppendStream(src.ctx, f, "evt-"+contracts.NewID(), "token",
		pj(map[string]any{"t": "world"}))
	requireNoErr(t, err, "AppendStream #2")
	_, err = src.client.Enqueue(src.ctx, lp.thread.ID, "msg-"+contracts.NewID(), "note",
		pj(map[string]any{"m": "ping"}))
	requireNoErr(t, err, "Enqueue")

	// The ordinary source reader retains its task token; export uses separate admin authority.
	src.setToken(src.signTask(t, "task-"+contracts.NewID()))
	_, err = src.client.ExportThread(src.ctx, lp.thread.ID)
	mustCode(t, err, contracts.ErrForbidden, "task cannot export thread")
	var exp sdk.ControlExport
	src.adminCall(t, "ExportThread", map[string]string{"thread_id": lp.thread.ID}, &exp)
	if exp.Thread.ID != lp.thread.ID {
		t.Fatalf("export thread id %q != source %q", exp.Thread.ID, lp.thread.ID)
	}
	if len(exp.Checkpoints) != 1 {
		t.Fatalf("export checkpoints = %d, want 1", len(exp.Checkpoints))
	}
	if len(exp.Writes) != 1 || len(exp.Writes[0].Writes) != 1 {
		t.Fatalf("export writes = %+v, want one checkpoint with one write", exp.Writes)
	}
	srcStreamBefore, err := src.client.ReadStream(src.ctx, lp.thread.ID, 0, 100)
	requireNoErr(t, err, "ReadStream source before import")
	if len(exp.Stream) != len(srcStreamBefore) || len(exp.Stream) < 2 {
		t.Fatalf("export stream count %d, source %d", len(exp.Stream), len(srcStreamBefore))
	}
	if len(exp.Inbox) < 1 {
		t.Fatalf("export inbox = %d, want >= 1", len(exp.Inbox))
	}

	// Import into another deployment of the SAME identity: that is what
	// export/import exists for (PDF rev3.2 - the hand-off between drivers).
	// Another tenant or another subject is refused outright, which
	// TestImportThreadRefusesForeignIdentityOnTheWire pins over HTTP.
	dst := newEnvScoped(t, src.tenant, "d-driver2-"+contracts.NewID())
	dst.setToken(dst.signTask(t, "task-"+contracts.NewID()))
	imported, err := dst.client.ImportThread(dst.ctx, exp)
	requireNoErr(t, err, "ImportThread")
	if imported.ID == lp.thread.ID {
		t.Fatal("import must allocate a NEW thread id, got the source id")
	}
	if imported.Scope.Deployment != dst.deployment {
		t.Fatalf("imported thread scope = %+v, want deployment %q", imported.Scope, dst.deployment)
	}

	// Every run is re-materialised in the destination thread.
	dstRuns, err := dst.client.ListRuns(dst.ctx, imported.ID)
	requireNoErr(t, err, "ListRuns imported")
	if len(dstRuns) != len(exp.Runs) {
		t.Fatalf("imported runs = %d, want %d", len(dstRuns), len(exp.Runs))
	}

	// The checkpoint survives with identical opaque bytes, read back via GetPart.
	dstCPs, err := dst.client.ListCheckpoints(dst.ctx, imported.ID, "lg", 100)
	requireNoErr(t, err, "ListCheckpoints imported")
	if len(dstCPs) != 1 || dstCPs[0].ID != cpID {
		t.Fatalf("imported checkpoints = %+v, want one with id %q", dstCPs, cpID)
	}
	if len(dstCPs[0].Parts) != 1 {
		t.Fatalf("imported checkpoint parts = %+v, want 1", dstCPs[0].Parts)
	}
	gotPart, err := dst.client.GetPart(dst.ctx, imported.ID, "lg",
		dstCPs[0].Parts[0].Channel, dstCPs[0].Parts[0].Version)
	requireNoErr(t, err, "GetPart imported")
	if !bytes.Equal(gotPart, secret) {
		t.Fatalf("imported blob bytes = %x, want %x", gotPart, secret)
	}

	// The pending write survives with identical bytes.
	gotWrites, err := dst.client.GetWrites(dst.ctx, imported.ID, "lg", cpID)
	requireNoErr(t, err, "GetWrites imported")
	if len(gotWrites) != 1 || string(gotWrites[0].Data) != "partial-result" {
		t.Fatalf("imported pending writes = %+v, want one 'partial-result'", gotWrites)
	}

	// The ordered stream survives with order and payload preserved.
	dstStream, err := dst.client.ReadStream(dst.ctx, imported.ID, 0, 100)
	requireNoErr(t, err, "ReadStream imported")
	if len(dstStream) != len(srcStreamBefore) {
		t.Fatalf("imported stream = %d, want %d", len(dstStream), len(srcStreamBefore))
	}
	for i := range dstStream {
		if dstStream[i].Payload == nil || dstStream[i].Sequence != int64(i+1) {
			t.Fatalf("imported stream event %d malformed: %+v", i, dstStream[i])
		}
	}

	// The inbox survives.
	dstInbox, err := dst.client.ReadInbox(dst.ctx, imported.ID, 0, 100)
	requireNoErr(t, err, "ReadInbox imported")
	if len(dstInbox) < 1 {
		t.Fatalf("imported inbox = %d events, want >= 1", len(dstInbox))
	}

	// The source thread and its ordered stream are completely untouched.
	srcStreamAfter, err := src.client.ReadStream(src.ctx, lp.thread.ID, 0, 100)
	requireNoErr(t, err, "ReadStream source after import")
	if len(srcStreamAfter) != len(srcStreamBefore) {
		t.Fatalf("source stream mutated by import: %d -> %d", len(srcStreamBefore), len(srcStreamAfter))
	}
	for i := range srcStreamBefore {
		if srcStreamAfter[i].ID != srcStreamBefore[i].ID {
			t.Fatalf("source stream event %d changed identity across import", i)
		}
	}
}

// TestControlDeleteThreadCascadesWrites proves the control-plane hard-delete
// leaves NO orphaned retention data behind: after DeleteThread the checkpoint
// rows AND their pending-write rows for that thread vanish at the storage layer
// (FK ON DELETE CASCADE on checkpoint_writes.thread_id), not merely disappear
// from the API surface. A leak here would silently retain opaque checkpoint
// payloads for a thread a subject asked to forget — a retention/PII bug the
// control plane must never have.
func TestControlDeleteThreadCascadesWrites(t *testing.T) {
	e := newEnv(t)
	lp := e.claimFirst(t, "w-cascade")
	f := lp.fence
	cpID := "cp-cascade-1"

	_, err := e.client.PutCheckpoint(e.ctx, f, cp(cpID, 1, []byte{0xde, 0xad, 0xbe, 0xef}), "cpk-"+contracts.NewID())
	requireNoErr(t, err, "PutCheckpoint")
	requireNoErr(t, e.client.PutWrites(e.ctx, f, "lg", cpID, []sdk.PendingWrite{
		{Task: "task-a", Index: 1, Channel: "messages", Data: []byte("durable-partial")},
	}), "PutWrites")

	// Precondition: storage really holds one checkpoint row and one pending-write
	// row for this thread, so the post-delete zero below is meaningful.
	var cpBefore, wrBefore int
	requireNoErr(t, e.pool.QueryRow(e.ctx,
		"SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id=$1", lp.thread.ID).Scan(&cpBefore), "count checkpoints before")
	requireNoErr(t, e.pool.QueryRow(e.ctx,
		"SELECT count(*) FROM ahp_state.checkpoint_writes WHERE thread_id=$1", lp.thread.ID).Scan(&wrBefore), "count writes before")
	if cpBefore != 1 || wrBefore != 1 {
		t.Fatalf("precondition storage = (checkpoints=%d writes=%d), want (1,1)", cpBefore, wrBefore)
	}

	// Control-plane delete needs a subject-bearing scope token, not the lease token.
	e.setToken(e.signTask(t, "task-"+contracts.NewID()))
	requireNoErr(t, e.client.DeleteThread(e.ctx, lp.thread.ID), "DeleteThread")

	var cpAfter, wrAfter int
	requireNoErr(t, e.pool.QueryRow(e.ctx,
		"SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id=$1", lp.thread.ID).Scan(&cpAfter), "count checkpoints after")
	requireNoErr(t, e.pool.QueryRow(e.ctx,
		"SELECT count(*) FROM ahp_state.checkpoint_writes WHERE thread_id=$1", lp.thread.ID).Scan(&wrAfter), "count writes after")
	if cpAfter != 0 {
		t.Fatalf("DeleteThread left %d orphaned checkpoint rows for thread %s", cpAfter, lp.thread.ID)
	}
	if wrAfter != 0 {
		t.Fatalf("DeleteThread left %d orphaned pending-write rows (retention leak) for thread %s", wrAfter, lp.thread.ID)
	}
}
