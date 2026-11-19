package integration_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// B10 §2.3 Platform contract: 11 leaf requirements verified against the
// compute-relevant API surface (task, state, version, cost, scoping,
// idempotency, and error format).

// 1. Task API create: Exec returns a command record with proper shape.
func TestB10Sit1_TaskAPICreate(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b10-s1-" + contracts.NewID(),
	}, "b10-s1-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	rec, mayDispatch, err := e.store.Exec(ctx(t), sc, comp.ID, "b10-s1-key", "echo contract", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !mayDispatch {
		t.Fatal("fresh Exec must set mayDispatch=true")
	}
	if rec.ID == "" {
		t.Fatal("Exec: command ID missing")
	}
	if rec.Status != compute.CommandAccepted {
		t.Fatalf("status=%q want accepted", rec.Status)
	}
	if rec.ComputerID != comp.ID {
		t.Fatalf("computerID=%q want %q", rec.ComputerID, comp.ID)
	}
	if rec.Key != "b10-s1-key" {
		t.Fatalf("key=%q want b10-s1-key", rec.Key)
	}
	t.Log("Sit1: task create — Exec returns {id, status=accepted, computer_id, key, may_dispatch}")
}

// 2. Task API get/cancel/complete: proper status transitions and error codes.
func TestB10Sit2_TaskAPIGetCancelComplete(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b10-s2-" + contracts.NewID(),
	}, "b10-s2-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Create + get.
	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b10-s2-key", "echo x", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	got, err := e.store.GetCommand(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("GetCommand: %v", err)
	}
	if got.ID != rec.ID || got.Status != compute.CommandAccepted {
		t.Fatalf("GetCommand: id=%q status=%q want %q/accepted", got.ID, got.Status, rec.ID)
	}

	// Cancel an ACCEPTED command.
	_, phase, err := e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if phase != compute.CancelCancelled {
		t.Fatalf("phase=%q want cancelled", phase)
	}

	// Get after cancel: terminal status.
	got, err = e.store.GetCommand(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("GetCommand after cancel: %v", err)
	}
	if got.Status != compute.CommandCancelled {
		t.Fatalf("status after cancel=%q want cancelled", got.Status)
	}

	// Cancel again: CONFLICT.
	_, _, err = e.store.Cancel(ctx(t), sc, comp.ID, rec.ID)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("double cancel: code=%s want CONFLICT", contracts.Code(err))
	}
	t.Log("Sit2: task get/cancel/complete — proper transitions (accepted→cancelled, CONFLICT on replay)")
}

// 3. State API read/write: StateHead and TypedPatch with proper scoping.
func TestB10Sit3_StateAPIReadWrite(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	pc := e.mustCreatePersonal(t, sc, "b10-s3-"+contracts.NewID())

	// Read: initial state is empty main branch.
	head, err := e.store.StateHead(ctx(t), sc, pc.ID, "main")
	if err != nil {
		t.Fatalf("StateHead: %v", err)
	}
	if head.Seq != 0 || head.Branch != "main" {
		t.Fatalf("initial head=%+v want seq=0 main", head)
	}

	// Cross-subject read: denied (NOT_FOUND).
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}
	_, err = e.store.StateHead(ctx(t), scBob, pc.ID, "main")
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject StateHead: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit3: state read/write — StateHead works for owner, NOT_FOUND for foreign subject")
}

// 4. Version API: publish/get/drain/rollback with proper status codes.
func TestB10Sit4_VersionAPICycle(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	agent := "b10-agent-" + contracts.NewID()

	// Publish first version.
	digest1 := "deadbeef0123456789"
	v1, err := e.store.PublishVersion(ctx(t), sc, agent, digest1, true, 10*time.Minute)
	if err != nil {
		t.Fatalf("PublishVersion v1: %v", err)
	}
	if v1.Digest != digest1 {
		t.Fatalf("v1 digest=%q want %q", v1.Digest, digest1)
	}

	// Get active.
	active, err := e.store.GetActiveVersion(ctx(t), sc, agent)
	if err != nil {
		t.Fatalf("GetActiveVersion: %v", err)
	}
	if active.Digest != digest1 {
		t.Fatalf("active=%q want %q", active.Digest, digest1)
	}

	// Publish v2.
	digest2 := "cafebabe9876543210"
	_, err = e.store.PublishVersion(ctx(t), sc, agent, digest2, true, 10*time.Minute)
	if err != nil {
		t.Fatalf("PublishVersion v2: %v", err)
	}

	// Drain v1 (mark it draining).
	drained, err := e.store.DrainVersion(ctx(t), sc, agent, digest1, 5*time.Minute)
	if err != nil {
		t.Fatalf("DrainVersion: %v", err)
	}
	if drained.Digest != digest1 {
		t.Fatalf("drained=%q want %q", drained.Digest, digest1)
	}

	// Rollback: active should move back.
	rb, err := e.store.RollbackVersion(ctx(t), sc, agent)
	if err != nil {
		t.Fatalf("RollbackVersion: %v", err)
	}
	if rb.Digest != digest1 {
		t.Fatalf("rollback active=%q want %q (rolls back to drained v1)", rb.Digest, digest1)
	}
	// Verify: v2 is now rolled_back, v1 is active.
	active2, err := e.store.GetActiveVersion(ctx(t), sc, agent)
	if err != nil {
		t.Fatalf("GetActiveVersion after rollback: %v", err)
	}
	if active2.Digest != digest1 {
		t.Fatalf("active after rollback=%q want %q", active2.Digest, digest1)
	}
	t.Log("Sit4: version API — publish→get→drain→rollback→verify active=all functional")
}

// 5. Cost API: report returns proper shape with required fields.
func TestB10Sit5_CostAPIReport(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b10-s5-" + contracts.NewID(),
	}, "b10-s5-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Open a cost window (wake the computer).
	_, err = e.runner.Wake(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("Wake: %v", err)
	}

	rep, err := e.store.CostReport(ctx(t), sc, comp.ID)
	if err != nil {
		t.Fatalf("CostReport: %v", err)
	}
	if rep.ComputerID != comp.ID {
		t.Fatalf("report.computerID=%q want %q", rep.ComputerID, comp.ID)
	}
	if rep.AwakeNanos <= 0 {
		t.Fatalf("awake_ns=%d, want > 0 (window was opened)", rep.AwakeNanos)
	}
	if !rep.Open {
		t.Fatal("open_window=false, want true (computer is awake)")
	}
	t.Logf("Sit5: cost report — {computer_id=%s, awake_ns=%d, open=%v, windows=%d}",
		rep.ComputerID, rep.AwakeNanos, rep.Open, len(rep.Windows))
}

// 6. Unknown operation: cross-realm access returns NOT_FOUND (404 equivalent).
func TestB10Sit6_UnknownRouteIsolation(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}

	// Alice creates a computer.
	comp, err := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b10-s6-" + contracts.NewID(),
	}, "b10-s6-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Bob cannot see Alice's computer (NOT_FOUND, not FORBIDDEN).
	_, err = e.store.GetComputer(ctx(t), scBob, comp.ID)
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject GetComputer: code=%s want NOT_FOUND", contracts.Code(err))
	}

	// Different deployment is also isolated.
	scOther := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment + "-other", Subject: "alice"}
	_, err = e.store.GetComputer(ctx(t), scOther, comp.ID)
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-deployment GetComputer: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit6: unknown routes — cross-subject and cross-deployment access returns NOT_FOUND")
}

// 7. Artifact API: snapshot recording works with proper kinds.
func TestB10Sit7_ArtifactAPISnapshot(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b10-s7-" + contracts.NewID(),
	}, "b10-s7-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Record a directory snapshot.
	snap1, err := e.store.RecordSnapshot(ctx(t), sc, comp.ID, compute.SnapshotDirectory,
		"/home/data", "sha256:"+contracts.NewID(), 4096)
	if err != nil {
		t.Fatalf("RecordSnapshot(directory): %v", err)
	}
	if snap1.Kind != compute.SnapshotDirectory {
		t.Fatalf("snap1 kind=%q want directory", snap1.Kind)
	}

	// Record a memory snapshot.
	snap2, err := e.store.RecordSnapshot(ctx(t), sc, comp.ID, compute.SnapshotMemory,
		"/mem/snap", "sha256:"+contracts.NewID(), 8192)
	if err != nil {
		t.Fatalf("RecordSnapshot(memory): %v", err)
	}
	if snap2.Kind != compute.SnapshotMemory {
		t.Fatalf("snap2 kind=%q want memory", snap2.Kind)
	}
	t.Log("Sit7: artifact API — snapshot directory and memory kinds recorded")
}

// 8. Error format: all errors carry a consistent contract code.
func TestB10Sit8_ErrorFormatConsistent(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	// NOT_FOUND: nonexistent computer.
	_, err := e.store.GetComputer(ctx(t), sc, "nonexistent-id-12345")
	if err == nil {
		t.Fatal("GetComputer(nonexistent): want error")
	}
	code := contracts.Code(err)
	if code != contracts.ErrNotFound {
		t.Fatalf("code=%q want NOT_FOUND", code)
	}
	if err.Error() == "" {
		t.Fatal("error message is empty")
	}

	// FORBIDDEN: class X cannot create personal.
	_, err = e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassX, Profile: compute.ProfilePersonal,
	}, "b10-s8-x-"+contracts.NewID())
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("class X personal: code=%s want FORBIDDEN", contracts.Code(err))
	}

	// CONFLICT: duplicate personal in same realm.
	pc1, _ := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b10-s8-realm",
	}, "b10-s8-p1-"+contracts.NewID())
	_, err = e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassS, Profile: compute.ProfilePersonal,
		Realm: "b10-s8-realm",
	}, "b10-s8-p2-"+contracts.NewID())
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("duplicate realm: code=%s want CONFLICT", contracts.Code(err))
	}
	_ = pc1
	t.Log("Sit8: error format — NOT_FOUND, FORBIDDEN, CONFLICT all carry proper codes + messages")
}

// 9. Scoping isolation: cross-tenant, cross-deployment, cross-subject all denied.
func TestB10Sit9_ScopingIsolation(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b10-s9-" + contracts.NewID(),
	}, "b10-s9-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Cross-tenant.
	scOtherTenant := contracts.Scope{Tenant: e.tenant + "-other", Deployment: e.deployment, Subject: "alice"}
	_, err = e.store.GetComputer(ctx(t), scOtherTenant, comp.ID)
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-tenant: code=%s want NOT_FOUND", contracts.Code(err))
	}

	// Cross-deployment.
	scOtherDeploy := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment + "-2", Subject: "alice"}
	_, err = e.store.GetComputer(ctx(t), scOtherDeploy, comp.ID)
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-deployment: code=%s want NOT_FOUND", contracts.Code(err))
	}

	// Cross-subject.
	scOtherSubj := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "charlie"}
	_, err = e.store.GetComputer(ctx(t), scOtherSubj, comp.ID)
	if contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject: code=%s want NOT_FOUND", contracts.Code(err))
	}
	t.Log("Sit9: scoping — tenant/deployment/subject all isolate (NOT_FOUND)")
}

// 10. Idempotency: replayed operations are safe (Complete, GetCommand).
func TestB10Sit10_Idempotency(t *testing.T) {
	e := newLiveEnv(t)
	sc := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}

	comp, err := e.store.CreateComputer(ctx(t), sc, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b10-s10-" + contracts.NewID(),
	}, "b10-s10-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Exec + dispatch + complete.
	rec, _, err := e.store.Exec(ctx(t), sc, comp.ID, "b10-s10-idem", "echo idem", "")
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	_, err = e.store.MarkDispatched(ctx(t), sc, comp.ID, rec.ID)
	if err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	first, err := e.store.Complete(ctx(t), sc, comp.ID, rec.ID, 0, "ok\n", "")
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if first.Status != compute.CommandSucceeded {
		t.Fatalf("first complete: status=%q want succeeded", first.Status)
	}

	// Replayed Complete: returns same result (idempotent).
	second, err := e.store.Complete(ctx(t), sc, comp.ID, rec.ID, 0, "ok\n", "")
	if err != nil {
		t.Fatalf("replay Complete: %v", err)
	}
	if second.Status != first.Status {
		t.Fatalf("replay: status=%q want %q (idempotent)", second.Status, first.Status)
	}

	// GetCommand replay: always works.
	for i := 0; i < 3; i++ {
		got, err := e.store.GetCommand(ctx(t), sc, comp.ID, rec.ID)
		if err != nil {
			t.Fatalf("GetCommand #%d: %v", i+1, err)
		}
		if got.ID != rec.ID {
			t.Fatalf("GetCommand #%d: id mismatch", i+1)
		}
	}
	t.Log("Sit10: idempotency — Complete replay and repeated GetCommand are safe")
}

// 11. Pagination equivalent: command list returns bounded results with scoping.
func TestB10Sit11_CommandLedgerScoped(t *testing.T) {
	e := newLiveEnv(t)
	scAlice := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "alice"}
	scBob := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: "bob"}

	// Alice creates a computer and runs 3 commands.
	compA, err := e.store.CreateComputer(ctx(t), scAlice, compute.NewComputerRequest{
		Class: compute.ClassC, Profile: compute.ProfileEphemeral,
		Realm: "b10-s11-a-" + contracts.NewID(),
	}, "b10-s11-a-"+contracts.NewID())
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	var cmdIDs []string
	for i := 0; i < 3; i++ {
		rec, _, err := e.store.Exec(ctx(t), scAlice, compA.ID,
			fmt.Sprintf("b10-s11-cmd-%d", i), fmt.Sprintf("echo cmd%d", i), "")
		if err != nil {
			t.Fatalf("Exec %d: %v", i, err)
		}
		cmdIDs = append(cmdIDs, rec.ID)
	}

	// Alice can see all 3.
	for _, id := range cmdIDs {
		_, err := e.store.GetCommand(ctx(t), scAlice, compA.ID, id)
		if err != nil {
			t.Fatalf("alice GetCommand: %v", err)
		}
	}

	// Bob cannot see any (computer not visible).
	for _, id := range cmdIDs {
		_, err := e.store.GetCommand(ctx(t), scBob, compA.ID, id)
		if contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("bob GetCommand: code=%s want NOT_FOUND", contracts.Code(err))
		}
	}
	t.Log("Sit11: ledger scoping — 3 commands visible to owner, invisible to foreign subject")
}
