package state

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// streamDigest digests the ordered content of a stream, ignoring ids and
// sequence numbers: an import must reproduce the bytes, not the addresses.
func streamDigest(events []StreamEvent) string {
	h := sha256.New()
	for _, e := range events {
		h.Write([]byte(e.Key))
		h.Write([]byte{0})
		h.Write([]byte(e.Kind))
		h.Write([]byte{0})
		h.Write(e.Payload)
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func exportFixture(t *testing.T) (*cpEnv, ControlExport) {
	t.Helper()
	e := cpFixture(t)
	if _, err := e.s.AppendStream(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"k-1", "tool", json.RawMessage(`{"n":1}`)); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if _, err := e.s.AppendStream(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"k-2", "tool", json.RawMessage(`{"n":2}`)); err != nil {
		t.Fatalf("AppendStream #2: %v", err)
	}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-imp",
		Part{Channel: "values", Version: "v1", Data: []byte("durable-bytes")}), "key-imp"); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	exp, err := e.s.ExportThread(e.ctx, e.scope, e.threadID)
	if err != nil {
		t.Fatalf("ExportThread: %v", err)
	}
	if len(exp.Stream) != 2 || len(exp.Checkpoints) != 1 {
		t.Fatalf("export is not a faithful snapshot: stream=%d checkpoints=%d",
			len(exp.Stream), len(exp.Checkpoints))
	}
	return e, exp
}

// TestImportThreadRefusesForeignIdentity pins the isolation rule: export bytes
// are a bearer artefact that travels, so the scope stored inside them has to be
// checked on the way in. A foreign subject of the SAME tenant must not be able to
// materialise a durable clone of another subject's thread - and must not leave a
// single row behind.
func TestImportThreadRefusesForeignIdentity(t *testing.T) {
	e, exp := exportFixture(t)
	invader := contracts.Scope{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment,
		Subject: "subj-invader-" + contracts.NewID()}

	before := tenantThreadCount(t, e)
	imported, err := e.s.ImportThread(e.ctx, invader, exp, "")
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign subject importing another identity's bytes: want FORBIDDEN, got %v (thread %+v)", err, imported)
	}
	if reason := contracts.ReasonOf(err); reason != ReasonImportScopeMismatch {
		t.Fatalf("reason = %q, want %s", reason, ReasonImportScopeMismatch)
	}
	if got := tenantThreadCount(t, e); got != before {
		t.Fatalf("the refused import created rows: threads %d -> %d", before, got)
	}
	// A foreign tenant is refused the same way, whatever the context looks like.
	foreign := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: "d-" + contracts.NewID(),
		Subject: e.scope.Subject}
	if _, err := e.s.ImportThread(e.ctx, foreign, exp, "ctx-free-"+contracts.NewID()); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign tenant import: want FORBIDDEN, got %v", err)
	}
}

// TestImportRefusesContextOwnedByAnotherSubjectOfTheDeployment is the exact
// read-back the director measured: the importer's own lookup was scope-scoped,
// so a context_id already owned by a DIFFERENT subject used to be taken freely
// (tenant ahpstand, subjects demo-human and peer-human, one context_id, a full
// durable clone under the wrong identity). Context ids are how the human edge
// resolves a thread, so the import check runs across subjects.
func TestImportRefusesContextOwnedByAnotherSubjectOfTheDeployment(t *testing.T) {
	e, exp := exportFixture(t)
	clash := "ctx-clash-" + contracts.NewID()
	other := contracts.Scope{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment,
		Subject: "subj-other-" + contracts.NewID()}
	if _, err := e.s.CreateThread(e.ctx, other, clash, "k-"+contracts.NewID()); err != nil {
		t.Fatalf("CreateThread for the other subject: %v", err)
	}
	before := tenantThreadCount(t, e)

	// The importer IS the origin identity here, so only the cross-subject check
	// stands between them and taking over an address inside the deployment.
	_, err := e.s.ImportThread(e.ctx, e.scope, exp, clash)
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("import into a context owned by another subject of the tenant: want CONFLICT, got %v", err)
	}
	if reason := contracts.ReasonOf(err); reason != ReasonImportContextTaken {
		t.Fatalf("reason = %q, want %s", reason, ReasonImportContextTaken)
	}
	if got := tenantThreadCount(t, e); got != before {
		t.Fatalf("the refused import created rows: threads %d -> %d", before, got)
	}
}

// TestImportThreadRestoresOwnSnapshotUnderAnExplicitRetarget is F2: the owner must
// be able to restore. An explicit context_id makes the new address deliberate,
// the thread id is fresh, and the durable content - stream bytes and checkpoint
// parts - comes back identical.
func TestImportThreadRestoresOwnSnapshotUnderAnExplicitRetarget(t *testing.T) {
	e, exp := exportFixture(t)
	target := "ctx-restore-" + contracts.NewID()

	imported, err := e.s.ImportThread(e.ctx, e.scope, exp, target)
	if err != nil {
		t.Fatalf("owner restoring with an explicit retarget: %v", err)
	}
	if imported.ID == e.threadID {
		t.Fatal("import reused the source thread id")
	}
	if imported.ContextID != target {
		t.Fatalf("imported context_id = %q, want the retarget %q", imported.ContextID, target)
	}
	got, err := e.s.ReadStream(e.ctx, e.scope, imported.ID, 0, 100)
	if err != nil {
		t.Fatalf("ReadStream imported: %v", err)
	}
	if streamDigest(got) != streamDigest(exp.Stream) {
		t.Fatalf("stream digest differs after restore: imported %s, exported %s",
			streamDigest(got), streamDigest(exp.Stream))
	}
	cps, err := e.s.ListCheckpoints(e.ctx, e.scope, imported.ID, "ns", 10)
	if err != nil || len(cps) != 1 || len(cps[0].Parts) != 1 {
		t.Fatalf("imported checkpoints/parts = %+v (err %v), want one checkpoint with one part", cps, err)
	}
	data, err := e.s.GetPart(e.ctx, e.scope, imported.ID, "ns", "values", "v1")
	if err != nil {
		t.Fatalf("GetPart imported: %v", err)
	}
	if string(data) != "durable-bytes" {
		t.Fatalf("imported part bytes = %q", data)
	}
}

// TestImportThreadMigratesAcrossDeployments keeps the reason the operation
// exists (PDF rev3.2: hand-off between drivers): the same identity may move the
// snapshot to another deployment of the same tenant.
func TestImportThreadMigratesAcrossDeployments(t *testing.T) {
	e, exp := exportFixture(t)
	dst := contracts.Scope{Tenant: e.scope.Tenant, Deployment: "d-driver2-" + contracts.NewID(),
		Subject: e.scope.Subject}
	imported, err := e.s.ImportThread(e.ctx, dst, exp, "ctx-migrated-"+contracts.NewID())
	if err != nil {
		t.Fatalf("cross-deployment import of the same identity: %v", err)
	}
	if imported.Scope.Deployment != dst.Deployment {
		t.Fatalf("imported scope = %+v, want deployment %q", imported.Scope, dst.Deployment)
	}
}

// tenantThreadCount counts every thread row of the tenant, across subjects and
// deployments: a refused import must not have written anything at all.
func tenantThreadCount(t *testing.T, e *cpEnv) int {
	t.Helper()
	var n int
	if err := e.s.pool.QueryRow(e.ctx,
		`SELECT count(*) FROM ahp_state.threads WHERE tenant = $1`, e.scope.Tenant).Scan(&n); err != nil {
		t.Fatalf("count tenant threads: %v", err)
	}
	return n
}
