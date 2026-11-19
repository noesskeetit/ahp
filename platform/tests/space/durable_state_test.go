//go:build state_integration

package space_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/stateapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// These tests drive the production Space StateClient against the REAL State
// API (internal/stateapi over real PostgreSQL), proving the three reliability
// properties the director acceptance demands -- process-restart durability,
// the UNKNOWN reconciliation gate, crash/resume fencing (STALE_EPOCH), and
// per-subject private-context isolation -- on the ACCEPTED 461aa2ce server,
// not on any in-process mock or hand-written reference stub.
//
// Why httptest + a fresh pool/models a real process restart: the State service
// holds NO in-memory authority -- every thread/run/lease/checkpoint/stream/KV
// mutation is committed to PostgreSQL, and stateapi.New builds a stateless
// handler over the pool. Rebooting therefore means exactly what a k8s restart
// means to State: drop the listener and connection pool, then open a NEW pool
// and a NEW handler over the SAME database with the SAME signing keys and
// mTLS CA. That is precisely what reboot() does below. A value that survives a
// brand-new pool + handler over the same DSN is, by construction, durable in
// the database -- which is the only place State persists anything.

// reboot brings up a brand-new State process (new pgxpool + new stateapi
// handler + new mTLS listener) over the same database, keys and CA as this
// env, and returns a Space client wired to it. The previous server keeps
// running (its t.Cleanup still fires); the test must use the returned client to
// exercise only-durable state.
func (e *cEnv) reboot(t *testing.T) *space.StateClient {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("no DSN for reboot")
	}
	pool, err := pgxpool.New(e.ctx, dsn)
	if err != nil {
		t.Fatalf("reboot pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := state.New(pool)
	// A restarting State process re-runs its idempotent migrations on boot.
	if err := store.Migrate(e.ctx); err != nil {
		t.Fatalf("reboot Migrate: %v", err)
	}
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	issue := func(_ context.Context, sc contracts.Scope, taskID string) (string, error) {
		return e.signer.Sign(auth.Claims{
			Audience: cAudience, Kind: auth.KindTask,
			Tenant: sc.Tenant, Deployment: sc.Deployment,
			Subject: sc.Subject, Task: taskID, Root: "root-" + taskID,
			Context: "ctx-" + taskID, Grant: "grant-" + taskID, Depth: 1,
		}, 5*time.Minute)
	}
	handler, err := stateapi.New(store, e.verifier, authorize, issue)
	if err != nil {
		t.Fatalf("reboot stateapi.New: %v", err)
	}
	cert := e.ca.clientCert(t, cWorkloadURI(e.tenant, e.deployment))
	serverCAs := x509.NewCertPool()
	if !serverCAs.AppendCertsFromPEM(e.ca.pem) {
		t.Fatal("reboot CA PEM rejected")
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: serverCAs}
	srv.StartTLS()
	tr := srv.Client().Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{cert}
	srv.Client().Transport = tr
	t.Cleanup(srv.Close)
	return space.NewStateClient(srv.URL, srv.Client())
}

// signSubject mints a task token for an arbitrary subject inside THIS env's
// tenant/deployment, so a second human can be modeled on the same workload
// certificate (subject comes from the token, workload scope from the cert).
func (e *cEnv) signSubject(t *testing.T, subject, task string) string {
	t.Helper()
	tok, err := e.signer.Sign(auth.Claims{
		Audience: cAudience, Kind: auth.KindTask,
		Tenant: e.tenant, Deployment: e.deployment,
		Subject: subject, Task: task, Root: "run-" + task,
		Context: "ctx-" + task, Grant: "grant-" + task, Depth: 0,
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func sameJSONBytes(t *testing.T, a, b []byte) bool {
	t.Helper()
	var ma, mb any
	if err := json.Unmarshal(a, &ma); err != nil {
		t.Fatalf("unmarshal a %q: %v", a, err)
	}
	if err := json.Unmarshal(b, &mb); err != nil {
		t.Fatalf("unmarshal b %q: %v", b, err)
	}
	return reflect.DeepEqual(ma, mb)
}

// TestStateAPI_DurableSurvivesRestart proves director acceptance item 3 (process
// restart): a run, its checkpoint, an in-flight dispatched intent and a KV value
// committed on one State process are read back byte-identically after a full
// process restart (new pool + new handler over the same database).
func TestStateAPI_DurableSurvivesRestart(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "rs-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	_, err = e.client.CreateRun(ctx, human, th.ID, "rs-run", json.RawMessage(`{"type":"turn","n":1}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w1", 120*time.Second)
	if err != nil || claim.Lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := space.Fence{RunID: claim.Lease.Run.ID, Worker: "w1", Epoch: claim.Lease.Epoch}
	tok := claim.TaskToken

	cpID := "cp-" + contracts.NewID()
	if _, err := e.client.PutCheckpoint(ctx, tok, fence, "space", cpID, "", "rs-ckpt", 1,
		json.RawMessage(`{"stage":"pre-restart"}`),
		[]space.Part{{Channel: "messages", Version: "v1", Data: []byte(`[{"role":"user","text":"hi"}]`)}}); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	const node, call = "node-a", "call-a"
	rec, err := e.client.RecordIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, node, call, "tool", "echo", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if _, err := e.client.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, node, call, state.IntentDispatched, nil); err != nil {
		t.Fatalf("UpdateIntent dispatched: %v", err)
	}
	ns := []string{"space", "profile"}
	if _, err := e.client.PutKV(ctx, tok, fence, ns, "k1", "op1", 0, json.RawMessage(`{"marker":"persisted"}`)); err != nil {
		t.Fatalf("PutKV: %v", err)
	}

	// Pre-restart sanity: everything is readable on the live process.
	preRun, err := e.client.GetRun(ctx, tok, fence.RunID)
	if err != nil || preRun.Status != state.RunRunning {
		t.Fatalf("pre-restart GetRun: %v status=%+v", err, preRun)
	}

	// ---- process restart ----
	rc := e.reboot(t)

	postRun, err := rc.GetRun(ctx, tok, fence.RunID)
	if err != nil {
		t.Fatalf("post-restart GetRun: %v", err)
	}
	if postRun.Status != preRun.Status || postRun.ThreadID != th.ID || postRun.ID != fence.RunID {
		t.Fatalf("run not durable: pre=%+v post=%+v", preRun, postRun)
	}
	cps, err := rc.ListCheckpoints(ctx, tok, th.ID, "space", 10)
	if err != nil {
		t.Fatalf("post-restart ListCheckpoints: %v", err)
	}
	if len(cps) != 1 || cps[0].ID != cpID {
		t.Fatalf("checkpoint not durable: %+v", cps)
	}
	got, err := rc.GetKV(ctx, tok, ns, "k1")
	if err != nil {
		t.Fatalf("post-restart GetKV: %v", err)
	}
	if !sameJSONBytes(t, got.Value, []byte(`{"marker":"persisted"}`)) {
		t.Fatalf("kv not durable: got %s", got.Value)
	}
	// The dispatched intent must also survive: it is exactly the record a
	// resuming worker reads to decide whether to re-run the tool call.
	itr, err := rc.GetIntent(ctx, tok, fence.RunID, node, call)
	if err != nil {
		t.Fatalf("post-restart GetIntent: %v", err)
	}
	if itr.Status != state.IntentDispatched || itr.Digest != rec.Digest {
		t.Fatalf("intent not durable: %+v (want dispatched, digest %q)", itr, rec.Digest)
	}
	t.Logf("durable across restart: run=%s cp=%s intent=%s kv version=%d", postRun.ID, cps[0].ID, itr.Status, got.Version)
}

// TestStateAPI_IntentUnknownReconcile proves director acceptance item 3 (UNKNOWN
// state): an intent left in-flight (dispatched) and then crashed-to-unknown
// stays unknown across a restart, cannot be silently completed through the
// normal UpdateIntent path, and only the privileged ReconcileIntent -- armed
// with the exact original fingerprint -- can move it to a terminal status.
func TestStateAPI_IntentUnknownReconcile(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "uk-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "uk-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w1", 120*time.Second)
	if err != nil || claim.Lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := space.Fence{RunID: claim.Lease.Run.ID, Worker: "w1", Epoch: claim.Lease.Epoch}
	tok := claim.TaskToken
	const node, call = "node-x", "call-x"

	rec, err := e.client.RecordIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, node, call, "tool", "echo", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if _, err := e.client.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, node, call, state.IntentDispatched, nil); err != nil {
		t.Fatalf("UpdateIntent dispatched: %v", err)
	}
	// A worker crash is surfaced as UNKNOWN (the harness cannot claim a result
	// it never observed).
	if _, err := e.client.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, node, call, state.IntentUnknown, nil); err != nil {
		t.Fatalf("UpdateIntent unknown: %v", err)
	}

	// Restart: UNKNOWN must survive -- it is the durable marker that tells a
	// resuming worker "a side effect MAY have happened, reconcile before retry".
	rc := e.reboot(t)
	itr, err := rc.GetIntent(ctx, tok, fence.RunID, node, call)
	if err != nil {
		t.Fatalf("post-restart GetIntent: %v", err)
	}
	if itr.Status != state.IntentUnknown {
		t.Fatalf("intent should survive restart unknown, got %q", itr.Status)
	}
	// The normal update path must NOT be able to complete an unknown intent.
	if _, err := rc.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch, node, call, state.IntentSucceeded, json.RawMessage(`{"ok":true}`)); err == nil {
		t.Fatal("UpdateIntent must not resolve an unknown intent")
	}
	// Only ReconcileIntent, with the exact original fingerprint, resolves it.
	res := json.RawMessage(`{"ok":true,"from":"reconcile"}`)
	// Wrong digest is rejected.
	if _, err := rc.ReconcileIntent(ctx, tok, fence, node, call, state.IntentSucceeded, "not-the-digest", res); err == nil {
		t.Fatal("reconcile with wrong digest must be rejected")
	}
	done, err := rc.ReconcileIntent(ctx, tok, fence, node, call, state.IntentSucceeded, rec.Digest, res)
	if err != nil {
		t.Fatalf("ReconcileIntent: %v", err)
	}
	if done.Status != state.IntentSucceeded {
		t.Fatalf("reconciled status = %q want succeeded", done.Status)
	}
	if fin, err := rc.GetIntent(ctx, tok, fence.RunID, node, call); err != nil || fin.Status != state.IntentSucceeded {
		t.Fatalf("post-reconcile GetIntent: %v %+v", err, fin)
	}
}

// TestStateAPI_CrashResumeRejectsOldWriter proves director acceptance item 3 /
// ruling gate B: after a worker's lease is suspended past its deadline the run
// is requeued and a NEW worker claims the SAME run at a HIGHER epoch; the old
// worker's writes are then rejected with STALE_EPOCH/LEASE_LOST while the new
// epoch's writes succeed. This is same-run crash/resume fencing.
func TestStateAPI_CrashResumeRejectsOldWriter(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	human := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, human, contracts.Scope{}, "ctx-"+contracts.NewID(), "cr-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, human, th.ID, "cr-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	c1, err := e.client.ClaimRun(ctx, "", "w1", 120*time.Second)
	if err != nil || c1.Lease == nil {
		t.Fatalf("ClaimRun w1: %v", err)
	}
	f1 := space.Fence{RunID: c1.Lease.Run.ID, Worker: "w1", Epoch: c1.Lease.Epoch}
	if _, err := e.client.PutCheckpoint(ctx, c1.TaskToken, f1, "space", "cp-w1", "", "cr-ckpt", 1, nil, nil); err != nil {
		t.Fatalf("w1 PutCheckpoint: %v", err)
	}
	// Simulate a crash: suspend the lease with a deadline already in the past,
	// which requeues the run for another worker.
	past := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	if err := e.client.Suspend(ctx, c1.TaskToken, f1.RunID, "w1", f1.Epoch, "crashed", &past); err != nil {
		t.Fatalf("Suspend w1: %v", err)
	}
	c2, err := e.client.ClaimRun(ctx, "", "w2", 120*time.Second)
	if err != nil || c2.Lease == nil {
		t.Fatalf("ClaimRun w2: %v", err)
	}
	if c2.Lease.Run.ID != f1.RunID {
		t.Fatalf("resume must be same-run: %s vs %s", c2.Lease.Run.ID, f1.RunID)
	}
	if c2.Lease.Epoch <= f1.Epoch {
		t.Fatalf("resume must raise epoch: w1=%d w2=%d", f1.Epoch, c2.Lease.Epoch)
	}
	// Old writer (w1, epoch E1) is now fenced: its checkpoint write is rejected.
	if _, err := e.client.PutCheckpoint(ctx, c1.TaskToken, f1, "space", "cp-w1-late", "", "cr-ckpt", 2, nil, nil); err == nil {
		t.Fatal("old writer must be rejected after requeue")
	} else if !strings.Contains(err.Error(), "STALE_EPOCH") && !strings.Contains(err.Error(), "LEASE_LOST") {
		t.Fatalf("old writer error unexpected: %v", err)
	}
	// New writer (w2, epoch E2) succeeds on the same run.
	f2 := space.Fence{RunID: c2.Lease.Run.ID, Worker: "w2", Epoch: c2.Lease.Epoch}
	if _, err := e.client.PutCheckpoint(ctx, c2.TaskToken, f2, "space", "cp-w2", "", "cr-ckpt-resume", 2, nil, nil); err != nil {
		t.Fatalf("w2 PutCheckpoint should succeed: %v", err)
	}
	t.Logf("crash/resume fenced: run=%s w1_epoch=%d rejected, w2_epoch=%d accepted", f1.RunID, f1.Epoch, f2.Epoch)
}

// TestStateAPI_PrivateContextAcrossSubjects proves director acceptance item 3
// (private context does not leak): a thread and a KV row written under subject
// user-ada are invisible -- NOT_FOUND, never a probeable forbidden -- to a
// second human user-bob who shares the SAME tenant and deployment (same
// workload certificate) but a different subject. The owner can read both back,
// so the isolation is per-subject, not a generic read failure.
func TestStateAPI_PrivateContextAcrossSubjects(t *testing.T) {
	e := cNewEnv(t)
	ctx := e.ctx
	ada := e.signTask(t, "task-"+contracts.NewID())
	th, err := e.client.CreateThread(ctx, ada, contracts.Scope{}, "ctx-"+contracts.NewID(), "pc-thread")
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := e.client.CreateRun(ctx, ada, th.ID, "pc-run", json.RawMessage(`{"type":"turn"}`), ""); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w1", 120*time.Second)
	if err != nil || claim.Lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	fence := space.Fence{RunID: claim.Lease.Run.ID, Worker: "w1", Epoch: claim.Lease.Epoch}
	adaTok := claim.TaskToken // carries subject user-ada (inherited from the run)
	ns := []string{"space", "secret"}
	if _, err := e.client.PutKV(ctx, adaTok, fence, ns, "private", "op1", 0, json.RawMessage(`{"note":"ada only"}`)); err != nil {
		t.Fatalf("ada PutKV: %v", err)
	}
	// Owner reads back: proves the rows exist and are readable by their subject.
	if _, err := e.client.GetThread(ctx, ada, th.ID); err != nil {
		t.Fatalf("owner GetThread should succeed: %v", err)
	}
	if got, err := e.client.GetKV(ctx, adaTok, ns, "private"); err != nil || got == nil {
		t.Fatalf("owner GetKV should succeed: %v", err)
	}

	// A different subject in the same tenant/deployment must not see them.
	bob := e.signSubject(t, "user-bob", "task-"+contracts.NewID())
	if _, err := e.client.GetThread(ctx, bob, th.ID); err == nil {
		t.Fatal("subject user-bob must NOT read user-ada's thread")
	} else if !strings.Contains(err.Error(), "NOT_FOUND") {
		t.Fatalf("cross-subject GetThread want NOT_FOUND, got %v", err)
	}
	if _, err := e.client.GetKV(ctx, bob, ns, "private"); err == nil {
		t.Fatal("subject user-bob must NOT read user-ada's private KV")
	} else if !strings.Contains(err.Error(), "NOT_FOUND") {
		t.Fatalf("cross-subject GetKV want NOT_FOUND, got %v", err)
	}
}
