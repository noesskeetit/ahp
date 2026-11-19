package tools

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// This file exercises the owner result contract against REAL PostgreSQL. The
// MCP endpoint behind every successful call is the in-process go-sdk fixture
// built in service_test.go: it is a FAKE protocol server, and nothing here is
// evidence about a real external provider.

func openPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// ownerReader is the reader the HTTP face would build from a verified task
// credential: the fixture task token carries task-1 / ctx-1.
func ownerReader() ResultReader { return ResultReader{TaskID: "task-1", ContextID: "ctx-1"} }

func callEcho(t *testing.T, e *env, ctx context.Context, key string) operations.Operation {
	t.Helper()
	op, err := e.svc.CallWithProvenance(ctx, e.scope, key, "echo", json.RawMessage(`{"v":1}`),
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"})
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("seed call: op=%+v err=%v, want SUCCEEDED", op, err)
	}
	return op
}

// (1) Positive: the owner authority of the producing context reads the raw MCP
// result; the labels recorded at dispatch are echoed back; a sealed replay of
// the call keeps the labels identical (they are write-once).
func TestGetResultOwnerReadsOwnResult(t *testing.T) {
	e, ctx := newTestService(t)
	callEcho(t, e, ctx, "k-gr-1")

	res, err := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-1")
	if err != nil {
		t.Fatalf("owner GetResult: %v", err)
	}
	if res.Storage != "inline" || res.Status != operations.StatusSucceeded {
		t.Fatalf("view = %+v, want inline/SUCCEEDED", res)
	}
	if !strings.Contains(string(res.Payload), "ok") {
		t.Fatalf("payload lost the raw MCP result: %s", res.Payload)
	}
	if res.Key != "k-gr-1" || res.Target != "echo" || res.Digest == "" {
		t.Fatalf("view identity fields = %+v", res)
	}
	var labels provenanceLabels
	if err := json.Unmarshal(res.Provenance, &labels); err != nil {
		t.Fatalf("provenance %s: %v", res.Provenance, err)
	}
	if labels.Schema != ProvenanceSchema || labels.Context != "ctx-1" || labels.Task != "task-1" || labels.Source != "c" {
		t.Fatalf("labels = %+v, want schema/%s ctx-1 task-1 source c", labels, ProvenanceSchema)
	}
	if strings.Contains(string(res.Provenance), "credential") || strings.Contains(string(res.Provenance), "Bearer") {
		t.Fatalf("provenance leaked credential material: %s", res.Provenance)
	}

	// Replay of the same call must not rewrite the labels.
	if _, err := e.svc.CallWithProvenance(ctx, e.scope, "k-gr-1", "echo", json.RawMessage(`{"v":1}`),
		CallProvenance{TaskID: "task-Z", ContextID: "ctx-Z"}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	again, err := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-1")
	if err != nil {
		t.Fatalf("GetResult after replay: %v", err)
	}
	if string(again.Provenance) != string(res.Provenance) {
		t.Fatalf("provenance rewritten by replay: %s vs %s", again.Provenance, res.Provenance)
	}
}

// (2) Same subject, different context: the personal taint of the producing
// context does not transfer to a sibling context of the same owner.
func TestGetResultDifferentContextDenied(t *testing.T) {
	e, ctx := newTestService(t)
	callEcho(t, e, ctx, "k-gr-2")
	if _, err := e.svc.GetResult(ctx, e.scope, ResultReader{TaskID: "task-2", ContextID: "ctx-2"}, "k-gr-2"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("sibling context read: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
	// A reader with no verified context cannot be bound to anything either.
	if _, err := e.svc.GetResult(ctx, e.scope, ResultReader{TaskID: "task-1"}, "k-gr-2"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("contextless read: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
}

// (3) Foreign scope: another subject, tenant or deployment never learns the
// key exists.
func TestGetResultForeignScopeDenied(t *testing.T) {
	e, ctx := newTestService(t)
	callEcho(t, e, ctx, "k-gr-3")
	foreign := []contracts.Scope{
		{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment, Subject: "uother"},
		{Tenant: "othertenant", Deployment: e.scope.Deployment, Subject: e.scope.Subject},
		{Tenant: e.scope.Tenant, Deployment: "otherdeployment", Subject: e.scope.Subject},
	}
	for i, sc := range foreign {
		if _, err := e.svc.GetResult(ctx, sc, ownerReader(), "k-gr-3"); contracts.Code(err) != contracts.ErrNotFound {
			t.Fatalf("foreign scope %d: code=%q err=%v, want %q", i, contracts.Code(err), err, contracts.ErrNotFound)
		}
	}
}

// (4) Legacy rows: an operation dispatched without a verified binding has no
// provenance, and the reader's own claims are never substituted for it.
func TestGetResultLegacyWithoutProvenanceDenied(t *testing.T) {
	e, ctx := newTestService(t)
	op, err := e.svc.Call(ctx, e.scope, "k-gr-4", "echo", json.RawMessage(`{"v":1}`))
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("legacy call: op=%+v err=%v", op, err)
	}
	if len(op.Provenance) != 0 {
		t.Fatalf("legacy call invented provenance: %s", op.Provenance)
	}
	_, err = e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-4")
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("legacy read: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
	if !strings.Contains(err.Error(), "invent") {
		t.Fatalf("legacy read must say it refuses to invent, got: %v", err)
	}
}

// (5) Uncertain completion stays explicit and never re-dispatches: after the
// ambiguous effect the fake server must not see another call.
func TestGetResultUncertainDeniedAndNeverRedispatches(t *testing.T) {
	e, ctx := newTestService(t)
	args := json.RawMessage(`{"v":1}`)
	dig, err := ApprovalDigest("errwrite", args)
	if err != nil {
		t.Fatalf("digest: %v", err)
	}
	e.g.AllowDigest(dig)
	_, err = e.svc.CallWithProvenance(ctx, e.scope, "k-gr-5", "errwrite", args,
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"})
	if err == nil {
		t.Fatal("ambiguous side effect must surface an error")
	}
	op, gerr := e.svc.Get(ctx, e.scope, "k-gr-5")
	if gerr != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("status = %q err=%v, want SIDE_EFFECT_UNKNOWN", op.Status, gerr)
	}
	before := *e.effCount
	for i := 0; i < 3; i++ {
		if _, gerr := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-5"); contracts.Code(gerr) != contracts.ErrConflict {
			t.Fatalf("uncertain read %d: code=%q err=%v, want %q", i, contracts.Code(gerr), gerr, contracts.ErrConflict)
		}
	}
	if *e.effCount != before {
		t.Fatalf("GetResult re-dispatched the effect: effects %d -> %d", before, *e.effCount)
	}
}

// (6) Pending and failed operations are reported by status only, with no
// result body.
func TestGetResultPendingAndFailedDenied(t *testing.T) {
	e, ctx := newTestService(t)
	pool := openPool(t)

	led, err := operations.New(pool, e.schema)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	for _, req := range []operations.Request{
		{Key: "k-gr-acc", Kind: EffectKind, Target: "echo"},
		{Key: "k-gr-disp", Kind: EffectKind, Target: "echo"},
	} {
		if _, err := led.Accept(ctx, e.scope, req); err != nil {
			t.Fatalf("accept %s: %v", req.Key, err)
		}
	}
	if _, err := led.DispatchWithProvenance(ctx, e.scope, "k-gr-disp",
		json.RawMessage(`{"schema":"tools-result-v1","grant_task":"task-1","grant_context":"ctx-1","source":"c"}`)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	for _, key := range []string{"k-gr-acc", "k-gr-disp"} {
		_, err := e.svc.GetResult(ctx, e.scope, ownerReader(), key)
		if contracts.Code(err) != contracts.ErrConflict {
			t.Fatalf("%s read: code=%q err=%v, want %q", key, contracts.Code(err), err, contracts.ErrConflict)
		}
		if !strings.Contains(err.Error(), "no result yet") {
			t.Fatalf("%s: want an explicit pending answer, got %v", key, err)
		}
	}

	// FAILED through the real dispatch path: the credential cannot be resolved,
	// so the effect never leaves and the ledger records a deterministic failure.
	e.svc.cfg.Credentials = failingCreds{}
	if _, err := e.svc.CallWithProvenance(ctx, e.scope, "k-gr-fail", "echo", json.RawMessage(`{"v":1}`),
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"}); err == nil {
		t.Fatal("expected the call to fail")
	}
	op, err := e.svc.Get(ctx, e.scope, "k-gr-fail")
	if err != nil || op.Status != operations.StatusFailed {
		t.Fatalf("status = %q err=%v, want FAILED", op.Status, err)
	}
	if _, err := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-fail"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("failed read: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrConflict)
	}
}

type failingCreds struct{}

func (failingCreds) Resolve(context.Context, contracts.Scope, string) (Credential, error) {
	return Credential{}, errors.New("fixture: secret store unavailable")
}

// (7) Current-authority revalidation: a result produced by a tool/connection
// that is no longer configured cannot be read (nothing to grant against), and
// a revoked personal grant denies the read without refreshing anything.
func TestGetResultStaleConfigurationAndRevokedGrantDenied(t *testing.T) {
	e, ctx := newTestService(t)
	callEcho(t, e, ctx, "k-gr-7")

	e.svc.cfg.Grants = revokedGrants{}
	if _, err := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-7"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked grant read: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrForbidden)
	}
	e.svc.cfg.Grants = nil

	delete(e.svc.cfg.Tools, "echo")
	if _, err := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-7"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("stale tool read: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrConflict)
	}
	e.svc.cfg.Tools["echo"] = ToolSpec{Name: "echo", Connection: "c"}
	delete(e.svc.cfg.Connections, "c")
	if _, err := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-7"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("stale connection read: code=%q err=%v, want %q", contracts.Code(err), err, contracts.ErrConflict)
	}
}

// revokedGrants is a FIXTURE, not an external provider: it answers the
// read-time question "is the personal grant still active?" with "no", the way
// a revoked consent does.
type revokedGrants struct{}

func (revokedGrants) ConsentActive(context.Context, contracts.Scope, string) (bool, error) {
	return false, nil
}

// (8) Large results go out of line as an immutable reference; nothing is
// truncated and no second ledger appears (the operations row keeps only the
// reference). The sink is a FIXTURE: wiring the real Artifacts owner API is a
// separate composition step.
func TestGetResultLargeResultBecomesImmutableRef(t *testing.T) {
	e, ctx := newTestService(t)
	sink := &fakeSink{}
	e.svc.cfg.Artifacts = sink
	// Small threshold so the fixture's ordinary answer demonstrates the spill
	// path; the rule is size-based, not tool-based.
	e.svc.cfg.ArtifactThreshold = 8

	op, err := e.svc.CallWithProvenance(ctx, e.scope, "k-gr-8", "echo", json.RawMessage(`{"v":1}`),
		CallProvenance{TaskID: "task-1", ContextID: "ctx-1"})
	if err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("spill call: op=%+v err=%v", op, err)
	}
	ref, ok := parseArtifactRef(op.Result)
	if !ok {
		t.Fatalf("ledger must hold a reference, got %s", op.Result)
	}
	if strings.Contains(string(op.Result), "ok\":") {
		t.Fatalf("ledger still holds the result body: %s", op.Result)
	}
	if sink.calls != 1 || !strings.Contains(string(sink.payload), "ok") {
		t.Fatalf("sink calls=%d payload=%s, want the original result stored once", sink.calls, sink.payload)
	}
	if ref.Size != int64(len(sink.payload)) {
		t.Fatalf("ref size = %d, want %d", ref.Size, len(sink.payload))
	}

	res, err := e.svc.GetResult(ctx, e.scope, ownerReader(), "k-gr-8")
	if err != nil {
		t.Fatalf("GetResult: %v", err)
	}
	if res.Storage != "artifact" || res.Ref == nil {
		t.Fatalf("view = %+v, want artifact storage with a reference", res)
	}
	if len(res.Payload) != 0 {
		t.Fatalf("artifact result must not inline the body: %s", res.Payload)
	}
	if res.Ref.ArtifactID != sink.id || res.Ref.Digest != sink.digest {
		t.Fatalf("ref = %+v, want the sink's immutable reference", res.Ref)
	}
	// Out-of-line output is still bound to its producing context.
	if _, err := e.svc.GetResult(ctx, e.scope, ResultReader{ContextID: "ctx-2"}, "k-gr-8"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("artifact read from another context: code=%q, want %q", contracts.Code(err), contracts.ErrForbidden)
	}
}

type fakeSink struct {
	id      string
	digest  string
	calls   int
	payload []byte
}

func (f *fakeSink) PutImmutable(_ context.Context, _ contracts.Scope, payload []byte) (string, string, error) {
	f.calls++
	f.payload = append([]byte(nil), payload...)
	f.id = "art-fixture-1"
	f.digest = "sha256:" + strings.Repeat("a", 64)
	return f.id, f.digest, nil
}

// (9) Without a wired sink an oversized result stays an EXPLICIT failure: the
// ledger refuses it instead of truncating, and the spill seam reports
// "not handled" rather than dropping bytes on the floor.
func TestGetResultNoSinkKeepsExplicitFailure(t *testing.T) {
	e, ctx := newTestService(t)
	if e.svc.cfg.Artifacts != nil {
		t.Fatal("fixture must have no sink")
	}
	big := json.RawMessage(`{"text":"` + strings.Repeat("y", (1<<20)+1) + `"}`)
	if handled, err := func() (bool, error) {
		_, h, e := e.svc.spill(ctx, e.scope, big)
		return h, e
	}(); err != nil || handled {
		t.Fatalf("spill without a sink must be a no-op, got handled=%v err=%v", handled, err)
	}
	pool := openPool(t)
	led, err := operations.New(pool, e.schema)
	if err != nil {
		t.Fatalf("ledger: %v", err)
	}
	if _, err := led.Accept(ctx, e.scope, operations.Request{Key: "k-gr-9", Kind: EffectKind, Target: "echo"}); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if _, err := led.Dispatch(ctx, e.scope, "k-gr-9"); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	_, err = led.Complete(ctx, e.scope, "k-gr-9", big, false)
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("oversized completion: code=%q err=%v, want %q (never a silent truncation)",
			contracts.Code(err), err, contracts.ErrInvalid)
	}
	op, err := led.Get(ctx, e.scope, "k-gr-9")
	if err != nil || op.Status != operations.StatusDispatched || string(op.Result) != "null" {
		t.Fatalf("row after refused completion = %+v err=%v, want DISPATCHED with no partial body", op, err)
	}
}
