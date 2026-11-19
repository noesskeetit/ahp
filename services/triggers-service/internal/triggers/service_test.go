package triggers

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := crand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// ---- mocks (pointer receivers so post-New mutation is observed) ----

type fakeIssuer struct {
	err  error
	refs []string
	n    int
}

func (f *fakeIssuer) IssueToken(_ context.Context, _ contracts.Scope, ref string, ttl time.Duration) (string, time.Time, error) {
	if f.err != nil {
		return "", time.Time{}, f.err
	}
	f.n++
	f.refs = append(f.refs, ref)
	return fmt.Sprintf("tok-%d", f.n), time.Now().Add(ttl), nil
}

type fakeDispatcher struct {
	err  error
	reqs []FireRequest
	n    int
}

func (f *fakeDispatcher) Fire(_ context.Context, _ contracts.Scope, req FireRequest) (FireResult, error) {
	if f.err != nil {
		return FireResult{}, f.err
	}
	f.n++
	f.reqs = append(f.reqs, req)
	return FireResult{TaskID: fmt.Sprintf("task-%d", f.n)}, nil
}

type fakeSecrets struct{ m map[string][]byte }

func (f *fakeSecrets) Resolve(_ context.Context, _ contracts.Scope, ref string) ([]byte, error) {
	if b, ok := f.m[ref]; ok {
		return b, nil
	}
	return nil, fmt.Errorf("secret %q not found", ref)
}

type fakeTasks struct {
	m   map[string]bool
	def bool
}

func (f *fakeTasks) IsTaskActive(_ context.Context, _ contracts.Scope, taskID string) (bool, error) {
	if v, ok := f.m[taskID]; ok {
		return v, nil
	}
	return f.def, nil
}

type env struct {
	svc     *Service
	pool    *pgxpool.Pool
	schema  string
	scope   contracts.Scope
	iss     *fakeIssuer
	disp    *fakeDispatcher
	secrets *fakeSecrets
	tasks   *fakeTasks
}

func newTestService(t *testing.T) (*env, context.Context) {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)
	schema := "ahp_trg_t" + randHex(6)
	sfx := randHex(6)
	scope := contracts.Scope{Tenant: "t" + sfx, Deployment: "d" + sfx, Subject: "s" + sfx}
	// Registered last => runs first (LIFO), while the pool is still open.
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer ccancel()
		_, _ = pool.Exec(cctx, "DROP SCHEMA IF EXISTS "+quoteIdent(schema)+" CASCADE")
	})
	iss := &fakeIssuer{}
	disp := &fakeDispatcher{}
	secrets := &fakeSecrets{m: map[string][]byte{"sec-1": []byte("s3cr3t")}}
	tasks := &fakeTasks{m: map[string]bool{}, def: true}
	svc, err := New(ctx, pool, schema, Config{
		Issuer:     iss,
		Dispatcher: disp,
		Secrets:    secrets,
		Tasks:      tasks,
		FireTTL:    time.Minute,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return &env{svc: svc, pool: pool, schema: schema, scope: scope, iss: iss, disp: disp, secrets: secrets, tasks: tasks}, ctx
}

func (e *env) mkSchedule(t *testing.T, ctx context.Context, base time.Time, interval time.Duration, maxFires int64, delegated bool) Trigger {
	t.Helper()
	tr, err := e.svc.CreateSchedule(ctx, e.scope, CreateScheduleSpec{
		ParentTask:    "parent-" + randHex(4),
		DelegationRef: "del-ref-XYZ",
		Target:        "agent:echo",
		Interval:      interval,
		MaxFires:      maxFires,
		Delegated:     delegated,
		FirstFireAt:   &base,
	})
	if err != nil {
		t.Fatalf("CreateSchedule: %v", err)
	}
	return tr
}

var baseTime = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

func TestScheduleFiresOncePerDue(t *testing.T) {
	e, ctx := newTestService(t)
	e.mkSchedule(t, ctx, baseTime, time.Hour, 5, false)

	n, err := e.svc.ProcessDue(ctx, baseTime, 0)
	if err != nil {
		t.Fatalf("ProcessDue#1: %v", err)
	}
	if n != 1 || e.disp.n != 1 {
		t.Fatalf("expected 1 fire, got n=%d disp=%d", n, e.disp.n)
	}
	// Same clock: next fire already rolled forward, must not double-fire.
	n, err = e.svc.ProcessDue(ctx, baseTime, 0)
	if err != nil {
		t.Fatalf("ProcessDue#2: %v", err)
	}
	if n != 0 || e.disp.n != 1 {
		t.Fatalf("expected no re-fire, got n=%d disp=%d", n, e.disp.n)
	}
}

func TestRevokeStopsSchedule(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, false)
	if _, err := e.svc.Revoke(ctx, e.scope, tr.TriggerID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	// Idempotent second revoke.
	if _, err := e.svc.Revoke(ctx, e.scope, tr.TriggerID); err != nil {
		t.Fatalf("revoke#2: %v", err)
	}
	n, err := e.svc.ProcessDue(ctx, baseTime, 0)
	if err != nil {
		t.Fatalf("ProcessDue: %v", err)
	}
	if n != 0 || e.disp.n != 0 {
		t.Fatalf("revoked trigger must not fire, got n=%d disp=%d", n, e.disp.n)
	}
}

func TestScheduleExhaustsAtMaxFires(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 2, false)

	if n, err := e.svc.ProcessDue(ctx, baseTime, 0); err != nil || n != 1 {
		t.Fatalf("fire#1: n=%d err=%v", n, err)
	}
	if n, err := e.svc.ProcessDue(ctx, baseTime.Add(time.Hour), 0); err != nil || n != 1 {
		t.Fatalf("fire#2: n=%d err=%v", n, err)
	}
	got, err := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Status != StatusExhausted || got.FiredCount != 2 {
		t.Fatalf("expected EXHAUSTED/2, got %s/%d", got.Status, got.FiredCount)
	}
	if e.disp.n != 2 {
		t.Fatalf("expected 2 dispatches, got %d", e.disp.n)
	}
	if n, err := e.svc.ProcessDue(ctx, baseTime.Add(2*time.Hour), 0); err != nil || n != 0 {
		t.Fatalf("exhausted must not fire: n=%d err=%v", n, err)
	}
}

func TestWebhookHappyPathAndIdempotent(t *testing.T) {
	e, ctx := newTestService(t)
	tr, err := e.svc.CreateWebhook(ctx, e.scope, CreateWebhookSpec{
		ParentTask:    "parent-" + randHex(4),
		DelegationRef: "del-ref-XYZ",
		Target:        "agent:echo",
		SecretRef:     "sec-1",
		MaxFires:      10,
	})
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}
	body := []byte(`{"event":"ping"}`)
	sig := SignWebhook([]byte("s3cr3t"), canonicalWebhookMessage(tr.TriggerID, "msg-1", body))

	op, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "msg-1", sig, body)
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if op.Status != operations.StatusSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", op.Status)
	}
	if e.disp.n != 1 {
		t.Fatalf("expected 1 dispatch, got %d", e.disp.n)
	}
	// Duplicate delivery of the same message id must not re-dispatch.
	op2, err := e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "msg-1", sig, body)
	if err != nil {
		t.Fatalf("receive#2: %v", err)
	}
	if op2.Status != operations.StatusSucceeded || e.disp.n != 1 {
		t.Fatalf("dup changed state: status=%s disp=%d", op2.Status, e.disp.n)
	}
	got, _ := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if got.FiredCount != 1 {
		t.Fatalf("expected fired_count=1, got %d", got.FiredCount)
	}
}

func TestWebhookBadSignatureNoLedgerWrite(t *testing.T) {
	e, ctx := newTestService(t)
	tr, err := e.svc.CreateWebhook(ctx, e.scope, CreateWebhookSpec{
		ParentTask:    "parent-" + randHex(4),
		DelegationRef: "del-ref-XYZ",
		Target:        "agent:echo",
		SecretRef:     "sec-1",
	})
	if err != nil {
		t.Fatalf("CreateWebhook: %v", err)
	}
	body := []byte(`{"event":"ping"}`)
	_, err = e.svc.ReceiveWebhook(ctx, e.scope, tr.TriggerID, "msg-1", "deadbeef", body)
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("expected ErrForbidden, got %v", err)
	}
	if e.disp.n != 0 {
		t.Fatalf("bad sig must not dispatch, got %d", e.disp.n)
	}
	// Ledger slot must be untouched (verify happens before Accept).
	_, gerr := e.svc.ledger.Get(ctx, e.scope, "trigger:"+tr.TriggerID+":wh:msg-1")
	if contracts.Code(gerr) != contracts.ErrNotFound {
		t.Fatalf("expected no ledger slot, got %v", gerr)
	}
}

func TestTenantIsolation(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, false)
	foreign := contracts.Scope{Tenant: "t" + randHex(6), Deployment: e.scope.Deployment, Subject: e.scope.Subject}
	if _, err := e.svc.Get(ctx, foreign, tr.TriggerID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign tenant must see NOT_FOUND, got %v", err)
	}
}

func TestTokenReissuedNotPersisted(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, false)
	if tr.DelegationRef != "del-ref-XYZ" {
		t.Fatalf("unexpected delegation_ref %q", tr.DelegationRef)
	}
	if n, err := e.svc.ProcessDue(ctx, baseTime, 0); err != nil || n != 1 {
		t.Fatalf("fire: n=%d err=%v", n, err)
	}
	if len(e.disp.reqs) != 1 {
		t.Fatalf("expected 1 fire req, got %d", len(e.disp.reqs))
	}
	// The dispatched token is freshly issued (tok-1), distinct from the stored ref.
	if e.disp.reqs[0].Token != "tok-1" {
		t.Fatalf("expected fresh token tok-1, got %q", e.disp.reqs[0].Token)
	}
	if e.disp.reqs[0].Token == tr.DelegationRef {
		t.Fatalf("token must not equal stored delegation ref")
	}
}

func TestNonDelegatedDiedWithParent(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, false)
	e.tasks.m[tr.ParentTask] = false
	n, err := e.svc.ProcessDue(ctx, baseTime, 0)
	if err != nil {
		t.Fatalf("ProcessDue: %v", err)
	}
	if n != 0 || e.disp.n != 0 {
		t.Fatalf("dead-parent non-delegated must not fire: n=%d disp=%d", n, e.disp.n)
	}
	got, _ := e.svc.Get(ctx, e.scope, tr.TriggerID)
	if got.Status != StatusRevoked {
		t.Fatalf("expected REVOKED, got %s", got.Status)
	}
}

func TestDelegatedOutlivesParent(t *testing.T) {
	e, ctx := newTestService(t)
	e.tasks.def = false // every task reported inactive
	e.mkSchedule(t, ctx, baseTime, time.Hour, 5, true)
	n, err := e.svc.ProcessDue(ctx, baseTime, 0)
	if err != nil {
		t.Fatalf("ProcessDue: %v", err)
	}
	if n != 1 || e.disp.n != 1 {
		t.Fatalf("delegated trigger must fire regardless of parent: n=%d disp=%d", n, e.disp.n)
	}
}

func TestAmbiguousFireMarkedUnknownNoReplay(t *testing.T) {
	e, ctx := newTestService(t)
	tr := e.mkSchedule(t, ctx, baseTime, time.Hour, 5, false)
	e.disp.err = errors.New("boom")

	if _, err := e.svc.ProcessDue(ctx, baseTime, 0); err == nil {
		t.Fatalf("expected error from ambiguous fire")
	}
	// The fire slot is sealed as UNKNOWN.
	op, err := e.svc.ledger.Get(ctx, e.scope, "trigger:"+tr.TriggerID+":fire:1")
	if err != nil {
		t.Fatalf("ledger get: %v", err)
	}
	if op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("expected SIDE_EFFECT_UNKNOWN, got %s", op.Status)
	}
	// A re-run must NOT auto-replay the ambiguous side effect.
	e.disp.err = nil
	n, err := e.svc.ProcessDue(ctx, baseTime, 0)
	if err != nil {
		t.Fatalf("ProcessDue#2: %v", err)
	}
	if n != 0 {
		t.Fatalf("ambiguous fire must not be replayed, got n=%d", n)
	}
	op2, _ := e.svc.ledger.Get(ctx, e.scope, "trigger:"+tr.TriggerID+":fire:1")
	if op2.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("slot must stay UNKNOWN, got %s", op2.Status)
	}
}

func TestEmptyTriggerListHasArrayWireShape(t *testing.T) {
	e, ctx := newTestService(t)
	rows, err := e.svc.List(ctx, e.scope)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(map[string]any{"triggers": rows})
	if err != nil {
		t.Fatal(err)
	}
	if string(wire) != `{"triggers":[]}` {
		t.Fatalf("empty tenant list must be an array for the browser contract, got %s", wire)
	}
}
