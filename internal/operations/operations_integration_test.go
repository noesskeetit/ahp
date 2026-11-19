package operations_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// testEnv holds a live journal store. Every test run uses a freshly
// randomised tenant, deployment and schema so nothing is ever shared with or
// destructive to other data in the database.
type testEnv struct {
	pool   *pgxpool.Pool
	store  *operations.Store
	schema string
	scope  contracts.Scope
}

// newEnv connects to the real PostgreSQL instance named by
// AHP_TEST_DATABASE_URL. The whole package skips (with an explicit reason)
// when the variable is absent: these tests verify transactional invariants
// that cannot be exercised against a mock.
func newEnv(t *testing.T) *testEnv {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("integration test requires AHP_TEST_DATABASE_URL pointing at a live PostgreSQL instance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	suffix := randomHex(t, 6)
	schema := "ahp_ops_t" + suffix
	store, err := operations.New(pool, schema)
	if err != nil {
		t.Fatalf("operations.New(%q): %v", schema, err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("second Migrate (idempotency): %v", err)
	}

	return &testEnv{
		pool:   pool,
		store:  store,
		schema: schema,
		scope: contracts.Scope{
			Tenant:     "t-" + suffix,
			Deployment: "dep-" + suffix,
			Subject:    "user-" + suffix,
		},
	}
}

// scopeFor derives a sibling scope sharing the env's tenant and deployment
// but with a different subject.
func (e *testEnv) scopeFor(subject string) contracts.Scope {
	s := e.scope
	s.Subject = subject
	return s
}

// deploymentFor derives a sibling scope sharing tenant and subject but a
// different deployment.
func (e *testEnv) deploymentFor(deployment string) contracts.Scope {
	s := e.scope
	s.Deployment = deployment
	return s
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random: %v", err)
	}
	return hex.EncodeToString(b)
}

func req(t *testing.T, key string) operations.Request {
	t.Helper()
	return operations.Request{
		Key:       key,
		Kind:      "tool.call",
		Target:    "tool:echo",
		Arguments: json.RawMessage(`{"text":"hello","n":1}`),
	}
}

// jsonEqual reports whether two JSON documents carry the same value. The
// journal stores results as jsonb, so stored bytes may differ in whitespace
// and key order from what was submitted while remaining the same value.
func jsonEqual(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	da, err := contracts.Digest(json.RawMessage(a))
	if err != nil {
		t.Fatalf("digest %s: %v", a, err)
	}
	db, err := contracts.Digest(json.RawMessage(b))
	if err != nil {
		t.Fatalf("digest %s: %v", b, err)
	}
	return da == db
}

func codeOf(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %s, got nil", want)
	}
	if got := contracts.Code(err); got != want {
		t.Fatalf("error code = %q (%v), want %q", got, err, want)
	}
}

func TestAcceptIdempotentAndKeyReuseMismatch(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)
	r := req(t, key)

	first, err := env.store.Accept(ctx, env.scope, r)
	if err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	if first.Status != operations.StatusAccepted || first.Key != key {
		t.Fatalf("unexpected first operation: %+v", first)
	}

	// Identical canonical request returns the same operation.
	again, err := env.store.Accept(ctx, env.scope, r)
	if err != nil {
		t.Fatalf("repeat Accept: %v", err)
	}
	if again.Status != first.Status || again.Version != first.Version || again.Digest != first.Digest {
		t.Fatalf("repeat Accept changed the operation: %+v vs %+v", again, first)
	}

	// Changed arguments for the same key: KEY_REUSE_MISMATCH.
	changed := r
	changed.Arguments = json.RawMessage(`{"text":"goodbye","n":1}`)
	_, err = env.store.Accept(ctx, env.scope, changed)
	codeOf(t, err, contracts.ErrKeyReuse)

	// Changed kind, target and guard each also mismatch.
	for _, mut := range []func(*operations.Request){
		func(q *operations.Request) { q.Kind = "tool.other" },
		func(q *operations.Request) { q.Target = "tool:shout" },
		func(q *operations.Request) { q.Guard = "acct:42" },
	} {
		m := r
		mut(&m)
		if _, err := env.store.Accept(ctx, env.scope, m); contracts.Code(err) != contracts.ErrKeyReuse {
			t.Fatalf("mutation mismatch: got %v, want KEY_REUSE_MISMATCH", err)
		}
	}

	// Byte-order differences in the arguments are canonicalised away and
	// replay idempotently.
	reordered := r
	reordered.Arguments = json.RawMessage(`{"n": 1, "text":"hello"}`)
	if again, err := env.store.Accept(ctx, env.scope, reordered); err != nil || again.Version != first.Version {
		t.Fatalf("reordered replay: %+v err %v", again, err)
	}

	// The stored operation is untouched by the rejected attempts.
	got, err := env.store.Get(ctx, env.scope, key)
	if err != nil || got.Status != operations.StatusAccepted {
		t.Fatalf("Get after mismatches: %+v err %v", got, err)
	}
}

func TestAcceptValidation(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)

	r := req(t, "")
	if _, err := env.store.Accept(ctx, env.scope, r); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("empty key: got %v, want INVALID_ARGUMENT", err)
	}
	r = req(t, "k")
	r.Arguments = json.RawMessage(`{"broken":`)
	if _, err := env.store.Accept(ctx, env.scope, r); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("bad JSON: got %v, want INVALID_ARGUMENT", err)
	}
	badScope := env.scope
	badScope.Subject = ""
	if _, err := env.store.Accept(ctx, badScope, req(t, "k")); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("bad scope: got %v, want INVALID_ARGUMENT", err)
	}
}

func TestConcurrentAcceptIdenticalReturnsSameOperation(t *testing.T) {
	// Regression: concurrent Accepts of the identical request must all
	// return the same operation; none may surface a unique violation.
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)
	r := req(t, key)

	const racers = 16
	var (
		wg         sync.WaitGroup
		oks        int64
		unexpected int64
	)
	ops := make([]operations.Operation, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			op, err := env.store.Accept(ctx, env.scope, r)
			if err != nil {
				atomic.AddInt64(&unexpected, 1)
				return
			}
			ops[i] = op
			atomic.AddInt64(&oks, 1)
		}(i)
	}
	close(start)
	wg.Wait()

	if unexpected != 0 {
		t.Fatalf("%d concurrent identical Accepts failed", unexpected)
	}
	if oks != racers {
		t.Fatalf("%d Accepts must succeed, got %d", racers, oks)
	}
	for i, op := range ops {
		if op.Key != key || op.Status != operations.StatusAccepted || op.Digest != ops[0].Digest {
			t.Fatalf("racer %d returned a different operation: %+v vs %+v", i, op, ops[0])
		}
	}
}

func TestExactNumberAndDuplicateKeyFingerprints(t *testing.T) {
	// Regression: arguments are digested as raw canonical JSON, so numbers
	// beyond float64 precision distinguish requests and duplicate object
	// keys are rejected outright instead of silently collapsing.
	ctx := context.Background()
	env := newEnv(t)

	keyA := "op-" + randomHex(t, 4)
	rA := req(t, keyA)
	rA.Arguments = json.RawMessage(`{"amount":9007199254740992}`)
	if _, err := env.store.Accept(ctx, env.scope, rA); err != nil {
		t.Fatalf("Accept exact number: %v", err)
	}
	near := rA
	near.Arguments = json.RawMessage(`{"amount":9007199254740993}`)
	if _, err := env.store.Accept(ctx, env.scope, near); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("9007199254740993 vs 9007199254740992: got %v, want KEY_REUSE_MISMATCH", err)
	}
	// The exact original still replays idempotently.
	if _, err := env.store.Accept(ctx, env.scope, rA); err != nil {
		t.Fatalf("exact replay: %v", err)
	}

	// Duplicate keys are invalid input, not a silent last-write-wins.
	keyD := "op-" + randomHex(t, 4)
	rD := req(t, keyD)
	rD.Arguments = json.RawMessage(`{"a":1,"a":2}`)
	if _, err := env.store.Accept(ctx, env.scope, rD); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("duplicate JSON keys: got %v, want INVALID_ARGUMENT", err)
	}
	// And they cannot masquerade as an existing single-key request either.
	rDup := req(t, keyA)
	rDup.Arguments = json.RawMessage(`{"amount":1,"amount":1}`)
	if _, err := env.store.Accept(ctx, env.scope, rDup); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("duplicate keys on existing key: got %v, want INVALID_ARGUMENT", err)
	}
}

func TestLateCompleteAfterFenceRequiresReconciliation(t *testing.T) {
	// Regression: a dispatch whose response arrives after a Fence must not
	// complete the operation or release the guard early; only the trusted
	// Reconcile path may finalise the unknown outcome.
	ctx := context.Background()
	env := newEnv(t)
	guard := "account:" + randomHex(t, 4)
	result := json.RawMessage(`{"late":true}`)

	keyA := "op-" + randomHex(t, 4)
	rA := req(t, keyA)
	rA.Guard = guard
	if _, err := env.store.Accept(ctx, env.scope, rA); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, keyA); err != nil {
		t.Fatal(err)
	}
	// The dispatcher is cut off; the key is fenced while in flight.
	if op, err := env.store.Fence(ctx, env.scope, keyA); err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("fence in-flight: %+v err %v", op, err)
	}

	// The late response arrives. Complete is rejected; the row stays unknown.
	if _, err := env.store.Complete(ctx, env.scope, keyA, result, false); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("late Complete after fence: got %v, want CONFLICT", err)
	}
	if op, err := env.store.Get(ctx, env.scope, keyA); err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("row left unknown state: %+v err %v", op, err)
	}

	// The guard is still held while the outcome is unknown: no other
	// operation of this tenant may dispatch under the same guard.
	subjectB := env.scopeFor("subject-late-" + randomHex(t, 3))
	keyB := "op-" + randomHex(t, 4)
	rB := req(t, keyB)
	rB.Guard = guard
	if _, err := env.store.Accept(ctx, subjectB, rB); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, subjectB, keyB); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("guard must stay held after fence-induced unknown: got %v, want CONFLICT", err)
	}

	// Trusted reconciliation records the true outcome and releases the guard.
	if op, err := env.store.Reconcile(ctx, env.scope, keyA, result, false); err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("reconcile late outcome: %+v err %v", op, err)
	}
	if _, err := env.store.Dispatch(ctx, subjectB, keyB); err != nil {
		t.Fatalf("guard not released after reconciliation: %v", err)
	}
}

func TestDispatchRaceExactlyOneWins(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)
	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	const racers = 20
	var (
		wg      sync.WaitGroup
		wins    int64
		already int64
		other   int64
	)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := env.store.Dispatch(ctx, env.scope, key)
			switch {
			case err == nil:
				atomic.AddInt64(&wins, 1)
			case errors.Is(err, operations.ErrAlreadyDispatched):
				atomic.AddInt64(&already, 1)
			default:
				atomic.AddInt64(&other, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if other != 0 {
		t.Fatalf("%d dispatches failed with unexpected errors", other)
	}
	if wins != 1 {
		t.Fatalf("exactly one dispatch must win, got %d", wins)
	}
	if already != racers-1 {
		t.Fatalf("%d dispatches must see ALREADY_DISPATCHED, got %d", racers-1, already)
	}

	op, err := env.store.Get(ctx, env.scope, key)
	if err != nil || op.Status != operations.StatusDispatched {
		t.Fatalf("after race: %+v err %v", op, err)
	}

	// Even after completion, Dispatch never re-grants the effect.
	if _, err := env.store.Complete(ctx, env.scope, key, json.RawMessage(`{"ok":true}`), false); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("dispatch after terminal: got %v, want CONFLICT", err)
	}
}

func TestDispatchAcceptRaceNeverDoubleGrants(t *testing.T) {
	// Concurrent Accept + Fence + Dispatch on a fresh key: dispatch may win
	// before or after the fence, but it must never succeed against a fenced
	// key and must never be granted twice.
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)

	var wg sync.WaitGroup
	var dispatchWins, fenceOK int64
	acceptErrs := make([]error, 2)
	start := make(chan struct{})
	run := func(fn func() error, ok *int64) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if err := fn(); err == nil && ok != nil {
				atomic.AddInt64(ok, 1)
			}
		}()
	}
	run(func() error { _, err := env.store.Fence(ctx, env.scope, key); return err }, &fenceOK)
	run(func() error { _, err := env.store.Dispatch(ctx, env.scope, key); return err }, &dispatchWins)
	for i := range acceptErrs {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, acceptErrs[i] = env.store.Accept(ctx, env.scope, req(t, key))
		}()
	}
	close(start)
	wg.Wait()

	if dispatchWins > 1 {
		t.Fatalf("dispatch granted more than once: %d", dispatchWins)
	}
	if fenceOK == 0 {
		t.Fatal("fence must always succeed")
	}
	// Every Accept resolves cleanly: it either joins the open/in-flight
	// row (same fingerprint) or is CONFLICT-rejected against the sealed
	// one. No Accept may surface a unique violation or other error.
	for i, err := range acceptErrs {
		if err != nil && contracts.Code(err) != contracts.ErrConflict {
			t.Fatalf("Accept %d did not resolve cleanly: %v", i, err)
		}
	}
	op, err := env.store.Get(ctx, env.scope, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	switch op.Status {
	case operations.StatusFenced, operations.StatusSideEffectUnknown:
		// Fenced before dispatch, or dispatched then fenced.
	default:
		t.Fatalf("unexpected final status after fence race: %s", op.Status)
	}
	// Whatever the interleaving, the key is sealed: a fresh Accept is now
	// permanently rejected and Dispatch can never grant the effect again.
	// An in-flight (SIDE_EFFECT_UNKNOWN) row is not sealed: Accept must
	// replay it idempotently there, never surface a raw error.
	switch acc, err := env.store.Accept(ctx, env.scope, req(t, key)); {
	case op.Status == operations.StatusFenced && contracts.Code(err) != contracts.ErrConflict:
		t.Fatalf("Accept after fence race must be rejected: got %v, want CONFLICT", err)
	case op.Status == operations.StatusSideEffectUnknown && (err != nil || acc.Status != operations.StatusSideEffectUnknown):
		t.Fatalf("Accept must replay an in-flight row: %+v err %v", acc, err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); contracts.Code(err) != contracts.ErrConflict && !errors.Is(err, operations.ErrAlreadyDispatched) {
		t.Fatalf("Dispatch after race must not grant: got %v", err)
	}
}

func TestCompleteTransitionsAndReplay(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)
	result := json.RawMessage(`{"exit":0,"stdout":"ok"}`)

	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatal(err)
	}
	// Complete before dispatch is rejected.
	if _, err := env.store.Complete(ctx, env.scope, key, result, false); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("complete before dispatch: got %v, want CONFLICT", err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); err != nil {
		t.Fatal(err)
	}
	done, err := env.store.Complete(ctx, env.scope, key, result, false)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if done.Status != operations.StatusSucceeded || !json.Valid(done.Result) {
		t.Fatalf("unexpected completed operation: %+v", done)
	}
	// The stored result is the canonical jsonb form of what was submitted.
	if !jsonEqual(t, done.Result, result) {
		t.Fatalf("stored result %s differs from submitted %s", done.Result, result)
	}

	// Idempotent replay of the identical terminal result. The journal
	// compares results by canonical JSON value, so a byte-order variant of
	// the same document replays the stored operation unchanged. (Raw bytes
	// cannot be compared: PostgreSQL jsonb re-serialises the document.)
	replay, err := env.store.Complete(ctx, env.scope, key, result, false)
	if err != nil {
		t.Fatalf("replay Complete: %v", err)
	}
	if replay.Status != operations.StatusSucceeded || replay.Version != done.Version {
		t.Fatalf("replay changed the operation: %+v", replay)
	}
	reordered := json.RawMessage(`{"stdout":"ok", "exit": 0}`)
	replay2, err := env.store.Complete(ctx, env.scope, key, reordered, false)
	if err != nil {
		t.Fatalf("canonical replay Complete: %v", err)
	}
	if replay2.Version != done.Version {
		t.Fatalf("canonical replay changed the operation: %+v", replay2)
	}
	// A different result or status for a terminal row is rejected.
	if _, err := env.store.Complete(ctx, env.scope, key, json.RawMessage(`{"exit":1}`), false); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("conflicting result: got %v, want CONFLICT", err)
	}
	if _, err := env.store.Complete(ctx, env.scope, key, result, true); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("conflicting status: got %v, want CONFLICT", err)
	}
	// Terminal rows survive every later mutation attempt.
	if _, err := env.store.Fence(ctx, env.scope, key); err != nil {
		t.Fatalf("fence on terminal must be idempotent: %v", err)
	}
	if _, err := env.store.MarkUnknown(ctx, env.scope, key); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("mark unknown on terminal: got %v, want CONFLICT", err)
	}
	if _, err := env.store.Reconcile(ctx, env.scope, key, result, false); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("reconcile on terminal: got %v, want CONFLICT", err)
	}
	final, err := env.store.Get(ctx, env.scope, key)
	if err != nil || final.Status != operations.StatusSucceeded || !jsonEqual(t, final.Result, result) {
		t.Fatalf("terminal row mutated: %+v err %v", final, err)
	}
}

func TestFailedOutcomeAndReplay(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)
	failure := json.RawMessage(`{"error":"upstream rejected"}`)

	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); err != nil {
		t.Fatal(err)
	}
	done, err := env.store.Complete(ctx, env.scope, key, failure, true)
	if err != nil || done.Status != operations.StatusFailed {
		t.Fatalf("failed Complete: %+v err %v", done, err)
	}
	replay, err := env.store.Complete(ctx, env.scope, key, failure, true)
	if err != nil || replay.Status != operations.StatusFailed || !jsonEqual(t, replay.Result, failure) {
		t.Fatalf("failed replay: %+v err %v", replay, err)
	}
}

func TestFenceAbsentKeyTombstone(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	key := "ghost-" + randomHex(t, 4)

	fenced, err := env.store.Fence(ctx, env.scope, key)
	if err != nil {
		t.Fatalf("fence absent key: %v", err)
	}
	if fenced.Status != operations.StatusFenced {
		t.Fatalf("tombstone status = %s, want FENCED", fenced.Status)
	}
	// The tombstone is durable across connections (see restart test) and
	// permanently rejects Accept and Dispatch.
	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("accept on tombstone: got %v, want CONFLICT", err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("dispatch on tombstone: got %v, want CONFLICT", err)
	}
	// Re-fencing is idempotent.
	again, err := env.store.Fence(ctx, env.scope, key)
	if err != nil || again.Status != operations.StatusFenced {
		t.Fatalf("re-fence: %+v err %v", again, err)
	}
	// A different deployment of the same tenant has its own key space: the
	// tombstone does not leak there, and its own fence creates a separate
	// tombstone.
	otherDep := env.deploymentFor("dep-t-" + randomHex(t, 3))
	if _, err := env.store.Accept(ctx, otherDep, req(t, key)); err != nil {
		t.Fatalf("same key in another deployment must be independent: %v", err)
	}
}

func TestFenceTransitions(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)

	// ACCEPTED => FENCED.
	keyA := "op-" + randomHex(t, 4)
	if _, err := env.store.Accept(ctx, env.scope, req(t, keyA)); err != nil {
		t.Fatal(err)
	}
	if op, err := env.store.Fence(ctx, env.scope, keyA); err != nil || op.Status != operations.StatusFenced {
		t.Fatalf("fence accepted: %+v err %v", op, err)
	}
	// DISPATCHED => SIDE_EFFECT_UNKNOWN, and stays unknown on re-fence.
	keyD := "op-" + randomHex(t, 4)
	if _, err := env.store.Accept(ctx, env.scope, req(t, keyD)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, keyD); err != nil {
		t.Fatal(err)
	}
	if op, err := env.store.Fence(ctx, env.scope, keyD); err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("fence dispatched: %+v err %v", op, err)
	}
	if op, err := env.store.Fence(ctx, env.scope, keyD); err != nil || op.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("re-fence unknown: %+v err %v", op, err)
	}
	// A final status set via Reconcile is permanent.
	if op, err := env.store.Reconcile(ctx, env.scope, keyD, json.RawMessage(`{"recovered":true}`), false); err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("reconcile unknown: %+v err %v", op, err)
	}
	if op, err := env.store.Fence(ctx, env.scope, keyD); err != nil || op.Status != operations.StatusSucceeded {
		t.Fatalf("fence must not overwrite final status: %+v err %v", op, err)
	}
}

func TestMarkUnknownAndReconcile(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)

	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatal(err)
	}
	// MarkUnknown requires DISPATCHED.
	if _, err := env.store.MarkUnknown(ctx, env.scope, key); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("mark unknown on accepted: got %v, want CONFLICT", err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); err != nil {
		t.Fatal(err)
	}
	unk, err := env.store.MarkUnknown(ctx, env.scope, key)
	if err != nil || unk.Status != operations.StatusSideEffectUnknown {
		t.Fatalf("mark unknown: %+v err %v", unk, err)
	}
	// Idempotent.
	if again, err := env.store.MarkUnknown(ctx, env.scope, key); err != nil || again.Version != unk.Version {
		t.Fatalf("repeat mark unknown: %+v err %v", again, err)
	}
	// Complete cannot touch an unknown row; only Reconcile can.
	if _, err := env.store.Complete(ctx, env.scope, key, json.RawMessage(`{}`), false); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("complete on unknown: got %v, want CONFLICT", err)
	}
	// Reconcile to terminal with a result.
	term, err := env.store.Reconcile(ctx, env.scope, key, json.RawMessage(`{"outcome":"confirmed"}`), true)
	if err != nil || term.Status != operations.StatusFailed {
		t.Fatalf("reconcile: %+v err %v", term, err)
	}
	// Reconcile on a non-unknown row is rejected.
	if _, err := env.store.Reconcile(ctx, env.scope, key, json.RawMessage(`{"outcome":"other"}`), false); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("reconcile on terminal: got %v, want CONFLICT", err)
	}
}

func TestGuardHeldAcrossSubjectsAndDeployments(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	guard := "account:" + randomHex(t, 4)

	// Subject A accepts and dispatches a guarded operation.
	keyA := "op-" + randomHex(t, 4)
	rA := req(t, keyA)
	rA.Guard = guard
	if _, err := env.store.Accept(ctx, env.scope, rA); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, keyA); err != nil {
		t.Fatal(err)
	}

	// Subject B, different key, same tenant+guard: dispatch must be blocked.
	subjectB := env.scopeFor("subject-b-" + randomHex(t, 3))
	keyB := "op-" + randomHex(t, 4)
	rB := req(t, keyB)
	rB.Guard = guard
	if _, err := env.store.Accept(ctx, subjectB, rB); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, subjectB, keyB); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("guard bypass via another key/subject: got %v, want CONFLICT", err)
	}

	// A different deployment of the same tenant sharing the account is also
	// blocked while the guard is DISPATCHED.
	depC := env.deploymentFor("dep-c-" + randomHex(t, 3))
	keyC := "op-" + randomHex(t, 4)
	rC := req(t, keyC)
	rC.Guard = guard
	if _, err := env.store.Accept(ctx, depC, rC); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, depC, keyC); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("guard bypass via another deployment: got %v, want CONFLICT", err)
	}

	// While the guard is UNKNOWN it is still held.
	if _, err := env.store.MarkUnknown(ctx, env.scope, keyA); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, subjectB, keyB); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("guard must stay held while unknown: got %v, want CONFLICT", err)
	}
	// Reconciling to a terminal status releases the guard.
	if _, err := env.store.Reconcile(ctx, env.scope, keyA, json.RawMessage(`{"settled":true}`), false); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, subjectB, keyB); err != nil {
		t.Fatalf("guard not released after terminal: %v", err)
	}
	// Normal Complete also releases the guard.
	if _, err := env.store.Complete(ctx, subjectB, keyB, json.RawMessage(`{"done":true}`), false); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, depC, keyC); err != nil {
		t.Fatalf("guard not released after normal complete: %v", err)
	}
}

func TestGuardlessOperationsRunConcurrently(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)

	// Two unguarded operations from different subjects must both dispatch.
	ka := "op-" + randomHex(t, 4)
	kb := "op-" + randomHex(t, 4)
	if _, err := env.store.Accept(ctx, env.scope, req(t, ka)); err != nil {
		t.Fatal(err)
	}
	other := env.scopeFor("subject-b-" + randomHex(t, 3))
	if _, err := env.store.Accept(ctx, other, req(t, kb)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, ka); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, other, kb); err != nil {
		t.Fatalf("unguarded operations must not serialise: %v", err)
	}
}

func TestSubjectAndScopeIsolation(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)
	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatal(err)
	}

	// Another subject in the same scope cannot read or fence it.
	other := env.scopeFor("subject-x-" + randomHex(t, 3))
	if _, err := env.store.Get(ctx, other, key); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject Get: got %v, want NOT_FOUND", err)
	}
	if _, err := env.store.Fence(ctx, other, key); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject Fence: got %v, want NOT_FOUND", err)
	}
	if _, err := env.store.Dispatch(ctx, other, key); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject Dispatch: got %v, want NOT_FOUND", err)
	}
	// Another deployment cannot see it either.
	otherDep := env.deploymentFor("dep-x-" + randomHex(t, 3))
	if _, err := env.store.Get(ctx, otherDep, key); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-deployment Get: got %v, want NOT_FOUND", err)
	}
	// The owner still sees it, unchanged by the failed fence.
	got, err := env.store.Get(ctx, env.scope, key)
	if err != nil || got.Status != operations.StatusAccepted {
		t.Fatalf("owner Get: %+v err %v (status=%s)", got, err, got.Status)
	}
	// Accepting the same key under a different subject never returns the
	// foreign operation: it is rejected without exposing any state.
	if _, err := env.store.Accept(ctx, other, req(t, key)); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject Accept: got %v, want NOT_FOUND", err)
	}
}

func TestCrossSubjectCannotSeeFencedState(t *testing.T) {
	// A tombstone owned by one subject must be indistinguishable from a
	// missing key for every other subject: NOT_FOUND on Get, Accept and
	// Fence, with no fenced status or digest ever returned.
	ctx := context.Background()
	env := newEnv(t)
	key := "ghost-" + randomHex(t, 4)

	if _, err := env.store.Fence(ctx, env.scope, key); err != nil {
		t.Fatalf("owner fence: %v", err)
	}
	other := env.scopeFor("subject-y-" + randomHex(t, 3))
	if _, err := env.store.Get(ctx, other, key); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject Get on tombstone: got %v, want NOT_FOUND", err)
	}
	if _, err := env.store.Accept(ctx, other, req(t, key)); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject Accept on tombstone: got %v, want NOT_FOUND", err)
	}
	if _, err := env.store.Fence(ctx, other, key); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("cross-subject Fence on tombstone: got %v, want NOT_FOUND", err)
	}
	// The owner still sees its own tombstone.
	if op, err := env.store.Get(ctx, env.scope, key); err != nil || op.Status != operations.StatusFenced {
		t.Fatalf("owner Get on tombstone: %+v err %v", op, err)
	}
}

func TestRestartDurability(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	if env.schema == "" {
		t.Fatal("testEnv must record its schema")
	}
	key := "op-" + randomHex(t, 4)
	tomb := "ghost-" + randomHex(t, 4)
	result := json.RawMessage(`{"durable":true}`)

	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Complete(ctx, env.scope, key, result, false); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Fence(ctx, env.scope, tomb); err != nil {
		t.Fatal(err)
	}

	// Simulate a service restart: drop every connection, reconnect, migrate
	// again and verify the journal is intact.
	env.pool.Close()
	pool, err := pgxpool.New(ctx, os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatalf("reconnect: %v", err)
	}
	defer pool.Close()
	store, err := operations.New(pool, env.schema)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate after restart: %v", err)
	}

	got, err := store.Get(ctx, env.scope, key)
	if err != nil || got.Status != operations.StatusSucceeded || !jsonEqual(t, got.Result, result) {
		t.Fatalf("journal lost terminal state across restart: %+v err %v", got, err)
	}
	if _, err := store.Accept(ctx, env.scope, req(t, tomb)); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("tombstone not durable: got %v, want CONFLICT", err)
	}
}

func TestNewRejectsInvalidSchema(t *testing.T) {
	ctx := context.Background()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("integration test requires AHP_TEST_DATABASE_URL pointing at a live PostgreSQL instance")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	for _, bad := range []string{"tools", "ahp_Tools", `ahp_x"; DROP TABLE t; --`, "public", "ahp_"} {
		if _, err := operations.New(pool, bad); contracts.Code(err) != contracts.ErrInvalid {
			t.Errorf("New(%q): got %v, want INVALID_ARGUMENT", bad, err)
		}
	}
	if _, err := operations.New(nil, "ahp_ok"); contracts.Code(err) != contracts.ErrInvalid {
		t.Errorf("New(nil pool): got %v, want INVALID_ARGUMENT", err)
	}
}

func TestConcurrentFenceAndCompleteRace(t *testing.T) {
	// Fence and Complete race on a DISPATCHED row: the outcome is either
	// SUCCEEDED (complete won) or SIDE_EFFECT_UNKNOWN (fence won), never a
	// mix, and the loser's later reconcile/complete follows the rules.
	ctx := context.Background()
	env := newEnv(t)
	key := "op-" + randomHex(t, 4)
	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatal(err)
	}
	if _, err := env.store.Dispatch(ctx, env.scope, key); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]error, 2)
	actions := []func() error{
		func() error {
			_, err := env.store.Fence(ctx, env.scope, key)
			return err
		},
		func() error {
			_, err := env.store.Complete(ctx, env.scope, key, json.RawMessage(`{"won":true}`), false)
			return err
		},
	}
	for i := range actions {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = actions[i]()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range results {
		if err != nil && contracts.Code(err) != contracts.ErrConflict {
			t.Fatalf("action %d failed unexpectedly: %v", i, err)
		}
	}
	op, err := env.store.Get(ctx, env.scope, key)
	if err != nil {
		t.Fatal(err)
	}
	switch op.Status {
	case operations.StatusSucceeded:
		// Complete won; fence must not have overwritten it.
		if _, err := env.store.Fence(ctx, env.scope, key); err != nil {
			t.Fatalf("fence on succeeded: %v", err)
		}
		if again, _ := env.store.Get(ctx, env.scope, key); again.Status != operations.StatusSucceeded {
			t.Fatalf("succeeded row overwritten: %s", again.Status)
		}
	case operations.StatusSideEffectUnknown:
		// Fence won; Complete must now be rejected and Reconcile finalises.
		if _, err := env.store.Complete(ctx, env.scope, key, json.RawMessage(`{"won":true}`), false); contracts.Code(err) != contracts.ErrConflict {
			t.Fatalf("complete after fence: got %v, want CONFLICT", err)
		}
		if _, err := env.store.Reconcile(ctx, env.scope, key, json.RawMessage(`{"settled":"by reconciliation"}`), true); err != nil {
			t.Fatalf("reconcile after fence: %v", err)
		}
	default:
		t.Fatalf("unexpected race outcome: %s", op.Status)
	}
}

func TestKeyLengthBoundEnforcedInStore(t *testing.T) {
	ctx := context.Background()
	env := newEnv(t)
	long := "op-" + strings.Repeat("x", maxKeyLenForTest)
	if _, err := env.store.Accept(ctx, env.scope, req(t, long)); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("oversized key: got %v, want INVALID_ARGUMENT", err)
	}
	if _, err := env.store.Get(ctx, env.scope, long); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("oversized key Get: got %v, want INVALID_ARGUMENT", err)
	}
}

// maxKeyLenForTest mirrors the package bound; kept literal so a silent
// loosening of the bound breaks the test loudly.
const maxKeyLenForTest = 128

func TestOperationKeyInterop(t *testing.T) {
	// The journal accepts keys produced by contracts.OperationKey verbatim.
	ctx := context.Background()
	env := newEnv(t)
	key := contracts.OperationKey(env.scope, "run-1", "node-2", "call-3")
	if _, err := env.store.Accept(ctx, env.scope, req(t, key)); err != nil {
		t.Fatalf("Accept with OperationKey: %v", err)
	}
	got, err := env.store.Get(ctx, env.scope, key)
	if err != nil || got.Key != key {
		t.Fatalf("Get by OperationKey: %+v err %v", got, err)
	}
	_ = fmt.Sprintf
}
