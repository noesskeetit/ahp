package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func newEventStore(t *testing.T) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Per-run database (isolation_test.go): see the note in kv_test.go.
	pool, _ := isolatedPool(t)
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate core: %v", err)
	}
	if err := s.MigrateEvents(ctx); err != nil {
		t.Fatalf("migrate events: %v", err)
	}
	return s
}

func eventScope(subject string) contracts.Scope {
	return contracts.Scope{Tenant: "t-" + subject, Deployment: "d1", Subject: subject}
}

// setupWaiting parks the head run of a fresh thread in waiting via Suspend,
// which is the only supported way to reach RunWaiting under a lease. The
// returned lease is the one that was used to suspend: it is already released
// (Suspend clears the thread lease), so it is valid only as an event key
// carrier and as a fence reference — tests needing an ACTIVE lease must wake
// the run (setWaitFor + Enqueue of the waited kind, or a deadline wake) and
// reclaim it via ClaimRun. Never bypass fencing with raw SQL.
func setupWaiting(t *testing.T) (*Store, contracts.Scope, string, *Lease) {
	t.Helper()
	s := newEventStore(t)
	ctx := context.Background()
	scope := eventScope("subj-" + contracts.NewID())
	th, err := s.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := s.CreateRun(ctx, scope, th.ID, "task-1", "k-"+contracts.NewID(), json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w1", time.Minute)
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if lease.Run.ID != run.ID {
		t.Fatalf("claimed run %s, want %s", lease.Run.ID, run.ID)
	}
	if err := s.Suspend(ctx, scope, run.ID, lease.Worker, lease.Epoch, "payment", nil); err != nil {
		t.Fatalf("Suspend waiting: %v", err)
	}
	return s, scope, run.ID, lease
}

// wakeAndReclaim reclaims the suspended head run of setupWaiting as a fresh,
// active lease. The run must already be woken (pending) — this never bypasses
// fencing, it goes through the regular claim path.
func wakeAndReclaim(t *testing.T, s *Store, scope contracts.Scope, worker string) *Lease {
	t.Helper()
	lease, err := s.ClaimRun(context.Background(), scope.Tenant, scope.Deployment, worker, time.Minute)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if lease == nil {
		t.Fatalf("reclaim: nothing claimable in scope %s", scope.Tenant)
	}
	return lease
}

func TestEnqueueWakesMatchingWait(t *testing.T) {
	s, scope, runID, lease := setupWaiting(t)
	ctx := context.Background()
	setWaitFor(t, s, runID, "payment")

	ev, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "payment", json.RawMessage(`{"amt":10}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if ev.Sequence != 1 || ev.Acked {
		t.Fatalf("unexpected event %+v", ev)
	}
	status, waitFor := runState(t, s, runID)
	if status != RunPending || waitFor != "" {
		t.Fatalf("run not woken: status=%s wait_for=%s", status, waitFor)
	}
}

func TestEnqueueUnrelatedKindDoesNotWake(t *testing.T) {
	s, scope, runID, lease := setupWaiting(t)
	ctx := context.Background()
	setWaitFor(t, s, runID, "payment")

	if _, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "shipping", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	status, waitFor := runState(t, s, runID)
	if status != RunWaiting || waitFor != "payment" {
		t.Fatalf("run should stay waiting: status=%s wait_for=%s", status, waitFor)
	}
}

func TestEnqueueReplayAndMismatch(t *testing.T) {
	s, scope, _, lease := setupWaiting(t)
	ctx := context.Background()
	body := json.RawMessage(`{"amt":10}`)

	first, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "payment", body)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	replay, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "payment", json.RawMessage(`{"amt": 10}`))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replay.ID != first.ID || replay.Sequence != first.Sequence {
		t.Fatalf("replay returned new event: %+v vs %+v", replay, first)
	}
	// The store dedupes per (thread,key) and binds the stored digest to the
	// (kind,payload) body: a differing kind under a reused key is rejected...
	_, err = s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "refund", json.RawMessage(`{"amt":99}`))
	if code := contracts.Code(err); code != "KEY_REUSE_MISMATCH" {
		t.Fatalf("want KEY_REUSE_MISMATCH, got %v (code %s)", err, code)
	}
	// ...and so is a differing payload under the same (key,kind): the replay
	// path must compare the persisted digest, never the raw JSON text.
	_, err = s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "payment", json.RawMessage(`{"amt":11}`))
	if code := contracts.Code(err); code != "KEY_REUSE_MISMATCH" {
		t.Fatalf("changed-payload replay: want KEY_REUSE_MISMATCH, got %v (code %s)", err, code)
	}
}

func TestStreamSequenceAndReplay(t *testing.T) {
	s, scope, runID, lease := setupWaiting(t)
	ctx := context.Background()

	// The suspended fixture holds no active lease: wake the run through the
	// inbox path, then reclaim it under a fresh fence.
	setWaitFor(t, s, runID, "payment")
	if _, err := s.Enqueue(ctx, scope, lease.Thread.ID, "wake", "payment", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("wake Enqueue: %v", err)
	}
	lease = wakeAndReclaim(t, s, scope, "w2")

	e1, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "sk1", "token", json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("AppendStream 1: %v", err)
	}
	e2, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "sk2", "token", json.RawMessage(`{"n":2}`))
	if err != nil {
		t.Fatalf("AppendStream 2: %v", err)
	}
	if e1.Sequence != 1 || e2.Sequence != 2 {
		t.Fatalf("bad stream sequence: %d %d", e1.Sequence, e2.Sequence)
	}
	replay, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "sk1", "token", json.RawMessage(`{"n": 1}`))
	if err != nil {
		t.Fatalf("stream replay: %v", err)
	}
	if replay.ID != e1.ID {
		t.Fatalf("stream replay returned new event")
	}
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "sk1", "other", json.RawMessage(`{"n":1}`)); contracts.Code(err) != "KEY_REUSE_MISMATCH" {
		t.Fatalf("want KEY_REUSE_MISMATCH, got %v", err)
	}
	// Changed payload under the same (key,kind) must also fail replay.
	if _, err := s.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "sk1", "token", json.RawMessage(`{"n":99}`)); contracts.Code(err) != "KEY_REUSE_MISMATCH" {
		t.Fatalf("changed-payload stream replay: want KEY_REUSE_MISMATCH, got %v", err)
	}

	events, err := s.ReadStream(ctx, scope, lease.Thread.ID, 0, 10)
	if err != nil {
		t.Fatalf("ReadStream: %v", err)
	}
	if len(events) != 2 || events[0].Sequence != 1 || events[1].Sequence != 2 {
		t.Fatalf("unexpected stream read: %+v", events)
	}
	tail, err := s.ReadStream(ctx, scope, lease.Thread.ID, 1, 10)
	if err != nil {
		t.Fatalf("ReadStream tail: %v", err)
	}
	if len(tail) != 1 || tail[0].ID != e2.ID {
		t.Fatalf("unexpected tail: %+v", tail)
	}
}

func TestStaleEpochRejected(t *testing.T) {
	s, scope, runID, lease := setupWaiting(t)
	ctx := context.Background()

	// Suspend released the lease; wake and reclaim so the run has a live
	// fence, then present an epoch far above the current one.
	setWaitFor(t, s, runID, "payment")
	if _, err := s.Enqueue(ctx, scope, lease.Thread.ID, "wake", "payment", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("wake Enqueue: %v", err)
	}
	lease = wakeAndReclaim(t, s, scope, "w2")

	ev, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "payment", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := s.AckInbox(ctx, scope, runID, lease.Worker, lease.Epoch+1000, []string{ev.ID}); contracts.Code(err) != "STALE_EPOCH" {
		t.Fatalf("want STALE_EPOCH, got %v (code %s)", err, contracts.Code(err))
	}
	// An empty id list must still be fenced: no early return around the lease.
	if err := s.AckInbox(ctx, scope, runID, lease.Worker, lease.Epoch+1000, nil); contracts.Code(err) != "STALE_EPOCH" {
		t.Fatalf("want STALE_EPOCH on empty ack, got %v (code %s)", err, contracts.Code(err))
	}
	if _, err := s.AppendStream(ctx, scope, runID, lease.Worker, lease.Epoch+1000, "sk", "k", json.RawMessage(`{}`)); contracts.Code(err) != "STALE_EPOCH" {
		t.Fatalf("want STALE_EPOCH on append, got %v", err)
	}
}

func TestForeignThreadNotFound(t *testing.T) {
	s, scope, _, lease := setupWaiting(t)
	ctx := context.Background()
	// Same tenant and deployment as the owner; only the subject differs, so a
	// pass here proves the subject dimension of scope isolation.
	other := contracts.Scope{Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: "subj-" + contracts.NewID()}
	if _, err := s.Enqueue(ctx, other, lease.Thread.ID, "k1", "payment", json.RawMessage(`{}`)); contracts.Code(err) != "NOT_FOUND" {
		t.Fatalf("want NOT_FOUND, got %v (code %s)", err, contracts.Code(err))
	}
	// Reads are scope-checked: the foreign subject sees no events, and the
	// foreign thread itself is reported NOT_FOUND by the scope-checked
	// thread lookup (never a probeable success over a foreign thread).
	// The production read path must report NOT_FOUND for a foreign thread
	// instead of an empty success (no probeable difference for outsiders).
	if _, err := s.ReadInbox(ctx, other, lease.Thread.ID, 0, 10); contracts.Code(err) != "NOT_FOUND" {
		t.Fatalf("foreign ReadInbox want NOT_FOUND, got %v (code %s)", err, contracts.Code(err))
	}
	if _, err := s.ReadStream(ctx, other, lease.Thread.ID, 0, 10); contracts.Code(err) != "NOT_FOUND" {
		t.Fatalf("foreign ReadStream want NOT_FOUND, got %v (code %s)", err, contracts.Code(err))
	}
	if _, err := s.GetThread(ctx, other, lease.Thread.ID); contracts.Code(err) != "NOT_FOUND" {
		t.Fatalf("foreign GetThread want NOT_FOUND, got %v (code %s)", err, contracts.Code(err))
	}
	// Same-scope reads keep working.
	if _, err := s.ReadInbox(ctx, scope, lease.Thread.ID, 0, 10); err != nil {
		t.Fatalf("own ReadInbox: %v", err)
	}
	if _, err := s.ReadStream(ctx, scope, lease.Thread.ID, 0, 10); err != nil {
		t.Fatalf("own ReadStream: %v", err)
	}
}

func TestAckInboxScopedIdempotent(t *testing.T) {
	s, scope, runID, lease := setupWaiting(t)
	ctx := context.Background()

	// AckInbox needs a live lease: wake the suspended run and reclaim it.
	setWaitFor(t, s, runID, "payment")
	if _, err := s.Enqueue(ctx, scope, lease.Thread.ID, "wake", "payment", json.RawMessage(`{}`)); err != nil {
		t.Fatalf("wake Enqueue: %v", err)
	}
	lease = wakeAndReclaim(t, s, scope, "w2")

	ev, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "payment", json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := s.AckInbox(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, []string{ev.ID}); err != nil {
		t.Fatalf("AckInbox: %v", err)
	}
	if err := s.AckInbox(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, []string{ev.ID}); err != nil {
		t.Fatalf("AckInbox replay: %v", err)
	}
	events, err := s.ReadInbox(ctx, scope, lease.Thread.ID, 0, 10)
	if err != nil {
		t.Fatalf("ReadInbox: %v", err)
	}
	// The wake event consumed by the reclaim path is still unacked, so the
	// only acceptable residue of this test is that one event; the acked one
	// must be gone.
	for _, e := range events {
		if e.ID == ev.ID {
			t.Fatalf("acked event still visible: %+v", events)
		}
	}
}

func TestPayloadLimits(t *testing.T) {
	s, scope, _, lease := setupWaiting(t)
	ctx := context.Background()
	if _, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k1", "payment", json.RawMessage(`not json`)); err == nil {
		t.Fatalf("want invalid JSON error")
	}
	big := append([]byte(`{"x":"`), make([]byte, MaxPayloadBytes)...)
	big = append(big, []byte(`"}`)...)
	if _, err := s.Enqueue(ctx, scope, lease.Thread.ID, "k2", "payment", big); err == nil {
		t.Fatalf("want oversized payload error")
	}
}

func runState(t *testing.T, s *Store, runID string) (string, string) {
	t.Helper()
	var status, waitFor string
	err := s.pool.QueryRow(context.Background(),
		`SELECT status, wait_for FROM ahp_state.runs WHERE id = $1`, runID).
		Scan(&status, &waitFor)
	if err != nil {
		t.Fatalf("runState: %v", err)
	}
	return status, waitFor
}

func setWaitFor(t *testing.T, s *Store, runID, waitFor string) {
	t.Helper()
	_, err := s.pool.Exec(context.Background(),
		`UPDATE ahp_state.runs SET wait_for = $2 WHERE id = $1`, runID, waitFor)
	if err != nil {
		t.Fatalf("setWaitFor: %v", err)
	}
}
