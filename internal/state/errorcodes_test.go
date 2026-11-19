package state

// Every error path of the State store must carry a stable contract code,
// because the transport maps ONLY contract codes: anything uncategorized
// reaches the caller as an anonymous HTTP 500. Space already shows the cost of
// that (a State error surfacing as a generic 500), so these tests pin the
// distinguishable codes for the paths the SDK and the A2A agent actually hit:
// a stale fence, a missing snapshot/part/run/intent, a foreign scope, an
// illegal intent transition, an import collision - and the rule that a raw
// database failure is never surfaced as 500 with driver text attached.
//
// "Driver text" means the message PostgreSQL wrote, which can carry identifiers.
// A SQLSTATE is not driver text: it is a fixed five-character classification, and
// the concurrency aborts quote exactly that and nothing more, because the caller
// has to tell "the database aborted you, replay" apart from "you lost on a rule,
// change the request".

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestStaleFenceIsDistinguishable(t *testing.T) {
	e := kvFixture(t)
	stale := Fence{RunID: e.fence.RunID, Worker: e.fence.Worker, Epoch: e.fence.Epoch + 1}
	ns := []string{"codes", "fence"}

	if _, err := e.s.PutKV(e.ctx, e.scope, stale, ns, "k", "op-"+contracts.NewID(), 0,
		json.RawMessage(`{"a":1}`)); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("PutKV with a future epoch: want STALE_EPOCH, got %v", err)
	}
	otherWorker := Fence{RunID: e.fence.RunID, Worker: "someone-else", Epoch: e.fence.Epoch}
	if _, err := e.s.Heartbeat(e.ctx, e.scope, otherWorker.RunID, otherWorker.Worker,
		otherWorker.Epoch, time.Minute); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("Heartbeat by another worker: want STALE_EPOCH, got %v", err)
	}
	if err := e.s.Release(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch, RunDone); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if _, err := e.s.Heartbeat(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		time.Minute); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("Heartbeat after Release: want STALE_EPOCH, got %v", err)
	}
	// Every lease-bound mutation reports the same code once the lease is gone.
	for name, call := range map[string]func() error{
		"PutCheckpoint": func() error {
			_, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{ID: "cp-x",
				Namespace: "ns", Version: 1}, "key-x")
			return err
		},
		"PutWrites": func() error {
			return e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "cp-x",
				[]PendingWrite{{Task: "t", Index: 0, Channel: "c", Data: []byte("d")}})
		},
		"AppendStream": func() error {
			_, err := e.s.AppendStream(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
				"k", "token", json.RawMessage(`{"a":1}`))
			return err
		},
		"RecordIntent": func() error {
			_, err := e.s.RecordIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
				"n", "c", "tool", "target", json.RawMessage(`{}`))
			return err
		},
		"PutBatch": func() error {
			_, err := e.s.PutBatch(e.ctx, e.scope, e.fence, BatchRequest{Key: "b"})
			return err
		},
		"Suspend": func() error {
			return e.s.Suspend(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch, "resume", nil)
		},
	} {
		if code := contracts.Code(call()); code != contracts.ErrStaleEpoch {
			t.Fatalf("%s after lease loss: want STALE_EPOCH, got %v", name, code)
		}
	}
}

func TestMissingResourcesAreNotFound(t *testing.T) {
	e := cpFixture(t)
	missing := map[string]func() error{
		"GetThread":          func() error { _, err := e.s.GetThread(e.ctx, e.scope, "nope"); return err },
		"GetRun":             func() error { _, err := e.s.GetRun(e.ctx, e.scope, "nope"); return err },
		"GetThreadByContext": func() error { _, err := e.s.GetThreadByContext(e.ctx, e.scope, "nope"); return err },
		"GetCheckpoint":      func() error { _, err := e.s.GetCheckpoint(e.ctx, e.scope, e.threadID, "ns", "nope"); return err },
		"GetPart":            func() error { _, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "ch", "v-none"); return err },
		"GetWrites":          func() error { _, err := e.s.GetWrites(e.ctx, e.scope, "nope", "ns", "nope"); return err },
		"ListCheckpoints":    func() error { _, err := e.s.ListCheckpoints(e.ctx, e.scope, "nope", "ns", 10); return err },
		"GetIntent":          func() error { _, err := e.s.GetIntent(e.ctx, e.scope, "nope", "n", "c"); return err },
		"ReadInbox":          func() error { _, err := e.s.ReadInbox(e.ctx, e.scope, "nope", 0, 10); return err },
		"ReadStream":         func() error { _, err := e.s.ReadStream(e.ctx, e.scope, "nope", 0, 10); return err },
		"ExportThread":       func() error { _, err := e.s.ExportThread(e.ctx, e.scope, "nope"); return err },
		"DeleteThread":       func() error { return e.s.DeleteThread(e.ctx, e.scope, "nope") },
		"Enqueue": func() error {
			_, err := e.s.Enqueue(e.ctx, e.scope, "nope", "k", "kind", json.RawMessage(`{}`))
			return err
		},
		"AckInbox": func() error {
			return e.s.AckInbox(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch, []string{"no-event"})
		},
	}
	for name, call := range missing {
		if code := contracts.Code(call()); code != contracts.ErrNotFound {
			t.Fatalf("%s: want NOT_FOUND, got %v", name, code)
		}
	}
}

// TestWritesOfUnknownCheckpointAreEmpty documents the deliberate asymmetry: a
// thread that does not exist is NOT_FOUND, but an unknown checkpoint inside a
// live thread has no empty list to distinguish from "no writes yet", because
// PutWrites buckets are keyed deterministically and never pre-register a
// checkpoint id. Clients must therefore read writes, not probe for them.
func TestWritesOfUnknownCheckpointAreEmpty(t *testing.T) {
	e := cpFixture(t)
	writes, err := e.s.GetWrites(e.ctx, e.scope, e.threadID, "ns", "cp-never-written")
	if err != nil {
		t.Fatalf("GetWrites on an unknown checkpoint must not fail: %v", err)
	}
	if writes == nil {
		t.Fatalf("writes must materialize as an empty slice, never nil")
	}
	if len(writes) != 0 {
		t.Fatalf("want no writes, got %d", len(writes))
	}
}

// TestForeignScopeIsReportedAsNotFound encodes the probing rule: a resource of
// another subject is invisible, never "forbidden".
func TestForeignScopeIsReportedAsNotFound(t *testing.T) {
	e := cpFixture(t)
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-foreign",
		Part{Channel: "values", Version: "v1", Data: []byte("secret")}), "key-foreign"); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	foreign := contracts.Scope{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment,
		Subject: "subj-" + contracts.NewID()}
	calls := map[string]func() error{
		"GetThread":     func() error { _, err := e.s.GetThread(e.ctx, foreign, e.threadID); return err },
		"GetCheckpoint": func() error { _, err := e.s.GetCheckpoint(e.ctx, foreign, e.threadID, "ns", "cp-foreign"); return err },
		"GetPart":       func() error { _, err := e.s.GetPart(e.ctx, foreign, e.threadID, "ns", "values", "v1"); return err },
		"GetIntent":     func() error { _, err := e.s.GetIntent(e.ctx, foreign, e.fence.RunID, "n", "c"); return err },
		"ExportThread":  func() error { _, err := e.s.ExportThread(e.ctx, foreign, e.threadID); return err },
		"DeleteThread":  func() error { return e.s.DeleteThread(e.ctx, foreign, e.threadID) },
		"Heartbeat": func() error {
			_, err := e.s.Heartbeat(e.ctx, foreign, e.fence.RunID, e.fence.Worker, e.fence.Epoch, time.Minute)
			return err
		},
	}
	for name, call := range calls {
		if code := contracts.Code(call()); code != contracts.ErrNotFound {
			t.Fatalf("%s from a foreign subject: want NOT_FOUND, got %v", name, code)
		}
	}
}

func TestUnknownIntentCompletionIsConflict(t *testing.T) {
	e := cpFixture(t)
	intent, err := e.s.RecordIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"nodeA", "callA", "tool", "target", json.RawMessage(`{"a":1}`))
	if err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	// args is optional in the contract: an intent without args is legal.
	if _, err := e.s.RecordIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"nodeA", "callNoArgs", "tool", "target", nil); err != nil {
		t.Fatalf("RecordIntent without args must be accepted: %v", err)
	}
	if _, err := e.s.UpdateIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"nodeA", "callA", IntentDispatched, nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, err := e.s.UpdateIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"nodeA", "callA", IntentUnknown, nil); err != nil {
		t.Fatalf("mark unknown: %v", err)
	}
	_, err = e.s.UpdateIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"nodeA", "callA", IntentSucceeded, json.RawMessage(`{"ok":true}`))
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("completing an unknown intent: want CONFLICT, got %v", err)
	}
	if !errors.Is(err, ErrNotReconcilable) {
		t.Fatalf("the refusal must stay identifiable: %v", err)
	}
	if _, err := e.s.ReconcileIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"nodeA", "callA", IntentSucceeded, "sha256:wrong", nil); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("reconciliation with the wrong fingerprint: want KEY_REUSE_MISMATCH, got %v", err)
	}
	if _, err := e.s.ReconcileIntent(e.ctx, e.scope, e.fence.RunID, e.fence.Worker, e.fence.Epoch,
		"nodeA", "callA", IntentSucceeded, intent.Digest, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatalf("reconciliation with the exact fingerprint: %v", err)
	}
}

func TestImportThreadRefusesContextCollision(t *testing.T) {
	e := cpFixture(t)
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, cp("cp-export",
		Part{Channel: "values", Version: "v1", Data: []byte("bytes")}), "key-export"); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	exp, err := e.s.ExportThread(e.ctx, e.scope, e.threadID)
	if err != nil {
		t.Fatalf("ExportThread: %v", err)
	}
	// The target scope still owns this context_id: an import must be refused
	// rather than merged into the live thread.
	if _, err := e.s.ImportThread(e.ctx, e.scope, exp, ""); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("import into the owning scope: want CONFLICT, got %v", err)
	} else if reason := contracts.ReasonOf(err); reason != ReasonImportContextTaken {
		t.Fatalf("import into the owning scope: reason = %q, want %s", reason, ReasonImportContextTaken)
	}
	before, err := e.s.ListCheckpoints(e.ctx, e.scope, e.threadID, "ns", 10)
	if err != nil || len(before) != 1 {
		t.Fatalf("the refused import disturbed the live thread: %d checkpoints, err=%v", len(before), err)
	}
	// The same snapshot under a context nobody owns imports as a fresh thread.
	exp.Thread.ContextID = "ctx-import-" + contracts.NewID()
	imported, err := e.s.ImportThread(e.ctx, e.scope, exp, "")
	if err != nil {
		t.Fatalf("ImportThread(fresh context): %v", err)
	}
	if imported.ID == e.threadID {
		t.Fatalf("import reused the source thread id")
	}
	parts, err := e.s.ListCheckpoints(e.ctx, e.scope, imported.ID, "ns", 10)
	if err != nil || len(parts) != 1 {
		t.Fatalf("the import lost the snapshots: %d checkpoints, err=%v", len(parts), err)
	}
}

// TestClassifyStoreError pins the last-resort rule: an uncategorized database
// failure becomes a distinguishable contract code without driver text, while
// the driver error stays reachable for callers that branch on the SQL state.
func TestClassifyStoreError(t *testing.T) {
	pg := func(code string) error {
		return &pgconn.PgError{Code: code, Message: "duplicate key value violates unique constraint \"x\"",
			ConstraintName: "kv_pkey", Detail: `Key (id)=(secret-value) already exists.`}
	}
	cases := map[string]struct {
		in   error
		want string
	}{
		"unique":         {pg("23505"), contracts.ErrConflict},
		"foreign key":    {pg("23503"), contracts.ErrNotFound},
		"check":          {pg("23514"), contracts.ErrNotFound},
		"not null":       {pg("23502"), contracts.ErrInvalid},
		"value too long": {pg("22001"), contracts.ErrInvalid},
		"bad text":       {pg("22P02"), contracts.ErrInvalid},
		// Concurrency aborts are availability, not conflict: see
		// TestConcurrencyAbortsAreClassifiedByAction.
		"serialization":     {pg("40001"), contracts.ErrUnavailable},
		"deadlock":          {pg("40P01"), contracts.ErrUnavailable},
		"availability":      {pg("57P01"), ""},
		"already code-free": {errors.New("boom"), ""},
	}
	for name, tc := range cases {
		got := classifyStoreError(tc.in)
		if code := contracts.Code(got); code != tc.want {
			t.Fatalf("%s: want code %q, got %q (%v)", name, tc.want, code, got)
		}
		if tc.want == "" && got != tc.in {
			t.Fatalf("%s: uncategorized errors must pass through untouched", name)
		}
		if tc.want != "" {
			if msg := got.Error(); msg != contracts.Code(got)+": "+errMessage(got) {
				t.Fatalf("%s: the message must be the contract text only, got %q", name, msg)
			}
			var pe *pgconn.PgError
			var in *pgconn.PgError
			if !errors.As(got, &pe) || !errors.As(tc.in, &in) || pe.Code != in.Code {
				t.Fatalf("%s: the driver error must stay reachable through Unwrap", name)
			}
		}
	}
	// A contract error is never re-wrapped.
	already := contracts.NewError(contracts.ErrForbidden, "nope")
	if classifyStoreError(already) != already {
		t.Fatalf("classified errors must pass through untouched")
	}
	if classifyStoreError(nil) != nil {
		t.Fatalf("nil must stay nil")
	}
}

// TestConcurrencyAbortsAreClassifiedByAction pins the classification by what the
// caller must DO, not by how the database felt about it.
//
// 23505 means the write was evaluated and lost on a rule the caller can fix: a
// replay returns the same answer, so it is a conflict and it is final. 40001 and
// 40P01 mean the write was never evaluated at all - permission to commit was
// refused - so it is availability, the transport answers 503, and the only useful
// instruction is to replay. Before this rule both arrived as CONFLICT, which is
// how a lease release that lost a lock cycle was read by its caller as a decision
// about its contents rather than as a request to try again.
func TestConcurrencyAbortsAreClassifiedByAction(t *testing.T) {
	cases := []struct {
		name        string
		sqlstate    string
		want        string
		quotesState bool
		replayable  bool
	}{
		{"deadlock between two writers", "40P01", contracts.ErrUnavailable, true, true},
		{"serialization failure", "40001", contracts.ErrUnavailable, true, true},
		{"unique rule rejected the write", "23505", contracts.ErrConflict, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := &pgconn.PgError{Code: tc.sqlstate,
				Message:        "deadlock detected on a name nobody may echo",
				Detail:         "Key (id)=(secret-value) already exists.",
				ConstraintName: "kv_pkey"}
			got := classifyStoreError(raw)
			if code := contracts.Code(got); code != tc.want {
				t.Fatalf("want %s, got %s (%v)", tc.want, code, got)
			}
			msg := errMessage(got)
			if quoted := strings.Contains(msg, "SQLSTATE "+tc.sqlstate); quoted != tc.quotesState {
				t.Fatalf("SQLSTATE quoted = %v, want %v; message %q", quoted, tc.quotesState, msg)
			}
			// Nothing that could name a row, a key or a value may ride along.
			for _, forbidden := range []string{"secret-value", "kv_pkey", "deadlock detected"} {
				if strings.Contains(msg, forbidden) {
					t.Fatalf("the message carries driver text %q", forbidden)
				}
			}
			// The replay decision is taken from the driver state, not from the
			// contract code, so it must survive the classification above.
			if isTransientAbort(got) != tc.replayable {
				t.Fatalf("replayable = %v, want %v for %s", isTransientAbort(got), tc.replayable, msg)
			}
		})
	}
}

// errMessage extracts the message part of a "CODE: message" error string.
func errMessage(err error) string {
	var cerr *contracts.Error
	if errors.As(err, &cerr) {
		return cerr.Message
	}
	return ""
}

// TestInFlightCheckpointWritesAreFenced reproduces the measured Space case at
// the State layer. While a run is in flight and its lease belongs to the
// executor, a writer that is not the holder is refused with STALE_EPOCH and
// nothing is written; after the lease has expired and another worker has taken
// over (the epoch grew), the previous holder is stale too. State must be
// distinguishable on both paths - a caller that fails to convert the code is
// the caller's defect, not the store's. A run this scope does not know stays
// NOT_FOUND.
func TestInFlightCheckpointWritesAreFenced(t *testing.T) {
	e := cpFixture(t)
	zombie := e.fence

	for _, tc := range []struct {
		name  string
		fence Fence
		want  string
	}{
		{"another worker while the run is in flight",
			Fence{RunID: zombie.RunID, Worker: "space-writer", Epoch: zombie.Epoch},
			contracts.ErrStaleEpoch},
		{"a run this scope does not know",
			Fence{RunID: "run-" + contracts.NewID(), Worker: zombie.Worker, Epoch: zombie.Epoch},
			contracts.ErrNotFound},
	} {
		if _, err := e.s.PutCheckpoint(e.ctx, e.scope, tc.fence, cp("cp-fenced"),
			"key-"+contracts.NewID()); contracts.Code(err) != tc.want {
			t.Fatalf("%s: want %s, got %v", tc.name, tc.want, err)
		}
	}

	// Let the lease expire and hand the same run to another worker: the epoch
	// grows, so everything the first holder holds becomes stale.
	if _, err := e.s.Heartbeat(e.ctx, e.scope, zombie.RunID, zombie.Worker, zombie.Epoch,
		time.Millisecond); err != nil {
		t.Fatalf("Heartbeat to a short ttl: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	taken, err := e.s.ClaimRun(e.ctx, e.scope.Tenant, e.scope.Deployment, "w2", time.Minute)
	if err != nil || taken == nil {
		t.Fatalf("ClaimRun after the lease expired: %v", err)
	}
	if taken.Epoch <= zombie.Epoch {
		t.Fatalf("a takeover must grow the fence: epoch %d after %d", taken.Epoch, zombie.Epoch)
	}
	newHolder := Fence{RunID: taken.Run.ID, Worker: taken.Worker, Epoch: taken.Epoch}

	// The zombie holder - right run, right worker, wrong epoch - is refused.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, zombie, cp("cp-fenced"),
		"key-"+contracts.NewID()); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("the displaced holder: want STALE_EPOCH, got %v", err)
	}
	// Both refusals left no trace.
	parts, err := e.s.ListCheckpoints(e.ctx, e.scope, e.threadID, "ns", 10)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(parts) != 0 {
		t.Fatalf("a refused write must not materialize a snapshot, got %d parts", len(parts))
	}
	// The current holder writes normally.
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, newHolder, cp("cp-fenced"),
		"key-"+contracts.NewID()); err != nil {
		t.Fatalf("the current holder must be able to write: %v", err)
	}
}
