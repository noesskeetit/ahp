package state

// Checkpoint history bounds (matrix row 15, PDF section 4 / section 6.8).
//
// The claim under test is narrow and deliberate: a thread's checkpoint history
// is bounded by an operator action, and the bound never becomes a way to lose
// something that is still in use. Every leg reads the database back out of band
// (direct SQL) rather than trusting the report, because a retention pass that
// reports the right numbers and writes the wrong rows is exactly the failure
// mode this feature could have.
//
// Kept-on-purpose legs, in the order a reviewer should read them:
//   - the default is a bound, not a silent unbounded default;
//   - retention is not a side effect of a write (nothing vanishes until an
//     operator asks);
//   - the readable window is what survives, and shared blobs are not eaten;
//   - an in-flight step and an artifact descriptor survive regardless of depth;
//   - the pass is scope- and namespace-bounded, and idempotent;
//   - the durable administrative journal stays reviewable after retention.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

type cpHistoryEnv struct {
	s        *Store
	pool     *pgxpool.Pool
	ctx      context.Context
	scope    contracts.Scope
	fence    Fence
	threadID string
}

func cpHistoryFixture(t *testing.T, options ...StoreOption) *cpHistoryEnv {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	pool, _ := isolatedPool(t)
	s := New(pool, options...)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("MigrateCheckpoints: %v", err)
	}
	scope := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: "d-" + contracts.NewID(),
		Subject: "subj-" + contracts.NewID()}
	threadID := createHistoryThread(t, ctx, s, scope)
	return &cpHistoryEnv{s: s, pool: pool, ctx: ctx, scope: scope, threadID: threadID,
		fence: claimHistoryLease(t, ctx, s, scope)}
}

func createHistoryThread(t *testing.T, ctx context.Context, s *Store, scope contracts.Scope) string {
	t.Helper()
	th, err := s.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if _, err := s.CreateRun(ctx, scope, th.ID, "task-"+contracts.NewID(), "k-"+contracts.NewID(),
		json.RawMessage(`{}`)); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return th.ID
}

func claimHistoryLease(t *testing.T, ctx context.Context, s *Store, scope contracts.Scope) Fence {
	t.Helper()
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "w-"+contracts.NewID(), time.Minute)
	if err != nil || lease == nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	return Fence{RunID: lease.Run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
}

// writeSteps appends count checkpoints to a namespace, each chained to the
// previous one, each with a private blob version on channel "log". The very
// first step of the whole history also carries the shared blob that every later
// step only references by version, which is how a retention pass gets the
// chance to eat something it must not eat.
func (e *cpHistoryEnv) writeSteps(t *testing.T, namespace string, start, count int, parent string) []string {
	t.Helper()
	ids := make([]string, 0, count)
	for i := start; i < start+count; i++ {
		id := fmt.Sprintf("cp-%02d", i)
		checkpoint := Checkpoint{ID: id, Namespace: namespace, Version: int64(i + 1),
			Parts: []Part{{Channel: "log", Version: fmt.Sprintf("log-%02d", i),
				Data: []byte(fmt.Sprintf("state at step %d", i))}}}
		if parent == "" {
			checkpoint.Parts = append(checkpoint.Parts,
				Part{Channel: "shared", Version: "shared-1", Data: []byte("written once, read by many")})
		} else {
			checkpoint.Parts = append(checkpoint.Parts,
				Part{Channel: "shared", Version: "shared-1"})
		}
		if parent != "" {
			checkpoint.Parent = parent
		}
		if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, checkpoint, "k-"+id); err != nil {
			t.Fatalf("PutCheckpoint %s: %v", id, err)
		}
		ids = append(ids, id)
		parent = id
	}
	return ids
}

func (e *cpHistoryEnv) writeChain(t *testing.T, namespace string, count int) []string {
	t.Helper()
	return e.writeSteps(t, namespace, 0, count, "")
}

// rank is the position of a checkpoint in the readable order, 1 being the
// newest. A protection that fires inside the bound proves nothing, so the legs
// below use this to show the protected row really is beyond the bound.
// jsonField reads one top-level string field of a descriptor without pulling a
// second JSON type into the fixture.
func jsonField(payload []byte, key string) string {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return ""
	}
	var value string
	_ = json.Unmarshal(fields[key], &value)
	return value
}

func (e *cpHistoryEnv) rank(t *testing.T, namespace, publicID string) int {
	t.Helper()
	internal, err := internalCheckpointID(e.threadID, namespace, publicID)
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := e.pool.QueryRow(e.ctx, `
SELECT count(*) FROM ahp_state.checkpoints c
 WHERE c.thread_id = $1 AND c.namespace = $2
   AND (c.created_at, c.id) > (SELECT created_at, id FROM ahp_state.checkpoints WHERE id = $3)`,
		e.threadID, namespace, internal).Scan(&n); err != nil {
		t.Fatalf("rank of %s: %v", publicID, err)
	}
	return n + 1
}

func (e *cpHistoryEnv) countCheckpoints(t *testing.T, namespace string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_state.checkpoints
	 WHERE thread_id = $1 AND namespace = $2`, e.threadID, namespace).Scan(&n); err != nil {
		t.Fatalf("count checkpoints: %v", err)
	}
	return n
}

func (e *cpHistoryEnv) countBlobs(t *testing.T, version string) int {
	t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_state.checkpoint_blobs
	 WHERE thread_id = $1 AND version = $2`, e.threadID, version).Scan(&n); err != nil {
		t.Fatalf("count blobs: %v", err)
	}
	return n
}

// TestCheckpointHistoryDefaultIsABound pins the default the product ships:
// keeping history forever was the defect, so a zero/negative default would
// reproduce it by another name. Unbounded stays available, but only as an
// explicit operator choice, and then a prune without an explicit limit must
// refuse rather than guess a depth.
func TestCheckpointHistoryDefaultIsABound(t *testing.T) {
	e := cpHistoryFixture(t)
	if got := e.s.CheckpointHistoryLimit(); got != DefaultCheckpointHistoryLimit {
		t.Fatalf("default history limit = %d, want %d", got, DefaultCheckpointHistoryLimit)
	}
	if DefaultCheckpointHistoryLimit < 1 {
		t.Fatalf("the shipped default must bound history, got %d", DefaultCheckpointHistoryLimit)
	}
	unbounded := New(e.pool, WithCheckpointHistory(UnboundedCheckpointHistory))
	if got := unbounded.CheckpointHistoryLimit(); got >= 1 {
		t.Fatalf("explicit unbounded configuration reported limit %d", got)
	}
	report, err := unbounded.PruneCheckpointHistory(e.ctx, e.scope,
		CheckpointPruneRequest{ThreadID: e.threadID, AllNamespaces: true})
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("unbounded store with no explicit limit: err=%v want INVALID", err)
	}
	if report.Pruned != 0 || e.countCheckpoints(t, "") != 0 {
		t.Fatal("a refused prune must not touch a single row")
	}
}

// TestCheckpointHistoryRetentionIsNotAWriteSideEffect: history beyond the bound
// must still be there right up to the moment an operator runs retention. A
// write that quietly dropped the oldest snapshot would be indistinguishable
// from a store that lost data, and it would make "recovery from any step
// boundary" depend on how many steps happened to have run.
func TestCheckpointHistoryRetentionIsNotAWriteSideEffect(t *testing.T) {
	e := cpHistoryFixture(t, WithCheckpointHistory(4))
	ids := e.writeChain(t, "ns", 12)
	if got := e.countCheckpoints(t, "ns"); got != 12 {
		t.Fatalf("checkpoints after %d writes = %d: retention deleted rows without being asked", len(ids), got)
	}
	if got, err := e.s.GetCheckpoint(e.ctx, e.scope, e.threadID, "ns", ids[0]); err != nil || got.ID != ids[0] {
		t.Fatalf("the oldest checkpoint must survive until retention runs: %v", err)
	}
}

// TestPruneCheckpointHistoryKeepsTheReadableWindow is the core bound: what
// survives is exactly what one ListCheckpoints call would have returned, the
// chain of survivors is walkable, the pruned head of history is honestly gone,
// and a blob that surviving rows still reference is not eaten because some
// older row also pointed at it.
func TestPruneCheckpointHistoryKeepsTheReadableWindow(t *testing.T) {
	const limit = 4
	e := cpHistoryFixture(t, WithCheckpointHistory(limit))
	ids := e.writeChain(t, "ns", 12)

	report, err := e.s.PruneCheckpointHistory(e.ctx, e.scope,
		CheckpointPruneRequest{ThreadID: e.threadID, Namespace: "ns"})
	if err != nil {
		t.Fatalf("PruneCheckpointHistory: %v", err)
	}
	if report.Pruned != 8 || report.Kept != limit || report.Limit != limit {
		t.Fatalf("report = %+v, want 8 pruned / 4 kept", report)
	}
	if report.TruncatedChains != 1 {
		t.Fatalf("one surviving chain head must have been truncated, report = %+v", report)
	}
	if got := e.countCheckpoints(t, "ns"); got != limit {
		t.Fatalf("database holds %d checkpoints, want %d", got, limit)
	}
	listed, err := e.s.ListCheckpoints(e.ctx, e.scope, e.threadID, "ns", 0)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	if len(listed) != limit {
		t.Fatalf("listed %d, want %d", len(listed), limit)
	}
	wantNewest := ids[len(ids)-limit:]
	for i := range wantNewest {
		want := wantNewest[len(wantNewest)-1-i] // ListCheckpoints is newest first.
		if got := listed[i].ID; got != want {
			t.Fatalf("survivor %d = %s, want %s (the readable window must be the newest history)", i, got, want)
		}
	}
	// The pruned head of history is really gone, and says so.
	if _, err := e.s.GetCheckpoint(e.ctx, e.scope, e.threadID, "ns", ids[0]); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("pruned checkpoint read = %v, want NOT_FOUND", err)
	}
	// The new tail has no parent: the chain is truncated, not left dangling.
	tail, err := e.s.GetCheckpoint(e.ctx, e.scope, e.threadID, "ns", wantNewest[0])
	if err != nil {
		t.Fatalf("GetCheckpoint tail: %v", err)
	}
	if tail.Parent != "" {
		t.Fatalf("truncated tail still names a pruned parent %q", tail.Parent)
	}
	// Every listed parent must resolve, otherwise the bound produced history no
	// reader can walk.
	for _, item := range listed {
		if item.Parent == "" {
			continue
		}
		if _, err := e.s.GetCheckpoint(e.ctx, e.scope, e.threadID, "ns", item.Parent); err != nil {
			t.Fatalf("listed checkpoint %s names unresolvable parent %s: %v", item.ID, item.Parent, err)
		}
	}
	// A blob only the pruned rows referenced is reclaimed; the blob surviving
	// rows still reference is not.
	if got := e.countBlobs(t, "log-00"); got != 0 {
		t.Fatalf("pruned private blob still present (%d rows)", got)
	}
	if _, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "log", "log-00"); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("pruned blob read = %v, want NOT_FOUND", err)
	}
	got, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "shared", "shared-1")
	if err != nil || string(got) != "written once, read by many" {
		t.Fatalf("shared blob was damaged by retention: %q, %v", got, err)
	}
	if _, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "log", "log-11"); err != nil {
		t.Fatalf("newest private blob must survive: %v", err)
	}
	// Idempotent: a second pass finds nothing to do and changes nothing.
	second, err := e.s.PruneCheckpointHistory(e.ctx, e.scope,
		CheckpointPruneRequest{ThreadID: e.threadID, Namespace: "ns"})
	if err != nil {
		t.Fatalf("second prune: %v", err)
	}
	if second.Pruned != 0 || second.PrunedBlobs != 0 || second.TruncatedChains != 0 || second.Kept != limit {
		t.Fatalf("second prune not idempotent: %+v", second)
	}
}

// recordingGuard is the Artifacts owner as State sees it: it records every pin
// and every release so a test can prove retention never releases a reference it
// does not own.
type recordingGuard struct {
	pinned   []references.CheckpointArtifact
	released []string
}

func (g *recordingGuard) PinCheckpointReferences(_ context.Context, _ contracts.Scope, _ string, refs []references.CheckpointArtifact) error {
	g.pinned = append(g.pinned, refs...)
	return nil
}

func (g *recordingGuard) ReleaseCheckpointReferences(_ context.Context, operation string) error {
	g.released = append(g.released, operation)
	return nil
}

// TestPruneCheckpointHistoryProtectsInFlightAndReferenceBearingRows: depth is
// not the only reason a snapshot matters. A step that is still being assembled
// around an old checkpoint, and a checkpoint that carries an artifact
// descriptor, both survive a bound that would otherwise remove them - State
// must not weaken the Artifacts owner's liveness authority as a side effect of
// its own retention, and must not delete work in progress.
func TestPruneCheckpointHistoryProtectsInFlightAndReferenceBearingRows(t *testing.T) {
	const limit = 2
	guard := &recordingGuard{}
	e := cpHistoryFixture(t, WithCheckpointHistory(limit))
	e.s.SetCheckpointArtifacts(guard)
	descriptor, err := json.Marshal(references.CheckpointArtifact{
		ID: "art-" + contracts.NewID(), Tenant: e.scope.Tenant, Deployment: e.scope.Deployment,
		Subject: e.scope.Subject, ContextID: "ctx", TaskID: "task", Version: "1",
		SHA256: "sha256:" + contracts.NewID(), Size: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The two protected rows are placed in the MIDDLE of the history, so they
	// are neither the readable window nor the head: the only thing that can
	// save them is the rule under test.
	early := e.writeSteps(t, "ns", 0, 2, "")
	inFlight := early[0]
	descriptorID := "cp-artifact"
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, Checkpoint{
		ID: descriptorID, Namespace: "ns", Version: 99, Parent: early[1],
		Parts: []Part{{Channel: "artifact", Version: "artifact-1",
			Encoding: CheckpointArtifactEncoding, Data: descriptor}},
	}, "k-"+descriptorID); err != nil {
		t.Fatalf("descriptor checkpoint: %v", err)
	}
	e.writeSteps(t, "ns", 2, 3, descriptorID)
	// An in-flight step against the OLDEST checkpoint: pending writes exist, so
	// the step is still being assembled around it.
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", inFlight,
		[]PendingWrite{{Task: "task-1", Index: 0, Channel: "log", Data: []byte("partial")}}); err != nil {
		t.Fatalf("PutWrites: %v", err)
	}
	if got := e.rank(t, "ns", inFlight); got <= limit {
		t.Fatalf("test setup is vacuous: the in-flight row sits at rank %d, inside the bound of %d", got, limit)
	}
	if got := e.rank(t, "ns", descriptorID); got <= limit {
		t.Fatalf("test setup is vacuous: the descriptor row sits at rank %d, inside the bound of %d", got, limit)
	}
	if len(guard.pinned) != 1 {
		t.Fatalf("the descriptor must have been pinned with the artifact owner, pins=%d", len(guard.pinned))
	}
	// The writer's operation has settled, so the Artifacts owner has already
	// dropped its transient operation pin: from here on the descriptor row
	// inside this checkpoint is the ONLY thing that keeps the artifact alive.
	// That is what makes the refusal below a live-reference guarantee rather
	// than a pin that would have covered for it anyway.
	if len(guard.released) != 1 {
		t.Fatalf("expected the transient operation pin to have settled, releases=%v", guard.released)
	}
	source := ReferenceSource(e.pool, "ahp_state")
	before, err := source.InspectReferences(e.ctx, e.scope.Tenant, nil,
		[]references.Artifact{{ID: string(jsonField(descriptor, "id")), Digest: string(jsonField(descriptor, "sha256"))}})
	if err != nil {
		t.Fatalf("InspectReferences before retention: %v", err)
	}
	if !before.Referenced[string(jsonField(descriptor, "id"))] {
		t.Fatal("the descriptor is not a live reference even before retention - this leg proves nothing")
	}
	releasedBefore := len(guard.released)

	report, err := e.s.PruneCheckpointHistory(e.ctx, e.scope,
		CheckpointPruneRequest{ThreadID: e.threadID, Namespace: "ns"})
	if err != nil {
		t.Fatalf("PruneCheckpointHistory: %v", err)
	}
	if report.RefusedReferenceBearing != 1 {
		t.Fatalf("descriptor-bearing rows refused = %d, want 1 (report %+v)", report.RefusedReferenceBearing, report)
	}
	if len(guard.released) != releasedBefore {
		t.Fatalf("retention released artifact references it does not own: %v", guard.released)
	}
	kept, err := e.s.ListCheckpoints(e.ctx, e.scope, e.threadID, "ns", 0)
	if err != nil {
		t.Fatalf("ListCheckpoints: %v", err)
	}
	present := map[string]bool{}
	for _, item := range kept {
		present[item.ID] = true
	}
	if !present[inFlight] {
		t.Fatalf("an in-flight checkpoint was truncated away: kept %v", present)
	}
	if !present[descriptorID] {
		t.Fatalf("a reference-bearing checkpoint was truncated away: kept %v", present)
	}
	if report.Kept != len(kept) {
		t.Fatalf("the report says %d kept, the database holds %d", report.Kept, len(kept))
	}
	// Exactly: the readable window (the two newest steps) plus the in-flight
	// step plus the refused descriptor row. Any other row surviving means the
	// bound is not a bound; any of these missing means a protection leaked.
	want := map[string]bool{"cp-03": true, "cp-04": true, inFlight: true, descriptorID: true}
	if len(present) != len(want) {
		t.Fatalf("survivors = %v, want exactly %v", present, want)
	}
	for id := range want {
		if !present[id] {
			t.Fatalf("survivor %s missing: %v", id, present)
		}
	}
	if report.Pruned != 2 {
		t.Fatalf("pruned %d rows, want the two unprotected middle steps: %+v", report.Pruned, report)
	}
	// The descriptor is still readable with its codec, so the artifact owner can
	// still resolve the reference it was promised.
	payload, err := e.s.GetPartEncoded(e.ctx, e.scope, e.threadID, "ns", "artifact", "artifact-1")
	if err != nil {
		t.Fatalf("descriptor must remain readable: %v", err)
	}
	if payload.Encoding != CheckpointArtifactEncoding {
		t.Fatalf("descriptor codec lost: %q", payload.Encoding)
	}
	var round references.CheckpointArtifact
	if err := json.Unmarshal(payload.Data, &round); err != nil || round.ID == "" {
		t.Fatalf("descriptor bytes damaged: %v", err)
	}
	// The pending write of the protected row is still there too.
	if _, err := e.s.GetPart(e.ctx, e.scope, e.threadID, "ns", "log", "log-00"); err != nil {
		t.Fatalf("the in-flight checkpoint's blob must survive: %v", err)
	}
	// The reference source still sees the descriptor: this is the query the
	// Artifacts GC asks, so refusing to prune here is what keeps a live
	// reference live after retention.
	result, err := source.InspectReferences(e.ctx, e.scope.Tenant, nil,
		[]references.Artifact{{ID: round.ID, Digest: round.SHA256}})
	if err != nil {
		t.Fatalf("InspectReferences: %v", err)
	}
	if !result.Referenced[round.ID] {
		t.Fatalf("retention removed a live reference the Artifacts owner could still see")
	}
}

// TestPruneCheckpointHistoryIsScopeAndNamespaceBounded: retention is an
// operation on one caller's thread. A foreign subject must not be able to
// truncate someone else's history - not even by getting a NOT_FOUND that
// silently did the work anyway - and bounding one namespace must not reach
// another.
func TestPruneCheckpointHistoryIsScopeAndNamespaceBounded(t *testing.T) {
	e := cpHistoryFixture(t, WithCheckpointHistory(2))
	e.writeChain(t, "ns-a", 6)
	e.writeChain(t, "ns-b", 6)
	foreign := contracts.Scope{Tenant: e.scope.Tenant, Deployment: e.scope.Deployment,
		Subject: "subj-" + contracts.NewID()}
	if _, err := e.s.PruneCheckpointHistory(e.ctx, foreign,
		CheckpointPruneRequest{ThreadID: e.threadID, AllNamespaces: true}); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("foreign subject prune = %v, want NOT_FOUND", err)
	}
	if got := e.countCheckpoints(t, "ns-a") + e.countCheckpoints(t, "ns-b"); got != 12 {
		t.Fatalf("a refused prune changed history: %d rows remain", got)
	}
	otherThread := createHistoryThread(t, e.ctx, e.s, e.scope)
	otherFence := claimHistoryLease(t, e.ctx, e.s, e.scope)
	other := &cpHistoryEnv{s: e.s, pool: e.pool, ctx: e.ctx, scope: e.scope, threadID: otherThread, fence: otherFence}
	other.writeChain(t, "ns-a", 6)

	if _, err := e.s.PruneCheckpointHistory(e.ctx, e.scope,
		CheckpointPruneRequest{ThreadID: e.threadID, Namespace: "ns-a"}); err != nil {
		t.Fatalf("prune ns-a: %v", err)
	}
	if got := e.countCheckpoints(t, "ns-a"); got != 2 {
		t.Fatalf("ns-a holds %d rows, want the bound of 2", got)
	}
	if got := e.countCheckpoints(t, "ns-b"); got != 6 {
		t.Fatalf("ns-b was pruned as a side effect: %d rows, want 6", got)
	}
	var otherRows int
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id = $1`,
		otherThread).Scan(&otherRows); err != nil {
		t.Fatalf("count other thread: %v", err)
	}
	if otherRows != 6 {
		t.Fatalf("pruning one thread reached another thread's history: %d rows", otherRows)
	}
	if _, err := e.s.PruneCheckpointHistory(e.ctx, e.scope,
		CheckpointPruneRequest{ThreadID: e.threadID, AllNamespaces: true}); err != nil {
		t.Fatalf("prune all namespaces: %v", err)
	}
	for _, namespace := range []string{"ns-a", "ns-b"} {
		if got := e.countCheckpoints(t, namespace); got != 2 {
			t.Fatalf("%s holds %d rows after an all-namespace prune, want 2", namespace, got)
		}
	}
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id = $1`,
		otherThread).Scan(&otherRows); err != nil {
		t.Fatalf("count other thread: %v", err)
	}
	if otherRows != 6 {
		t.Fatalf("all-namespace prune escaped the target thread: %d rows", otherRows)
	}
}

// TestPruneCheckpointHistoryLeavesTheAdminJournalReviewable: retention touches
// checkpoint history, nothing else. The durable administrative journal - the
// legal hold an operator set and the reason recorded with it - must still be
// readable after history has been bounded, and the administrative path must
// still work on a thread whose history was truncated.
func TestPruneCheckpointHistoryLeavesTheAdminJournalReviewable(t *testing.T) {
	e := cpHistoryFixture(t, WithCheckpointHistory(2))
	e.writeChain(t, "ns", 8)
	if err := e.s.SetLegalhold(e.ctx, e.scope, e.threadID, "litigation hold 2026-11"); err != nil {
		t.Fatalf("SetLegalhold: %v", err)
	}
	if _, err := e.s.PruneCheckpointHistory(e.ctx, e.scope,
		CheckpointPruneRequest{ThreadID: e.threadID, Namespace: "ns"}); err != nil {
		t.Fatalf("PruneCheckpointHistory: %v", err)
	}
	var reason string
	if err := e.pool.QueryRow(e.ctx,
		`SELECT reason FROM ahp_state.legalhold WHERE tenant = $1 AND thread_id = $2`,
		e.scope.Tenant, e.threadID).Scan(&reason); err != nil {
		t.Fatalf("the durable administrative journal must survive retention: %v", err)
	}
	if reason != "litigation hold 2026-11" {
		t.Fatalf("legal hold reason changed: %q", reason)
	}
	// The administrative path still works on a truncated thread, so an auditor
	// can close the hold afterwards; a hold that cannot be lifted would be a
	// new kind of data loss.
	if err := e.s.RemoveLegalhold(e.ctx, e.scope, e.threadID); err != nil {
		t.Fatalf("RemoveLegalhold after retention: %v", err)
	}
	var held bool
	if err := e.pool.QueryRow(e.ctx,
		`SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant = $1 AND thread_id = $2)`,
		e.scope.Tenant, e.threadID).Scan(&held); err != nil {
		t.Fatalf("probe legalhold: %v", err)
	}
	if held {
		t.Fatal("the administrative path stopped working after retention")
	}
	// And the thread itself is still a live thread: retention truncated
	// history, it did not delete the subject's data.
	if _, err := e.s.GetThread(e.ctx, e.scope, e.threadID); err != nil {
		t.Fatalf("thread must outlive retention: %v", err)
	}
}
