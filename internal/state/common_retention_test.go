package state

import (
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"reflect"
	"testing"
	"time"
)

func retentionFixture(t *testing.T) (*rev7Owners, CommonCommit) {
	t.Helper()
	x := rev40Fixture(t)
	option, e := WithCommonRetention([]string{"test/1"})
	if e != nil {
		t.Fatal(e)
	}
	option(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	first := rev40DeltaRequest(t, "first", 0, rev40Initial)
	second := rev40DeltaRequest(t, "second", 1, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"b":{"kind":"event","attributes":{"text":"beta"},"relations":{}}},"relations":[{"source":"s","role":"events","mode":"append","expected_length":1,"targets":["b"]}]}`)
	third := rev40DeltaRequest(t, "third", 2, `{"format":"ahp-common-semantic-records/research-2","profile":"test/1","root":"s","put":{"s":{"kind":"session","attributes":{"v":3},"relations":{}}},"delete":["a"],"relations":[{"source":"s","role":"events","mode":"replace","targets":["b"]}]}`)
	for _, r := range []CommonCommit{first, second, third} {
		if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, r); e != nil {
			t.Fatal(e)
		}
	}
	return x, first
}
func retentionRelease(t *testing.T, x *rev7Owners, f Fence) {
	t.Helper()
	if e := x.s.Release(x.ctx, x.a, f.RunID, f.Worker, f.Epoch, RunDone); e != nil {
		t.Fatal(e)
	}
}
func TestCommonRetentionPreservesPinnedCutAndOperationReceipt(t *testing.T) {
	x, first := retentionFixture(t)
	rev := int64(2)
	root, e := x.s.PutCommonRoot(x.ctx, x.a, x.fa, CommonRootRequest{Namespace: first.Namespace, Key: "branch", OperationKey: "pin-branch", Revision: &rev})
	if e != nil || root.Version != 1 {
		t.Fatal("ROOT_PIN_NOT_IMPLEMENTED", e)
	}
	saved, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, 2)
	if e != nil {
		t.Fatal(e)
	}
	retentionRelease(t, x, x.fa)
	request := CommonPruneRequest{ThreadID: x.ta, Namespace: first.Namespace, Key: "prune-one", ExpectedRevision: 3, KeepFromRevision: 3, Limit: 10}
	report, e := x.s.PruneCommonHistory(x.ctx, x.a, request)
	if e != nil || report.ArchivedCuts != 1 || report.ReadableFrom != 2 {
		t.Fatal("protected history pruning", e, report)
	}
	actual, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, 2)
	if e != nil || !reflect.DeepEqual(saved, actual) {
		t.Fatal("pinned native snapshot changed", e)
	}
	if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, 1); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("pruned snapshot returned partial state", e)
	}
	replay, e := x.s.CommitRecords(x.ctx, x.a, x.fa, first)
	if e != nil || replay.Revision != 1 {
		t.Fatal("pruning destroyed a durable operation receipt", e)
	}
	again, e := x.s.PruneCommonHistory(x.ctx, x.a, request)
	if e != nil || !reflect.DeepEqual(report, again) {
		t.Fatal("prune operation replay changed", e)
	}
	request.KeepFromRevision = 2
	if _, e := x.s.PruneCommonHistory(x.ctx, x.a, request); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatal("prune key accepted a changed request", e)
	}
	run, e := x.s.CreateRun(x.ctx, x.a, x.ta, "unpin-run", "unpin-task", json.RawMessage(`{}`))
	if e != nil {
		t.Fatal(e)
	}
	lease, e := x.s.ClaimRunFor(x.ctx, x.a.Tenant, x.a.Deployment, "new-owner", run.ID, time.Minute)
	if e != nil || lease == nil {
		t.Fatal(e)
	}
	f := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	if _, e := x.s.PutCommonRoot(x.ctx, x.a, f, CommonRootRequest{Namespace: first.Namespace, Key: "branch", OperationKey: "release-branch", ExpectedVersion: 1}); e != nil {
		t.Fatal(e)
	}
	retentionRelease(t, x, f)
	request.Key = "prune-two"
	request.KeepFromRevision = 3
	report, e = x.s.PruneCommonHistory(x.ctx, x.a, request)
	if e != nil || report.ReadableFrom != 3 || report.ArchivedCuts != 1 || report.ReclaimedPayloads < 1 {
		t.Fatal("unreachable native payload not reclaimed", e, report)
	}
	head, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, 0)
	if e != nil || head.Receipt.Revision != 3 || !reflect.DeepEqual(head.Document.Records["s"].Relations["events"], []string{"b"}) {
		t.Fatal("current recovery state lost", e)
	}
	var alpha int
	e = x.s.pool.QueryRow(x.ctx, `SELECT count(*) FROM ahp_state.common_payloads WHERE thread_id=$1 AND attributes=$2`, x.ta, []byte(`{"text":"alpha"}`)).Scan(&alpha)
	if e != nil || alpha != 0 {
		t.Fatal("dead native bytes retained", e, alpha)
	}
	if _, e := x.s.PutCommonRoot(x.ctx, x.a, x.fa, CommonRootRequest{Namespace: first.Namespace, Key: "late", OperationKey: "late-root", Revision: &rev}); e == nil {
		t.Fatal("old writer pinned already pruned state")
	}
}
func TestCommonRetentionRefusesActiveLeaseHoldAndStaleHead(t *testing.T) {
	x, first := retentionFixture(t)
	req := CommonPruneRequest{ThreadID: x.ta, Namespace: first.Namespace, Key: "prune", ExpectedRevision: 3, KeepFromRevision: 3}
	if _, e := x.s.PruneCommonHistory(x.ctx, x.a, req); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("active writer not protected", e)
	}
	retentionRelease(t, x, x.fa)
	req.ExpectedRevision = 2
	if _, e := x.s.PruneCommonHistory(x.ctx, x.a, req); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("stale head admitted", e)
	}
	req.ExpectedRevision = 3
	if e := x.s.SetLegalhold(x.ctx, x.a, x.ta, "fixture hold"); e != nil {
		t.Fatal(e)
	}
	if _, e := x.s.PruneCommonHistory(x.ctx, x.a, req); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("legal hold ignored", e)
	}
	if _, e := x.s.PruneCommonHistory(x.ctx, x.b, req); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("foreign thread disclosed or pruned", e)
	}
	for _, rev := range []int64{1, 2, 3} {
		if _, e := x.s.ReadRecords(x.ctx, x.a, x.ta, first.Namespace, rev); e != nil {
			t.Fatal("refused prune removed history", e)
		}
	}
}

func TestCommonRetentionReleasesNonRootArtifactReferenceAccurately(t *testing.T) {
	f, req, _, _ := rev36Fixture(t)
	// The migration fixture contains an intentionally UNKNOWN intent. This
	// metadata-only case explicitly reconciles its synthetic receipt first;
	// the pruner must never bypass that guard on its own.
	if _, e := f.source.ReconcileIntent(f.ctx, f.actors[0], f.fences[0].RunID, f.fences[0].Worker, f.fences[0].Epoch, "effect", "call", IntentSucceeded, f.intents[0].Digest, json.RawMessage(`{"fixture_receipt":42}`)); e != nil {
		t.Fatal(e)
	}
	option, e := WithCommonRetention([]string{"test/1", "native/research-1", "native/research-1" + CommonArtifactProfileSuffix})
	if e != nil {
		t.Fatal(e)
	}
	option(f.source)
	if e := f.source.Migrate(f.ctx); e != nil {
		t.Fatal(e)
	}
	ref := req.ArtifactReferences[0].Artifact
	req.Key = "artifact-child"
	req.ExpectedRevision = 3
	req.Document = CommonDocument{Format: CommonFormat, Profile: "native/research-1" + CommonArtifactProfileSuffix, Root: "s", Records: map[string]CommonRecord{"s": {Kind: "session", Attributes: json.RawMessage(`{}`), Relations: map[string][]string{"payload": {"external"}}}, "external": {Kind: "value", Attributes: commonArtifactMarker(ref), Relations: map[string][]string{}}}}
	req.ArtifactReferences[0].RecordID = "external"
	if _, e := f.source.CommitRecords(f.ctx, f.actors[0], f.fences[0], req); e != nil {
		t.Fatal(e)
	}
	req.Key = "without-artifact"
	req.ExpectedRevision = 4
	req.Document.Records = map[string]CommonRecord{"s": {Kind: "session", Attributes: json.RawMessage(`{"finished":true}`), Relations: map[string][]string{}}}
	req.ArtifactReferences = nil
	if _, e := f.source.CommitRecords(f.ctx, f.actors[0], f.fences[0], req); e != nil {
		t.Fatal(e)
	}
	if e := f.source.Release(f.ctx, f.actors[0], f.fences[0].RunID, f.fences[0].Worker, f.fences[0].Epoch, RunDone); e != nil {
		t.Fatal(e)
	}
	report, e := f.source.PruneCommonHistory(f.ctx, f.actors[0], CommonPruneRequest{ThreadID: f.threads[0], Namespace: "native", Key: "release-old-artifacts", ExpectedRevision: 5, KeepFromRevision: 5})
	if e != nil {
		t.Fatal(e)
	}
	if report.ReleasedArtifactRoots != 2 {
		t.Fatalf("non-root references disappeared before being accounted: got=%d want=2", report.ReleasedArtifactRoots)
	}
	result, e := CommonReferenceSource(f.source.pool, "ahp_state").InspectReferences(f.ctx, f.unit.Tenant, nil, []references.Artifact{{ID: ref.ID, Digest: ref.SHA256}})
	if e != nil || result.Referenced[ref.ID] {
		t.Fatal("pruned-only native Artifact root remains live", e)
	}
}

func TestCommonRetentionBoundedSweepKeepsOtherNamespacePayloadAndOneConnection(t *testing.T) {
	x, first := retentionFixture(t)
	// Another namespace retains the same alpha bytes. Pruning one namespace
	// must not reclaim payloads used anywhere else in this thread.
	other := rev40DeltaRequest(t, "other", 0, rev40Initial)
	other.Namespace = "other"
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, other); e != nil {
		t.Fatal(e)
	}
	retentionRelease(t, x, x.fa)
	cfg := x.s.pool.Config()
	cfg.MaxConns = 1
	pool, e := pgxpool.NewWithConfig(x.ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	option, e := WithCommonRetention([]string{"test/1"})
	if e != nil {
		t.Fatal(e)
	}
	solo := New(pool, WithCommonRecordVersions(), option)
	req := CommonPruneRequest{ThreadID: x.ta, Namespace: first.Namespace, Key: "bounded-one", ExpectedRevision: 3, KeepFromRevision: 3, Limit: 1}
	report, e := solo.PruneCommonHistory(x.ctx, x.a, req)
	if e != nil || !report.More || report.ReadableFrom != 2 || report.ArchivedCuts != 1 {
		t.Fatal("bounded one-connection prune", report, e)
	}
	req.Key = "bounded-two"
	report, e = solo.PruneCommonHistory(x.ctx, x.a, req)
	if e != nil || report.More || report.ReadableFrom != 3 || report.ArchivedCuts != 1 {
		t.Fatal("bounded sweep continuation", report, e)
	}
	cut, e := solo.ReadRecords(x.ctx, x.a, x.ta, "other", 1)
	if e != nil || string(cut.Document.Records["a"].Attributes) != `{"text":"alpha"}` {
		t.Fatal("other namespace payload reclaimed", e)
	}
	first.Key = "first"
	first.Delta = nil
	first.Document = CommonDocument{Format: CommonFormat, Profile: "test/1", Root: "s", Records: map[string]CommonRecord{"s": {Kind: "session", Attributes: json.RawMessage(`{"changed":true}`), Relations: map[string][]string{}}}}
	if _, e := solo.CommitRecords(x.ctx, x.a, x.fa, first); contracts.Code(e) != contracts.ErrKeyReuse {
		t.Fatal("archived key accepted changed payload", e)
	}
}
