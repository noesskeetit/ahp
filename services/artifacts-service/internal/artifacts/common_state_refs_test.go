package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	st "github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"strings"
	"testing"
)

func rev34CommonEnv(t *testing.T) (*checkpointTestEnv, st.CommonCommit, string) {
	e := newCheckpointTestEnv(t)
	st.WithCommonPayloadObjects()(e.state)
	st.WithCommonArtifactReferences()(e.state)
	if err := e.state.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	source := st.CommonReferenceSource(e.readPool, "ahp_state")
	scan := ReferenceScanFunc(func(ctx context.Context, tenant string, arts []ArtifactQuery) (map[string]bool, error) {
		q := make([]references.Artifact, len(arts))
		for i, a := range arts {
			q[i] = references.Artifact{ID: a.ID, Digest: a.Digest}
		}
		r, err := source.InspectReferences(ctx, tenant, nil, q)
		return r.Referenced, err
	})
	e.store.keep = scan
	var err error
	e.gc, err = NewGC(e.store, WithReferenceScan(scan))
	if err != nil {
		t.Fatal(err)
	}
	// Exact external native-like JSON bytes, including duplicate payload keys
	// and an escaped NUL. They are never converted to PostgreSQL JSONB.
	body := `{"duplicate":1,"duplicate":2,"native":"alpha\u0000omega","large":"` + strings.Repeat("abcdefgh", 140000) + `"}`
	e.ref, e.upload = e.sealBoundWithKey(t, Binding{ContextID: e.thread.ContextID, TaskID: "task"}, "common-large", body)
	var ref references.CheckpointArtifact
	raw, err := json.Marshal(e.ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(raw, &ref); err != nil {
		t.Fatal(err)
	}
	// ArtifactRef contains public fields not in the immutable internal projection.
	ref = references.CheckpointArtifact{ID: e.ref.ID, Tenant: e.ref.Tenant, Deployment: e.ref.Deployment, Subject: e.ref.Subject, ContextID: e.ref.ContextID, TaskID: e.ref.TaskID, Version: e.ref.Version, SHA256: e.ref.SHA256, Size: e.ref.Size, Labels: e.ref.Labels}
	marker, err := json.Marshal(map[string]any{"encoding": st.CheckpointArtifactEncoding, "ref": ref})
	if err != nil {
		t.Fatal(err)
	}
	req := st.CommonCommit{Namespace: "common/research-2", Key: "large-external", Document: st.CommonDocument{Format: st.CommonFormat, Profile: "fixture-native/research-1" + st.CommonArtifactProfileSuffix, Root: "session", Records: map[string]st.CommonRecord{"session": {Kind: "session", Attributes: marker, Relations: map[string][]string{}}}}, ArtifactReferences: []st.CommonArtifactReference{{RecordID: "session", Artifact: ref}}}
	return e, req, body
}
func TestRev34CommonRealArtifactHistoryProtectsBytesUntilThreadDeletion(t *testing.T) {
	e, req, body := rev34CommonEnv(t)
	ctx := context.Background()
	first, err := e.state.CommitRecords(ctx, e.scope, e.fence, req)
	if err != nil {
		t.Fatal(err)
	}
	r, err := e.state.ReadRecords(ctx, e.scope, e.thread.ID, req.Namespace, 1)
	if err != nil || len(r.ArtifactReferences) != 1 || len(r.Document.Records["session"].Attributes) > 4096 {
		t.Fatal("large native data stayed inline or ref lost", err)
	}
	actual, err := e.rawGet(t, e.upload.Object, e.ref.Version)
	if err != nil || !bytes.Equal(actual, []byte(body)) {
		t.Fatal("external bytes changed", err)
	}
	next := req
	next.Key = "later-inline"
	next.ExpectedRevision = 1
	next.ArtifactReferences = nil
	next.Document.Records = map[string]st.CommonRecord{"session": {Kind: "session", Attributes: json.RawMessage(`{"later":true}`), Relations: map[string][]string{}}}
	if _, err = e.state.CommitRecords(ctx, e.scope, e.fence, next); err != nil {
		t.Fatal(err)
	}
	if err = e.store.Delete(ctx, e.scope, e.ref.ID, "cannot-delete-historical"); contracts.Code(err) != contracts.ErrConflict {
		t.Fatal("historical common reference did not block tombstone", err)
	}
	replay, err := e.state.CommitRecords(ctx, e.scope, e.fence, req)
	if err != nil || !reflect.DeepEqual(replay, first) || e.pinCount(t) != 0 {
		t.Fatal("commit replay changed receipt/pin", err)
	}
	// Simulate a legacy tombstone to exercise GC's independent final recheck.
	e.tombstone(t)
	e.waitTombstoneAge(t, e.ref.ID)
	rep, err := e.gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil || rep.PurgedTombstoned != 0 {
		t.Fatal("GC removed a referenced historical payload", err)
	}
	if actual, err = e.rawGet(t, e.upload.Object, e.ref.Version); err != nil || !bytes.Equal(actual, []byte(body)) {
		t.Fatal("GC changed live native bytes", err)
	}
	if err = e.state.DeleteThread(ctx, e.scope, e.thread.ID); err != nil {
		t.Fatal(err)
	}
	rep, err = e.gc.RunOnce(ctx, e.scope, fastPolicy())
	if err != nil || rep.PurgedTombstoned != 1 {
		t.Fatal("GC did not collect after last root removed", rep, err)
	}
	if _, err = e.rawGet(t, e.upload.Object, e.ref.Version); err == nil {
		t.Fatal("collected exact version still exists")
	}
}
func TestRev34CommonRealArtifactLostReleasePinReconcilesToCommittedRoot(t *testing.T) {
	e, req, body := rev34CommonEnv(t)
	ctx := context.Background()
	e.state.SetCheckpointArtifacts(checkpointFaultGuard{Store: e.store, releaseFailure: true})
	if _, err := e.state.CommitRecords(ctx, e.scope, e.fence, req); err != nil {
		t.Fatal(err)
	}
	if e.pinCount(t) != 1 {
		t.Fatal("lost release did not retain durable pin")
	}
	e.tombstone(t)
	e.waitTombstoneAge(t, e.ref.ID)
	if _, err := e.gc.RunOnce(ctx, e.scope, fastPolicy()); err != nil {
		t.Fatal(err)
	}
	if e.pinCount(t) != 0 {
		t.Fatal("settled publication pin not reconciled")
	}
	if actual, err := e.rawGet(t, e.upload.Object, e.ref.Version); err != nil || string(actual) != body {
		t.Fatal("committed common root did not take over protection", err)
	}
}
func TestRev34CommonRealArtifactAbortAfterPinLeavesNoPartialCut(t *testing.T) {
	e, req, _ := rev34CommonEnv(t)
	ctx := context.Background()
	e.state.SetCheckpointArtifacts(checkpointFaultGuard{Store: e.store, releaseFailure: true, afterPin: func(context.Context, string) error {
		return contracts.NewError(contracts.ErrUnavailable, "fixture failed after durable Artifact pin")
	}})
	if _, err := e.state.CommitRecords(ctx, e.scope, e.fence, req); contracts.Code(err) != contracts.ErrUnavailable {
		t.Fatal("injected failure ignored", err)
	}
	if _, err := e.state.ReadRecords(ctx, e.scope, e.thread.ID, req.Namespace, 0); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatal("partial common head committed", err)
	}
	if e.pinCount(t) != 1 {
		t.Fatal("lost release did not preserve pin")
	}
	if err := e.store.ReconcileCheckpointPins(ctx, e.scope.Tenant); err != nil || e.pinCount(t) != 0 {
		t.Fatal("aborted settled pin did not reconcile", err)
	}
	if err := e.store.Delete(ctx, e.scope, e.ref.ID, "unrooted-after-abort"); err != nil {
		t.Fatal("aborted root leaked protection", err)
	}
}

func TestRev34CommonReferenceOwnerOutageKeepsActualObjectBytes(t *testing.T) {
	e, req, body := rev34CommonEnv(t)
	ctx := context.Background()
	if _, err := e.state.CommitRecords(ctx, e.scope, e.fence, req); err != nil {
		t.Fatal(err)
	}
	e.tombstone(t)
	e.waitTombstoneAge(t, e.ref.ID)
	if _, err := e.readPool.Exec(ctx, `ALTER TABLE ahp_state.common_artifact_refs RENAME TO common_artifact_refs_rev34_outage`); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if _, err := e.readPool.Exec(ctx, `ALTER TABLE ahp_state.common_artifact_refs_rev34_outage RENAME TO common_artifact_refs`); err != nil {
			t.Error("own reference table restore failed")
		}
	}()
	rep, _ := e.gc.RunOnce(ctx, e.scope, fastPolicy())
	if rep.PurgedTombstoned != 0 {
		t.Fatal("unavailable reference authority treated as no references")
	}
	actual, err := e.rawGet(t, e.upload.Object, e.ref.Version)
	if err != nil || string(actual) != body {
		t.Fatal("authority outage lost native bytes", err)
	}
}
