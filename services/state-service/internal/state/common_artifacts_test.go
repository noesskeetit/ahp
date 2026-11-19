package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"strings"
	"testing"
)

type rev34Guard struct {
	t              *testing.T
	s              *Store
	pins, releases int
	deny           bool
}

func TestRev34OldDeploymentArchiveCannotSilentlyDropCommonArtifactRoots(t *testing.T) {
	x, _, req := rev34Fixture(t)
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal(e)
	}
	WithDeploymentTransfer(DeploymentTransferConfig{NativeBuild: "common-owner/research-3", Authorize: func(context.Context, DeploymentCustody) error { return nil }})(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	_, e := x.s.ExportDeploymentV3(x.ctx, DeploymentUnit{Tenant: x.a.Tenant, Deployment: x.a.Deployment}, "must-not-drop-object-roots", "common-owner/research-3")
	if e == nil || contracts.Code(e) != contracts.ErrInvalid || !strings.Contains(e.Error(), "State owner schema changed; deployment format upgrade required") {
		t.Fatal("old inline transfer did not explicitly reject the new root schema", e)
	}
}

func (g *rev34Guard) PinCheckpointReferences(ctx context.Context, sc contracts.Scope, op string, refs []references.CheckpointArtifact) error {
	settled, e := g.s.CheckpointOperationSettled(ctx, op)
	if e != nil || settled {
		g.t.Fatal("common publication operation was not locked before pin", e)
	}
	if len(refs) != 1 {
		g.t.Fatal("expected explicit ref")
	}
	g.pins++
	if g.deny {
		return contracts.NewError(contracts.ErrForbidden, "fixture owner refused descriptor")
	}
	return nil
}
func (g *rev34Guard) ReleaseCheckpointReferences(context.Context, string) error {
	g.releases++
	return nil
}
func rev34Fixture(t *testing.T) (*rev7Owners, *rev34Guard, CommonCommit) {
	x := rev29Fixture(t)
	WithCommonArtifactReferences()(x.s)
	if e := x.s.Migrate(x.ctx); e != nil {
		t.Fatal(e)
	}
	g := &rev34Guard{t: t, s: x.s}
	x.s.SetCheckpointArtifacts(g)
	ref := references.CheckpointArtifact{ID: "artifact-" + contracts.NewID(), Tenant: x.a.Tenant, Deployment: x.a.Deployment, Subject: x.a.Subject, ContextID: "alice-a", TaskID: "alice-a", Version: "version-1", SHA256: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", Size: 1048577}
	req := rev27Request(t, "external-cut", 0, nil)
	req.Document.Profile = "test/1" + CommonArtifactProfileSuffix
	row := req.Document.Records["s"]
	row.Attributes = commonArtifactMarker(ref)
	req.Document.Records["s"] = row
	req.ArtifactReferences = []CommonArtifactReference{{RecordID: "s", Artifact: ref}}
	return x, g, req
}
func TestRev34CommonExplicitRefsAtomicHistoryReplayAndKeeper(t *testing.T) {
	x, g, req := rev34Fixture(t)
	first, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil {
		t.Fatal(e)
	}
	if g.pins != 1 || g.releases != 1 {
		t.Fatal("pin lifecycle", g.pins, g.releases)
	}
	r, e := x.s.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 1)
	if e != nil || len(r.ArtifactReferences) != 1 || !r.ArtifactReferences[0].Artifact.Equal(req.ArtifactReferences[0].Artifact) {
		t.Fatal("explicit ref did not restore", e)
	}
	next := rev27Request(t, "inline-after-external", 1, nil)
	next.Document.Profile = req.Document.Profile
	if _, e = x.s.CommitRecords(x.ctx, x.a, x.fa, next); e != nil {
		t.Fatal(e)
	}
	again, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req)
	if e != nil || !reflect.DeepEqual(first, again) || g.pins != 1 {
		t.Fatal("receipt replay repinned/rewrote", e)
	}
	id := req.ArtifactReferences[0].Artifact.ID
	arts := []references.Artifact{{ID: id}}
	old, e := ReferenceSource(x.s.pool, "ahp_state").InspectReferences(x.ctx, x.a.Tenant, nil, arts)
	if e != nil || old.Referenced[id] {
		t.Fatal("old keeper unexpectedly knows typed common refs", e)
	}
	live, e := CommonReferenceSource(x.s.pool, "ahp_state").InspectReferences(x.ctx, x.a.Tenant, nil, arts)
	if e != nil || !live.Referenced[id] {
		t.Fatal("historical common ref not rooted", e)
	}
	foreign, e := CommonReferenceSource(x.s.pool, "ahp_state").InspectReferences(x.ctx, "other-tenant", nil, arts)
	if e != nil || foreign.Referenced[id] {
		t.Fatal("keeper crosses tenant", e)
	}
	plain := New(x.s.pool, WithCommonPayloadObjects())
	if _, e = plain.ReadRecords(x.ctx, x.a, x.ta, req.Namespace, 1); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("unconfigured reader ignored external profile", e)
	}
	if e = x.s.DeleteThread(x.ctx, x.a, x.ta); e != nil {
		t.Fatal(e)
	}
	dead, e := CommonReferenceSource(x.s.pool, "ahp_state").InspectReferences(x.ctx, x.a.Tenant, nil, arts)
	if e != nil || dead.Referenced[id] {
		t.Fatal("thread deletion left common root", e)
	}
}
func TestRev34ArtifactOwnerRefusalRollsBackCompleteCommonCut(t *testing.T) {
	x, g, req := rev34Fixture(t)
	g.deny = true
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("owner refusal ignored", e)
	}
	for _, table := range []string{"common_cuts", "common_records", "common_heads", "common_receipts", "common_artifact_refs", "common_payloads"} {
		var n int
		if e := x.s.pool.QueryRow(x.ctx, "SELECT count(*) FROM ahp_state."+table+" WHERE thread_id=$1", x.ta).Scan(&n); e != nil || n != 0 {
			t.Fatal("partial cut after refusal", table, n, e)
		}
	}
	g.deny = false
	if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e != nil {
		t.Fatal("clean retry after rollback", e)
	}
}
func TestRev34ExplicitRefScopeMarkerAndProfileGuards(t *testing.T) {
	for _, kind := range []string{"foreign-actor", "marker-mismatch", "missing-record", "duplicate-record", "old-profile", "foreign-context", "foreign-task"} {
		t.Run(kind, func(t *testing.T) {
			x, _, req := rev34Fixture(t)
			switch kind {
			case "foreign-actor":
				req.ArtifactReferences[0].Artifact.Subject = "bob"
			case "marker-mismatch":
				row := req.Document.Records["s"]
				row.Attributes = json.RawMessage(`{"opaque":"url is not a reference"}`)
				req.Document.Records["s"] = row
			case "missing-record":
				req.ArtifactReferences[0].RecordID = "missing"
			case "duplicate-record":
				req.ArtifactReferences = append(req.ArtifactReferences, req.ArtifactReferences[0])
			case "old-profile":
				req.Document.Profile = "test/1"
			case "foreign-context":
				req.ArtifactReferences[0].Artifact.ContextID = "alice-b"
			case "foreign-task":
				req.ArtifactReferences[0].Artifact.TaskID = "other-task"
			}
			if kind == "foreign-context" || kind == "foreign-task" {
				row := req.Document.Records["s"]
				row.Attributes = commonArtifactMarker(req.ArtifactReferences[0].Artifact)
				req.Document.Records["s"] = row
			}
			if _, e := x.s.CommitRecords(x.ctx, x.a, x.fa, req); e == nil {
				t.Fatal("unsafe descriptor accepted", kind)
			}
		})
	}
}
