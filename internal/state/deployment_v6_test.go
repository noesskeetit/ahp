package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"testing"
)

type retentionExporter interface {
	ExportDeploymentV6(context.Context, DeploymentUnit, string, string) (DeploymentV4, error)
}
type retentionImporter interface {
	ImportDeploymentV6(context.Context, DeploymentUnit, DeploymentV4, string) (DeploymentV4Receipt, error)
}

func retainedDeploymentFixture(t *testing.T) *rev32Fixture {
	t.Helper()
	f := rev32FixtureNew(t)
	for _, s := range []*Store{f.source, f.target} {
		WithCommonRecordVersions()(s)
		option, e := WithCommonRetention([]string{"test/1"})
		if e != nil {
			t.Fatal(e)
		}
		option(s)
		s.SetCheckpointArtifacts(&rev34Guard{t: t, s: s})
		s.deploymentTransfer.SharedArtifacts = &SharedArtifactTransfer{OwnerID: "retention-artifact-owner", Exports: &rev36MemoryCache{}, Verify: func(_ context.Context, c SharedArtifactCheck) error {
			if c.OwnerID != "retention-artifact-owner" || len(c.References) != 0 {
				return invalid("fixture ownership mismatch")
			}
			return nil
		}}
		if e := s.Migrate(f.ctx); e != nil {
			t.Fatal(e)
		}
	}
	fence := f.fences[0]
	if _, e := f.source.ReconcileIntent(f.ctx, f.actors[0], fence.RunID, fence.Worker, fence.Epoch, "effect", "call", IntentSucceeded, f.intents[0].Digest, []byte(`{"fixture_receipt":42}`)); e != nil {
		t.Fatal(e)
	}
	req := rev32Request("alice", 2, 2, f.intents[0].ID)
	if _, e := f.source.CommitRecords(f.ctx, f.actors[0], fence, req); e != nil {
		t.Fatal(e)
	}
	rev := int64(2)
	if _, e := f.source.PutCommonRoot(f.ctx, f.actors[0], fence, CommonRootRequest{Namespace: "native", Key: "branch", OperationKey: "root-branch", Revision: &rev}); e != nil {
		t.Fatal(e)
	}
	if e := f.source.Release(f.ctx, f.actors[0], fence.RunID, fence.Worker, fence.Epoch, RunDone); e != nil {
		t.Fatal(e)
	}
	report, e := f.source.PruneCommonHistory(f.ctx, f.actors[0], CommonPruneRequest{ThreadID: f.threads[0], Namespace: "native", Key: "prune-prefix", ExpectedRevision: 3, KeepFromRevision: 3})
	if e != nil || report.ArchivedCuts != 1 || report.ReadableFrom != 2 {
		t.Fatal("source retention", e, report)
	}
	return f
}
func TestCommonRetentionV6TransferKeepsArchivedReceiptsPinsAndIntervalSnapshots(t *testing.T) {
	f := retainedDeploymentFixture(t)
	if _, e := f.source.ExportDeploymentV5(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("old transfer silently omitted retention", e)
	}
	source, ok := any(f.source).(retentionExporter)
	if !ok {
		t.Fatal("RETENTION_V6_EXPORT_NOT_IMPLEMENTED")
	}
	image, e := source.ExportDeploymentV6(f.ctx, f.unit, "move", rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	if image.State.Manifest.Format != "ahp-state-deployment/6" || len(image.State.Entities["common_retention_roots"]) != 1 {
		t.Fatal("retention catalog missing")
	}
	for _, tc := range []struct {
		name, table, field string
		value              any
	}{
		{"bad-floor", "common_retention_floors", "readable_from", 3},
		{"pin-archived", "common_retention_roots", "revision", 1},
		{"pin-version-gap", "common_retention_roots", "version", 2},
		{"unknown-op", "common_retention_ops", "kind", "unknown"},
		{"unreadable-layout", "common_cuts", "format", "unsupported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, _ := json.Marshal(image)
			var corrupt DeploymentV6
			if e := json.Unmarshal(raw, &corrupt); e != nil {
				t.Fatal(e)
			}
			var row deploymentRow
			if e := json.Unmarshal(corrupt.State.Entities[tc.table][0], &row); e != nil {
				t.Fatal(e)
			}
			row[tc.field], _ = json.Marshal(tc.value)
			corrupt.State.Entities[tc.table][0], _ = json.Marshal(row)
			corrupt.State.Digest, _ = deploymentDigest(corrupt.State)
			corrupt.Digest, _ = deploymentV4Digest(corrupt)
			if _, e := ValidateDeploymentV6(corrupt, f.unit, rev32Build); e == nil {
				t.Fatal("resealed corrupt retention image accepted")
			}
		})
	}
	target, ok := any(f.target).(retentionImporter)
	if !ok {
		t.Fatal("RETENTION_V6_IMPORT_NOT_IMPLEMENTED")
	}
	receipt, e := target.ImportDeploymentV6(f.ctx, f.unit, image, rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	repeat, e := target.ImportDeploymentV6(f.ctx, f.unit, image, rev32Build)
	if e != nil || !reflect.DeepEqual(receipt, repeat) {
		t.Fatal("migration lost-ACK receipt", e)
	}
	if _, e := f.target.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", 1); contracts.Code(e) != contracts.ErrNotFound {
		t.Fatal("archived snapshot resurrected", e)
	}
	for _, revision := range []int64{2, 3} {
		a, e := f.source.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", revision)
		if e != nil {
			t.Fatal(e)
		}
		b, e := f.target.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", revision)
		if e != nil || !reflect.DeepEqual(a, b) {
			t.Fatal("retained native state changed", revision, e)
		}
	}
	replay, e := f.target.CommitRecords(f.ctx, f.actors[0], f.fences[0], f.first[0])
	if e != nil || replay.Revision != 1 {
		t.Fatal("archived operation lost replay protection", e)
	}
	after, e := f.target.PruneCommonHistory(f.ctx, f.actors[0], CommonPruneRequest{ThreadID: f.threads[0], Namespace: "native", Key: "target-prune", ExpectedRevision: 3, KeepFromRevision: 3})
	if e != nil || after.ReadableFrom != 2 || after.ArchivedCuts != 0 {
		t.Fatal("transferred root no longer protects history", e, after)
	}
}
