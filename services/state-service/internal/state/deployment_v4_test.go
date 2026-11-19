package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"strings"
	"testing"
	"time"
)

func rev36Fixture(t *testing.T) (*rev32Fixture, CommonCommit, *bool, *[]string) {
	t.Helper()
	f := rev32FixtureNew(t)
	deny := false
	phases := []string{}
	for _, s := range []*Store{f.source, f.target} {
		WithCommonArtifactReferences()(s)
		s.SetCheckpointArtifacts(&rev34Guard{t: t, s: s})
		s.deploymentTransfer.SharedArtifacts = &SharedArtifactTransfer{OwnerID: "artifact-owner-fixture", Exports: &rev36MemoryCache{}, Verify: func(_ context.Context, c SharedArtifactCheck) error {
			phases = append(phases, c.Phase)
			if c.OwnerID != "artifact-owner-fixture" || c.Unit != f.unit || len(c.References) != 1 {
				return invalid("wrong fixture dependency")
			}
			if deny && c.Phase == "before-commit" {
				return contracts.NewError(contracts.ErrForbidden, "fixture dependency authority revoked")
			}
			return nil
		}}
		if e := s.Migrate(f.ctx); e != nil {
			t.Fatal(e)
		}
	}
	run, e := f.source.GetRun(f.ctx, f.actors[0], f.fences[0].RunID)
	if e != nil {
		t.Fatal(e)
	}
	ref := references.CheckpointArtifact{ID: "artifact-native-1", Tenant: f.unit.Tenant, Deployment: f.unit.Deployment, Subject: f.actors[0].Subject, ContextID: "context-alice", TaskID: run.TaskID, Version: "exact-source-version", SHA256: strings.Repeat("a", 64), Size: 1120004}
	req := CommonCommit{Namespace: "native", Key: "external-cut", ExpectedRevision: 2, Document: CommonDocument{Format: CommonFormat, Profile: "native/research-1" + CommonArtifactProfileSuffix, Root: "root", Records: map[string]CommonRecord{"root": {Kind: "session", Attributes: commonArtifactMarker(ref), Relations: map[string][]string{}}}}, ArtifactReferences: []CommonArtifactReference{{RecordID: "root", Artifact: ref}}}
	if _, e = f.source.CommitRecords(f.ctx, f.actors[0], f.fences[0], req); e != nil {
		t.Fatal(e)
	}
	return f, req, &deny, &phases
}
func rev36Clone(image DeploymentV4) DeploymentV4 {
	b, _ := json.Marshal(image)
	var out DeploymentV4
	_ = json.Unmarshal(b, &out)
	return out
}
func rev36Seal(image *DeploymentV4) {
	image.State.Digest, _ = deploymentDigest(image.State)
	image.Digest, _ = deploymentV4Digest(*image)
}
func rev36Export(t *testing.T, f *rev32Fixture) DeploymentV4 {
	t.Helper()
	image, e := f.source.ExportDeploymentV4(f.ctx, f.unit, "move", rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	return image
}
func TestRev36V4RoundTripHistorySharedOwnersRefsReceiptsAndEpoch(t *testing.T) {
	f, req, _, phases := rev36Fixture(t)
	image := rev36Export(t, f)
	refs, e := ValidateDeploymentV4(image, f.unit, rev32Build)
	if e != nil || len(refs) != 1 || len(image.State.Entities["common_artifact_refs"]) != 1 {
		t.Fatal("archive refs", e)
	}
	receipt, e := f.target.ImportDeploymentV4(f.ctx, f.unit, image, rev32Build)
	if e != nil || receipt.ArtifactOwnerID != image.ArtifactOwnerID || receipt.Digest != image.Digest {
		t.Fatal("import", e)
	}
	again, e := f.target.ImportDeploymentV4(f.ctx, f.unit, image, rev32Build)
	if e != nil || !reflect.DeepEqual(again, receipt) {
		t.Fatal("migration replay", e)
	}
	for _, revision := range []int64{1, 2, 3} {
		before, e := f.source.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", revision)
		if e != nil {
			t.Fatal(e)
		}
		after, e := f.target.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", revision)
		if e != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("native/history/ref bytes changed", revision, e)
		}
	}
	for i, sc := range f.actors {
		before, e := f.source.GetKV(f.ctx, sc, []string{"user"}, "state")
		if e != nil {
			t.Fatal(e)
		}
		after, e := f.target.GetKV(f.ctx, sc, []string{"user"}, "state")
		if e != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("shared user lost", i, e)
		}
	}
	req.Key = "stale-source"
	req.ExpectedRevision = 3
	if _, e = f.target.CommitRecords(f.ctx, f.actors[0], f.fences[0], req); contracts.Code(e) != contracts.ErrStaleEpoch {
		t.Fatal("source lease survived transfer", e)
	}
	lease, e := f.target.ClaimRunFor(f.ctx, f.unit.Tenant, f.unit.Deployment, "fresh-target", f.fences[0].RunID, time.Minute)
	if e != nil || lease == nil || lease.Epoch <= f.fences[0].Epoch {
		t.Fatal("target ownership", e)
	}
	if len(*phases) < 3 || (*phases)[0] != "after-export" {
		t.Fatal("dependency custody skipped", *phases)
	}
	if _, e = f.source.ExportDeploymentV3(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("V3 silently accepted new root schema", e)
	}
}
func TestRev36V4DependencyRefusalBeforeCommitRollsBackAllRows(t *testing.T) {
	f, _, deny, _ := rev36Fixture(t)
	image := rev36Export(t, f)
	*deny = true
	if _, e := f.target.ImportDeploymentV4(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("dependency revocation ignored", e)
	}
	for _, table := range []string{"threads", "runs", "common_cuts", "common_records", "common_receipts", "common_artifact_refs", "intents", "kv", "deployment_transfers_v3"} {
		if n := rev32Rows(t, f.target, table); n != 0 {
			t.Fatal("partial migration committed", table, n)
		}
	}
	*deny = false
	if _, e := f.target.ImportDeploymentV4(f.ctx, f.unit, image, rev32Build); e != nil {
		t.Fatal("clean retry", e)
	}
}
func TestRev36V4WrongDependencyCannotBeApprovedByDigestAlone(t *testing.T) {
	f, _, _, _ := rev36Fixture(t)
	image := rev36Export(t, f)
	image.ArtifactOwnerID = "another-owner"
	rev36Seal(&image)
	f.approved = image.Digest
	if _, e := f.target.ImportDeploymentV4(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("caller-selected owner accepted", e)
	}
	if n := rev32Rows(t, f.target, "threads"); n != 0 {
		t.Fatal("foreign owner wrote target")
	}
}
func TestRev36V4ResealingDoesNotHideMissingDuplicateOrForeignRoots(t *testing.T) {
	for _, kind := range []string{"missing-ref", "duplicate-ref", "foreign-context", "null-namespace", "missing-count", "old-wire"} {
		t.Run(kind, func(t *testing.T) {
			f, _, _, _ := rev36Fixture(t)
			image := rev36Clone(rev36Export(t, f))
			switch kind {
			case "missing-ref":
				image.State.Entities["common_artifact_refs"] = []json.RawMessage{}
				image.State.Manifest.Counts["common_artifact_refs"] = 0
			case "duplicate-ref":
				image.State.Entities["common_artifact_refs"] = append(image.State.Entities["common_artifact_refs"], image.State.Entities["common_artifact_refs"][0])
				image.State.Manifest.Counts["common_artifact_refs"]++
			case "foreign-context":
				var row map[string]any
				_ = json.Unmarshal(image.State.Entities["common_artifact_refs"][0], &row)
				row["artifact"].(map[string]any)["json"].(map[string]any)["context_id"] = "other-context"
				image.State.Entities["common_artifact_refs"][0], _ = json.Marshal(row)
			case "null-namespace":
				var row map[string]any
				_ = json.Unmarshal(image.State.Entities["common_artifact_refs"][0], &row)
				row["namespace"] = nil
				image.State.Entities["common_artifact_refs"][0], _ = json.Marshal(row)
			case "missing-count":
				delete(image.State.Manifest.Counts, "common_artifact_refs")
			case "old-wire":
				image.State.Manifest.Format = DeploymentV3Format
			}
			rev36Seal(&image)
			if _, e := ValidateDeploymentV4(image, f.unit, rev32Build); e == nil {
				t.Fatal("invalid dependency archive validated", kind)
			}
		})
	}
}

// Explicit test cache, not a durability/HA claim. Native E2E mounts an fsynced
// private disk cache and tests HTTP loss/reload separately.
type rev36MemoryCache struct {
	key   DeploymentExportKey
	image *DeploymentV4
}

func (c *rev36MemoryCache) Load(_ context.Context, key DeploymentExportKey) (*DeploymentV4, error) {
	if c.image == nil {
		return nil, nil
	}
	if c.key != key {
		return nil, contracts.NewError(contracts.ErrKeyReuse, "fixture cache key reuse")
	}
	copy := rev36Clone(*c.image)
	return &copy, nil
}
func (c *rev36MemoryCache) Store(_ context.Context, key DeploymentExportKey, image DeploymentV4) error {
	if c.image != nil && (c.key != key || c.image.Digest != image.Digest) {
		return contracts.NewError(contracts.ErrKeyReuse, "fixture cache seal is immutable")
	}
	copy := rev36Clone(image)
	c.image = &copy
	c.key = key
	return nil
}
func (c *rev36MemoryCache) Forget(context.Context, DeploymentExportKey) error {
	c.image = nil
	return nil
}
func TestRev36V4ExportReplayReturnsSameImageAndHonorsCurrentControls(t *testing.T) {
	f, _, _, _ := rev36Fixture(t)
	first := rev36Export(t, f)
	second := rev36Export(t, f)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	t.Logf("source cache wire_equal=%v digest_equal=%v exported_at_equal=%v", string(a) == string(b), first.Digest == second.Digest, first.State.Manifest.ExportedAt.Equal(second.State.Manifest.ExportedAt))
	if string(a) != string(b) || first.Digest != second.Digest || !first.State.Manifest.ExportedAt.Equal(second.State.Manifest.ExportedAt) {
		t.Fatal("source export replay changed its HTTP wire image/timestamp/digest")
	}
	if e := f.source.SetLegalhold(f.ctx, f.actors[0], f.threads[0], "fixture current hold"); e != nil {
		t.Fatal(e)
	}
	if _, e := f.source.ExportDeploymentV4(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrConflict {
		t.Fatal("cached export bypassed current control marker", e)
	}
}
