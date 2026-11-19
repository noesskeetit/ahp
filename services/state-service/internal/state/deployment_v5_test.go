package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"testing"
	"time"
)

type rev46Exporter interface {
	ExportDeploymentV5(context.Context, DeploymentUnit, string, string) (DeploymentV4, error)
}
type rev46Importer interface {
	ImportDeploymentV5(context.Context, DeploymentUnit, DeploymentV4, string) (DeploymentV4Receipt, error)
}

func rev46Fixture(t *testing.T) (*rev32Fixture, CommonCommit) {
	t.Helper()
	f, req, _, _ := rev36Fixture(t)
	for _, s := range []*Store{f.source, f.target} {
		WithCommonRecordVersions()(s)
		if e := s.Migrate(f.ctx); e != nil {
			t.Fatal(e)
		}
	}
	for _, revision := range []int64{3, 4} {
		req.Key = "interval-" + string(rune('a'+revision))
		req.ExpectedRevision = revision
		root := req.Document.Records["root"]
		root.Relations = map[string][]string{"events": {"child", "child"}, "empty": {}}
		req.Document.Records["root"] = root
		req.Document.Records["child"] = CommonRecord{Kind: "event", NativeID: json.RawMessage(`{"id": "x", "id":"y"}`), Sequence: json.RawMessage(`null`), OwnerHint: json.RawMessage(`"native"`), Attributes: json.RawMessage(`{"value": ` + string(rune('0'+revision)) + `}`), Relations: map[string][]string{}}
		if _, e := f.source.CommitRecords(f.ctx, f.actors[0], f.fences[0], req); e != nil {
			t.Fatal(e)
		}
	}
	return f, req
}

func rev46Export(t *testing.T, f *rev32Fixture) DeploymentV4 {
	t.Helper()
	x, ok := any(f.source).(rev46Exporter)
	if !ok {
		t.Fatal("INTERVAL_MIGRATION_NOT_IMPLEMENTED")
	}
	image, e := x.ExportDeploymentV5(f.ctx, f.unit, "move", rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	return image
}

func TestRev46V5PhysicalHistoryReplayAndFencing(t *testing.T) {
	f, req := rev46Fixture(t)
	if _, e := f.source.ExportDeploymentV4(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("old export silently accepted interval layout", e)
	}
	image := rev46Export(t, f)
	if image.State.Manifest.Format != "ahp-state-deployment/5" || len(image.State.Entities["common_record_versions"]) == 0 || len(image.State.Entities["common_version_roots"]) != 2 {
		t.Fatal("physical versions not transferred")
	}
	x, ok := any(f.target).(rev46Importer)
	if !ok {
		t.Fatal("INTERVAL_IMPORT_NOT_IMPLEMENTED")
	}
	receipt, e := x.ImportDeploymentV5(f.ctx, f.unit, image, rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	again, e := x.ImportDeploymentV5(f.ctx, f.unit, image, rev32Build)
	if e != nil || !reflect.DeepEqual(receipt, again) {
		t.Fatal("import replay", e)
	}
	for _, revision := range []int64{1, 2, 3, 4, 5} {
		a, e := f.source.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", revision)
		if e != nil {
			t.Fatal(e)
		}
		b, e := f.target.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", revision)
		if e != nil || !reflect.DeepEqual(a, b) {
			t.Fatal("historic native cut lost", revision, e)
		}
	}
	for _, table := range []string{"common_record_versions", "common_role_versions", "common_edge_versions", "common_version_roots"} {
		if rev32Rows(t, f.source, table) != rev32Rows(t, f.target, table) {
			t.Fatal("physical layout expanded or omitted", table)
		}
	}
	req.Key = "old-writer"
	req.ExpectedRevision = 5
	if _, e = f.target.CommitRecords(f.ctx, f.actors[0], f.fences[0], req); contracts.Code(e) != contracts.ErrStaleEpoch {
		t.Fatal("source epoch accepted", e)
	}
	lease, e := f.target.ClaimRunFor(f.ctx, f.unit.Tenant, f.unit.Deployment, "new-target", f.fences[0].RunID, time.Minute)
	if e != nil || lease == nil || lease.Epoch <= f.fences[0].Epoch {
		t.Fatal("target lease", e)
	}
}

func TestRev46V5ResealedCorruptionCannotHideLostNativeData(t *testing.T) {
	f, _ := rev46Fixture(t)
	original := rev46Export(t, f)
	before, _ := json.Marshal(original)
	if _, e := ValidateDeploymentV5(original, f.unit, rev32Build); e != nil {
		t.Fatal(e)
	}
	after, _ := json.Marshal(original)
	if string(before) != string(after) {
		t.Fatal("validation mutated sealed archive")
	}
	cases := []string{"missing-root", "wrong-root", "orphan-root", "missing-record", "overlapping-record", "missing-last-edge", "overlapping-role", "missing-empty-role", "edge-gap", "foreign-thread", "bad-end", "null-from", "unknown-column", "hidden-sequence", "bad-native-bytes", "anchor-mismatch", "extra-anchor", "legacy-edge", "missing-artifact-ref", "duplicate-entity", "wrong-format", "wrong-unit", "missing-catalog"}
	for _, kind := range cases {
		t.Run(kind, func(t *testing.T) {
			image := rev36Clone(original)
			change := func(table string, index int, key string, value any) {
				var r map[string]any
				if e := json.Unmarshal(image.State.Entities[table][index], &r); e != nil {
					t.Fatal(e)
				}
				r[key] = value
				image.State.Entities[table][index], _ = json.Marshal(r)
			}
			switch kind {
			case "missing-root":
				image.State.Entities["common_version_roots"] = image.State.Entities["common_version_roots"][1:]
			case "wrong-root":
				change("common_version_roots", 0, "digest", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
			case "orphan-root":
				change("common_version_roots", 0, "revision", 1)
			case "missing-record":
				image.State.Entities["common_record_versions"] = image.State.Entities["common_record_versions"][1:]
			case "overlapping-record":
				r := image.State.Entities["common_record_versions"][0]
				image.State.Entities["common_record_versions"] = append(image.State.Entities["common_record_versions"], r)
				change("common_record_versions", len(image.State.Entities["common_record_versions"])-1, "from_revision", 5)
			case "missing-last-edge":
				a := image.State.Entities["common_edge_versions"]
				image.State.Entities["common_edge_versions"] = a[:len(a)-1]
			case "overlapping-role":
				r := image.State.Entities["common_role_versions"][0]
				image.State.Entities["common_role_versions"] = append(image.State.Entities["common_role_versions"], r)
				change("common_role_versions", len(image.State.Entities["common_role_versions"])-1, "from_revision", 5)
			case "missing-empty-role":
				rows := []json.RawMessage{}
				for _, r := range deploymentRows(image.State, "common_role_versions") {
					if deploymentString(r, "role") != "empty" {
						b, _ := json.Marshal(r)
						rows = append(rows, b)
					}
				}
				image.State.Entities["common_role_versions"] = rows
			case "edge-gap":
				change("common_edge_versions", 0, "ordinal", 12)
			case "foreign-thread":
				change("common_record_versions", 0, "thread_id", "another-thread")
			case "bad-end":
				change("common_record_versions", 0, "to_revision", 40000)
			case "null-from":
				change("common_record_versions", 0, "from_revision", nil)
			case "unknown-column":
				change("common_record_versions", 0, "unreviewed", true)
			case "hidden-sequence":
				change("common_record_versions", 0, "presence", 0)
				change("common_record_versions", 0, "sequence", 2)
			case "bad-native-bytes":
				change("common_record_versions", 0, "native_id", "not base64")
			case "anchor-mismatch":
				for i, r := range deploymentRows(image.State, "common_records") {
					n, _ := deploymentInt(r, "revision")
					if n == 4 {
						change("common_records", i, "kind", "wrong")
						break
					}
				}
			case "extra-anchor":
				for _, r := range deploymentRows(image.State, "common_records") {
					n, _ := deploymentInt(r, "revision")
					if n == 4 {
						r["id"] = deploymentV5JSON("extraneous")
						b, _ := json.Marshal(r)
						image.State.Entities["common_records"] = append(image.State.Entities["common_records"], b)
						break
					}
				}
			case "legacy-edge":
				image.State.Entities["common_edges"] = append(image.State.Entities["common_edges"], deploymentV5JSON(map[string]any{"thread_id": f.threads[0], "namespace": "native", "revision": 4, "source": "root", "role": "events", "ordinal": 0, "target": "child"}))
			case "missing-artifact-ref":
				image.State.Entities["common_artifact_refs"] = []json.RawMessage{}
			case "duplicate-entity":
				a := image.State.Entities["common_record_versions"]
				image.State.Entities["common_record_versions"] = append(a, a[0])
			case "wrong-format":
				image.State.Manifest.Format = DeploymentV4Format
			case "wrong-unit":
				image.State.Manifest.Unit.Deployment = "other"
			case "missing-catalog":
				delete(image.State.Entities, "common_role_versions")
			}
			for name, rows := range image.State.Entities {
				image.State.Manifest.Counts[name] = len(rows)
			}
			rev36Seal(&image)
			if _, e := ValidateDeploymentV5(image, f.unit, rev32Build); e == nil {
				t.Fatal("resealed invalid interval archive accepted", kind)
			}
		})
	}
}

func TestRev46V5LateCustodyRefusalRollsBackIntervalsAndRetry(t *testing.T) {
	f, _ := rev46Fixture(t)
	image := rev46Export(t, f)
	f.refuseCommit = true
	if _, e := f.target.ImportDeploymentV5(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrForbidden {
		t.Fatal("late custody was not rechecked", e)
	}
	for _, table := range []string{"threads", "common_cuts", "common_records", "common_artifact_refs", "common_record_versions", "common_role_versions", "common_edge_versions", "common_version_roots", "deployment_transfers_v3"} {
		if n := rev32Rows(t, f.target, table); n != 0 {
			t.Fatal("partial interval import", table, n)
		}
	}
	f.refuseCommit = false
	f.target.commonRecordVersions = false
	if _, e := f.target.ImportDeploymentV5(f.ctx, f.unit, image, rev32Build); contracts.Code(e) != contracts.ErrUnavailable {
		t.Fatal("disabled interval target accepted image", e)
	}
	f.target.commonRecordVersions = true
	if _, e := f.target.ImportDeploymentV5(f.ctx, f.unit, image, rev32Build); e != nil {
		t.Fatal("retry after rollback", e)
	}
}
