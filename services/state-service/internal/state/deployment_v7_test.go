package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"reflect"
	"testing"
	"time"
)

func TestCommonIncrementalV7TransferAndPrunedRoots(t *testing.T) {
	f := retainedDeploymentFixture(t)
	for _, s := range []*Store{f.source, f.target} {
		WithCommonIncrementalAppend()(s)
		if e := s.Migrate(f.ctx); e != nil {
			t.Fatal(e)
		}
	}
	run, e := f.source.CreateRun(f.ctx, f.actors[0], f.threads[0], "next", "next-task", json.RawMessage(`{}`))
	if e != nil {
		t.Fatal(e)
	}
	lease, e := f.source.ClaimRunFor(f.ctx, f.unit.Tenant, f.unit.Deployment, "next-worker", run.ID, time.Minute)
	if e != nil || lease == nil {
		t.Fatal(e)
	}
	fence := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	prev, e := f.source.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", 3)
	if e != nil {
		t.Fatal(e)
	}
	req := CommonCommit{Namespace: "native", Key: "seed-incremental", ExpectedRevision: 3, Document: prev.Document}
	if _, e = f.source.CommitRecords(f.ctx, f.actors[0], fence, req); e != nil {
		t.Fatal(e)
	}
	if e = f.source.Release(f.ctx, f.actors[0], run.ID, fence.Worker, fence.Epoch, RunDone); e != nil {
		t.Fatal(e)
	}
	if _, e = f.source.ExportDeploymentV6(f.ctx, f.unit, "move", rev32Build); contracts.Code(e) != contracts.ErrInvalid {
		t.Fatal("old format omitted incremental roots", e)
	}
	exporter, ok := any(f.source).(interface {
		ExportDeploymentV7(context.Context, DeploymentUnit, string, string) (DeploymentV4, error)
	})
	if !ok {
		t.Fatal("INCREMENTAL_TRANSFER_NOT_IMPLEMENTED")
	}
	image, e := exporter.ExportDeploymentV7(f.ctx, f.unit, "move", rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	importer, ok := any(f.target).(interface {
		ImportDeploymentV7(context.Context, DeploymentUnit, DeploymentV4, string) (DeploymentV4Receipt, error)
	})
	if !ok {
		t.Fatal("incremental importer missing")
	}
	a, e := importer.ImportDeploymentV7(f.ctx, f.unit, image, rev32Build)
	if e != nil {
		t.Fatal(e)
	}
	b, e := importer.ImportDeploymentV7(f.ctx, f.unit, image, rev32Build)
	if e != nil || !reflect.DeepEqual(a, b) {
		t.Fatal("migration replay", e)
	}
	for _, rev := range []int64{2, 3, 4} {
		source, e := f.source.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", rev)
		if e != nil {
			t.Fatal(e)
		}
		target, e := f.target.ReadRecords(f.ctx, f.actors[0], f.threads[0], "native", rev)
		if e != nil || !reflect.DeepEqual(source, target) {
			t.Fatal("native retained cut differs", rev, e)
		}
	}
	// Hash and row catalogs alone must not legitimise a resealed corrupt bucket.
	raw, _ := json.Marshal(image)
	var corrupt DeploymentV4
	json.Unmarshal(raw, &corrupt)
	var row deploymentRow
	json.Unmarshal(corrupt.State.Entities["common_append_nodes"][0], &row)
	row["data"] = commonAppendJSON([]byte(`{}`))
	corrupt.State.Entities["common_append_nodes"][0] = commonAppendJSON(row)
	corrupt.State.Digest, _ = deploymentDigest(corrupt.State)
	corrupt.Digest, _ = deploymentV4Digest(corrupt)
	if _, e := importer.ImportDeploymentV7(f.ctx, f.unit, corrupt, rev32Build); e == nil {
		t.Fatal("resealed corrupt bucket accepted")
	}
}
