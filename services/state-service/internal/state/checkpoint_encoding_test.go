package state

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"testing"
)

func TestCheckpointEncodingLegacyFingerprintAndUpgrade(t *testing.T) {
	e := cpFixture(t)
	raw := []byte("\x00AHP-PAYLOAD-v1\nIlegacy")
	metadata := json.RawMessage(`{"$ahp_checkpoint_payload_v1":"AEFIUC1QQVlMT0FELXYxCkl7ImtlZXAiOiJsZWdhY3kifQ=="}`)
	c := cp("legacy", Part{Channel: "ch", Version: "v1", Data: raw})
	c.Metadata = metadata
	// The original canonical request had no encoding fields at all.
	legacy := map[string]any{"id": c.ID, "namespace": c.Namespace, "parent": c.Parent, "version": c.Version, "metadata": c.Metadata, "parts": []map[string]string{{"channel": "ch", "version": "v1", "data": string(raw)}}}
	want, _ := contracts.Digest(legacy)
	got, _ := checkpointFingerprint(c, c.Parts)
	if got != want {
		t.Fatalf("legacy fingerprint changed: %s != %s", got, want)
	}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "legacy"); err != nil {
		t.Fatal(err)
	}
	if err := e.s.MigrateCheckpoints(e.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "legacy"); err != nil {
		t.Fatalf("upgrade replay: %v", err)
	}
	blob, err := e.s.GetPartEncoded(e.ctx, e.scope, e.threadID, "ns", "ch", "v1")
	if err != nil || blob.Encoding != "" || !bytes.Equal(blob.Data, raw) {
		t.Fatalf("legacy upgraded bytes: %+v %v", blob, err)
	}
	header, err := e.s.GetCheckpoint(e.ctx, e.scope, e.threadID, "ns", "legacy")
	var old, actual any
	_ = json.Unmarshal(metadata, &old)
	_ = json.Unmarshal(header.Metadata, &actual)
	oldJSON, _ := json.Marshal(old)
	actualJSON, _ := json.Marshal(actual)
	if err != nil || header.MetadataEncoding != "" || !bytes.Equal(oldJSON, actualJSON) {
		t.Fatalf("legacy metadata changed: %+v %v", header, err)
	}
}

func TestCheckpointEncodingRoundtripReuseBatchAndExport(t *testing.T) {
	e := cpFixture(t)
	e.s.SetCheckpointArtifacts(codecOnlyGuard{})
	descriptor := []byte(`{"id":"artifact-1","version":"version-1"}`)
	c := cp("encoded", Part{Channel: "ch", Version: "v1", Data: descriptor, Encoding: CheckpointArtifactEncoding})
	c.Metadata = descriptor
	c.MetadataEncoding = CheckpointArtifactEncoding
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "encoded"); err != nil {
		t.Fatal(err)
	}
	// Reusing the exact bytes as legacy opaque data changes their meaning.
	conflict := cp("conflict", Part{Channel: "ch", Version: "v1", Data: descriptor})
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, conflict, "conflict"); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("encoding reuse accepted: %v", err)
	}
	copy := cp("copy", Part{Channel: "ch", Version: "v1"})
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, copy, "copy"); err != nil {
		t.Fatal(err)
	}
	w := PendingWrite{Task: "task", Index: -1, Channel: "ch", Data: descriptor, Encoding: CheckpointArtifactEncoding}
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "encoded", []PendingWrite{w}); err != nil {
		t.Fatal(err)
	}
	conflictWrite := w
	conflictWrite.Encoding = ""
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "encoded", []PendingWrite{conflictWrite}); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("pending codec reuse accepted: %v", err)
	}
	batch := cp("batch", Part{Channel: "batch", Version: "v1", Data: descriptor, Encoding: CheckpointArtifactEncoding})
	batch.Metadata = descriptor
	batch.MetadataEncoding = CheckpointArtifactEncoding
	if _, err := e.s.PutBatch(e.ctx, e.scope, e.fence, BatchRequest{Checkpoint: &batch, Key: "batch", Writes: []PendingWrite{w}}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.s.PutBatch(e.ctx, e.scope, e.fence, BatchRequest{Checkpoint: &batch, Key: "batch", Writes: []PendingWrite{conflictWrite}}); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("batch pending codec reuse accepted: %v", err)
	}
	list, err := e.s.ListCheckpoints(e.ctx, e.scope, e.threadID, "ns", 10)
	if err != nil || len(list) != 3 {
		t.Fatalf("list: %v %#v", err, list)
	}
	exp, err := e.s.ExportThread(e.ctx, e.scope, e.threadID)
	if err != nil {
		t.Fatal(err)
	}
	for _, cp := range exp.Checkpoints {
		if len(cp.Parts) != 1 || cp.Parts[0].Encoding != CheckpointArtifactEncoding {
			t.Fatalf("lost copied/exported encoding: %+v", cp)
		}
	}
	if _, err := e.s.ImportThread(e.ctx, e.scope, exp, "restored"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("unbound encoded import accepted: %v", err)
	}
	// This test owns only the codec with a deliberately synthetic owner. Supply
	// target-bound fixtures for import; actual authority-preserving rehome is
	// covered by TestCheckpointRecoveryRealOwners through mTLS and real storage.
	descriptor, _ = json.Marshal(map[string]any{"id": "artifact-1", "version": "version-1", "tenant": e.scope.Tenant, "deployment": e.scope.Deployment, "subject": e.scope.Subject, "context_id": "restored", "task_id": "restored-task"})
	for i := range exp.Checkpoints {
		if exp.Checkpoints[i].MetadataEncoding != "" {
			exp.Checkpoints[i].Metadata = descriptor
		}
		for j := range exp.Checkpoints[i].Parts {
			exp.Checkpoints[i].Parts[j].Data = descriptor
		}
	}
	for i := range exp.Writes {
		for j := range exp.Writes[i].Writes {
			exp.Writes[i].Writes[j].Data = descriptor
		}
	}
	restored, err := e.s.ImportThread(e.ctx, e.scope, exp, "restored")
	if err != nil {
		t.Fatal(err)
	}
	blob, err := e.s.GetPartEncoded(e.ctx, e.scope, restored.ID, "ns", "ch", "v1")
	if err != nil || blob.Encoding != CheckpointArtifactEncoding || !bytes.Equal(blob.Data, descriptor) {
		t.Fatalf("import blob: %+v %v", blob, err)
	}
	header, err := e.s.GetCheckpoint(e.ctx, e.scope, restored.ID, "ns", "encoded")
	if err != nil || header.MetadataEncoding != CheckpointArtifactEncoding {
		t.Fatalf("import metadata: %+v %v", header, err)
	}
	for _, id := range []string{"encoded", "batch"} {
		writes, err := e.s.GetWrites(e.ctx, e.scope, restored.ID, "ns", id)
		if err != nil || len(writes) != 1 || writes[0].Encoding != CheckpointArtifactEncoding {
			t.Fatalf("import pending %s: %+v %v", id, writes, err)
		}
	}
	c.ID = "future"
	c.MetadataEncoding = "future-v2"
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "future"); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("unknown codec accepted: %v", err)
	}
}

func TestCheckpointEncodingReferenceSource(t *testing.T) {
	e := cpFixture(t)
	e.s.SetCheckpointArtifacts(codecOnlyGuard{})
	descriptor := func(id string) []byte {
		raw, _ := json.Marshal(map[string]string{"id": id, "version": "v1"})
		return raw
	}
	c := cp("refs", Part{Channel: "encoded", Version: "v1", Data: descriptor("blob-ref"), Encoding: CheckpointArtifactEncoding}, Part{Channel: "legacy", Version: "v1", Data: append([]byte("\x00AHP-PAYLOAD-v1\nR"), 0xff)})
	c.Metadata = descriptor("metadata-ref")
	c.MetadataEncoding = CheckpointArtifactEncoding
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "refs"); err != nil {
		t.Fatal(err)
	}
	if err := e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "ahead-of-header", []PendingWrite{{Task: "task", Channel: "pending", Data: descriptor("pending-ref"), Encoding: CheckpointArtifactEncoding}}); err != nil {
		t.Fatal(err)
	}
	// Exact legacy JSON resembling a descriptor is not a live artifact reference.
	legacy := cp("legacy-json", Part{Channel: "json", Version: "v1", Data: descriptor("opaque-ref")})
	legacy.Metadata = descriptor("opaque-ref")
	if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, legacy, "legacy-json"); err != nil {
		t.Fatal(err)
	}
	source := ReferenceSource(e.s.pool, "ahp_state")
	queries := []references.Artifact{{ID: "blob-ref"}, {ID: "metadata-ref"}, {ID: "pending-ref"}, {ID: "opaque-ref"}}
	for _, tenant := range []string{e.scope.Tenant, "foreign"} {
		got, err := source.InspectReferences(e.ctx, tenant, nil, queries)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range queries {
			want := tenant == e.scope.Tenant && q.ID != "opaque-ref"
			if got.Referenced[q.ID] != want {
				t.Fatalf("reference %s tenant %s=%v want%v", q.ID, tenant, got.Referenced[q.ID], want)
			}
		}
	}
	if err := e.s.DeleteThread(e.ctx, e.scope, e.threadID); err != nil {
		t.Fatal(err)
	}
	got, err := source.InspectReferences(e.ctx, e.scope.Tenant, nil, queries)
	if err != nil || len(got.Referenced) != 0 {
		t.Fatalf("references survived cleanup: %+v %v", got, err)
	}
}

func TestCheckpointEncodingRejectsMalformedDescriptors(t *testing.T) {
	for _, raw := range [][]byte{nil, {}, []byte(`null`), []byte(`{}`), []byte(`{"id":"a"}`)} {
		if err := validatePayloadEncoding(CheckpointArtifactEncoding, raw); err == nil {
			t.Fatalf("invalid descriptor accepted: %q", raw)
		}
	}
	if err := validatePayloadEncoding("future-v2", []byte(`{"id":"a","version":"v1"}`)); err == nil {
		t.Fatal("unknown encoding accepted")
	}
	if err := validatePayloadEncoding("", []byte{0, 0xff}); err != nil {
		t.Fatalf("opaque bytes rejected: %v", err)
	}
}

// Only the codec projection is under test here. Real owner/GC behavior lives
// in artifacts/checkpoint_gc_test.go against PostgreSQL and MinIO.
type codecOnlyGuard struct{}

func (codecOnlyGuard) PinCheckpointReferences(context.Context, contracts.Scope, string, []references.CheckpointArtifact) error {
	return nil
}
func (codecOnlyGuard) ReleaseCheckpointReferences(context.Context, string) error { return nil }
