package state

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func invalidDescriptorFormats() map[string][]byte {
	return map[string][]byte{
		"duplicate-id-null":      []byte(`{"id":"review-ref","id":null,"version":"v1"}`),
		"duplicate-id-string":    []byte(`{"id":"old-ref","id":"review-ref","version":"v1"}`),
		"duplicate-version-null": []byte(`{"id":"review-ref","version":"v1","version":null}`),
		"duplicate-label-key":    []byte(`{"id":"review-ref","version":"v1","labels":{"subjects":["alice"],"subjects":null}}`),
		"duplicate-escaped-id":   []byte(`{"id":"review-ref","\u0069d":null,"version":"v1"}`),

		"review-nul-extra": []byte(`{"id":"review-ref","version":"v1","extra":"\u0000"}`),
		"unknown-field":    []byte(`{"id":"review-ref","version":"v1","extra":"ordinary"}`),
		"wrong-case":       []byte(`{"ID":"review-ref","version":"v1"}`),
		"unknown-label":    []byte(`{"id":"review-ref","version":"v1","labels":{"extra":[]}}`),
		"nul-coordinate":   []byte(`{"id":"review-ref","version":"\u0000"}`),
		"nul-label":        []byte(`{"id":"review-ref","version":"v1","labels":{"subjects":["\u0000"]}}`),
		"invalid-utf8":     append([]byte(`{"id":"review-ref","version":"`), append([]byte{0xff}, []byte(`"}`)...)...),
		"unpaired-high":    []byte(`{"id":"review-ref","version":"\ud800"}`),
		"unpaired-low":     []byte(`{"id":"review-ref","version":"\udc00"}`),
		"wrong-pair":       []byte(`{"id":"review-ref","version":"\ud800\ud800"}`),
	}
}

func TestCheckpointDescriptorFormat(t *testing.T) {
	for name, raw := range invalidDescriptorFormats() {
		t.Run(name, func(t *testing.T) {
			if err := validatePayloadEncoding(CheckpointArtifactEncoding, raw); contracts.Code(err) != contracts.ErrInvalid {
				t.Fatalf("malformed encoded descriptor accepted: %s (%v)", raw, err)
			}
			if err := validatePayloadEncoding("", raw); err != nil {
				t.Fatalf("opaque legacy parsed: %v", err)
			}
		})
	}
	for _, raw := range [][]byte{
		[]byte(`{"id":"review-ref","version":"v1"}`),
		[]byte(`{"id":"review-ref","version":"literal \\u0000"}`),
		[]byte(`{"id":"review-ref","version":"\ud83d\ude00"}`),
		[]byte(`{"id":"review-ref","version":"�"}`),
		[]byte(`{"id":"review-ref","version":"v1","sha256":"abc","size":1,"tenant":"t","deployment":"d","subject":"s","context_id":"c","task_id":"task","labels":{"subjects":["s"],"workspaces":[],"taints":null}}`),
	} {
		if err := validatePayloadEncoding(CheckpointArtifactEncoding, raw); err != nil {
			t.Fatalf("valid format rejected: %s: %v", raw, err)
		}
	}
}

func TestCheckpointDescriptorWriterScannerAgreement(t *testing.T) {
	e := cpFixture(t)
	e.s.SetCheckpointArtifacts(codecOnlyGuard{})
	source := ReferenceSource(e.s.pool, "ahp_state")
	for name, raw := range invalidDescriptorFormats() {
		t.Run(name, func(t *testing.T) {
			c := cp("invalid", Part{Channel: "ch", Version: "v1", Data: raw, Encoding: CheckpointArtifactEncoding})
			w := PendingWrite{Task: "task", Channel: "ch", Data: raw, Encoding: CheckpointArtifactEncoding}
			metadata := cp("invalid-metadata")
			metadata.Metadata = raw
			metadata.MetadataEncoding = CheckpointArtifactEncoding
			th, err := e.s.GetThread(e.ctx, e.scope, e.threadID)
			if err != nil {
				t.Fatal(err)
			}
			routes := map[string]func() error{
				"part": func() error { _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, "invalid"); return err },
				"metadata": func() error {
					_, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, metadata, "invalid-metadata")
					return err
				},
				"pending": func() error { return e.s.PutWrites(e.ctx, e.scope, e.fence, "ns", "future", []PendingWrite{w}) },
				"batch-part": func() error {
					_, err := e.s.PutBatch(e.ctx, e.scope, e.fence, BatchRequest{Key: "invalid", Checkpoint: &c})
					return err
				},
				"batch-pending": func() error {
					valid := cp("valid-before-refusal")
					_, err := e.s.PutBatch(e.ctx, e.scope, e.fence, BatchRequest{Key: "invalid-write", Checkpoint: &valid, Writes: []PendingWrite{w}})
					return err
				},
				"import-part": func() error {
					_, err := e.s.ImportThread(e.ctx, e.scope, ControlExport{Thread: th, Checkpoints: []Checkpoint{c}}, "invalid-import")
					return err
				},
				"import-metadata": func() error {
					_, err := e.s.ImportThread(e.ctx, e.scope, ControlExport{Thread: th, Checkpoints: []Checkpoint{metadata}}, "invalid-import")
					return err
				},
				"import-pending": func() error {
					_, err := e.s.ImportThread(e.ctx, e.scope, ControlExport{Thread: th, Checkpoints: []Checkpoint{cp("valid")}, Writes: []ExportedWrites{{Namespace: "ns", CheckpointID: "valid", Writes: []PendingWrite{w}}}}, "invalid-import")
					return err
				},
			}
			for route, write := range routes {
				t.Run(route, func(t *testing.T) {
					if err := write(); contracts.Code(err) != contracts.ErrInvalid {
						t.Fatalf("want INVALID before persist: %v", err)
					}
					var headers, blobs, writes, threads int
					err := e.s.pool.QueryRow(e.ctx, `SELECT (SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id=$1),(SELECT count(*) FROM ahp_state.checkpoint_blobs WHERE thread_id=$1),(SELECT count(*) FROM ahp_state.checkpoint_writes WHERE thread_id=$1),(SELECT count(*) FROM ahp_state.threads WHERE tenant=$2)`, e.threadID, e.scope.Tenant).Scan(&headers, &blobs, &writes, &threads)
					if err != nil || headers != 0 || blobs != 0 || writes != 0 || threads != 1 {
						t.Fatalf("partial write survived refusal: headers=%d blobs=%d writes=%d threads=%d err=%v", headers, blobs, writes, threads, err)
					}
					got, err := source.InspectReferences(e.ctx, e.scope.Tenant, nil, []references.Artifact{{ID: "review-ref"}})
					if err != nil || got.Referenced["review-ref"] {
						t.Fatalf("scan after refusal: %+v %v", got, err)
					}
				})
			}
		})
	}
	// Accepted Unicode forms remain byte-exact, are representable as JSONB, and
	// protect their reference; no canonicalization changes State fingerprints.
	for i, raw := range [][]byte{[]byte(`{"id":"review-ref","version":"\ud83d\ude00"}`), []byte(`{"id":"review-ref","version":"literal \\u0000"}`), []byte(`{"id":"review-ref","version":"�"}`)} {
		id := string(rune('a' + i))
		c := cp(id, Part{Channel: id, Version: "v1", Data: raw, Encoding: CheckpointArtifactEncoding})
		c.Metadata = json.RawMessage(raw)
		c.MetadataEncoding = CheckpointArtifactEncoding
		if _, err := e.s.PutCheckpoint(e.ctx, e.scope, e.fence, c, id); err != nil {
			t.Fatal(err)
		}
		stored, err := e.s.GetPartEncoded(e.ctx, e.scope, e.threadID, "ns", id, "v1")
		if err != nil || !bytes.Equal(stored.Data, raw) {
			t.Fatalf("accepted bytes changed: %q %v", stored.Data, err)
		}
		got, err := source.InspectReferences(e.ctx, e.scope.Tenant, nil, []references.Artifact{{ID: "review-ref"}})
		if err != nil || !got.Referenced["review-ref"] {
			t.Fatalf("accepted descriptor not safely scanned: %+v %v", got, err)
		}
	}
}
