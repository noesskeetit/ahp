package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// This exercises the SDK through mTLS HTTP, with a bounded State contract
// double. It is not evidence for PostgreSQL or object-store durability.
func TestCheckpointLargePayloadRoundTrip(t *testing.T) {
	m := newTestTLS(t)
	var saved Checkpoint
	objects := map[string][]byte{}
	refs := map[string]map[string]any{}
	var pending []PendingWrite
	var uploadID string
	uploadServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b bytes.Buffer
		_, _ = b.ReadFrom(r.Body)
		objects[uploadID] = b.Bytes()
		w.WriteHeader(200)
	}))
	defer uploadServer.Close()
	deny := false
	corrupt := false
	corruptSize := false
	uploads := 0
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			var b bytes.Buffer
			_, _ = b.ReadFrom(r.Body)
			objects[uploadID] = b.Bytes()
			w.WriteHeader(200)
			return
		}
		var in map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&in)
		reply := func(v any) { w.Header().Set("Content-Type", "application/json"); _ = json.NewEncoder(w).Encode(v) }
		switch strings.TrimPrefix(r.URL.Path, "/v1/") {
		case "artifacts/BeginUpload":
			uploads++
			uploadID = "object-" + string(rune('a'+uploads))
			var sha, name, mime string
			var size int64
			_ = json.Unmarshal(in["sha256"], &sha)
			_ = json.Unmarshal(in["size"], &size)
			_ = json.Unmarshal(in["name"], &name)
			_ = json.Unmarshal(in["mime"], &mime)
			refs[uploadID] = map[string]any{"id": uploadID, "version": "v1", "sha256": sha, "size": size, "tenant": "t", "deployment": "d", "subject": "s", "context_id": "ctx", "task_id": "run", "name": name, "mime": mime}
			reply(map[string]any{"id": uploadID, "presigned_url": uploadServer.URL + "/upload", "sha256": sha, "size": size})
		case "artifacts/Seal", "artifacts/Get":
			var id string
			_ = json.Unmarshal(in["id"], &id)
			if deny {
				w.WriteHeader(403)
				reply(map[string]string{"code": "FORBIDDEN", "message": "revoked"})
				return
			}
			reply(refs[id])
		case "artifacts/Download":
			var id string
			_ = json.Unmarshal(in["id"], &id)
			raw := objects[id]
			if corrupt {
				raw = append([]byte(nil), raw...)
				raw[0] ^= 1
			}
			if corruptSize {
				raw = []byte("tampered")
			}
			_, _ = w.Write(raw)
		case "state/PutCheckpoint":
			saved = Checkpoint{}
			_ = json.Unmarshal(in["checkpoint"], &saved)
			for _, p := range saved.Parts {
				if len(p.Data) > 1<<20 {
					w.WriteHeader(400)
					reply(map[string]string{"code": "INVALID_ARGUMENT", "message": "part data exceeds 1MiB"})
					return
				}
			}
			reply(saved)
		case "state/GetPart":
			reply(map[string]any{"data": saved.Parts[0].Data, "encoding": saved.Parts[0].Encoding})
		case "state/GetCheckpoint":
			cp := saved
			cp.Parts = append([]CheckpointPart(nil), saved.Parts...)
			for i := range cp.Parts {
				cp.Parts[i].Data = nil
			}
			reply(cp)
		case "state/PutWrites":
			_ = json.Unmarshal(in["writes"], &pending)
			reply(map[string]bool{"ok": true})
		case "state/GetWrites":
			reply(map[string]any{"writes": pending})
		default:
			t.Errorf("unexpected %s", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	data := bytes.Repeat([]byte("large-value"), 120000)
	cp := Checkpoint{ID: "cp", ThreadID: "thread", Namespace: "ns", Version: 1, Metadata: json.RawMessage(`{"serializer":"kept"}`), Parts: []CheckpointPart{{Channel: "value", Version: "v1", Data: data}}}
	out, err := c.PutCheckpoint(context.Background(), Fence{"run", "worker", 1}, cp, "key")
	if err != nil {
		t.Fatalf("automatic spill failed: %v", err)
	}
	if !bytes.Equal(out.Parts[0].Data, data) || !bytes.Equal(cp.Parts[0].Data, data) {
		t.Fatal("put mutated caller bytes or returned envelope")
	}
	if uploads != 1 || len(saved.Parts[0].Data) > 1<<20 {
		t.Fatalf("no bounded spill: uploads=%d bytes=%d", uploads, len(saved.Parts[0].Data))
	}
	restored, err := c.GetPart(context.Background(), "thread", "ns", "value", "v1")
	if err != nil || !bytes.Equal(restored, data) {
		t.Fatalf("hydrate failed: %v bytes=%d", err, len(restored))
	}
	sha := sha256.Sum256(data)
	if refs[uploadID]["sha256"] != hex.EncodeToString(sha[:]) {
		t.Fatal("wrong artifact digest")
	}
	if _, err := c.PutCheckpoint(context.Background(), Fence{"run", "worker", 1}, Checkpoint{ID: "small", Namespace: "ns", Version: 1, Parts: []CheckpointPart{{Channel: "v", Version: "v2", Data: []byte("small")}}}, "small"); err != nil {
		t.Fatal(err)
	}
	if uploads != 1 {
		t.Fatal("small payload was externalized")
	}
	// Keep an externalized pending write across a fresh client, preserving index.
	if err := c.PutWrites(context.Background(), Fence{"run", "worker", 1}, "ns", "cp", []PendingWrite{{Task: "task", Index: -1, Channel: "v", Data: data}}); err != nil {
		t.Fatal(err)
	}
	fresh := clientFor(t, m, srv.URL, staticToken("tok"))
	writes, err := fresh.GetWrites(context.Background(), "thread", "ns", "cp")
	if err != nil || len(writes) != 1 || writes[0].Index != -1 || !bytes.Equal(writes[0].Data, data) {
		t.Fatalf("pending hydration: %v", err)
	}
	// Pins protect every immutable coordinate, not only the content hash.
	for _, field := range []string{"id", "version", "sha256", "size", "tenant", "deployment", "subject", "context_id", "task_id", "labels"} {
		old := refs[uploadID][field]
		refs[uploadID][field] = "mismatch"
		if field == "size" {
			refs[uploadID][field] = int64(1)
		}
		if _, err = fresh.GetWrites(context.Background(), "thread", "ns", "cp"); err == nil {
			t.Fatalf("descriptor %s mismatch accepted", field)
		}
		refs[uploadID][field] = old
	}
	corrupt = true
	if _, err = fresh.GetWrites(context.Background(), "thread", "ns", "cp"); err == nil {
		t.Fatal("digest/size corruption accepted")
	}
	corrupt = false
	corruptSize = true
	if _, err = fresh.GetWrites(context.Background(), "thread", "ns", "cp"); err == nil {
		t.Fatal("download size mismatch accepted")
	}
	corruptSize = false
	deny = true
	if _, err = fresh.GetWrites(context.Background(), "thread", "ns", "cp"); err == nil {
		t.Fatal("revoked artifact accepted")
	}
}

func TestPayloadInlineAndMetadataCollisionCompatibility(t *testing.T) {
	c := &Client{}
	ctx := context.Background()
	for _, data := range [][]byte{nil, {}, []byte("legacy"), []byte(payloadMagic + "Rnot-a-reference"), bytes.Repeat([]byte("x"), inlinePayloadBytes)} {
		spilled, err := c.spill(ctx, "run", data, false)
		if err != nil {
			t.Fatal(err)
		}
		restored, err := c.hydrate(ctx, spilled)
		if err != nil || !bytes.Equal(restored, data) {
			t.Fatalf("inline changed: %v", err)
		}
	}
	for _, raw := range []json.RawMessage{json.RawMessage(`{"$ahp_checkpoint_payload_v1":"ordinary user text"}`), json.RawMessage(`{"$ahp_checkpoint_payload_v1":"eA=="}`), json.RawMessage(`{"$ahp_checkpoint_payload_v1":{"x":1}}`)} {
		legacy, err := c.hydrateCheckpoint(ctx, Checkpoint{Metadata: raw})
		if err != nil || !bytes.Equal(legacy.Metadata, raw) {
			t.Fatalf("legacy metadata misclassified: %v", err)
		}
		cp, err := c.spillCheckpoint(ctx, "run", Checkpoint{Metadata: raw})
		if err != nil {
			t.Fatal(err)
		}
		cp, err = c.hydrateCheckpoint(ctx, cp)
		if err != nil || !bytes.Equal(cp.Metadata, raw) {
			t.Fatalf("metadata collision lost data: %v", err)
		}
	}
}

func TestCheckpointReferenceOverheadFailsBeforeHTTP(t *testing.T) {
	m := newTestTLS(t)
	requests := 0
	srv := newServer(t, m, func(http.ResponseWriter, *http.Request) { requests++ })
	c := clientFor(t, m, srv.URL, staticToken("tok"))
	parts := make([]CheckpointPart, 3000)
	for i := range parts {
		parts[i] = CheckpointPart{Channel: strings.Repeat("c", 512), Version: strings.Repeat("v", 512)}
	}
	_, err := c.PutCheckpoint(context.Background(), Fence{"run", "worker", 1}, Checkpoint{ID: "cp", Version: 1, Parts: parts}, "key")
	if err == nil || !strings.Contains(err.Error(), "after payload spill") || requests != 0 {
		t.Fatalf("request body limit: err=%v sent=%d", err, requests)
	}
}

func TestPreexistingOpaqueCheckpointCollision(t *testing.T) {
	c := &Client{}
	for _, data := range [][]byte{[]byte(payloadMagic + "Ilegacy"), []byte(payloadMagic + "Rnot-a-reference")} {
		got, err := c.hydrate(context.Background(), data)
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("legacy opaque bytes changed: got=%x want=%x err=%v", got, data, err)
		}
	}
	raw := json.RawMessage(`{"$ahp_checkpoint_payload_v1":"AEFIUC1QQVlMT0FELXYxCkl7ImtlZXAiOiJsZWdhY3kifQ=="}`)
	got, err := c.hydrateCheckpoint(context.Background(), Checkpoint{Metadata: raw})
	if err != nil || !bytes.Equal(got.Metadata, raw) {
		t.Fatalf("legacy metadata changed: got=%s err=%v", got.Metadata, err)
	}
}
