package artifacts

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestSealedBytesAreNotExtractedText(t *testing.T) {
	e := setup(t)
	ref, _ := e.uploadSeal(t, "ingestion-required", "a document requiring processing")
	ready, err := e.store.Readiness(context.Background(), e.scope, ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(ready)
	var fields map[string]any
	json.Unmarshal(b, &fields)
	if fields["content_ready"] != true || fields["extraction_ready"] != false || fields["search_ready"] != false || ready.Ready {
		t.Fatalf("seal must not fabricate extraction or search readiness: %s", b)
	}
}

func TestIngestionSandboxCacheChunkQuarantine(t *testing.T) {
	parser := os.Getenv("AHP_ARTIFACT_PARSER")
	if parser == "" {
		t.Skip("requires built Linux artifact parser")
	}
	e := setup(t, WithParserCommand(ParserCommand{Path: parser}))
	ctx := context.Background()
	upload := func(key, mime, body string) ArtifactRef {
		sum := sha256.Sum256([]byte(body))
		up, err := e.store.BeginUploadWithBinding(ctx, e.scope, Binding{ContextID: "document-context"}, key, UploadRequest{Name: "document.txt", MIME: mime, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))})
		if err != nil {
			t.Fatal(err)
		}
		put(t, up.PresignedURL, body)
		ref, err := e.store.Seal(ctx, e.scope, up.ID)
		if err != nil {
			t.Fatal(err)
		}
		return ref
	}
	body := "alpha привет document searchable"
	ref := upload("parsed", "text/plain", body)
	rd, err := e.store.Readiness(ctx, e.scope, ref.ID)
	if err != nil || !rd.ContentReady || !rd.ExtractionReady || !rd.SearchReady || rd.Quarantined || !rd.Ready {
		t.Fatalf("ready=%+v err=%v", rd, err)
	}
	chunk, err := e.store.ReadTextChunk(ctx, e.scope, ref.ID, ref.Version, 0, 32, 8)
	if err != nil || len(chunk.Text) > 8 || !strings.HasPrefix(body, chunk.Text) || !utf8.ValidString(chunk.Text) {
		t.Fatalf("budgeted chunk=%+v err=%v", chunk, err)
	}
	matched, err := e.store.SearchText(ctx, e.scope, ref.ID, ref.Version, "searchable")
	if err != nil || !matched {
		t.Fatalf("indexed search=%v err=%v", matched, err)
	}
	// Cache hit must work after the parser is removed, within this tenant only.
	e.store.parser.Path = "/unavailable/parser"
	second := upload("cache-hit", "text/plain", body)
	if rd, err := e.store.Readiness(ctx, e.scope, second.ID); err != nil || !rd.Ready {
		t.Fatalf("cache hit=%+v %v", rd, err)
	}
	originalScope := e.scope
	e.scope.Tenant += "-other"
	other := upload("tenant-cache-miss", "text/plain", body)
	if rd, err := e.store.Readiness(ctx, e.scope, other.ID); err != nil || !rd.Quarantined {
		t.Fatalf("tenant cache leaked=%+v %v", rd, err)
	}
	e.scope = originalScope
	e.store.parser.Path = parser
	bad := upload("malformed", "application/json", "{broken json")
	if rd, err := e.store.Readiness(ctx, e.scope, bad.ID); err != nil || !rd.Quarantined || rd.Ready || rd.ExtractionReady {
		t.Fatalf("malformed not quarantined=%+v %v", rd, err)
	}
	if _, _, err := e.store.OpenRange(ctx, e.scope, bad.ID, 0, 4); err == nil {
		t.Fatal("quarantined bytes served")
	}
	if err := e.store.MarkDerivation(ctx, e.scope, bad.ID, Derivation{Kind: "quarantine", Status: "ready"}); err == nil {
		t.Fatal("client cleared quarantine")
	}
	if _, err := e.store.ReadTextChunk(ctx, e.foreign, ref.ID, ref.Version, 0, 16, 16); err == nil {
		t.Fatal("cross-subject text leak")
	}
	if _, err := e.store.ReadTextChunk(WithCallerContext(ctx, "foreign-context"), e.scope, ref.ID, ref.Version, 0, 16, 16); err == nil {
		t.Fatal("artifact visible to foreign context token")
	}
	if _, err := e.store.ReadTextChunk(ctx, e.scope, ref.ID, "wrong-version", 0, 16, 16); err == nil {
		t.Fatal("wrong version allowed")
	}
	e.allow.Store(false)
	if _, err := e.store.ReadTextChunk(ctx, e.scope, ref.ID, ref.Version, 0, 16, 16); err == nil {
		t.Fatal("revoked text access")
	}
}

func TestParserTimeoutAndOutputLimits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "parser")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexec sleep 10\n"), 0700); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := (ParserCommand{Path: path, Timeout: 20 * time.Millisecond}).extract(context.Background(), "text/plain", strings.NewReader("hi")); err == nil {
		t.Fatal("timeout accepted")
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("parser timeout unbounded")
	}
	b := cappedBuffer{limit: 4}
	if _, err := b.Write([]byte("overflow")); err == nil || b.Len() != 0 {
		t.Fatal("parser output unbounded")
	}
}

func TestPresignedPutRejectsWrongSize(t *testing.T) {
	e := setup(t)
	sum := sha256.Sum256([]byte("abc"))
	up, err := e.store.BeginUpload(context.Background(), e.scope, "size-bound", UploadRequest{Name: "data.txt", MIME: "text/plain", SHA256: hex.EncodeToString(sum[:]), Size: 3})
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPut, up.PresignedURL, strings.NewReader("longer"))
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode/100 == 2 {
		t.Fatal("presigned PUT accepted a different content length")
	}
}

func TestUnsupportedOpaqueContentRemainsDownloadable(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	for i, mime := range []string{"application/octet-stream", "image/png", "application/zip", "application/pdf"} {
		body := string([]byte{0, 255, 1, 2, byte(i)})
		sum := sha256.Sum256([]byte(body))
		up, err := e.store.BeginUpload(ctx, e.scope, mime, UploadRequest{Name: "opaque.bin", MIME: mime, SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))})
		if err != nil {
			t.Fatal(err)
		}
		put(t, up.PresignedURL, body)
		ref, err := e.store.Seal(ctx, e.scope, up.ID)
		if err != nil {
			t.Fatal(err)
		}
		rd, err := e.store.Readiness(ctx, e.scope, ref.ID)
		if err != nil || !rd.ContentReady || rd.ExtractionReady || rd.SearchReady || rd.Ready || rd.Quarantined || rd.ExtractionStatus != "unsupported" {
			t.Fatalf("opaque readiness %+v %v", rd, err)
		}
		rc, _, err := e.store.OpenRange(ctx, e.scope, ref.ID, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		readAll(t, rc, body)
	}
}

func TestIngestionOversizedAndPendingTextFailClosed(t *testing.T) {
	e := setup(t)
	ctx := context.Background()
	for _, body := range []string{"not processed", strings.Repeat("x", MaxExtractBytes+1)} {
		sum := sha256.Sum256([]byte(body))
		up, err := e.store.BeginUpload(ctx, e.scope, hex.EncodeToString(sum[:]), UploadRequest{Name: "large.txt", MIME: "text/plain", SHA256: hex.EncodeToString(sum[:]), Size: int64(len(body))})
		if err != nil {
			t.Fatal(err)
		}
		put(t, up.PresignedURL, body)
		ref, err := e.store.Seal(ctx, e.scope, up.ID)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := e.store.Open(ctx, e.scope, ref.ID); err == nil {
			t.Fatal("unprocessed supported document exposed raw bytes")
		}
		if len(body) > MaxExtractBytes {
			rd, err := e.store.Process(ctx, e.scope, ref.ID)
			if err != nil || !rd.Quarantined || rd.Detail != "extraction_size_limit" {
				t.Fatalf("oversized=%+v %v", rd, err)
			}
		}
	}
}
