package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// fakeSink records the exact bytes handed to it and returns a deterministic
// handle so the test can assert the model's args flow through unchanged.
type fakeSink struct {
	gotName  string
	gotMIME  string
	gotBytes []byte
	ret      ArtifactHandle
	err      error
}

func (f *fakeSink) SaveArtifact(_ context.Context, name, mime string, data []byte) (ArtifactHandle, error) {
	f.gotName, f.gotMIME, f.gotBytes = name, mime, append([]byte(nil), data...)
	if f.err != nil {
		return ArtifactHandle{}, f.err
	}
	return f.ret, nil
}

func TestSaveArtifactToolFlowsModelBytes(t *testing.T) {
	content := "revenue report\nQ3=2026\n"
	want := sha256.Sum256([]byte(content))
	wantSHA := hex.EncodeToString(want[:])
	sink := &fakeSink{ret: ArtifactHandle{ID: "art-1", Version: "v1"}}
	tool := NewSaveArtifactTool(sink)

	res, err := tool.Execute(context.Background(), ToolSaveArtifact,
		json.RawMessage(`{"name":"revenue.txt","content":"revenue report\nQ3=2026\n","mime":"text/csv"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if sink.gotName != "revenue.txt" || sink.gotMIME != "text/csv" {
		t.Fatalf("sink got name=%q mime=%q", sink.gotName, sink.gotMIME)
	}
	if string(sink.gotBytes) != content {
		t.Fatalf("sink got bytes %q, want %q", sink.gotBytes, content)
	}
	var h ArtifactHandle
	if err := json.Unmarshal(res, &h); err != nil {
		t.Fatalf("result not ArtifactHandle json: %v (%s)", err, res)
	}
	if h.ID != "art-1" || h.Version != "v1" {
		t.Fatalf("handle not passed through: %+v", h)
	}
	// The sink omitted sha/size, so the tool must backfill the digest of the
	// REAL bytes it saved (so a downloader can verify PUT==GET).
	if h.SHA256 != wantSHA {
		t.Fatalf("backfilled sha = %q, want %q", h.SHA256, wantSHA)
	}
	if h.Size != int64(len(content)) {
		t.Fatalf("backfilled size = %d, want %d", h.Size, len(content))
	}
}

func TestSaveArtifactToolValidatesInput(t *testing.T) {
	sink := &fakeSink{}
	tool := NewSaveArtifactTool(sink)
	cases := map[string]string{
		"empty name":    `{"name":"","content":"x"}`,
		"empty content": `{"name":"a.txt","content":""}`,
		"bad json":      `{`,
	}
	for label, args := range cases {
		if _, err := tool.Execute(context.Background(), ToolSaveArtifact, json.RawMessage(args)); err == nil {
			t.Fatalf("%s: expected error, got nil", label)
		}
	}
	// wrong tool name is rejected
	if _, err := tool.Execute(context.Background(), "other", json.RawMessage(`{"name":"a","content":"b"}`)); err == nil {
		t.Fatal("expected error for mismatched tool name")
	}
}

func TestSaveArtifactToolNoSinkFailsClosed(t *testing.T) {
	tool := &SaveArtifactTool{} // nil sink
	if _, err := tool.Execute(context.Background(), ToolSaveArtifact, json.RawMessage(`{"name":"a","content":"b"}`)); err == nil {
		t.Fatal("nil sink must fail closed (never fabricate an artifact)")
	}
}

func TestSaveArtifactToolPropagatesSinkError(t *testing.T) {
	boom := errors.New("minio down")
	tool := NewSaveArtifactTool(&fakeSink{err: boom})
	_, err := tool.Execute(context.Background(), ToolSaveArtifact, json.RawMessage(`{"name":"a","content":"b"}`))
	if err == nil || !strings.Contains(err.Error(), "minio down") {
		t.Fatalf("sink error must propagate, got %v", err)
	}
}

func TestRunScopeContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	if _, _, ok := RunScopeFromContext(ctx); ok {
		t.Fatal("bare context must report no scope")
	}
	sc := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	ctx = WithRunScope(ctx, sc, "tok-abc")
	got, tok, ok := RunScopeFromContext(ctx)
	if !ok {
		t.Fatal("expected scope after WithRunScope")
	}
	if got != sc || tok != "tok-abc" {
		t.Fatalf("scope round-trip wrong: %+v tok=%q", got, tok)
	}
}

func TestMultiExecutorRoutesAndRejects(t *testing.T) {
	sink := &fakeSink{ret: ArtifactHandle{ID: "x", Version: "1"}}
	mx := NewMultiExecutor()
	mx.Register(ToolSaveArtifact, NewSaveArtifactTool(sink))
	if _, err := mx.Execute(context.Background(), ToolSaveArtifact, json.RawMessage(`{"name":"a","content":"b"}`)); err != nil {
		t.Fatalf("mapped tool should execute: %v", err)
	}
	if _, err := mx.Execute(context.Background(), "ghost", json.RawMessage(`{}`)); err == nil {
		t.Fatal("unmapped tool must be a hard error (not a fabricated result)")
	}
}
