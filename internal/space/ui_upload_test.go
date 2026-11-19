package space

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func TestUploadProxySealsComputedBytesWithoutExposingStorage(t *testing.T) {
	content := "actual uploaded bytes"
	sum := sha256.Sum256([]byte(content))
	digest := hex.EncodeToString(sum[:])
	var uploaded string
	obj := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("task token leaked to object store")
		}
		b, _ := io.ReadAll(r.Body)
		uploaded = string(b)
		w.WriteHeader(200)
	}))
	defer obj.Close()
	calls := 0
	machine := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") != "Bearer scoped-task" {
			t.Error("wrong machine identity")
		}
		var b map[string]any
		_ = json.NewDecoder(r.Body).Decode(&b)
		switch r.URL.Path {
		case "/v1/artifacts/BeginUpload":
			if b["sha256"] != digest || b["size"] != float64(len(content)) || b["name"] != "file.txt" {
				t.Error("upload metadata is not derived from bytes")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "art", "presigned_url": obj.URL + "/private-object?secret=signed"})
		case "/v1/artifacts/Seal":
			if uploaded != content {
				t.Error("not the uploaded bytes")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "art", "version": "exact", "sha256": digest, "size": len(content)})
		default:
			t.Error("unexpected operation")
		}
	}))
	defer machine.Close()
	dir := t.TempDir()
	h, err := NewUploadProxy(UploadProxyConfig{ArtifactsURL: machine.URL, Client: machine.Client(), TempDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", UIUploadPath+"?name=file.txt&key=stable-key", strings.NewReader(content))
	req.Header.Set("Authorization", "Bearer browser-token")
	rec := httptest.NewRecorder()
	withTask(h, "scoped-task").ServeHTTP(rec, req)
	if rec.Code != 200 || calls != 2 {
		t.Fatalf("status=%d calls=%d response=%s", rec.Code, calls, rec.Body.String())
	}
	for _, forbidden := range []string{"presigned", "secret", "private-object", "browser-token", "scoped-task"} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Error("secret leaked")
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Error("upload temporary file remains")
	}
}

func TestUploadProxyRejectsOversizeAndUnexchangedRequests(t *testing.T) {
	calls := 0
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; t.Error("upstream must not be reached") }))
	defer up.Close()
	dir := t.TempDir()
	h, err := NewUploadProxy(UploadProxyConfig{ArtifactsURL: up.URL, Client: up.Client(), TempDir: dir, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range []bool{false, true} {
		req := httptest.NewRequest("POST", UIUploadPath+"?name=file.txt&key=k", strings.NewReader("too large"))
		rec := httptest.NewRecorder()
		if task {
			withTask(h, "task").ServeHTTP(rec, req)
		} else {
			h.ServeHTTP(rec, req)
		}
		if task && rec.Code != 400 || !task && rec.Code != 403 {
			t.Fatalf("unexpected refusal %d", rec.Code)
		}
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 || calls != 0 {
		t.Fatal("upload left state")
	}
}
