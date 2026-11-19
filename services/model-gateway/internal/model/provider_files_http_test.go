package model

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// These are Files protocol fixtures, not a real SaaS/provider or LLM claim.
func TestHTTPFileProtocolUploadDeleteAndLostACK(t *testing.T) {
	var mode atomic.Value
	mode.Store("normal")
	var calls atomic.Int32
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-secret" {
			t.Error("wrong provider credential")
		}
		if r.Method == http.MethodPost {
			if r.URL.Path != "/v1/files" {
				t.Error("wrong upload path")
			}
			if err := r.ParseMultipartForm(FileMaxBytes); err != nil {
				t.Fatal(err)
			}
			defer r.MultipartForm.RemoveAll()
			if r.FormValue("purpose") != "user_data" {
				t.Error("purpose")
			}
			f, h, e := r.FormFile("file")
			if e != nil {
				t.Fatal(e)
			}
			defer f.Close()
			data, _ := io.ReadAll(f)
			if string(data) != "fixture" || h.Filename != "opaque.bin" {
				t.Error("multipart payload")
			}
			if mode.Load().(string) == "lost" {
				conn, _, _ := w.(http.Hijacker).Hijack()
				conn.Close()
				return
			}
			if mode.Load().(string) == "pending-size" {
				io.WriteString(w, `{"id":"file-a","bytes":null}`)
				return
			}
			if mode.Load().(string) == "bad" {
				io.WriteString(w, `{"id":"file-a","bytes":999}`)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"id": "file-a", "bytes": len(data)})
			return
		}
		if r.Method != http.MethodDelete || r.URL.Path != "/v1/files/file-a" {
			t.Error("delete target")
		}
		if mode.Load().(string) == "missing" {
			w.WriteHeader(404)
			return
		}
		if mode.Load().(string) == "bad-delete" {
			io.WriteString(w, `{"id":"other","deleted":true}`)
			return
		}
		if mode.Load().(string) == "secret" {
			w.WriteHeader(500)
			io.WriteString(w, "fixture-secret")
			return
		}
		io.WriteString(w, `{"id":"file-a","deleted":true}`)
	}))
	defer fixture.Close()
	// Only this protocol fixture bypasses public-host policy to reach httptest.
	p := &HTTPFileProvider{base: fixture.URL + "/v1", key: "fixture-secret", identity: "fixture", client: fixture.Client()}
	ctx := context.Background()
	if id, e := p.Upload(ctx, "opaque.bin", "text/plain", []byte("fixture")); e != nil || id != "file-a" {
		t.Fatalf("upload %s %v", id, e)
	}
	if e := p.Delete(ctx, "file-a"); e != nil {
		t.Fatal(e)
	}
	mode.Store("pending-size")
	if id, e := p.Upload(ctx, "opaque.bin", "text/plain", []byte("fixture")); e != nil || id != "file-a" {
		t.Fatal("valid nullable-size receipt refused", e)
	}
	for _, m := range []string{"bad", "lost"} {
		mode.Store(m)
		if _, e := p.Upload(ctx, "opaque.bin", "text/plain", []byte("fixture")); e == nil {
			t.Fatalf("%s ack accepted", m)
		}
	}
	mode.Store("missing")
	if e := p.Delete(ctx, "file-a"); e != nil {
		t.Fatal(e)
	}
	for _, m := range []string{"bad-delete", "secret"} {
		mode.Store(m)
		if e := p.Delete(ctx, "file-a"); e == nil || strings.Contains(e.Error(), "fixture-secret") {
			t.Fatalf("bad deletion error for %s", m)
		}
	}
	before := calls.Load()
	if e := p.Delete(ctx, "../other"); e == nil || calls.Load() != before {
		t.Fatal("unsafe ID dispatched")
	}
}

func TestHTTPFileEgressAndRedirectsFailClosed(t *testing.T) {
	p, e := NewHTTPFileProvider(HTTPFileConfig{BaseURL: "http://127.0.0.1:1/v1", Account: "fixture", APIKey: "fixture-secret", Egress: netpolicy.Policy{PrivateHosts: []string{"127.0.0.1:1"}, Timeout: time.Second}})
	if e == nil {
		if _, e = p.Upload(context.Background(), "opaque.bin", "text/plain", []byte("fixture")); e == nil {
			t.Fatal("loopback export allowed")
		}
	}
	p, e = NewHTTPFileProvider(HTTPFileConfig{BaseURL: "https://files.example.com/v1", Account: "fixture", APIKey: "fixture-secret", Egress: netpolicy.Policy{AllowedHosts: []string{"files.example.com"}}})
	if e != nil {
		t.Fatal(e)
	}
	if p.client.CheckRedirect == nil || p.client.CheckRedirect(&http.Request{}, nil) != http.ErrUseLastResponse {
		t.Fatal("redirect credential forwarding enabled")
	}
	q, e := NewHTTPFileProvider(HTTPFileConfig{BaseURL: "https://files.example.com/v1", Account: "fixture", APIKey: "rotated", Egress: netpolicy.Policy{AllowedHosts: []string{"files.example.com"}}})
	if e != nil || q.Identity() != p.Identity() {
		t.Fatal("key rotation changed account identity")
	}
}
