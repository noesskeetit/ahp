package embed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEgressRejectsForbiddenDestinations(t *testing.T) {
	for _, base := range []string{"http://127.0.0.1:9999", "http://169.254.169.254", "https://user:secret@example.com", "https://example.com?key=secret"} {
		if _, err := New(Config{BaseURL: base, APIKey: "credential", Model: "m", Dimensions: 2}); err == nil {
			t.Fatalf("unsafe destination accepted: %s", base)
		}
	}
}
func TestEmbeddingNeverFollowsRedirect(t *testing.T) {
	reached := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reached = true }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	b, err := newWithHTTP(Config{BaseURL: source.URL, APIKey: "credential", Model: "m", Dimensions: 2}, source.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Embed(context.Background(), "synthetic"); err == nil {
		t.Fatal("redirect must be dependency failure")
	}
	if reached {
		t.Fatal("redirect leaked a request to another origin")
	}
}
func TestInvalidEmbeddingResponsesFailClosed(t *testing.T) {
	for name, body := range map[string]string{
		"zero":     `{"data":[{"index":0,"embedding":[0,0]}]}`,
		"overflow": `{"data":[{"index":0,"embedding":[1e100,1]}]}`,
		"model":    `{"model":"other","data":[{"index":0,"embedding":[1,1]}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer srv.Close()
			b, err := newWithHTTP(Config{BaseURL: srv.URL, APIKey: "credential", Model: "m", Dimensions: 2}, srv.Client())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := b.Embed(context.Background(), "synthetic"); err == nil {
				t.Fatal("invalid embedding accepted")
			}
		})
	}
}
