package memory

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestClientMutationKeysRedirectAndCancellation(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Authorization") != "Bearer task" {
			t.Error("task credential absent")
		}
		if r.URL.Path == "/v1/memory/remember" {
			if r.Header.Get("Idempotency-Key") != "same-key" {
				t.Error("idempotency key changed")
			}
			w.WriteHeader(409)
			_, _ = w.Write([]byte(`{"error":{"code":"idempotency_conflict","message":"key already used"}}`))
			return
		}
		http.Redirect(w, r, "/credential-sink", http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	c, err := New(srv.URL, func(context.Context) (string, error) { return "task", nil }, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Remember(context.Background(), "", RememberRequest{}); err == nil || requests.Load() != 0 {
		t.Fatal("missing key reached network")
	}
	_, err = c.Remember(context.Background(), "same-key", RememberRequest{})
	var api *Error
	if !errors.As(err, &api) || api.Status != 409 || api.Code != "idempotency_conflict" {
		t.Fatalf("contract error lost: %v", err)
	}
	_, err = c.Settings(context.Background(), "")
	if !errors.As(err, &api) || api.Status != 307 || requests.Load() != 2 {
		t.Fatalf("redirect followed: %v requests=%d", err, requests.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.Settings(ctx, "")
	if !errors.Is(err, context.Canceled) || requests.Load() != 2 {
		t.Fatalf("cancellation lost or retried: %v", err)
	}
}

func TestClientRejectsCredentialURLAndOversizedResponse(t *testing.T) {
	provider := func(context.Context) (string, error) { return "task", nil }
	for _, base := range []string{"https://user:secret@host", "https://host/path", "https://host?token=secret", "file:///tmp/memory"} {
		if _, err := New(base, provider, nil); err == nil {
			t.Fatal("unsafe origin accepted")
		}
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"padding":"` + strings.Repeat("x", 4<<20) + `"}`))
	}))
	defer srv.Close()
	c, _ := New(srv.URL, provider, srv.Client())
	_, err := c.Settings(context.Background(), "")
	var api *Error
	if !errors.As(err, &api) || api.Code != "invalid_response" {
		t.Fatalf("unbounded response accepted: %v", err)
	}
}
