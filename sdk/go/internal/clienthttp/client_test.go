package clienthttp

import (
	"context"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCurrentTokenRedirectBoundsReasonAndCancellation(t *testing.T) {
	var leaked atomic.Int32
	trap := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	defer trap.Close()
	token := "one"
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			if r.Header.Get("Authorization") != "Bearer "+token {
				t.Error("stale token")
			}
			w.Write([]byte(`{}`))
		case "/redirect":
			http.Redirect(w, r, trap.URL, 307)
		case "/reason":
			w.WriteHeader(400)
			w.Write([]byte(`{"code":"INVALID_ARGUMENT","reason":"KEY_REUSE_MISMATCH","message":"different payload"}`))
		case "/large":
			w.Write([]byte(strings.Repeat("x", MaxBytes+1)))
		case "/cancel":
			select {
			case <-r.Context().Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
	}))
	defer srv.Close()
	c, e := New(Config{BaseURL: srv.URL, HTTPClient: srv.Client(), TokenSource: func(context.Context) (string, error) { return token, nil }})
	if e != nil {
		t.Fatal(e)
	}
	var out map[string]any
	if e = c.Call(context.Background(), "/token", struct{}{}, &out, false); e != nil {
		t.Fatal(e)
	}
	token = "two"
	if e = c.Call(context.Background(), "/token", struct{}{}, &out, false); e != nil {
		t.Fatal(e)
	}
	if e = c.Call(context.Background(), "/redirect", struct{}{}, &out, true); e == nil || leaked.Load() != 0 {
		t.Fatal("redirect leaked credential")
	}
	if e = c.Call(context.Background(), "/reason", struct{}{}, &out, true); contracts.Code(e) != contracts.ErrInvalid || contracts.ReasonOf(e) != contracts.ErrKeyReuse {
		t.Fatal("platform reason lost", e)
	}
	if e = c.Call(context.Background(), "/large", struct{}{}, &out, true); e == nil {
		t.Fatal("unbounded response accepted")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	e = c.Call(ctx, "/cancel", struct{}{}, &out, true)
	var unknown *Error
	if !errors.Is(e, context.DeadlineExceeded) || !errors.As(e, &unknown) || !unknown.OutcomeUnknown {
		t.Fatal("cancellation/uncertainty lost", e)
	}
}

func TestFailClosedOriginAndToken(t *testing.T) {
	for _, base := range []string{"http://owner", "https://user:secret@owner", "https://owner/x", "https://owner?key=x", "https://owner#x"} {
		if _, e := New(Config{BaseURL: base, HTTPClient: http.DefaultClient, TokenSource: func(context.Context) (string, error) { return "token", nil }}); e == nil {
			t.Fatal("unsafe origin accepted")
		}
	}
	calls := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))
	defer srv.Close()
	c, e := New(Config{BaseURL: srv.URL, HTTPClient: srv.Client(), TokenSource: func(context.Context) (string, error) { return "", nil }})
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Call(context.Background(), "/write", struct{}{}, nil, true); contracts.Code(e) != contracts.ErrForbidden || calls != 0 {
		t.Fatal("empty token reached network", e)
	}
}
