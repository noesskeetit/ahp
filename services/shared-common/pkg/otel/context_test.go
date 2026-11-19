package otel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDurableTraceParentAndTransportExcludeBaggage(t *testing.T) {
	const parent = "00-11111111111111111111111111111111-2222222222222222-01"
	ctx := WithTraceParent(context.Background(), parent)
	if TraceParent(ctx) != parent {
		t.Fatal("valid parent lost")
	}
	if TraceParent(WithTraceParent(context.Background(), "invalid")) != "" {
		t.Fatal("invalid parent accepted")
	}
	seen := make(chan http.Header, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen <- r.Header.Clone(); w.WriteHeader(204) }))
	defer srv.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	req.Header.Set("baggage", "private=sentinel")
	response, err := (&http.Client{Transport: Transport(nil)}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	header := <-seen
	if header.Get("traceparent") != parent || header.Get("baggage") != "" {
		t.Fatal("propagation lost ancestry or included baggage")
	}
	if req.Header.Get("baggage") == "" {
		t.Fatal("transport mutated caller request")
	}
}
