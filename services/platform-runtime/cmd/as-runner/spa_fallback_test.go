package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// TestProductionMuxReservedPathsAnswerAsAPI runs the guard through buildMux, i.e. the exact
// routing the deployed as-runner uses, not just the web.Handler in isolation. The Space handler
// is irrelevant here (no /v1/space/ path is requested); the point is that a missing path under a
// machine-facing prefix reaches the UI catch-all and must be answered as an API rather than as
// the app shell.
func TestProductionMuxReservedPathsAnswerAsAPI(t *testing.T) {
	srv := httptest.NewServer(buildMux(http.NotFoundHandler(), nil))
	t.Cleanup(srv.Close)

	for _, path := range []string{
		"/api/definitely-not-a-route",
		"/v1/definitely-not-a-route",
		"/a2a/definitely-not-a-route",
		"/.well-known/agent-card.json",
		"/robots.txt",
		"/API/definitely-not-a-route",
		"/U-AHP/v1/auth/login",
	} {
		resp, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			t.Errorf("GET %s through buildMux = %d, want 404", path, resp.StatusCode)
		}

		if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
			t.Errorf("GET %s content type = %q, want application/json", path, got)
		}

		if !strings.Contains(string(body), `"code":"NOT_FOUND"`) {
			t.Errorf("GET %s body = %s, want the contract NOT_FOUND envelope", path, body)
		}
	}

	// Positive controls, in the same request path a smoke test would take: the liveness route the
	// mux owns still answers, and the UI is still reachable at / and at a client-side route.
	if resp, err := srv.Client().Get(srv.URL + "/healthz"); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz broken by the guard: err=%v status=%v", err, resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	// The other half of the contract: the guard must not swallow a route that exists. The human
	// download path is mounted as the second handler, so it is a route on the mux and the guard
	// never sees the request. This is what an integration review has to see proved, because the
	// same path measured 404 through this very catch-all when nothing was mounted behind it.
	mounted := httptest.NewServer(buildMux(http.NotFoundHandler(), http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Disposition", `attachment; filename="report.pdf"`)
			_, _ = w.Write([]byte("bytes"))
		})))
	t.Cleanup(mounted.Close)

	resp, err := mounted.Client().Get(mounted.URL + space.UIDownloadPath)
	if err != nil {
		t.Fatalf("GET mounted download: %v", err)
	}
	downloaded, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(downloaded) != "bytes" {
		t.Errorf("mounted download route = %d %q, want 200 \"bytes\" — the guard hid a real route",
			resp.StatusCode, downloaded)
	}

	for _, path := range []string{"/", "/space", "/workspace", "/dot", "/admin"} {
		resp, err := srv.Client().Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "Agents Space") {
			t.Errorf("GET %s = %d, want the UI shell — the guard must not hide the product", path, resp.StatusCode)
		}
	}
}
