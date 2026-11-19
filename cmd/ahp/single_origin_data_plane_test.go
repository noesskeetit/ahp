//go:build ahp_data

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// DEF-H24-13. The composition the user chain needs — one binary that is the human
// origin AND the artifact/workspace plane (AHP_ROLE=all with
// AHP_ENABLE_DATA=1 + AHP_ENABLE_BROWSER=1 + AHP_SINGLE_ORIGIN_UPSTREAM) — died at
// startup: `panic: pattern "/v1/workspace/" (single_origin.go) conflicts with
// pattern "/v1/workspace/" (role_data.go:61)`. Every stand that needed both faces
// therefore ran the edge WITHOUT the data plane, and that is the shape the browser
// legs measured: /v1/artifacts/Get and /v1/artifacts/Resolve answering
// "404 page not found" from the human origin, so the human download and the digest
// resolver both dead-ended.
//
// The edge must not claim a prefix this process already serves. What it still
// forwards is pinned in the same breath, because dropping the local mount would
// send the process's own face through a token exchange to somebody else.

func TestSingleOriginWithLocalDataPlaneServesLocallyAndDoesNotPanic(t *testing.T) {
	var workspaceHits, artifactHits int32
	workspacePlane := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&workspaceHits, 1)
		_, _ = w.Write([]byte(`{"owner":"local-data-plane"}`))
	})
	artifactPlane := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&artifactHits, 1)
		_, _ = w.Write([]byte(`{"owner":"local-data-plane","path":"` + r.URL.Path + `"}`))
	})

	var upstreamSpaceHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamSpaceHits, 1)
		_, _ = w.Write([]byte(`{"upstream":"` + r.URL.Path + `"}`))
	}))
	defer upstream.Close()

	// The workspace boundary WOULD have been wired here: the whole point is that a
	// local data plane wins even when the forwarding edge is fully configured.
	t.Setenv(envWorkspaceOrigin, upstream.URL)
	t.Setenv(envWorkspaceExchange, upstream.URL+"/v1/identity/control/exchange")
	t.Setenv("AHP_WORKSPACE_TASK_PREFIX", "workspace")
	t.Setenv("AHP_WORKSPACE_TASK_CAPABILITY", "workspace")

	mux := http.NewServeMux()
	mountDataHandlers(mux, artifactPlane, workspacePlane)
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("mounting the edge beside a local data plane panicked (%v); one prefix must have one owner", rec)
			}
		}()
		if !mountSingleOriginWithPlane(mux, upstream.URL, true, true, quietLogger()) {
			t.Fatal("mountSingleOriginWithPlane returned false for a usable origin")
		}
	}()

	for _, path := range []string{"/v1/workspace/GetWorkspace", "/v1/workspace/Create"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
		if !strings.Contains(rec.Body.String(), "local-data-plane") {
			t.Errorf("POST %s did not reach the local data plane: %d %s", path, rec.Code, rec.Body.String())
		}
	}
	for _, path := range []string{"/v1/artifacts/BeginUpload", "/v1/artifacts/Seal", "/v1/artifacts/Resolve"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`)))
		if !strings.Contains(rec.Body.String(), "local-data-plane") {
			t.Errorf("POST %s did not reach the local artifact plane: %d %s", path, rec.Code, rec.Body.String())
		}
	}
	if got := atomic.LoadInt32(&workspaceHits); got != 2 {
		t.Errorf("local workspace plane got %d requests, want 2", got)
	}
	if got := atomic.LoadInt32(&artifactHits); got != 3 {
		t.Errorf("local artifact plane got %d requests, want 3", got)
	}

	// Space stays the runner's face: the local data plane does not own it, so the
	// edge keeps forwarding it.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/space/GetTurn", strings.NewReader(`{}`)))
	if !strings.Contains(rec.Body.String(), "upstream") {
		t.Errorf("POST /v1/space/GetTurn did not reach the runner: %d %s", rec.Code, rec.Body.String())
	}
	if got := atomic.LoadInt32(&upstreamSpaceHits); got != 1 {
		t.Errorf("runner got %d requests, want the Space call only", got)
	}
}

func TestSingleOriginWithoutLocalDataPlaneStillForwardsWorkspace(t *testing.T) {
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		_, _ = w.Write([]byte(`{"upstream":"` + r.URL.Path + `"}`))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOriginWithPlane(mux, upstream.URL, true, false, quietLogger()) {
		t.Fatal("mountSingleOriginWithPlane returned false for a usable origin")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/workspace/GetWorkspace", strings.NewReader(`{}`)))
	if !strings.Contains(rec.Body.String(), "upstream") {
		t.Errorf("without a local data plane the Workspace prefix must still be forwarded: %d %s", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt32(&upstreamHits) == 0 {
		t.Error("the runner received nothing")
	}
}
