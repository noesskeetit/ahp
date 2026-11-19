//go:build ahp_data

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/pkg/auth"
)

// The mount prefix must be the prefix the tier itself cuts off the request path.
// The edge forwards this literal and the aggregate routes it; if workspaceapi
// ever moves its mount point, this test fails instead of the user discovering an
// HTML 404 where an upload face should be.
func TestWorkspaceMountPrefixPinnedToTheTier(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "workspace", "workspaceapi", "api.go"))
	if err != nil {
		t.Fatalf("read workspaceapi source: %v", err)
	}
	if !strings.Contains(string(raw), `strings.CutPrefix(r.URL.Path, "`+workspaceMountPrefix+`")`) {
		t.Errorf("workspaceapi no longer mounts at %q - update the edge and this pin together", workspaceMountPrefix)
	}
}

// A typo in the opt-in must not half-mount a face that accepts file writes.
func TestWorkspaceRequestedOnlyOnLiteralOne(t *testing.T) {
	for value, want := range map[string]bool{"": false, "0": false, "1": true, "true": false, " 1": false, "yes": false} {
		t.Setenv(envEnableWorkspace, value)
		if got := workspaceRequested(); got != want {
			t.Errorf("AHP_ENABLE_WORKSPACE=%q -> %v, want %v", value, got, want)
		}
	}
}

// Unset opt-in means the process is the old one: the route does not exist, and no
// workspace/artifact migration is attempted on a database that never asked.
func TestTryMountWorkspaceIsANoOpWhenNotRequested(t *testing.T) {
	t.Setenv(envEnableWorkspace, "")
	s := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	mux := http.NewServeMux()
	if err := s.tryMountWorkspace(context.Background(), mux); err != nil {
		t.Fatalf("tryMountWorkspace without opt-in: %v", err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, workspaceMountPrefix+"Create", strings.NewReader("{}")))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unmounted plane answered %d, want 404 (the route must not exist)", rec.Code)
	}
}

// The face refuses to mount on an empty object-store config: an upload surface
// whose bytes would land nowhere is a fabricated readiness, not a feature.
func TestBuildWorkspaceHandlerRefusesWithoutObjectStore(t *testing.T) {
	t.Setenv("AHP_S3_ENDPOINT", "")
	s := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	_, err := s.buildWorkspaceHandler(context.Background())
	if err == nil {
		t.Fatal("mount succeeded with no database pool and no object store")
	}
	if !strings.Contains(err.Error(), "database pool") {
		t.Fatalf("refusal must name the missing dependency, got %v", err)
	}
}

// The refusal order matters for the operator: with a pool but no AHP_S3_ENDPOINT
// the named reason must be the object store, not a generic invalid argument.
func TestBuildWorkspaceHandlerNamesTheMissingObjectStore(t *testing.T) {
	t.Setenv("AHP_S3_ENDPOINT", "")
	s := &server{pool: nil, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, err := s.buildWorkspaceHandler(context.Background()); !strings.Contains(err.Error(), "database pool") {
		t.Fatalf("a nil pool must be reported first, got %v", err)
	}
}

// The upload surface is served by THIS mount, never by the single-origin proxy.
// The measured reason: cmd/as-runner (the runner origin the edge forwards to)
// mounts only /v1/space/, the human download route and the SPA - it owns no
// Workspace face - so a forwarded /v1/workspace/ would come back as the SPA's
// answer from an origin that cannot serve it. Forwarding an unowned prefix is a
// fabricated capability; this pin keeps the edge honest.
// TestWorkspaceIsNotProxiedToTheRunnerOrigin guards one fact: a Workspace call from a
// browser must be answered by the origin that MOUNTS the Workspace face, never by the
// runner, which answers /v1/workspace/GetWorkspace with the same 404 it gives an unknown
// path (measured on the acceptance contour at 20261101T161421Z: :18382 -> 404
// {"code":"NOT_FOUND","message":"no route or asset at /v1/workspace/GetWorkspace"}).
//
// The guard as written at b6b1797 could not pass anywhere: it searched single_origin.go for
// the literal "/v1/workspace/", and that literal is the value of singleOriginWorkspacePrefix,
// declared in the same file. Measured at b6b1797 itself (16:18Z, isolated worktree,
// go test -tags ahp_data,ahp_exec ./cmd/ahp): --- FAIL, so the window head ships this test
// red. A guard that fails for every possible implementation guards nothing, so it now drives
// the behavior it was trying to protect instead of reading source text: with a workspace
// origin wired, the operation must arrive at THAT origin, and the runner must never see it.
func TestWorkspaceIsNotProxiedToTheRunnerOrigin(t *testing.T) {
	runner, err := os.ReadFile(filepath.Join("..", "as-runner", "main.go"))
	if err != nil {
		t.Fatalf("read as-runner/main.go: %v", err)
	}
	if strings.Contains(string(runner), workspaceMountPrefix) {
		t.Skip("the runner origin mounted the Workspace face - move the mount there and re-decide the edge route together with it")
	}

	var runnerHits []string
	runnerUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runnerHits = append(runnerHits, r.URL.Path)
		http.Error(w, "404 page not found\n", http.StatusNotFound)
	}))
	defer runnerUpstream.Close()

	var originHits []string
	pki := newPassthroughPKI(t)
	origin := passthroughMTLSOrigin(t, pki, func(w http.ResponseWriter, r *http.Request) {
		originHits = append(originHits, r.URL.Path)
		_, _ = w.Write([]byte(`{"id":"ws-owned-by-the-data-plane"}`))
	})
	certPath, keyPath, caPath := pki.files(t, pki.leaf(t, "edge", false,
		auth.WorkloadScheme+"/tenant/t1/deployment/asws-edge-lane"))
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Task string `json:"task"`
		}
		if err := json.Unmarshal(mustReadAllBody(t, r), &req); err != nil {
			t.Errorf("exchange body is not JSON: %v", err)
		}
		_, _ = w.Write([]byte(`{"token":"task-9","task":"` + req.Task + `"}`))
	}))
	defer exchange.Close()
	passthroughEnv(t, origin.URL, exchange.URL, certPath, keyPath, caPath, "")

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, runnerUpstream.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false for a usable origin")
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, singleOriginWorkspacePrefix+"GetWorkspace", strings.NewReader(`{"workspace":"proj-1"}`))
	req.Header.Set("Authorization", "Bearer human-token")
	mux.ServeHTTP(rec, req)

	if !strings.Contains(rec.Body.String(), "ws-owned-by-the-data-plane") {
		t.Errorf("edge /v1/workspace/GetWorkspace = %d %q, want the answer of the origin that mounts the face",
			rec.Code, rec.Body.String())
	}
	if len(originHits) != 1 || originHits[0] != singleOriginWorkspacePrefix+"GetWorkspace" {
		t.Errorf("the workspace origin saw %v, want the one operation the browser asked for", originHits)
	}
	for _, hit := range runnerHits {
		if strings.HasPrefix(hit, singleOriginWorkspacePrefix) {
			t.Errorf("the runner upstream received %s: a Workspace call must never be answered by an origin that does not mount the face", hit)
		}
	}
}

// An unowned /v1/... path on the edge must stay a JSON NOT_FOUND - the same
// contract the artifact and compute routes are held to.
func TestWorkspaceRoutesRefuseInJsonNeverHtml(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/", spaOnly(http.NotFoundHandler()))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, workspaceMountPrefix+"NoSuchOperation", strings.NewReader("{}")))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code=%d, want 404", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type=%q, want application/json (an HTML 404 on an API path is a lie of success to every client)", ct)
	}
	if !strings.Contains(rec.Body.String(), `"NOT_FOUND"`) {
		t.Fatalf("body=%q, want the canonical NOT_FOUND code", rec.Body.String())
	}
}
