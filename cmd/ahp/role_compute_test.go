//go:build ahp_exec

package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/compute/computeapi"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// The mount prefix must be the tier's own prefix. The edge forwards this literal
// and the aggregate routes it; if computeapi ever moves its mount point, this
// test fails instead of the user discovering an HTML 404.
func TestComputeMountPrefixPinnedToTheTier(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "compute", "computeapi", "api.go"))
	if err != nil {
		t.Fatalf("read computeapi source: %v", err)
	}
	if !strings.Contains(string(raw), `apiPrefix = "`+computeMountPrefix+`"`) {
		t.Errorf("computeapi no longer mounts at %q - update the edge and this pin together", computeMountPrefix)
	}
}

// A typo in the opt-in must not half-mount a plane that runs commands.
func TestComputeRequestedOnlyOnLiteralOne(t *testing.T) {
	for value, want := range map[string]bool{"": false, "0": false, "1": true, "true": false, " 1": false, "yes": false} {
		t.Setenv(envEnableCompute, value)
		if got := computeRequested(); got != want {
			t.Errorf("AHP_ENABLE_COMPUTE=%q -> %v, want %v", value, got, want)
		}
	}
}

// Unset opt-in means the process is byte-for-byte the old one: no route, and no
// compute migration attempted on a database that never asked for one.
func TestTryMountComputeIsANoOpWhenNotRequested(t *testing.T) {
	t.Setenv(envEnableCompute, "")
	s := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	mux := http.NewServeMux()
	if err := s.tryMountCompute(context.Background(), mux); err != nil {
		t.Fatalf("tryMountCompute without opt-in: %v", err)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, computeMountPrefix+"GetComputer", strings.NewReader("{}")))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unmounted plane answered %d, want 404 (the route must not exist)", rec.Code)
	}
}

// With the opt-in set but no database, mounting must fail loudly. A plane that
// executes commands cannot degrade into "mounted, answers nothing" - that is the
// silent-empty-check class this repo has been bitten by.
func TestBuildComputeHandlerFailsClosedWithoutAPool(t *testing.T) {
	t.Setenv(envEnableCompute, "1")
	s := &server{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if _, err := s.buildComputeHandler(context.Background()); err == nil {
		t.Fatal("buildComputeHandler without a pool returned no error")
	}
}

// The route contract the frontend walks: every Compute answer is JSON with a
// code, an unknown operation is a refusal rather than a success, and a known
// operation without a task token is refused - never 200, never index.html.
func TestComputeRoutesRefuseInJsonNeverHtml(t *testing.T) {
	verifier, _ := devIdentityAuth()
	// denyAll: the HTTP face must refuse on its own gate before any handler runs.
	denyAll := func(context.Context, auth.Claims, string) error {
		return errors.New("this test authorizes nobody")
	}
	handler, err := computeapi.New(compute.New(nil), verifier, denyAll, nil)
	if err != nil {
		t.Fatalf("computeapi.New: %v", err)
	}
	mux := http.NewServeMux()
	mountComputePlane(mux, handler)

	for _, path := range []string{computeMountPrefix + "NoSuchOperation", computeMountPrefix + "GetComputer"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		body := rec.Body.String()
		if rec.Code == http.StatusOK {
			t.Errorf("POST %s answered 200 without a task token: %s", path, body)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
			t.Errorf("POST %s content-type %q, want JSON", path, ct)
		}
		if strings.Contains(body, "<!DOCTYPE") || !strings.Contains(body, `"code"`) {
			t.Errorf("POST %s body is not a JSON refusal: %q", path, body)
		}
	}
}
