package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestSingleOriginForwardsTheHumanPlaneSurfaces: the browser speaks to ONE origin, so the
// edge has to hand the four human plane surfaces to the runner that exchanges the session
// for a task token. A path inside /u-ahp/v1/ that nobody owns must still be answered as an
// API 404 here, never forwarded as success and never dressed up as the SPA shell.
func TestSingleOriginForwardsTheHumanPlaneSurfaces(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"upstream":"`+r.URL.Path+`"}`)
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("proxy not mounted")
	}

	for _, path := range []string{
		"/u-ahp/v1/profile/GetProfile",
		"/u-ahp/v1/profile/PutProfile",
		"/u-ahp/v1/profile/ListPersonalSkills",
		"/u-ahp/v1/registry/CatalogList",
		"/u-ahp/v1/evals/Verdict",
		"/u-ahp/v1/triggers/List",
		"/u-ahp/v1/memory/Search",
		"/u-ahp/v1/model/Cost?thread_id=turn-1",
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		req.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(rec, req)
		want, _, _ := strings.Cut(path, "?")
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("%s through the edge: status=%d body=%s", path, rec.Code, rec.Body.String())
		}
	}

	// A human surface nobody mounted stays an honest API 404 on this origin.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/u-ahp/v1/notaplane/Get", strings.NewReader("{}")))
	if rec.Code != http.StatusNotFound || strings.Contains(rec.Body.String(), "<!doctype") {
		t.Fatalf("unowned human path answered %d %q: expected the API 404, not a forward or the shell",
			rec.Code, rec.Body.String())
	}
}

// TestSingleOriginPlanePrefixesMatchTheSurfaceThatServesThem keeps the edge literals pinned
// against the runner-side implementation, the way the other forwarded prefixes are: a rename
// on either side has to fail a build here, not as a 404 in front of a person.
func TestSingleOriginPlanePrefixesMatchTheSurfaceThatServesThem(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "internal", "space", "ui_planes.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, prefix := range singleOriginPlanePrefixList() { // cost prefix pinned separately below
		if !strings.Contains(source, `"`+prefix+`"`) {
			t.Errorf("internal/space/ui_planes.go no longer serves %s: the edge forwards a dead route", prefix)
		}
	}
	cost, err := os.ReadFile(filepath.Join("..", "..", "internal", "space", "ui_cost.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cost), `UICostPath = "`+singleOriginCostPrefix+`Cost"`) {
		t.Errorf("internal/space/ui_cost.go no longer serves %sCost: the edge forwards a dead route", singleOriginCostPrefix)
	}
	mounted, err := os.ReadFile(filepath.Join("..", "as-runner", "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mounted), "buildMuxWithPlanes(") {
		t.Error("cmd/as-runner/main.go no longer mounts the human plane surfaces the edge forwards")
	}
}
