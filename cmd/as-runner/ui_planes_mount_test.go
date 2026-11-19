package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// planeSpaceHandler builds the Space handler the production mux mounts, so the routing a
// test asserts is the routing the process installs.
func planeSpaceHandler() http.Handler {
	sc := space.NewStateClient("http://127.0.0.1:1", nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, WorkerID: "w-planes"})
	return space.NewHandler(space.HandlerConfig{Coordinator: coord})
}

// exchangedAs stands in for ExchangeMiddleware: the one place a scoped task token enters the
// request context. Nothing in a test or in production carries the browser bearer onward.
func exchangedAs(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(space.WithScopedTask(r.Context(), token)))
	})
}

// TestHumanPlaneRoutesAreMountedOnlyWhenConfigured pins both halves of the mount rule for
// all four surfaces: with an origin configured the frozen browser path reaches the plane,
// and with none the path is not a route of ours -- the accepted surfaces answer exactly as
// they did before, as an API-shaped 404 rather than as success.
func TestHumanPlaneRoutesAreMountedOnlyWhenConfigured(t *testing.T) {
	var seen []string
	plane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer plane.Close()

	specs := space.PlanePipes()
	mounted := map[string]http.Handler{}
	for _, spec := range specs {
		h, err := space.NewPlaneProxy(space.PlaneProxyConfig{Spec: spec, Origin: plane.URL, Client: plane.Client()})
		if err != nil {
			t.Fatalf("NewPlaneProxy(%s): %v", spec.RoutePrefix, err)
		}
		mounted[spec.RoutePrefix] = exchangedAs("task-m1", h)
	}

	on := httptest.NewServer(buildMuxWithPlanes(planeSpaceHandler(), nil, nil, nil, mounted))
	defer on.Close()
	off := httptest.NewServer(buildMuxWithPlanes(planeSpaceHandler(), nil, nil, nil, nil))
	defer off.Close()

	for _, spec := range specs {
		op := spec.AllowedOps[0]
		req, err := http.NewRequest(http.MethodPost, on.URL+spec.RoutePrefix+op, strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), "ok") {
			t.Fatalf("mounted %s%s: status=%d body=%s", spec.RoutePrefix, op, resp.StatusCode, body)
		}
		if !strings.Contains(seen[len(seen)-1], spec.MachinePrefix+op) {
			t.Fatalf("plane saw %s for %s, expected %s%s", seen[len(seen)-1], spec.RoutePrefix, spec.MachinePrefix, op)
		}

		// The same call against an unconfigured deployment must not reach any plane.
		req2, err := http.NewRequest(http.MethodPost, off.URL+spec.RoutePrefix+op, strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		req2.Header.Set("Content-Type", "application/json")
		resp2, err := http.DefaultClient.Do(req2)
		if err != nil {
			t.Fatal(err)
		}
		body2, _ := io.ReadAll(resp2.Body)
		_ = resp2.Body.Close()
		if resp2.StatusCode == http.StatusOK || strings.Contains(string(body2), "\"ok\"") {
			t.Fatalf("unconfigured %s%s answered %d %s: the default changed", spec.RoutePrefix, op, resp2.StatusCode, body2)
		}
	}
}

// TestHumanPlaneSurfaceNamesTheTaskTokenItExchanged is the GREEN leg through the real mount:
// what the plane receives is the exchanged task token, never the browser bearer.
func TestHumanPlaneSurfaceNamesTheTaskTokenItExchanged(t *testing.T) {
	var auth string
	plane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"deployments":[]}`)
	}))
	defer plane.Close()

	h, err := space.NewPlaneProxy(space.PlaneProxyConfig{
		Spec: space.RegistryPlane, Origin: plane.URL, Client: plane.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := buildMuxWithPlanes(planeSpaceHandler(), nil, nil, nil,
		map[string]http.Handler{space.UIRegistryPath: exchangedAs("task-exchanged-77", h)})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+space.UIRegistryPath+"CatalogList", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer browser-human-jwt")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if auth != "Bearer task-exchanged-77" {
		t.Fatalf("plane was addressed with %q, want the exchanged task token", auth)
	}
	if strings.Contains(auth, "browser-human-jwt") {
		t.Fatal("the browser credential reached the machine plane")
	}
}

// TestBuildPlaneProxiesIsOffByDefaultAndRefusesAHalfConfiguredSurface: no env, no route; an
// origin without Space's workload identity is a startup refusal, not a route that would have
// to borrow the browser's credential.
func TestBuildPlaneProxiesIsOffByDefaultAndRefusesAHalfConfiguredSurface(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	handlers, err := buildPlaneProxies(logger, config{})
	if err != nil || handlers != nil {
		t.Fatalf("no origin variables must mount nothing, got handlers=%v err=%v", handlers, err)
	}
	if _, err := buildPlaneProxies(logger, config{EvalsURL: "https://front.example"}); err == nil {
		t.Fatal("a plane origin without the workload certificate must refuse to start")
	}

	dir := t.TempDir()
	cert, key, ca := dir+"/c.crt", dir+"/c.key", dir+"/ca.crt"
	for _, f := range []string{cert, key, ca} {
		if err := os.WriteFile(f, []byte("not a certificate"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := buildPlaneProxies(logger, config{
		RegistryURL: "https://front.example/v1/registry", MemoryURL: "https://front.example",
		WorkloadCert: cert, WorkloadKey: key, WorkloadCA: ca,
	}); err == nil {
		t.Fatal("a half-configured plane surface must refuse to start instead of mounting a route that cannot answer")
	}
}

// contextUsage keeps the context import meaningful without a live exchange in this file.
var _ = context.Background
