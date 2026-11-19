package main

// The digest resolver's wiring lives or dies on two facts this binary owns: the
// frozen browser path is actually routed, and a half-configured resolver refuses to
// start rather than quietly answering as the browser's own identity.

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

func TestBuildResolveProxyFailsClosedOnHalfConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := buildResolveProxy(logger, config{})
	if err != nil || h != nil {
		t.Fatalf("unconfigured resolver = (%v, %v), want (nil, nil) - no route", h, err)
	}
	if _, err := buildResolveProxy(logger, config{ArtifactsURL: "https://artifacts.internal:8083"}); err == nil {
		t.Fatal("AS_ARTIFACTS_URL without AS_WORKLOAD_CERT/KEY/CA must refuse the deployment")
	}
	if _, err := buildResolveProxy(logger, config{
		ArtifactsURL: "https://artifacts.internal:8083",
		WorkloadCert: "/nonexistent/client.crt",
		WorkloadKey:  "/nonexistent/client.key",
		WorkloadCA:   "/nonexistent/ca.crt",
	}); err == nil {
		t.Fatal("an unloadable workload certificate bundle must refuse the route")
	}
}

func TestBuildMuxRoutesTheFrozenResolvePath(t *testing.T) {
	stub := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	download := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	sc := space.NewStateClient("http://127.0.0.1:1", nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, WorkerID: "w-rs"})
	spaceHandler := space.NewHandler(space.HandlerConfig{Coordinator: coord})

	srv := httptest.NewServer(buildMuxWithResolve(spaceHandler, download, stub))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + space.UIResolvePath + "?sha256=0000000000000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("%s reached status %d, want the mounted resolver", space.UIResolvePath, resp.StatusCode)
	}
	if _, err := http.Get(srv.URL + space.UIDownloadPath + "?artifact_id=a&version=1"); err != nil {
		t.Fatal(err)
	}

	// Without a resolver the path must not be routed to anything of ours.
	without := httptest.NewServer(buildMux(spaceHandler, download))
	t.Cleanup(without.Close)
	resp2, err := http.Get(without.URL + space.UIResolvePath + "?sha256=abc")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode == http.StatusTeapot {
		t.Fatal("the resolver leaked into a deployment that configured none")
	}
}
