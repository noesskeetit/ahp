package main

// Routing and wiring of the human download route, at the level this binary owns:
// it must reach the proxy at exactly the frozen path, and it must refuse to mount
// something it cannot authorize.

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// TestBuildDownloadProxyFailsClosedOnHalfConfig: no Artifacts URL is an opt-out (no
// route at all). An Artifacts URL without Space's own workload certificate is a startup
// REFUSAL - the alternative is a proxy whose only way to serve is the browser's own
// credential, which is the defect this route exists to prevent.
func TestBuildDownloadProxyFailsClosedOnHalfConfig(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	h, err := buildDownloadProxy(logger, config{})
	if err != nil || h != nil {
		t.Fatalf("unconfigured download proxy = (%v, %v), want (nil, nil) - no route", h, err)
	}
	if _, err := buildDownloadProxy(logger, config{ArtifactsURL: "https://artifacts.internal:8083"}); err == nil {
		t.Fatal("AS_ARTIFACTS_URL without AS_WORKLOAD_CERT/KEY/CA must refuse the deployment, not weaken it")
	}
	if _, err := buildDownloadProxy(logger, config{
		ArtifactsURL: "https://artifacts.internal:8083",
		WorkloadCert: "/nonexistent/client.crt",
		WorkloadKey:  "/nonexistent/client.key",
		WorkloadCA:   "/nonexistent/ca.crt",
	}); err == nil {
		t.Fatal("an unloadable workload certificate bundle must refuse the route")
	}
}

// TestBuildMuxRoutesTheFrozenDownloadPath: the path as-web codes against is the one
// mounted, and with no proxy configured that path is not routed to any handler of ours.
func TestBuildMuxRoutesTheFrozenDownloadPath(t *testing.T) {
	stub := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	sc := space.NewStateClient("http://127.0.0.1:1", nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, WorkerID: "w-dl"})
	spaceHandler := space.NewHandler(space.HandlerConfig{Coordinator: coord})

	srv := httptest.NewServer(buildMux(spaceHandler, stub))
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + space.UIDownloadPath + "?artifact_id=a&version=1")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("%s reached status %d, want the mounted proxy handler", space.UIDownloadPath, resp.StatusCode)
	}

	without := httptest.NewServer(buildMux(spaceHandler, nil))
	t.Cleanup(without.Close)
	resp2, err := http.Get(without.URL + space.UIDownloadPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp2.Body.Close()
	if resp2.StatusCode == http.StatusTeapot {
		t.Fatal("the download handler leaked into a deployment that configured none")
	}
}
