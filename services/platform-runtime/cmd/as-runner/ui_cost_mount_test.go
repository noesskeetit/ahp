package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// TestBuildMuxRoutesTheFrozenCostPath pins both halves of the mount rule: when a cost
// handler exists the frozen browser path reaches it, and when the deployment has no Model
// Gateway the path is NOT a route of ours -- the accepted surfaces stay exactly as they were.
func TestBuildMuxRoutesTheFrozenCostPath(t *testing.T) {
	costStub := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	download := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	sc := space.NewStateClient("http://127.0.0.1:1", nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, WorkerID: "w-cost"})
	spaceHandler := space.NewHandler(space.HandlerConfig{Coordinator: coord})

	mounted := httptest.NewServer(buildMuxWithCost(spaceHandler, download, nil, costStub))
	t.Cleanup(mounted.Close)
	resp, err := http.Get(mounted.URL + space.UICostPath + "?thread_id=t-1")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTeapot {
		t.Fatalf("%s reached status %d, want the mounted cost handler", space.UICostPath, resp.StatusCode)
	}

	// Unconfigured: the route must not exist at all. This is the byte-identical-defaults
	// claim, checked on the routing the process actually installs.
	off := httptest.NewServer(buildMuxWithCost(spaceHandler, download, nil, nil))
	t.Cleanup(off.Close)
	resp2, err := http.Get(off.URL + space.UICostPath + "?thread_id=t-1")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp2.Body)
	_ = resp2.Body.Close()
	if resp2.StatusCode == http.StatusTeapot {
		t.Fatal("the cost handler ran although the deployment configured no Model Gateway")
	}
	if strings.Contains(string(body), "spent") {
		t.Fatalf("an unconfigured deployment answered with consumption data: %q", body)
	}

	// The older builder still behaves: a cost route it knows nothing about is not ours.
	legacy := httptest.NewServer(buildMuxWithResolve(spaceHandler, download, nil))
	t.Cleanup(legacy.Close)
	resp3, err := http.Get(legacy.URL + space.UICostPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp3.Body.Close()
	if resp3.StatusCode == http.StatusTeapot {
		t.Fatal("buildMuxWithResolve must not mount a cost route")
	}
}

// TestBuildCostProxyIsOffByDefaultAndRefusesAHalfConfiguredSurface: no env, no route; an
// origin without Space's workload identity is a startup refusal rather than a route that
// would have to borrow the browser's credential.
func TestBuildCostProxyIsOffByDefaultAndRefusesAHalfConfiguredSurface(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sc := space.NewStateClient("http://127.0.0.1:1", nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, WorkerID: "w-cost-cfg"})

	h, err := buildCostProxy(logger, config{}, coord)
	if err != nil || h != nil {
		t.Fatalf("unset AS_MODEL_COST_URL must mount nothing, got handler=%v err=%v", h != nil, err)
	}

	if _, err := buildCostProxy(logger, config{ModelCostURL: "https://front.example"}, coord); err == nil {
		t.Fatal("a cost origin without the workload certificate must refuse to start")
	}

	dir := t.TempDir()
	cert, key, ca := dir+"/c.crt", dir+"/c.key", dir+"/ca.crt"
	for _, f := range []string{cert, key, ca} {
		if err := os.WriteFile(f, []byte("not a certificate"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := buildCostProxy(logger, config{
		ModelCostURL: "https://front.example/v1/model", WorkloadCert: cert, WorkloadKey: key, WorkloadCA: ca,
	}, coord); err == nil {
		t.Fatal("an origin carrying the API path must be refused before the server starts")
	}
}
