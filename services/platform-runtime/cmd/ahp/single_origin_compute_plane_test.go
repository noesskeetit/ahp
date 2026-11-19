//go:build ahp_exec

package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// DEF-COMPUTE-1. The composition the acceptance stand runs -- one binary that is
// the human origin AND the execution+compute plane (AHP_ROLE=all with
// AHP_ENABLE_COMPUTE=1, AHP_ENABLE_EXECUTION=1, AHP_ENABLE_BROWSER=1 and
// AHP_SINGLE_ORIGIN_UPSTREAM set) -- panicked at startup:
//
//	panic: pattern "/v1/compute/" (registered at cmd/ahp/single_origin.go:178)
//	       conflicts with pattern "/v1/compute/"
//
// One optional plane therefore took the WHOLE canonical front down, which is the
// same defect class the director already fixed for the data plane (DEF-H24-13,
// single_origin_data_plane_test.go): the edge claims a prefix this process already
// serves. The two answers this file pins are the ones an operator can act on:
// who owns the route, and the fact that a plane that cannot mount must not
// remove the planes that could.

func TestSingleOriginWithLocalComputePlaneServesLocallyAndDoesNotPanic(t *testing.T) {
	var computeHits int32
	computePlane := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&computeHits, 1)
		_, _ = w.Write([]byte(`{"owner":"local-compute-plane"}`))
	})
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		_, _ = w.Write([]byte(`{"upstream":"` + r.URL.Path + `"}`))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	mountComputePlane(mux, computePlane)
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("mounting the edge beside a local compute plane panicked (%v); one prefix must have one owner", rec)
			}
		}()
		if !mountSingleOriginEdge(mux, upstream.URL, true, false, true, quietLogger()) {
			t.Fatal("mountSingleOriginEdge returned false for a usable origin")
		}
	}()

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/compute/Run", strings.NewReader(`{}`)))
	if !strings.Contains(rec.Body.String(), "local-compute-plane") {
		t.Fatalf("POST /v1/compute/Run did not reach the local compute plane: %d %s", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt32(&computeHits) != 1 {
		t.Fatalf("local compute plane hits = %d, want 1", computeHits)
	}
	// The rest of the edge is untouched: Space still belongs to the runner.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/space/AcceptTurn", strings.NewReader(`{}`)))
	if !strings.Contains(rec.Body.String(), "upstream") {
		t.Fatalf("POST /v1/space/AcceptTurn was not forwarded to the runner: %d %s", rec.Code, rec.Body.String())
	}
}

// The other half: an operator who did NOT enable compute here must still get the
// forwarding behaviour that was measured and accepted before this fix. Nothing is
// weakened -- the prefix simply follows whoever owns the store.
func TestSingleOriginStillForwardsComputeWhenItIsNotLocal(t *testing.T) {
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
		_, _ = w.Write([]byte(`{"upstream":"` + r.URL.Path + `"}`))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOriginEdge(mux, upstream.URL, true, false, false, quietLogger()) {
		t.Fatal("mountSingleOriginEdge returned false for a usable origin")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/compute/Run", strings.NewReader(`{}`)))
	if !strings.Contains(rec.Body.String(), "upstream") {
		t.Fatalf("compute was not forwarded although this process does not serve it: %d %s", rec.Code, rec.Body.String())
	}
	if atomic.LoadInt32(&upstreamHits) == 0 {
		t.Fatal("the runner saw no compute request")
	}
}

// A plane that cannot mount must not take the others with it. Here compute is
// asked for with no database at all: the answer must be a warning and a missing
// plane, not a failed startup and not an execution plane that vanished.
func TestComputeMountFailureDoesNotRemoveOtherPlanes(t *testing.T) {
	t.Setenv(envEnableCompute, "1")
	var logs bytes.Buffer
	s := &server{logger: slog.New(slog.NewTextHandler(&logs, nil))} // pool == nil on purpose

	execPlane := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"plane":"execution"}`))
	})
	mux := http.NewServeMux()
	mux.Handle("/v1/tasks/", execPlane)

	if err := s.tryMountCompute(context.Background(), mux); err != nil {
		t.Fatalf("a failed compute mount must degrade, not abort the composition: %v", err)
	}
	if s.computePlaneMounted {
		t.Fatal("computePlaneMounted must stay false when the plane did not mount")
	}
	if !strings.Contains(logs.String(), "compute") || !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("the operator must be told the plane is absent: %s", logs.String())
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/tasks/", nil))
	if !strings.Contains(rec.Body.String(), "execution") {
		t.Fatalf("the execution plane disappeared with the failed compute mount: %d %s", rec.Code, rec.Body.String())
	}

	// And the absent plane must be VISIBLE as absent. Forwarding the prefix after a
	// failed mount would show the caller somebody else's 404 (or a runner's plane)
	// where this process was asked and could not deliver, so the contract here is an
	// explicit 503 the UI can render as "unavailable" -- never a success, never a
	// silently inherited answer.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, computeMountPrefix+"AcquireIncarnation", strings.NewReader(`{}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("a requested-but-unmountable compute plane answered %d, want 503: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"unavailable"`) {
		t.Fatalf("the refusal carries no machine-readable reason: %s", rec.Body.String())
	}
	if got := rec.Header().Get(headerBackend); got != "unavailable" {
		t.Fatalf("%s = %q, want the literal %q so the caller can see the plane state without parsing the body", headerBackend, got, "unavailable")
	}
	if strings.Contains(logs.String(), "password") || strings.Contains(logs.String(), "postgres://") {
		t.Fatalf("the mount failure leaked connection detail into the log: %s", logs.String())
	}
}

// The rule the whole fix rests on, pinned where it can only ever cost a test run:
// one prefix, one owner. http.ServeMux enforces it by panicking, and DEF-COMPUTE-1
// is what that enforcement looks like when it is discovered at stand startup
// instead of here.
func TestOnlyOneOwnerOfTheComputePrefix(t *testing.T) {
	plane := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})

	// (a) The enforcer still fires: a second owner of the prefix must panic. If this
	// ever stops panicking, the ownership logic below is protecting nothing and this
	// test is the notice to say so.
	func() {
		defer func() {
			if recover() == nil {
				t.Error("http.ServeMux accepted two owners of /v1/compute/; the ownership rule this fix implements is no longer enforced by the mux")
			}
		}()
		two := http.NewServeMux()
		mountComputePlane(two, plane)
		mountComputePlane(two, plane)
	}()

	// (b) The composition the stand runs -- local plane plus edge -- registers the
	// prefix exactly once, which is what (a) would have punished.
	var upstreamHits int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&upstreamHits, 1)
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	mountComputePlane(mux, plane)
	if !mountSingleOriginEdge(mux, upstream.URL, true, false, true, quietLogger()) {
		t.Fatal("mountSingleOriginEdge returned false for a usable origin")
	}
	// The edge's catch-all for the SPA is registered by the same call, so reaching
	// this point at all already proves no duplicate pattern was added. What is left
	// to pin is that the local plane, not the proxy, owns the route: an upstream that
	// is never contacted is the out-of-band witness.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, computeMountPrefix+"Sleep", strings.NewReader(`{}`)))
	if atomic.LoadInt32(&upstreamHits) != 0 {
		t.Fatalf("the edge forwarded a compute request although this process owns the prefix (%d upstream hits)", upstreamHits)
	}
}
