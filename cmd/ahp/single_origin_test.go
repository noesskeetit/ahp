package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aot "github.com/ahp-platform/ahp/pkg/otel"

	"github.com/ahp-platform/ahp/web"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// Everything the proxy is for: the caller's call must arrive unchanged. A
// dropped Authorization would turn every authenticated leg into a 401 from the
// runner; a dropped Range would turn a resume into a full re-download; a
// rewritten query would lose artifact_id/version, which is the exact version
// the download contract requires.
func TestSingleOriginForwardsTheCallersRequest(t *testing.T) {
	var gotMethod, gotPath, gotQuery, gotAuth, gotRange, gotBody string
	var gotHost string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotMethod, gotPath, gotQuery = r.Method, r.URL.Path, r.URL.RawQuery
		gotAuth, gotRange, gotBody = r.Header.Get("Authorization"), r.Header.Get("Range"), string(body)
		gotHost = r.Host
		_, _ = w.Write([]byte(`{"frames":[{"kind":"tool"}]}`))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false for a usable origin")
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/space/ReadStream?thread_id=t1&limit=80", strings.NewReader(`{"thread_id":"t1"}`))
	req.Header.Set("Authorization", "Bearer human-token")
	req.Header.Set("Range", "bytes=0-15")
	req.Host = "stand.example"
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if gotMethod != http.MethodPost {
		t.Errorf("upstream method = %q, want POST", gotMethod)
	}
	if gotPath != "/v1/space/ReadStream" {
		t.Errorf("upstream path = %q, want /v1/space/ReadStream", gotPath)
	}
	if gotQuery != "thread_id=t1&limit=80" {
		t.Errorf("upstream query = %q, want thread_id=t1&limit=80", gotQuery)
	}
	if gotAuth != "Bearer human-token" {
		t.Errorf("upstream Authorization = %q, want the caller's token forwarded verbatim", gotAuth)
	}
	if gotRange != "bytes=0-15" {
		t.Errorf("upstream Range = %q, want bytes=0-15", gotRange)
	}
	if gotBody != `{"thread_id":"t1"}` {
		t.Errorf("upstream body = %q, want the caller's body", gotBody)
	}
	// The browser has exactly one origin. If the edge restamped Host, an
	// absolute URL built upstream would name the runner's loopback port and
	// the user would be sent somewhere that is not their origin.
	if gotHost != "stand.example" {
		t.Errorf("upstream Host = %q, want the caller's Host forwarded", gotHost)
	}
	if rec.Code != http.StatusOK || rec.Body.String() != `{"frames":[{"kind":"tool"}]}` {
		t.Errorf("response = %d %q, want 200 with the upstream body", rec.Code, rec.Body.String())
	}
}

// A 403 must arrive as a 403. If the edge collapsed the runner's refusals, the
// "another user cannot download this" acceptance leg would be measuring the
// proxy instead of the authorization gate - and a 416 without its
// Content-Range would lose the object size the client needs to recompute.
func TestSingleOriginCopiesUpstreamVerdicts(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == singleOriginArtifactsPrefix+"download" &&
			r.Header.Get("Range") == "bytes=100000-100001":
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", "bytes */29")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			_, _ = w.Write([]byte(`{"code":"range_not_satisfiable"}`))
		case r.URL.Path == singleOriginArtifactsPrefix+"download":
			// A real 206 from the runner carries an explicit Content-Length;
			// the edge must hand over the same number, or a resuming client
			// reads a truncated body as a complete one.
			w.Header().Set("Accept-Ranges", "bytes")
			w.Header().Set("Content-Range", "bytes 0-15/29")
			w.Header().Set("Content-Length", "16")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("0123456789abcdef"))
		case r.URL.Path == "/v1/space/GetTurn":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"forbidden"}`))
		default:
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("proxy not mounted")
	}

	for _, tc := range []struct {
		path       string
		rng        string
		wantStatus int
		wantBody   string
		wantCR     string
		wantCL     string
	}{
		{"/u-ahp/v1/artifacts/download?artifact_id=a&version=1", "bytes=0-15",
			http.StatusPartialContent, "0123456789abcdef", "bytes 0-15/29", "16"},
		{"/u-ahp/v1/artifacts/download?artifact_id=a&version=1", "bytes=100000-100001",
			http.StatusRequestedRangeNotSatisfiable, `{"code":"range_not_satisfiable"}`, "bytes */29", ""},
		{"/v1/space/GetTurn", "", http.StatusForbidden, `{"code":"forbidden"}`, "", ""},
	} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		if tc.rng != "" {
			req.Header.Set("Range", tc.rng)
		}
		mux.ServeHTTP(rec, req)
		if rec.Code != tc.wantStatus {
			t.Errorf("%s status = %d, want %d", tc.path, rec.Code, tc.wantStatus)
		}
		if rec.Body.String() != tc.wantBody {
			t.Errorf("%s body = %q, want %q", tc.path, rec.Body.String(), tc.wantBody)
		}
		if tc.wantCR != "" {
			if got := rec.Header().Get("Content-Range"); got != tc.wantCR {
				t.Errorf("%s Content-Range = %q, want %q", tc.path, got, tc.wantCR)
			}
			if got := rec.Header().Get("Accept-Ranges"); got != "bytes" {
				t.Errorf("%s Accept-Ranges = %q, want bytes", tc.path, got)
			}
		}
		if tc.wantCL != "" && rec.Header().Get("Content-Length") != tc.wantCL {
			t.Errorf("%s Content-Length = %q, want %q", tc.path,
				rec.Header().Get("Content-Length"), tc.wantCL)
		}
	}
}

// spa_index is the first probe the acceptance runs against the edge: the SPA
// comes from the runner's embedded assets, and an unknown /v1/... route must
// still be a 404 rather than index.html with a 200.
func TestSingleOriginServesSPAWithoutSwallowingAPIRoutes(t *testing.T) {
	var upstreamHits []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits = append(upstreamHits, r.URL.Path)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte("<!doctype html><title>ahp</title>"))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("proxy not mounted")
	}

	for _, path := range []string{"/", "/space"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<!doctype html") {
			t.Errorf("GET %s = %d %q, want the SPA", path, rec.Code, rec.Body.String())
		}
	}

	// Real UI surfaces (/space, /workspace, /dot, /admin) and any deep link that
	// is not asset-shaped must still receive the app shell, and a real asset path
	// must still be forwarded to the runner that owns the bytes -- the guard is a
	// namespace refusal, never an asset-shape guess.
	for _, path := range []string{"/", "/space", "/workspace", "/dot", "/admin", "/space/t/abc", "/assets/app.js"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<!doctype html") {
			t.Errorf("GET %s = %d %q, want the SPA forwarded from the runner", path, rec.Code, rec.Body.String())
		}
	}

	// Every machine-facing namespace with no handler here must answer as an API,
	// not as index.html. The /u-api/bff-console/... case is the DEF-H24-2
	// regression: the browser leg read a failed BFF call as success because the
	// catch-all served the app shell with 200.
	rejected := []string{
		"/v1/nope", "/healthz", "/readyz", "/debug/stats",
		"/a2a/nope", "/u-ahp/v1/auth/login",
		"/u-api/bff-console/v2/customers/me",
		"/api/definitely-not-a-route", "/.well-known/agent-card.json", "/metrics",
		"/V1/nope", "/A2A/nope",
	}
	for _, path := range rejected {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want a local 404", path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "<!doctype") {
			t.Errorf("GET %s returned the SPA body: an API miss must not look like a page", path)
		}
		if !strings.Contains(rec.Body.String(), `"code":"NOT_FOUND"`) {
			t.Errorf("GET %s body missing canonical code NOT_FOUND: %q", path, rec.Body.String())
		}
	}
	for _, hit := range upstreamHits {
		if spaOnlyRejectsAsAPI(hit) {
			t.Errorf("upstream was called for %q: the catch-all must not forward API routes", hit)
		}
	}
}

// TestSingleOriginRealFrontDoesNotLeakShellForBFFAPI is the DEF-H24-2 regression
// against the REAL front handler rather than a stand-in. The upstream is
// web.Handler() itself -- the exact SPA server the runner mounts at "/". Behind
// that handler, GET /u-api/bff-console/v2/customers/me is UI-shaped to the runner
// (no dot in the last segment, and /u-api/ is not one of the runner's own reserved
// namespaces), so if the edge forwarded it, the browser would read a failed BFF
// call as a 200 with the app shell. The edge must refuse it as an API before it
// ever reaches the front -- while a genuine SPA route still gets the real shell
// forwarded from that same handler.
func TestSingleOriginRealFrontDoesNotLeakShellForBFFAPI(t *testing.T) {
	front := httptest.NewServer(web.Handler())
	defer front.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, front.URL, true, quietLogger()) {
		t.Fatal("proxy not mounted against the real front handler")
	}

	// The exact reported failure path: must be an API 404, never the shell.
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/u-api/bff-console/v2/customers/me", nil))
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET /u-api/bff-console/v2/customers/me = %d %q, want a local 404", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "<!doctype") {
		t.Errorf("the edge leaked the real front's app shell for a BFF API miss: %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"code":"NOT_FOUND"`) {
		t.Errorf("BFF API miss body missing canonical NOT_FOUND envelope: %q", rec.Body.String())
	}

	// A real SPA route still receives the real front's shell (200), proving the
	// guard is a namespace refusal and not a blanket block.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/space", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "<") {
		t.Errorf("GET /space = %d %q, want the real front's app shell", rec.Code, rec.Body.String())
	}
}

// An unreachable runner is 502, not 404: a client that sees 404 for an
// artifact would conclude the object is gone.
func TestSingleOriginReportsUnreachableRunner(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := upstream.URL
	upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, url, true, quietLogger()) {
		t.Fatal("proxy not mounted")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/space/ListTurns", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q is not the JSON error: %v", rec.Body.String(), err)
	}
	if body["code"] != "unavailable" {
		t.Errorf("body code = %q, want unavailable", body["code"])
	}
}

// Every refusal below leaves the edge answering nothing new. Each one was a
// way to get a half-wired origin: a proxy that silently serves no SPA, or that
// prepends a path to every runner route, or that forwards login because the
// identity face was never mounted.
func TestSingleOriginRefusesInsteadOfHalfWiring(t *testing.T) {
	for _, tc := range []struct {
		name           string
		raw            string
		browserMounted bool
	}{
		{"unset", "", true},
		{"no scheme", "127.0.0.1:18082", true},
		{"wrong scheme", "unix:///run/ahp/runner.sock", true},
		{"carries a path", "http://127.0.0.1:18082/api", true},
		{"identity browser face absent", "http://127.0.0.1:18082", false},
	} {
		mux := http.NewServeMux()
		if mountSingleOrigin(mux, tc.raw, tc.browserMounted, quietLogger()) {
			t.Errorf("%s: mountSingleOrigin returned true", tc.name)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/space", nil))
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: GET /space = %d, want the untouched edge 404", tc.name, rec.Code)
		}
	}
}

// The forwarded prefixes are literals here (the untagged edge build must not
// start linking internal/space), so pin them against the packages that
// actually serve them: a route rename on either side has to fail here, not as
// a 404 in front of a user.
func TestSingleOriginRoutesMatchTheSurfaceTheyProxy(t *testing.T) {
	for _, tc := range []struct {
		file  string
		needs string
	}{
		{filepath.Join("..", "..", "internal", "space", "ui_download.go"), `"` + singleOriginArtifactsPrefix + `download"`},
		{filepath.Join("..", "..", "cmd", "as-runner", "main.go"), `"` + singleOriginSpacePrefix + `"`},
		{filepath.Join("..", "..", "cmd", "as-runner", "main.go"), `mux.Handle("/", web.Handler())`},
		{filepath.Join("..", "..", "cmd", "ahp", "role_data.go"), `mux.Handle("` + singleOriginWorkspacePrefix + `", workspaceHandler)`},
	} {
		raw, err := os.ReadFile(tc.file)
		if err != nil {
			t.Fatalf("read %s: %v", tc.file, err)
		}
		if !strings.Contains(string(raw), tc.needs) {
			t.Errorf("%s no longer contains %s: the proxied route moved", tc.file, tc.needs)
		}
	}
}

// The measurement this test exists for (deputy, 04:07Z, one user path, two
// edges): through the old stand edge an AcceptTurn followed by
// GET /v1/space/stream/<thread_id> delivered 400 bytes of events in 5.2 s;
// through this edge the same call returned 0 bytes in 20 s and the socket hung
// until a 25 s client timeout. The non-streaming POST /v1/space/ReadStream
// through the same proxy returned 200/2173 bytes, so routing, auth and status
// copying were fine - only a stream that must reach the browser *while the
// turn is still running* did not.
//
// FlushInterval was not the cause: the proxy already set it to -1, and Go
// forces -1 for text/event-stream anyway (net/http/httputil
// reverseproxy.go flushInterval). The proxy asked for a flush through
// http.NewResponseController(dst), where dst is the ResponseWriter the mux was
// handed - the tracing middleware's *statusRecorder (see aot.HTTPMiddleware in
// serveAll). A wrapper that names neither Flush nor Unwrap hides the real
// Flusher underneath, ResponseController reports ErrNotSupported, and the
// proxy writes the events into a buffer nobody drains: to a browser that is
// indistinguishable from a dead origin.
//
// So this test drives the chain the edge actually serves - the same middleware
// over the same mux - over a real listener, and requires the first event to
// arrive at the client while the upstream is still inside its handler.
func TestSingleOriginStreamsEventStreamThroughTheRealChain(t *testing.T) {
	const firstEvent = "data: {\"kind\":\"tool\",\"seq\":0}\n\n"
	// The upstream stays open well past the first event: that gap is what
	// distinguishes a streaming proxy from a buffering one.
	const upstreamTail = 500 * time.Millisecond

	upstreamDone := new(atomic.Bool)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("upstream ResponseWriter is not a Flusher; this test cannot measure streaming")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, firstEvent)
		flusher.Flush()
		time.Sleep(upstreamTail)
		_, _ = io.WriteString(w, "data: {\"kind\":\"turn\",\"status\":\"done\"}\n\n")
		flusher.Flush()
		upstreamDone.Store(true)
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false for a usable origin")
	}
	edge := httptest.NewServer(aot.HTTPMiddleware("all")(mux))
	defer edge.Close()

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest(http.MethodGet, edge.URL+"/v1/space/stream/thread-1", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	sentAt := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET stream through the edge: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("stream Content-Type = %q, want text/event-stream", ct)
	}

	first := make(chan string, 1)
	go func() {
		buf := make([]byte, len(firstEvent))
		if _, err := io.ReadFull(resp.Body, buf); err != nil {
			first <- ""
			return
		}
		first <- string(buf)
	}()

	select {
	case got := <-first:
		elapsed := time.Since(sentAt)
		if got != firstEvent {
			t.Fatalf("first event = %q, want %q", got, firstEvent)
		}
		if upstreamDone.Load() {
			t.Errorf("first event arrived only after the upstream finished (after %s): the edge buffered the stream", elapsed)
		}
		if elapsed > upstreamTail {
			t.Errorf("first event took %s, longer than the %s the upstream stays open: nothing streamed", elapsed, upstreamTail)
		}
	case <-time.After(upstreamTail + 5*time.Second):
		t.Fatalf("no event bytes within %s while the upstream was mid-stream: the edge buffered text/event-stream", upstreamTail+5*time.Second)
	}

	rest, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the rest of the stream: %v", err)
	}
	if !strings.Contains(string(rest), `"status":"done"`) {
		t.Errorf("stream ended without the final event, got %q", string(rest))
	}
}

// P23: file upload into a project reaches the workspace backend through the
// single user edge. Before this the edge forwarded only Space and artifacts, so
// POST /v1/workspace/Create fell through to the SPA catch-all and came back a
// local 404 ("no route or asset") - exactly what :18381 and :18382 measured on
// the acceptance origin. The workspace prefix must now reach the backend with
// method/query/Authorization/body intact, and a foreign subject's 403 must
// arrive as 403, not be swallowed into a 404 or rewritten.
func TestSingleOriginProxiesWorkspaceUpload(t *testing.T) {
	cases := []struct {
		name       string
		method     string
		path       string
		wantCode   int
		wantBody   string
		wantUpPath string
	}{
		{"owner upload", http.MethodPost, "/v1/workspace/CreateCandidateFiles", http.StatusOK, `{"sha":"abc","size":12}`, "/v1/workspace/CreateCandidateFiles"},
		{"foreign subject", http.MethodPost, "/v1/workspace/Create", http.StatusForbidden, `{"code":"forbidden"}`, "/v1/workspace/Create"},
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/workspace/Create" {
			http.Error(w, `{"code":"forbidden"}`, http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(`{"sha":"abc","size":12}`))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false for a usable origin")
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{"candidate_id":"c1"}`))
			req.Header.Set("Authorization", "Bearer user-token")
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Errorf("edge %s %s = %d, want %d", tc.method, tc.path, rec.Code, tc.wantCode)
			}
			if got := strings.TrimSpace(rec.Body.String()); got != tc.wantBody {
				t.Errorf("edge body = %q, want %q", got, tc.wantBody)
			}
		})
	}
	// The catch-all must still be the one that 404s unknown /v1/... routes;
	// adding the workspace prefix must not have turned it into an open proxy.
	unknown := httptest.NewRecorder()
	mux.ServeHTTP(unknown, httptest.NewRequest(http.MethodGet, "/v1/workspace-not-really", nil))
	if unknown.Code != http.StatusNotFound {
		t.Errorf("GET /v1/workspace-not-really = %d, want a local 404 from the catch-all", unknown.Code)
	}
}

// The edge forwarding a prefix is NOT the same as the edge owning a face behind
// it, and the difference was measured on the flipped acceptance pair at 14:50Z:
// AHP_SINGLE_ORIGIN_UPSTREAM=http://127.0.0.1:18382 (as-runner), which answers
// /v1/space/AcceptTurn 403 and /u-ahp/v1/artifacts/download 403 (it owns both)
// but /v1/workspace/GetHead 404 -- the same 404 it gives for /v1/bogus. The
// Workspace face is mounted on the aggregate (:18380, AHP_ENABLE_DATA=1), not on
// the runner. So this proxy must mirror an upstream that has no such face,
// instead of answering something that makes the edge look like an owner.
func TestSingleOriginMirrorsAnUpstreamThatOwnsNoWorkspaceFace(t *testing.T) {
	var workspaceHits int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/space/AcceptTurn", "/u-ahp/v1/artifacts/download":
			http.Error(w, `{"code":"forbidden"}`, http.StatusForbidden) // the runner owns these
		case "/v1/workspace/Create":
			workspaceHits++ // the runner does not: this is its generic not-found
			http.Error(w, "404 page not found\n", http.StatusNotFound)
		default:
			http.Error(w, "404 page not found\n", http.StatusNotFound)
		}
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false for a usable origin")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/workspace/Create", strings.NewReader(`{"workspace":"proj-1"}`)))
	if rec.Code != http.StatusNotFound {
		t.Errorf("edge /v1/workspace/Create against a faceless runner = %d, want the upstream 404 mirrored, not an edge-invented verdict", rec.Code)
	}
	if workspaceHits != 1 {
		t.Errorf("the edge answered the workspace call locally (upstream hits=%d, want 1): the request must travel", workspaceHits)
	}
}

// And the Workspace face cannot simply be fronted from the aggregate: that face
// is reached over mTLS with a workload certificate, and this proxy has no client
// certificate and no private-CA trust -- it uses the default transport. Measured
// here so the claim stays a measurement: an https upstream is reported as
// unavailable (502, named code) and the request never reaches the origin. Wiring
// the user edge to an mTLS Workspace face needs a decision about which origin
// owns a user-authenticated Workspace face, not a silent transport swap here.
func TestSingleOriginCannotFrontAnMTLSWorkspaceFace(t *testing.T) {
	var reached int
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached++
		_, _ = w.Write([]byte(`{"id":"should-not-happen"}`))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false for a usable https origin")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/workspace/Create", strings.NewReader(`{"workspace":"proj-1"}`)))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("edge against an mTLS-only origin = %d, want 502 (no workload certificate on the proxy)", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "unavailable") {
		t.Errorf("edge body = %q, want the named unavailable code, never a bare 404 the browser would cache", rec.Body.String())
	}
	if reached != 0 {
		t.Errorf("the origin was reached %d times; a proxy that cannot present a client certificate must not deliver the call", reached)
	}
}

// The Workspace edge must be off unless it is fully wired. Half-wiring is the
// dangerous state: an edge that answers /v1/workspace/* while unable to obtain a
// task token would look like an upload face and deliver failures nobody can read.
// These cases pin that a missing piece falls back to plain forwarding, which is
// the behavior measured on the acceptance pair today.
func TestWorkspaceEdgeOnlyMountsWhenFullyWired(t *testing.T) {
	runnerHits := 0
	runner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runnerHits++
		http.Error(w, "404 page not found\n", http.StatusNotFound) // what as-runner answers today
	}))
	defer runner.Close()

	cases := []struct {
		name     string
		env      map[string]string
		wantEdge bool
	}{
		{"off by default", map[string]string{envWorkspaceOrigin: "", envWorkspaceExchange: ""}, false},
		{"origin without an exchange", map[string]string{envWorkspaceOrigin: "http://127.0.0.1:1", envWorkspaceExchange: ""}, false},
		{"https origin without a workload certificate", map[string]string{
			envWorkspaceOrigin: "https://127.0.0.1:1", envWorkspaceExchange: "https://127.0.0.1:1/v1/identity/control/exchange"}, false},
		{"http origin with an exchange", map[string]string{
			envWorkspaceOrigin: "http://127.0.0.1:1", envWorkspaceExchange: "http://127.0.0.1:1/v1/identity/control/exchange"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			mux := http.NewServeMux()
			if !mountSingleOrigin(mux, runner.URL, true, quietLogger()) {
				t.Fatal("mountSingleOrigin returned false")
			}
			before := runnerHits
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/workspace/Create", strings.NewReader(`{"workspace":"p"}`)))
			if tc.wantEdge {
				// The edge owns the call: it must not reach the runner, and it cannot
				// reach a nonexistent exchange either, so the caller gets a named 502.
				if runnerHits != before {
					t.Errorf("the fully wired edge still forwarded to the runner (hits=%d)", runnerHits-before)
				}
				if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), "unavailable") {
					t.Errorf("unreachable exchange = %d %q, want 502 with the named unavailable code", rec.Code, rec.Body.String())
				}
				return
			}
			if runnerHits != before+1 {
				t.Errorf("half-wired edge did not fall back to the runner (hits=%d, want 1): the prefix must keep plain forwarding", runnerHits-before)
			}
			if rec.Code != http.StatusNotFound {
				t.Errorf("fallback code = %d, want the runner's 404 mirrored", rec.Code)
			}
		})
	}
}

// Wired and working, the edge has to behave like a proxy and not like a policy:
// it exchanges the caller's own token, forwards method/path/body untouched, and
// carries the origin's verdict back verbatim. The context it sends is derived per
// subject, because the exchange refuses a context id that belongs to another
// scope (measured 14:46Z on the acceptance deployment).
func TestWorkspaceEdgeExchangesThenForwardsTheCallersRequest(t *testing.T) {
	var gotAuth, gotPath, gotBody string
	type exchangeCall struct {
		human        string
		context      string
		task         string
		capabilities []string
	}
	var calls []exchangeCall
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth, gotPath, gotBody = r.Header.Get("Authorization"), r.URL.Path, readAllForTest(t, r)
		if _, err := w.Write([]byte(`{"id":"ws-1"}`)); err != nil {
			t.Error(err)
		}
	}))
	defer origin.Close()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			HumanToken   string   `json:"human_token"`
			Context      string   `json:"context"`
			Task         string   `json:"task"`
			Capabilities []string `json:"capabilities"`
		}
		if err := json.Unmarshal(mustReadAllBody(t, r), &req); err != nil {
			t.Errorf("exchange body is not JSON: %v", err)
		}
		// The frozen contract: one capability per exchange. A request naming two
		// would build a single merged authority root, which the contract rejects.
		if len(req.Capabilities) != 1 {
			t.Errorf("exchange asked for capabilities %v, want exactly one", req.Capabilities)
		}
		switch req.Task {
		case workspaceExchangeTask:
			if len(req.Capabilities) != 1 || req.Capabilities[0] != "workspace" {
				t.Errorf("%s asked for %v, want [workspace]", req.Task, req.Capabilities)
			}
		case artifactUploadTask:
			if len(req.Capabilities) != 1 || req.Capabilities[0] != artifactUploadCap {
				t.Errorf("%s asked for %v, want [%s]", req.Task, req.Capabilities, artifactUploadCap)
			}
		default:
			t.Errorf("the edge exchanged under an unknown task %q", req.Task)
		}
		if strings.Contains(req.Context, "secret") {
			t.Errorf("the derived context leaked the caller's cookie: %q", req.Context)
		}
		calls = append(calls, exchangeCall{human: req.HumanToken, context: req.Context,
			task: req.Task, capabilities: req.Capabilities})
		_, _ = w.Write([]byte(`{"token":"task-9","task":"` + req.Task + `"}`))
	}))
	defer exchange.Close()

	t.Setenv(envWorkspaceOrigin, origin.URL)
	t.Setenv(envWorkspaceExchange, exchange.URL+"/v1/identity/control/exchange")
	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, "http://127.0.0.1:1", true, quietLogger()) {
		t.Fatal("mountSingleOrigin returned false")
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/workspace/UpdateCandidateFiles?x=1", strings.NewReader(`{"candidate_id":"c1"}`))
	req.Header.Set("Authorization", "Bearer human-A")
	req.Header.Set("Cookie", "session=secret") // must not travel to the aggregate
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"id":"ws-1"`) {
		t.Fatalf("edge = %d %q, want the origin's 200 and body verbatim", rec.Code, rec.Body.String())
	}
	if gotPath != "/v1/workspace/UpdateCandidateFiles" || !strings.Contains(gotBody, `"candidate_id":"c1"`) {
		t.Errorf("origin saw path=%q body=%q: path and body must arrive untouched", gotPath, gotBody)
	}
	if gotAuth != "Bearer task-9" {
		t.Errorf("origin saw Authorization %q, want the exchanged task token", gotAuth)
	}
	if len(calls) != 1 || calls[0].human != "human-A" {
		t.Fatalf("the exchange saw %v, want exactly one call for human-A", calls)
	}
	if !strings.HasPrefix(calls[0].context, "ws-") {
		t.Errorf("the edge sent no derived exchange context: %q", calls[0].context)
	}
	if calls[0].task != workspaceExchangeTask {
		t.Errorf("a workspace call exchanged under task %q, want %q", calls[0].task, workspaceExchangeTask)
	}

	// A second subject must not collide on the context id: the exchange binds a
	// context id to one scope and refuses it from another (measured 14:46Z), so a
	// shared id would make one user's leg fail inside the other user's window.
	second := httptest.NewRequest(http.MethodPost, "/v1/workspace/Create", strings.NewReader(`{"workspace":"p2"}`))
	second.Header.Set("Authorization", "Bearer human-B")
	secondRec := httptest.NewRecorder()
	mux.ServeHTTP(secondRec, second)
	if len(calls) != 2 {
		t.Fatalf("the exchange saw %d calls, want 2", len(calls))
	}
	if calls[1].human != "human-B" {
		t.Errorf("the exchange saw human token %q on the second call, want the caller's own", calls[1].human)
	}
	if calls[1].context == calls[0].context {
		t.Errorf("two subjects were sent the same exchange context %q", calls[1].context)
	}

	// The artifact upload operations travel through the same edge, and nothing else
	// from that face does: the edge takes BeginUpload and Seal for the upload leg
	// and leaves every other artifact route with the runner it already forwards to.
	uploadReq := httptest.NewRequest(http.MethodPost, "/v1/artifacts/BeginUpload", strings.NewReader(`{"name":"hello.txt"}`))
	uploadReq.Header.Set("Authorization", "Bearer human-A")
	uploadRec := httptest.NewRecorder()
	mux.ServeHTTP(uploadRec, uploadReq)
	if gotPath != "/v1/artifacts/BeginUpload" {
		t.Errorf("BeginUpload arrived at the origin as %q, want its own path forwarded untouched", gotPath)
	}
	sealReq := httptest.NewRequest(http.MethodPost, "/v1/artifacts/Seal", strings.NewReader(`{"id":"a1"}`))
	sealReq.Header.Set("Authorization", "Bearer human-A")
	sealRec := httptest.NewRecorder()
	mux.ServeHTTP(sealRec, sealReq)
	if gotPath != "/v1/artifacts/Seal" {
		t.Errorf("Seal arrived at the origin as %q", gotPath)
	}
	// The two boundaries must not share an authority root: distinct task prefixes
	// and a distinct context tag, so revoking the upload boundary leaves workspace
	// authority alone and the contract's rejection of a merged root holds.
	if len(calls) != 4 {
		t.Fatalf("the exchange saw %d calls, want 4", len(calls))
	}
	if calls[3].task != artifactUploadTask {
		t.Errorf("Seal exchanged under task %q, want %q", calls[3].task, artifactUploadTask)
	}
	if calls[3].task == calls[0].task {
		t.Errorf("the artifact boundary reused the workspace task %q: one root for two boundaries", calls[3].task)
	}
	if !strings.HasPrefix(calls[3].context, "art-") {
		t.Errorf("the artifact boundary reused the workspace context %q", calls[3].context)
	}
	for _, c := range calls {
		if len(c.capabilities) != 1 {
			t.Errorf("call under %q carried capabilities %v, want exactly one per boundary", c.task, c.capabilities)
		}
	}
	if uploadRec.Code != http.StatusOK || sealRec.Code != http.StatusOK {
		t.Errorf("artifact upload ops through the edge = %d / %d, want the origin's 200 mirrored", uploadRec.Code, sealRec.Code)
	}
	for i, c := range calls {
		if c.human == "" {
			t.Errorf("call %d carried no human token: %+v", i, c)
		}
		wantPrefix := "ws-"
		if c.task == artifactUploadTask {
			wantPrefix = "art-"
		}
		if !strings.HasPrefix(c.context, wantPrefix) {
			t.Errorf("call %d exchanged under %q with context %q, want the %q prefix", i, c.task, c.context, wantPrefix)
		}
	}
	untouched := httptest.NewRecorder()
	mux.ServeHTTP(untouched, httptest.NewRequest(http.MethodPost, "/v1/artifacts/Export", strings.NewReader(`{}`)))
	if strings.Contains(gotPath, "Export") {
		t.Errorf("the edge took /v1/artifacts/Export: it may forward only the two upload operations")
	}

}

func mustReadAllBody(t *testing.T, r *http.Request) []byte {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

func readAllForTest(t *testing.T, r *http.Request) string {
	t.Helper()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	return string(b)
}

// TestWorkspaceEdgeContextIsStablePerSubjectAcrossTokens pins the property the
// lane deployment disproved at 15:24Z. The exchange reuses one child task per
// (root, effective capability set), so a caller that returns for the same subject
// with a different context is refused: KEY_REUSE_MISMATCH "child task id was
// already used with a different canonical request or parent". Hashing the bearer
// made every fresh login a new context, so a user's SECOND session could not upload
// at all. One context per subject and boundary, whatever the session.
func TestWorkspaceEdgeContextIsStablePerSubjectAcrossTokens(t *testing.T) {
	var contexts []string
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"id":"ok"}`))
	}))
	defer origin.Close()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req exchangeRequest
		if err := json.Unmarshal(mustReadAllBody(t, r), &req); err != nil {
			t.Errorf("exchange body: %v", err)
		}
		contexts = append(contexts, req.Context)
		_, _ = w.Write([]byte(`{"token":"task-1","task":"` + req.Task + `"}`))
	}))
	defer exchange.Close()
	t.Setenv(envWorkspaceOrigin, origin.URL)
	t.Setenv(envWorkspaceExchange, exchange.URL+"/v1/identity/control/exchange")

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, "http://127.0.0.1:1", true, quietLogger()) {
		t.Fatal("the workspace edge did not mount")
	}
	call := func(token string) {
		req := httptest.NewRequest(http.MethodPost, "/v1/workspace/Create", strings.NewReader(`{"workspace":"p"}`))
		req.Header.Set("Authorization", "Bearer "+token)
		mux.ServeHTTP(httptest.NewRecorder(), req)
	}
	// A re-login produces a different signed token for the SAME human.
	sessionOne := fakeHumanToken(t, "asws-lane-tenant", "asws-lane-human-a", "signature-one")
	sessionTwo := fakeHumanToken(t, "asws-lane-tenant", "asws-lane-human-a", "signature-two")
	otherHuman := fakeHumanToken(t, "asws-lane-tenant", "asws-lane-human-b", "signature-one")

	call(sessionOne)
	call(sessionTwo)
	call(otherHuman)
	if len(contexts) != 3 {
		t.Fatalf("the exchange saw %d calls, want 3", len(contexts))
	}
	if contexts[0] != contexts[1] {
		t.Errorf("the same human got two exchange contexts across sessions (%q, %q): the authority tree will refuse the second login",
			contexts[0], contexts[1])
	}
	if contexts[2] == contexts[0] {
		t.Errorf("two humans share the exchange context %q: the exchange binds a context to one scope", contexts[2])
	}

	// A token whose claims cannot be read still gets a distinct, prefixed context
	// rather than an empty one.
	call("not-a-jwt")
	if len(contexts) != 4 || !strings.HasPrefix(contexts[3], "ws-t") {
		t.Errorf("an unreadable token produced %v, want a fourth ws-t context", contexts)
	}
}

// fakeHumanToken builds an unreadable-by-anyone-else payload for the context
// derivation only: the edge never verifies it, and the stub exchange in these tests
// never verifies anything.
func fakeHumanToken(t *testing.T, tenant, subject, signature string) string {
	t.Helper()
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"` + subject + `","tenant":"` + tenant + `"}`))
	return "header." + payload + "." + signature
}

// TestSingleOriginProxiesWorkspaceOperations exists for a measured gap, not a hypothesis: on the
// acceptance contour at 15:01Z the same task token got 200 from the aggregate for
// workspace/Create + CreateCandidate + UpdateCandidateFiles (vm2-as-workspace's asws-acceptance-leg,
// ASWS_ACC_LEG=PASS), while through the browser edge the owner's own GetWorkspace and
// UpdateCandidateFiles answered 404 {"code":"NOT_FOUND","message":"route not found"} - the catch-all.
// Uploading into a project context is part of the user chain, so the edge has to carry it; whoever is
// inside the workload network could always reach it, the user never could.
func TestSingleOriginProxiesWorkspaceOperations(t *testing.T) {
	var mu sync.Mutex
	var hits []string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits = append(hits, r.Method+" "+r.URL.Path)
		authorization := r.Header.Get("Authorization")
		mu.Unlock()
		if authorization == "" {
			t.Error("workspace proxy dropped the Authorization header")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"workspace-1"}`))
	}))
	defer upstream.Close()

	mux := http.NewServeMux()
	if !mountSingleOrigin(mux, upstream.URL, true, quietLogger()) {
		t.Fatal("proxy not mounted")
	}

	for _, op := range []string{"GetWorkspace", "CreateCandidate", "UpdateCandidateFiles"} {
		req := httptest.NewRequest(http.MethodPost, singleOriginWorkspacePrefix+op, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer task-token")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("POST %s%s = %d %q, want the upstream verdict", singleOriginWorkspacePrefix, op,
				rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "workspace-1") {
			t.Errorf("POST %s%s body = %q, want the document the surface returned", singleOriginWorkspacePrefix,
				op, rec.Body.String())
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for _, op := range []string{"GetWorkspace", "CreateCandidate", "UpdateCandidateFiles"} {
		want := http.MethodPost + " " + singleOriginWorkspacePrefix + op
		var seen bool
		for _, hit := range hits {
			if hit == want {
				seen = true
				break
			}
		}
		if !seen {
			t.Errorf("upstream never received %q (hits: %v)", want, hits)
		}
	}
}
