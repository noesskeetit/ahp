package web

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, string(body)
}

func TestHandlerServesUI(t *testing.T) {
	h := Handler()

	code, body := get(t, h, "/")
	if code != http.StatusOK || !strings.Contains(body, "Agents Space") {
		t.Fatalf("GET / = %d, missing UI shell", code)
	}

	if code, body := get(t, h, "/app.js"); code != http.StatusOK || !strings.Contains(body, "/v1/space/") {
		t.Fatalf("GET /app.js = %d, expected real Space client", code)
	}

	if code, _ := get(t, h, "/style.css"); code != http.StatusOK {
		t.Fatalf("GET /style.css = %d", code)
	}

	// SPA fallback: unknown deep path still returns the app shell.
	if code, body := get(t, h, "/some/deep/route"); code != http.StatusOK || !strings.Contains(body, "Agents Space") {
		t.Fatalf("SPA fallback = %d, expected shell", code)
	}
}

// TestEmbeddedClientMatchesStreamContract locks the shipped UI client to the
// Space API wire shape that tests/space/handler_http_test.go asserts on the
// server: ReadStream returns the {"events":[...]} envelope and each event is
// keyed by "sequence" (not "seq"). If a future edit makes app.js iterate a
// bare array or read ev.seq, the chat stream silently breaks; this test fails
// that edit at build time instead.
func TestEmbeddedClientMatchesStreamContract(t *testing.T) {
	_, body := get(t, Handler(), "/app.js")

	// The client must unwrap the events envelope...
	if !strings.Contains(body, "data.events") {
		t.Fatalf("app.js must unwrap the ReadStream {events:[...]} envelope (data.events)")
	}
	// ...and advance the cursor with ev.sequence, not ev.seq.
	if !strings.Contains(body, "ev.sequence") {
		t.Fatalf("app.js must advance the stream cursor with ev.sequence")
	}
	if strings.Contains(strings.ReplaceAll(body, "ev.sequence", ""), "ev.seq") {
		t.Fatalf("app.js must not read ev.seq; the server field is sequence")
	}
	if !strings.Contains(body, "/v1/space/") {
		t.Fatalf("app.js must talk to the real /v1/space/* API")
	}
}

// TestEmbeddedClientSurfacesArtifacts ensures the browser client renders the
// real artifact metadata + provenance the Space tool stream emits (locked by
// tests/space TestEdgeStreamSurfacesArtifactProvenance), instead of leaving the
// files panel as a static placeholder.
func TestEmbeddedClientSurfacesArtifacts(t *testing.T) {
	_, body := get(t, Handler(), "/app.js")
	for _, want := range []string{"content.artifacts", "content.provenance", "renderArtifacts", "getElementById(\"artifacts\")"} {
		if !strings.Contains(body, want) {
			t.Fatalf("app.js must surface streamed artifacts/provenance (%s)", want)
		}
	}
}

// TestEmbeddedClientExposesTurnAndIsolation locks the Developer/Admin surfacing
// that acceptance calls for: a real GetTurn of the caller's own turn, and a
// two-subject browser flow that exercises the authorization boundary -- the
// client attempts the foreign subject's turn via the real API and relies on the
// server's NOT_FOUND (not FORBIDDEN) answer to prove a private context does not
// leak. It guards against silently regressing these back into static panels.
func TestEmbeddedClientExposesTurnAndIsolation(t *testing.T) {
	_, body := get(t, Handler(), "/app.js")
	for _, want := range []string{
		`call(s, "GetTurn"`, // real GetTurn round-trip
		"isolationProbe",    // cross-subject probe exists
		"NOT_FOUND",         // asserts the not-found isolation contract
		"/v1/space/",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("app.js must surface developer/admin turn + two-subject isolation (%s)", want)
		}
	}
	// The probe must drive the real op against the *other* subject's turn, not a
	// fabricated response: it reads last[other].turnID and calls GetTurn on it.
	if !strings.Contains(body, "theirs.turnID") {
		t.Fatalf("isolation probe must target the other subject's real turn id")
	}

	_, page := get(t, Handler(), "/")
	for _, want := range []string{`id="getturn"`, `id="isoprobe"`} {
		if !strings.Contains(page, want) {
			t.Fatalf("developer/admin UI must expose the turn/isolation control (%s)", want)
		}
	}
}

// getFull issues a GET and returns status, content type and body.
func getFull(t *testing.T, h http.Handler, path string) (int, string, string) {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)

	return rec.Code, rec.Header().Get("Content-Type"), string(body)
}

// TestReservedUnknownPathsAnswerAsAPI locks the boundary the SPA fallback may not cross.
//
// Measured on the live human edge before this guard: GET /api/definitely-not-a-route,
// GET /.well-known/security.txt and GET /robots.txt all returned 200 text/html carrying the
// 2678-byte index.html, while the real agent card is served only on the workload-mTLS edge.
// An external A2A client hitting the well-known path on the human edge therefore received a UI
// shell instead of an answer it could act on, and any smoke that only checks "the path returned
// 200" was structurally unable to fail. Reserved namespaces and asset-shaped paths must answer
// 404 with the contract envelope.
func TestReservedUnknownPathsAnswerAsAPI(t *testing.T) {
	h := Handler()

	for _, path := range []string{
		"/api/definitely-not-a-route",
		"/v1/definitely-not-a-route",
		"/a2a/definitely-not-a-route",
		"/.well-known/agent-card.json",
		"/.well-known/security.txt",
		"/robots.txt",
		"/metrics/nonsense",
		"/assets/missing-bundle.js",
		"/img/logo.png",
	} {
		code, contentType, body := getFull(t, h, path)

		if code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404 (a reserved/asset path must not fall back to the shell)", path, code)
		}

		if !strings.HasPrefix(contentType, "application/json") {
			t.Errorf("GET %s content type = %q, want application/json", path, contentType)
		}

		if !strings.Contains(body, `"code":"NOT_FOUND"`) {
			t.Errorf("GET %s body = %s, want the contract NOT_FOUND envelope", path, body)
		}

		if strings.Contains(body, "Agents Space") {
			t.Errorf("GET %s leaked the UI shell: %s", path, body)
		}
	}
}

// TestUIRoutesStillServeShell is the positive control: the guard must not hide the product.
// Every real client-side route still renders the app shell, and real assets keep their own bytes.
func TestUIRoutesStillServeShell(t *testing.T) {
	h := Handler()

	for _, path := range []string{"/", "/space", "/workspace", "/dot", "/admin", "/some/deep/route"} {
		code, contentType, body := getFull(t, h, path)

		if code != http.StatusOK || !strings.Contains(body, "Agents Space") {
			t.Errorf("GET %s = %d, want the UI shell (content type %q)", path, code, contentType)
		}

		if !strings.Contains(contentType, "text/html") {
			t.Errorf("GET %s content type = %q, want text/html for a UI route", path, contentType)
		}
	}

	if code, contentType, body := getFull(t, h, "/app.js"); code != http.StatusOK ||
		!strings.Contains(contentType, "javascript") || !strings.Contains(body, "/v1/space/") {
		t.Errorf("GET /app.js = %d %s, want the real JS client", code, contentType)
	}

	if code, contentType, _ := getFull(t, h, "/style.css"); code != http.StatusOK || !strings.Contains(contentType, "text/css") {
		t.Errorf("GET /style.css = %d %s, want the real stylesheet", code, contentType)
	}
}

// TestNonGETNeverGetsTheShell: a POST to a UI route is not a page view. Answering it with the
// shell would report "the endpoint accepted my call" to a client that never hit an endpoint.
func TestNonGETNeverGetsTheShell(t *testing.T) {
	h := Handler()

	for _, path := range []string{"/space", "/some/deep/route"} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		body, _ := io.ReadAll(rec.Body)

		if rec.Code == http.StatusOK || strings.Contains(string(body), "Agents Space") {
			t.Errorf("POST %s = %d serving the shell, want a machine 404 (body %s)", path, rec.Code, body)
		}

		if rec.Code != http.StatusNotFound || !strings.Contains(string(body), `"code":"NOT_FOUND"`) {
			t.Errorf("POST %s = %d %s, want 404 NOT_FOUND", path, rec.Code, body)
		}
	}
}

// TestReservedNamespacesAreCaseInsensitiveAndCoverAuthPrefix measures the two extensions of the
// reserved list against the boundary they were added for. Both were observed as live false-200s on
// the human edge: /API/<x> and /V1/<x> reach the catch-all the same way their lowercase twins do
// (Go routing is case-sensitive, so neither is routable), and POST /u-ahp/v1/auth/login — an auth
// path with no handler on this deployment — answered 200 with the shell while the identity edge
// answers the same path 403 application/json. A smoke that treats "answered 200" as liveness must
// not be able to believe any of them.
func TestReservedNamespacesAreCaseInsensitiveAndCoverAuthPrefix(t *testing.T) {
	h := Handler()

	for _, path := range []string{
		"/API/definitely-not-a-route",
		"/V1/definitely-not-a-route",
		"/A2A/definitely-not-a-route",
		"/.WELL-KNOWN/agent-card.json",
		"/U-AHP/v1/auth/login",
		"/u-ahp/v1/auth/login",
	} {
		code, contentType, body := getFull(t, h, path)

		if code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404 — the reserved check must not be case-tripped", path, code)
		}

		if !strings.HasPrefix(contentType, "application/json") || !strings.Contains(body, `"code":"NOT_FOUND"`) {
			t.Errorf("GET %s = %s %s, want the contract NOT_FOUND envelope", path, contentType, body)
		}
	}

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/u-ahp/v1/auth/login", nil))

	if rec.Code == http.StatusOK || strings.Contains(rec.Body.String(), "<!doctype") {
		t.Errorf("POST /u-ahp/v1/auth/login = %d serving %s, want a machine refusal", rec.Code, rec.Body.String())
	}

	// The extensions must not reach into UI territory: a capitalised route is still a page view.
	if code, _, body := getFull(t, h, "/Space"); code != http.StatusOK || !strings.Contains(body, "Agents Space") {
		t.Errorf("GET /Space = %d, want the shell — the guard may only refuse machine namespaces", code)
	}
}

// TestDebugNamespaceIsNotAPage pins the /debug extension. The measurement that produced it: GET
// /debug/pprof/ on the runner edge answered 200 text/html with the 4623-byte app shell, because an
// empty last segment is not asset-shaped and /debug was not a reserved namespace. An unmounted
// profiler endpoint has to answer like an API, and it must do so under every spelling a caller can
// reach the catch-all with.
func TestDebugNamespaceIsNotAPage(t *testing.T) {
	h := Handler()

	for _, path := range []string{
		"/debug/pprof/",
		"/debug/pprof",
		"/debug/pprof/heap",
		"/debug/vars",
		"/DEBUG/PPROF/",
	} {
		code, contentType, body := getFull(t, h, path)

		if code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404 — an unmounted debug endpoint is a missing route, not a page", path, code)
		}

		if !strings.HasPrefix(contentType, "application/json") {
			t.Errorf("GET %s content type = %q, want application/json", path, contentType)
		}

		if !strings.Contains(body, `"code":"NOT_FOUND"`) {
			t.Errorf("GET %s body = %s, want the contract NOT_FOUND envelope", path, body)
		}

		if strings.Contains(body, "Agents Space") {
			t.Errorf("GET %s leaked the UI shell: %s", path, body)
		}
	}

	// The extension may only refuse machine namespaces: the four product surfaces stay reachable.
	for _, path := range []string{"/space", "/workspace", "/dot", "/admin"} {
		if code, _, body := getFull(t, h, path); code != http.StatusOK || !strings.Contains(body, "Agents Space") {
			t.Errorf("GET %s = %d, want the shell — /debug must not reach into UI territory", path, code)
		}
	}
}

// TestDiskServedUIKeepsTheContract covers the deployment override (AHP_WEB_STATIC_DIR) with the
// same table that guards the embedded UI. A deployment knob is exactly where a policy tends to be
// skipped: if the disk backend served the shell for /.well-known/agent-card.json, a stand pointing
// at the real built UI would reintroduce the false 200 that started this whole contract - and it
// would do so only in production, where the embedded tests cannot see it.
func TestDiskServedUIKeepsTheContract(t *testing.T) {
	dir := t.TempDir()

	writeUI := func(rel, body string) {
		t.Helper()
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", rel, err)
		}
		if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}

	writeUI("index.html", "<html><head><title>Agents Space</title></head><body>disk shell</body></html>")
	writeUI(filepath.Join("assets", "app.js"), "console.log('built bundle')")

	t.Setenv(StaticDirEnv, dir)
	h := Handler()

	for _, path := range []string{
		"/api/definitely-not-a-route",
		"/v1/definitely-not-a-route",
		"/.well-known/agent-card.json",
		"/robots.txt",
		"/assets/missing-bundle.js",
		"/debug/pprof/",
		"/debug/pprof/heap",
	} {
		code, contentType, body := getFull(t, h, path)

		if code != http.StatusNotFound {
			t.Errorf("disk UI GET %s = %d, want 404", path, code)
		}

		if !strings.HasPrefix(contentType, "application/json") {
			t.Errorf("disk UI GET %s content type = %q, want application/json", path, contentType)
		}

		if !strings.Contains(body, `"code":"NOT_FOUND"`) {
			t.Errorf("disk UI GET %s body = %s, want the contract NOT_FOUND envelope", path, body)
		}
	}

	if code, _, body := getFull(t, h, "/space"); code != http.StatusOK || !strings.Contains(body, "disk shell") {
		t.Errorf("disk UI GET /space = %d, body %q, want the disk-served shell (client-side routing must survive the knob)", code, body)
	}

	if code, _, body := getFull(t, h, "/assets/app.js"); code != http.StatusOK || !strings.Contains(body, "built bundle") {
		t.Errorf("disk UI GET /assets/app.js = %d, body %q, want the real asset bytes", code, body)
	}
}

// TestStaticDirWithoutIndexFailsLoudly pins the fail-closed half of the knob: a configured
// directory that is not a UI must stop the process, not be quietly replaced by the embedded stub.
// A stub silently standing in for the product is the false impression this package exists to remove.
func TestStaticDirWithoutIndexFailsLoudly(t *testing.T) {
	t.Setenv(StaticDirEnv, t.TempDir())

	defer func() {
		if recover() == nil {
			t.Error("Handler() accepted an empty AHP_WEB_STATIC_DIR; it must fail closed rather than serve the embedded stub")
		}
	}()

	_ = Handler()
}
