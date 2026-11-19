package space

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/transport"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// planeUpstream records exactly what the machine face saw. The assertions that matter are
// about that request: it is where a human credential could leak or a browser could name a
// scope it was never given.
type planeUpstream struct {
	calls   int
	path    string
	method  string
	auth    string
	body    string
	status  int
	payload string
}

func (u *planeUpstream) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u.calls++
		u.path = r.URL.Path
		u.method = r.Method
		u.auth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		u.body = string(b)
		if u.payload != "" {
			w.Header().Set("Content-Type", "application/json")
		}
		if u.status != 0 {
			w.WriteHeader(u.status)
		}
		if u.payload != "" {
			_, _ = io.WriteString(w, u.payload)
		}
	}
}

func planeHandlerFor(t *testing.T, spec PlaneSpec, upstream *httptest.Server) http.Handler {
	t.Helper()
	h, err := NewPlaneProxy(PlaneProxyConfig{Spec: spec, Origin: upstream.URL, Client: upstream.Client()})
	if err != nil {
		t.Fatalf("NewPlaneProxy(%s): %v", spec.RoutePrefix, err)
	}
	return h
}

// doPlaneRequest drives the surface the way the middleware-composed process does: an
// exchanged task token in the context, the human bearer nowhere near the pipe.
func doPlaneRequest(t *testing.T, h http.Handler, method, target, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	ctx := context.Background()
	if token != "" {
		ctx = WithScopedTask(ctx, token)
	}
	req := httptest.NewRequest(method, target, strings.NewReader(body)).WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer human.jwt.must.not.pass")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestPlaneSurfaceForwardsOnlyTheExchangedToken is the GREEN leg: the plane receives the
// task token this process exchanged, the operation it names, and the body untouched.
func TestPlaneSurfaceForwardsOnlyTheExchangedToken(t *testing.T) {
	up := &planeUpstream{payload: `{"deployments":[{"id":"d-1"}]}`}
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := doPlaneRequest(t, planeHandlerFor(t, RegistryPlane, srv), http.MethodPost,
		RegistryPlane.RoutePrefix+"CatalogList", `{"page":1}`, "task-token-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if up.calls != 1 || up.path != "/v1/registry/CatalogList" || up.method != http.MethodPost {
		t.Fatalf("plane saw calls=%d path=%q method=%s", up.calls, up.path, up.method)
	}
	if up.auth != "Bearer task-token-1" {
		t.Fatal("the plane must be addressed with the EXCHANGED task token")
	}
	if strings.Contains(up.auth, "human.jwt") {
		t.Fatal("the human JWT reached the machine face")
	}
	if up.body != `{"page":1}` {
		t.Fatalf("body rewritten in transit: %q", up.body)
	}
	if got := rec.Body.String(); got != up.payload {
		t.Fatalf("plane answer was reformatted: %q", got)
	}
	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("a human surface must not be cacheable")
	}
}

func TestRegistryConfigurationSurfaceForwardsScopedRequests(t *testing.T) {
	for _, operation := range []string{"GetConfig", "ResolveConfig"} {
		t.Run(operation, func(t *testing.T) {
			up := &planeUpstream{payload: `{"config":{"enabled":true}}`}
			srv := httptest.NewTLSServer(up.handler())
			defer srv.Close()
			body := `{"installation_id":"installation-one","key":"configuration-key","config":{"enabled":true}}`
			rec := doPlaneRequest(t, planeHandlerFor(t, RegistryPlane, srv), http.MethodPost,
				RegistryPlane.RoutePrefix+operation, body, "current-scoped-task")
			if rec.Code != http.StatusOK || up.calls != 1 || up.path != "/v1/registry/"+operation || up.body != body || up.auth != "Bearer current-scoped-task" {
				t.Fatalf("configuration request was not forwarded with scoped authority: status=%d path=%q calls=%d", rec.Code, up.path, up.calls)
			}
			if rec.Body.String() != up.payload {
				t.Fatal("configuration response changed in transit")
			}
		})
	}
}

// TestPlaneSurfaceNeedsAnExchangedToken is the RED leg for authority: with no exchanged
// task token the surface refuses before the plane is touched at all.
func TestPlaneSurfaceNeedsAnExchangedToken(t *testing.T) {
	up := &planeUpstream{}
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := doPlaneRequest(t, planeHandlerFor(t, MemoryPlane, srv), http.MethodPost,
		MemoryPlane.RoutePrefix+"Search", `{"q":"x"}`, "")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d want 403", rec.Code)
	}
	if up.calls != 0 {
		t.Fatal("the plane was asked even though no task token was exchanged")
	}
	var problem map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &problem); err != nil || problem["code"] != "FORBIDDEN" {
		t.Fatalf("refusal is not the product envelope: %s", rec.Body.String())
	}
}

// TestPlaneSurfaceUnknownOperationIsNotFound pins the surface, not the plane: an operation
// outside the explicit screen allowlist (registry DropAll,
// triggers CreateWebhook) is a 404 HERE, and the plane is never asked.
func TestPlaneSurfaceUnknownOperationIsNotFound(t *testing.T) {
	up := &planeUpstream{}
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	reg := planeHandlerFor(t, RegistryPlane, srv)
	trig := planeHandlerFor(t, TriggersPlane, srv)
	mem := planeHandlerFor(t, MemoryPlane, srv)

	for _, c := range []struct {
		h      http.Handler
		method string
		target string
	}{
		{reg, http.MethodPost, RegistryPlane.RoutePrefix + "DropAll"},
		{reg, http.MethodPost, RegistryPlane.RoutePrefix + "NoSuchOperation"},
		{reg, http.MethodPost, RegistryPlane.RoutePrefix + "CatalogList/extra"},
		{reg, http.MethodGet, RegistryPlane.RoutePrefix + "CatalogList"},
		{reg, http.MethodPost, RegistryPlane.RoutePrefix},
		{trig, http.MethodPost, TriggersPlane.RoutePrefix + "CreateWebhook"},
		{mem, http.MethodPost, MemoryPlane.RoutePrefix + "DeleteSubject"},
		{mem, http.MethodPost, MemoryPlane.RoutePrefix + "Fence"},
	} {
		rec := doPlaneRequest(t, c.h, c.method, c.target, `{}`, "task-token-2")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s: status=%d body=%s want 404", c.method, c.target, rec.Code, rec.Body.String())
		}
	}
	if up.calls != 0 {
		t.Fatalf("the plane answered %d forbidden operation(s)", up.calls)
	}
}

// TestPlaneSurfacePassesPlaneRefusalThrough covers the negative-rights rule: a plane 403 (a
// capability the person does not have) reaches the screen as that 403, with its code, so a
// refusal can never render as an empty success.
func TestPlaneSurfacePassesPlaneRefusalThrough(t *testing.T) {
	body := `{"code":"FORBIDDEN","message":"registry: capability registry is required"}`
	up := &planeUpstream{status: http.StatusForbidden, payload: body}
	srv := httptest.NewServer(up.handler())
	defer srv.Close()

	rec := doPlaneRequest(t, planeHandlerFor(t, RegistryPlane, srv), http.MethodPost,
		RegistryPlane.RoutePrefix+"PutVersion", `{"digest":"sha256:aa"}`, "task-token-3")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d want the plane's own 403", rec.Code)
	}
	if rec.Body.String() != body {
		t.Fatalf("refusal rewritten: %q", rec.Body.String())
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatal("a plane refusal has to arrive as product JSON")
	}
}

// TestPlaneSurfaceRefusesAnOriginCarryingAPIPath re-applies the operator rule measured as
// DEF-H24-12: a base that already carries the API path doubles it, and the mount fails
// loudly instead of impersonating a plane 404 at request time.
func TestPlaneSurfaceRefusesAnOriginCarryingAPIPath(t *testing.T) {
	_, err := NewPlaneProxy(PlaneProxyConfig{
		Spec:   RegistryPlane,
		Origin: "https://front.example.test/v1/registry",
		Client: http.DefaultClient,
	})
	if err == nil || !strings.Contains(err.Error(), RegistryPlane.OriginEnv) {
		t.Fatalf("an origin with an API path must be refused naming %s, got %v", RegistryPlane.OriginEnv, err)
	}
	if _, err := NewPlaneProxy(PlaneProxyConfig{Spec: EvalsPlane, Origin: "https://front.example.test"}); err == nil {
		t.Fatal("a surface without Space's workload client must not build")
	}
	if _, err := NewPlaneProxy(PlaneProxyConfig{
		Spec:   PlaneSpec{RoutePrefix: "/u-ahp/v1/nothing/", MachinePrefix: "/v1/nothing/", OriginEnv: "X", AllowedOps: nil},
		Origin: "https://front.example.test", Client: http.DefaultClient,
	}); err == nil {
		t.Fatal("a surface with no allowed operations must not build")
	}
}

// TestPlaneSurfacesAreTheDocumentedContract keeps the browser-visible routing, the machine
// prefixes and the deliberate exclusions in one assertion, so a rename cannot quietly
// strand a screen.
func TestPlaneSurfacesAreTheDocumentedContract(t *testing.T) {
	want := map[string]string{
		UIProfilePath:          "/v1/registry/",
		UIRegistryPath:         "/v1/registry/",
		UIArtifactsPath:        "/v1/artifacts/",
		UIEvalsPath:            "/v1/evals/",
		UITriggersPath:         "/v1/triggers/",
		UIMemoryPath:           "/v1/memory/",
		"/u-ahp/v1/compute/":   "/v1/compute/",
		"/u-ahp/v1/workspace/": "/v1/workspace/",
	}
	seen := map[string]bool{}
	for _, spec := range PlanePipes() {
		machine, ok := want[spec.RoutePrefix]
		if !ok {
			t.Fatalf("unexpected human surface %s", spec.RoutePrefix)
		}
		if seen[spec.RoutePrefix] {
			t.Fatalf("duplicate human surface %s", spec.RoutePrefix)
		}
		seen[spec.RoutePrefix] = true
		if spec.MachinePrefix != machine {
			t.Fatalf("%s forwards to %s, expected %s", spec.RoutePrefix, spec.MachinePrefix, machine)
		}
		if spec.OriginEnv == "" || len(spec.AllowedOps) == 0 {
			t.Fatalf("%s is incompletely specified", spec.RoutePrefix)
		}
	}
	if len(seen) != len(want) {
		t.Fatalf("expected %d human surfaces, found %d", len(want), len(seen))
	}

	forbidden := map[string][]string{
		RegistryPlane.RoutePrefix:  {"DropAll"},
		WorkspacePlane.RoutePrefix: {"SetCheck", "PurgeRevision"},
		ArtifactsPlane.RoutePrefix: {"BeginUpload", "Seal", "GC", "DeleteBySubject"},
		TriggersPlane.RoutePrefix:  {"CreateWebhook"},
		MemoryPlane.RoutePrefix:    {"DeleteSubject", "Fence"},
		EvalsPlane.RoutePrefix:     {"Propose"},
	}
	for _, spec := range PlanePipes() {
		for _, banned := range forbidden[spec.RoutePrefix] {
			for _, op := range spec.AllowedOps {
				if op == banned {
					t.Fatalf("%s must not expose %s to a browser", spec.RoutePrefix, banned)
				}
			}
		}
	}
}

func TestRegistryObservationForwardsVerifiedHumanWithoutExecutionToken(t *testing.T) {
	up := &planeUpstream{payload: `{"task":{"id":"old-task","status":{"state":"completed"}}}`}
	srv := httptest.NewServer(up.handler())
	defer srv.Close()
	h := planeHandlerFor(t, RegistryPlane, srv)
	req := httptest.NewRequest(http.MethodPost, UIRegistryPath+"GetInvokedTask", strings.NewReader(`{"installation_id":"installed","task_id":"old-task"}`))
	req.Header.Set("Authorization", "Bearer fresh-human-session")
	req = req.WithContext(transport.WithPrincipal(req.Context(), transport.Principal{Claims: auth.Claims{Kind: auth.KindHuman}}))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || up.auth != "Bearer fresh-human-session" {
		t.Fatalf("owner history depends on old execution grant: status=%d auth=%q", w.Code, up.auth)
	}
	req = httptest.NewRequest(http.MethodPost, UIRegistryPath+"GetInvokedTask", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer unverified-human")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 403 || up.calls != 1 {
		t.Fatal("unverified identity reached owner reader")
	}
}
