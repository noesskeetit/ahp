package gateway_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// platformCard is a minimal, contract-shaped public Agent Card: a named,
// versioned service advertising one public JSON-RPC interface, capabilities and
// one skill. Its only URL is the PUBLIC face -- it carries no /a2a/internal.
func platformCard() *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:    "AHP Execution Plane",
		Version: "1.0.0",
		SupportedInterfaces: []*a2a.AgentInterface{{
			URL:             "https://exec.example.com/a2a/public",
			ProtocolBinding: a2a.TransportProtocolJSONRPC,
			ProtocolVersion: a2a.Version,
		}},
		Capabilities: a2a.AgentCapabilities{
			Streaming: true,
			Extensions: []a2a.AgentExtension{{
				URI:      gateway.AgentExtensionURI,
				Required: true,
				Params: map[string]any{
					"history": "owned", "steering": "queue", "run_timeout_ms": float64(300000),
				},
			}},
		},
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		Skills:             []a2a.AgentSkill{{ID: "execute-task", Name: "Execute a durable A2A task"}},
	}
}

func mount(t *testing.T, cfg gateway.ServerConfig) *httptest.Server {
	t.Helper()
	h := gateway.NewHandler(nil, gateway.HandlerConfig{})
	srv, err := gateway.NewServer(h, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

func baseCfg() gateway.ServerConfig {
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "internal disabled", http.StatusForbidden)
		})
	}
	inject := func(next http.Handler) http.Handler { return next }
	return gateway.ServerConfig{
		Public:       gateway.AuthConfig{Middleware: inject},
		Internal:     gateway.AuthConfig{Middleware: deny},
		PublicPath:   "/a2a/public",
		InternalPath: "/a2a/internal",
		HealthPath:   "/a2a/healthz",
	}
}

// TestAgentCardServedOnPublicFace proves the mounted static card is served at
// the canonical well-known path, is valid JSON carrying the platform fields, and
// never reveals the internal face -- while the internal face stays fail-closed
// and the health probe stays open.
func TestAgentCardServedOnPublicFace(t *testing.T) {
	cfg := baseCfg()
	cfg.AgentCard = platformCard()
	ts := mount(t, cfg)

	resp, err := http.Get(ts.URL + gateway.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("GET card: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("card status = %d want 200 (body=%s)", resp.StatusCode, body)
	}
	var card a2a.AgentCard
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatalf("card is not valid JSON: %v (body=%s)", err, body)
	}
	if card.Name == "" || card.Version == "" || len(card.Skills) == 0 ||
		len(card.SupportedInterfaces) == 0 || card.SupportedInterfaces[0].URL == "" {
		t.Fatalf("card missing platform fields: %s", body)
	}
	if !card.Capabilities.Streaming {
		t.Fatal("card capabilities not preserved")
	}
	if strings.Contains(string(body), "/a2a/internal") {
		t.Fatalf("card leaks the internal face path: %s", body)
	}

	// The card is served WITHOUT authentication (public, untrusted face): GET with
	// no Authorization header already returned 200 above.

	// The internal face stays fail-closed even though the card is public.
	if r, err := http.Post(ts.URL+"/a2a/internal", "application/json", strings.NewReader("{}")); err != nil {
		t.Fatalf("POST internal: %v", err)
	} else {
		r.Body.Close()
		if r.StatusCode != http.StatusForbidden {
			t.Fatalf("internal face status = %d want 403 (fail-closed)", r.StatusCode)
		}
	}
	// Health stays open.
	if r, err := http.Get(ts.URL + "/a2a/healthz"); err != nil {
		t.Fatalf("GET health: %v", err)
	} else {
		r.Body.Close()
		if r.StatusCode != http.StatusOK {
			t.Fatalf("health status = %d want 200", r.StatusCode)
		}
	}
}

// TestAgentCardNotServedWhenUnconfigured pins the fail-closed default: with no
// card the well-known path is 404 -- the plane never fabricates a document.
func TestAgentCardNotServedWhenUnconfigured(t *testing.T) {
	ts := mount(t, baseCfg()) // AgentCard nil
	r, err := http.Get(ts.URL + gateway.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("GET card: %v", err)
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusNotFound {
		t.Fatalf("unconfigured card status = %d want 404", r.StatusCode)
	}
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return p
}

func TestLoadAgentCardFile(t *testing.T) {
	dir := t.TempDir()

	if c, err := gateway.LoadAgentCardFile("", "/a2a/internal"); err != nil || c != nil {
		t.Fatalf("empty path: card=%v err=%v want nil,nil", c, err)
	}

	// The AHP section 6.1 extension, embedded as it would appear in a card file.
	ext := `"capabilities":{"streaming":true,"extensions":[{"uri":"` + gateway.AgentExtensionURI +
		`","required":true,"params":{"history":"owned","steering":"queue","run_timeout_ms":300000}}]}`

	good := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		` + ext + `,
		"skills":[{"id":"execute-task","name":"Execute a durable A2A task","tags":["exec"]}]}`
	c, err := gateway.LoadAgentCardFile(writeFile(t, dir, "good.json", good), "/a2a/internal")
	if err != nil || c == nil || c.Name != "AHP" || len(c.Skills) != 1 {
		t.Fatalf("good card: card=%v err=%v", c, err)
	}

	// A card WITHOUT the required AHP extension must be rejected.
	noExt := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		"capabilities":{"streaming":true},
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "noext.json", noExt), "/a2a/internal"); err == nil {
		t.Fatal("card missing the AHP extension must be rejected")
	}

	// A wrong history enum must be rejected.
	badHist := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		"capabilities":{"extensions":[{"uri":"` + gateway.AgentExtensionURI +
		`","required":true,"params":{"history":"sideload","steering":"queue","run_timeout_ms":1000}}]},
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "badhist.json", badHist), "/a2a/internal"); err == nil {
		t.Fatal("card with an invalid history enum must be rejected")
	}

	// A wrong steering enum must be rejected.
	badSteer := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		"capabilities":{"extensions":[{"uri":"` + gateway.AgentExtensionURI +
		`","required":true,"params":{"history":"owned","steering":"telepathy","run_timeout_ms":1000}}]},
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "badsteer.json", badSteer), "/a2a/internal"); err == nil {
		t.Fatal("card with an invalid steering enum must be rejected")
	}

	// A non-positive run_timeout_ms must be rejected.
	badTimeout := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		"capabilities":{"extensions":[{"uri":"` + gateway.AgentExtensionURI +
		`","required":true,"params":{"history":"owned","steering":"queue","run_timeout_ms":0}}]},
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "badto.json", badTimeout), "/a2a/internal"); err == nil {
		t.Fatal("card with a non-positive run_timeout_ms must be rejected")
	}

	// A non-required extension must be rejected.
	optExt := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		"capabilities":{"extensions":[{"uri":"` + gateway.AgentExtensionURI +
		`","params":{"history":"owned","steering":"queue","run_timeout_ms":1000}}]},
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "optext.json", optExt), "/a2a/internal"); err == nil {
		t.Fatal("a non-required AHP extension must be rejected")
	}

	// Leak / DSN guards still fire once the extension is well formed.
	leak := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/internal","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		` + ext + `,
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "leak.json", leak), "/a2a/internal"); err == nil {
		t.Fatal("card exposing /a2a/internal must fail closed")
	}

	dsn := `{"name":"AHP","version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		"description":"primary store postgres://user:pass@db.internal:5432/ahp",
		` + ext + `,
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "dsn.json", dsn), "/a2a/internal"); err == nil {
		t.Fatal("card embedding a DSN must fail closed")
	}

	noName := `{"version":"1.0.0",
		"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"JSONRPC","protocolVersion":"1.0"}],
		` + ext + `,
		"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "noname.json", noName), "/a2a/internal"); err == nil {
		t.Fatal("card without a name must be rejected")
	}

	noURL := `{"name":"AHP","version":"1.0.0",` + ext + `,"skills":[{"id":"x","name":"X"}]}`
	if _, err := gateway.LoadAgentCardFile(writeFile(t, dir, "nourl.json", noURL), "/a2a/internal"); err == nil {
		t.Fatal("card without a public interface url must be rejected")
	}

	if _, err := gateway.LoadAgentCardFile(filepath.Join(dir, "missing.json"), "/a2a/internal"); err == nil {
		t.Fatal("missing card file must fail closed")
	}
}

// TestExtendedCardMatchesPublic proves the authenticated extended card agrees
// with the public well-known card: the execution role wires the SAME
// operator-provided document into both, so the two can never disagree.
func TestExtendedCardMatchesPublic(t *testing.T) {
	card := platformCard()
	h := gateway.NewHandler(nil, gateway.HandlerConfig{ExtendedCard: card})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}
	ctx := transport.WithPrincipal(context.Background(), transport.Principal{
		Claims: auth.Claims{Kind: auth.KindHuman, Tenant: scope.Tenant, Deployment: scope.Deployment, Subject: scope.Subject},
		Scope:  scope,
	})
	got, err := h.GetExtendedAgentCard(ctx, nil)
	if err != nil {
		t.Fatalf("GetExtendedAgentCard: %v", err)
	}
	wantRaw, _ := json.Marshal(card)
	gotRaw, _ := json.Marshal(got)
	if string(wantRaw) != string(gotRaw) {
		t.Fatalf("extended card disagrees with the public card:\n want=%s\n got =%s", wantRaw, gotRaw)
	}
}

// TestPublicFaceFailClosedWithoutCredential pins the acceptance contract against
// the PRODUCTION public middleware (transport.Task: a verified SPIFFE workload
// certificate AND a scoped task bearer). An unauthenticated request -- no client
// certificate, no bearer -- must be refused 403, while the public Agent Card
// (well-known path, no auth) and the health probe stay open. This mirrors exactly
// how cmd/ahp role_execution mounts the plane, so it proves the real face, not a
// pass-through stub.
func TestPublicFaceFailClosedWithoutCredential(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	verifier := auth.NewVerifier("https://identity.test", map[string]ed25519.PublicKey{"k1": pub})
	authorize := func(context.Context, auth.Claims, string) error { return nil }

	cfg := baseCfg()
	cfg.AgentCard = platformCard()
	cfg.Public = gateway.AuthConfig{Middleware: transport.Task(verifier, "ahp", "execution", authorize)}

	h := gateway.NewHandler(nil, gateway.HandlerConfig{ExtendedCard: cfg.AgentCard})
	srv, err := gateway.NewServer(h, cfg)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// 1) /a2a/public with no credential (and no workload cert) => 403, fail-closed.
	r, err := http.Post(ts.URL+"/a2a/public", "application/json", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"message/send"}`))
	if err != nil {
		t.Fatalf("POST public: %v", err)
	}
	body, _ := io.ReadAll(r.Body)
	r.Body.Close()
	if r.StatusCode != http.StatusForbidden {
		t.Fatalf("public face without credential = %d want 403 (body=%s)", r.StatusCode, body)
	}

	// 2) the public Agent Card stays open and valid at the well-known path.
	if cr, err := http.Get(ts.URL + gateway.WellKnownAgentCardPath); err != nil {
		t.Fatalf("GET card: %v", err)
	} else {
		cbody, _ := io.ReadAll(cr.Body)
		cr.Body.Close()
		if cr.StatusCode != http.StatusOK {
			t.Fatalf("card status = %d want 200", cr.StatusCode)
		}
		var card a2a.AgentCard
		if err := json.Unmarshal(cbody, &card); err != nil {
			t.Fatalf("card invalid JSON: %v", err)
		}
		if card.Name == "" || len(card.Skills) == 0 || !card.Capabilities.Streaming {
			t.Fatalf("card missing platform fields: %s", cbody)
		}
	}

	// 3) health stays open (liveness must not require a credential).
	if hr, err := http.Get(ts.URL + "/a2a/healthz"); err != nil {
		t.Fatalf("GET health: %v", err)
	} else {
		hr.Body.Close()
		if hr.StatusCode != http.StatusOK {
			t.Fatalf("health status = %d want 200", hr.StatusCode)
		}
	}
}

// TestProtocolSurfaceConstants pins what the plane claims to speak. The card
// validator compares a published card against these values, so a silent change
// here must be a deliberate one.
func TestProtocolSurfaceConstants(t *testing.T) {
	if gateway.ServedProtocolBinding != "JSONRPC" {
		t.Fatalf("served binding = %q, want JSONRPC", gateway.ServedProtocolBinding)
	}
	if gateway.ServedProtocolVersion != "1.0" {
		t.Fatalf("served protocol version = %q, want 1.0 (a2a.Version)", gateway.ServedProtocolVersion)
	}
	for _, m := range []string{gateway.MethodSendMessage, gateway.MethodGetTask, gateway.MethodCancelTask,
		gateway.MethodSendStreamingMessage, gateway.MethodSubscribeToTask, gateway.MethodGetExtendedAgentCard} {
		if !gateway.MethodIsServed(m) {
			t.Fatalf("core method %q missing from the served surface", m)
		}
	}
	for _, m := range gateway.UnservedLegacyMethods {
		if gateway.MethodIsServed(m) {
			t.Fatalf("v0 method %q must stay unserved in this build", m)
		}
	}
}

// TestValidatePublicAgentCardProtocolClaims is the anti-lying gate: a card whose
// machine-readable protocol claim (binding/version) differs from what the mounted
// transport serves, or whose prose tells a client to call an A2A v0 method this
// build does not compile in, is refused at LOAD time. A card that advertises an
// unserved method is a false claim about the deployment -- measured on pin
// 1370d01, where the honest task token passed the face and "message/send" came
// back METHOD_NOT_FOUND (-32601).
func TestValidatePublicAgentCardProtocolClaims(t *testing.T) {
	dir := t.TempDir()
	ext := `"capabilities":{"streaming":true,"extensions":[{"uri":"` + gateway.AgentExtensionURI +
		`","required":true,"params":{"history":"owned","steering":"interrupt","run_timeout_ms":300000}}]}`
	doc := func(binding, version, description string) string {
		return `{"name":"AHP","version":"1.0.0","description":` + jsonQuote(description) + `,"supportedInterfaces":[{"url":"https://exec.example.com/a2a/public","protocolBinding":"` +
			binding + `","protocolVersion":"` + version + `"}],
		` + ext + `,
		"skills":[{"id":"agents-space-turn","name":"Agents Space turn","tags":["a2a"]}]}`
	}
	load := func(name, body string) error {
		_, err := gateway.LoadAgentCardFile(writeFile(t, dir, name, body), "/a2a/internal")
		return err
	}

	// A wrong protocol version: the card promises 0.3, the plane speaks 1.0.
	err := load("v03.json", doc("JSONRPC", "0.3", "Executes a durable A2A turn."))
	if err == nil || !strings.Contains(err.Error(), "protocolVersion") {
		t.Fatalf("card claiming protocolVersion 0.3 must be refused with the version reason, got %v", err)
	}
	if !strings.Contains(err.Error(), gateway.ServedProtocolVersion) {
		t.Fatalf("refusal must name the served version: %v", err)
	}

	// A wrong binding: gRPC/REST are not mounted by this plane.
	if err := load("grpc.json", doc("GRPC", "1.0", "Executes a durable A2A turn.")); err == nil ||
		!strings.Contains(err.Error(), "protocolBinding") {
		t.Fatalf("card claiming the GRPC binding must be refused, got %v", err)
	}

	// Prose that tells the client to call a v0 method is the same false claim.
	err = load("v0names.json", doc("JSONRPC", "1.0",
		"Принимает задачу A2A (message/send, message/stream); статус через tasks/get."))
	if err == nil || !strings.Contains(err.Error(), "does not serve") {
		t.Fatalf("card naming v0 methods must be refused, got %v", err)
	}
	if !strings.Contains(err.Error(), gateway.MethodSendMessage) {
		t.Fatalf("refusal must name the canonical methods that ARE served: %v", err)
	}

	// The honest card: canonical names, served binding/version.
	if err := load("honest.json", doc("JSONRPC", "1.0",
		"Accepts a task with SendMessage, observed with GetTask, cancelled with CancelTask.")); err != nil {
		t.Fatalf("honest card must load: %v", err)
	}
}

func jsonQuote(s string) string {
	raw, err := json.Marshal(s)
	if err != nil {
		return `""`
	}
	return string(raw)
}
