//go:build ahp_exec

package main

import (
	"bytes"
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
)

const edgeCardURL = "https://stand.example/a2a/public"

// standShapedCard is the shape of the document the stand publishes
// (state/integration-stand/agent-card.json): one public JSONRPC interface whose
// url is the live A2A address, the mandatory AHP extension, and no internal face.
func standShapedCard() *a2a.AgentCard {
	return &a2a.AgentCard{
		Name:        "AHP Stand",
		Description: "AHP composed stand",
		Version:     "deadbeef",
		SupportedInterfaces: []*a2a.AgentInterface{{
			URL:             edgeCardURL,
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

func writeStandCard(t *testing.T, card *a2a.AgentCard) string {
	t.Helper()
	raw, err := json.Marshal(card)
	if err != nil {
		t.Fatalf("marshal card: %v", err)
	}
	path := filepath.Join(t.TempDir(), "agent-card.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write card: %v", err)
	}
	return path
}

// edgeMux assembles the human edge the way serveAll does -- baseMux, then the
// card mount, then the single-origin catch-all -- with the runner replaced by
// nothing: spaOnly over http.NotFoundHandler is exactly what the live edge
// returns for a path it does not own (it proxies it and the runner 404s).
func edgeMux(t *testing.T) *http.ServeMux {
	t.Helper()
	s := &server{logger: quietLogger()}
	mux := s.baseMux()
	s.mountPublicAgentCard(mux)
	mux.Handle("/", spaOnly(http.NotFoundHandler()))
	return mux
}

func get(t *testing.T, mux *http.ServeMux, path string) (int, []byte) {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, _ := io.ReadAll(rec.Body)
	return rec.Code, body
}

// TestWellKnownCardAbsentWithoutFile pins the audit symptom as the behaviour of a
// stand that names no card: 404, and nothing invented to fill it.
func TestWellKnownCardAbsentWithoutFile(t *testing.T) {
	t.Setenv("AHP_EXEC_AGENT_CARD_FILE", "")
	code, body := get(t, edgeMux(t), gateway.WellKnownAgentCardPath)
	if code != http.StatusNotFound {
		t.Fatalf("status = %d want 404 (body=%s)", code, body)
	}
}

// TestWellKnownCardServedOnHumanEdge is the leg the six-block audit asked for:
// the accepted card answers 200 at the well-known path on an edge that hosts no
// execution plane, and it declares the live A2A address of the stand.
func TestWellKnownCardServedOnHumanEdge(t *testing.T) {
	t.Setenv("AHP_EXEC_AGENT_CARD_FILE", writeStandCard(t, standShapedCard()))
	code, body := get(t, edgeMux(t), gateway.WellKnownAgentCardPath)
	if code != http.StatusOK {
		t.Fatalf("status = %d want 200 (body=%s)", code, body)
	}
	var card a2a.AgentCard
	if err := json.Unmarshal(body, &card); err != nil {
		t.Fatalf("served card is not valid JSON: %v (body=%s)", err, body)
	}
	if len(card.SupportedInterfaces) == 0 || card.SupportedInterfaces[0].URL != edgeCardURL {
		t.Fatalf("served card does not declare the live interface url (body=%s)", body)
	}
	if strings.Contains(string(body), "/a2a/internal") {
		t.Fatalf("served card leaks the internal face: %s", body)
	}
	// The catch-all of the edge must not have answered: the card route is owned here.
	if !json.Valid(body) {
		t.Fatalf("served body is not the card document: %s", body)
	}
}

// TestWellKnownCardBytesMatchTheExecutionPlane is why this mount reuses the
// gateway loader and the library handler instead of writing its own response: two
// entries of one deployment must not answer two documents. A hand-written
// encoder, or a second copy of the file, would drift here silently.
func TestWellKnownCardBytesMatchTheExecutionPlane(t *testing.T) {
	card := standShapedCard()
	t.Setenv("AHP_EXEC_AGENT_CARD_FILE", writeStandCard(t, card))
	_, edgeBody := get(t, edgeMux(t), gateway.WellKnownAgentCardPath)

	// The execution plane, assembled exactly the way the execution role does it,
	// with the internal face fail-closed. Its served bytes are the reference this
	// edge must match byte for byte.
	deny := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "internal disabled", http.StatusForbidden)
		})
	}
	pass := func(next http.Handler) http.Handler { return next }
	plane, err := gateway.NewServer(gateway.NewHandler(nil, gateway.HandlerConfig{}), gateway.ServerConfig{
		AgentCard:    card,
		Public:       gateway.AuthConfig{Middleware: pass},
		Internal:     gateway.AuthConfig{Middleware: deny},
		PublicPath:   "/a2a/public",
		InternalPath: "/a2a/internal",
		HealthPath:   "/a2a/healthz",
	})
	if err != nil {
		t.Fatalf("gateway.NewServer: %v", err)
	}
	ts := httptest.NewServer(plane)
	defer ts.Close()
	resp, err := http.Get(ts.URL + gateway.WellKnownAgentCardPath)
	if err != nil {
		t.Fatalf("GET plane card: %v", err)
	}
	defer resp.Body.Close()
	planeBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("plane card status = %d want 200", resp.StatusCode)
	}
	if !bytes.Equal(edgeBody, planeBody) {
		t.Fatalf("edge and plane served different card bytes:\n edge=%s\n plane=%s", edgeBody, planeBody)
	}
}

// TestWellKnownCardRefusedIsNotServed keeps the fail-closed half of the contract
// on this face too: a card the platform validator rejects must produce a 404, not
// a partially-accepted document.
func TestWellKnownCardRefusedIsNotServed(t *testing.T) {
	broken := standShapedCard()
	broken.Capabilities.Extensions = nil // the AHP extension is mandatory
	t.Setenv("AHP_EXEC_AGENT_CARD_FILE", writeStandCard(t, broken))
	code, body := get(t, edgeMux(t), gateway.WellKnownAgentCardPath)
	if code != http.StatusNotFound {
		t.Fatalf("a refused card must not be served: status = %d (body=%s)", code, body)
	}
}

// TestCardFaceAddsNoA2AFace guards the power boundary the single-origin wiring
// decided: this mount publishes a document, it does not put an A2A ingress, a
// second Space or an executor on the human edge.
func TestCardFaceAddsNoA2AFace(t *testing.T) {
	t.Setenv("AHP_EXEC_AGENT_CARD_FILE", writeStandCard(t, standShapedCard()))
	mux := edgeMux(t)
	for _, path := range []string{"/a2a/public", "/a2a/internal", "/a2a/healthz"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		if rec.Code == http.StatusOK {
			t.Fatalf("the card face opened an A2A route on the edge: POST %s = 200", path)
		}
	}
}

// TestCardFaceStepsAsideForTheExecutionPlane pins the no-double-registration
// guard: the execution plane registers the same well-known pattern on the same
// mux, and a second registration panics the process at startup. The pattern is
// registered here first, so a missing guard fails this test by panicking.
func TestCardFaceStepsAsideForTheExecutionPlane(t *testing.T) {
	t.Setenv("AHP_EXEC_AGENT_CARD_FILE", writeStandCard(t, standShapedCard()))
	t.Setenv("AHP_ENABLE_EXECUTION", "1")
	s := &server{logger: quietLogger()}
	mux := s.baseMux()
	mux.Handle(gateway.WellKnownAgentCardPath, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("plane card"))
	}))
	if s.mountPublicAgentCard(mux) {
		t.Fatal("mountPublicAgentCard registered the well-known path a second time")
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, gateway.WellKnownAgentCardPath, nil))
	if rec.Body.String() != "plane card" {
		t.Fatalf("the execution plane's card route was shadowed: body=%s", rec.Body.String())
	}
}
