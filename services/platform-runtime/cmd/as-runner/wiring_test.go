package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/space"
)

// clearWiringEnv blanks every wiring input so a test starts from a clean slate.
func clearWiringEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		core.EnvOpenAIBaseURL, core.EnvOpenAIModel, core.EnvOpenAIAPIKey,
		"EMIL_BASE_URL", "EMIL_BEARER", "EMIL_MCP",
		"AS_DATA_DATABASE_URL", "DATABASE_URL", "AHP_DB_URL",
		"AHP_S3_ENDPOINT", "AHP_MINIO_ROOT_USER", "AHP_MINIO_ROOT_PASSWORD",
		"AS_ARTIFACT_BUCKET", "AS_ARTIFACTS_URL",
	} {
		t.Setenv(k, "")
	}
}

// TestBuildModelFailClosed verifies the binary refuses to construct a model
// when the LLM endpoint is unset: no silent stub, no foreign-provider fallback.
func TestBuildModelFailClosed(t *testing.T) {
	for _, k := range []string{core.EnvOpenAIBaseURL, core.EnvOpenAIModel, core.EnvOpenAIAPIKey} {
		t.Setenv(k, "")
	}
	if _, _, err := buildModel(); err == nil {
		t.Fatal("buildModel must fail closed when AHP_LLM_* unset")
	}
}

// TestBuildModelRealClient verifies that with the ahp Qwen role env set the
// runner constructs the real OpenAI-compatible client (no network call here),
// reporting the configured model id.
func TestBuildModelRealClient(t *testing.T) {
	t.Setenv("AS_ALLOW_DIRECT_MODEL", "1")
	t.Setenv(core.EnvOpenAIBaseURL, "http://127.0.0.1:59999/v1")
	t.Setenv(core.EnvOpenAIAPIKey, "test-key-not-a-secret")
	t.Setenv(core.EnvOpenAIModel, "qwen-test")
	m, id, err := buildModel()
	if err != nil {
		t.Fatalf("buildModel: %v", err)
	}
	if id != "qwen-test" {
		t.Fatalf("model id = %q, want qwen-test", id)
	}
	if _, ok := m.(*core.OpenAICompatClient); !ok {
		t.Fatalf("model is %T, want *core.OpenAICompatClient", m)
	}
}

// TestBuildToolsModelOnlyIsNoTools verifies the de-Emil contract: with neither a
// Data store nor an Emil endpoint configured, buildTools returns a nil registry
// and NO error. A model-only deployment is valid — the model is the only
// mandatory dependency (enforced by buildModel). This is the corrected
// behaviour after Emil ceased to be the readiness gate.
func TestBuildToolsModelOnlyIsNoTools(t *testing.T) {
	clearWiringEnv(t)
	reg, names, err := buildTools()
	if err != nil {
		t.Fatalf("model-only buildTools must not error, got: %v", err)
	}
	if reg != nil || names != nil {
		t.Fatalf("model-only must yield nil registry/names, got reg=%v names=%v", reg, names)
	}
}

// TestBuildToolsFailClosedArtifactPartial verifies a PARTIALLY configured Data
// store fails closed: a present endpoint but missing secret must error rather
// than silently dropping the artifact tool (which would let a "file" journey
// quietly no-op).
func TestBuildToolsFailClosedArtifactPartial(t *testing.T) {
	clearWiringEnv(t)
	t.Setenv("AS_ARTIFACTS_URL", "https://artifacts.test")
	t.Setenv("AS_WORKLOAD_CERT", "")
	t.Setenv("AS_WORKLOAD_KEY", "")
	// Configured API without workload material must refuse startup.
	if _, _, err := buildTools(); err == nil {
		t.Fatal("buildTools must fail closed on a partial Data store config")
	}
}

// TestBuildToolsRegistersRealSaveArtifact verifies that with a fully set Data
// group the runner registers the product save_artifact tool backed by the real
// store sink. The store/pgx/minio clients are built lazily (no DB or MinIO
// connection here), so this is an offline structural check. Executing the tool
// without a run scope in context must hit the REAL sink's guard (no fake
// result) — proving the wiring is the real Data path, not a stub.
func TestBuildToolsRegistersRealSaveArtifact(t *testing.T) {
	clearWiringEnv(t)
	t.Setenv("AS_ARTIFACTS_URL", "https://artifacts.test")
	setOwnerWorkloadEnv(t, "tenant", "runner")
	// Storage credentials are absent; the runner must only need the owner origin.

	reg, names, err := buildTools()
	if err != nil {
		t.Fatalf("buildTools with full Data group: %v", err)
	}
	if len(names) != 1 || names[0] != core.ToolSaveArtifact {
		t.Fatalf("tools = %v, want [%s]", names, core.ToolSaveArtifact)
	}
	// Real sink guard: no run scope in this bare context => explicit error, never
	// a fabricated artifact handle.
	_, err = reg.Execute(context.Background(), core.ToolSaveArtifact,
		json.RawMessage(`{"name":"r.txt","content":"hi"}`))
	if err == nil {
		t.Fatal("save_artifact must reject a call with no run scope (real sink, not a stub)")
	}
	if !strings.Contains(err.Error(), "scope") {
		t.Fatalf("expected scope guard error, got: %v", err)
	}
}

// TestBuildToolsRealEmilExecution drives a REAL tool call over HTTP against an
// Emil-shaped A2A server, proving the runner's executor is the live network
// adapter (not a stub) and returns provenance with an artifact digest. httptest
// here fronts the ADAPTER as a component test only — it is NOT live proof.
func TestBuildToolsRealEmilExecution(t *testing.T) {
	const body = "TOTAL=237\n"
	var srv *httptest.Server
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case emil.AgentCardPath:
			_ = json.NewEncoder(w).Encode(emil.AgentCard{
				Name: "core-agent", Version: "1.0.0",
				Skills: []emil.Skill{{ID: emil.ToolName, Name: emil.ToolName}},
			})
		case "/":
			task := emil.Task{
				ID: "task-w-1", ContextID: "ctx-w-1", Kind: "task",
				Status: emil.TaskStatus{State: emil.StateCompleted, Timestamp: "2026-10-29T10:00:00Z"},
				Artifacts: []emil.Artifact{{
					ArtifactID: "art-w-1", Name: "totals.txt",
					Parts: []emil.Part{{Kind: "file", File: &emil.FilePart{
						Bytes: body, URI: srv.URL + "/artifacts/art-w-1",
						Name: "totals.txt", MimeType: "text/plain",
					}}},
				}},
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": task})
		default:
			http.NotFound(w, r)
		}
	})
	srv = httptest.NewServer(h)
	defer srv.Close()

	t.Setenv("EMIL_BASE_URL", srv.URL)
	t.Setenv("EMIL_MCP", "cr-mcp|bearer|EMIL_MCP_CR_SECRET")
	t.Setenv("EMIL_MCP_CR_SECRET", "mcp-secret-must-not-leak")

	reg, names, err := buildTools()
	if err != nil {
		t.Fatalf("buildTools: %v", err)
	}
	var sawEmil bool
	for _, n := range names {
		if n == emil.ToolName {
			sawEmil = true
		}
	}
	if !sawEmil {
		t.Fatalf("tools = %v, want it to include %s", names, emil.ToolName)
	}

	res, err := reg.Execute(context.Background(), emil.ToolName, json.RawMessage(`{"prompt":"give totals"}`))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !strings.Contains(string(res), "art-w-1") {
		t.Fatalf("tool result missing artifact id: %s", res)
	}
	if !strings.Contains(string(res), emil.StateCompleted) {
		t.Fatalf("tool result missing completed state: %s", res)
	}
	// Provenance must redact the MCP secret.
	if strings.Contains(string(res), "mcp-secret-must-not-leak") {
		t.Fatalf("tool result leaked an MCP secret: %s", res)
	}
	if !strings.Contains(string(res), "cr-mcp") {
		t.Fatalf("tool result missing mcp server ref: %s", res)
	}
	// Unknown tools must be rejected by the registry.
	if _, err := reg.Execute(context.Background(), "nope", json.RawMessage(`{}`)); err == nil {
		t.Fatal("registry must reject unknown tool")
	}
}

// TestParseMCPServers verifies secrets come from named env vars, never from
// the config string, and authType defaults to none.
func TestParseMCPServers(t *testing.T) {
	t.Setenv("SOME_SECRET", "value-A")
	srvs := parseMCPServers("a|bearer|SOME_SECRET, b, c|none")
	if len(srvs) != 3 {
		t.Fatalf("want 3 servers, got %d (%+v)", len(srvs), srvs)
	}
	if srvs[0].ID != "a" || srvs[0].AuthType != "bearer" || srvs[0].Secret != "value-A" {
		t.Fatalf("server[0] wrong: %+v", srvs[0])
	}
	if srvs[1].ID != "b" || srvs[1].AuthType != "none" || srvs[1].Secret != "" {
		t.Fatalf("server[1] wrong: %+v", srvs[1])
	}
	if srvs[2].ID != "c" || srvs[2].AuthType != "none" {
		t.Fatalf("server[2] wrong: %+v", srvs[2])
	}
	// A secret named in the config but absent in env stays empty (no leak).
	t.Setenv("MISSING_SECRET", "")
	got := parseMCPServers("x|api-key|MISSING_SECRET")
	if len(got) != 1 || got[0].Secret != "" {
		t.Fatalf("absent secret must be empty: %+v", got)
	}
}

// discardLogger is a no-op logger for wiring fail-closed tests.
func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestBuildRunGrantIssuerFailClosed verifies Tier-2 refuses to build without the
// Identity endpoint or Space's workload mTLS — the deployment must not start a browser
// runner that would fall back to the shared ingress authority.
func TestBuildRunGrantIssuerFailClosed(t *testing.T) {
	l := discardLogger()
	// no Identity endpoint at all
	if _, err := buildRunGrantIssuer(l, config{}); err == nil {
		t.Fatal("buildRunGrantIssuer must fail closed without an Identity endpoint")
	}
	// Identity endpoint present but no workload mTLS (bounded cancel + delegate need it)
	if _, err := buildRunGrantIssuer(l, config{IdentityURL: "https://identity:8443"}); err == nil {
		t.Fatal("buildRunGrantIssuer must fail closed without workload cert/key/ca")
	}
	// exchange URL present (base derived from it) but no workload mTLS -> still refuse
	if _, err := buildRunGrantIssuer(l, config{IdentityExchangeURL: "https://identity:8443" + space.IdentityExchangePath}); err == nil {
		t.Fatal("buildRunGrantIssuer must derive base from exchange URL but still require workload mTLS")
	}
}

// TestBuildExchangerFailClosed verifies the human->task exchange is required for human
// auth (a browser human JWT must never reach State).
func TestBuildExchangerFailClosed(t *testing.T) {
	l := discardLogger()
	if _, err := buildExchanger(l, config{}); err == nil {
		t.Fatal("buildExchanger must fail closed without AS_IDENTITY_EXCHANGE_URL")
	}
	if _, err := buildExchanger(l, config{IdentityExchangeURL: "https://identity:8443"}); err == nil {
		t.Fatal("buildExchanger must fail closed without workload cert/key/ca")
	}
}

// The save_artifact idempotency key has to survive a resumed run: the same bytes must replay the
// same upload, while regenerated bytes must not be refused forever under a used filename.
func TestArtifactIdemKeyIsContentScoped(t *testing.T) {
	const sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	other := strings.Replace(sha, "aaaa", "bbbb", 1)

	if a, b := artifactIdemKey("report.md", sha, core.RunBinding{TaskID: "run", ContextID: "context"}), artifactIdemKey("report.md", sha, core.RunBinding{TaskID: "run", ContextID: "context"}); a != b {
		t.Fatalf("identical bytes and name must replay the same key: %q vs %q", a, b)
	}
	if artifactIdemKey("report.md", sha, core.RunBinding{TaskID: "run", ContextID: "context"}) == artifactIdemKey("report.md", other, core.RunBinding{TaskID: "run", ContextID: "context"}) {
		t.Fatal("different bytes must be a different upload, otherwise a resumed run is blocked forever")
	}
	if artifactIdemKey("report.md", sha, core.RunBinding{TaskID: "run", ContextID: "context"}) == artifactIdemKey("other.md", sha, core.RunBinding{TaskID: "run", ContextID: "context"}) {
		t.Fatal("different names must not share a key")
	}
	long := strings.Repeat("очень-длинное-имя-файла", 40)
	if k := artifactIdemKey(long, sha, core.RunBinding{TaskID: "run", ContextID: "context"}); len(k) > 128 || k == "" {
		t.Fatalf("the key must stay inside the artifacts contract limit, got %d bytes", len(k))
	}
}

func TestArtifactKeyKeepsLogicalRunAndContextSeparate(t *testing.T) {
	a := core.RunBinding{TaskID: "run-a", ContextID: "context-a"}
	key := artifactIdemKey("same.txt", "same-sha", a)
	if key != artifactIdemKey("same.txt", "same-sha", a) {
		t.Fatal("same run lost idempotency")
	}
	for _, b := range []core.RunBinding{{TaskID: "run-b", ContextID: a.ContextID}, {TaskID: a.TaskID, ContextID: "context-b"}} {
		if key == artifactIdemKey("same.txt", "same-sha", b) {
			t.Fatal("artifact key aliases another logical owner")
		}
	}
}
