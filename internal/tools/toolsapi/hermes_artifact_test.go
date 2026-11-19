package toolsapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/artifactsapi"
	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/internal/tools"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNativeHermesSpilledReceipt(t *testing.T) {
	python := os.Getenv("AHP_JOURNAL_GATEWAY_HERMES_PYTHON")
	source := os.Getenv("AHP_JOURNAL_GATEWAY_HERMES_SOURCE")
	stateDSN := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL")
	gatewayDSN := os.Getenv("AHP_GATEWAY_TEST_DATABASE_URL")
	if python == "" || source == "" || stateDSN == "" || gatewayDSN == "" {
		t.Skip("requires pinned Hermes and two disposable local PostgreSQL databases")
	}
	names := make(map[string]bool)
	for _, dsn := range []string{stateDSN, gatewayDSN} {
		cfg, err := pgxpool.ParseConfig(dsn)
		if err != nil || cfg.ConnConfig.Host != "127.0.0.1" ||
			!strings.HasPrefix(cfg.ConnConfig.Database, "ahp_carbon_compact") || names[cfg.ConnConfig.Database] {
			t.Fatal("set two distinct disposable local PostgreSQL databases")
		}
		names[cfg.ConnConfig.Database] = true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()
	statePool, err := pgxpool.New(ctx, stateDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(statePool.Close)
	var service *tools.Service
	owner := newReceiptArtifactOwner(t, ctx, statePool, func() *tools.Service { return service })
	journal := statejournal.NewWithArtifacts(statePool, owner)
	if err := journal.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	e := newEnvForDatabase(t, gatewayDSN, func(config *tools.Config) { config.JournalArtifacts = owner })
	service = e.svc
	e.allowDigest(t, "write", json.RawMessage(`{"item":17}`))
	e.fixture.writePadding.Store(300 << 10)
	effectFile, err := os.OpenFile(filepath.Join(t.TempDir(), "mcp-effects.log"), os.O_CREATE|os.O_EXCL|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer effectFile.Close()
	effect := func() error {
		if _, err := effectFile.WriteString("WRITE\n"); err != nil {
			return err
		}
		return syncEffectFile(effectFile)
	}
	e.fixture.onWrite.Store(&effect)

	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	stateSigner, err := auth.NewSigner("hermes-state", "hermes-state-key", private)
	if err != nil {
		t.Fatal(err)
	}
	leaseVerifier := auth.NewVerifier("hermes-state", map[string]ed25519.PublicKey{"hermes-state-key": public})
	authorize := func(context.Context, auth.Claims, string) error { return nil }
	stateHTTP, err := statejournal.NewHTTPWithToolLeases(journal, e.verifier, authorize, stateSigner)
	if err != nil {
		t.Fatal(err)
	}
	gatewayHTTP, err := NewHandlerWithToolLeases(ctx, service, e.verifier, authorize, leaseVerifier)
	if err != nil {
		t.Fatal(err)
	}
	artifactsHTTP, err := artifactsapi.New(owner, e.verifier, authorize)
	if err != nil {
		t.Fatal(err)
	}
	routes := http.NewServeMux()
	routes.Handle("/v1/state/", stateHTTP)
	routes.Handle("/v1/tools/", gatewayHTTP)
	routes.Handle("/v1/artifacts/", artifactsHTTP)
	var lostReply atomic.Bool
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tools/Call" || r.Header.Get("X-Test-Lose-Reply") != "yes" {
			routes.ServeHTTP(w, r)
			return
		}
		captured := httptest.NewRecorder()
		routes.ServeHTTP(captured, r)
		var result map[string]any
		if captured.Code != 200 || json.Unmarshal(captured.Body.Bytes(), &result) != nil || result["status"] != "SUCCEEDED" {
			t.Errorf("Gateway did not commit a successful receipt before TCP fault: %d %s", captured.Code, captured.Body.String())
		}
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("Gateway TCP failure injection: %v", err)
			return
		}
		lostReply.Store(true)
		_ = conn.Close()
	}))
	server.TLS = e.srv.TLS.Clone()
	server.StartTLS()
	t.Cleanup(server.Close)
	session := "hermes-blob-" + randomHex(t, 8)
	mint := func(task string) string {
		t.Helper()
		token, err := e.signer.Sign(auth.Claims{
			Audience: tAudience, Kind: auth.KindTask, Tenant: e.tenant, Deployment: e.deployment,
			Subject: tSubject, Task: task, Root: "hermes-artifact", Context: session, Grant: "hermes-artifact",
		}, 2*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	runNativeGatewayTrial(t, python, server, e, mint("original"), mint("replacement"),
		"large-artifact", "hermes_gateway_probe.py", source, "--session-id", session)
	content, err := os.ReadFile(effectFile.Name())
	if err != nil || string(content) != "WRITE\n" || atomic.LoadInt32(&e.fixture.effCount) != 1 || !lostReply.Load() {
		t.Fatalf("native effect/transport mismatch: effects=%q, dropped=%t (%v)", content, lostReply.Load(), err)
	}
	var stagedRows int
	if err := statePool.QueryRow(ctx, `SELECT count(*) FROM ahp_artifacts.artifacts
		WHERE tenant=$1 AND context_id=$2 AND journal_stage`, e.tenant, session).Scan(&stagedRows); err != nil || stagedRows != 1 {
		t.Fatalf("native Hermes message stages=%d, want 1 (%v)", stagedRows, err)
	}
	t.Log("native Hermes: Gateway TCP acknowledgement lost after commit, SIGKILL, mcp_effects=1 F_FULLFSYNC")
}
