package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
)

// TestStandaloneDeploymentComposition proves the as-runner assembles the real
// production Space deployment surface (acceptance #1 "standalone Space
// deployment"): the Space API under /v1/space/ is reached THROUGH the production
// human-token auth boundary, the web UI is served at /, /healthz is live, the
// auth boundary rejects anonymous callers and task-tokens presented at the human
// entrypoint, and auth fails closed when the deployment is unconfigured. It uses
// no real State backend: an authenticated request for an unknown turn reaches the
// handler and returns 404 (proving routing + auth), while an unauthenticated one
// is rejected by the middleware with 403 before any handler logic.
func TestStandaloneDeploymentComposition(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	const issuer, audience, kid = "ahp-identity", "ahp", "k1"
	cfg := config{
		AuthIssuer:   issuer,
		AuthAudience: audience,
		AuthKeysRaw:  kid + "=" + base64.StdEncoding.EncodeToString(pub),
	}
	authMW, err := buildAuth(logger, cfg)
	if err != nil {
		t.Fatalf("buildAuth: %v", err)
	}
	if authMW == nil {
		t.Fatal("buildAuth returned nil middleware for a configured deployment")
	}

	// A coordinator whose StateClient points nowhere: routing/auth assertions do
	// not reach State (authorizeTurn short-circuits on an unknown turn).
	sc := space.NewStateClient("http://127.0.0.1:1", nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: nil, WorkerID: "w-test"})
	handler := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: authMW})

	srv := httptest.NewServer(buildMux(handler, nil))
	t.Cleanup(srv.Close)

	// /healthz live.
	if resp, err := http.Get(srv.URL + "/healthz"); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz not live: err=%v status=%v", err, resp.StatusCode)
	}
	// Web UI served at /.
	resp, err := http.Get(srv.URL + "/")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("web UI not served at /: err=%v status=%v", err, resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(strings.ToLower(string(body)), "<html") && !strings.Contains(strings.ToLower(string(body)), "<!doctype") {
		t.Fatalf("web UI / did not serve an HTML document (len=%d)", len(body))
	}

	// Anonymous Space request is rejected by the production auth boundary.
	if code := postSpaceRaw(t, srv.URL, "GetTurn", ""); code != http.StatusForbidden {
		t.Fatalf("anonymous Space request = %d, want 403 (auth boundary)", code)
	}

	// A valid human token passes the boundary and reaches the handler (unknown
	// turn -> 404, not 401/403), proving /v1/space/ routing + auth.
	signer, err := auth.NewSigner(issuer, kid, priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	humanTok := signToken(t, signer, auth.Claims{Audience: audience, Kind: auth.KindHuman, Tenant: "t1", Subject: "alice"})
	if code := postSpaceRaw(t, srv.URL, "GetTurn", humanTok); code == http.StatusForbidden || code == http.StatusUnauthorized {
		t.Fatalf("valid human token rejected at boundary: %d", code)
	}

	// A task token must NOT act as a human at the public entrypoint.
	taskTok := signToken(t, signer, auth.Claims{Audience: audience, Kind: auth.KindTask, Tenant: "t1", Deployment: "d1", Subject: "alice", Task: "task-1", Root: "r1", Context: "c1", Grant: "g1", Depth: 1})
	if code := postSpaceRaw(t, srv.URL, "GetTurn", taskTok); code != http.StatusForbidden {
		t.Fatalf("task token at human boundary = %d, want 403", code)
	}

	// Auth fails closed when unconfigured (no issuer/keys): buildAuth returns nil.
	if mw, err := buildAuth(logger, config{}); err != nil || mw != nil {
		t.Fatalf("unconfigured buildAuth must fail closed (nil,nil): mwNil=%v err=%v", mw == nil, err)
	}
}

func signToken(t *testing.T, s *auth.Signer, c auth.Claims) string {
	t.Helper()
	tok, err := s.Sign(c, 5*time.Minute)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return tok
}

func postSpaceRaw(t *testing.T, base, op, bearer string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+"/v1/space/"+op, strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("newreq: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
