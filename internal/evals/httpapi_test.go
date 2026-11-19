package evals

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// These tests drive the read-only /v1/evals/Verdict face against the real evals
// plane on real PostgreSQL. They prove: a verified principal is required (403
// otherwise); a digest with no verdict is NOT_FOUND; after a passing verdict is
// recorded via the accepted writer, the face answers passing=true with a
// content-free summary. Before httpapi.go existed the route 404'd; the route-map
// lookup fails the test in that case, so this file is the red->green marker.

func randDigest(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return "sha256:" + hex.EncodeToString(b)
}

func newEvalsFixture(t *testing.T) (*HTTPHandler, *Store) {
	t.Helper()
	url := os.Getenv("AHP_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("AHP_TEST_DATABASE_URL not set: evals HTTP tests run against the real PostgreSQL instance")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(pool.Close)
	s := NewStore(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	verifier := auth.NewVerifier("ahp", map[string]ed25519.PublicKey{})
	allow := func(context.Context, auth.Claims, string) error { return nil }
	h, err := NewHTTPHandler(s, verifier, allow)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	return h.(*HTTPHandler), s
}

func callVerdict(t *testing.T, hh *HTTPHandler, body any, scope contracts.Scope, withPrincipal bool) (int, map[string]any) {
	t.Helper()
	fn, ok := hh.routes["Verdict"]
	if !ok {
		t.Fatalf("evals route Verdict is NOT wired (would be 404 unknown route)")
	}
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/v1/evals/Verdict", bytes.NewReader(raw))
	if withPrincipal {
		req = req.WithContext(transport.WithPrincipal(req.Context(), transport.Principal{Scope: scope}))
	}
	w := httptest.NewRecorder()
	fn(w, req)
	out := map[string]any{}
	if b := w.Body.Bytes(); len(b) > 0 {
		_ = json.Unmarshal(b, &out)
	}
	return w.Code, out
}

func TestEvalsVerdictReadFace(t *testing.T) {
	hh, s := newEvalsFixture(t)
	scope := contracts.Scope{Tenant: "t-h26-evals", Deployment: "d-h26", Subject: "pub-h26"}
	ctx := context.Background()

	// No verified principal -> refused.
	if code, _ := callVerdict(t, hh, map[string]any{"agent_digest": randDigest(t)}, scope, false); code != http.StatusForbidden {
		t.Fatalf("unauthenticated Verdict must be 403, got %d", code)
	}

	// A digest with no verdict -> NOT_FOUND.
	missing := randDigest(t)
	if code, _ := callVerdict(t, hh, map[string]any{"agent_digest": missing}, scope, true); code != http.StatusNotFound {
		t.Fatalf("Verdict for an un-evaluated digest must be 404, got %d", code)
	}

	// Record a real passing verdict via the accepted writer, then read it.
	d := randDigest(t)
	if _, err := s.SetPolicy(ctx, Policy{ID: "face-policy", TaskSetID: "h26-evals-face", RequiredTaskIDs: []string{"a", "b"}, MaxAgeSeconds: 3600}); err != nil {
		t.Fatalf("declare policy: %v", err)
	}
	if _, err := s.Record(ctx, RecordInput{AgentDigest: d, TaskSetID: "h26-evals-face", Setup: map[string]any{"h": "1"}, Outcome: OutcomePass, Tasks: []TaskResult{{TaskID: "a", Passed: true, TerminalState: TerminalCompleted, Cost: 0.01}, {TaskID: "b", Passed: true, TerminalState: TerminalCompleted, Cost: 0}}, PolicyID: "face-policy", TargetKind: TargetFixture}); err != nil {
		t.Fatalf("record: %v", err)
	}
	code, out := callVerdict(t, hh, map[string]any{"agent_digest": d}, scope, true)
	if code != http.StatusOK {
		t.Fatalf("Verdict for an evaluated digest must be 200, got %d body=%v", code, out)
	}
	v, _ := out["verdict"].(map[string]any)
	if v == nil || v["passing"] != true || v["agent_digest"] != d {
		t.Fatalf("verdict payload wrong: %v", out)
	}
	if sum, _ := v["task_summary"].(map[string]any); sum["total"] != float64(2) || sum["passed"] != float64(2) {
		t.Fatalf("summary wrong: %v", v["task_summary"])
	}
	// Read-only: the face must not accept write fields.
	if c2, _ := callVerdict(t, hh, map[string]any{"agent_digest": d, "outcome": "fail"}, scope, true); c2 != http.StatusBadRequest {
		t.Fatalf("unknown field must be rejected 400, got %d", c2)
	}
}
