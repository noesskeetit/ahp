package ahpbridge

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/hindsight"
	memory "github.com/ahp-platform/ahp/internal/memory/longterm"
	pgstore "github.com/ahp-platform/ahp/internal/memory/longterm/pgstore"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestLiveOwnerPathWritesRecallsAndErasesOnRealPostgres drives the mounted
// owner API exactly as a deployment does - HTTP through the Identity bridge,
// durable rows in PostgreSQL, jobs drained by the worker and executed by the
// native Hindsight engine - and then erases through the same surface.
//
// Every other test in this package uses an in-memory store and a scripted
// engine, so none of them can show that the colleague service that is the
// product's memory base actually stores, recalls and deletes on the platform.
// Both dependencies are external prerequisites and the test says so rather
// than faking them:
//
//	pgvector/pgvector:17 (AHP_TEST_DATABASE_URL, the lane database)
//	infra/memory/compose.hindsight.yaml (AHP_MEMORY_HINDSIGHT_URL, handbook-owned)
//
// The personal Hindsight service on 127.0.0.1:8889 and its data must never be
// pointed at this variable.
func TestLiveOwnerPathWritesRecallsAndErasesOnRealPostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("AHP_TEST_DATABASE_URL"))
	origin := strings.TrimSpace(os.Getenv("AHP_MEMORY_HINDSIGHT_URL"))
	if dsn == "" || origin == "" {
		t.Skip("needs AHP_TEST_DATABASE_URL (real PostgreSQL) and AHP_MEMORY_HINDSIGHT_URL (handbook-owned Hindsight)")
	}
	var rnd [5]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(rnd[:])

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	defer pool.Close()
	store := pgstore.UsePool(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("pgstore migrate: %v", err)
	}
	engine, err := hindsight.New(origin, os.Getenv("AHP_MEMORY_HINDSIGHT_API_KEY"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}

	f := fixture(t)
	f.claims.Tenant = "t-live-" + suffix
	f.claims.Subject = "u-live-" + suffix
	f.claims.CtxScope = auth.CtxScopeAuthority
	f.claims.Audience = "ahp"
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp-test", "key", priv)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.Sign(f.claims, 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	service := memory.New(store, engine, f.access)
	handler, err := NewHandler(service, auth.NewVerifier("ahp-test", map[string]ed25519.PublicKey{"key": pub}), f.access.d.Authorize)
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	uri, _ := url.Parse(auth.WorkloadID(f.claims.Scope()))
	call := func(op, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "https://memory.internal/v1/memory/"+op, bytes.NewBufferString(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{uri}}}}}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}

	// The source ref is passed explicitly so the platform's own source check
	// stays on the path: the fixture accepts only state://runs/run/input, and an
	// omitted ref would be filled with a console:// one and refused.
	remembered := call("Remember", `{"target":"personal","text":"I prefer Russian-language answers and I do local development on macOS.","source_refs":["state://runs/run/input"],"op_key":"live-remember-`+suffix+`"}`)
	if remembered.Code < 200 || remembered.Code >= 300 {
		t.Fatalf("Remember over the mounted surface failed: %d %s", remembered.Code, remembered.Body.String())
	}
	var op memory.Operation
	if err := json.Unmarshal(remembered.Body.Bytes(), &op); err != nil {
		t.Fatalf("Remember reply %s: %v", remembered.Body.String(), err)
	}
	t.Logf("Remember -> operation %s state %s", op.ID, op.State)

	// Drain the durable queue the way the deployment's worker loop does. The
	// worker mirrors the native operation: "submitted" means Hindsight is still
	// extracting, so only succeeded/failed/cancelled may end this wait.
	deadline := time.Now().Add(3 * time.Minute)
	state := op.State
	for time.Now().Before(deadline) {
		if err := service.Tick(ctx); err != nil {
			t.Fatalf("worker tick: %v", err)
		}
		reply := call("Operation", `{"id":"`+op.ID+`"}`)
		if reply.Code < 200 || reply.Code >= 300 {
			t.Fatalf("Operation read failed: %d %s", reply.Code, reply.Body.String())
		}
		current := memory.Operation{}
		if err := json.Unmarshal(reply.Body.Bytes(), &current); err != nil {
			t.Fatalf("Operation reply %s: %v", reply.Body.String(), err)
		}
		state = current.State
		if state == "succeeded" || state == "failed" || state == "cancelled" {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if state != "succeeded" {
		t.Fatalf("the native engine did not complete the durable write: state=%s", state)
	}
	t.Logf("operation %s settled in state %s", op.ID, state)

	// Recall through the same HTTP surface: the fact must come back for its
	// owner, which is the whole point of the memory plane.
	found := ""
	for time.Now().Before(deadline) {
		search := call("Search", `{"scope":"personal","query":"which platform do I develop on"}`)
		if search.Code < 200 || search.Code >= 300 {
			t.Fatalf("Search failed: %d %s", search.Code, search.Body.String())
		}
		found = strings.ToLower(search.Body.String())
		if strings.Contains(found, "macos") || strings.Contains(found, "russian") {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if !strings.Contains(found, "macos") && !strings.Contains(found, "russian") {
		t.Fatalf("the durable fact was never recallable over the owner API: last reply %s", found)
	}
	t.Logf("recall over the owner API: %s", found)

	// Erasure must be real: after Clear the owner recalls nothing.
	cleared := call("Clear", `{"scope":"personal","op_key":"live-clear-`+suffix+`"}`)
	if cleared.Code < 200 || cleared.Code >= 300 {
		t.Fatalf("Clear failed: %d %s", cleared.Code, cleared.Body.String())
	}
	for time.Now().Before(deadline) {
		if err := service.Tick(ctx); err != nil {
			t.Fatalf("worker tick after clear: %v", err)
		}
		gone := call("Search", `{"scope":"personal","query":"which platform do I develop on"}`)
		if gone.Code < 200 || gone.Code >= 300 {
			t.Fatalf("Search after Clear failed: %d %s", gone.Code, gone.Body.String())
		}
		body := strings.ToLower(gone.Body.String())
		if !strings.Contains(body, "macos") && !strings.Contains(body, "russian") {
			t.Logf("erasure confirmed over the owner API")
			return
		}
		time.Sleep(3 * time.Second)
	}
	t.Fatalf("the fact survived Clear on the native engine: %s", found)
}
