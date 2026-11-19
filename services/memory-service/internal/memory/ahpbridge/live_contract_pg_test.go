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

// ownerSession is one browser/console principal against the mounted surface,
// sharing the lane database and the handbook-owned native engine.
type ownerSession struct {
	service *memory.Service
	call    func(op, body string) *httptest.ResponseRecorder
	fixture *authorityFixture
	claims  auth.Claims
}

func liveOwner(t *testing.T, tag string) *ownerSession {
	t.Helper()
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
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := pgstore.UsePool(pool)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("pgstore migrate: %v", err)
	}
	engine, err := hindsight.New(origin, os.Getenv("AHP_MEMORY_HINDSIGHT_API_KEY"))
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	f := fixture(t)
	f.claims.Tenant = "t-" + tag + "-" + suffix
	f.claims.Subject = "u-" + tag + "-" + suffix
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
	workload, _ := url.Parse(auth.WorkloadID(f.claims.Scope()))
	call := func(op, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "https://memory.internal/v1/memory/"+op, bytes.NewBufferString(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.TLS = &tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{{URIs: []*url.URL{workload}}}}}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	return &ownerSession{service: service, call: call, fixture: f, claims: f.claims}
}

func (o *ownerSession) remember(t *testing.T, text, key string) memory.Operation {
	t.Helper()
	reply := o.call("Remember", `{"target":"personal","text":`+jsonBytes(text)+`,"source_refs":["state://runs/run/input"],"op_key":`+jsonBytes(key)+`}`)
	if reply.Code < 200 || reply.Code >= 300 {
		t.Fatalf("Remember failed: %d %s", reply.Code, reply.Body.String())
	}
	var op memory.Operation
	if err := json.Unmarshal(reply.Body.Bytes(), &op); err != nil {
		t.Fatalf("Remember reply %s: %v", reply.Body.String(), err)
	}
	return op
}

func jsonBytes(s string) string { b, _ := json.Marshal(s); return string(b) }

// settle drains the queue like the deployment worker and returns the final
// operation state. succeeded/failed/cancelled are terminal, submitted means the
// native engine is still extracting.
func (o *ownerSession) settle(t *testing.T, ctx context.Context, id string, ticks int) (string, string) {
	t.Helper()
	for i := 0; i < ticks; i++ {
		if err := o.service.Tick(ctx); err != nil {
			t.Fatalf("worker tick: %v", err)
		}
		reply := o.call("Operation", `{"id":`+jsonBytes(id)+`}`)
		if reply.Code < 200 || reply.Code >= 300 {
			t.Fatalf("Operation read failed: %d %s", reply.Code, reply.Body.String())
		}
		var current memory.Operation
		if err := json.Unmarshal(reply.Body.Bytes(), &current); err != nil {
			t.Fatalf("Operation reply %s: %v", reply.Body.String(), err)
		}
		if current.State == "succeeded" || current.State == "failed" || current.State == "cancelled" {
			return current.State, current.Error
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("operation %s never reached a terminal state", id)
	return "", ""
}

// TestLiveGrantRevocationStopsTheDurableWriteBeforeContentLeaves proves the
// last-moment authority gate with a real native engine behind it. A queued
// memory write must not reach Hindsight once the grant is gone: the worker
// revalidates immediately before it would send source content
// (internal/memory/longterm/worker.go), and a denial must end as a failed
// operation, not as a silent success.
func TestLiveGrantRevocationStopsTheDurableWriteBeforeContentLeaves(t *testing.T) {
	session := liveOwner(t, "revoke")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	op := session.remember(t, "I keep my production runbooks in the private wiki.", "revoke-remember")

	// The grant disappears after the write was accepted and queued, which is the
	// only ordering where the gate can still be proven: everything before it is
	// covered by the request-time authorization.
	session.fixture.revoked = true
	// Drain while revoked. Reading the operation over the mounted surface is
	// itself gated, exactly as it should be, so the state is observed after the
	// job has terminated and authority is restored - no ticks run after that, so
	// nothing can be written in the meantime.
	for i := 0; i < 5; i++ {
		if err := session.service.Tick(ctx); err != nil {
			t.Fatalf("worker tick while revoked: %v", err)
		}
		time.Sleep(time.Second)
	}
	session.fixture.revoked = false
	reply := session.call("Operation", `{"id":`+jsonBytes(op.ID)+`}`)
	if reply.Code < 200 || reply.Code >= 300 {
		t.Fatalf("Operation read after revocation failed: %d %s", reply.Code, reply.Body.String())
	}
	var final memory.Operation
	if err := json.Unmarshal(reply.Body.Bytes(), &final); err != nil {
		t.Fatalf("Operation reply %s: %v", reply.Body.String(), err)
	}
	state, cause := final.State, final.Error
	if state != "failed" {
		t.Fatalf("a revoked grant must end the durable write as failed, got %q", state)
	}
	if !strings.Contains(cause, "grant") && !strings.Contains(cause, "access") && !strings.Contains(cause, "forb") {
		t.Fatalf("the operation failed for an unrelated reason: %q", cause)
	}

	// Authority restored, the same owner must recall nothing: the content never
	// became native memory, so the denial is real erasure-by-never-writing rather
	// than a hidden read filter.
	session.fixture.revoked = false
	deadline := time.Now().Add(30 * time.Second)
	for {
		reply := session.call("Search", `{"scope":"personal","query":"where are my production runbooks"}`)
		if reply.Code < 200 || reply.Code >= 300 {
			t.Fatalf("Search after revocation failed: %d %s", reply.Code, reply.Body.String())
		}
		var res memory.SearchResult
		if err := json.Unmarshal(reply.Body.Bytes(), &res); err != nil {
			t.Fatalf("Search reply %s: %v", reply.Body.String(), err)
		}
		if len(res.Facts) == 0 {
			t.Logf("revoked write produced no native memory (operation failed: %s)", cause)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the revoked write still reached the native engine: %s", reply.Body.String())
		}
		time.Sleep(2 * time.Second)
	}
}

// TestLiveForeignPrincipalCannotReadOrDestroyMemory proves tenant separation on
// the mounted surface rather than in a unit double: a second principal in a
// different tenant must neither recall another tenant's fact nor invalidate it,
// and the real owner must be unaffected afterwards.
func TestLiveForeignPrincipalCannotReadOrDestroyMemory(t *testing.T) {
	owner := liveOwner(t, "owner")
	foreign := liveOwner(t, "foreign")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	op := owner.remember(t, "I prefer Russian-language answers and I do local development on macOS.", "owner-remember")
	if state, cause := owner.settle(t, ctx, op.ID, 90); state != "succeeded" {
		t.Fatalf("owner write did not succeed: state=%s cause=%s", state, cause)
	}
	recalled := ""
	for attempt := 0; attempt < 20; attempt++ {
		reply := owner.call("Search", `{"scope":"personal","query":"which platform do I develop on"}`)
		if reply.Code < 200 || reply.Code >= 300 {
			t.Fatalf("owner Search failed: %d %s", reply.Code, reply.Body.String())
		}
		recalled = strings.ToLower(reply.Body.String())
		if strings.Contains(recalled, "macos") || strings.Contains(recalled, "russian") {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if !strings.Contains(recalled, "macos") && !strings.Contains(recalled, "russian") {
		t.Fatalf("owner could not recall their own memory: %s", recalled)
	}
	var mine memory.SearchResult
	if err := json.Unmarshal([]byte(recalled), &mine); err != nil {
		t.Fatalf("decode owner recall: %v", err)
	}
	ownRef := mine.Facts[0].Ref

	// A foreign principal searches: it must get an empty answer, and the answer
	// must not carry the owner's bank or record identifiers.
	leak := foreign.call("Search", `{"scope":"personal","query":"which platform do I develop on"}`)
	if leak.Code >= 500 {
		t.Fatalf("foreign Search errored: %d %s", leak.Code, leak.Body.String())
	}
	body := strings.ToLower(leak.Body.String())
	if strings.Contains(body, "macos") || strings.Contains(body, "russian") {
		t.Fatalf("foreign principal recalled another tenant's memory: %s", body)
	}
	if strings.Contains(body, strings.ToLower(strings.Split(ownRef, ".")[0])) {
		t.Fatalf("foreign reply leaks the owner's bank identifier: %s", body)
	}
	t.Logf("foreign Search -> %d %s", leak.Code, body)

	// The foreign principal must also be unable to destroy it: invalidating an
	// owner record is a write to another tenant's memory.
	// memory_ref is the contract field and a mutation needs an idempotency key -
	// both are refused before authorization, so without them the probe would
	// prove nothing about cross-tenant behaviour.
	attempt := foreign.call("Invalidate", `{"scope":"personal","memory_ref":`+jsonBytes(ownRef)+`,"reason":"cross tenant probe","op_key":"foreign-probe"}`)
	if attempt.Code >= 200 && attempt.Code < 300 {
		t.Fatalf("foreign principal was allowed to invalidate an owner record: %s", attempt.Body.String())
	}
	if attempt.Code != http.StatusForbidden && attempt.Code != http.StatusNotFound {
		t.Fatalf("expected 403 or 404 for a foreign record, got %d %s", attempt.Code, attempt.Body.String())
	}
	if strings.Contains(strings.ToLower(attempt.Body.String()), "macos") || strings.Contains(strings.ToLower(attempt.Body.String()), "russian") {
		t.Fatalf("the refusal leaked the record content: %s", attempt.Body.String())
	}
	t.Logf("foreign Invalidate -> %d (no leak)", attempt.Code)

	// The owner still has the memory after both probes.
	kept := owner.call("Search", `{"scope":"personal","query":"which platform do I develop on"}`)
	if kept.Code < 200 || kept.Code >= 300 {
		t.Fatalf("owner Search after probes failed: %d %s", kept.Code, kept.Body.String())
	}
	if !strings.Contains(strings.ToLower(kept.Body.String()), "macos") && !strings.Contains(strings.ToLower(kept.Body.String()), "russian") {
		t.Fatalf("the cross-tenant probes destroyed the owner's memory: %s", kept.Body.String())
	}
}
