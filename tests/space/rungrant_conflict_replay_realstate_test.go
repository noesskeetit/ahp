//go:build state_integration

package space_test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The injected first response represents Identity's decided transaction abort,
// before a grant is committed. State and all successful Identity operations use
// the real HTTP handlers and PostgreSQL stores. Repeating AcceptTurn must reuse
// its partial thread, then its committed child grant and run, even after losing
// the successful ACK. A mock that deduplicates its own rows cannot prove this.
func TestAcceptTurnRetryAfterIdentityConflictIsDurablyIdempotent(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL is required for this real PostgreSQL proof")
	}
	e := cNewEnv(t)
	pool, err := pgxpool.New(e.ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	ids := identity.New(pool)
	if err := ids.Migrate(e.ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject}
	parent := "parent-" + contracts.NewID()
	_, err = ids.CreateRoot(e.ctx, scope, identity.RootRequest{
		Task: parent, Context: "parent-ctx-" + contracts.NewID(), Deployment: scope.Deployment,
		Capabilities: []string{"state", "identity:delegate"}, TTL: time.Hour, AllowSelf: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	parentToken, err := ids.MintTask(e.ctx, scope, parent, e.signer, cAudience, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	owner := identityapi.Handler(identityapi.Config{
		Store: ids, Signer: e.signer, Verifier: e.verifier, Audience: cAudience,
	})
	var refuseFirst atomic.Bool
	refuseFirst.Store(true)
	var delegateCalls atomic.Int32
	identityHTTP := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delegateCalls.Add(1)
		if refuseFirst.Swap(false) {
			transport.WriteError(w, r, contracts.NewError(contracts.ErrConflict,
				"identity: the database aborted this write 14 times as concurrent within a replay budget of 5s (SQLSTATE 40001); replay the request unchanged"))
			return
		}
		owner.ServeHTTP(w, r)
	}))
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(e.ca.pem) {
		t.Fatal("test CA rejected")
	}
	identityHTTP.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientCAs}
	identityHTTP.StartTLS()
	t.Cleanup(identityHTTP.Close)
	client := identityHTTP.Client()
	tr := client.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{e.ca.clientCert(t, cWorkloadURI(e.tenant, e.deployment))}
	client.Transport = tr
	issuer, err := space.NewIdentityDelegateIssuer(identityHTTP.URL, client)
	if err != nil {
		t.Fatal(err)
	}
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client, WorkerID: "conflict-replay", RequireRunGrant: true,
		RunGrantIssuer: issuer, RunGrantCaps: []string{"state"}, TurnDispatchTimeout: -time.Second,
	})
	h := space.NewHandler(space.HandlerConfig{
		Coordinator: coord, RequireScopedTask: true,
		Auth: func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := transport.WithPrincipal(r.Context(), transport.Principal{Scope: scope})
				ctx = space.WithScopedTask(ctx, parentToken)
				ctx = space.WithAuthorityTask(ctx, parent)
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		},
	})
	key := "replay-" + contracts.NewID()
	body := []byte(`{"key":"` + key + `","context_id":"ctx-` + key + `","input":{"role":"user","content":"go"},"max_steps":1}`)
	accept := func(payload []byte) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/space/AcceptTurn", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Request-Id", "conflict-replay-proof")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	assertRows := func(wantThreads, wantRuns, wantChildren int) {
		t.Helper()
		var threads, runs, children int
		err := pool.QueryRow(e.ctx, `SELECT
		 (SELECT count(*) FROM ahp_state.threads WHERE tenant=$1),
		 (SELECT count(*) FROM ahp_state.runs r JOIN ahp_state.threads t ON t.id=r.thread_id WHERE t.tenant=$1),
		 (SELECT count(*) FROM ahp_identity.grants_child WHERE tenant=$1)`, e.tenant).Scan(&threads, &runs, &children)
		if err != nil {
			t.Fatal(err)
		}
		if threads != wantThreads || runs != wantRuns || children != wantChildren {
			t.Fatalf("durable rows (thread/run/child) = %d/%d/%d, want %d/%d/%d", threads, runs, children, wantThreads, wantRuns, wantChildren)
		}
	}
	first := accept(body)
	if first.Code != http.StatusConflict {
		t.Fatalf("first AcceptTurn = %d, want 409", first.Code)
	}
	var failure struct{ Code, Reason string }
	if err := json.Unmarshal(first.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != contracts.ErrConflict || failure.Reason != "identity:delegate:http-409:conflict" {
		t.Fatalf("conflict lost its typed cause: %+v", failure)
	}
	assertRows(1, 0, 0)
	var accepted space.AcceptTurnResponse
	for attempt := 0; attempt < 2; attempt++ {
		rec := accept(body)
		if rec.Code != http.StatusOK {
			t.Fatalf("same-body retry %d = %d", attempt, rec.Code)
		}
		var current space.AcceptTurnResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &current); err != nil {
			t.Fatal(err)
		}
		if current.TurnID == "" || current.ThreadID == "" {
			t.Fatal("retry returned no durable identity")
		}
		if attempt == 0 {
			accepted = current
		} else if current.TurnID != accepted.TurnID || current.ThreadID != accepted.ThreadID {
			t.Fatal("lost-ACK retry created a different turn or thread")
		}
		assertRows(1, 1, 1)
	}
	if delegateCalls.Load() != 3 {
		t.Fatalf("expected one explicit dependency call per AcceptTurn, got %d", delegateCalls.Load())
	}
	// Reusing the key with a different input is permanent key misuse, not a
	// license to manufacture another run while recovering the first one.
	changed := bytes.Replace(body, []byte(`"content":"go"`), []byte(`"content":"different"`), 1)
	bad := accept(changed)
	var misuse struct{ Code, Message string }
	if err := json.Unmarshal(bad.Body.Bytes(), &misuse); err != nil {
		t.Fatal(err)
	}
	// The existing transport renders key misuse as INVALID_ARGUMENT/400 and
	// retains KEY_REUSE_MISMATCH in the message; this lane does not change it.
	if bad.Code != http.StatusBadRequest || misuse.Code != contracts.ErrInvalid || !strings.Contains(misuse.Message, contracts.ErrKeyReuse) {
		t.Fatalf("changed-body replay = HTTP%d %q, want permanent key-misuse refusal", bad.Code, misuse.Code)
	}
	assertRows(1, 1, 1)
}
