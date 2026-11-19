//go:build ahp_data

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/internal/identityapi"
	"github.com/ahp-platform/ahp/internal/memory"
	"github.com/ahp-platform/ahp/internal/memory/memoryapi"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Regression adapted from the independent reviewer reproduction. Every API,
// token verifier, exchange, grant and
// journal is real product code. Only source workspace membership is a fixture.
func TestMemoryHumanJournalVisibility(t *testing.T) {
	ctx := context.Background()
	conf, err := pgxpool.ParseConfig(os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	db := "ahp_review_memory_" + memFaceSuffix(t)
	create := "CREATE DATABASE " + pgx.Identifier{db}.Sanitize()
	if template := os.Getenv("AHP_TEST_DATABASE_TEMPLATE"); template != "" {
		create += " TEMPLATE " + pgx.Identifier{template}.Sanitize()
	}
	if _, err = admin.Exec(ctx, create); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(ctx, `DROP DATABASE `+db+` WITH (FORCE)`) }()
	conf.ConnConfig.Database = db
	pool, err := pgxpool.NewWithConfig(ctx, conf)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err = ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	verifier, signer := devIdentityAuth()
	tenant, dep := "review-memory", "review-edge"
	grants := identityGrantLabels{evaluate: ids}
	var failJournal atomic.Bool
	sink := memoryReadLabelsFromGrants(grants)
	store, err := memory.New(pool, nil, memoryStoreAuthorize(ids.Authorize), nil,
		memory.WithLabels(memoryCeilingFromGrants(grants)), memory.WithReadLabels(func(ctx context.Context, s contracts.Scope, set labels.Set) error {
			if failJournal.Load() {
				return contracts.NewError(contracts.ErrUnavailable, "injected journal outage")
			}
			return sink(ctx, s, set)
		}),
		memory.WithWorkspaces(func(_ context.Context, s contracts.Scope) ([]string, error) {
			if s.Subject == "dave" {
				return nil, nil
			}
			return []string{"project"}, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	mh, err := memoryapi.New(store, verifier, ids.Authorize)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mountMemoryHandlers(mux, mh)
	ih := identityapi.Handler(identityapi.Config{Store: ids, Verifier: verifier, Signer: signer, Audience: "ahp", Browser: &identityapi.BrowserConfig{AccessAudience: "ahp-web"}, ExchangerDeployments: []string{dep}})
	mux.Handle("/v1/identity/", ih)
	ca := newMemFaceCA(t)
	origin := httptest.NewUnstartedServer(mux)
	origin.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{ca.server}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: ca.pool}
	origin.StartTLS()
	defer origin.Close()
	client := ca.httpClient(t, ca.client(t, tenant, dep))
	exchanger, err := space.NewIdentityGrantExchanger(space.GrantExchangerConfig{IdentityExchangeURL: origin.URL, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	proxy, err := space.NewPlaneProxy(space.PlaneProxyConfig{Spec: space.MemoryPlane, Origin: origin.URL, Client: client})
	if err != nil {
		t.Fatal(err)
	}
	humanAuth, err := space.HumanAuthMiddleware(space.HumanAuthConfig{Issuer: "ahp", Audience: "ahp-web", Keys: map[string]ed25519.PublicKey{"dev-1": signer.Public()}})
	if err != nil {
		t.Fatal(err)
	}
	edge := httptest.NewServer(space.ExchangeMiddleware(exchanger, humanAuth)(proxy))
	defer edge.Close()
	tokens := map[string]string{}
	jtis := map[string]string{}
	caps := []string{"space:access", "memory", "memory:review", "memory:label"}
	for _, subject := range []string{"alice", "bob", "charlie", "dave", "eve"} {
		user := identity.BrowserUser{UserID: subject, Tenant: tenant, Subject: subject, DisplayName: subject, Email: subject + "@example.invalid"}
		subjectCaps := caps
		if subject == "dave" || subject == "eve" {
			subjectCaps = []string{"space:access", "memory"}
		}
		if err = ids.SeedBrowserUser(ctx, user, subject, "synthetic-password-123", nil, subjectCaps); err != nil {
			t.Fatal(err)
		}
		session, err := ids.StartBrowserSession(ctx, subject)
		if err != nil {
			t.Fatal(err)
		}
		jti := "jti-" + subject
		jtis[subject] = jti
		if err = ids.BindBrowserToken(ctx, jti, session); err != nil {
			t.Fatal(err)
		}
		tokens[subject], err = signer.Sign(auth.Claims{Audience: "ahp-web", Kind: auth.KindHuman, Tenant: tenant, Subject: subject, JTI: jti}, 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
	}
	call := func(subject, op string, body any) (int, []byte) {
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", edge.URL+space.UIMemoryPath+op, bytes.NewReader(raw))
		req.Header.Set("Authorization", "Bearer "+tokens[subject])
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		out, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, out
	}
	assertOK := func(subject, op string, body any) []byte {
		code, out := call(subject, op, body)
		if code != 200 {
			t.Fatalf("%s %s status=%d body=%s", subject, op, code, out)
		}
		return out
	}
	propose := func(subject, key, kind, text string) string {
		req := memory.ProposeRequest{Kind: kind, Text: text, OpKey: key, SourceKind: "user_statement", Evidence: []memory.Evidence{{SourceKind: "user_statement"}}}
		if kind == "workspace" {
			req.Workspace = "project"
		}
		var out memory.ProposeResult
		if err := json.Unmarshal(assertOK(subject, "Propose", req), &out); err != nil {
			t.Fatal(err)
		}
		return out.Memory.ID
	}
	grantLabels := func(subject string) labels.Set {
		scope := contracts.Scope{Tenant: tenant, Deployment: dep, Subject: subject}
		task := "space-authority:" + subject + "::" + jtis[subject]
		g, err := ids.Get(ctx, scope, task)
		if err != nil {
			t.Fatal(err)
		}
		return g.Labels
	}
	// A fresh browser session maps to a fresh task journal, while operation
	// idempotency remains bound to the same tenant/deployment/subject.
	rotate := func(subject string) {
		session, err := ids.StartBrowserSession(ctx, subject)
		if err != nil {
			t.Fatal(err)
		}
		jti := "jti-" + subject + "-" + memFaceSuffix(t)
		jtis[subject] = jti
		if err := ids.BindBrowserToken(ctx, jti, session); err != nil {
			t.Fatal(err)
		}
		tokens[subject], err = signer.Sign(auth.Claims{Audience: "ahp-web", Kind: auth.KindHuman, Tenant: tenant, Subject: subject, JTI: jti}, 5*time.Minute)
		if err != nil {
			t.Fatal(err)
		}
	}
	expectStatus := func(subject, op string, body any, want int, secret string) {
		code, out := call(subject, op, body)
		if code != want {
			t.Errorf("%s %s status=%d want=%d body=%s", subject, op, code, want, out)
		}
		if secret != "" && strings.Contains(string(out), secret) {
			t.Errorf("%s emitted protected content on a denied response", op)
		}
	}
	expectWorkspaceJournal := func(subject string, external bool) {
		got := grantLabels(subject)
		if len(got.Workspaces) != 1 || got.Workspaces[0] != "project" {
			t.Errorf("%s source workspace was not journaled: %+v", subject, got)
		}
		if external && (len(got.Taints) != 1 || got.Taints[0] != labels.TaintUntrustedExternal) {
			t.Errorf("%s source taint was not journaled: %+v", subject, got)
		}
	}
	derivedAgent := memory.ProposeRequest{Kind: memory.KindAgent, Text: "derived workspace source", OpKey: "widen-source", Evidence: []memory.Evidence{{SourceKind: "user_statement"}}}

	// Positive human Read and Search independently start with empty journals.
	private := propose("alice", "private-note", "user", "private marker for journal")
	assertOK("alice", "Review", map[string]string{"id": private, "decision": "approve", "op_key": "approve-private"})
	rotate("alice")
	assertOK("alice", "Read", map[string]string{"id": private})
	if got := grantLabels("alice"); len(got.Subjects) != 1 || got.Subjects[0] != "alice" {
		t.Fatalf("human Read did not taint Identity: %+v", got)
	}
	rotate("alice")
	if out := assertOK("alice", "Search", memory.SearchRequest{Query: "private marker", Kind: "user"}); !strings.Contains(string(out), private) {
		t.Fatal("human Search did not return own source")
	}
	if got := grantLabels("alice"); len(got.Subjects) != 1 || got.Subjects[0] != "alice" {
		t.Fatalf("human Search did not taint Identity: %+v", got)
	}
	expectStatus("bob", "Read", map[string]string{"id": private}, 404, "private marker")
	t.Log("positive human Read/Search + actual Identity exchange/mTLS/PG + private cross-user 404: PASS")

	// Both a fresh review and its replay expose an existing source.
	shared := propose("charlie", "workspace-note", "workspace", "workspace source must keep project restriction")
	review := map[string]string{"id": shared, "decision": "approve", "op_key": "approve-shared"}
	assertOK("bob", "Review", review)
	expectWorkspaceJournal("bob", false)
	expectStatus("bob", "Propose", derivedAgent, 403, "")
	rotate("bob")
	assertOK("bob", "Review", review)
	expectWorkspaceJournal("bob", false)
	if out := assertOK("eve", "Search", memory.SearchRequest{Query: "workspace source", Kind: "workspace"}); !strings.Contains(string(out), shared) {
		t.Fatal("Search missing shared source")
	}
	expectWorkspaceJournal("eve", false)
	expectStatus("eve", "Propose", derivedAgent, 403, "")

	// AddLabels and idempotent Propose replay must propagate all current source
	// labels, including labels narrowed after the original proposal was written.
	add := map[string]any{"id": shared, "op_key": "label-shared", "labels": labels.Set{Taints: []string{labels.TaintUntrustedExternal}}}
	assertOK("charlie", "AddLabels", add)
	expectWorkspaceJournal("charlie", true)
	rotate("charlie")
	assertOK("charlie", "AddLabels", add)
	expectWorkspaceJournal("charlie", true)
	rotate("charlie")
	_ = propose("charlie", "workspace-note", "workspace", "workspace source must keep project restriction")
	expectWorkspaceJournal("charlie", true)

	// The author may inspect their own pending proposal without a reviewer grant;
	// a known ID does not authorize an ordinary reader to inspect it.
	rotate("eve")
	pending := propose("eve", "pending-agent", "agent", "unapproved pending shared content")
	expectStatus("dave", "Read", map[string]string{"id": pending}, 404, "unapproved pending")
	assertOK("eve", "Read", map[string]string{"id": pending})
	assertOK("bob", "Read", map[string]string{"id": pending}) // explicit memory:review
	if _, err := pool.Exec(ctx, `UPDATE ahp_memory.memories SET state='revoked' WHERE id=$1`, pending); err != nil {
		t.Fatal(err)
	}
	expectStatus("dave", "Read", map[string]string{"id": pending}, 404, "unapproved pending")
	expectStatus("eve", "Read", map[string]string{"id": pending}, 404, "unapproved pending")
	assertOK("bob", "Read", map[string]string{"id": pending})

	// Journal failure is injected only at the service-to-Identity boundary. The
	// HTTP/mTLS path and owning PG mutation remain real; retry reuses the same key.
	rotate("charlie")
	fault := propose("charlie", "fault-source", "workspace", "fault-source-content")
	rotate("bob")
	failedReview := map[string]string{"id": fault, "decision": "approve", "op_key": "fault-review"}
	failJournal.Store(true)
	expectStatus("bob", "Review", failedReview, 503, "fault-source-content")
	failJournal.Store(false)
	var state string
	if err := pool.QueryRow(ctx, `SELECT state FROM ahp_memory.memories WHERE id=$1`, fault).Scan(&state); err != nil || state != memory.StateActive {
		t.Fatalf("owning Review state after sink failure: %s %v", state, err)
	}
	assertOK("bob", "Review", failedReview)
	expectWorkspaceJournal("bob", false)
	failedAdd := map[string]any{"id": fault, "op_key": "fault-label", "labels": labels.Set{Taints: []string{labels.TaintUntrustedExternal}}}
	rotate("bob")
	failJournal.Store(true)
	expectStatus("bob", "AddLabels", failedAdd, 503, "fault-source-content")
	failJournal.Store(false)
	assertOK("bob", "AddLabels", failedAdd)
	expectWorkspaceJournal("bob", true)
	rotate("charlie")
	replayProposal := memory.ProposeRequest{Kind: memory.KindWorkspace, Workspace: "project", Text: "fault-source-content", OpKey: "fault-source", SourceKind: "user_statement", Evidence: []memory.Evidence{{SourceKind: "user_statement"}}}
	failJournal.Store(true)
	expectStatus("charlie", "Propose", replayProposal, 503, "fault-source-content")
	failJournal.Store(false)
	assertOK("charlie", "Propose", replayProposal)
	expectWorkspaceJournal("charlie", true)
	if !t.Failed() {
		t.Log("fresh/replay mutation responses journal source labels; failed journal denies content; inactive Read requires owner-proposal or reviewer: PASS")
	}
}
