package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/adapters/emil"
	"github.com/ahp-platform/ahp/internal/core"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/modelapi"
	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/internal/treedelegate"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// DEF-H10-9. Every record of one logical run has to be filed under one scope, or the
// two ends of an A->B chain have nothing comparable to compare. A browser-initiated
// run is handed the HUMAN principal's scope, which carries no deployment (a human
// token carries no azp), while the run itself executes under a task credential that
// does -- internal/space/coordinator.go mints that credential alongside the request
// and passes it as space.CoreRequest.TaskToken and as core.WithRunScope's bearer.
//
// Measured on a live stand with AHP_DELEGATE_TREE_BUDGET=1: a browser run died at
// step 0 with "scope deployment must not be empty" while the delegation leg of a run
// booked its ledger row WITH a deployment. One run, two scope labels, and a
// verification harness that cannot tell which number belongs to whom.
//
// These legs run against the real Postgres ledger on purpose: the defect is a
// property of the rows, and no mock can show that one run ended up with two scope
// labels or that a child's bound came out of its parent's remainder.

type h109Harness struct {
	tb   *treeBudget
	pool *pgxpool.Pool
	att  runScopeAttester

	signer     *auth.Signer
	aud        string
	real       contracts.Scope // what the run's credential says
	carried    contracts.Scope // what a browser turn hands the runner
	runID      string
	stateRunID string
}

func newH109Harness(t *testing.T, limits ...int) *h109Harness {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		// A failure, not a skip: these legs are the evidence that one run has one
		// accounting scope, and an uncounted run must not be able to look green.
		t.Fatal("AHP_TEST_DATABASE_URL is not set: source the suite environment (a budget-tree leg that does not run proves nothing)")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)
	store := model.NewBudgetStore(pool)
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	signer, err := auth.NewSigner("h109", "h109-1", priv)
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	u := fmt.Sprint(time.Now().UnixNano())
	real := contracts.Scope{Tenant: "h109" + u, Deployment: "runner-a", Subject: "demo-human"}
	// Exactly what a browser turn carries: tenant and subject, no deployment.
	carried := contracts.Scope{Tenant: real.Tenant, Subject: real.Subject}
	att := runScopeAttester{verifier: auth.NewVerifier("h109", map[string]ed25519.PublicKey{"h109-1": pub}),
		taskAudience: "ahp", workload: auth.WorkloadID(real)}

	states := state.New(pool)
	if err := states.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	thread, err := states.CreateThread(context.Background(), real, "ctx-run-"+u, "thread")
	if err != nil {
		t.Fatal(err)
	}
	limit := 3
	if len(limits) > 0 {
		limit = limits[0]
	}
	storedRun, err := states.CreateRun(context.Background(), real, thread.ID, "run-"+u, "run", []byte(fmt.Sprintf(`{"max_steps":%d}`, limit)))
	if err != nil {
		t.Fatal(err)
	}
	authority, err := modelapi.NewStateTreeAuthority(states, []string{auth.WorkloadID(real)}, func(context.Context, auth.Claims, string) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ca := newOwnerCA(t)
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(ca.pem)
	handler, err := modelapi.NewTreeBudgetHandler(store, att.verifier, func(context.Context, auth.Claims, string) error { return nil }, authority)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	hc := srv.Client()
	tr := hc.Transport.(*http.Transport).Clone()
	tr.TLSClientConfig.Certificates = []tls.Certificate{ca.clientCert(t, auth.WorkloadID(real))}
	hc.Transport = tr
	client, err := modelapi.NewTreeBudgetClient(srv.URL, hc)
	if err != nil {
		t.Fatal(err)
	}
	return &h109Harness{
		tb:         &treeBudget{client: client, scope: att},
		pool:       pool,
		att:        att,
		signer:     signer,
		aud:        "ahp",
		real:       real,
		carried:    carried,
		runID:      "run-" + u,
		stateRunID: storedRun.ID,
	}
}

// taskToken mints a credential the way Identity mints one: shape enforced by the
// signer, lifetime bounded, signed with the published key.
func (h *h109Harness) taskToken(t *testing.T, claims auth.Claims, ttl time.Duration) string {
	t.Helper()
	if claims.Kind == "" {
		claims.Kind = auth.KindTask
	}
	if claims.Audience == "" {
		claims.Audience = h.aud
	}
	if claims.Subject == "" {
		claims.Subject = h.real.Subject
	}
	if claims.Tenant == "" {
		claims.Tenant = h.real.Tenant
	}
	if claims.Kind == auth.KindTask {
		if claims.Task == "" {
			claims.Task = h.runID
		}
		if claims.Root == "" {
			claims.Root = h.runID
		}
		if claims.Context == "" {
			claims.Context = "ctx-" + h.runID
		}
		if claims.Grant == "" {
			claims.Grant = "grant-" + h.runID
		}
	}
	tok, err := h.signer.Sign(claims, ttl)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return tok
}

// runToken is the credential of THIS run in THIS deployment -- the bearer a browser
// turn cannot put in its scope but does present as the run's own token.
func (h *h109Harness) runToken(t *testing.T) string {
	t.Helper()
	return h.taskToken(t, auth.Claims{Deployment: h.real.Deployment, Actors: []string{h.real.Deployment}, Depth: 1}, 5*time.Minute)
}

// browserRequest rebuilds what the Run Coordinator hands the runner for a
// browser-initiated turn: the human scope plus the run's task token as its own field.
func (h *h109Harness) browserRequest(t *testing.T, maxSteps int) *space.CoreRequest {
	return &space.CoreRequest{RunID: h.stateRunID, Scope: h.carried, TaskToken: h.runToken(t), MaxSteps: maxSteps}
}

// delegate drives the seam a real delegation uses: the tool context internal/core
// builds mid-run (the run's scope plus its bearer plus its binding) into
// emilRunAuthority, which is what hands a child deployment its bound.
func (h *h109Harness) delegate(t *testing.T, bearer string, bound int) emil.DelegationBudget {
	t.Helper()
	ctx := core.WithRunScope(context.Background(), h.carried, bearer)
	ctx = core.WithRunBinding(ctx, core.RunBinding{
		ContextID: "thread-" + h.runID, TaskID: h.stateRunID, MaxSteps: bound, MaxStepsStated: true,
	})
	prev := runTreeBudget
	runTreeBudget = h.tb
	t.Cleanup(func() { runTreeBudget = prev })
	ra := emilRunAuthority(ctx)
	if ra == nil {
		return nil
	}
	return ra.Budget
}

type budgetRow struct {
	tenant, subject, deployment, key string
	limit, spent, reserved           int64
}

// rows reads the ledger the way an out-of-band verifier does: plain SQL, no product
// face involved.
func (h *h109Harness) rows(t *testing.T) []budgetRow {
	t.Helper()
	rs, err := h.pool.Query(context.Background(),
		`SELECT tenant, subject, deployment, key, "limit", spent, reserved FROM ahp_model.budgets
		  WHERE task = $1 OR task LIKE $2 ORDER BY key`,
		"treedelegate-run:"+h.stateRunID, "treedelegate-child:"+h.stateRunID+":%")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rs.Close()
	var out []budgetRow
	for rs.Next() {
		var r budgetRow
		if err := rs.Scan(&r.tenant, &r.subject, &r.deployment, &r.key, &r.limit, &r.spent, &r.reserved); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, r)
	}
	if err := rs.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func (h *h109Harness) rootRow(t *testing.T) budgetRow {
	t.Helper()
	for _, r := range h.rows(t) {
		if r.key == treedelegate.RunStepBudgetKey {
			return r
		}
	}
	t.Fatalf("no run-step budget row for %s (rows: %+v)", h.runID, h.rows(t))
	return budgetRow{}
}

// TestBrowserRunChargeStepZeroFilesUnderTheAttestedScope is leg (a): a fresh
// browser-initiated run must get past step 0, and its row must carry the deployment
// the verified credential named -- never an empty one.
func TestBrowserRunChargeStepZeroFilesUnderTheAttestedScope(t *testing.T) {
	h := newH109Harness(t)
	req := h.browserRequest(t, 3)

	// The carried scope really is the one the ledger refuses; otherwise this leg
	// would prove nothing about the defect.
	if err := h.carried.Validate(); err == nil {
		t.Fatal("the harness must reproduce a browser scope the ledger refuses")
	}
	if err := h.tb.ChargeStep(context.Background(), req, 0); err != nil {
		t.Fatalf("step 0 of a browser run must be chargeable, got %v", err)
	}
	row := h.rootRow(t)
	if row.deployment != h.real.Deployment {
		t.Fatalf("deployment = %q, want %q from the verified task token", row.deployment, h.real.Deployment)
	}
	if row.tenant != h.real.Tenant || row.subject != h.real.Subject {
		t.Fatalf("row scope = %q/%q, want %q/%q", row.tenant, row.subject, h.real.Tenant, h.real.Subject)
	}
	if row.limit != 3 || row.spent != 1 {
		t.Fatalf("row numbers = limit %d spent %d, want 3 and 1", row.limit, row.spent)
	}
}

// TestSameRunKeepsOneScopeLabelAcrossChargeAndDelegation is leg (b): steps charged
// before and after a resume, plus the delegation leg of the same run, must land in
// ONE scope. This is the assertion the acceptance of A->B depends on: the verifier
// has to be able to name one scope for one run.
func TestSameRunKeepsOneScopeLabelAcrossChargeAndDelegation(t *testing.T) {
	h := newH109Harness(t)

	if err := h.tb.ChargeStep(context.Background(), h.browserRequest(t, 3), 0); err != nil {
		t.Fatalf("charge step 0: %v", err)
	}
	// A resume hands the runner a fresh request with the same carried scope and a
	// re-minted token: the label must not move.
	resumed := h.browserRequest(t, 3)
	resumed.TaskToken = h.taskToken(t, auth.Claims{Deployment: h.real.Deployment, Actors: []string{h.real.Deployment}, Depth: 1}, time.Minute)
	if err := h.tb.ChargeStep(context.Background(), resumed, 1); err != nil {
		t.Fatalf("charge step 1 after resume: %v", err)
	}
	budget := h.delegate(t, h.runToken(t), 3)
	if budget == nil {
		t.Fatal("a browser run with a stated bound must get a budget tree on its delegation leg")
	}
	if _, err := budget.Reserve(context.Background(), "delegate-1", 1); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	labels := map[string]int{}
	for _, r := range h.rows(t) {
		labels[r.tenant+"|"+r.subject+"|"+r.deployment]++
	}
	if len(labels) != 1 {
		t.Fatalf("one logical run is accounted in %d scopes: %v", len(labels), labels)
	}
	want := h.real.Tenant + "|" + h.real.Subject + "|" + h.real.Deployment
	if _, ok := labels[want]; !ok {
		t.Fatalf("the one scope is %v, want %q", labels, want)
	}
	if row := h.rootRow(t); row.spent != 2 {
		t.Fatalf("spent = %d, want 2 (step 0 and the resumed step 1 in the same tree)", row.spent)
	}
}

// TestDelegatedChildGetsTheParentsRemainder is leg (c), asserted by numbers rather
// than by artifacts: a parent that spent 2 of 3 hands its child the remaining 1,
// never a fresh 3; a parent with nothing left refuses instead of granting a quiet
// zero; and a refusal moves no budget.
func TestDelegatedChildGetsTheParentsRemainder(t *testing.T) {
	h := newH109Harness(t)
	for step := 0; step < 2; step++ {
		if err := h.tb.ChargeStep(context.Background(), h.browserRequest(t, 3), step); err != nil {
			t.Fatalf("charge step %d: %v", step, err)
		}
	}
	budget := h.delegate(t, h.runToken(t), 3)
	if budget == nil {
		t.Fatal("expected a budget tree for a run with a stated bound")
	}
	grant, err := budget.Reserve(context.Background(), "delegate-remainder", 3)
	if err != nil {
		t.Fatalf("reserve of the remainder: %v", err)
	}
	if grant.Granted != 1 {
		t.Fatalf("child bound = %d, want the parent's remainder 1 (A spent 2 of 3), not the parent's own bound", grant.Granted)
	}
	row := h.rootRow(t)
	if row.limit != 3 || row.spent != 2 || row.reserved != 1 {
		t.Fatalf("tree = limit %d spent %d reserved %d, want 3/2/1", row.limit, row.spent, row.reserved)
	}
	if _, err := budget.Reserve(context.Background(), "delegate-exhausted", 1); err == nil {
		t.Fatal("an exhausted remainder must refuse the delegation")
	}
	if after := h.rootRow(t); after.reserved != 1 {
		t.Fatalf("reserved = %d after a refused delegation, want 1 (a refusal must not move budget)", after.reserved)
	}
}

// TestUnattestableTokenGivesNoScope is leg (d): nothing that is not THIS run's
// verified task credential may name an accounting scope, and a refusal must not
// leave a row behind under a guessed label.
func TestUnattestableTokenGivesNoScope(t *testing.T) {
	cases := []struct {
		name   string
		token  func(h *h109Harness, t *testing.T) string
		wantOK bool
	}{
		{"run's own task token", func(h *h109Harness, t *testing.T) string { return h.runToken(t) }, true},
		{"no token at all", func(_ *h109Harness, _ *testing.T) string { return "" }, false},
		{"human token", func(h *h109Harness, t *testing.T) string {
			return h.taskToken(t, auth.Claims{Kind: auth.KindHuman}, time.Hour)
		}, false},
		{"token minted for another workload", func(h *h109Harness, t *testing.T) string {
			return h.taskToken(t, auth.Claims{Deployment: "attacker-vc", Actors: []string{"attacker-vc"}}, 5*time.Minute)
		}, false},
		{"tampered signature", func(h *h109Harness, t *testing.T) string {
			tok := h.taskToken(t, auth.Claims{Deployment: h.real.Deployment, Actors: []string{h.real.Deployment}}, 5*time.Minute)
			return tok[:len(tok)-4] + "AAAA"
		}, false},
		{"expired token", func(h *h109Harness, t *testing.T) string {
			tok := h.taskToken(t, auth.Claims{Deployment: h.real.Deployment, Actors: []string{h.real.Deployment}}, time.Second)
			time.Sleep(2 * time.Second)
			return tok
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newH109Harness(t, 2)
			req := h.browserRequest(t, 2)
			req.TaskToken = tc.token(h, t)

			err := h.tb.ChargeStep(context.Background(), req, 0)
			if tc.wantOK {
				if err != nil {
					t.Fatalf("a run's own task token must name its scope, got %v", err)
				}
				if n := len(h.rows(t)); n != 1 {
					t.Fatalf("rows = %d, want the one row this run is entitled to", n)
				}
				if h.delegate(t, req.TaskToken, 2) == nil {
					t.Fatal("the delegation leg of an attested run must get a budget tree")
				}
				return
			}
			if err == nil {
				t.Fatal("an unattestable scope was accepted for accounting")
			}
			if n := len(h.rows(t)); n != 0 {
				t.Fatalf("a refused charge left %d ledger rows behind: %+v", n, h.rows(t))
			}
			if b := h.delegate(t, req.TaskToken, 2); b == nil {
				t.Fatal("configured tree must not fall back to an unbudgeted child")
			} else if _, err := b.Reserve(context.Background(), "invalid-authority", 1); err == nil {
				t.Fatal("the delegation leg accepted an unattestable scope")
			}
		})
	}
}

// TestTokenOfAnotherTenantNamesNoScope isolates the tenant rule: this credential
// really verifies (published key, workload this attester is pinned to), so only the
// tenant comparison can stop it naming a scope belonging to another tenant.
func TestTokenOfAnotherTenantNamesNoScope(t *testing.T) {
	h := newH109Harness(t)
	foreign := contracts.Scope{Tenant: h.real.Tenant + "-foreign", Deployment: h.real.Deployment, Subject: h.real.Subject}
	att := runScopeAttester{verifier: h.att.verifier, taskAudience: h.aud, workload: auth.WorkloadID(foreign)}
	tok := h.taskToken(t, auth.Claims{Tenant: foreign.Tenant, Deployment: foreign.Deployment,
		Actors: []string{foreign.Deployment}}, 5*time.Minute)

	if _, err := att.complete(h.carried, tok); err == nil {
		t.Fatal("a credential of another tenant named a scope")
	} else if !strings.Contains(err.Error(), "tenant does not match") {
		t.Fatalf("the refusal does not name the rule it applied: %v", err)
	}
}

// TestAttesterNeverRewritesACompletedScope keeps the fix from becoming a way to move
// records between scopes: a scope that already names its deployment is the
// authority's own statement, and a credential naming some other deployment cannot
// overrule it.
func TestAttesterNeverRewritesACompletedScope(t *testing.T) {
	h := newH109Harness(t)
	other := h.taskToken(t, auth.Claims{Deployment: "some-other-vc", Actors: []string{"some-other-vc"}}, 5*time.Minute)
	got, err := h.att.complete(h.real, other)
	if err != nil {
		t.Fatalf("a complete scope must stand on its own: %v", err)
	}
	if got != h.real {
		t.Fatalf("scope = %+v, want the carried %+v untouched", got, h.real)
	}
}
