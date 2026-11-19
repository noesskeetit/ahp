package execbridge

// ARCH-02: the provider the router chose must be the socket the request left on.
//
// The finding was a chain that decided a provider and then ignored it:
// gateway_model computed (model, provider), the provider bought a quota window,
// and every generation was dialled by the ONE client built from the ONE secret in
// the environment (bridge.go -> model.NewService(cfg.Inference) -> s.inf.Stream).
// An operator reading the ledger could therefore see "modelrun-in-tenant" for a
// request that had physically left through the external upstream, and the two
// copies -- which in infra/model/providers.json even shared one secret_ref -- were
// indistinguishable on the wire.
//
// These legs put TWO REAL HTTP sinks behind two declared copies and ask the only
// question that settles it: who received the bytes.
//
// Honest boundary, stated where it belongs: two local sinks prove ROUTING -- the
// decision selects the destination. They do not prove that a second real upstream
// exists; that remains an external prerequisite of the platform, and "provider
// copies are live" must not be claimed from these legs.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// qwenSecretFor is the runtime secret of one sink: its own endpoint, its own key.
func qwenSecretFor(s *qwenSink) secretstore.QwenSecret {
	return secretstore.QwenSecret{APIKey: "test-key-" + s.name, BaseURL: s.srv.URL + "/v1", Model: "model-under-test"}
}

// qwenSink is one OpenAI-compatible endpoint that remembers who asked.
type qwenSink struct {
	name string
	srv  *httptest.Server
	mu   sync.Mutex
	hits []string
}

func newQwenSink(t *testing.T, name, answer string) *qwenSink {
	t.Helper()
	s := &qwenSink{name: name}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-key-"+name {
			t.Errorf("sink %s received another provider's credential", name)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			t.Errorf("sink %s: path = %s, want .../chat/completions", name, r.URL.Path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":%q}}]}\n\n", answer)
		fmt.Fprintf(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":%d,\"completion_tokens\":%d,\"total_tokens\":%d}}\n\n",
			11, 7, 18)
		fmt.Fprint(w, "data: [DONE]\n\n")
		s.mu.Lock()
		s.hits = append(s.hits, r.Host)
		s.mu.Unlock()
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *qwenSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.hits)
}

// tableTwoCopySockets declares the same upstream twice, with DIFFERENT secret
// references -- the shape the canonical table already has, which until this file
// was written always resolved to one client.
const tableTwoCopySockets = `{
  "version": 1,
  "providers": [
    { "name": "copy-a", "class": "standard",
      "audience": { "kind": "external", "allow_external": false },
      "secret_ref": "ahp-hive/test/h21-copy-a", "models": ["model-under-test"] },
    { "name": "copy-b", "class": "restricted",
      "audience": { "kind": "tenant" },
      "secret_ref": "ahp-hive/test/h21-copy-b", "models": ["model-under-test"] }
  ],
  "routes": [
    { "name": "restricted-to-b", "when": { "classes": ["restricted"] }, "then": { "provider": "copy-b" } },
    { "name": "default", "when": { "tenants": ["*"] }, "then": { "provider": "copy-a" } }
  ]
}`

// twoSinkSecrets maps each declared copy's secret reference to ITS OWN endpoint.
// The substitute here is the secret store only: the client, the netpolicy-free
// injected transport and the selection logic are the production ones.
func twoSinkSecrets(a, b *qwenSink) SecretResolver {
	return func(_ context.Context, kvPath string) (secretstore.QwenSecret, error) {
		switch {
		case strings.HasSuffix(kvPath, "h21-copy-a"):
			return qwenSecretFor(a), nil
		case strings.HasSuffix(kvPath, "h21-copy-b"):
			return qwenSecretFor(b), nil
		}
		return secretstore.QwenSecret{}, fmt.Errorf("unknown secret path %q", kvPath)
	}
}

func sinkFactory() inferenceFactory {
	return func(secret secretstore.QwenSecret) (model.Inference, error) {
		return model.NewQwenClientWithHTTP(secret, &http.Client{})
	}
}

func generationProvider(t *testing.T, pool *pgxpool.Pool, tenant, taskID string) string {
	t.Helper()
	var provider string
	err := pool.QueryRow(context.Background(), `
SELECT provider FROM ahp_model.generations
 WHERE tenant=$1 AND task=$2 AND key LIKE 'execbridge:%'
 ORDER BY created_at, key LIMIT 1`, tenant, taskID).Scan(&provider)
	if err != nil {
		t.Fatalf("read the provider recorded on the generation: %v", err)
	}
	return provider
}

func runBridgedTurn(t *testing.T, b *Bridge, scope contracts.Scope) string {
	t.Helper()
	taskID := "sock-" + randHex(4)
	res, err := b.Execute(context.Background(), gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	if err != nil || res.State != a2a.TaskStateCompleted {
		t.Fatalf("bridged turn failed: state=%s error=%v detail=%s", res.State, err, taskFailure(res, err))
	}
	return taskID
}

// Leg 1: the decision moves the destination, and the ledger says who it was.
func TestRoutedProviderDialsItsOwnSocket(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	sinkA := newQwenSink(t, "copy-a", "from sink a")
	sinkB := newQwenSink(t, "copy-b", "from sink b")
	infA, err := model.NewQwenClientWithHTTP(qwenSecretFor(sinkA), &http.Client{})
	if err != nil {
		t.Fatalf("client for copy-a: %v", err)
	}

	// Quotas are mounted and named after copy-a -- the copy the environment
	// configured -- so a charge booked against copy-b can only come from the
	// ROUTE, never from the environment name.
	quotas := model.NewQuotaStore(pool)
	if err := quotas.Migrate(ctx); err != nil {
		t.Fatalf("quotas migrate: %v", err)
	}

	scopeStandard := uniqueScope()
	scopeRestricted := uniqueScope()
	for _, scope := range []contracts.Scope{scopeStandard, scopeRestricted} {
		for _, provider := range []string{"copy-a", "copy-b"} {
			if err := quotas.Set(ctx, model.ProviderQuota{Provider: provider, Tenant: scope.Tenant, RPM: 10, TPM: 100000, WindowSeconds: 60}); err != nil {
				t.Fatal(err)
			}
		}
	}

	b, err := New(ctx, pool, Config{
		Inference:          infA,
		MaxSteps:           2,
		TaskTokenLimit:     20000,
		StepReserve:        4000,
		QuotaProvider:      "copy-a",
		QuotaWindowSeconds: 60,
		RoutingFile:        writeProviderTable(t, tableTwoCopySockets),
		SecretResolver:     twoSinkSecrets(sinkA, sinkB),
		InferenceFactory:   sinkFactory(),
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}

	// Standard class: the default route, the configured copy, sink A.
	taskA := runBridgedTurn(t, b, scopeStandard)
	if sinkA.count() == 0 {
		t.Fatalf("the default route produced no request at sink A (sink B saw %d)", sinkB.count())
	}
	if got := generationProvider(t, pool, scopeStandard.Tenant, taskA); got != "copy-a" {
		t.Fatalf("generation recorded provider %q, want copy-a: the ledger cannot say who served it", got)
	}

	// Restricted class: a DIFFERENT copy must receive the bytes.
	beforeA, beforeB := sinkA.count(), sinkB.count()
	b.executionClass = "restricted"
	taskB := runBridgedTurn(t, b, scopeRestricted)
	if sinkA.count() != beforeA {
		t.Fatal("restricted request leaked to the default provider")
	}
	if sinkB.count() == beforeB {
		t.Fatalf("the restricted route reached no second sink: sink A %d->%d, sink B unchanged at %d -- "+
			"the provider was attributed, not dialled", beforeA, sinkA.count(), beforeB)
	}
	if got := generationProvider(t, pool, scopeRestricted.Tenant, taskB); got != "copy-b" {
		t.Fatalf("generation recorded provider %q, want copy-b", got)
	}

	// The quota window that paid is the copy that served, not the one the
	// environment named.
	key := stepKey(taskB, 0, 0)
	providers, err := quotas.AdmittedProviders(ctx, scopeRestricted.Tenant, key)
	if err != nil {
		t.Fatalf("admission lookup: %v", err)
	}
	if len(providers) != 1 || providers[0] != "copy-b" {
		t.Fatalf("quota admitted against %v, want exactly [copy-b] -- the actual provider must be the one charged", providers)
	}
}

// Leg 2: a declared copy that cannot be resolved is refused BEFORE anything is
// booked, and never served by the configured client.
func TestUnresolvableProviderCopyRefusesBeforeBooking(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	sinkA := newQwenSink(t, "copy-a", "from sink a")
	sinkB := newQwenSink(t, "copy-b", "must never be reached")
	infA, err := model.NewQwenClientWithHTTP(qwenSecretFor(sinkA), &http.Client{})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	scope := uniqueScope()

	failing := func(_ context.Context, kvPath string) (secretstore.QwenSecret, error) {
		if strings.HasSuffix(kvPath, "h21-copy-a") {
			return qwenSecretFor(sinkA), nil
		}
		return secretstore.QwenSecret{}, fmt.Errorf("secret store has no such path")
	}

	b, err := New(ctx, pool, Config{
		Inference:        infA,
		MaxSteps:         2,
		TaskTokenLimit:   20000,
		StepReserve:      4000,
		ExecutionClass:   "restricted",
		RoutingFile:      writeProviderTable(t, tableTwoCopySockets),
		SecretResolver:   failing,
		InferenceFactory: sinkFactory(),
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}

	taskID := "sock-" + randHex(4)
	res, err := b.Execute(ctx, gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	failure := taskFailure(res, err)
	if !strings.Contains(failure, ReasonProviderSecretUnresolved) {
		t.Fatalf("the refusal must name %q, got: %s", ReasonProviderSecretUnresolved, failure)
	}
	if sinkA.count() != 0 || sinkB.count() != 0 {
		t.Fatalf("an unresolvable copy reached a socket (a=%d b=%d): substitution by the default client",
			sinkA.count(), sinkB.count())
	}
	assertNothingBooked(t, pool, scope.Tenant)
}

// Leg 3: with no provider table, nothing about the wiring changes -- the one
// configured client serves, and the generation names no provider.
func TestNoRoutingTableLeavesOneClientAndNoProviderClaim(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	sink := newQwenSink(t, "only", "plain answer")
	inf, err := model.NewQwenClientWithHTTP(qwenSecretFor(sink), &http.Client{})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	scope := uniqueScope()
	b, err := New(ctx, pool, Config{
		Inference: inf, MaxSteps: 2, TaskTokenLimit: 20000, StepReserve: 4000,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	taskID := runBridgedTurn(t, b, scope)
	if sink.count() == 0 {
		t.Fatal("the configured client served nothing")
	}
	if got := generationProvider(t, pool, scope.Tenant, taskID); got != "" {
		t.Fatalf("generation names provider %q with no routing mounted: a claim the deployment cannot support", got)
	}
}
