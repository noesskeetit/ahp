package routing_test

import (
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func scopeFor(tenant string) contracts.Scope {
	return contracts.Scope{Tenant: tenant, Subject: "s-rootuser", Deployment: "d-main"}
}

func writeFile(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := dir + "/providers.json"
	if err := writeFileAll(path, body); err != nil {
		t.Fatalf("write: %v", err)
	}
	return path
}

func mustRouter(t *testing.T, body string) *routing.Router {
	t.Helper()
	r, err := routing.New(writeFile(t, body), nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	return r
}

const twoProviderTable = `{
  "version": 1,
  "providers": [
    {"name": "public-run", "class": "standard", "audience": {"kind": "external"},
     "secret_ref": "ahp-hive/execution/qwen", "models": ["model-big"]},
    {"name": "in-tenant", "class": "restricted", "audience": {"kind": "tenant", "allow_external": true},
     "secret_ref": "AHP_QWEN_SECRET_PATH", "models": ["model-small", "model-big"]}
  ],
  "routes": [
    {"name": "tenant-acme-stays-inside", "when": {"tenants": ["acme"]}, "then": {"provider": "in-tenant", "model": "model-small"}},
    {"name": "class-restricted", "when": {"classes": ["restricted"]}, "then": {"provider": "in-tenant"}},
    {"name": "default", "when": {"tenants": ["*"]}, "then": {"provider": "public-run"}}
  ]
}`

// TestIdenticalRequestDifferentLabelsTenantGoesElsewhere is the acceptance
// question in its plainest form: the same request from a different tenant has to
// end up at a different provider, because that is what a route table is for.
func TestIdenticalRequestDifferentLabelsTenantGoesElsewhere(t *testing.T) {
	r := mustRouter(t, twoProviderTable)

	outside, err := r.Resolve(scopeFor("other"), routing.Request{Model: "model-big"})
	if err != nil {
		t.Fatalf("resolve for other tenant: %v", err)
	}
	inside, err := r.Resolve(scopeFor("acme"), routing.Request{Model: "model-big"})
	if err != nil {
		t.Fatalf("resolve for acme: %v", err)
	}
	if outside.Provider != "public-run" || outside.Model != "model-big" {
		t.Fatalf("other tenant routed to %+v, want public-run/model-big", outside)
	}
	if inside.Provider != "in-tenant" || inside.Model != "model-small" {
		t.Fatalf("acme routed to %+v, want in-tenant/model-small (policy overrides the model too)", inside)
	}
	if inside.RuleName != "tenant-acme-stays-inside" {
		t.Fatalf("acme matched %q, want the rule to be named in the decision", inside.RuleName)
	}
}

// TestClassRoutesSeparately covers the execution class dimension (PDF :1532,
// :1542 -- priority classes per tenant and class).
func TestClassRoutesSeparately(t *testing.T) {
	r := mustRouter(t, twoProviderTable)
	plain, err := r.Resolve(scopeFor("other"), routing.Request{Model: "model-big"})
	if err != nil {
		t.Fatalf("plain: %v", err)
	}
	classed, err := r.Resolve(scopeFor("other"), routing.Request{Model: "model-big", Class: "restricted"})
	if err != nil {
		t.Fatalf("classed: %v", err)
	}
	if plain.Provider == classed.Provider {
		t.Fatalf("class did not change the destination: both %+v", plain)
	}
	if classed.Provider != "in-tenant" || classed.Class != "restricted" {
		t.Fatalf("classed route = %+v, want in-tenant/restricted", classed)
	}
}

// TestSecretTaintIsRefusedEverywhere: labels are a gate, not a hint. The default
// route matches this request, and it is still refused, because a route may not
// waive the platform's write policy for a destination.
func TestSecretTaintIsRefusedEverywhere(t *testing.T) {
	r := mustRouter(t, twoProviderTable)
	_, err := r.Resolve(scopeFor("other"), routing.Request{
		Model:  "model-big",
		Labels: labels.Set{Taints: []string{labels.TaintSecret}},
	})
	if err == nil {
		t.Fatal("secret-tainted data was routed to a provider: the label gate is advisory, which is the leak PDF :347 is about")
	}
	if contracts.ReasonOf(err) != routing.ReasonLabelGate {
		t.Fatalf("refusal reason = %q, want %q (err=%v)", contracts.ReasonOf(err), routing.ReasonLabelGate, err)
	}
	if contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("refusal code = %v, want forbidden", contracts.Code(err))
	}
}

// TestSubjectRestrictedDataIsNotSentToASharedUpstream: data restricted to one
// subject may only reach a private audience for that subject. Both providers here
// are shared, so the honest answer is a refusal -- not "the closest provider".
func TestSubjectRestrictedDataIsNotSentToASharedUpstream(t *testing.T) {
	r := mustRouter(t, twoProviderTable)
	_, err := r.Resolve(scopeFor("acme"), routing.Request{
		Model:  "model-small",
		Labels: labels.Set{Subjects: []string{"s-onlyme"}},
	})
	if err == nil {
		t.Fatal("subject-restricted data was routed to a shared provider")
	}
	if contracts.ReasonOf(err) != routing.ReasonLabelGate {
		t.Fatalf("reason = %q, want %q (err=%v)", contracts.ReasonOf(err), routing.ReasonLabelGate, err)
	}
}

// TestNoRouteIsARefusalNotAFallback: a configured table that does not accept the
// request must not silently use whatever provider is mounted.
func TestNoRouteIsARefusalNotAFallback(t *testing.T) {
	body := `{
	  "version": 1,
	  "providers": [{"name": "only", "audience": {"kind": "external"}, "secret_ref": "AHP_X_PATH", "models": ["m1"]}],
	  "routes": [{"name": "just-acme", "when": {"tenants": ["acme"]}, "then": {"provider": "only"}}]
	}`
	r := mustRouter(t, body)
	_, err := r.Resolve(scopeFor("someone-else"), routing.Request{Model: "m1"})
	if err == nil {
		t.Fatal("a tenant no route accepts was served anyway")
	}
	if contracts.ReasonOf(err) != routing.ReasonNoRoute {
		t.Fatalf("reason = %q, want %q (err=%v)", contracts.ReasonOf(err), routing.ReasonNoRoute, err)
	}
}

// TestModelNotOfferedByRoutedProvider: a route cannot invent a model.
func TestModelNotOfferedByRoutedProvider(t *testing.T) {
	r := mustRouter(t, twoProviderTable)
	_, err := r.Resolve(scopeFor("acme"), routing.Request{Model: "model-big"})
	if err != nil {
		t.Fatalf("baseline resolve: %v", err)
	}
	body := strings.Replace(twoProviderTable, `"models": ["model-small", "model-big"]`, `"models": ["model-small"]`, 1)
	r2 := mustRouter(t, body)
	_, err = r2.Resolve(scopeFor("acme"), routing.Request{Model: "model-big"})
	if err != nil {
		t.Fatalf("expected the rule's own model override to be validated at load, got %v", err)
	}
	// A caller asking for a model the routed provider does not have is refused.
	body3 := strings.Replace(twoProviderTable, `{"name": "tenant-acme-stays-inside", "when": {"tenants": ["acme"]}, "then": {"provider": "in-tenant", "model": "model-small"}}`,
		`{"name": "tenant-acme-stays-inside", "when": {"tenants": ["acme"]}, "then": {"provider": "in-tenant"}}`, 1)
	r3 := mustRouter(t, body3)
	_, err = r3.Resolve(scopeFor("acme"), routing.Request{Model: "model-mystery"})
	if contracts.ReasonOf(err) != routing.ReasonModelNotOffered {
		t.Fatalf("reason = %q, want %q (err=%v)", contracts.ReasonOf(err), routing.ReasonModelNotOffered, err)
	}
}

// TestReloadKeepsLastGoodTable is the reload policy: a broken edit must not stop
// the gateway from routing, and must not be silent either.
func TestReloadKeepsLastGoodTable(t *testing.T) {
	body := twoProviderTable
	path := writeFile(t, body)
	r, err := routing.New(path, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	before := r.Digest()

	if changed, err := r.Reload(); err != nil || changed {
		t.Fatalf("reload of an unchanged file: changed=%v err=%v, want no-op", changed, err)
	}

	if err := writeFileAll(path, `{"version": 1, "providers": [{"name": "public-run", "audience": {"kind": "nonsense"}, "secret_ref": "AHP_X", "models": ["m"]}]}`); err != nil {
		t.Fatalf("write broken: %v", err)
	}
	changed, err := r.Reload()
	if err == nil || changed {
		t.Fatalf("broken config was accepted: changed=%v err=%v", changed, err)
	}
	if r.Digest() != before {
		t.Fatalf("a refused reload changed the live table: %s -> %s", before, r.Digest())
	}
	if _, err := r.Resolve(scopeFor("other"), routing.Request{Model: "model-big"}); err != nil {
		t.Fatalf("routing after a refused reload must still work: %v", err)
	}

	next := strings.Replace(body, `"models": ["model-big"]`, `"models": ["model-big", "model-new"]`, 1)
	if err := writeFileAll(path, next); err != nil {
		t.Fatalf("write new: %v", err)
	}
	changed, err = r.Reload()
	if err != nil || !changed {
		t.Fatalf("a valid change must be applied: changed=%v err=%v", changed, err)
	}
	if r.Digest() == before {
		t.Fatal("digest unchanged after a real change")
	}
	got, err := r.Resolve(scopeFor("other"), routing.Request{Model: "model-new"})
	if err != nil || got.Provider != "public-run" {
		t.Fatalf("new model not routable after reload: %+v err=%v", got, err)
	}
}

// TestConfigValidationCatchesTheMistakesThatSilentlyMisroute covers the ways a
// committed table can look fine and route nothing (or everything).
func TestConfigValidationCatchesTheMistakesThatSilentlyMisroute(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"no version", `{"providers": [{"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_X", "models": ["m"]}]}`, "version"},
		{"no providers", `{"version": 1, "providers": []}`, "no providers"},
		{"duplicate provider", `{"version": 1, "providers": [
		  {"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_X", "models": ["m"]},
		  {"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_Y", "models": ["m"]}]}`, "duplicate provider"},
		{"route to unknown provider", `{"version": 1, "providers": [{"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_X", "models": ["m"]}],
		  "routes": [{"name": "r", "then": {"provider": "ghost"}}]}`, "unknown provider"},
		{"route model not advertised", `{"version": 1, "providers": [{"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_X", "models": ["m"]}],
		  "routes": [{"name": "r", "then": {"provider": "p", "model": "ghost-model"}}]}`, "does not advertise"},
		{"provider with no models", `{"version": 1, "providers": [{"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_X", "models": []}]}`, "no model"},
		{"credential pasted as reference", `{"version": 1, "providers": [{"name": "p", "audience": {"kind": "external"}, "secret_ref": "sk-live-abcdef0123456789", "models": ["m"]}]}`, "secret_ref"},
		{"unknown taint in a rule", `{"version": 1, "providers": [{"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_X", "models": ["m"]}],
		  "routes": [{"name": "r", "when": {"taints_any": ["confidential-ish"]}, "then": {"provider": "p"}}]}`, "unknown taint"},
		{"unknown field (typo protection)", `{"version": 1, "providerz": [], "providers": [{"name": "p", "audience": {"kind": "external"}, "secret_ref": "AHP_X", "models": ["m"]}]}`, "providerz"},
		{"bad audience kind", `{"version": 1, "providers": [{"name": "p", "audience": {"kind": "sort-of-external"}, "secret_ref": "AHP_X", "models": ["m"]}]}`, "audience"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := routing.Parse([]byte(tc.body))
			if err == nil {
				t.Fatalf("config accepted: %s", tc.body)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to mention %q", err.Error(), tc.want)
			}
		})
	}
}

// TestCanonicalProviderTableIsLoadableAndCarriesNoSecrets keeps the committed
// single point honest: it must load, and it must stay free of credential values.
func TestCanonicalProviderTableIsLoadableAndCarriesNoSecrets(t *testing.T) {
	path := repoFile(t, "infra/model/providers.json")
	table, err := routing.LoadFile(path)
	if err != nil {
		t.Fatalf("canonical provider table does not load: %v", err)
	}
	if table.Version == 0 {
		t.Fatal("canonical table has no version")
	}
	for _, p := range table.Providers {
		if len(p.Models) == 0 {
			t.Fatalf("provider %q advertises nothing", p.Name)
		}
	}
	r, err := routing.New(path, nil)
	if err != nil {
		t.Fatalf("canonical router: %v", err)
	}
	// Every deployment's default must resolve: a table that loads but routes
	// nothing is a self-inflicted outage.
	got, err := r.Resolve(scopeFor("anybody"), routing.Request{})
	if err != nil {
		t.Fatalf("canonical table refused an unlabelled request: %v", err)
	}
	if got.Provider == "" || got.Model == "" {
		t.Fatalf("canonical default route is empty: %+v", got)
	}
}
