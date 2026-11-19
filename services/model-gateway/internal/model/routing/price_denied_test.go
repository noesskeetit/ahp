package routing_test

// The provider table is where an operator declares what a copy IS and what it may
// SPEND per window. It is deliberately not where a price lives.
//
// Cost honesty has a second half next to "the read model does not print money":
// nothing in the configuration may imply a number that looks like money. A
// price_per_1k_tokens field that parsed and was ignored would be worse than one
// that fails -- the operator would believe the platform prices its trees, and the
// next report would quote a figure this product never computed. Strict decoding
// already refuses unknown keys; these legs pin that the refusal covers exactly
// the fields someone would reach for when asked to "show cost in currency".

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/routing"
)

// priceBearingTables each take the accepted two-provider shape and add the field
// an operator might add under pressure to produce money.
var priceBearingTables = []struct{ name, body, wantRejected string }{
	{
		name:         "per-token price on a provider",
		body:         `{"version":1,"providers":[{"name":"only","audience":{"kind":"external"},"secret_ref":"AHP_X_PATH","models":["m1"],"price_per_1k_tokens":0.0003}],"routes":[{"name":"default","when":{"tenants":["*"]},"then":{"provider":"only"}}]}`,
		wantRejected: "price_per_1k_tokens",
	},
	{
		name:         "tariff block on a provider",
		body:         `{"version":1,"providers":[{"name":"only","audience":{"kind":"external"},"secret_ref":"AHP_X_PATH","models":["m1"],"tariff":{"currency":"RUB","input_per_1k":1.2,"output_per_1k":4.8}}],"routes":[{"name":"default","when":{"tenants":["*"]},"then":{"provider":"only"}}]}`,
		wantRejected: "tariff",
	},
	{
		name:         "currency at the table level",
		body:         `{"version":1,"currency":"RUB","providers":[{"name":"only","audience":{"kind":"external"},"secret_ref":"AHP_X_PATH","models":["m1"]}],"routes":[{"name":"default","when":{"tenants":["*"]},"then":{"provider":"only"}}]}`,
		wantRejected: "currency",
	},
	{
		name:         "a priced quota (spend limit in money)",
		body:         `{"version":1,"providers":[{"name":"only","audience":{"kind":"external"},"secret_ref":"AHP_X_PATH","models":["m1"],"quota":{"rpm":20,"tpm":120000,"window_seconds":60,"budget_rub":500}}],"routes":[{"name":"default","when":{"tenants":["*"]},"then":{"provider":"only"}}]}`,
		wantRejected: "budget_rub",
	},
}

// TestProviderTableRefusesToCarryAPrice: every money-shaped field is a refused
// configuration, not an ignored one.
func TestProviderTableRefusesToCarryAPrice(t *testing.T) {
	for _, tc := range priceBearingTables {
		t.Run(tc.name, func(t *testing.T) {
			_, err := routing.Parse([]byte(tc.body))
			if err == nil {
				t.Fatalf("a provider table carrying %q was accepted: the platform would be read as pricing its trees", tc.wantRejected)
			}
			if !strings.Contains(err.Error(), tc.wantRejected) {
				t.Fatalf("refusal must name the rejected field %q, got: %v", tc.wantRejected, err)
			}
		})
	}
}

// TestProviderTableAcceptsTheSameTableWithoutPrices keeps the legs above from
// passing because every parse fails: the identical structure minus the money
// field must be accepted, including its declared quota.
func TestProviderTableAcceptsTheSameTableWithoutPrices(t *testing.T) {
	bodies := []string{
		`{"version":1,"providers":[{"name":"only","audience":{"kind":"external"},"secret_ref":"AHP_X_PATH","models":["m1"]}],"routes":[{"name":"default","when":{"tenants":["*"]},"then":{"provider":"only"}}]}`,
		`{"version":1,"providers":[{"name":"only","audience":{"kind":"external"},"secret_ref":"AHP_X_PATH","models":["m1"],"quota":{"rpm":20,"tpm":120000,"window_seconds":60}}],"routes":[{"name":"default","when":{"tenants":["*"]},"then":{"provider":"only"}}]}`,
	}
	for i, body := range bodies {
		table, err := routing.Parse([]byte(body))
		if err != nil {
			t.Fatalf("table %d refused: %v", i, err)
		}
		if len(table.Providers) != 1 || table.Providers[0].Name != "only" {
			t.Fatalf("table %d parsed wrong: %+v", i, table.Providers)
		}
	}
	// And the accepted quota really is the spend declaration, in windows of
	// requests and tokens -- not in money.
	table, err := routing.Parse([]byte(bodies[1]))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	q := table.Providers[0].Quota
	if q == nil {
		t.Fatal("the declared quota vanished")
	}
	raw, err := json.Marshal(q)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, money := range []string{"rub", "usd", "price", "currency", "money", "tariff"} {
		if strings.Contains(strings.ToLower(string(raw)), money) {
			t.Fatalf("the accepted quota declaration carries %q: %s", money, raw)
		}
	}
}
