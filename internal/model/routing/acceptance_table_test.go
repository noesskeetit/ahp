package routing_test

// Legs for mounting a provider table on the ACCEPTED front (PDF rev3.2
// :1620-1630). The risk of routing an accepted deployment is specific and known:
// if the first route does not match what the accepted journey actually sends,
// every journey task starts failing with a routing refusal. These legs pin the
// opposite -- the canonical table answers a journey-shaped request, and the probe
// table does not, which is what makes the refusal leg testable at all.

import (
	"path/filepath"
	"testing"

	"github.com/ahp-platform/ahp/internal/model/routing"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// journeyScope is the tenant the accepted journey runs under
// (scripts/journey-deployed.mjs: AHP_DEPLOYED_TENANT, default "djourney").
func journeyScope() contracts.Scope {
	return contracts.Scope{Tenant: "djourney", Subject: "journey", Deployment: "d-delivery"}
}

// journeyRequest is what a bridged turn asks for today: no labels (the execution
// path carries none yet), no execution class (no deployment states one), and the
// model named by the environment.
func journeyRequest() routing.Request {
	return routing.Request{Labels: labels.Set{}, Class: "", Model: "qwen-38-27b-fp8"}
}

func loadCanonical(t *testing.T, name string) *routing.Router {
	t.Helper()
	path := filepath.Join(moduleRoot(t), "infra", "model", name)
	r, err := routing.New(path, nil)
	if err != nil {
		t.Fatalf("load %s: %v", name, err)
	}
	return r
}

// Leg 1: the canonical table must route the accepted journey. If someone reorders
// the routes or narrows the default, this reddens BEFORE an accepted journey does.
func TestCanonicalTableRoutesTheAcceptedJourney(t *testing.T) {
	r := loadCanonical(t, "providers.json")
	route, err := r.Resolve(journeyScope(), journeyRequest())
	if err != nil {
		t.Fatalf("the accepted journey would be refused by the canonical table: %v", err)
	}
	if route.Provider != "modelrun" {
		t.Fatalf("journey routed to %q, want the provider the accepted quotas are keyed by (modelrun)", route.Provider)
	}
	if route.Model != journeyRequest().Model {
		t.Fatalf("journey model = %q, want %q", route.Model, journeyRequest().Model)
	}
	if route.RuleName == "" {
		t.Fatal("a route must name the rule that produced it")
	}
}

// Leg 2: a restricted-class request must NOT land on the shared external
// provider -- it is the class the tenant-private upstream exists for.
func TestRestrictedClassDoesNotLandOnTheSharedUpstream(t *testing.T) {
	r := loadCanonical(t, "providers.json")
	req := journeyRequest()
	req.Class = "restricted"
	route, err := r.Resolve(journeyScope(), req)
	if err != nil {
		t.Fatalf("restricted class must be routable under the canonical table: %v", err)
	}
	if route.Provider == "modelrun" {
		t.Fatal("a restricted-class turn was routed to the external shared provider")
	}
	if route.Class != "restricted" {
		t.Fatalf("routed provider class = %q, want restricted", route.Class)
	}
}

// Leg 3: the probe table refuses a journey-shaped request, naming why. This is the
// table an operator mounts for one run to prove "no route = refusal, with a
// reason" on the live front; it must refuse the ordinary request, not everything
// including its own intended traffic.
func TestProbeTableRefusesOrdinaryTrafficWithAReason(t *testing.T) {
	r := loadCanonical(t, "providers.no-route-probe.json")
	_, err := r.Resolve(journeyScope(), journeyRequest())
	if err == nil {
		t.Fatal("the probe table accepted an unclassified request: it cannot demonstrate a routing refusal")
	}
	if reason := contracts.ReasonOf(err); reason != routing.ReasonNoRoute {
		t.Fatalf("probe refusal reason = %q, want %q", reason, routing.ReasonNoRoute)
	}
	// And it does serve what it claims to serve, so the probe is a routing
	// decision rather than a broken file. The probe model is deliberately not a
	// production id: the table cannot route real traffic even if it is mounted by
	// mistake, and it holds no copy of production configuration to drift from.
	probe := routing.Request{Class: "restricted", Model: "probe-model-not-routed"}
	route, err := r.Resolve(journeyScope(), probe)
	if err != nil {
		t.Fatalf("the probe table must route the class it declares: %v", err)
	}
	if route.Provider != "probe-restricted-only" {
		t.Fatalf("probe request routed to %q, want probe-restricted-only", route.Provider)
	}
}
