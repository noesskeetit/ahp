package execbridge

// Server-side label resolution on the routing path (PDF rev3.2 :1620-1630
// "маршруты с учётом меток"). The director's ruling (2026-11-02): routing labels are
// taken from the SERVER's record of the deployment, addressed by the attested
// workload (the certificate's tenant+deployment), and never from anything the
// caller asserts. These legs therefore ask one question with real provider
// selection: do two deployments of the SAME tenant, classified differently by the
// operator, actually end up at different upstreams -- and is that visible in the
// ledger rather than only in a log line?
//
// WHY THE CLASSIFICATION IS AN "external" TAINT AND NOT A "secret" ONE
//
// pkg/labels.CheckWrite refuses a secret taint for EVERY audience: a provider call
// is an export, and secret-tainted data may not be exported to any sink at all. That
// is the product's own vocabulary and it is not negotiable here, so "keep this
// inside the tenant" cannot be expressed with `secret` -- a route asking for it would
// be a route to a refusal. The classification that does distinguish two copies is
// the one CheckWrite gates by audience kind: external-tainted data may go to an
// external sink and must NOT be published to a trusted one. The negative leg below
// pins the secret case as the refusal it is.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/model/secretstore"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// tableTwoCopies is one trusted upstream kept inside the tenant plus a second copy
// of it outside the trust boundary. Same shape the acceptance stand uses: what
// differs between the two copies is the audience the data may reach, not the model
// family.
const tableTwoCopies = `{
  "version": 1,
  "providers": [
    {"name": "p-in-tenant", "class": "standard", "audience": {"kind": "tenant"},
     "secret_ref": "ahp-hive/execution/routing-test", "models": ["model-in-tenant-route"]},
    {"name": "p-external", "class": "standard", "audience": {"kind": "external"},
     "secret_ref": "ahp-hive/execution/routing-test", "models": ["model-external-route"]}
  ],
  "routes": [
    {"name": "external-tainted-uses-external-copy", "when": {"taints_any": ["external"]},
     "then": {"provider": "p-external", "model": "model-external-route"}},
    {"name": "everything-else-in-tenant", "when": {"tenants": ["*"]},
     "then": {"provider": "p-in-tenant", "model": "model-in-tenant-route"}}
  ]
}`

// labelsRig mounts the bridge on the two-copy table over real Postgres and returns
// the bridge, the label store the operator would write through, and a turn runner
// that reports which model the ledger says served the task.
func labelsRig(t *testing.T) (*model.DeploymentLabelStore, func(contracts.Scope) (string, string)) {
	t.Helper()
	pool := dbPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)

	labelStore := model.NewDeploymentLabelStore(pool)
	if err := labelStore.Migrate(ctx); err != nil {
		t.Fatalf("labels migrate: %v", err)
	}
	budgets := model.NewBudgetStore(pool)
	if err := budgets.Migrate(ctx); err != nil {
		t.Fatalf("budgets migrate: %v", err)
	}

	inf := &fakeInference{text: "ok", usage: modelUsage()}
	b, err := New(ctx, pool, Config{
		Inference:        inf,
		MaxSteps:         2,
		TaskTokenLimit:   20000,
		StepReserve:      4000,
		RoutingFile:      writeProviderTable(t, tableTwoCopies),
		SecretResolver:   func(context.Context, string) (secretstore.QwenSecret, error) { return secretstore.QwenSecret{}, nil },
		InferenceFactory: func(secretstore.QwenSecret) (model.Inference, error) { return inf, nil },
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if !b.RoutingMounted() {
		t.Fatal("the provider table did not mount routing, so this leg would prove nothing")
	}
	// Routing without a label source is a gate that only ever sees the empty set,
	// which is a decision an operator has to be able to see -- so the bridge exposes
	// what it routes on.
	if b.DeploymentLabels() == nil {
		t.Fatal("routing is mounted with no label source: the label gate would only ever be asked about the empty set")
	}

	run := func(scope contracts.Scope) (string, string) {
		t.Helper()
		taskID := "lbltask-" + randHex(4)
		res, err := b.Execute(ctx, gateway.ExecInput{
			Scope: scope,
			Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
			Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
		})
		var served string
		qerr := pool.QueryRow(ctx,
			`SELECT model FROM ahp_model.generations WHERE key LIKE '%'||$1||'%' ORDER BY ctid LIMIT 1`,
			taskID).Scan(&served)
		if res.State != a2a.TaskStateCompleted {
			return "", taskFailure(res, err)
		}
		if err != nil {
			t.Fatalf("execute(%s): %v", scope.Deployment, err)
		}
		if qerr != nil {
			t.Fatalf("read generation for %s: %v", scope.Deployment, qerr)
		}
		return served, ""
	}
	return labelStore, run
}

// Leg: two deployments of one tenant, classified differently, must be served by
// different copies, and the ledger must say so.
func TestRoutingLabelsComeFromTheServerRecord(t *testing.T) {
	labelStore, run := labelsRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenant := "h21lbl" + randHex(5)
	classified := contracts.Scope{Tenant: tenant, Deployment: "d-external-tainted", Subject: "user:h21-labels"}
	unclassified := contracts.Scope{Tenant: tenant, Deployment: "d-plain", Subject: "user:h21-labels"}
	if err := labelStore.Set(ctx, classified.Tenant, classified.Deployment,
		labels.Set{Taints: []string{labels.TaintExternal}}, "test"); err != nil {
		t.Fatalf("classify deployment: %v", err)
	}

	gotClassified, failClassified := run(classified)
	if failClassified != "" {
		t.Fatalf("classified deployment turn failed: %s", failClassified)
	}
	gotPlain, failPlain := run(unclassified)
	if failPlain != "" {
		t.Fatalf("unclassified deployment turn failed: %s", failPlain)
	}

	if gotPlain != "model-in-tenant-route" {
		t.Fatalf("an unclassified deployment routed to %q, want the trusted in-tenant copy: no classification means the empty set, which a trusted sink may serve", gotPlain)
	}
	if gotClassified != "model-external-route" {
		t.Fatalf(`deployment %q carries a server-side "external" record and still routed to %q, want the external copy: `+
			`the route must be decided on the operator's record of the deployment (model.DeploymentLabelStore, keyed by the `+
			`attested tenant+deployment), because a label the caller states is intent, not a gate (internal/model/routing gates on these labels and today receives none)`,
			classified.Deployment, gotClassified)
	}
	if gotClassified == gotPlain {
		t.Fatalf("both deployments reached the same upstream %q -- the classification changed nothing", gotClassified)
	}
}

// Negative leg: a classification no provider may satisfy is a REFUSAL with a named
// reason, not a silent reroute and not a quietly unlabelled turn. Secret taint is
// the case the vocabulary itself forbids to every audience, so it also proves the
// gate is consulted on the server's record rather than bypassed by a permissive
// default route.
func TestSecretClassifiedDeploymentIsRefusedNotRerouted(t *testing.T) {
	labelStore, run := labelsRig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenant := "h21sec" + randHex(5)
	secret := contracts.Scope{Tenant: tenant, Deployment: "d-secret", Subject: "user:h21-labels"}
	if err := labelStore.Set(ctx, secret.Tenant, secret.Deployment,
		labels.Set{Taints: []string{labels.TaintSecret}}, "test"); err != nil {
		t.Fatalf("classify deployment: %v", err)
	}

	served, failure := run(secret)
	if served != "" {
		t.Fatalf("secret-classified deployment was served by %q: no provider may receive secret-tainted data, so this is an export the labels forbid (PDF :323 label gate)", served)
	}
	if failure == "" {
		t.Fatal("secret-classified deployment neither failed nor succeeded -- the turn has no terminal outcome")
	}
	for _, want := range []string{"labels_forbid_provider", "FORBIDDEN"} {
		if !strings.Contains(failure, want) {
			t.Errorf("refusal does not name %q, so an operator cannot tell a label refusal from any other failure: %s", want, failure)
		}
	}
}

// Leg, end to end through the bridge: the copy routing chose is the copy the quota
// gate charges. The deployment names one configured copy (the environment), the
// route sends a labelled deployment to the OTHER copy, and the limits that bind are
// the second copy's -- otherwise a "restricted second copy" is a name with no
// window behind it (PDF :323 "квоты RPM/TPM, попытки", :1620-1630 provider quotas).
func TestRoutedCopyIsTheOneTheQuotaGateCharges(t *testing.T) {
	pool := dbPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	labelStore := model.NewDeploymentLabelStore(pool)
	if err := labelStore.Migrate(ctx); err != nil {
		t.Fatalf("labels migrate: %v", err)
	}
	for _, store := range []interface{ Migrate(context.Context) error }{
		model.NewBudgetStore(pool),
	} {
		if err := store.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
	quotas := model.NewQuotaStore(pool)
	if err := quotas.Migrate(ctx); err != nil {
		t.Fatalf("quotas migrate: %v", err)
	}

	tenant := "h21cop" + randHex(5)
	classified := contracts.Scope{Tenant: tenant, Deployment: "d-external-tainted", Subject: "user:h21-copies"}
	if err := labelStore.Set(ctx, classified.Tenant, classified.Deployment,
		labels.Set{Taints: []string{labels.TaintExternal}}, "test"); err != nil {
		t.Fatalf("classify deployment: %v", err)
	}

	inf := &fakeInference{text: "ok", usage: modelUsage()}
	b, err := New(ctx, pool, Config{
		Inference:        inf,
		MaxSteps:         2,
		TaskTokenLimit:   20000,
		StepReserve:      4000,
		RoutingFile:      writeProviderTable(t, tableTwoCopies),
		SecretResolver:   func(context.Context, string) (secretstore.QwenSecret, error) { return secretstore.QwenSecret{}, nil },
		InferenceFactory: func(secretstore.QwenSecret) (model.Inference, error) { return inf, nil },
		// The copy this deployment names, which is NOT the copy the label route
		// will pick.
		QuotaProvider: "p-in-tenant",
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if err := quotas.Set(ctx, model.ProviderQuota{Provider: "p-in-tenant", Tenant: tenant, RPM: 50, TPM: 100_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set in-tenant limit: %v", err)
	}
	if err := quotas.Set(ctx, model.ProviderQuota{Provider: "p-external", Tenant: tenant, RPM: 1, TPM: 100_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set external limit: %v", err)
	}

	runTurn := func() (a2a.TaskState, string) {
		t.Helper()
		taskID := "cop-" + randHex(4)
		res, err := b.Execute(ctx, gateway.ExecInput{
			Scope: classified,
			Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
			Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
		})
		if err != nil {
			return res.State, err.Error()
		}
		return res.State, taskFailure(res, err)
	}

	if state, failure := runTurn(); state != a2a.TaskStateCompleted {
		t.Fatalf("first turn on the routed copy failed: %s", failure)
	}
	// The routed copy is bounded at 1 request per window. Its second turn must be
	// refused by ITS limit, and the configured copy must stay untouched -- if the
	// gate had charged the environment's copy, this turn would sail through.
	state, failure := runTurn()
	if state == a2a.TaskStateCompleted {
		t.Fatal("the routed copy's RPM limit did not bind: the turn was admitted, so the quota was charged to some other window")
	}
	if !strings.Contains(failure, "provider_rpm_exhausted") {
		t.Fatalf("refusal does not name the provider-RPM reason, so an operator cannot tell a copy quota from any other failure: %s", failure)
	}

	window, werr := quotas.Window(ctx, "p-in-tenant", tenant)
	if werr != nil {
		t.Fatalf("read in-tenant window: %v", werr)
	}
	if window.Requests != 0 || window.Tokens != 0 {
		t.Fatalf("the configured copy was charged for turns routing sent elsewhere: %+v", window)
	}
	routed, werr := quotas.Window(ctx, "p-external", tenant)
	if werr != nil {
		t.Fatalf("read routed window: %v", werr)
	}
	if routed.Requests != 1 {
		t.Fatalf("the routed copy holds %d requests, want exactly 1 (its own admitted turn, no double booking)", routed.Requests)
	}
}
