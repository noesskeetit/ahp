package execbridge

// The operator's fail-closed quota profile (PDF rev3.2 :323 "квоты RPM/TPM,
// попытки" and :1627 list provider quotas among what Model Gateway owns; :2495
// bounds each installation's model quotas; :1542 keys quotas by tenant and
// execution class).
//
// Measured before this profile existed: with AHP_MODEL_PROVIDER unset the bridge
// logged "all tenants unlimited" and served the plane anyway, so a deployment
// could advertise bounded tenants while admitting every request, and with the
// provider named but a tenant unseeded the admission answered "unlimited" --
// which is not a quota, it is the absence of one. Root's acceptance wording was
// explicit: "all tenants unlimited" is not accepted.
//
// The profile is opt-in. Off (the default, and what every existing acceptance
// path runs), behaviour is byte-identical to before, and leg 3 pins that.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"

	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

func runBridgedTurnResult(t *testing.T, b *Bridge, scope contracts.Scope, label string) (string, gateway.ExecResult, error) {
	t.Helper()
	taskID := label + "-" + randHex(4)
	res, err := b.Execute(context.Background(), gateway.ExecInput{
		Scope: scope,
		Task:  gateway.TaskRow{ID: taskID, ContextID: "ctx-" + taskID},
		Input: a2a.NewMessage(a2a.MessageRoleUser, a2a.NewTextPart("answer")),
	})
	return taskID, res, err
}

// Leg 1: an operator who declares quotas mandatory is refused a gateway that
// holds none, and the refusal names both switches so the fix is obvious from the
// message alone.
func TestQuotaRequiredRefusesToMountWithoutAProvider(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	inf := &fakeInference{text: "never reached", usage: modelUsage()}

	_, err := New(ctx, pool, Config{
		Inference: inf, MaxSteps: 2, TaskTokenLimit: 20000, StepReserve: 4000,
		QuotaRequired: true,
	})
	if err == nil {
		t.Fatal("a quotas-mandatory deployment mounted with no provider named: every tenant would have been unlimited")
	}
	if got := contracts.ReasonOf(err); got != ReasonQuotaRequiredUnconfigured {
		t.Fatalf("refusal reason = %q, want %q", got, ReasonQuotaRequiredUnconfigured)
	}
	for _, want := range []string{"AHP_MODEL_QUOTA_REQUIRED", "AHP_MODEL_PROVIDER"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the mount refusal must name %s so an operator can fix it from the message: %v", want, err)
		}
	}

	// Anti-vacuity: the same configuration without the claim mounts. The refusal
	// is about the claim, not about the shape of the config.
	if _, err := New(ctx, pool, Config{
		Inference: inf, MaxSteps: 2, TaskTokenLimit: 20000, StepReserve: 4000,
	}); err != nil {
		t.Fatalf("the default profile must still mount (accepted paths unchanged): %v", err)
	}
}

// Leg 2: with quotas mandatory, a tenant that carries no limit row is REFUSED --
// not served an unbounded window, and not quietly zeroed. Seeding the tenant is
// the remedy, and the same turn then runs.
func TestQuotaRequiredRefusesAnUnseededTenant(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	provider := "copy-required-" + randHex(4)
	inf := &fakeInference{text: "answer", usage: modelUsage()}

	b, err := New(ctx, pool, Config{
		Inference: inf, MaxSteps: 2, TaskTokenLimit: 20000, StepReserve: 4000,
		QuotaProvider: provider, QuotaRequired: true, QuotaWindowSeconds: 60,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}

	_, res, err := runBridgedTurnResult(t, b, scope, "unbounded")
	detail := taskFailure(res, err)
	if res.State == a2a.TaskStateCompleted {
		t.Fatalf("an unseeded tenant was served under mandatory quotas: %s", detail)
	}
	if !strings.Contains(detail, model.ReasonQuotaUnboundedTenant) || !strings.Contains(detail, scope.Tenant) {
		t.Fatalf("the refusal must name %s and the tenant so the operator can seed it, got: %s",
			model.ReasonQuotaUnboundedTenant, detail)
	}
	if inf.count() != 0 {
		t.Fatalf("the provider was called %d times for a request the quota policy refused: silence is not a refusal", inf.count())
	}
	assertNothingBooked(t, pool, scope.Tenant)

	// Seed the tenant through the operator's table and the SAME deployment serves
	// it: the answer to a missing limit is a limit, not a permanent lockout.
	quotas := b.ProviderQuotas()
	if quotas == nil {
		t.Fatal("the mounted gate is not readable by the operator")
	}
	if err := quotas.Set(ctx, model.ProviderQuota{
		Provider: provider, Tenant: scope.Tenant, RPM: 10, TPM: 100_000, WindowSeconds: 60,
	}); err != nil {
		t.Fatalf("seed the tenant limit: %v", err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = quotas.Delete(cleanup, provider, scope.Tenant)
	})
	taskID, res, err := runBridgedTurnResult(t, b, scope, "seeded")
	if err != nil || res.State != a2a.TaskStateCompleted {
		t.Fatalf("a seeded tenant was not served under mandatory quotas: state=%s error=%v detail=%s", res.State, err, taskFailure(res, err))
	}
	if inf.count() == 0 {
		t.Fatal("the seeded turn produced no provider call")
	}
	w, err := quotas.Window(ctx, provider, scope.Tenant)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if w.Unlimited || w.Requests < 1 {
		t.Fatalf("the seeded tenant's spend did not land in its own window: %+v (task %s)", w, taskID)
	}
}

// Leg 3: the default profile is unchanged -- a named provider with no rows leaves
// tenants unlimited and the plane serving, exactly as every accepted journey
// measured so far. Without this leg the profile could be "fixed" by making the
// default fail closed behind everyone's back.
func TestQuotaOptionalLeavesAnUnseededTenantUnbounded(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	scope := uniqueScope()
	provider := "copy-optional-" + randHex(4)
	inf := &fakeInference{text: "answer", usage: modelUsage()}

	b, err := New(ctx, pool, Config{
		Inference: inf, MaxSteps: 2, TaskTokenLimit: 20000, StepReserve: 4000,
		QuotaProvider: provider, QuotaWindowSeconds: 60,
	})
	if err != nil {
		t.Fatalf("new bridge: %v", err)
	}
	if b.ProviderQuotas().RequireExplicitLimitsReports() {
		t.Fatal("the default profile must not require explicit limits")
	}
	if _, res, err := runBridgedTurnResult(t, b, scope, "optional"); err != nil || res.State != a2a.TaskStateCompleted {
		t.Fatalf("the default profile changed an accepted path: state=%s error=%v detail=%s", res.State, err, taskFailure(res, err))
	}

	w, err := b.ProviderQuotas().Window(ctx, provider, scope.Tenant)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if !w.Unlimited {
		t.Fatalf("an unseeded tenant under the default profile is no longer unlimited: %+v -- the opt-in leaked into the default", w)
	}
}
