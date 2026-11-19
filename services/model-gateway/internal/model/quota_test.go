package model

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Provider quotas (spec: the Model Gateway owns "квоты провайдеров" beside the
// generation operations and attempts; RPM/TPM is what a real provider answers
// with under load). Everything here runs against the real Postgres the suite is
// configured with: a quota whose atomicity is not tested against a real row lock
// is a counter, not a gate.
//
// Exhaustion tests use a 3600s window on purpose. A one-minute window can roll
// between two statements of a test and turn a refusal into a success for a reason
// that has nothing to do with the code under test; the rollover behaviour gets its
// own test with a one-second window.

func newQuotaStore(t *testing.T) *QuotaStore {
	t.Helper()
	env := newModelEnv(t)
	q := NewQuotaStore(env.pool)
	if err := q.Migrate(context.Background()); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	return q
}

func quotaProvider(t *testing.T) string {
	t.Helper()
	return "prov-" + randHex(5)
}

func TestQuotaLimitsArePerTenantWithAProviderDefault(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider := quotaProvider(t)
	tenantA, tenantB := "tenant-a-"+randHex(4), "tenant-b-"+randHex(4)

	// Nothing configured is not zero: it is unlimited, and an unconfigured
	// admission writes no window rows at all.
	if _, found, err := q.Limits(ctx, provider, tenantA); err != nil || found {
		t.Fatalf("unconfigured limits: found=%v err=%v", found, err)
	}
	unlimited, err := q.Admit(ctx, provider, tenantA, "op-any", 1, 1_000_000)
	if err != nil || !unlimited.Unlimited {
		t.Fatalf("unconfigured admission must pass unlimited, got %+v err=%v", unlimited, err)
	}

	// A provider-wide default applies to every tenant that has no row of its own.
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: QuotaAnyTenant, RPM: 5, TPM: 5000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set provider default: %v", err)
	}
	// ...and a tenant row wins over it, which is the whole point of tenant-scoped
	// quotas ("свои квоты ... на установку/проект").
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenantA, RPM: 2, TPM: 100, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set tenant row: %v", err)
	}
	limitsA, _, err := q.Limits(ctx, provider, tenantA)
	if err != nil || limitsA.RPM != 2 || limitsA.TPM != 100 {
		t.Fatalf("tenant A limits = %+v err=%v, want its own rpm=2 tpm=100", limitsA, err)
	}
	limitsB, _, err := q.Limits(ctx, provider, tenantB)
	if err != nil || limitsB.RPM != 5 || limitsB.TPM != 5000 {
		t.Fatalf("tenant B limits = %+v err=%v, want the provider default 5/5000", limitsB, err)
	}

	// Removing the tenant row returns it to the provider default, not to zero.
	if err := q.Delete(ctx, provider, tenantA); err != nil {
		t.Fatalf("delete: %v", err)
	}
	limitsA, _, _ = q.Limits(ctx, provider, tenantA)
	if limitsA.RPM != 5 {
		t.Fatalf("after delete tenant A limits = %+v, expected to fall back to the provider default", limitsA)
	}
}

// Criterion: an exhausted RPM refuses the NEXT operation with a stable reason.
func TestQuotaRefusesWhenRPMExhausted(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider, tenant := quotaProvider(t), "tenant-rpm-"+randHex(4)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenant, RPM: 2, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, err := q.Admit(ctx, provider, tenant, fmt.Sprintf("op-%d", i), 1, 10); err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
	}
	_, err := q.Admit(ctx, provider, tenant, "op-over", 1, 10)
	if err == nil {
		t.Fatal("a third request passed a quota of 2 RPM")
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("refusal code = %q, want %q (a structured, branchable error)", contracts.Code(err), contracts.ErrConflict)
	}
	if contracts.ReasonOf(err) != ReasonProviderRPMExhausted {
		t.Fatalf("refusal reason = %q, want %q", contracts.ReasonOf(err), ReasonProviderRPMExhausted)
	}
	// A refusal must leave nothing counted: the rejected attempt is not in the
	// window, so the window still reads exactly 2.
	w, err := q.Window(ctx, provider, tenant)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if w.Requests != 2 || w.RemainingRequests != 0 {
		t.Fatalf("window after a refused admission = %+v, want requests 2 remaining 0", w)
	}
}

// Criterion: TPM is its own limit with its own reason -- an operator reacting to
// "requests per minute" and one reacting to "tokens per minute" do different
// things, and a refusal that cannot say which is a refusal that cannot be acted on.
func TestQuotaRefusesWhenTPMExhausted(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider, tenant := quotaProvider(t), "tenant-tpm-"+randHex(4)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenant, RPM: 100, TPM: 1000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := q.Admit(ctx, provider, tenant, "op-big", 1, 900); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	_, err := q.Admit(ctx, provider, tenant, "op-over", 1, 200)
	if contracts.ReasonOf(err) != ReasonProviderTPMExhausted {
		t.Fatalf("err = %v (reason %q), want %q", err, contracts.ReasonOf(err), ReasonProviderTPMExhausted)
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("refusal code = %q, want %q", contracts.Code(err), contracts.ErrConflict)
	}
	// The request limit was never the binding one here, and the token window must
	// not have moved.
	w, _ := q.Window(ctx, provider, tenant)
	if w.Tokens != 900 || w.RemainingTokens != 100 {
		t.Fatalf("token window = %+v, want 900 counted / 100 remaining", w)
	}
	// A small-enough request still fits: the limit is per minute, not a tripwire.
	if _, err := q.Admit(ctx, provider, tenant, "op-small", 1, 100); err != nil {
		t.Fatalf("a request that fits the remaining 100 tokens was refused: %v", err)
	}
}

// Criterion: parallel admissions cannot both pass a limit they would jointly
// cross. One unit of remainder, many racers.
func TestConcurrentQuotaAdmitsCannotOvershoot(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider, tenant := quotaProvider(t), "tenant-race-"+randHex(4)
	const limit = 3
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenant, RPM: limit, TPM: 30, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set: %v", err)
	}
	const racers = 12
	var (
		wg       sync.WaitGroup
		admitted int64
		refused  int64
	)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := q.Admit(ctx, provider, tenant, fmt.Sprintf("race-%d", i), 1, 10)
			if err != nil {
				atomic.AddInt64(&refused, 1)
				return
			}
			atomic.AddInt64(&admitted, 1)
		}(i)
	}
	close(start)
	wg.Wait()

	if admitted != limit {
		t.Fatalf("%d admissions passed a quota of %d (refused %d)", admitted, limit, refused)
	}
	w, err := q.Window(ctx, provider, tenant)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if w.Requests != limit || w.Tokens != limit*10 {
		t.Fatalf("window = %+v, want exactly %d requests and %d tokens", w, limit, limit*10)
	}
}

// A quota is per provider AND per tenant: exhausting one must not throttle a
// neighbour, and a different provider's window must not be shared either.
func TestQuotaIsIsolatedPerProviderAndTenant(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	providerA, providerB := quotaProvider(t), quotaProvider(t)
	tenantA, tenantB := "iso-a-"+randHex(4), "iso-b-"+randHex(4)
	for _, p := range []string{providerA, providerB} {
		for _, tn := range []string{tenantA, tenantB} {
			if err := q.Set(ctx, ProviderQuota{Provider: p, Tenant: tn, RPM: 1, WindowSeconds: 3600}); err != nil {
				t.Fatalf("set %s/%s: %v", p, tn, err)
			}
		}
	}
	if _, err := q.Admit(ctx, providerA, tenantA, "a-a", 1, 1); err != nil {
		t.Fatalf("admit A/A: %v", err)
	}
	if _, err := q.Admit(ctx, providerA, tenantA, "a-a-2", 1, 1); err == nil {
		t.Fatal("second request inside one exhausted tenant window was admitted")
	}
	if _, err := q.Admit(ctx, providerA, tenantB, "a-b", 1, 1); err != nil {
		t.Fatalf("tenant B was throttled by tenant A's window: %v", err)
	}
	if _, err := q.Admit(ctx, providerB, tenantA, "b-a", 1, 1); err != nil {
		t.Fatalf("tenant A was throttled by another provider's window: %v", err)
	}
}

// A retry of an ALREADY admitted attempt must not pay for it again: without this,
// every provider timeout would consume a second unit of a tenant's minute, and the
// throttle would punish exactly the clients that retry.
func TestQuotaAdmissionReplayDoesNotDoubleCharge(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider, tenant := quotaProvider(t), "tenant-replay-"+randHex(4)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenant, RPM: 2, TPM: 100, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := q.Admit(ctx, provider, tenant, "same-op", 1, 40); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	if _, err := q.Admit(ctx, provider, tenant, "same-op", 1, 40); err != nil {
		t.Fatalf("replay of an admitted attempt errored: %v", err)
	}
	w, _ := q.Window(ctx, provider, tenant)
	if w.Requests != 1 || w.Tokens != 40 {
		t.Fatalf("window after a replayed admission = %+v, want 1 request / 40 tokens", w)
	}
	// The replayed booking is still a real one: a genuinely new attempt pays.
	if _, err := q.Admit(ctx, provider, tenant, "other-op", 1, 40); err != nil {
		t.Fatalf("new attempt: %v", err)
	}
	if _, err := q.Admit(ctx, provider, tenant, "third-op", 1, 40); err == nil {
		t.Fatal("quota of 2 RPM admitted three distinct attempts")
	}
}

// A generation is admitted for the most it could generate; the window must end at
// what the provider actually billed, or a tenant is throttled by a debt it does not
// owe. Once-only: a replayed convergence changes nothing.
func TestQuotaRefundConvergesWindowWithRealUsage(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider, tenant := quotaProvider(t), "tenant-refund-"+randHex(4)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenant, RPM: 10, TPM: 1000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := q.Admit(ctx, provider, tenant, "gen-1", 1, 900); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if err := q.Refund(ctx, provider, tenant, "gen-1", 100); err != nil {
		t.Fatalf("refund: %v", err)
	}
	w, _ := q.Window(ctx, provider, tenant)
	if w.Tokens != 100 {
		t.Fatalf("window tokens = %d after the provider billed 100, want 100 (the 800 estimate returns)", w.Tokens)
	}
	if err := q.Refund(ctx, provider, tenant, "gen-1", 55); err != nil {
		t.Fatalf("replayed refund: %v", err)
	}
	w, _ = q.Window(ctx, provider, tenant)
	if w.Tokens != 100 {
		t.Fatalf("window tokens = %d after a replayed refund, want 100 (converged once)", w.Tokens)
	}
	// An unknown key is not an error: generations admitted before quotas existed
	// have no booking to converge.
	if err := q.Refund(ctx, provider, tenant, "never-admitted", 7); err != nil {
		t.Fatalf("refund of an unbooked key: %v", err)
	}
}

// The window is a window: after it rolls, the same tenant is admitted again. A
// minute-long window cannot be tested for rollover in a test, so this one asks for
// the minimum legal window.
func TestQuotaWindowRollsOver(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider, tenant := quotaProvider(t), "tenant-roll-"+randHex(4)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenant, RPM: 1, WindowSeconds: 1}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := q.Admit(ctx, provider, tenant, "roll-1", 1, 1); err != nil {
		t.Fatalf("first admit: %v", err)
	}
	_, firstErr := q.Admit(ctx, provider, tenant, "roll-2", 1, 1)
	time.Sleep(1200 * time.Millisecond)
	if _, err := q.Admit(ctx, provider, tenant, "roll-3", 1, 1); err != nil {
		t.Fatalf("a new window refused an admission (%v); the first window refused with %v", err, firstErr)
	}
}

// Criterion: rollback is explicit and touches nothing else. The three quota tables
// go away; the budget and generation planes the acceptance depends on stay.
func TestQuotaRollbackRemovesOnlyQuotaTables(t *testing.T) {
	ctx := context.Background()
	q := newQuotaStore(t)
	provider := quotaProvider(t)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: "rb", RPM: 1, WindowSeconds: 60}); err != nil {
		t.Fatalf("set: %v", err)
	}
	if _, err := q.Admit(ctx, provider, "rb", "rb-1", 1, 1); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if err := q.Rollback(ctx); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	env := newModelEnv(t)
	for _, tbl := range []string{"ahp_model.provider_quotas", "ahp_model.quota_windows", "ahp_model.quota_admits"} {
		var exists bool
		if err := env.pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, tbl).Scan(&exists); err != nil {
			t.Fatalf("regclass %s: %v", tbl, err)
		}
		if exists {
			t.Fatalf("%s survived Rollback", tbl)
		}
	}
	// The planes this feature must not disturb are still there and still readable.
	for _, tbl := range []string{"ahp_model.budgets", "ahp_model.budget_reservations", "ahp_model.generations"} {
		var n int
		if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM `+tbl).Scan(&n); err != nil {
			t.Fatalf("%s readable after quota rollback: %v", tbl, err)
		}
	}
}

// ---- the gate on the admission path (Service.Submit) -----------------------

// Criterion: the quota bites where a generation operation is ACCEPTED. An
// exhausted window produces a structured refusal, no generation row, no budget
// hold, no provider call -- and the refusal does not echo the rejected prompt.
func TestSubmitRefusedByQuotaCreatesNothingAndLeaksNothing(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	q := NewQuotaStore(env.pool)
	if err := q.Migrate(ctx); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	provider := quotaProvider(t)
	tenant := env.scope.Tenant
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: tenant, RPM: 1, TPM: 10_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set: %v", err)
	}
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"ok"}, Usage{TotalTokens: 5})
	svc := env.service(inf).WithQuota(q, provider)

	const sentinel = "PROMPT-CONTENT-SENTINEL-must-never-be-echoed"
	first, err := svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-1", Key: "qgen-1", Request: chatReq(sentinel),
	})
	if err != nil {
		t.Fatalf("first submit: %v", err)
	}
	if first.ID == "" {
		t.Fatal("first submit produced no generation")
	}

	// A SECOND attempt (different key: RPM counts attempts, which is what the
	// provider sees) is refused.
	_, err = svc.Submit(ctx, SubmitArgs{
		Scope: env.scope, Task: "task-1", Key: "qgen-2", Request: chatReq(sentinel),
	})
	if err == nil {
		t.Fatal("Submit accepted a generation over the provider RPM limit")
	}
	if contracts.ReasonOf(err) != ReasonProviderRPMExhausted {
		t.Fatalf("reason = %q, want %q", contracts.ReasonOf(err), ReasonProviderRPMExhausted)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("the refusal echoed the rejected request content: %v", err)
	}
	if strings.Contains(err.Error(), "Bearer") || strings.Contains(err.Error(), "http") {
		t.Fatalf("the refusal carries transport or credential material: %v", err)
	}

	// Nothing was created: one generation row for this scope, and no budget hold
	// was taken for the refused attempt.
	var gens int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_model.generations WHERE tenant=$1`, tenant).Scan(&gens); err != nil {
		t.Fatalf("count generations: %v", err)
	}
	if gens != 1 {
		t.Fatalf("generations rows for tenant = %d, want 1 (the refused attempt must not exist)", gens)
	}
	if calls := inf.count(); calls != 0 {
		t.Fatalf("the provider was called %d times by a quota-gated submit; admission must precede any call", calls)
	}
	// The refusal is not a panic-in-disguise: the same error type every other
	// model refusal uses, so existing callers branch on it.
	var ce *contracts.Error
	if !errors.As(err, &ce) {
		t.Fatalf("refusal is not a structured contracts error: %T %v", err, err)
	}
}

// A replayed Submit (same generation key, e.g. after a crash) must not pay the
// provider window twice.
func TestSubmitReplayDoesNotPayQuotaTwice(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	q := NewQuotaStore(env.pool)
	if err := q.Migrate(ctx); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	provider := quotaProvider(t)
	if err := q.Set(ctx, ProviderQuota{Provider: provider, Tenant: env.scope.Tenant, RPM: 5, TPM: 50_000, WindowSeconds: 3600}); err != nil {
		t.Fatalf("set: %v", err)
	}
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"ok"}, Usage{TotalTokens: 3})
	svc := env.service(inf).WithQuota(q, provider)

	args := SubmitArgs{Scope: env.scope, Task: "task-1", Key: "qgen-replay", Request: chatReq("hello"), ReserveAmount: 500, BudgetID: ""}
	for i := 0; i < 3; i++ {
		if _, err := svc.Submit(ctx, args); err != nil {
			t.Fatalf("submit %d: %v", i, err)
		}
	}
	w, err := q.Window(ctx, provider, env.scope.Tenant)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	if w.Requests != 1 {
		t.Fatalf("window counted %d requests for three replays of one attempt, want 1", w.Requests)
	}
}

// Criterion: an unconfigured deployment is unchanged. No quota store attached =>
// unlimited, and with a store attached but no limit rows => unlimited too, with
// nothing written to the window table.
func TestSubmitWithoutQuotaConfigurationIsUnchanged(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"ok"}, Usage{TotalTokens: 3})

	plain := env.service(inf)
	for i := 0; i < 3; i++ {
		if _, err := plain.Submit(ctx, SubmitArgs{Scope: env.scope, Task: "t", Key: fmt.Sprintf("noq-%d", i), Request: chatReq("hello")}); err != nil {
			t.Fatalf("un-gated submit %d: %v", i, err)
		}
	}
	if got := plain.ProviderQuotas(); got != nil {
		t.Fatalf("a service without WithQuota reports a quota store: %+v", got)
	}

	q := NewQuotaStore(env.pool)
	if err := q.Migrate(ctx); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	gated := env.service(inf).WithQuota(q, quotaProvider(t)) // no limit rows
	for i := 0; i < 3; i++ {
		if _, err := gated.Submit(ctx, SubmitArgs{Scope: env.scope, Task: "t", Key: fmt.Sprintf("noq2-%d", i), Request: chatReq("hello")}); err != nil {
			t.Fatalf("submit with an unlimited quota mount %d: %v", i, err)
		}
	}
	var rows int
	if err := env.pool.QueryRow(ctx,
		`SELECT count(*) FROM ahp_model.quota_windows WHERE tenant=$1`, env.scope.Tenant).Scan(&rows); err != nil {
		t.Fatalf("count quota_windows: %v", err)
	}
	if rows != 0 {
		t.Fatalf("an unlimited tenant wrote %d quota window rows; an unconfigured quota must write nothing", rows)
	}
}

// Attaching a quota store without naming the provider is a wiring fault that has
// to surface as an error, not as every tenant silently sharing one anonymous
// window.
func TestQuotaStoreWithoutProviderNameFailsClosed(t *testing.T) {
	ctx := context.Background()
	env := newModelEnv(t)
	q := NewQuotaStore(env.pool)
	if err := q.Migrate(ctx); err != nil {
		t.Fatalf("quota migrate: %v", err)
	}
	inf := &fakeInference{model: "qwen-test"}
	inf.streamText([]string{"ok"}, Usage{TotalTokens: 1})
	svc := env.service(inf).WithQuota(q, "   ")
	_, err := svc.Submit(ctx, SubmitArgs{Scope: env.scope, Task: "t", Key: "wired-wrong", Request: chatReq("hello")})
	if contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("err = %v, want an INVALID_ARGUMENT wiring fault", err)
	}
}
