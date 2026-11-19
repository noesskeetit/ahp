//go:build ahp_data

package main

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/compute"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/triggers"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These coordinator tests use real owner stores. The tool leg covers its shared
// durable effect ledger; OAuth/Vault end-to-end is exercised by the tool owner.
func tenantTestOwners(t *testing.T, pool *pgxpool.Pool) map[string]TenantDeletionOwner {
	t.Helper()
	ctx := context.Background()
	c := compute.New(pool)
	if err := c.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	suffix := contracts.NewID()
	suffix = suffix[len(suffix)-12:]
	g, err := gateway.NewStore(pool, "ahp_tenantgw_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	gl, err := operations.New(pool, "ahp_tenantgw_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	if err = gl.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	gens := model.NewGenerationStore(pool)
	if err = gens.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	budgets := model.NewBudgetStore(pool)
	if err = budgets.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	ml, err := operations.New(pool, "ahp_model")
	if err != nil {
		t.Fatal(err)
	}
	if err = ml.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tl, err := operations.New(pool, "ahp_tenanttools_"+suffix)
	if err != nil {
		t.Fatal(err)
	}
	if err = tl.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	dep := tenantUnusedTriggerRuntime{}
	tr, err := triggers.New(ctx, pool, "ahp_tenanttrg_"+suffix, triggers.Config{Issuer: dep, Dispatcher: dep, Secrets: dep, Tasks: dep})
	if err != nil {
		t.Fatal(err)
	}
	return map[string]TenantDeletionOwner{"compute": c, "gateway": g, "model": model.NewService(gens, budgets, ml, nil), "tools": tl, "triggers": tr}
}

type tenantUnusedTriggerRuntime struct{}

func (tenantUnusedTriggerRuntime) IssueToken(context.Context, contracts.Scope, string, time.Duration) (string, time.Time, error) {
	return "", time.Time{}, contracts.NewError(contracts.ErrUnavailable, "no test dispatch")
}
func (tenantUnusedTriggerRuntime) Fire(context.Context, contracts.Scope, triggers.FireRequest) (triggers.FireResult, error) {
	return triggers.FireResult{}, contracts.NewError(contracts.ErrUnavailable, "no test dispatch")
}
func (tenantUnusedTriggerRuntime) Resolve(context.Context, contracts.Scope, string) ([]byte, error) {
	return nil, contracts.NewError(contracts.ErrUnavailable, "no test secret")
}
func (tenantUnusedTriggerRuntime) IsTaskActive(context.Context, contracts.Scope, string) (bool, error) {
	return false, nil
}
