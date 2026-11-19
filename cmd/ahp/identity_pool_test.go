//go:build ahp_data

package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// State writers may exhaust their own pool while Artifacts checks authority.
// Both grant reads and the synthetic IAM policy must use the isolated reader.
func TestSyntheticIdentityReadPoolSurvivesStateSaturationAndRevoke(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("real PostgreSQL required")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns, cfg.MinConns = 1, 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	policy := identity.NewSyntheticResourcePolicy(pool)
	s := &server{pool: pool, iam: policy}
	ids := s.identityStore()
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "pool-iam-" + randHexTest(), Subject: "alice", Deployment: "agent"}
	for _, resource := range []identity.Resource{{Kind: "tenant", ID: scope.Tenant}, {Kind: "deployment", ID: scope.Deployment}} {
		if _, err := policy.PutRule(ctx, identity.ResourceRule{Tenant: scope.Tenant, Subject: scope.Subject, Resource: resource, Action: "artifacts", Allowed: true}, 0); err != nil {
			t.Fatal(err)
		}
	}
	grant, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: scope.Tenant + "-task", Context: scope.Tenant + "-context", Deployment: scope.Deployment, Capabilities: []string{"artifacts"}, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	claims := auth.Claims{Kind: auth.KindTask, TokenUse: auth.TokenUse, Tenant: grant.Tenant, Deployment: grant.Deployment, Subject: grant.Subject, Task: grant.Task, Context: grant.Context, Grant: grant.ID, Root: grant.Root, Actors: grant.Actors, Depth: grant.Depth}
	dataPool, readPool, closePools, err := checkpointOwnerPools(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer closePools()
	if dataPool.Config().MaxConns != 4 || readPool.Config().MaxConns != 2 {
		t.Fatal("owner connection bounds changed")
	}
	reader := s.identityStoreFor(readPool)
	held, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Release()
	checkCtx, checkCancel := context.WithTimeout(ctx, 3*time.Second)
	defer checkCancel()
	if err := reader.Authorize(checkCtx, claims, "artifacts"); err != nil {
		t.Fatalf("isolated authority stalled behind State writers: %v", err)
	}
	operator := identity.NewSyntheticResourcePolicy(dataPool)
	if _, err := operator.PutRule(checkCtx, identity.ResourceRule{Tenant: scope.Tenant, Subject: scope.Subject, Resource: identity.Resource{Kind: "deployment", ID: scope.Deployment}, Action: "artifacts", Allowed: false}, 1); err != nil {
		t.Fatal(err)
	}
	if err := reader.Authorize(checkCtx, claims, "artifacts"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("isolated authority bypassed fresh IAM revoke: %v", err)
	}
}
