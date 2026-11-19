package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRuntimeAuthorityKeepsParentAndSelectedWorkload(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated real PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	ids := identity.New(pool)
	if err := ids.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSigner("ahp", "test", private)
	if err != nil {
		t.Fatal(err)
	}
	budget, err := newRuntimeBudgetAdmitter(ctx, pool, ids)
	if err != nil {
		t.Fatal(err)
	}
	a := runtimeAuthority{ids: ids, signer: signer, verifier: auth.NewVerifier("ahp", map[string]ed25519.PublicKey{"test": public})}
	a.budgets = budget
	scope := contracts.Scope{Tenant: "runtime-authority-" + contracts.NewID(), Deployment: "space", Subject: "operator"}
	permissions := []string{"state", "execution", "model", "identity:cancel", "identity:delegate"}
	root, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "parent", Context: contracts.NewID(), Deployment: scope.Deployment, Capabilities: permissions, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	parentBudget, err := budget.TaskBudget(ctx, scope, root.Task)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = budget.store.Reserve(ctx, scope, parentBudget.ID, "parent-spend", 5000); err != nil {
		t.Fatal(err)
	}
	if _, err = budget.store.Settle(ctx, scope, parentBudget.ID, "parent-spend", 5000); err != nil {
		t.Fatal(err)
	}
	parentToken, err := ids.MintTask(ctx, scope, root.Task, signer, "ahp", 0)
	if err != nil {
		t.Fatal(err)
	}
	policy := identity.DelegationTarget{Tenant: scope.Tenant, Deployment: "custom-v1", Capabilities: permissions[:4]}
	var childToken string
	for i := 0; i < 2; i++ {
		childToken, err = a.issue(ctx, scope, parentToken, "child", policy, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	childScope := scope
	childScope.Deployment = policy.Deployment
	childBudget, err := budget.store.TaskBudget(ctx, childScope, "a2a-task:child", "execbridge-task-root")
	if err != nil || childBudget.Parent != parentBudget.ID || childBudget.Limit != parentBudget.Limit/2 {
		t.Fatalf("token issued without inherited reserve: %+v err=%v", childBudget, err)
	}
	claims, err := a.verifier.Verify(childToken, "ahp", auth.KindTask, auth.WorkloadID(childScope))
	if err != nil || claims.Scope() != childScope || claims.Task != "child" {
		t.Fatalf("wrong child binding: %+v %v", claims, err)
	}
	if _, err := a.verifier.Verify(childToken, "ahp", auth.KindTask, auth.WorkloadID(scope)); err == nil {
		t.Fatal("child token accepted as parent workload")
	}
	if _, err := a.issue(ctx, scope, parentToken, "child", policy, true); err != nil {
		t.Fatal(err)
	}
	if _, err := a.issue(ctx, scope, parentToken, "absent", policy, true); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("read created absent child: %v", err)
	}
	other, err := ids.CreateRoot(ctx, scope, identity.RootRequest{Task: "other-parent", Context: contracts.NewID(), Deployment: scope.Deployment, Capabilities: permissions, TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	otherToken, err := ids.MintTask(ctx, scope, other.Task, signer, "ahp", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.issue(ctx, scope, otherToken, "child", policy, true); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("unrelated parent read child: %v", err)
	}
	if err := ids.CancelSubtree(ctx, scope, root.Task); err != nil {
		t.Fatal(err)
	}
	if _, err := a.issue(ctx, scope, parentToken, "child", policy, true); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked parent observed child: %v", err)
	}
	if err := ids.Authorize(ctx, claims, "model"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("revoked descendant retained model access: %v", err)
	}
}
