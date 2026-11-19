package main

import (
	"context"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/internal/gateway"
	"github.com/ahp-platform/ahp/internal/model"
	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

func TestBudgetCompletionUsesOnlyItsDurableOwner(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated real PostgreSQL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	completed, err := newTaskBudgetCompletion(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	states := state.New(pool)
	tasks, err := gateway.NewStore(pool, envOr("AHP_EXEC_SCHEMA", "ahp_exec"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		stateDone    bool
		gatewayState a2a.TaskState
		owner        string
	}{{"state pending gateway done", false, a2a.TaskStateCompleted, model.BudgetOwnerState}, {"state done gateway pending", true, a2a.TaskStateSubmitted, model.BudgetOwnerGateway}} {
		t.Run(tc.name, func(t *testing.T) {
			scope := contracts.Scope{Tenant: "completion-" + contracts.NewID(), Deployment: "runtime", Subject: "owner"}
			task := "shared-task"
			thread, err := states.CreateThread(ctx, scope, "context", "thread")
			if err != nil {
				t.Fatal(err)
			}
			run, err := states.CreateRun(ctx, scope, thread.ID, task, "run", []byte(`{}`))
			if err != nil {
				t.Fatal(err)
			}
			if tc.stateDone {
				lease, err := states.ClaimRunFor(ctx, scope.Tenant, scope.Deployment, "worker", run.ID, time.Minute)
				if err != nil {
					t.Fatal(err)
				}
				if err = states.Release(ctx, scope, run.ID, "worker", lease.Epoch, state.RunDone); err != nil {
					t.Fatal(err)
				}
			}
			// Task IDs are global within a gateway, so keep the State and Gateway
			// identifier collision explicit while separating the two test scenarios.
			gatewayID := contracts.NewID()
			if _, err = pool.Exec(ctx, `UPDATE ahp_state.runs SET task_id=$1 WHERE id=$2`, gatewayID, run.ID); err != nil {
				t.Fatal(err)
			}
			err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
				_, err := tasks.InsertTask(ctx, tx, gateway.TaskRow{ID: gatewayID, Scope: scope, Caller: "caller", Kind: "generic", ContextID: "context", State: tc.gatewayState})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			done, err := completed(ctx, model.BudgetTask{Scope: scope, Task: "a2a-task:" + gatewayID, Owner: tc.owner})
			if err != nil || done {
				t.Fatalf("conflicting lifecycle proof accepted: done=%v err=%v", done, err)
			}
			if err = states.DeleteThread(ctx, scope, thread.ID); err != nil {
				t.Fatal(err)
			}
			done, err = completed(ctx, model.BudgetTask{Scope: scope, Task: "a2a-task:" + gatewayID, Owner: model.BudgetOwnerState})
			if err != nil || done {
				t.Fatalf("deleted State fell back to another owner: done=%v err=%v", done, err)
			}
			done, err = completed(ctx, model.BudgetTask{Scope: scope, Task: "a2a-task:" + gatewayID})
			if err != nil || done {
				t.Fatalf("unknown owner inferred completion: done=%v err=%v", done, err)
			}
		})
	}
}
