package model

import (
	"context"
	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/jackc/pgx/v5/pgxpool"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestModelMigrateConcurrentAndSingleConnection(t *testing.T) {
	e := newModelEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := e.pool.Config()
	cfg.MaxConns = 1
	single, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			if _, err := e.budget.Root(ctx, e.scope, "writer", strconv.Itoa(i), 100); err != nil {
				t.Errorf("live admission during migration: %v", err)
				return
			}
		}
	}()

	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			pool := e.pool
			if i%2 == 0 {
				pool = single
			}
			ledger, _ := operations.New(pool, "ahp_model")
			for _, run := range []func(context.Context) error{NewBudgetStore(pool).Migrate, NewGenerationStore(pool).Migrate, NewQuotaStore(pool).Migrate, NewDeploymentLabelStore(pool).Migrate, ledger.Migrate} {
				if err := run(ctx); err != nil {
					t.Errorf("concurrent migration: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
