package identity

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrateTenantGuardsSingleConnection(t *testing.T) {
	base, _ := isolatedPool(t)
	config := base.Config()
	config.MaxConns = 1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("single-connection initial migration: %v", err)
	}
	writer, err := base.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Rollback(context.Background())
	// A live writer must not encounter repeated CREATE TRIGGER DDL on an
	// already migrated database. ROW EXCLUSIVE conflicts with that DDL.
	if _, err := writer.Exec(ctx, "LOCK TABLE ahp_identity.grants_root IN ROW EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("single-connection repeated migration with live writer: %v", err)
	}
	if got := pool.Stat().AcquiredConns(); got != 0 {
		t.Fatalf("migration retained %d pool leases", got)
	}
}
