package compute

import (
	"context"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"testing"
	"time"
)

func TestWakeUsesConfiguredClassBackend(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires real isolated PostgreSQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "wake-" + contracts.NewID(), Deployment: "compute", Subject: "owner"}
	computer, err := store.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassX, Profile: ProfileEphemeral}, "create")
	if err != nil {
		t.Fatal(err)
	}
	backends := NewRegistry(NewMockBackend())
	backends.SetClassBackend(ClassX, MockBackendName)
	runner := NewRunner(store, backends, "", "test-node", nil)
	inc, err := runner.Wake(ctx, scope, computer.ID)
	if err != nil {
		t.Fatal(err)
	}
	if inc.Backend != MockBackendName {
		t.Fatalf("wake substituted backend: %q", inc.Backend)
	}
	foreign := scope
	foreign.Subject = "other"
	if _, err := runner.Wake(ctx, foreign, computer.ID); contracts.Code(err) != contracts.ErrNotFound && contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("foreign wake: %v", err)
	}
}
