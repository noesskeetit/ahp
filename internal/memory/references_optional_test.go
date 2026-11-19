package memory

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOptionalLegacyReferencesForNativeMemory(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("real PostgreSQL required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema := fmt.Sprintf("ahp_memory_ref_%x", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE") })
	optional := ReferenceSourceIfPresent(pool, schema)
	arts := []references.Artifact{{ID: "kept", Digest: "digest"}}
	if _, err := optional.InspectReferences(ctx, "tenant", nil, arts); err != nil {
		t.Fatalf("native-only installation has no legacy corpus: %v", err)
	}
	if _, err := ReferenceSource(pool, schema).InspectReferences(ctx, "tenant", nil, arts); err == nil {
		t.Fatal("required legacy owner must fail closed when absent")
	}
	if _, err = pool.Exec(ctx, "CREATE SCHEMA "+schema+"; CREATE TABLE "+schema+".memories(tenant text,state text,evidence jsonb)"); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "INSERT INTO "+schema+".memories VALUES('tenant','active','[\"kept\"]')"); err != nil {
		t.Fatal(err)
	}
	got, err := optional.InspectReferences(ctx, "tenant", nil, arts)
	if err != nil || !got.Referenced["kept"] {
		t.Fatalf("rollback corpus must still protect artifacts: %v %+v", err, got)
	}
	got, err = optional.InspectReferences(ctx, "foreign", nil, arts)
	if err != nil || got.Referenced["kept"] {
		t.Fatalf("tenant boundary: %v %+v", err, got)
	}
	if _, err = pool.Exec(ctx, "ALTER TABLE "+schema+".memories DROP COLUMN evidence"); err != nil {
		t.Fatal(err)
	}
	if _, err := optional.InspectReferences(ctx, "tenant", nil, arts); err == nil {
		t.Fatal("present but unreadable legacy corpus must fail closed")
	}
}
