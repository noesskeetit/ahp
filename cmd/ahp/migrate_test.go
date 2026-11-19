package main

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/state"
)

// wantStateTables is the full set of tables the State Store must create on a
// fresh database for a runnable service role to serve every State operation.
// This is the regression guard for the integration-validation "state migration
// gap": a fresh deployment used to answer ~33 of 35 State operations with
// "relation does not exist" because only the control-plane tables were
// migrated by Store.Migrate alone. cmd/ahp must invoke migrateStateAll, which
// runs every sub-migration, so all of these tables must appear.
var wantStateTables = []string{
	"ahp_state.threads",
	"ahp_state.runs",
	"ahp_state.checkpoints",
	"ahp_state.checkpoint_parts",
	"ahp_state.checkpoint_blobs",
	"ahp_state.checkpoint_writes",
	"ahp_state.kv",
	"ahp_state.kv_ops",
	"ahp_state.inbox_events",
	"ahp_state.stream_events",
	"ahp_state.intents",
	"ahp_state.legalhold",
	"ahp_state.id_operations",
}

// TestMigrateStateAllCreatesAllTables proves that the exact migration path the
// service runs (migrateStateAll) creates the complete State schema from a
// clean database. It targets only the isolated block database selected by the
// launcher and is skipped when no database is configured.
func TestMigrateStateAllCreatesAllTables(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("AHP_DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("AHP_TEST_DATABASE_URL/AHP_DATABASE_URL not set; skipping DB migration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	// Start from a clean app schema so we prove the product migration, not
	// leftover test state, creates every table. ahp_state is this app's own
	// schema inside the isolated block DB; dropping it is non-destructive to
	// neighbors and to the isolated DB's other data.
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS ahp_state CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}

	st := state.New(pool)
	if err := migrateStateAll(ctx, st); err != nil {
		t.Fatalf("migrateStateAll: %v", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT table_schema || '.' || table_name
		FROM information_schema.tables
		WHERE table_schema = 'ahp_state' AND table_type = 'BASE TABLE'`)
	if err != nil {
		t.Fatalf("query tables: %v", err)
	}
	defer rows.Close()

	got := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}

	var missing []string
	for _, want := range wantStateTables {
		if !got[want] {
			missing = append(missing, want)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		t.Fatalf("migrateStateAll did not create %d of %d required State tables: %v",
			len(missing), len(wantStateTables), missing)
	}
	t.Logf("migrateStateAll created all %d required State tables", len(wantStateTables))
}
