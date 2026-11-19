package toolsapi

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/internal/tools"
)

// A large response can fit the MCP ingress bound and still exceed the
// operations ledger's limit. The effect already happened.
func TestOversizedUnstoredResultHoldsSideEffect(t *testing.T) {
	e := newEnv(t)
	token := e.signTask(t)
	args := json.RawMessage(`{"id":"large-result-fixture"}`)
	e.allowDigest(t, "write", args)
	e.fixture.writePadding.Store(1 << 20)

	for range 2 {
		status, body := e.call(t, "Call", token,
			`{"key":"big-result","tool":"write","arguments":`+string(args)+`}`)
		mustStatus(t, status, http.StatusOK, body)
		if body["status"] != operations.StatusSideEffectUnknown {
			t.Fatalf("Call status = %v, want SIDE_EFFECT_UNKNOWN", body["status"])
		}
	}
	status, body := e.call(t, "GetResult", token, `{"key":"big-result"}`)
	mustStatus(t, status, http.StatusConflict, body)
	status, body = e.call(t, "Call", token,
		`{"key":"second-key","tool":"write","arguments":`+string(args)+`}`)
	mustStatus(t, status, http.StatusConflict, body)
	if effects := atomic.LoadInt32(&e.fixture.effCount); effects != 1 {
		t.Fatalf("MCP effects = %d, want 1", effects)
	}
}

// TestMigrationIdempotent: constructing a second Service over the SAME schema
// re-runs the DDL (CREATE ... IF NOT EXISTS) without error -- Migrate is
// idempotent and non-destructive.
func TestMigrationIdempotent(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fixture := newMCPFixture(t)
	svc2, err := tools.New(ctx, e.pool, e.schema, tools.Config{
		Guardian:    tools.NewPolicyGuardian("second"),
		Credentials: fakeCreds{bearer: "x"},
		Connections: map[string]tools.Connector{"c": fixture.connector()},
		Tools:       map[string]tools.ToolSpec{"echo": {Name: "echo", Connection: "c"}},
		Verifier:    e.verifier,
		Audience:    tAudience,
	})
	if err != nil {
		t.Fatalf("second New on same schema (idempotent migrate): %v", err)
	}
	_ = svc2
}

// TestDropRemovesOnlyOwnSchema: the plane rollback drops this plane's tables and
// schema and nothing else -- a neighbouring schema the operator owns survives.
func TestDropRemovesOnlyOwnSchema(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Seed: the plane's schema and its tables exist after construction.
	if !schemaExists(t, e.pool, e.schema) {
		t.Fatalf("expected %s to exist before Drop", e.schema)
	}

	// A neighbouring schema owned by someone else, with data.
	other := "ahp_tools_neighbor_" + randomHex(t, 6)
	if _, err := e.pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+other); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(ctx, "CREATE TABLE "+other+".keep(id int)"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer ccancel()
		_, _ = e.pool.Exec(cctx, "DROP SCHEMA IF EXISTS "+other+" CASCADE")
	})

	if err := e.svc.Drop(ctx); err != nil {
		t.Fatalf("Drop: %v", err)
	}
	if schemaExists(t, e.pool, e.schema) {
		t.Fatalf("Drop left %s in place", e.schema)
	}
	if !schemaExists(t, e.pool, other) {
		t.Fatalf("Drop crossed into a neighbouring schema %s", other)
	}
	// Idempotent: a second Drop is a no-op, not an error.
	if err := e.svc.Drop(ctx); err != nil {
		t.Fatalf("second Drop not idempotent: %v", err)
	}
}

func schemaExists(t *testing.T, pool *pgxpool.Pool, schema string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var ok bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = $1)`, schema).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}
