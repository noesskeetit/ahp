package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// TestMissingSchemaIsDependencyNotConflict reproduces the misdiagnosis the
// acceptance run hit: the service was pointed at a database where its schema
// (earlier: the pgvector extension) was not provisioned, and every caller saw
// CONFLICT -- "your write collided with committed state" -- instead of a
// dependency failure. The record is created against the real database, the
// schema is then dropped, and the next write must name the real class.
func TestMissingSchemaIsDependencyNotConflict(t *testing.T) {
	e := newEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := e.mem.Propose(ctx, e.scope("dep-subject"), ProposeRequest{
		Kind: KindUser, Text: "dependency classification marker", OpKey: "op-dep-1",
	})
	mustOK(t, err)
	if _, err := e.pool.Exec(ctx, `DROP SCHEMA IF EXISTS ahp_memory CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}
	_, err = e.mem.Propose(ctx, e.scope("dep-subject"), ProposeRequest{
		Kind: KindUser, Text: "second write after the dependency disappeared", OpKey: "op-dep-2",
	})
	mustErr(t, err, contracts.ErrUnavailable)
	if reason := contracts.ReasonOf(err); reason != "memory:storage:pgstate_42P01_dependency_missing" {
		t.Fatalf("reason = %q, want the undefined-table dependency reason", reason)
	}
	// The class is honest, the behaviour is still fail-closed: the first record
	// is not readable through a store whose schema is gone.
	if _, err := e.mem.Get(ctx, e.scope("dep-subject"), res.Memory.ID); err == nil {
		t.Fatal("a read succeeded against a missing schema")
	}
}
