package gateway_test

// metadata_merge_test.go -- a task whose metadata was never set is stored as the
// jsonb scalar `null` (json.Marshal of a nil Go map), and merging facts onto it
// used to build an ARRAY: PostgreSQL concatenates a scalar with an object by
// making an array out of both, and SQL COALESCE does not catch a jsonb `null`.
// The result is silent data loss -- the completed task can never be read again,
// because its own metadata column no longer decodes. This was measured on
// candidate 913d0287a, where every live leg that completed a task then timed out
// polling a task that had already settled:
//
//	gateway: load task: gateway: decode metadata: json: cannot unmarshal array
//	into Go value of type map[string]interface {}
//
// The test drives the exact production sequence (accept a task with no caller
// metadata, merge the attempt stats onto it, read the task back).

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/internal/gateway"
)

func TestMergeAttemptStatsKeepsTaskMetadataAnObject(t *testing.T) {
	e := newEnv(t)
	svc := e.service(t, echoExecutor(), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	task := mustSubmit(t, svc, e.scope, e.caller, "meta-merge-null", nil)
	if len(task.Metadata) != 0 {
		t.Fatalf("this leg needs a task with NO caller metadata, got %v", task.Metadata)
	}

	// What a null-metadata row really holds before any merge.
	var before string
	if err := e.pool.QueryRow(ctx, `SELECT jsonb_typeof(metadata) FROM `+e.schema+`.tasks WHERE task_id=$1`,
		string(task.ID)).Scan(&before); err != nil {
		t.Fatalf("read metadata type before the merge: %v", err)
	}
	t.Logf("metadata column before the merge: %s", before)

	// The production writer runs inside the worker's transaction.
	if err := pgx.BeginFunc(ctx, e.pool, func(tx pgx.Tx) error {
		return e.store.MergeMetadataTx(ctx, tx, string(task.ID), map[string]any{
			gateway.MetadataStepsConsumed: 2,
			gateway.MetadataCloseReason:   gateway.CloseReasonModelFinished,
			gateway.MetadataAttempt:       1,
		})
	}); err != nil {
		t.Fatalf("MergeMetadataTx onto a %s metadata value: %v", before, err)
	}

	var after string
	if err := e.pool.QueryRow(ctx, `SELECT jsonb_typeof(metadata) FROM `+e.schema+`.tasks WHERE task_id=$1`,
		string(task.ID)).Scan(&after); err != nil {
		t.Fatalf("read metadata type after the merge: %v", err)
	}
	if after != "object" {
		t.Fatalf("merging attempt stats turned task metadata into jsonb %s (was %s): every later reader of this task fails",
			after, before)
	}

	// The reader that broke is the one that matters: a settled task must stay
	// readable, and the journalled facts must be visible on it.
	row, err := svc.Store().Task(ctx, e.scope, string(task.ID))
	if err != nil {
		t.Fatalf("the settled task became unreadable after the merge: %v", err)
	}
	if got := row.Metadata[gateway.MetadataCloseReason]; got != gateway.CloseReasonModelFinished {
		t.Fatalf("%s = %v, want %q", gateway.MetadataCloseReason, got, gateway.CloseReasonModelFinished)
	}
	if got := row.Metadata[gateway.MetadataStepsConsumed]; got != float64(2) {
		t.Fatalf("%s = %v (%T), want 2", gateway.MetadataStepsConsumed, got, got)
	}
	t.Logf("task %s stayed readable: metadata=%v", task.ID, row.Metadata)
}
