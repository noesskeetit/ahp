package identity

// Append-only journal: the two properties a rewrite of the journal's storage has
// to preserve, measured rather than asserted in a comment.
//
// Replay must be idempotent, because an append-only log answers with a union and
// the same widening arriving twice must not answer differently. A denied
// transaction must leave nothing behind, because the journal is written inside
// the caller's transaction and an error there is a rollback, not a partial
// append. Both are tested through the product's entry points, so they hold for
// whatever the storage shape becomes next.

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

func TestContextJournalReplayIsIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope, rootRequest("task-idem", "ctx-idem", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	added := labels.Set{Subjects: []string{"team-a", "env-prod"}}
	first, err := s.AddLabels(ctx, scope, "task-idem", added)
	if err != nil {
		t.Fatalf("AddLabels: %v", err)
	}
	want := append([]string{}, first.Labels.Subjects...)
	for i := 0; i < 5; i++ {
		again, err := s.AddLabels(ctx, scope, "task-idem", added)
		if err != nil {
			t.Fatalf("AddLabels replay %d: %v", i, err)
		}
		if !reflect.DeepEqual(again.Labels.Subjects, want) {
			t.Fatalf("replay %d changed the stored set: %v want %v", i, again.Labels.Subjects, want)
		}
	}

	// The log stays canonical: replayed widenings are recorded as events (an
	// append has nothing to compare against), but the answer is the union, in the
	// same sorted de-duplicated form, so no reader can see a duplicate.
	var events, distinct int
	if err := s.pool.QueryRow(ctx, `
	    SELECT count(*),
	           count(DISTINCT labels_subjects || labels_workspaces || labels_taints)
	      FROM ahp_identity.context_label_events
	     WHERE tenant = $1 AND subject = $2`, scope.Tenant, scope.Subject).Scan(&events, &distinct); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events == 0 {
		t.Fatalf("no widening was recorded")
	}
	stored, found, err := readContext(ctx, mustTx(t, s), scope, "ctx-idem-"+scope.Deployment)
	if err != nil {
		t.Fatalf("read journal: %v", err)
	}
	if !found {
		t.Fatalf("journal not found")
	}
	for _, set := range [][]string{stored.Subjects} {
		for i := 1; i < len(set); i++ {
			if set[i-1] >= set[i] {
				t.Fatalf("stored labels are not canonical: %v", set)
			}
		}
	}
	t.Logf("journal idempotency: events=%d distinct_payloads=%d labels=%v", events, distinct, stored.Subjects)
}

func TestDeniedLabelChangeAppendsNothing(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope, rootRequest("task-deny", "ctx-deny", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	before, err := s.Get(ctx, scope, "task-deny")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	// A subject the control plane revoked cannot widen anything, and the whole
	// transaction - the append included - is rolled back with the denial.
	revoked := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, revoked, rootRequest("task-rev", "ctx-rev", revoked.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot revoked scope: %v", err)
	}
	if err := s.RevokeSubject(ctx, revoked.Tenant, revoked.Subject); err != nil {
		t.Fatalf("RevokeSubject: %v", err)
	}
	if _, err := s.AddLabels(ctx, revoked, "task-rev", labels.Set{Subjects: []string{"after-revocation"}}); err == nil {
		t.Fatalf("a revoked subject widened its journal")
	} else if code := contracts.Code(err); code != contracts.ErrForbidden {
		t.Fatalf("want FORBIDDEN, got %s (%v)", code, err)
	}
	var events int
	if err := s.pool.QueryRow(ctx, `
	    SELECT count(*) FROM ahp_identity.context_label_events
	     WHERE tenant = $1 AND subject = $2`, revoked.Tenant, revoked.Subject).Scan(&events); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if events != 0 {
		t.Errorf("the denied transaction left %d journal events behind; a denial must roll the append back", events)
	}

	// And the untouched scope is unaffected: labels only grow through a committed
	// widening, never through somebody else's rollback.
	after, err := s.Get(ctx, scope, "task-deny")
	if err != nil {
		t.Fatalf("Get after: %v", err)
	}
	if !reflect.DeepEqual(before.Labels.Subjects, after.Labels.Subjects) {
		t.Errorf("an unrelated scope's labels changed: %v -> %v", before.Labels.Subjects, after.Labels.Subjects)
	}
}

// mustTx opens a read-only transaction so a test can call the package's own
// journal reader instead of restating its SQL.
func mustTx(t *testing.T, s *Store) pgx.Tx {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.WithoutCancel(ctx)) })
	return tx
}

// TestDatabaseItselfRefusesJournalRewrite proves the rule is the database's and
// not the service's good behaviour: a writer that bypasses this package is
// refused. If either of these statements ever succeeds, the append-only promise
// is decoration.
func TestDatabaseItselfRefusesJournalRewrite(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope, rootRequest("task-guard", "ctx-guard", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	if _, err := s.AddLabels(ctx, scope, "task-guard", labels.Set{Subjects: []string{"keep-me"}}); err != nil {
		t.Fatalf("AddLabels: %v", err)
	}
	contextID := "ctx-guard-" + scope.Deployment

	if _, err := s.pool.Exec(ctx, `
	    UPDATE ahp_identity.contexts
	       SET labels_subjects = '{}'
	     WHERE id = $1`, contextID); err == nil {
		t.Errorf("the database let a caller empty the context anchor; the immutability trigger is supposed to refuse it")
	} else {
		t.Logf("anchor UPDATE refused: %v", err)
	}

	if _, err := s.pool.Exec(ctx, `
	    DELETE FROM ahp_identity.context_label_events
	     WHERE context_id = $1`, contextID); err == nil {
		t.Errorf("the database let a caller delete journal events without declaring an erasure")
	} else {
		t.Logf("event DELETE refused: %v", err)
	}

	// An erasure is allowed, and it has to say so inside its own transaction.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if _, err := tx.Exec(ctx, `SET LOCAL ahp_identity.erasing = 'on'`); err != nil {
		t.Fatalf("set erasing: %v", err)
	}
	tag, err := tx.Exec(ctx, `DELETE FROM ahp_identity.context_label_events WHERE context_id = $1`, contextID)
	if err != nil {
		t.Fatalf("an erasure was refused: %v", err)
	}
	if tag.RowsAffected() == 0 {
		t.Errorf("the erasure deleted nothing; a widening event was expected")
	}
}
