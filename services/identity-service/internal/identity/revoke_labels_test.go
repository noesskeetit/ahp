// Package identity: tests for subtree cancellation, subject revocation and
// label propagation. They run against a real PostgreSQL (AHP_TEST_DATABASE_URL)
// and skip when it is absent.
package identity

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// seedTree creates a root with one child and one grandchild by direct SQL:
// Delegate is owned by another worker and may not exist yet. It returns the
// three grants in top-down order.
func seedTree(t *testing.T, s *Store, ctx context.Context, scope contracts.Scope, prefix string) (root, child, grand Grant) {
	t.Helper()
	insert := func(g *Grant, table, parent, rootID string, depth int, task, ctxID string) {
		_, err := s.pool.Exec(ctx, `
		    INSERT INTO ahp_identity.`+table+`
		        (id, root, parent, tenant, deployment, subject, task, context_id,
		         status, epoch, depth, actors, capabilities,
		         max_depth, max_fanout, max_active, body_digest, expires_at)
		    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'active',1,$9,'{dep}','{read}',
		            4,32,256,'sha256:seed', clock_timestamp() + interval '1 hour')`,
			g.ID, rootID, parent, scope.Tenant, scope.Deployment, scope.Subject,
			task, ctxID, depth)
		if err != nil {
			t.Fatalf("seed %s grant: %v", table, err)
		}
	}
	mkCtx := func(id string) string {
		cid := id + "-" + scope.Deployment
		if _, err := s.pool.Exec(ctx, `
		    INSERT INTO ahp_identity.contexts (id, tenant, deployment, subject)
		    VALUES ($1,$2,$3,$4) ON CONFLICT DO NOTHING`,
			cid, scope.Tenant, scope.Deployment, scope.Subject); err != nil {
			t.Fatalf("seed context: %v", err)
		}
		return cid
	}
	root = Grant{ID: prefix + "-root", Task: prefix + "-root-task"}
	child = Grant{ID: prefix + "-child", Task: prefix + "-child-task"}
	grand = Grant{ID: prefix + "-grand", Task: prefix + "-grand-task"}
	insert(&root, "grants_root", "", root.ID, 0, prefix+"-root-task", mkCtx(prefix+"-ctx-root"))
	insert(&child, "grants_child", root.ID, root.ID, 1, prefix+"-child-task", mkCtx(prefix+"-ctx-child"))
	insert(&grand, "grants_child", child.ID, root.ID, 2, prefix+"-grand-task", mkCtx(prefix+"-ctx-grand"))
	return root, child, grand
}

func statusEpoch(t *testing.T, s *Store, ctx context.Context, table, id string) (string, int64, bool) {
	t.Helper()
	var (
		status    string
		epoch     int64
		revokedAt *time.Time
	)
	err := s.pool.QueryRow(ctx, `SELECT status, epoch, revoked_at FROM ahp_identity.`+table+
		` WHERE id = $1`, id).Scan(&status, &epoch, &revokedAt)
	if err != nil {
		t.Fatalf("read %s %s: %v", table, id, err)
	}
	return status, epoch, revokedAt != nil
}

func TestCancelSubtreeChildThenIdempotent(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	p := sanitizeID(contracts.NewID())
	root, child, grand := seedTree(t, s, ctx, scope, p)

	if err := s.CancelSubtree(ctx, scope, child.Task); err != nil {
		t.Fatalf("CancelSubtree(child): %v", err)
	}
	for _, g := range []Grant{child, grand} {
		st, epoch, rev := statusEpoch(t, s, ctx, "grants_child", g.ID)
		if st != StatusRevoked || epoch != 2 || !rev {
			t.Errorf("%s: status=%s epoch=%d revoked_at_set=%v, want revoked/2/true",
				g.ID, st, epoch, rev)
		}
	}
	if st, epoch, rev := statusEpoch(t, s, ctx, "grants_root", root.ID); st != StatusActive || epoch != 1 || rev {
		t.Errorf("root must survive a child cancel: status=%s epoch=%d revoked_at_set=%v", st, epoch, rev)
	}
	if _, err := s.Evaluate(ctx, scope, child.Task, "read"); err == nil {
		t.Errorf("Evaluate must deny a cancelled child")
	}
	if _, err := s.Evaluate(ctx, scope, grand.Task, "read"); err == nil {
		t.Errorf("Evaluate must deny a cancelled descendant")
	}

	// Idempotent repeat: nothing changes a second time.
	if err := s.CancelSubtree(ctx, scope, child.Task); err != nil {
		t.Fatalf("repeated CancelSubtree(child): %v", err)
	}
	for _, g := range []Grant{child, grand} {
		if _, epoch, _ := statusEpoch(t, s, ctx, "grants_child", g.ID); epoch != 2 {
			t.Errorf("%s epoch moved on a repeat: %d, want 2", g.ID, epoch)
		}
	}
}

func TestCancelSubtreeRootAndForeignScope(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	p := sanitizeID(contracts.NewID())
	root, child, grand := seedTree(t, s, ctx, scope, p)

	if err := s.CancelSubtree(ctx, scope, root.Task); err != nil {
		t.Fatalf("CancelSubtree(root): %v", err)
	}
	if st, epoch, rev := statusEpoch(t, s, ctx, "grants_root", root.ID); st != StatusRevoked || epoch != 2 || !rev {
		t.Errorf("root: status=%s epoch=%d revoked_at_set=%v, want revoked/2/true", st, epoch, rev)
	}
	for _, g := range []Grant{child, grand} {
		if st, _, _ := statusEpoch(t, s, ctx, "grants_child", g.ID); st != StatusRevoked {
			t.Errorf("descendant %s survived a root cancel: %s", g.ID, st)
		}
	}
	if _, err := s.Evaluate(ctx, scope, root.Task, "read"); err == nil {
		t.Errorf("Evaluate must deny a cancelled root")
	}

	other := uniqueScope(t)
	other.Tenant, other.Deployment = scope.Tenant, scope.Deployment
	if err := s.CancelSubtree(ctx, other, root.Task); contracts.Code(err) != contracts.ErrNotFound {
		t.Errorf("foreign subject cancel: code=%q err=%v, want NOT_FOUND", contracts.Code(err), err)
	}
	if err := s.CancelSubtree(ctx, scope, "no-such-task"); contracts.Code(err) != contracts.ErrNotFound {
		t.Errorf("absent task cancel: code=%q, want NOT_FOUND", contracts.Code(err))
	}
}

func TestRevokeSubjectCancelsAllRootsAndDeniesNew(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	if _, err := s.CreateRoot(ctx, scope,
		rootRequest("task-subj-a", "ctx-subj-a", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot A: %v", err)
	}
	// A second deployment of the same tenant/subject must be revoked too.
	other := scope
	other.Deployment = scope.Deployment + "-b"
	if _, err := s.CreateRoot(ctx, other,
		rootRequest("task-subj-b", "ctx-subj-b", other.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot B: %v", err)
	}
	sibling := uniqueScope(t)
	sibling.Tenant = scope.Tenant
	if _, err := s.CreateRoot(ctx, sibling,
		rootRequest("task-sibling", "ctx-sibling", sibling.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot sibling: %v", err)
	}

	if err := s.RevokeSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatalf("RevokeSubject: %v", err)
	}
	if err := s.RevokeSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatalf("repeated RevokeSubject: %v", err)
	}

	for _, tc := range []struct {
		scope contracts.Scope
		task  string
	}{{scope, "task-subj-a"}, {other, "task-subj-b"}} {
		if _, err := s.Evaluate(ctx, tc.scope, tc.task, "read"); err == nil {
			t.Errorf("%s: Evaluate must deny after subject revocation", tc.task)
		}
	}
	if _, err := s.Evaluate(ctx, sibling, "task-sibling", "read"); err != nil {
		t.Errorf("same-tenant other subject must be unaffected: %v", err)
	}

	fresh := scope
	fresh.Deployment = scope.Deployment + "-c"
	if _, err := s.CreateRoot(ctx, fresh,
		rootRequest("task-subj-new", "ctx-subj-new", fresh.Deployment, time.Hour, "read")); err == nil {
		t.Fatalf("a revoked subject must not open a new root")
	}
}

func TestAddLabelsRootAndFutureContext(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	grant, err := s.CreateRoot(ctx, scope,
		rootRequest("task-lbl", "ctx-lbl", scope.Deployment, time.Hour, "read"))
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	added := labels.Set{Subjects: []string{"team-b", "team-b", "team-a"}, Taints: []string{labels.TaintSecret}}
	got, err := s.AddLabels(ctx, scope, "task-lbl", added)
	if err != nil {
		t.Fatalf("AddLabels: %v", err)
	}
	if got.ID != grant.ID {
		t.Errorf("AddLabels returned grant %q, labelled %q", got.ID, grant.ID)
	}
	want := []string{"team-a", "team-b"}
	if fmt.Sprint(got.Labels.Subjects) != fmt.Sprint(want) {
		t.Errorf("grant labels = %v, want %v", got.Labels.Subjects, want)
	}
	if fmt.Sprint(got.Labels.Taints) != fmt.Sprint([]string{labels.TaintSecret}) {
		t.Errorf("grant taints = %v", got.Labels.Taints)
	}

	// The journal carries the set, so a future task in the SAME context
	// inherits it.
	if _, err := s.CreateRoot(ctx, scope,
		rootRequest("task-lbl-future", "ctx-lbl", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot future task: %v", err)
	}
	future, err := s.Get(ctx, scope, "task-lbl-future")
	if err != nil {
		t.Fatalf("Get future task: %v", err)
	}
	if fmt.Sprint(future.Labels.Subjects) != fmt.Sprint(want) {
		t.Errorf("future task labels = %v, want %v", future.Labels.Subjects, want)
	}

	// Monotonic union: a second call only widens.
	if _, err := s.AddLabels(ctx, scope, "task-lbl",
		labels.Set{Workspaces: []string{"ws-1"}}); err != nil {
		t.Fatalf("second AddLabels: %v", err)
	}
	again, err := s.Get(ctx, scope, "task-lbl")
	if err != nil {
		t.Fatalf("Get after second AddLabels: %v", err)
	}
	if fmt.Sprint(again.Labels.Subjects) != fmt.Sprint(want) || len(again.Labels.Workspaces) != 1 {
		t.Errorf("labels not monotonic: %+v", again.Labels)
	}

	if _, err := s.AddLabels(ctx, scope, "task-lbl",
		labels.Set{Taints: []string{"nope"}}); contracts.Code(err) != contracts.ErrInvalid {
		t.Errorf("unknown taint: code=%q err=%v, want INVALID", contracts.Code(err), err)
	}
	if _, err := s.AddLabels(ctx, scope, "absent-task",
		labels.Set{Subjects: []string{"team-a"}}); contracts.Code(err) != contracts.ErrNotFound {
		t.Errorf("absent task: code=%q, want NOT_FOUND", contracts.Code(err))
	}
}

func TestAddLabelsDeniedForRevokedSubject(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope,
		rootRequest("task-lbl-revoked", "ctx-lbl-revoked", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	if err := s.RevokeSubject(ctx, scope.Tenant, scope.Subject); err != nil {
		t.Fatalf("RevokeSubject: %v", err)
	}
	_, err := s.AddLabels(ctx, scope, "task-lbl-revoked", labels.Set{Subjects: []string{"team-a"}})
	if err == nil {
		t.Fatalf("AddLabels must deny a revoked subject")
	}
}
