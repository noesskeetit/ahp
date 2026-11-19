package memory

import (
	"context"
	"fmt"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
	"reflect"
	"testing"
	"time"
)

func TestMutationKeysBindPayloadAndTarget(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	a, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "first", OpKey: "p1"})
	mustOK(t, err)
	b, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "second", OpKey: "p2"})
	mustOK(t, err)
	_, err = e.mem.AddLabels(ctx, scope, a.Memory.ID, "labels", labels.Set{Taints: []string{labels.TaintExternal}})
	mustOK(t, err)
	_, err = e.mem.AddLabels(ctx, scope, a.Memory.ID, "labels", labels.Set{Taints: []string{labels.TaintUntrustedExternal}})
	if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Errorf("changed label payload: got %v", err)
	}
	mustOK(t, e.mem.Delete(ctx, scope, a.Memory.ID, "delete"))
	err = e.mem.Delete(ctx, scope, b.Memory.ID, "delete")
	if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Errorf("changed delete target: got %v", err)
	}
	_, err = e.mem.AddLabels(ctx, scope, b.Memory.ID, "p2", labels.Set{Taints: []string{labels.TaintExternal}})
	if contracts.Code(err) != contracts.ErrKeyReuse {
		t.Errorf("cross-operation key reuse: got %v", err)
	}
}

func TestReviewAndSubjectDeletionReplay(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "review once", OpKey: "proposal"})
	mustOK(t, err)
	if _, err = e.mem.Review(ctx, scope, p.Memory.ID, DecisionApprove); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("missing key accepted: %v", err)
	}
	first, err := e.mem.Review(ctx, scope, p.Memory.ID, DecisionApprove, "review")
	mustOK(t, err)
	replay, err := e.mem.Review(ctx, scope, p.Memory.ID, DecisionApprove, "review")
	mustOK(t, err)
	if !first.UpdatedAt.Equal(replay.UpdatedAt) {
		t.Fatal("review replay mutated record")
	}
	_, err = e.mem.Review(ctx, scope, p.Memory.ID, DecisionReject, "review")
	mustErr(t, err, contracts.ErrKeyReuse)
	count, err := e.mem.DeleteSubject(ctx, scope, "alice", "erase")
	mustOK(t, err)
	repeated, err := e.mem.DeleteSubject(ctx, scope, "alice", "erase")
	mustOK(t, err)
	if count != 1 || repeated != count {
		t.Fatalf("deletion receipt count %d replay %d", count, repeated)
	}
	_, err = e.mem.DeleteSubject(ctx, scope, "bob", "erase")
	mustErr(t, err, contracts.ErrKeyReuse)
}

func TestProposalDigestCoversLifecycleFields(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	for _, name := range []string{"ttl", "review", "provenance"} {
		t.Run(name, func(t *testing.T) {
			req := ProposeRequest{Kind: KindUser, Text: "lifecycle fingerprint", OpKey: "proposal-" + name}
			_, err := e.mem.Propose(ctx, scope, req)
			mustOK(t, err)
			switch name {
			case "ttl":
				v := time.Now().Add(time.Hour)
				req.TTL = &v
			case "review":
				req.ReviewRequired = true
			case "provenance":
				req.Provenance = map[string]any{"proof": "changed"}
			}
			_, err = e.mem.Propose(ctx, scope, req)
			mustErr(t, err, contracts.ErrKeyReuse)
		})
	}
}

func TestConcurrentLabelNarrowingKeepsBothRestrictions(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "concurrent labels", OpKey: "proposal"})
	mustOK(t, err)
	blocker, err := e.pool.Begin(ctx)
	mustOK(t, err)
	defer blocker.Rollback(ctx)
	_, err = blocker.Exec(ctx, `SELECT id FROM ahp_memory.memories WHERE id=$1 FOR UPDATE`, p.Memory.ID)
	mustOK(t, err)
	done := make(chan error, 2)
	for i, taint := range []string{labels.TaintExternal, labels.TaintUntrustedExternal} {
		go func(i int, taint string) {
			_, err := e.mem.AddLabels(ctx, scope, p.Memory.ID, fmt.Sprintf("labels-%d", i), labels.Set{Taints: []string{taint}})
			done <- err
		}(i, taint)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting int
		err = e.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%ahp_memory.memories%' AND pid<>pg_backend_pid()`).Scan(&waiting)
		mustOK(t, err)
		if waiting >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("both label writes never reached the held row lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
	mustOK(t, blocker.Commit(ctx))
	mustOK(t, <-done)
	mustOK(t, <-done)
	got, err := e.mem.Get(ctx, scope, p.Memory.ID)
	mustOK(t, err)
	if !reflect.DeepEqual(got.Labels.Taints, []string{labels.TaintExternal, labels.TaintUntrustedExternal}) {
		t.Fatalf("lost concurrent restriction: %v", got.Labels.Taints)
	}
}

func TestLabelsCannotReadAnotherOwnersPrivateMemory(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "private owner text", OpKey: "proposal"})
	mustOK(t, err)
	_, err = e.mem.AddLabels(ctx, e.scope("bob"), p.Memory.ID, "foreign-label", labels.Set{Taints: []string{labels.TaintExternal}})
	mustErr(t, err, contracts.ErrNotFound)
}

// An already-authorized proposal from another deployment must not recreate a
// subject after erasure. The deletion fence belongs to the Memory owner.
func TestSubjectDeletionFencesFutureProposalsAcrossDeployments(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	if _, err := e.mem.DeleteSubject(ctx, scope, "alice", "erase-fence"); err != nil {
		t.Fatal(err)
	}
	scope.Deployment = "new-deployment"
	_, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "late in-flight proposal", OpKey: "late"})
	mustErr(t, err, contracts.ErrForbidden)
	if _, err := e.mem.Propose(ctx, e.scope("bob"), ProposeRequest{Kind: KindUser, Text: "foreign subject", OpKey: "foreign"}); err != nil {
		t.Fatal(err)
	}
}

func TestAlreadyStartedProposalCannotRecreateErasedSubject(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	entered, release := make(chan struct{}), make(chan struct{})
	e.mem.ceiling = func(context.Context, contracts.Scope) (labels.Set, error) {
		close(entered)
		<-release
		return labels.Set{}, nil
	}
	result := make(chan error, 1)
	go func() {
		_, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "in flight", OpKey: "inflight"})
		result <- err
	}()
	<-entered
	if _, err := e.mem.DeleteSubject(ctx, scope, "alice", "erase-inflight"); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	mustErr(t, <-result, contracts.ErrForbidden)
}
