package memory

import (
	"context"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// TestConcurrentIdenticalRetryAppliesOnce pins the idempotency contract under
// contention: N callers retrying the same key with the same arguments race, and
// the mutation must be applied exactly once. A replay that re-executes would
// double-narrow the record and hand out a second ledger row.
func TestConcurrentIdenticalRetryAppliesOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	p, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "contended retry", OpKey: "retry-proposal"})
	mustOK(t, err)

	const racers = 6
	var wg sync.WaitGroup
	ids := make([]string, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			got, err := e.mem.AddLabels(ctx, scope, p.Memory.ID, "retry-labels", labels.Set{Taints: []string{labels.TaintExternal}})
			ids[i], errs[i] = got.ID, err
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
		if ids[i] != p.Memory.ID {
			t.Fatalf("racer %d returned a different record: %s vs %s", i, ids[i], p.Memory.ID)
		}
	}

	var rows int
	mustOK(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_memory.operations WHERE tenant=$1 AND subject=$2 AND op_key=$3`,
		scope.Tenant, scope.Subject, "retry-labels").Scan(&rows))
	if rows != 1 {
		t.Fatalf("one operation key must hold exactly one ledger row, got %d", rows)
	}
	got, err := e.mem.Get(ctx, scope, p.Memory.ID)
	mustOK(t, err)
	if len(got.Labels.Taints) != 1 || got.Labels.Taints[0] != labels.TaintExternal {
		t.Fatalf("replays must not re-apply the narrowing: %v", got.Labels.Taints)
	}
}

// TestConcurrentIdenticalProposeWritesOnce covers the write side of the same
// contract: a racing identical proposal must produce one record, not N.
func TestConcurrentIdenticalProposeWritesOnce(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")

	const racers = 6
	var wg sync.WaitGroup
	ids := make([]string, racers)
	replays := make([]bool, racers)
	errs := make([]error, racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			res, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "one record only", OpKey: "retry-propose"})
			if err == nil {
				ids[i], replays[i] = res.Memory.ID, res.Replay
			}
			errs[i] = err
		}(i)
	}
	close(start)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
		if ids[i] != ids[0] {
			t.Fatalf("racers wrote different records: %s vs %s", ids[i], ids[0])
		}
	}
	var records, ledgers int
	mustOK(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_memory.memories WHERE tenant=$1 AND subject=$2 AND op_key=$3`,
		scope.Tenant, scope.Subject, "retry-propose").Scan(&records))
	mustOK(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_memory.operations WHERE tenant=$1 AND subject=$2 AND op_key=$3`,
		scope.Tenant, scope.Subject, "retry-propose").Scan(&ledgers))
	if records != 1 || ledgers != 1 {
		t.Fatalf("racing identical proposal wrote records=%d ledger=%d, want 1/1", records, ledgers)
	}
	var applied int
	for _, replay := range replays {
		if !replay {
			applied++
		}
	}
	if applied != 1 {
		t.Fatalf("exactly one racer must have applied the write, %d did", applied)
	}
}

// TestOperationKeyAfterSubjectErasureDoesNotResurrect pins what a stale key
// replay means once its record has been erased with the subject: the caller may
// be refused, but no content may come back.
func TestOperationKeyAfterSubjectErasureDoesNotResurrect(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")
	res, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "erase me then replay", OpKey: "erase-then-replay"})
	mustOK(t, err)
	if res.Memory.ID == "" {
		t.Fatal("proposal must have an id")
	}
	n, err := e.mem.DeleteSubject(ctx, scope, "alice", "erase-zeno")
	mustOK(t, err)
	if n < 1 {
		t.Fatalf("subject erasure must remove the record, deleted=%d", n)
	}
	replay, err := e.mem.Propose(ctx, scope, ProposeRequest{Kind: KindUser, Text: "erase me then replay", OpKey: "erase-then-replay"})
	if err != nil {
		if code := contracts.Code(err); code != string(contracts.ErrNotFound) && code != string(contracts.ErrConflict) {
			t.Fatalf("replay after erasure must be refused as not-found or contention, got %s (%v)", code, err)
		}
		return
	}
	if replay.Memory.Text != "" {
		t.Fatalf("replay after subject erasure resurrected content: %q", replay.Memory.Text)
	}
}

// TestSearchModeMarksKeywordFallback pins the honesty of degraded search. A
// stand without an embedder must say so in the result mode and must not pretend
// to recall semantically: lexical hits stay, a lexically disjoint paraphrase
// finds nothing. A store with an embedder reports the other mode.
func TestSearchModeMarksKeywordFallback(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	scope := e.scope("alice")

	wired, err := e.mem.Search(ctx, scope, SearchRequest{Query: "contended retry", Limit: 10})
	mustOK(t, err)
	if wired.Mode != "hybrid" {
		t.Fatalf("a store with a wired embedder must report hybrid, got %q", wired.Mode)
	}

	bare, err := New(e.pool, e.arts, func(context.Context, contracts.Scope, string, string) error { return nil }, nil)
	mustOK(t, err)
	mustOK(t, bare.Migrate(ctx))
	bareScope := contracts.Scope{Tenant: scope.Tenant + "-bare", Deployment: scope.Deployment, Subject: "alice"}
	bp, err := bare.Propose(ctx, bareScope, ProposeRequest{Kind: KindUser, Text: "quarterly report ships on Friday", OpKey: "bare-propose"})
	mustOK(t, err)
	if _, err := bare.Review(ctx, bareScope, bp.Memory.ID, "approve", "bare-review"); err != nil {
		t.Fatalf("bare review: %v", err)
	}

	lexical, err := bare.Search(ctx, bareScope, SearchRequest{Query: "quarterly", Limit: 10})
	mustOK(t, err)
	if lexical.Mode != "lexical_graph" {
		t.Fatalf("a store without an embedder must mark the degraded mode, got %q", lexical.Mode)
	}
	if len(lexical.Hits) == 0 {
		t.Fatal("keyword fallback must still answer keyword queries")
	}
	paraphrase, err := bare.Search(ctx, bareScope, SearchRequest{Query: "financial briefing weekend deadline", Limit: 10})
	mustOK(t, err)
	if paraphrase.Mode != "lexical_graph" || len(paraphrase.Hits) != 0 {
		t.Fatalf("without an embedder a lexically disjoint paraphrase must NOT match: mode=%q hits=%d", paraphrase.Mode, len(paraphrase.Hits))
	}
}
