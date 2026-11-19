package statejournal

import (
	"crypto/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
)

func TestDotsFIFOAdmissionAndPromotion(t *testing.T) {
	ctx, pool, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "dots", Subject: "user"}
	input := func(key string) Payload {
		return Payload{Inline: []byte(`{"question":"` + key + `"}`)}
	}
	admit := func(key, task string) journal.DotsEnqueueReceipt {
		t.Helper()
		req := journal.DotsEnqueueRequest{SessionID: "chat", Key: key, Input: input(key)}
		receipt, err := store.EnqueueDotsTurn(ctx, scope, task, req)
		if err != nil {
			t.Fatalf("admit %s: %v", key, err)
		}
		return receipt
	}
	first, second := admit("a", "task-a"), admit("b", "task-b")
	if first.RunID == "" || second.RunID == "" || first.RunID == second.RunID ||
		first.RunSequence != 1 || second.RunSequence != 2 || first.HeadSeq != 1 || second.HeadSeq != 2 {
		t.Fatalf("bad durable run identities: first=%+v second=%+v", first, second)
	}
	if admit("a", "task-a") != first {
		t.Fatal("queued replay changed the first run's identity")
	}
	if _, err := store.ClaimDotsTurn(ctx, scope, "chat", "task-b", time.Minute); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("newer task overtook the FIFO head: %v", err)
	}
	if _, err := store.ClaimTurn(ctx, scope, "chat", "native", time.Minute); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("generic claim reached a Dots run: %v", err)
	}
	lease, err := store.ClaimDotsTurn(ctx, scope, "chat", "task-a", time.Minute)
	if err != nil || lease.RunID != first.RunID || lease.RunSequence != 1 || lease.TaskID != "task-a" ||
		lease.Epoch != 1 || lease.InputSeq != first.HeadSeq || lease.HeadSeq != second.HeadSeq {
		t.Fatalf("claim head: %+v (%v)", lease, err)
	}
	third := admit("c", "task-c")
	if third.RunSequence != 3 || third.HeadSeq != 3 || admit("a", "task-a") != first {
		t.Fatalf("admit while active or replay failed: %+v", third)
	}
	changed := journal.DotsEnqueueRequest{SessionID: "chat", Key: "a", Input: input("changed")}
	if _, err := store.EnqueueDotsTurn(ctx, scope, "task-a", changed); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("changed retry under original key: %v", err)
	}
	changed.Input = input("a")
	if _, err := store.EnqueueDotsTurn(ctx, scope, "task-b", changed); contracts.Code(err) != contracts.ErrKeyReuse {
		t.Fatalf("task substitution under original key: %v", err)
	}
	generic := EnqueueRequest{SessionID: "chat", Harness: "dots", FormatVersion: 1, Key: "generic", Input: changed.Input}
	if _, err := store.EnqueueTurn(ctx, scope, generic); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("generic admission crossed into Dots FIFO: %v", err)
	}
	staleHead := AppendRequest{Key: "old-cas", ExpectedSeq: lease.HeadSeq, Events: []EventInput{
		{ID: "old-cas", Type: "turn.progress", Role: "assistant", Codec: "json", Payload: input("progress")},
	}}
	if _, err := store.AppendEvents(ctx, scope, "chat", lease.Lease, false, staleHead); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("admission advanced the head but native CAS accepted it: %v", err)
	}
	if _, err := store.AcquireOrCreateSession(ctx, scope, "chat", "dots", 1, "generic", time.Minute); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("generic acquisition crossed into Dots FIFO: %v", err)
	}
	if err := store.ReleaseLease(ctx, scope, "chat", lease.Lease); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("generic release bypassed the terminal write: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ahp_journal.sessions SET lease_expires_at=clock_timestamp()-interval '1 second'
		WHERE tenant_id=$1 AND session_id='chat'`, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if tenants, err := store.FindExpiredTurnTenants(ctx); err != nil || slices.Contains(tenants, scope.Tenant) {
		t.Fatalf("native sweep selected a Dots tenant: %v (%v)", tenants, err)
	}
	if count, err := store.RequeueExpiredTurns(ctx, scope.Tenant, 64); err != nil || count != 0 {
		t.Fatalf("native sweep moved Dots head: %d (%v)", count, err)
	}
	reclaimed, err := New(pool).ClaimDotsTurn(ctx, scope, "chat", "task-a", time.Minute)
	if err != nil || reclaimed.Epoch != 2 || reclaimed.RunID != first.RunID ||
		reclaimed.InputSeq != first.HeadSeq || reclaimed.HeadSeq != third.HeadSeq {
		t.Fatalf("reclaim must recover same head: %+v (%v)", reclaimed, err)
	}
	finish := AppendRequest{Key: "complete-a", ExpectedSeq: third.HeadSeq, FinishTurn: SessionIdle, Events: []EventInput{
		{ID: "complete-a", Type: "turn.completed", Role: "assistant", Codec: "json", Payload: input("done")},
	}}
	wait := finish
	wait.FinishTurn = SessionWaitingInput
	if _, err := store.AppendEvents(ctx, scope, "chat", reclaimed.Lease, false, wait); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("the scope of this Dots queue permits terminal completion only: %v", err)
	}
	if status, err := store.GetTurnStatus(ctx, scope, "chat"); err != nil || status.HeadSeq != third.HeadSeq ||
		status.InputSeq == nil || *status.InputSeq != first.HeadSeq {
		t.Fatalf("rejected disposition published an event or moved the head: %+v (%v)", status, err)
	}
	if _, err := store.AppendEvents(ctx, scope, "chat", lease.Lease, false, finish); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("stale lease promoted the FIFO: %v", err)
	}
	if _, err := store.AppendEvents(ctx, scope, "chat", reclaimed.Lease, false, finish); err != nil {
		t.Fatalf("finish head: %v", err)
	}
	if admit("a", "task-a") != first {
		t.Fatal("terminal admission replay changed the first run's identity")
	}
	if _, err := store.ClaimDotsTurn(ctx, scope, "chat", "task-c", time.Minute); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("third run overtook the second: %v", err)
	}
	next, err := store.ClaimDotsTurn(ctx, scope, "chat", "task-b", time.Minute)
	if err != nil || next.RunID != second.RunID || next.InputSeq != second.HeadSeq || next.Epoch != 3 {
		t.Fatalf("next run was not promoted: %+v (%v)", next, err)
	}
	if replay, err := store.AppendEvents(ctx, scope, "chat", reclaimed.Lease, false, finish); err != nil || replay.HeadSeq != 4 {
		t.Fatalf("terminal retry changed the current head: %+v (%v)", replay, err)
	}
	if status, err := store.GetTurnStatus(ctx, scope, "chat"); err != nil || status.InputSeq == nil ||
		*status.InputSeq != second.HeadSeq || status.Epoch != next.Epoch {
		t.Fatalf("terminal replay affected successor: %+v (%v)", status, err)
	}
}

func TestDotsFIFORequiresExplicitOptInForNewSession(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "dots", Subject: "user"}
	if err := store.CreateSession(ctx, scope, "existing", "dots", 1); err != nil {
		t.Fatal(err)
	}
	req := journal.DotsEnqueueRequest{SessionID: "existing", Key: "first", Input: Payload{Inline: []byte(`{}`)}}
	if _, err := store.EnqueueDotsTurn(ctx, scope, "task", req); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("existing session silently switched modes: %v", err)
	}
	if _, err := store.AcquireSession(ctx, scope, "existing", "generic", time.Minute); err != nil {
		t.Fatalf("existing session lost generic lease access: %v", err)
	}
}

func TestDotsFIFOClaimSkipsIndependentLockedSession(t *testing.T) {
	ctx, pool, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "dots", Subject: "user"}
	req := journal.DotsEnqueueRequest{SessionID: "chat-a", Key: "first", Input: Payload{Inline: []byte(`{}`)}}
	if _, err := store.EnqueueDotsTurn(ctx, scope, "worker-task", req); err != nil {
		t.Fatal(err)
	}
	req.SessionID = "chat-b"
	if _, err := store.EnqueueDotsTurn(ctx, scope, "worker-task", req); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT session_id FROM ahp_journal.sessions
		WHERE tenant_id=$1 AND session_id='chat-a' FOR UPDATE`, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	next, err := store.ClaimDotsTurn(ctx, scope, "", "worker-task", time.Minute)
	if err != nil || next.SessionID != "chat-b" {
		t.Fatalf("locked old session blocked independent session: %+v (%v)", next, err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	next, err = store.ClaimDotsTurn(ctx, scope, "", "worker-task", time.Minute)
	if err != nil || next.SessionID != "chat-a" {
		t.Fatalf("older session not claimable when unlocked: %+v (%v)", next, err)
	}
}

func TestDotsFIFOConcurrentAdmissionAndClaims(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "dots", Subject: "user"}
	const writers = 12
	type result struct {
		receipt journal.DotsEnqueueReceipt
		err     error
	}
	admitted := make(chan result, writers)
	start := make(chan struct{})
	for i := range writers {
		go func() {
			req := journal.DotsEnqueueRequest{SessionID: "chat", Key: strconv.Itoa(i / 2), Input: Payload{Inline: []byte(`{}`)}}
			<-start
			receipt, err := store.EnqueueDotsTurn(ctx, scope, "task", req)
			admitted <- result{receipt, err}
		}()
	}
	close(start)
	receipts := make(map[int64]journal.DotsEnqueueReceipt)
	duplicates := make(map[int64]int)
	for range writers {
		out := <-admitted
		if out.err != nil {
			t.Fatal(out.err)
		}
		duplicates[out.receipt.RunSequence]++
		if earlier, ok := receipts[out.receipt.RunSequence]; ok && earlier != out.receipt {
			t.Fatalf("concurrent retry produced two receipts: %+v != %+v", earlier, out.receipt)
		}
		receipts[out.receipt.RunSequence] = out.receipt
	}
	for seq := int64(1); seq <= writers/2; seq++ {
		if duplicates[seq] != 2 || receipts[seq].HeadSeq != seq || receipts[seq].RunID == "" {
			t.Fatalf("unexpected durable order: %+v / %+v", receipts, duplicates)
		}
	}

	type claim struct {
		lease journal.DotsTurnLease
		err   error
	}
	claims := make(chan claim, writers)
	beginClaims := make(chan struct{})
	for range writers {
		go func() {
			<-beginClaims
			lease, err := store.ClaimDotsTurn(ctx, scope, "chat", "task", time.Minute)
			claims <- claim{lease, err}
		}()
	}
	close(beginClaims)
	var taken int
	for range writers {
		out := <-claims
		if contracts.Code(out.err) == contracts.ErrNotFound {
			continue
		}
		if out.err != nil || out.lease.RunID != receipts[1].RunID || out.lease.Epoch != 1 {
			t.Fatalf("claim did not take the oldest run: %+v (%v)", out.lease, out.err)
		}
		taken++
	}
	if taken != 1 {
		t.Fatalf("claimed %d times, expected one lease", taken)
	}
}
