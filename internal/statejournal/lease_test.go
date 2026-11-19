package statejournal

import (
	"crypto/rand"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestAcquireOrCreateSessionPostgres(t *testing.T) {
	ctx, _, store := journalTestStore(t)
	scope := contracts.Scope{Tenant: "tenant-" + rand.Text(), Deployment: "langgraph", Subject: "user"}
	const session = "thread"
	if _, err := store.AcquireOrCreateSession(ctx, scope, session, "langgraph", 1, "", time.Minute); contracts.Code(err) != contracts.ErrInvalid {
		t.Fatalf("invalid worker should prevent creation: %v", err)
	}
	page, err := store.ListSessions(ctx, scope, "", "", 10)
	if err != nil || len(page.IDs) != 0 {
		t.Fatalf("a rejected acquisition left a session: %+v (%v)", page, err)
	}

	type result struct {
		lease Lease
		err   error
	}
	const attempts = 5
	start := make(chan struct{})
	results := make(chan result, attempts)
	for range attempts {
		go func() {
			<-start
			lease, err := store.AcquireOrCreateSession(ctx, scope, session, "langgraph", 1, rand.Text(), time.Minute)
			results <- result{lease: lease, err: err}
		}()
	}
	close(start)
	var winner Lease
	for range attempts {
		got := <-results
		if got.err == nil {
			if winner.Worker != "" || got.lease.Epoch != 1 {
				t.Fatalf("expected exactly one first lease, already have %+v, got %+v", winner, got.lease)
			}
			winner = got.lease
		} else if contracts.Code(got.err) != contracts.ErrConflict {
			t.Fatalf("unexpected admission error: %v", got.err)
		}
	}
	if winner.Worker == "" {
		t.Fatal("no lease won")
	}
	if err := store.ReleaseLease(ctx, scope, session, winner); err != nil {
		t.Fatal(err)
	}
	foreign := scope
	foreign.Subject = "another-user"
	if _, err := store.AcquireOrCreateSession(ctx, foreign, session, "langgraph", 1, "second", time.Minute); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("foreign user acquired the session: %v", err)
	}
	if _, err := store.AcquireOrCreateSession(ctx, scope, session, "langgraph", 2, "second", time.Minute); contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("an unsupported format change acquired the session: %v", err)
	}
	next, err := store.AcquireOrCreateSession(ctx, scope, session, "langgraph", 1, "second", time.Minute)
	if err != nil || next.Epoch != 2 || next.Worker != "second" {
		t.Fatalf("unexpected second lease: %+v (%v)", next, err)
	}
	if err := store.ReleaseLease(ctx, scope, session, winner); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("previous owner retained write ability: %v", err)
	}
}
