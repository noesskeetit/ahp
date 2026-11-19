package stateint

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// TestConcurrentClaimsSingleWinner starts one thread with one pending run in an
// isolated tenant/deployment, then races 100 concurrent ClaimRun calls against
// it. Exactly one claim must win the single lease slot; every other claim must
// return nil because, once granted, the thread lease is live for the whole TTL
// and is filtered out of the candidate scan (FOR UPDATE ... SKIP LOCKED plus
// the lease_expires predicate). This is the lease-slot invariant that makes the
// durable queue safe under contention.
func TestConcurrentClaimsSingleWinner(t *testing.T) {
	e := newEnv(t)

	// A pending, unclaimed head run: the thread lease is absent, so it is a
	// candidate. Do NOT claim it here — the goroutines do that.
	e.setToken(e.signTask(t, "task-"+contracts.NewID()))
	th, err := e.client.CreateThread(e.ctx, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	requireNoErr(t, err, "CreateThread")
	_, err = e.client.CreateRun(e.ctx, th.ID, "k-"+contracts.NewID(), pj(map[string]int{"n": 1}))
	requireNoErr(t, err, "CreateRun")

	const racers = 100
	var (
		wg      sync.WaitGroup
		wins    int64
		losers  int64
		errored int64
	)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			claimed, err := e.client.ClaimRun(e.ctx, fmt.Sprintf("w%d", i), 60000)
			switch {
			case err != nil:
				atomic.AddInt64(&errored, 1)
			case claimed != nil:
				atomic.AddInt64(&wins, 1)
			default:
				atomic.AddInt64(&losers, 1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if errored != 0 {
		t.Fatalf("%d claims returned errors (must be nil or win): lost=%d", errored, losers)
	}
	if wins != 1 {
		t.Fatalf("expected exactly one winner, got %d (losers=%d)", wins, losers)
	}
	if losers != racers-1 {
		t.Fatalf("expected %d losers, got %d", racers-1, losers)
	}
}

// TestConcurrentKVCASExactlyOneWins exercises compare-and-swap on a KV key:
// first an insert at expected version 0 (creating version 1), then N concurrent
// updates all asserting expected version 1 with distinct operation keys. Exactly
// one update must advance the value to version 2; the rest must observe a stale
// version and fail with CONFLICT. The surviving row must read back as version 2.
func TestConcurrentKVCASExactlyOneWins(t *testing.T) {
	e := newEnv(t)
	lp := e.claimFirst(t, "w")
	f := lp.fence
	ns := []string{"acct", "shared"}
	key := "counter"
	value := pj(map[string]int{"n": 1})

	seed, err := e.client.PutKV(e.ctx, f, ns, key, "seed-"+contracts.NewID(), 0, value)
	requireNoErr(t, err, "PutKV insert")
	if seed.Version != 1 {
		t.Fatalf("insert version = %d, want 1", seed.Version)
	}

	const racers = 24
	var (
		wg       sync.WaitGroup
		advanced int64
		conflict int64
	)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := e.client.PutKV(e.ctx, f, ns, key, fmt.Sprintf("op-%d-%s", i, contracts.NewID()), 1, value)
			switch {
			case err == nil:
				atomic.AddInt64(&advanced, 1)
			case contracts.Code(err) == contracts.ErrConflict:
				atomic.AddInt64(&conflict, 1)
			default:
				t.Errorf("PutKV CAS: unexpected error code %v (%v)", contracts.Code(err), err)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if advanced != 1 {
		t.Fatalf("expected exactly one CAS to advance the version, got %d", advanced)
	}
	if conflict != racers-1 {
		t.Fatalf("expected %d CONFLICT losers, got %d", racers-1, conflict)
	}

	got, err := e.client.GetKV(e.ctx, ns, key)
	requireNoErr(t, err, "GetKV after CAS")
	if got.Version != 2 {
		t.Fatalf("value version = %d, want 2", got.Version)
	}
}

// TestKVIdempotentReplayAndCrossThreadSharing proves two KV properties: an
// operation-key replay with an identical body returns the same version without
// advancing it, and a KV namespace is shared across runs/threads of the same
// subject (a value written under one thread's lease is visible and writable from
// another thread's lease in the same scope).
func TestKVIdempotentReplayAndCrossThreadSharing(t *testing.T) {
	e := newEnv(t)

	// First lease/thread writes the value under a stable operation key.
	lp1 := e.claimFirst(t, "w1")
	ns := []string{"profile"}
	key := "user"
	val := pj(map[string]string{"name": "ada"})
	opKey := "op-" + contracts.NewID()
	written, err := e.client.PutKV(e.ctx, lp1.fence, ns, key, opKey, 0, val)
	requireNoErr(t, err, "PutKV first")
	if written.Version != 1 {
		t.Fatalf("first write version = %d, want 1", written.Version)
	}

	// Replaying the same operation key with the same body is idempotent: same
	// version, no error.
	replay, err := e.client.PutKV(e.ctx, lp1.fence, ns, key, opKey, 0, val)
	requireNoErr(t, err, "PutKV replay")
	if replay.Version != 1 || replay.Key != key {
		t.Fatalf("replay drifted: version=%d key=%s", replay.Version, replay.Key)
	}

	// A second, independent thread+run in the same scope can read the same KV
	// value: namespaces are subject-scoped, not thread-scoped.
	lp2 := e.claimFirst(t, "w2")
	if lp2.thread.ID == lp1.thread.ID {
		t.Fatal("expected a distinct second thread")
	}
	seen, err := e.client.GetKV(e.ctx, ns, key)
	requireNoErr(t, err, "GetKV from second thread")
	if seen.Version != 1 {
		t.Fatalf("cross-thread read version = %d, want 1", seen.Version)
	}

	// And it can advance the shared value under its own lease + expected version.
	advanced, err := e.client.PutKV(e.ctx, lp2.fence, ns, key, "op2-"+contracts.NewID(), 1,
		pj(map[string]string{"name": "ada", "tier": "gold"}))
	requireNoErr(t, err, "PutKV cross-thread CAS")
	if advanced.Version != 2 {
		t.Fatalf("cross-thread CAS version = %d, want 2", advanced.Version)
	}

	var _ sdk.KVItem = advanced
}
