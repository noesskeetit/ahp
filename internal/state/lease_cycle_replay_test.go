package state

// The R2 decision, measured instead of argued: after one lock order exists, does
// anything still abort?
//
// WHY THIS IS A SIBLING FILE AND NOT PART OF THE LOAD FILE. The client-visible
// tally lives in lease_cycle_repro_test.go, which has to compile on the broken tree
// as well as the fixed one. These counters - the replays and give-ups a bounded
// replay loop keeps - exist only on the fixed tree. Reading them from the load file
// would make that file unbuildable on the tree it is supposed to go red on, which
// would quietly turn the RED half of this closure into an assertion nobody ran.
//
// WHAT THE NUMBERS MEAN. Zero replays with a non-zero overlap count means the
// lock order removed the aborts and the loop is dormant insurance: correct to keep,
// nothing to lean on, no reason to widen it. Non-zero replays with zero give-ups
// means something outside this package's order still aborts the lease writers, and
// the loop is load-bearing. Give-ups are a failure at any number: the abort reached
// a client, which is the original complaint wearing a bounded-budget costume.

import (
	"context"
	"testing"
	"time"
)

func TestLeaseCycleReplayBudgetAfterTheLockOrderFix(t *testing.T) {
	_ = cpFixture(t) // provisions and migrates this run's isolated database
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cfg := laneConfig(t)
	cfg.MaxConns = 24
	pool, err := newCyclePool(t, ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := s.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("MigrateCheckpoints: %v", err)
	}

	rounds := cycleRoundsOrDefault(t, 20)
	count := runLeaseCycleLoad(t, s, ctx, rounds)
	count.report(t, rounds)

	if n := count.contended.Load(); n < int64(rounds) {
		t.Fatalf("the claim side and the lease-writer side overlapped only %d time(s) in %d rounds, "+
			"so no abort was possible and these counters would prove nothing", n, rounds)
	}

	var replays uint64
	counts := s.LeaseRetryCounts()
	for _, door := range []string{"release", "suspend"} {
		c := counts[door]
		t.Logf("lease-cycle replay counters: door=%s replays=%d give_ups=%d", door, c.Replays, c.GiveUps)
		replays += c.Replays
		if c.GiveUps != 0 {
			t.Errorf("%s ran out of replay budget %d time(s): the aborts are still there, only bounded",
				door, c.GiveUps)
		}
	}
	if n := count.deadlock.Load() + count.serialization.Load() + count.conflict.Load() + count.unavailable.Load(); n != 0 {
		t.Errorf("%d database abort(s) still reached a client after the lock-order fix", n)
	}
	if replays == 0 {
		t.Logf("lease-cycle R2 verdict: the replay loop never fired in %d rounds x %d workers - the lock "+
			"order removed the aborts, so the loop is dormant insurance and stays as it is",
			rounds, cycleWorkersPerRound)
		return
	}
	t.Logf("lease-cycle R2 verdict: the replay loop fired %d time(s) in %d rounds x %d workers with no "+
		"give-up - something outside this package's order still aborts the lease writers, so the loop is "+
		"load-bearing at this depth", replays, rounds, cycleWorkersPerRound)
}
