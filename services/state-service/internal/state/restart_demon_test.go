package state

// Disaster-recovery / restart fidelity for the State demon (root P1 "backup /
// restore / lifecycle" + matrix row 15). The property: a State demon RESTARTED
// from the durable database serves the SAME checkpoints, and the history bound
// and the tenant fence are enforced from the database alone -- never from a
// cache that a restart would have emptied.
//
// The restart is faithful in the only ways the claim depends on: a brand-new
// pgxpool AND a brand-new *Store over the same run database, so nothing the
// first instance held in memory can carry over. The fresh instance runs the
// production startup (Migrate + MigrateCheckpoints) idempotently over data it
// did not author -- exactly a demon rebooting -- and must return byte-identical
// history, bound it from a cold cache, and refuse a foreign tenant on the same
// restored database.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

func TestRestoredDemonReadsIdenticalCheckpointHistory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// --- first demon writes a bounded chain with a shared blob. ---
	pool1, _ := isolatedPool(t)
	s1 := New(pool1)
	if err := s1.Migrate(ctx); err != nil {
		t.Fatalf("s1.Migrate: %v", err)
	}
	if err := s1.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("s1.MigrateCheckpoints: %v", err)
	}
	scope := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: "d-" + contracts.NewID(),
		Subject: "subj-" + contracts.NewID()}
	threadID := createHistoryThread(t, ctx, s1, scope)
	fence := claimHistoryLease(t, ctx, s1, scope)

	const steps = 6
	const ns = "ns"
	for i := 0; i < steps; i++ {
		id := fmt.Sprintf("cp-%02d", i)
		parent := ""
		if i > 0 {
			parent = fmt.Sprintf("cp-%02d", i-1)
		}
		parts := []Part{{Channel: "log", Version: fmt.Sprintf("log-%02d", i),
			Data: []byte(fmt.Sprintf("durable state at step %d", i))}}
		if i == 0 {
			parts = append(parts, Part{Channel: "shared", Version: "shared-1", Data: []byte("written once, read by many")})
		} else {
			parts = append(parts, Part{Channel: "shared", Version: "shared-1"})
		}
		if _, err := s1.PutCheckpoint(ctx, scope, fence,
			Checkpoint{ID: id, Namespace: ns, Version: int64(i + 1), Parent: parent, Parts: parts}, "k-"+id); err != nil {
			t.Fatalf("PutCheckpoint %s: %v", id, err)
		}
	}
	want, err := s1.ListCheckpoints(ctx, scope, threadID, ns, 0)
	if err != nil {
		t.Fatalf("s1.ListCheckpoints: %v", err)
	}
	if len(want) != steps {
		t.Fatalf("wrote %d checkpoints, first read returned %d", steps, len(want))
	}

	// --- RESTART: brand-new pool AND brand-new Store over the same run DB. ---
	pool2, err := pgxpool.NewWithConfig(ctx, isolatedConfig(t))
	if err != nil {
		t.Fatal("restarted pool could not be created (DSN not printed)")
	}
	defer pool2.Close()
	s2 := New(pool2)
	// A demon restart runs startup migrations against data it did not author;
	// that must be deterministic and must not disturb what is already durable.
	if err := s2.Migrate(ctx); err != nil {
		t.Fatalf("restored s2.Migrate: %v", err)
	}
	if err := s2.MigrateCheckpoints(ctx); err != nil {
		t.Fatalf("restored s2.MigrateCheckpoints: %v", err)
	}

	got, err := s2.ListCheckpoints(ctx, scope, threadID, ns, 0)
	if err != nil {
		t.Fatalf("restored s2.ListCheckpoints: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("restored history length = %d, want %d", len(got), len(want))
	}
	gotByID := indexCheckpoints(got)
	for _, w := range want {
		g, ok := gotByID[w.ID]
		if !ok {
			t.Fatalf("restored demon is missing checkpoint %s", w.ID)
		}
		if g.Parent != w.Parent || g.Version != w.Version {
			t.Fatalf("restored checkpoint %s identity drifted: got parent=%q ver=%d want parent=%q ver=%d",
				w.ID, g.Parent, g.Version, w.Parent, w.Version)
		}
		if err := sameParts(g.Parts, w.Parts); err != nil {
			t.Fatalf("restored checkpoint %s bytes differ: %v", w.ID, err)
		}
	}

	// --- a cold instance bounds history from the DB alone. ---
	if _, err := s2.PruneCheckpointHistory(ctx, scope,
		CheckpointPruneRequest{ThreadID: threadID, Namespace: ns, Limit: 3}); err != nil {
		t.Fatalf("restored prune: %v", err)
	}
	bounded, err := s2.ListCheckpoints(ctx, scope, threadID, ns, 0)
	if err != nil {
		t.Fatalf("post-prune read: %v", err)
	}
	if len(bounded) != 3 {
		t.Fatalf("cold-instance prune did not bound history to 3, got %d", len(bounded))
	}
	// The kept window must be the three newest (highest version), proven from the
	// surviving bytes -- not a positional slice of an order we never asserted.
	kept := indexCheckpoints(bounded)
	for _, w := range want {
		newest := w.Version > int64(steps-3) // versions steps-2 .. steps are kept
		_, present := kept[w.ID]
		if present != newest {
			t.Fatalf("cold-instance prune kept the wrong window: %s (version %d) present=%v expected=%v",
				w.ID, w.Version, present, newest)
		}
	}

	// --- a foreign tenant on the same restored DB never sees the owner's rows:
	// the fence is enforced from the database, not a cache the restart dropped. ---
	foreign := contracts.Scope{Tenant: "t-" + contracts.NewID(), Deployment: "d-" + contracts.NewID(),
		Subject: "subj-" + contracts.NewID()}
	leaked, err := s2.ListCheckpoints(ctx, foreign, threadID, ns, 0)
	if err != nil {
		code := contracts.Code(err)
		if code != contracts.ErrNotFound && code != contracts.ErrForbidden {
			t.Fatalf("foreign restore read must be denied (NOT_FOUND/FORBIDDEN), got code=%v", code)
		}
		return // honestly denied
	}
	if len(leaked) > 0 {
		t.Fatalf("a foreign tenant read %d of a restored thread it never earned -- fence is in-memory, not durable", len(leaked))
	}
}

func indexCheckpoints(cps []Checkpoint) map[string]Checkpoint {
	m := make(map[string]Checkpoint, len(cps))
	for _, c := range cps {
		m[c.ID] = c
	}
	return m
}

func sameParts(got, want []Part) error {
	if len(got) != len(want) {
		return fmt.Errorf("part count %d vs %d", len(got), len(want))
	}
	key := func(p Part) string { return p.Channel + "/" + p.Version }
	gm := map[string]Part{}
	for _, p := range got {
		gm[key(p)] = p
	}
	for _, w := range want {
		g, ok := gm[key(w)]
		if !ok {
			return fmt.Errorf("missing part %s", key(w))
		}
		if !bytes.Equal(g.Data, w.Data) {
			return fmt.Errorf("part %s bytes differ (len %d vs %d)", key(w), len(g.Data), len(w.Data))
		}
	}
	return nil
}
