package emil

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"

	"github.com/ahp-platform/ahp/internal/space"
)

// RunStoreDurabilitySink implements space.DurabilitySink by routing every Core-loop
// step through the seam (StateRunStore.AppendEvent -> the EXISTING StateClient.AppendStream),
// i.e. the authoritative AHP State API. It turns the Core engine into a stateless pool
// whose per-step durability lives in the Agent State Store per the State API contract
// (PDF rev3.2 л.250/430: stateless-пул Core Agent; шаги хранятся в State Store по контракту
// State API). It adds NO new wire and NO new State operation — it only calls the seam.
//
// The per-run fence + task-token arrive via context (space.RunContext, injected by the
// Core loop), so one shared sink serves concurrent runs and scope/tenant/subject/deployment
// are NEVER placed in a body (they derive from the task-token principal at the State edge).
//
// Idempotency is AUTHORITATIVE at the State stream: an identical (key, digest) re-append is
// deduped to the original event; a reused key with different content is rejected (KEY_REUSE);
// a stale fence is rejected (STALE_EPOCH/LEASE_LOST -> ErrStaleEpoch). The in-sink fingerprint
// cache is consulted BEFORE the round-trip: it answers a same-process re-append with
// `replayed` without a second write, and rejects reused-key-different-content without
// touching the durable record. Fail-closed: a missing run-context is an error, never a
// silent skip.
type RunStoreDurabilitySink struct {
	store *StateRunStore
	mu    sync.Mutex
	seen  map[string]string
}

var _ space.DurabilitySink = (*RunStoreDurabilitySink)(nil)

// NewRunStoreDurabilitySink wires the durable sink to an existing seam StateRunStore.
func NewRunStoreDurabilitySink(s *StateRunStore) *RunStoreDurabilitySink {
	return &RunStoreDurabilitySink{store: s, seen: map[string]string{}}
}

func (k *RunStoreDurabilitySink) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	rc, ok := space.RunContextFrom(ctx)
	if !ok || rc.TaskToken == "" || rc.Fence.RunID == "" {
		return false, errors.New("emil/durability: missing run context (fence/task-token) — fail-closed")
	}
	l := Lease{
		ThreadID:  rc.ThreadID,
		RunID:     rc.Fence.RunID,
		Worker:    rc.Fence.Worker,
		Epoch:     rc.Fence.Epoch,
		TaskToken: rc.TaskToken,
	}
	// The in-process cache is checked BEFORE the round-trip. A key this sink already wrote
	// with this body is a replay and needs no second write; the same key with a different
	// body is a permanent reuse and must not reach the stream at all. A key this process
	// has not seen still goes to State, which stays the authority (KEY_REUSE /
	// STALE_EPOCH surface fail-closed from there).
	k.mu.Lock()
	prev, seenBefore := k.seen[key]
	k.mu.Unlock()
	if seenBefore {
		if prev == fingerprint {
			return true, nil // same-process idempotent replay; existing record unchanged
		}
		return false, space.ErrKeyReuse // permanent reject; existing record untouched
	}

	if _, err := k.store.AppendEvent(ctx, l, key, kind, payload); err != nil {
		return false, err // ErrStaleEpoch | KEY_REUSE | ... surfaced, fail-closed (never swallowed)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	k.seen[key] = fingerprint
	return false, nil
}

// StepHighWater implements core.DurableHighWater: it asks the AUTHORITATIVE State stream
// which step number comes next for this loop id, by paging the thread stream and taking
// max(n)+1 over the keys "<loopID>:<n>". It reads, never writes, and adds no State
// operation (ReadStream is the same read the Space Coordinator's own high-water scan
// uses, with the same page/bound so both writers agree on what "drained" means).
//
// Fail-closed: without a run context there is no token to read with, and a read that did
// not drain does not know the high-water. Both are errors, never a returned zero -- a
// guessed zero is what put a foreign body on a live writer's key (measured on stand
// 99f3396: a resumed run was closed failed although its work completed).
func (k *RunStoreDurabilitySink) StepHighWater(ctx context.Context, loopID string) (int, error) {
	rc, ok := space.RunContextFrom(ctx)
	if !ok || rc.TaskToken == "" || rc.Fence.RunID == "" {
		return 0, errors.New("emil/durability: missing run context (fence/task-token) — fail-closed")
	}
	if strings.TrimSpace(loopID) == "" {
		return 0, errors.New("emil/durability: high-water scan needs the loop id")
	}
	const page = 500
	const maxScan = 200000
	prefix := loopID + ":"
	highest := -1
	scanned := 0
	after := int64(0)
	for {
		evs, err := k.store.ReadEvents(ctx, rc.TaskToken, rc.ThreadID, after, page)
		if err != nil {
			return 0, err
		}
		for _, e := range evs {
			scanned++
			if strings.HasPrefix(e.Key, prefix) {
				if n, cerr := strconv.Atoi(strings.TrimPrefix(e.Key, prefix)); cerr == nil && n > highest {
					highest = n
				}
			}
			after = e.Sequence
		}
		if len(evs) < page {
			return highest + 1, nil
		}
		if scanned >= maxScan {
			return 0, errors.New("emil/durability: step high-water scan exceeded its bound without draining the stream")
		}
	}
}
