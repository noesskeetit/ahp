package core

import (
	"context"
	"errors"

	"github.com/ahp-platform/ahp/internal/space"
)

// DurableHighWater is the OPTIONAL capability a space.DurabilitySink may implement to
// report the step number the durable store expects NEXT for a given loop id, i.e.
// max(existing "<loopID>:<n>")+1 over the authoritative stream.
//
// It exists because the Core loop is not always the first writer of its own step keys.
// In the composed deployment the Space Coordinator streams every step of a leased run
// to the SAME State thread stream under the SAME canonical StepKey space, and it seeds
// its counter from the durable high-water on takeover. A recorder that restarts at zero
// after a re-claim is then one key behind: its appends collide with already-durable
// bodies (silently, because the recorder is best-effort) until it reaches a key the
// coordinator has not written yet -- where it lands a FOREIGN body (measured on stand
// 99f3396, run 20261031T165014.594581557Z: the recorder put the terminal `done` on the
// key the coordinator needed for its next `model` step, the coordinator got
// KEY_REUSE_MISMATCH, treated it as a durable stream hole and closed a run whose work
// had actually completed as status=failed). High-water seeding keeps the two writers in
// lockstep so identical bodies dedup instead of colliding.
type DurableHighWater interface {
	StepHighWater(ctx context.Context, loopID string) (int, error)
}

// stepRecorder drives the Core loop's durable per-step emission. It keys every
// emitted step with the canonical space.StepKey at the step's LOGICAL POSITION in the
// loop (0, 1, 2, ... in emission order) and binds the State-compatible fingerprint
// (space.StepFingerprint) so the Core loop owns its durability contract, independent of
// who wires the underlying store.
//
// The key is a logical position, NOT a count of this process's appends: re-delivering the
// same request re-emits the same steps in the same order, so it lands on the same keys and
// the authoritative stream dedupes it instead of growing. (Measured red before this rule:
// a re-run through the production seam added rows —
// tests/space/runtime_durability_seam_realstate_test.go:113.)
//
// It is nil-safe: a Runtime with cfg.Durable == nil skips direct sink recording.
// Recording is best-effort and never aborts the
// loop; a sink error is captured for inspection but does not change control flow
// (the Coordinator's own durable stream remains the recovery source of truth).
//
// Three rules keep "best-effort" from becoming "destructive":
//   - a life that CONTINUES a run whose earlier steps are already durable — the request
//     carried a restored checkpoint, so those bodies cannot be reproduced here — starts
//     where the durable store says the next key is, when the sink can answer that
//     (DurableHighWater). If the answer cannot be read, recording is DISABLED for the run
//     rather than guessed: an unknown key is exactly how a foreign body gets written onto
//     another writer's step.
//   - the same key carrying a different body stays a KEY_REUSE_MISMATCH: it is reported,
//     not renumbered away. That rejection is the only signal that a step was not
//     reproducible, and losing it is how a nondeterministic step becomes silent.
//   - a KEY_REUSE rejection stops all further recording. One collision already means the
//     two writers disagree about numbering; continuing would put foreign bodies on every
//     remaining key the coordinator still needs.
type stepRecorder struct {
	intentKey  string
	requireAck bool
	sink       space.DurabilitySink
	loopID     string
	pos        int
	lastErr    error
	appends    int
	seeded     bool
	blocked    bool
	// continuing marks a life that resumes a run from a restored checkpoint. Only such a
	// life aligns its first key with the durable high-water; see seed.
	continuing bool
}

func newStepRecorder(sink space.DurabilitySink, loopID string) *stepRecorder {
	return &stepRecorder{sink: sink, loopID: loopID}
}

// resumeFromDurable marks this life as a continuation of a run whose earlier steps an
// earlier life already made durable. The caller (Runtime.Run) sets it from the request's
// restored checkpoint, which is how production itself signals "this run was resumed".
func (rec *stepRecorder) resumeFromDurable() {
	if rec != nil {
		rec.continuing = true
	}
}

// seed aligns the counter with the durable store before the first append. It applies to a
// CONTINUING life only: a cold (re)delivery of the same request must reuse the logical
// keys an earlier life wrote, so identical bodies dedup instead of growing the stream.
// Sinks that do not implement DurableHighWater keep the historical behavior (they make no
// promise about keys an earlier life already wrote, and none is being claimed here).
func (rec *stepRecorder) seed(ctx context.Context) {
	rec.seeded = true
	if !rec.continuing {
		return
	}
	hw, ok := rec.sink.(DurableHighWater)
	if !ok {
		return
	}
	next, err := hw.StepHighWater(ctx, rec.loopID)
	if err != nil {
		rec.lastErr = err
		rec.blocked = true
		return
	}
	if next > rec.pos {
		rec.pos = next
	}
}

// record persists one step under the next canonical key. payload mirrors what the
// Coordinator persists to the thread stream (the whole marshaled StepResult), so the
// canonical fingerprint matches the State stream event digest byte-for-byte.
func (rec *stepRecorder) record(ctx context.Context, sr space.StepResult) {
	if rec == nil || rec.sink == nil || rec.blocked {
		return
	}
	if !rec.seeded {
		rec.seed(ctx)
		if rec.blocked {
			return
		}
	}
	key := space.StepKey(rec.loopID, rec.pos)
	if sr.StepType == space.StepModelDelta {
		key = space.ModelDeltaKey(rec.loopID, sr.GenerationID, sr.DeltaSequence)
	} else {
		rec.pos++
	}
	rec.appends++
	payload := jsonBytes(sr)
	fp, err := space.StepFingerprint(string(sr.StepType), payload)
	if err != nil {
		rec.lastErr = err
		return
	}
	if _, err := rec.sink.AppendDurable(ctx, key, string(sr.StepType), fp, payload); err != nil {
		rec.lastErr = err
		if errors.Is(err, space.ErrKeyReuse) {
			rec.blocked = true
		}
	}
}

// push records the step durably (if a sink is wired) then forwards it to the result
// channel. When Coordinator journals model intents, the next step waits for
// its durable acknowledgement; result publication cannot lag a later intent.
func (rec *stepRecorder) push(ctx context.Context, ch chan<- space.StepResult, sr space.StepResult) {
	if rec != nil {
		sr.ModelIntentKey = rec.intentKey
		if rec.requireAck {
			sr.Persisted = make(chan error, 1)
		}
	}
	rec.record(ctx, sr)
	select {
	case ch <- sr:
	default:
		select {
		case ch <- sr:
		case <-ctx.Done():
			return
		}
	}
	if sr.Persisted != nil {
		select {
		case <-sr.Persisted:
		case <-ctx.Done():
		}
	}
}
