package space

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// This file owns the durable-emission contract the Core loop relies on. It lives in
// the space layer (the lowest shared package: internal/core imports space, never the
// reverse) so BOTH the Coordinator's production stream emit and the Core loop's own
// durable drive key steps through one canonical generator.
//
// Background (iv#74): a State stream event is idempotent per (thread, key) and binds
// a digest of (kind, payload) to that key. Re-appending the same key with the SAME
// body is deduped (a safe retried emit); re-appending a key bound to a DIFFERENT
// body is rejected with KEY_REUSE_MISMATCH. Consequently a multi-step loop MUST use a
// distinct key per step — a single constant key rejects every step after the first.

// StepKey returns the canonical, per-step idempotency key for a durable Core-loop
// emission of loop loopID at monotonic index step. The key is stable for a given
// (loopID, step) so a retried emit of the same step dedupes instead of duplicating,
// and distinct across steps so a loop does not collide with itself.
func StepKey(loopID string, step int) string {
	return loopID + ":" + strconv.Itoa(step)
}

// CancelKey is the durable stream key of the terminal frame a CANCEL writes for a turn.
//
// It deliberately lives OUTSIDE the numeric step-key space that StepKey produces: the worker
// is still appending steps under StepKey(turn, n) while the cancel lands, so reusing that
// space would race (same key, different payload -> KEY_REUSE_MISMATCH, and the terminal
// frame would be lost). One fixed key per turn is also idempotent by payload digest, so a
// retried cancel cannot stack duplicate terminal frames.
func CancelKey(turnID string) string {
	return turnID + ":cancel"
}

// CancelRequestedKey is the durable, append-only journal key carrying a cancel INTENT for
// a run that is currently LEASED to a runner.
//
// It exists for the case CancelTurn cannot close honestly: the writer asking for the
// cancel does not hold the run's lease, so it is not entitled to write the run's status -
// that fence (threads.lease_run_id/lease_worker/lease_epoch, enforced by State) is what
// keeps a zombie worker from stamping verdicts. Refusing such a cancel with 409 and
// writing nothing is honest but invisible: the user cannot distinguish "my Stop was
// queued" from "my Stop never arrived".
//
// The contract (director, 2026-11-09): the intent is appended to the run's own journal
// under THIS key, which breaks no fence because it is an append-only event, not a status
// write. The lease holder - and only the holder, because it is the only writer entitled to
// close the run - observes it at the next checkpoint/lease tick and closes the run itself
// with close_reason=cancel_requested. Drain bound: the holder's lease TTL (PDF rev3.2
// §3.6 с.36) plus the attempt ceiling (close_reason=attempt_ceiling_exceeded); if the
// holder never confirms, the reconcile path decides the terminal outcome, so no run can
// sit leased-and-cancelling forever.
//
// Writer side (the State operation and the user-visible status) is owned by Galileo;
// holder side (observation and the honest terminal) lives here. Both sides use THIS key.
// Like CancelKey it lives outside the numeric step-key space, so it cannot collide with
// the holder's in-flight StepKey appends, and it is idempotent by payload digest, so a
// retried intent does not stack.
func CancelRequestedKey(runID string) string {
	return runID + ":cancel-requested"
}

// StepFingerprint mirrors the State service's per-event digest (internal/state/
// events.go AppendStream): the idempotency body is the digest of the pair (kind,
// payload). Two emissions sharing a key are the same durable event iff their
// fingerprints match; a differing fingerprint is a key reuse.
func StepFingerprint(kind string, payload json.RawMessage) (string, error) {
	return contracts.Digest(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: kind, Payload: payload})
}

// ErrKeyReuse is returned by a DurabilitySink when a key already bound to one
// fingerprint is written with a different fingerprint. The rejection is PERMANENT: a
// later retry of the same key with the conflicting body still fails, matching the
// State contract (KEY_REUSE_MISMATCH).
var ErrKeyReuse = errors.New("space: durable step key reused with a different fingerprint")

// DurabilitySink is the port the Core loop uses to persist each step durably under a
// canonical per-step key. Implementations MUST honor the State idempotency contract:
//   - a key not yet seen is stored and reported with replayed=false;
//   - re-appending a key with the SAME fingerprint is a no-op reported with
//     replayed=true, leaving the existing record unchanged (no growth);
//   - appending a key bound to a DIFFERENT fingerprint returns ErrKeyReuse and leaves
//     the existing record untouched.
type DurabilitySink interface {
	AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (replayed bool, err error)
}

type memDurable struct {
	kind        string
	fingerprint string
	payload     json.RawMessage
}

// MemDurabilitySink is a contract-faithful, in-memory DurabilitySink. It is the
// reference for the PORT (idempotent replay, permanent key-reuse reject) and is used
// by the Core loop tests; it is NOT the authoritative store. The store-side reuse
// semantics it models are validated against the REAL State service over PostgreSQL in
// tests/space/coordinator_idempotency_realstate_test.go.
type MemDurabilitySink struct {
	byKey map[string]memDurable
}

func NewMemDurabilitySink() *MemDurabilitySink {
	return &MemDurabilitySink{byKey: make(map[string]memDurable)}
}

func (s *MemDurabilitySink) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	if cur, ok := s.byKey[key]; ok {
		if cur.fingerprint == fingerprint {
			return true, nil // idempotent replay: existing record unchanged
		}
		return false, ErrKeyReuse // permanent reject; existing record untouched
	}
	s.byKey[key] = memDurable{
		kind:        kind,
		fingerprint: fingerprint,
		payload:     append(json.RawMessage(nil), payload...),
	}
	return false, nil
}

// Len reports the number of distinct durable records stored.
func (s *MemDurabilitySink) Len() int { return len(s.byKey) }

// Get returns the stored (kind, payload) for key, if present.
func (s *MemDurabilitySink) Get(key string) (kind string, payload json.RawMessage, ok bool) {
	d, ok := s.byKey[key]
	return d.kind, d.payload, ok
}

var _ DurabilitySink = (*MemDurabilitySink)(nil)

// ModelDeltaKey keeps partial fragments outside completed-step/checkpoint numbering.
func ModelDeltaKey(runID, generationID string, sequence int64) string {
	return runID + ":delta:" + generationID + ":" + strconv.FormatInt(sequence, 10)
}
