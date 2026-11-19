package core

// durability_highwater_test.go — the Core loop's stepRecorder shares the canonical
// StepKey space with the Space Coordinator, which streams the SAME run's steps to the
// SAME State thread stream and seeds its counter from the durable high-water. These
// tests pin the two-writer contract that the stand defect broke (99f3396, run
// 20261031T165014.594581557Z_eeqbhtxulfdyqlvkcbjbyp2pie): after a re-claim the recorder
// restarted at key 0, so its writes silently collided until it reached a free key, where
// it landed the terminal `done` on the key the coordinator needed for its next `model`
// step. The coordinator saw KEY_REUSE_MISMATCH, closed the run as failed, and the run
// that had actually finished its work was reported failed.

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/ahp-platform/ahp/internal/space"
)

// streamStore is a faithful stand-in for the authoritative State stream: an append on a
// fresh key appends, an append on an existing key replays iff the body is byte-identical,
// and otherwise is a PERMANENT key reuse (exactly State's documented rule, mirrored by
// space.StepFingerprint).
type streamStore struct {
	mu       sync.Mutex
	keys     map[string]string // key -> fingerprint
	order    []string
	sequence int64
	readErr  error
}

func newStreamStore() *streamStore {
	return &streamStore{keys: map[string]string{}}
}

func (s *streamStore) fingerprint(kind string, payload []byte) string {
	fp, err := space.StepFingerprint(kind, json.RawMessage(payload))
	if err != nil {
		panic(err)
	}
	return fp
}

func (s *streamStore) append(key, kind string, payload json.RawMessage) (bool, error) {
	if err := s.canary(); err != nil {
		return false, err
	}
	fp := s.fingerprint(kind, payload)
	s.mu.Lock()
	defer s.mu.Unlock()
	if prev, ok := s.keys[key]; ok {
		if prev == fp {
			return true, nil // idempotent replay: the same durable event
		}
		return false, space.ErrKeyReuse
	}
	s.keys[key] = fp
	s.order = append(s.order, key+"\x00"+kind)
	s.sequence++
	return false, nil
}

func (s *streamStore) canary() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readErr
}

// highWater mirrors the coordinator's own scan: max(n)+1 over keys "<loopID>:<n>".
func (s *streamStore) highWater(loopID string) (int, error) {
	if err := s.canary(); err != nil {
		return 0, err
	}
	prefix := loopID + ":"
	highest := -1
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.keys {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimPrefix(key, prefix)); err == nil && n > highest {
			highest = n
		}
	}
	return highest + 1, nil
}

func (s *streamStore) kinds() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.order))
	for _, e := range s.order {
		parts := strings.SplitN(e, "\x00", 2)
		out = append(out, parts[0]+"="+parts[1])
	}
	return out
}

// twoWriterSink is the sink the recorder drives; the coordinator writes through append()
// directly, so both writers really share one key space (the production shape).
type twoWriterSink struct{ store *streamStore }

func (t twoWriterSink) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	return t.store.append(key, kind, payload)
}

func (t twoWriterSink) StepHighWater(ctx context.Context, loopID string) (int, error) {
	return t.store.highWater(loopID)
}

// plainSink deliberately does NOT implement DurableHighWater, to pin that such sinks keep
// the historical behavior instead of being silently disabled.
type plainSink struct {
	mu   sync.Mutex
	keys []string
	err  error
}

func (p *plainSink) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return false, p.err
	}
	p.keys = append(p.keys, key+":"+kind)
	return false, nil
}

type reuseSink struct {
	calls []string
}

func (r *reuseSink) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	r.calls = append(r.calls, key)
	if len(r.calls) == 2 {
		return false, space.ErrKeyReuse
	}
	return false, nil
}

type failingHWSink struct {
	calls []string
}

func (f *failingHWSink) AppendDurable(ctx context.Context, key, kind, fingerprint string, payload json.RawMessage) (bool, error) {
	f.calls = append(f.calls, key)
	return false, nil
}

func (f *failingHWSink) StepHighWater(ctx context.Context, loopID string) (int, error) {
	return 0, errors.New("state: unavailable: read stream")
}

func steps() []space.StepResult {
	return []space.StepResult{
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"m1"}`)},
		{StepType: space.StepTool, Content: json.RawMessage(`{"done":false,"content":{"size":6}}`)},
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"m2"}`)},
		{StepType: space.StepDone, Content: json.RawMessage(`null`), Done: true},
	}
}

// A recorder that starts a new attempt on a run whose stream already holds steps must
// continue from the durable high-water, never from 0.
func TestStepRecorderResumesFromDurableHighWater(t *testing.T) {
	store := newStreamStore()
	loopID := "run-reclaim"
	// The previous life wrote keys 0 and 1; the coordinator's high-water is therefore 2.
	for i, kind := range []string{"model", "tool"} {
		if _, err := store.append(space.StepKey(loopID, i), kind, json.RawMessage(`{"prior":1}`)); err != nil {
			t.Fatalf("seed stream: %v", err)
		}
	}

	rec := newStepRecorder(twoWriterSink{store: store}, loopID)
	rec.resumeFromDurable() // this life resumes a run: it is the ONLY case that seeds
	for _, sr := range steps() {
		rec.record(context.Background(), sr)
	}
	if rec.lastErr != nil {
		t.Fatalf("recorder error after high-water seed: %v", rec.lastErr)
	}
	got := store.kinds()
	want := []string{
		space.StepKey(loopID, 0) + "=model", space.StepKey(loopID, 1) + "=tool",
		space.StepKey(loopID, 2) + "=model", space.StepKey(loopID, 3) + "=tool",
		space.StepKey(loopID, 4) + "=model", space.StepKey(loopID, 5) + "=done",
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("stream keys/wrong: got %v want %v", got, want)
	}
}

// If the high-water cannot be read the recorder must NOT guess: it stops recording, and
// the run's own emission to the result channel is untouched (best-effort, never fatal).
func TestStepRecorderStopsWhenHighWaterUnknowable(t *testing.T) {
	sink := &failingHWSink{}
	rec := newStepRecorder(sink, "run-x")
	rec.resumeFromDurable() // a continuation is the life that must not guess the high-water
	ch := make(chan space.StepResult, 8)
	for _, sr := range steps() {
		rec.push(context.Background(), ch, sr)
	}
	close(ch)
	if len(sink.calls) != 0 {
		t.Fatalf("recorder wrote keys with an unknown high-water: %v", sink.calls)
	}
	if rec.lastErr == nil || !rec.blocked {
		t.Fatalf("recorder must capture the read error and stop: err=%v blocked=%v", rec.lastErr, rec.blocked)
	}
	if n := len(ch); n != len(steps()) {
		t.Fatalf("recorder failure must not swallow steps: %d of %d", n, len(steps()))
	}
}

// One key reuse means the two writers disagree about numbering. Continuing would put a
// foreign body on every remaining key the coordinator still needs, so recording stops.
func TestStepRecorderStopsAfterKeyReuse(t *testing.T) {
	sink := &reuseSink{}
	rec := newStepRecorder(sink, "run-y")
	for _, sr := range steps() {
		rec.record(context.Background(), sr)
	}
	if len(sink.calls) != 2 {
		t.Fatalf("recorder kept appending after KEY_REUSE: %v", sink.calls)
	}
	if !rec.blocked {
		t.Fatal("recorder must be blocked after KEY_REUSE")
	}
}

// A sink that makes no high-water promise keeps working exactly as before (no silent
// behavior change for other lanes' wiring).
func TestStepRecorderWithoutHighWaterCapabilityKeepsHistoricalBehavior(t *testing.T) {
	sink := &plainSink{}
	rec := newStepRecorder(sink, "run-z")
	for _, sr := range steps() {
		rec.record(context.Background(), sr)
	}
	if strings.Join(sink.keys, ",") != "run-z:0:model,run-z:1:tool,run-z:2:model,run-z:3:done" {
		t.Fatalf("unexpected keys for a sink without the capability: %v", sink.keys)
	}
	if rec.blocked {
		t.Fatal("a sink without the capability must not be treated as broken")
	}
}

// twoWriterAttempt replays ONE production attempt against one shared durable stream with
// BOTH writers that exist in the composed deployment:
//
//	recorder     - internal/core stepRecorder (AS_ENGINE_DURABLE=1), records each step
//	               BEFORE sending it on the results channel;
//	coordinator  - internal/space coordinator, which seeds its counter from the durable
//	               high-water, then appends the body it received on the channel.
//
// The channel is buffered (32 in production) and the coordinator also writes a runtime
// snapshot per step, so the recorder legitimately runs AHEAD of the coordinator. That
// ordering is the whole defect: a recorder whose counter is one behind writes each step
// onto the PREVIOUS step's key, colliding silently, until it reaches a key the
// coordinator has not written yet — and there it lands a body that belongs to a LATER
// step. The coordinator's next append then hits KEY_REUSE_MISMATCH, treats it as a
// durable stream hole, cancels Core and closes a run whose work had completed as
// status=failed. That is exactly what the stand measured (99f3396, run
// 20261031T165014.594581557Z_eeqbhtxulfdyqlvkcbjbyp2pie: `done` durable at :5, the
// coordinator's `model` append for :5 rejected 0.9 s later, final status failed).
//
// crash=true stops after recording, before any coordinator append — a SIGKILL mid-run.
// resume=true marks the attempt as a checkpoint continuation, the only life that seeds its
// first key from the durable high-water.
func twoWriterAttempt(t *testing.T, store *streamStore, loopID string, steps []space.StepResult, crash, resume bool) (int, error) {
	t.Helper()
	ctx := context.Background()
	coordinatorSeq, err := store.highWater(loopID)
	if err != nil {
		return 0, err
	}
	rec := newStepRecorder(twoWriterSink{store: store}, loopID)
	if resume {
		rec.resumeFromDurable()
	}
	results := make(chan space.StepResult, len(steps)) // buffered: the recorder runs ahead
	for _, sr := range steps {
		rec.push(ctx, results, sr)
	}
	if crash {
		return coordinatorSeq, nil
	}
	if rec.lastErr != nil {
		return coordinatorSeq, rec.lastErr // the recorder's own collision is a symptom
	}
	seq := coordinatorSeq
	for range steps {
		got := <-results
		payload, err := json.Marshal(got)
		if err != nil {
			return seq, err
		}
		if _, err := store.append(space.StepKey(loopID, seq), string(got.StepType), payload); err != nil {
			return seq, err // this append is the coordinator's KEY_REUSE_MISMATCH on the stand
		}
		seq++
	}
	if _, err := store.append(space.StepKey(loopID, seq), "status", json.RawMessage(`{"status":"done"}`)); err != nil {
		return seq, err
	}
	return seq + 1, nil
}

// The stand defect as a unit repro: one step durable, the worker dies, the re-claimed run
// resumes with different bodies. Every step must land exactly once and neither writer may
// see a key reuse.
func TestRecorderAndCoordinatorStreamStayInLockstepAcrossReclaim(t *testing.T) {
	store := newStreamStore()
	loopID := "turn-1"

	if _, err := twoWriterAttempt(t, store, loopID, []space.StepResult{
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"m-before-kill"}`)},
	}, true, false); err != nil {
		t.Fatalf("attempt 1 (before the kill): %v", err)
	}

	resumed := []space.StepResult{
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"m-resumed"}`)},
		{StepType: space.StepTool, Content: json.RawMessage(`{"done":false,"content":{"size":6}}`)},
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"report"}`)},
		{StepType: space.StepTool, Content: json.RawMessage(`{"done":false,"content":{"size":10}}`)},
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"summary"}`)},
		{StepType: space.StepDone, Content: json.RawMessage(`null`), Done: true},
	}
	next, err := twoWriterAttempt(t, store, loopID, resumed, false, true)
	if err != nil {
		t.Fatalf("re-claimed run must not collide on the durable stream: %v", err)
	}

	got := strings.Join(store.kinds(), ",")
	want := "turn-1:0=model,turn-1:1=model,turn-1:2=tool,turn-1:3=model,turn-1:4=tool,turn-1:5=model,turn-1:6=done,turn-1:7=status"
	if got != want {
		t.Fatalf("durable stream shape wrong:\n got %s\nwant %s", got, want)
	}
	if next != 8 {
		t.Fatalf("next coordinator key = %d, want 8", next)
	}
}

// The negative: switching the recorder off must never change a run's outcome. A step that
// genuinely failed still reaches the reader (and the coordinator still closes the run
// failed); a run that was abandoned has nothing but its own failure to report. Durability
// convenience never becomes a verdict.
func TestBlockedRecorderNeverChangesRunOutcome(t *testing.T) {
	sink := &failingHWSink{}
	rec := newStepRecorder(sink, "run-negative")
	rec.resumeFromDurable() // the blocked recorder under test is a continuation's recorder
	ch := make(chan space.StepResult, 4)
	failure := space.StepResult{StepType: space.StepError, Content: json.RawMessage(`{"error":"model generate step 0: boom"}`)}
	rec.push(context.Background(), ch, space.StepResult{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"x"}`)})
	rec.push(context.Background(), ch, failure)
	close(ch)

	if len(sink.calls) != 0 {
		t.Fatalf("a blocked recorder must write nothing, wrote %v", sink.calls)
	}
	var got []space.StepResult
	for sr := range ch {
		got = append(got, sr)
	}
	if len(got) != 2 || got[1].StepType != space.StepError {
		t.Fatalf("the reader must still see the failed step, got %v", got)
	}
	if rec.lastErr == nil {
		t.Fatal("the recorder's own failure must be captured for inspection")
	}
}

// The negative the acceptance rule demands: resuming from the durable high-water may only
// converge work that was ACTUALLY finished. A run that was abandoned twice gains no
// durable `done` from the engine recorder, the coordinator's own terminal append after
// lease expiry still lands, and it stays readable as failed -- so a genuinely abandoned
// run remains failed and cancellable instead of being silently rescued into success.
func TestAbandonedRunStaysFailedAfterHighWaterSeed(t *testing.T) {
	store := newStreamStore()
	loopID := "turn-abandoned"

	// Life 1: one step is recorded, then SIGKILL before the coordinator appends anything.
	if _, err := twoWriterAttempt(t, store, loopID, []space.StepResult{
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"m-before-kill"}`)},
	}, true, false); err != nil {
		t.Fatalf("life 1 (before the kill): %v", err)
	}

	// Life 2 (the re-claim): the model genuinely fails, so NO terminal step is emitted.
	rec := newStepRecorder(twoWriterSink{store: store}, loopID)
	rec.resumeFromDurable() // the re-claim resumes from the restored checkpoint
	results := make(chan space.StepResult, 4)
	rec.push(context.Background(), results, space.StepResult{
		StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"m-resumed"}`)})
	rec.push(context.Background(), results, space.StepResult{
		StepType: space.StepError, Content: json.RawMessage(`{"error":"model generate step 1: boom"}`)})
	close(results)
	if rec.lastErr != nil {
		t.Fatalf("the resumed recorder must not collide on its own keys: %v", rec.lastErr)
	}

	// Lease expires with nobody to finish the run: the coordinator writes its own verdict.
	hw, err := store.highWater(loopID)
	if err != nil {
		t.Fatalf("high-water after the abandonment: %v", err)
	}
	if _, err := store.append(space.StepKey(loopID, hw), "status",
		json.RawMessage(`{"status":"failed","reason":"lease_expired"}`)); err != nil {
		t.Fatalf("the abandoned run could not record its terminal status: %v", err)
	}

	for _, entry := range store.kinds() {
		if strings.HasSuffix(entry, "=done") {
			t.Fatalf("an abandoned run gained a durable done from the engine recorder: %v", store.kinds())
		}
	}
	got := strings.Join(store.kinds(), ",")
	want := loopID + ":0=model," + loopID + ":1=model," + loopID + ":2=error," + loopID + ":3=status"
	if got != want {
		t.Fatalf("abandoned-run stream shape wrong:\n got %s\nwant %s", got, want)
	}
}

// The Core-side half of the idempotency contract, pinned without a database: a COLD
// re-delivery of the same request re-emits the same steps in the same order, so every key
// it writes is the key the first life wrote, and the stream does not grow. Seeding from the
// durable high-water on a cold life is exactly what broke this (the re-run continued at the
// keys AFTER the first life — realstate: before=2 after=4).
func TestColdRedeliveryOfTheSameStepsDoesNotGrowStream(t *testing.T) {
	store := newStreamStore()
	loopID := "run-cold-redelivery"
	for attempt := 1; attempt <= 3; attempt++ {
		rec := newStepRecorder(twoWriterSink{store: store}, loopID)
		for _, sr := range steps() {
			rec.record(context.Background(), sr)
		}
		if rec.lastErr != nil {
			t.Fatalf("attempt %d: an identical re-delivery must dedup, got %v", attempt, rec.lastErr)
		}
		if got := len(store.kinds()); got != len(steps()) {
			t.Fatalf("attempt %d: the stream grew on an identical re-delivery: rows=%d want=%d",
				attempt, got, len(steps()))
		}
	}
}

// The negative that keeps the fix honest: when a re-delivered run produces a DIFFERENT body
// for the same logical step, that stays a KEY_REUSE_MISMATCH and stays VISIBLE. Renumbering
// the step to make the write land would erase the only signal that the step is not
// reproducible, and would drop a foreign body onto keys the coordinator still needs (the
// 99f3396 shape). Recording stops; the durable stream keeps the original bodies.
func TestColdRedeliveryWithDifferentBodyStaysKeyReuse(t *testing.T) {
	store := newStreamStore()
	loopID := "run-nondeterministic"
	first := newStepRecorder(twoWriterSink{store: store}, loopID)
	for _, sr := range steps() {
		first.record(context.Background(), sr)
	}
	rows := len(store.kinds())
	if rows != len(steps()) {
		t.Fatalf("baseline rows=%d want=%d", rows, len(steps()))
	}

	conflicting := []space.StepResult{
		{StepType: space.StepModel, Content: json.RawMessage(`{"done":false,"content":"DIFFERENT"}`)},
		{StepType: space.StepDone, Content: json.RawMessage(`null`), Done: true},
	}
	second := newStepRecorder(twoWriterSink{store: store}, loopID)
	for _, sr := range conflicting {
		second.record(context.Background(), sr)
	}
	if !errors.Is(second.lastErr, space.ErrKeyReuse) {
		t.Fatalf("a different body on the same logical key must report KEY_REUSE, got %v", second.lastErr)
	}
	if !second.blocked {
		t.Fatal("the recorder must stop at the mismatch instead of renumbering the remaining steps")
	}
	if got := len(store.kinds()); got != rows {
		t.Fatalf("a rejected re-delivery must not change the durable stream: rows=%d want=%d", got, rows)
	}
}
