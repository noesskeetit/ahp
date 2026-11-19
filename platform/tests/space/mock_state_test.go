package space_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// mockState is a thread-safe in-memory State API mock. It is intentionally a
// faithful reimplementation of the *published* State wire contract so that the
// Space half of the integration can be contract-tested end to end while the
// real State HTTP surface is still being wired by its owner. In particular it
// enforces, exactly as the State contract requires:
//
//   - checkpoints belong to a thread + namespace, are keyed by their own id,
//     are listed newest-first, and are never collapsed onto run_id;
//   - a thread's data is only ever visible to callers scoped to that thread
//     (private-context isolation);
//   - writes that carry a lease epoch different from the thread's current
//     fencing epoch are rejected with STALE_EPOCH (takeover safety);
//   - the inbox returns only unacknowledged events and AckInbox marks exactly
//     the requested ids;
//   - Enqueue is idempotent by (thread, key);
//   - KV is namespace+key scoped, not run scoped.
type mockState struct {
	mu       sync.Mutex
	threads  map[string]*threadRecord
	runs     map[string]*runRecord
	leases   map[string]*leaseRecord // runID -> active lease
	inbox    []inboxItem
	streams  []streamItem
	checkpts []checkpointRec // thread+namespace scoped, newest appended last
	intents  []intentItem
	kv       map[string]kvEntry // "ns1/ns2/key" -> entry
	nextSeq  int64

	// dropACK, when set, is asked about every AppendStream AFTER the write has been
	// committed. Answering true aborts the handler, so the connection dies with no
	// response: the caller cannot tell that from "nothing happened", which is exactly
	// the lost-ACK shape. Always called with mu held.
	dropACK func(op, key, kind string) bool

	// failAppend, when set, is asked about every AppendStream BEFORE anything is written.
	// Answering true aborts the handler with the connection unwritten, i.e. a transient
	// transport failure whose effect is NOT durable. Together with dropACK this is the
	// pair the caller cannot tell apart from outside - which is the whole point of an
	// idempotent retry. Always called with mu held.
	failAppend func(op, key, kind string) bool

	// failEnqueue, when set, is asked about every Enqueue BEFORE anything is written.
	// Answering true aborts the handler with the connection unwritten - a transient
	// transport failure whose effect is NOT durable, the same shape as failAppend. Steer
	// depends on this: a turn's status must not be claimed until the durable inbox row
	// exists, so a refused enqueue must leave the turn exactly as it was (the lane's "a
	// refusal mutates nothing" rule, as Recover was fixed to in D15).
	failEnqueue func(op, key, kind string) bool

	// failIntent / dropIntentACK are the same indistinguishable pair as
	// failAppend / dropACK, for the INTENT journal: "the journal write never
	// happened" and "the journal write is durable but its ACK was lost" must look
	// identical to the caller, or a mutation retry can never be proved honest.
	// status is the requested intent status ("" for RecordIntent/GetIntent).
	failIntent    func(op, node, call, status string) bool
	dropIntentACK func(op, node, call, status string) bool

	// autoAnswerGate, when set, is asked about every hitl_request event AFTER that event has
	// been stored. Answering with an award makes this double store the gate's answer right
	// there - before the Enqueue call that published the gate has even returned to Space - and
	// apply the same wake rule to it. That is the deterministic shape of a real responder: an
	// approver that watches the thread's durable inbox (which is where SubmitHITL publishes the
	// gate) and answers the moment it appears. Whether the park survives that answer is then
	// decided by the server's own wake rule (wakeLocked), which this double already mirrors and
	// which was measured against the real server on the wire.
	autoAnswerGate func(threadID, gateID string) *autoAnswer

	// flushFault, when true, makes every FlushStream answer a plain INTERNAL failure
	// with nothing persisted: the injected checkpoint-boundary durability failure Root
	// required to be provable at the COORDINATOR, not only as a store-level owner
	// refusal. Off by default, so the default chunk-mode tests (which never call
	// FlushStream) are completely unaffected.
	flushFault bool

	// failStatusAppend, when true, refuses the terminal "status" stream append with a
	// non-retryable INTERNAL and writes nothing, so a run that finished its work but
	// cannot commit its terminal frame must NOT be reported done (the coordinator must
	// gate the success verdict on that commit). Off by default.
	failStatusAppend bool
}

// autoAnswer is the verdict autoAnswerGate hands back: what the approver decided and the
// commentary it sent with the decision, in the same field names RespondHITL writes.
type autoAnswer struct {
	approved bool
	response json.RawMessage
}

type threadRecord struct {
	ID        string    `json:"id"`
	ContextID string    `json:"context_id"`
	Epoch     int64     `json:"epoch"`
	CreatedAt time.Time `json:"created_at"`
	// Owner is the credential that created the thread. It is double-internal bookkeeping
	// (json:"-"), never part of the published thread DTO. ListThreads filters by it so this
	// double keeps the isolation its own doc comment promises: "a thread's data is only ever
	// visible to callers scoped to that thread". Without it an unscoped per-thread read of
	// every thread (the unscoped ListTurns rebuild) silently mixes subjects.
	Owner string `json:"-"`
}

type runRecord struct {
	ID       string `json:"id"`
	ThreadID string `json:"thread_id"`
	TaskID   string `json:"task_id"`
	Status   string `json:"status"`
	// WaitFor mirrors the real State run contract (internal/state/types.go): what the run is
	// parked on, published to readers so a cold projection can tell a HITL gate from any other
	// wait instead of guessing from the word `waiting`.
	WaitFor  string          `json:"wait_for,omitempty"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

type leaseRecord struct {
	RunID  string
	Worker string
	Epoch  int64
}

type inboxItem struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
	Acked    bool            `json:"acked"`
}

type streamItem struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`

	// digest mirrors the stored digest of internal/state/events.go: double-internal
	// bookkeeping (never published) that lets appendStream apply the real idempotency
	// rule instead of pretending every append is a new event.
	digest string `json:"-"`
}

type intentItem struct {
	ID     string          `json:"id"`
	RunID  string          `json:"run_id"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Digest string          `json:"digest"`
	Status string          `json:"status"`
	Args   json.RawMessage `json:"args"`
	// Result mirrors the stored completion payload. State returns it on GetIntent
	// (and on an UpdateIntent replay), never on a RecordIntent replay.
	Result json.RawMessage `json:"result,omitempty"`
}

// checkpointRec mirrors the published State checkpoint DTO. There is no
// "sequence" on the wire; ordering is by created_at and monotonic version.
type checkpointRec struct {
	ID        string          `json:"id"`
	ThreadID  string          `json:"thread_id"`
	Namespace string          `json:"namespace"`
	Parent    string          `json:"parent,omitempty"`
	Version   int64           `json:"version"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// kvEntry mirrors the published State KVItem DTO.
type kvEntry struct {
	Namespace []string        `json:"namespace"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   int64           `json:"version"`
	ThreadID  string          `json:"thread_id,omitempty"`
	UpdatedAt time.Time       `json:"updated_at"`
}

func newMockState() *mockState {
	return &mockState{
		threads: make(map[string]*threadRecord),
		runs:    make(map[string]*runRecord),
		leases:  make(map[string]*leaseRecord),
		kv:      make(map[string]kvEntry),
	}
}

func (m *mockState) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/state/", func(w http.ResponseWriter, r *http.Request) {
		op := strings.TrimPrefix(r.URL.Path, "/v1/state/")
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeErr(w, http.StatusBadRequest, contracts.ErrInvalid, err.Error())
			return
		}
		caller := r.Header.Get("Authorization")
		switch op {
		case "CreateThread":
			m.createThread(w, body, caller)
		case "GetThread":
			m.getThread(w, body, caller)
		case "ListThreads":
			m.listThreads(w, body, caller)
		case "DeleteThread":
			m.deleteThread(w, body, caller)
		case "CreateRun":
			m.createRun(w, body)
		case "GetRun":
			m.getRun(w, body)
		case "ClaimRun":
			m.claimRun(w, body)
		case "Heartbeat":
			m.heartbeat(w, body)
		case "Release":
			m.release(w, body)
		case "Suspend":
			m.suspend(w, body)
		case "RecordIntent":
			m.recordIntent(w, body)
		case "UpdateIntent":
			m.updateIntent(w, body)
		case "GetIntent":
			m.getIntent(w, body)
		case "Enqueue":
			m.enqueue(w, body)
		case "ReadInbox":
			m.readInbox(w, body)
		case "AckInbox":
			m.ackInbox(w, body)
		case "PutCheckpoint":
			m.putCheckpoint(w, body)
		case "ListRuns":
			m.listRuns(w, body)
		case "ListCheckpoints":
			m.listCheckpoints(w, body)
		case "AppendStream":
			m.appendStream(w, body)
		case "ReadStream":
			m.readStream(w, body)
		case "FlushStream":
			m.flushStream(w, body)
		case "PutKV":
			m.putKV(w, body)
		case "GetKV":
			m.getKV(w, body)
		default:
			writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "unknown op: "+op)
		}
	})
	return mux
}

func (m *mockState) Server() *httptest.Server {
	return httptest.NewServer(m.Handler())
}

func (m *mockState) createThread(w http.ResponseWriter, body map[string]any, caller string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ctxID, _ := body["context_id"].(string)
	for _, t := range m.threads {
		if t.ContextID == ctxID {
			writeJSON(w, http.StatusOK, t)
			return
		}
	}
	id := contracts.NewID()
	t := &threadRecord{ID: id, ContextID: ctxID, Epoch: 1, CreatedAt: time.Now().UTC(), Owner: caller}
	m.threads[id] = t
	writeJSON(w, http.StatusOK, t)
}

// getThread is scope-filtered, because the real store is: Store.GetThread takes the
// caller's contracts.Scope and a foreign thread is not-found (internal/state/store.go).
// The Space edge leans on exactly this for the restart case - when its in-memory turn
// table is empty, authorization falls back to VerifyThreadOwnership/GetThread - so a
// double that answered any thread id to any bearer would let the offline suite pass a
// cross-subject stream read that the real State refuses. Found by
// TestLateSubscriberReplaysFinishedChatAfterRestart, which was RED here and GREEN on
// PostgreSQL (TestDurableThreadOwnershipRealState) until this line was fixed.
func (m *mockState) getThread(w http.ResponseWriter, body map[string]any, caller string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid, _ := body["thread_id"].(string)
	t, ok := m.threads[tid]
	if !ok || (caller != "" && t.Owner != "" && t.Owner != caller) {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "thread not found")
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// deleteThread mirrors the real control-plane op: the DELETE is filtered by the
// caller's own scope (a foreign or absent thread affects 0 rows and answers
// NOT_FOUND, never a 403 that would leak existence), and the schema's
// ON DELETE CASCADE is reproduced by hand - runs with their leases and intent
// journal, stream events, inbox events, checkpoints and KV rows all go with the
// thread. Reproducing the cascade matters for this double: the whole point of the
// Space-side rule is that a delete takes the duplicate-effect barrier with the run,
// so a double that left the rows behind could never show the difference.
func (m *mockState) deleteThread(w http.ResponseWriter, body map[string]any, caller string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid, _ := body["thread_id"].(string)
	t, ok := m.threads[tid]
	if !ok || (caller != "" && t.Owner != "" && t.Owner != caller) {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "thread not found")
		return
	}
	deletedRuns := map[string]bool{}
	for id, r := range m.runs {
		if r.ThreadID == tid {
			deletedRuns[id] = true
			delete(m.runs, id)
		}
	}
	for id := range deletedRuns {
		delete(m.leases, id)
	}
	keptIntents := m.intents[:0]
	for _, it := range m.intents {
		if deletedRuns[it.RunID] {
			continue
		}
		keptIntents = append(keptIntents, it)
	}
	m.intents = keptIntents
	keptStream := m.streams[:0]
	for _, ev := range m.streams {
		if ev.ThreadID == tid {
			continue
		}
		keptStream = append(keptStream, ev)
	}
	m.streams = keptStream
	keptInbox := m.inbox[:0]
	for _, ev := range m.inbox {
		if ev.ThreadID == tid {
			continue
		}
		keptInbox = append(keptInbox, ev)
	}
	m.inbox = keptInbox
	keptCheckpoints := m.checkpts[:0]
	for _, cp := range m.checkpts {
		if cp.ThreadID == tid {
			continue
		}
		keptCheckpoints = append(keptCheckpoints, cp)
	}
	m.checkpts = keptCheckpoints
	for key, entry := range m.kv {
		if entry.ThreadID == tid {
			delete(m.kv, key)
		}
	}
	delete(m.threads, tid)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// listThreads mirrors the accepted State contract: scope comes from the token
// (ignored by this unscoped double), ordering is newest-first by CreatedAt then
// id, and it returns an OBJECT envelope {threads, next_cursor, has_more} with
// keyset pagination by the last returned id.
func (m *mockState) listThreads(w http.ResponseWriter, body map[string]any, caller string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cursor, _ := body["cursor"].(string)
	limit := 50
	if v, ok := body["limit"].(float64); ok && int(v) > 0 {
		limit = int(v)
	}
	all := make([]*threadRecord, 0, len(m.threads))
	for _, t := range m.threads {
		// A thread created without a credential stays visible (the older tests in this
		// package seed state directly); once a caller is identified, only its own threads
		// are enumerated, which is what real State does from the task token.
		if caller != "" && t.Owner != "" && t.Owner != caller {
			continue
		}
		all = append(all, t)
	}
	// newest-first: CreatedAt desc, then id desc for stability.
	for i := 1; i < len(all); i++ {
		for j := i; j > 0; j-- {
			a, b := all[j], all[j-1]
			if a.CreatedAt.After(b.CreatedAt) || (a.CreatedAt.Equal(b.CreatedAt) && a.ID > b.ID) {
				all[j], all[j-1] = all[j-1], all[j]
			} else {
				break
			}
		}
	}
	// keyset: drop everything up to and including the cursor id.
	start := 0
	if cursor != "" {
		for i, t := range all {
			if t.ID == cursor {
				start = i + 1
				break
			}
		}
	}
	page := all[start:]
	next := ""
	if len(page) > limit {
		page = page[:limit]
		next = page[len(page)-1].ID
	}
	writeJSON(w, http.StatusOK, map[string]any{"threads": page, "next_cursor": next, "has_more": next != ""})
}

func (m *mockState) createRun(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID, _ := body["thread_id"].(string)
	taskID, _ := body["task_id"].(string)
	if taskID == "" {
		taskID = "task-" + contracts.NewID()
	}
	var payload json.RawMessage
	if p, ok := body["payload"]; ok {
		payload, _ = json.Marshal(p)
	}
	m.nextSeq++
	id := contracts.NewID()
	r := &runRecord{ID: id, ThreadID: threadID, TaskID: taskID, Status: "pending", Sequence: m.nextSeq, Payload: payload}
	m.runs[id] = r
	writeJSON(w, http.StatusOK, r)
}

func (m *mockState) getRun(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, _ := body["run_id"].(string)
	r, ok := m.runs[runID]
	if !ok {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "run not found")
		return
	}
	writeJSON(w, http.StatusOK, r)
}

func (m *mockState) listRuns(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tid, _ := body["thread_id"].(string)
	// FIFO by sequence, mirroring the real State contract (map order is unstable).
	var rs []*runRecord
	for _, r := range m.runs {
		if r.ThreadID == tid {
			rs = append(rs, r)
		}
	}
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j].Sequence < rs[j-1].Sequence; j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": rs})
}

// claimRun hands out a lease for the oldest pending run. m.runs is a map, so
// the real State FIFO contract (claim the lowest-sequence pending run) must be
// reproduced by an explicit Sequence comparison rather than map iteration,
// whose order is randomized in Go. It preserves run_id (so a retry of the same
// run keeps its identity) but bumps the thread's fencing epoch and issues a
// fresh task token, matching the State takeover contract.
func (m *mockState) claimRun(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	worker, _ := body["worker"].(string)
	var target *runRecord
	for _, r := range m.runs {
		if r.Status != "pending" {
			continue
		}
		if target == nil || r.Sequence < target.Sequence {
			target = r
		}
	}
	if target == nil {
		writeJSON(w, http.StatusOK, map[string]any{"lease": nil, "task_token": ""})
		return
	}
	target.Status = "running"
	th := m.threads[target.ThreadID]
	var epoch int64
	if th != nil {
		th.Epoch++
		epoch = th.Epoch
	} else {
		epoch = 1
	}
	m.leases[target.ID] = &leaseRecord{RunID: target.ID, Worker: worker, Epoch: epoch}
	resp := map[string]any{
		"lease": map[string]any{
			"thread":     th,
			"run":        target,
			"worker":     worker,
			"epoch":      epoch,
			"expires_at": time.Now().Add(30 * time.Second),
		},
		"task_token": "task-token-" + contracts.NewID(),
	}
	writeJSON(w, http.StatusOK, resp)
}

func (m *mockState) heartbeat(w http.ResponseWriter, body map[string]any) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "expires_at": time.Now().Add(30 * time.Second)})
}

// crashRun simulates the worker that held a run's lease dying: the lease is dropped
// and the run returns to the queue, which is exactly what a lease expiry looks like in
// real State ("a run left running by a dead worker becomes claimable the moment its
// lease expires"). The double has no wall-clock lease expiry, so a restart/orphan
// pickup test states the crash explicitly instead of sleeping past a TTL.
func (m *mockState) crashRun(runID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.leases, runID)
	if r, ok := m.runs[runID]; ok {
		r.Status = "pending"
	}
}

// release is fenced exactly like the real server: Store.Release runs the SAME
// currentLease check as every other lease operation, so a stale, foreign or already
// cleared token is refused with STALE_EPOCH and changes NOTHING. An unfenced double
// here is not a simplification - it lets a superseded worker overwrite the terminal
// status the new owner already committed, which is the single property the handoff
// tests exist to prove.
func (m *mockState) release(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, worker, epoch := fenceOf(body)
	status, _ := body["status"].(string)
	if !m.holdsLease(runID, worker, epoch) {
		writeErr(w, http.StatusConflict, "STALE_EPOCH", "stale lease epoch")
		return
	}
	if r, ok := m.runs[runID]; ok {
		r.Status = status
	}
	delete(m.leases, runID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// suspend is fenced by the same rule as release.
func (m *mockState) suspend(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, worker, epoch := fenceOf(body)
	if !m.holdsLease(runID, worker, epoch) {
		writeErr(w, http.StatusConflict, "STALE_EPOCH", "stale lease epoch")
		return
	}
	if r, ok := m.runs[runID]; ok {
		r.Status = "waiting"
		r.WaitFor, _ = body["wait_for"].(string)
	}
	delete(m.leases, runID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// holdsLease answers whether the presented lease token is the CURRENT one for this
// run - same worker, same epoch - which is the check the real State performs under
// SELECT ... FOR UPDATE before it touches anything.
func (m *mockState) holdsLease(runID, worker string, epoch int64) bool {
	l, ok := m.leases[runID]
	return ok && l != nil && l.Worker == worker && l.Epoch == epoch
}

// intentFault reports whether an intent op must die mid-flight. beforeWrite picks
// the half of the indistinguishable pair: true dies with NOTHING stored, false dies
// AFTER the row is mutated (the write is durable, the ACK is not). Always called
// with mu held.
func (m *mockState) intentFault(op, node, call, status string, beforeWrite bool) bool {
	if beforeWrite {
		return m.failIntent != nil && m.failIntent(op, node, call, status)
	}
	return m.dropIntentACK != nil && m.dropIntentACK(op, node, call, status)
}

// armFailedIntent makes the next `times` matching intent writes die BEFORE anything is
// stored; armDroppedIntentACK makes them die AFTER the row is mutated. `times` < 0 is
// permanent, i.e. the plane is genuinely unreachable rather than blipping.
func (m *mockState) armFailedIntent(times int, match func(op, node, call, status string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failIntent = func(op, node, call, status string) bool {
		if times == 0 || !match(op, node, call, status) {
			return false
		}
		if times > 0 {
			times--
		}
		return true
	}
}

func (m *mockState) armDroppedIntentACK(times int, match func(op, node, call, status string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropIntentACK = func(op, node, call, status string) bool {
		if times == 0 || !match(op, node, call, status) {
			return false
		}
		if times > 0 {
			times--
		}
		return true
	}
}

func (m *mockState) disarmIntentFaults() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failIntent = nil
	m.dropIntentACK = nil
}

// mockIntentDigest mirrors internal/state/intents.go RecordIntent: the fingerprint
// covers exactly {kind,target,args}, canonicalised. The double must refuse a key
// reuse on the SAME bytes the real plane does, otherwise a mutation replay proves
// nothing about the real rule.
func mockIntentDigest(kind, target string, args json.RawMessage) string {
	d, _ := contracts.Digest(struct {
		Kind   string          `json:"kind"`
		Target string          `json:"target"`
		Args   json.RawMessage `json:"args"`
	}{Kind: kind, Target: target, Args: args})
	return d
}

// mockIntentTransition mirrors the published lifecycle: recorded -> dispatched ->
// terminal, recorded/dispatched -> fenced, and unknown -> fenced ONLY. An unknown
// intent never completes through this path.
func mockIntentTransition(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case "recorded":
		return to == "dispatched" || to == "fenced"
	case "dispatched":
		return to == "succeeded" || to == "failed" || to == "unknown" || to == "fenced"
	case "unknown":
		return to == "fenced"
	default:
		return false
	}
}

func mockSameJSON(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	ca, ea := contracts.Digest(a)
	cb, eb := contracts.Digest(b)
	if ea != nil || eb != nil {
		return string(a) == string(b)
	}
	return ca == cb
}

// recordIntent mirrors Store.RecordIntent: the (run,node,call) key is idempotent by
// fingerprint, the row is written under the run's LIVE lease (a stale or foreign or
// already-cleared fence is refused with STALE_EPOCH and stores nothing), and a replay
// of the identical fingerprint returns the STORED row with its current status -- and,
// exactly as State does, WITHOUT the stored result, which only GetIntent publishes.
func (m *mockState) recordIntent(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, _ := body["node"].(string)
	call, _ := body["call"].(string)
	if m.intentFault("RecordIntent", node, call, "", true) {
		panic(http.ErrAbortHandler)
	}
	runID, worker, epoch := fenceOf(body)
	kind, _ := body["kind"].(string)
	target, _ := body["target"].(string)
	if node == "" || call == "" || kind == "" {
		writeErr(w, http.StatusBadRequest, contracts.ErrInvalid, "intent node, call and kind are required")
		return
	}
	if !m.holdsLease(runID, worker, epoch) {
		writeErr(w, http.StatusConflict, "STALE_EPOCH", "stale lease epoch")
		return
	}
	var args json.RawMessage
	if a, ok := body["args"]; ok && a != nil {
		var err error
		args, err = json.Marshal(a)
		if err != nil {
			writeErr(w, http.StatusBadRequest, contracts.ErrInvalid, "intent args must be valid JSON")
			return
		}
	}
	digest := mockIntentDigest(kind, target, args)
	for i := range m.intents {
		stored := m.intents[i]
		if stored.RunID != runID || stored.Node != node || stored.Call != call {
			continue
		}
		if stored.Digest != digest {
			writeErr(w, http.StatusConflict, contracts.ErrKeyReuse, "intent key reused with a different fingerprint")
			return
		}
		replay := stored
		replay.Result = nil
		if m.intentFault("RecordIntent", node, call, "", false) {
			panic(http.ErrAbortHandler)
		}
		writeJSON(w, http.StatusOK, replay)
		return
	}
	item := intentItem{ID: contracts.NewID(), RunID: runID, Node: node, Call: call,
		Kind: kind, Target: target, Digest: digest, Status: "recorded", Args: args}
	m.intents = append(m.intents, item)
	if m.intentFault("RecordIntent", node, call, "", false) {
		panic(http.ErrAbortHandler)
	}
	writeJSON(w, http.StatusOK, item)
}

// updateIntent mirrors Store.UpdateIntent under the same live-lease fence: monotonic
// transitions, an unknown intent that only the privileged ReconcileIntent may move,
// and a completion replayable only with the byte-identical result.
func (m *mockState) updateIntent(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	node, _ := body["node"].(string)
	call, _ := body["call"].(string)
	status, _ := body["status"].(string)
	if m.intentFault("UpdateIntent", node, call, status, true) {
		panic(http.ErrAbortHandler)
	}
	runID, worker, epoch := fenceOf(body)
	switch status {
	case "recorded", "dispatched", "succeeded", "failed", "unknown", "fenced":
	default:
		writeErr(w, http.StatusBadRequest, contracts.ErrInvalid, "unknown intent status")
		return
	}
	var result json.RawMessage
	if r, ok := body["result"]; ok && r != nil {
		var err error
		result, err = json.Marshal(r)
		if err != nil {
			writeErr(w, http.StatusBadRequest, contracts.ErrInvalid, "intent result must be valid JSON")
			return
		}
	}
	if !m.holdsLease(runID, worker, epoch) {
		writeErr(w, http.StatusConflict, "STALE_EPOCH", "stale lease epoch")
		return
	}
	idx := -1
	for i := range m.intents {
		if m.intents[i].RunID == runID && m.intents[i].Node == node && m.intents[i].Call == call {
			idx = i
			break
		}
	}
	if idx < 0 {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "intent not found")
		return
	}
	it := m.intents[idx]
	if !mockIntentTransition(it.Status, status) {
		if it.Status == "unknown" {
			writeErr(w, http.StatusConflict, contracts.ErrConflict,
				"unknown intents reconcile only through the privileged ReconcileIntent")
			return
		}
		writeErr(w, http.StatusConflict, contracts.ErrConflict,
			"illegal intent transition "+it.Status+" -> "+status)
		return
	}
	if it.Status == "succeeded" || it.Status == "failed" {
		if !mockSameJSON(it.Result, result) {
			writeErr(w, http.StatusConflict, contracts.ErrConflict,
				"intent already completed with a different result")
			return
		}
		writeJSON(w, http.StatusOK, it)
		return
	}
	it.Status = status
	if len(result) > 0 {
		it.Result = result
	}
	m.intents[idx] = it
	if m.intentFault("UpdateIntent", node, call, status, false) {
		panic(http.ErrAbortHandler)
	}
	writeJSON(w, http.StatusOK, it)
}

// getIntent mirrors Store.GetIntent: a scope read with NO fence, publishing the
// stored result as well.
func (m *mockState) getIntent(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, _ := body["run_id"].(string)
	node, _ := body["node"].(string)
	call, _ := body["call"].(string)
	for _, it := range m.intents {
		if it.RunID == runID && it.Node == node && it.Call == call {
			writeJSON(w, http.StatusOK, it)
			return
		}
	}
	writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "intent not found")
}

// enqueue appends an inbox event. No lease fence: it is an append keyed by
// (thread, key) and applied idempotently.
func (m *mockState) enqueue(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID, _ := body["thread_id"].(string)
	key, _ := body["key"].(string)
	kind, _ := body["kind"].(string)
	var payload json.RawMessage
	if p, ok := body["payload"]; ok {
		payload, _ = json.Marshal(p)
	}
	if m.failEnqueue != nil && m.failEnqueue("Enqueue", key, kind) {
		panic(http.ErrAbortHandler)
	}
	for _, it := range m.inbox {
		if it.ThreadID == threadID && it.Key == key {
			// Real State calls an existing key idempotent ONLY for the same {kind,payload} digest and
			// refuses a different body under one key (internal/state/events.go ErrKeyReuse). The double
			// used to answer 200 with the stored event either way, which made a second, DIFFERENT decision
			// under the gate's key look like a decision that had been recorded - a double that is laxer
			// than the server is how a false success survives review.
			//
			// The answer has to be the one the server actually gives, not a more helpful one: State's
			// verdict is KEY_REUSE_MISMATCH, which transport.WriteError flattens to HTTP 400 carrying
			// code INVALID_ARGUMENT, the domain code left in the message text and in a `reason` field
			// Space's envelope never reads (internal/space/stateclient.go decodes code and message only).
			// So the message below is the server's own error text, built the same way, and a double that
			// answered 409 CONFLICT here would hand Space an informative refusal the real server withholds.
			if !sameInboxDigest(it.Kind, it.Payload, kind, payload) {
				reuse := contracts.NewError(contracts.ErrKeyReuse, "key reused with a different body")
				writeErr(w, http.StatusBadRequest, contracts.ErrInvalid, reuse.Error())
				return
			}
			// And the wake below is skipped on State's dedup path, so it is skipped here too.
			writeJSON(w, http.StatusOK, it)
			return
		}
	}
	m.nextSeq++
	item := inboxItem{ID: contracts.NewID(), ThreadID: threadID, Key: key, Kind: kind, Sequence: m.nextSeq, Payload: payload}
	m.inbox = append(m.inbox, item)
	// "Событие в inbox будит запуск" is part of the published contract, not an extra: State wakes the
	// thread's FIRST non-terminal run when an event's kind is what that run is parked on
	// (status=waiting && (wait_for == kind || wait_for == "*")). The double ignored the wake, so a HITL
	// answer looked inert here while the real server released the run - a double that is STRICTER than
	// the contract hides the mechanism the product depends on, which is the failure direction this
	// mirror exists to close.
	m.wakeLocked(threadID, kind)
	if kind == "hitl_request" && m.autoAnswerGate != nil {
		// The gate is durable from the line above, so a watcher of the thread's inbox can answer
		// it NOW - while Space is still inside SubmitHITL and has not parked the run yet. The wake
		// then finds a run that is not waiting, exactly as on the real server, and the answer sits
		// in the inbox unacknowledged with nothing left that would ever deliver it.
		gateID := strings.TrimPrefix(key, "hitl:")
		if ans := m.autoAnswerGate(threadID, gateID); ans != nil {
			m.insertGateAnswerLocked(threadID, gateID, ans.approved, ans.response)
		}
	}
	writeJSON(w, http.StatusOK, item)
}

// insertGateAnswerLocked stores one gate's answer the way RespondHITL stores it - key
// "hitl_response:<id>", kind hitl_response, payload {hitl_id, approved, response} - and applies the
// wake rule to it. Those names are the ones the product writes, and they are pinned from the other
// side by the gate's own reader (GetHITL), so a drift here cannot stay green; the answer goes in
// through the same shape an HTTP Enqueue would take, not through a private shape of this double's
// own - which is precisely the class of bug that killed the SDK's HITL path on the live server.
func (m *mockState) insertGateAnswerLocked(threadID, gateID string, approved bool, response json.RawMessage) {
	payload, _ := json.Marshal(map[string]any{"hitl_id": gateID, "approved": approved, "response": response})
	m.nextSeq++
	m.inbox = append(m.inbox, inboxItem{
		ID: contracts.NewID(), ThreadID: threadID, Key: "hitl_response:" + gateID,
		Kind: "hitl_response", Sequence: m.nextSeq, Payload: payload,
	})
	m.wakeLocked(threadID, "hitl_response")
}

// sameInboxDigest mirrors the digest State compares an existing inbox key on - the canonical digest
// of {kind, payload} (internal/state/events.go Enqueue) - so "same decision" in this double means the
// same thing it means on the server, down to the kind field being part of the identity.
func sameInboxDigest(storedKind string, stored json.RawMessage, kind string, payload json.RawMessage) bool {
	a, err := contracts.Digest(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: storedKind, Payload: stored})
	if err != nil {
		return false
	}
	b, err := contracts.Digest(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: kind, Payload: payload})
	if err != nil {
		return false
	}
	return a == b
}

// wakeLocked mirrors State's wake rule exactly, including the part that matters for an approval: the
// wake matches on the EVENT KIND against the run's own wait_for, so an event naming no gate still
// releases a run parked on a real gate as long as it carries the kind that gate waits for. That is a
// property of the published contract and is pinned as such in hitl_gate_answer_test.go.
func (m *mockState) wakeLocked(threadID, kind string) {
	var first *runRecord
	for _, r := range m.runs {
		if r.ThreadID != threadID {
			continue
		}
		switch r.Status {
		case "pending", "running", "waiting":
		default:
			continue
		}
		if first == nil || r.Sequence < first.Sequence {
			first = r
		}
	}
	if first == nil || first.Status != "waiting" {
		return
	}
	if first.WaitFor != kind && first.WaitFor != "*" {
		return
	}
	first.Status = "pending"
	first.WaitFor = ""
}

func (m *mockState) readInbox(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID, _ := body["thread_id"].(string)
	after, _ := body["after"].(float64)
	var out []inboxItem
	for _, item := range m.inbox {
		if item.ThreadID == threadID && !item.Acked && int64(after) < item.Sequence {
			out = append(out, item)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

func (m *mockState) ackInbox(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idsRaw, _ := body["ids"].([]any)
	want := make(map[string]bool, len(idsRaw))
	for _, v := range idsRaw {
		if s, ok := v.(string); ok {
			want[s] = true
		}
	}
	for i := range m.inbox {
		if want[m.inbox[i].ID] {
			m.inbox[i].Acked = true
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// putCheckpoint stores a thread+namespace scoped checkpoint, fenced by the run
// lease. The thread is derived from run_id, never from the body. A stale epoch
// (post-takeover writer) is rejected with STALE_EPOCH; re-submitting the same
// (thread, namespace, id) is idempotent.
func (m *mockState) putCheckpoint(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, _, epoch := fenceOf(body)
	run, ok := m.runs[runID]
	if !ok {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "run not found")
		return
	}
	th := m.threads[run.ThreadID]
	if th == nil {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "thread not found")
		return
	}
	if epoch != th.Epoch {
		writeErr(w, http.StatusConflict, "STALE_EPOCH", "stale lease epoch")
		return
	}
	cpb := body
	if c2, ok := body["checkpoint"].(map[string]any); ok {
		cpb = c2
	}
	ns, _ := cpb["namespace"].(string)
	id, _ := cpb["id"].(string)
	parent, _ := cpb["parent"].(string)
	version := int64(num(cpb["version"]))
	var metadata json.RawMessage
	if md, ok := cpb["metadata"]; ok {
		metadata, _ = json.Marshal(md)
	}
	for i := range m.checkpts {
		cp := &m.checkpts[i]
		if cp.ThreadID == run.ThreadID && cp.Namespace == ns && cp.ID == id {
			writeJSON(w, http.StatusOK, cp)
			return
		}
	}
	rec := checkpointRec{ID: id, ThreadID: run.ThreadID, Namespace: ns, Parent: parent, Version: version, Metadata: metadata, CreatedAt: time.Now().UTC()}
	m.checkpts = append(m.checkpts, rec)
	writeJSON(w, http.StatusOK, rec)
}

// listCheckpoints returns the newest-first checkpoints for a thread within a
// namespace. The thread scope is enforced first: a caller scoped to thread B
// never sees thread A's checkpoints.
func (m *mockState) listCheckpoints(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID, _ := body["thread_id"].(string)
	ns, _ := body["namespace"].(string)
	limit := int(num(body["limit"]))
	if limit <= 0 {
		limit = 10
	}
	var out []checkpointRec
	for i := len(m.checkpts) - 1; i >= 0; i-- {
		cp := m.checkpts[i]
		if cp.ThreadID == threadID && (ns == "" || cp.Namespace == ns) {
			out = append(out, cp)
			if len(out) >= limit {
				break
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"checkpoints": out})
}

func (m *mockState) appendStream(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, worker, epoch := fenceOf(body)
	run, ok := m.runs[runID]
	if !ok {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "run not found")
		return
	}
	th := m.threads[run.ThreadID]
	// Real State fences EVERY stream write with the same currentLease check that guards
	// Release: the run must still hold the thread lease at exactly this worker/epoch. Once
	// the run is closed (Release) or taken over, the lease is gone and the write is refused
	// with STALE_EPOCH, changing nothing. An unfenced double hides the whole class of
	// "a run that already ended cannot write its own terminal frame" defects (D4).
	if th == nil || epoch != th.Epoch || !m.holdsLease(runID, worker, epoch) {
		writeErr(w, http.StatusConflict, "STALE_EPOCH", "stale lease epoch")
		return
	}
	key, _ := body["key"].(string)
	kind, _ := body["kind"].(string)
	var payload json.RawMessage
	if p, ok := body["payload"]; ok {
		payload, _ = json.Marshal(p)
	}
	// Real State (internal/state/events.go AppendStream) makes an append IDEMPOTENT by
	// (key, kind, payload) digest: the same write twice returns the STORED event and
	// creates no new row, while a different body under an existing key fails with
	// KEY_REUSE_MISMATCH. A double without this rule makes every retry look correct while
	// the real plane would duplicate or refuse it - so a lost-ACK retry could never be
	// proved here.
	digest, derr := contracts.Digest(struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}{Kind: kind, Payload: payload})
	if derr != nil {
		writeErr(w, http.StatusBadRequest, contracts.ErrInvalid, derr.Error())
		return
	}
	// The "nothing was written" half of the indistinguishable pair: die before mutating.
	if m.failAppend != nil && m.failAppend("AppendStream", key, kind) {
		panic(http.ErrAbortHandler)
	}
	// A checkpoint-boundary experiment run finished its work but must not be reported
	// done unless its terminal "status" frame is durable. Refuse it here, writing
	// nothing, so the coordinator's success gate is the only thing that can be blamed.
	if m.failStatusAppend && kind == "status" {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "injected terminal status append fault")
		return
	}
	for _, existing := range m.streams {
		if existing.ThreadID != run.ThreadID || existing.Key != key {
			continue
		}
		if existing.digest != digest {
			writeErr(w, http.StatusConflict, contracts.ErrKeyReuse, "stream key reused with a different body")
			return
		}
		if m.lostACK("AppendStream", key, kind) {
			panic(http.ErrAbortHandler)
		}
		writeJSON(w, http.StatusOK, existing)
		return
	}
	m.nextSeq++
	item := streamItem{ID: contracts.NewID(), ThreadID: run.ThreadID, Key: key, Kind: kind, Sequence: m.nextSeq, Payload: payload}
	item.digest = digest
	m.streams = append(m.streams, item)
	if m.lostACK("AppendStream", key, kind) {
		// The write IS durable; the ACK is not. Committed-then-silent is the only honest
		// way a double can express it.
		panic(http.ErrAbortHandler)
	}
	writeJSON(w, http.StatusOK, item)
}

// lostACK reports whether this append must lose its ACK, consuming one armed drop.
func (m *mockState) lostACK(op, key, kind string) bool {
	if m.dropACK == nil {
		return false
	}
	return m.dropACK(op, key, kind)
}

// armDroppedACK makes the next `times` appends that match commit their write and then
// die without a response. Tests arm it for one frame - typically the terminal one - to
// prove the caller retries an idempotent write instead of declaring it not durable.
func (m *mockState) armDroppedACK(times int, match func(key, kind string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropACK = func(op, key, kind string) bool {
		if times <= 0 || !match(key, kind) {
			return false
		}
		times--
		return true
	}
}

// armFailedAppend makes the next `times` matching appends die BEFORE anything is
// written. The caller sees the same transport error armDroppedACK produces, so a test
// that only passes with both arms proves the writer retried an idempotent operation
// rather than got lucky.
func (m *mockState) armFailedAppend(times int, match func(key, kind string) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failAppend = func(op, key, kind string) bool {
		if times <= 0 || !match(key, kind) {
			return false
		}
		times--
		return true
	}
}

// disarmDroppedACK stops every injected AppendStream failure.
func (m *mockState) disarmDroppedACK() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dropACK = nil
	m.failAppend = nil
}

// flushStream answers the experimental checkpoint-boundary FlushStream. With no
// fault armed it persists nothing (this double has no volatile tail) and answers an
// empty event list -- the shape of a successful boundary flush. When flushFault is
// armed it answers a plain INTERNAL with nothing written: the injected durability
// failure the coordinator must NOT mistake for a committed terminal.
func (m *mockState) flushStream(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.flushFault {
		writeErr(w, http.StatusInternalServerError, "INTERNAL", "injected flush fault")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": []any{}})
}

// armFlushFault makes every subsequent FlushStream fail; used only by the
// checkpoint-boundary coordinator tests.
func (m *mockState) armFlushFault() { m.mu.Lock(); m.flushFault = true; m.mu.Unlock() }

// armStatusAppendFault makes the terminal "status" stream append fail (writing
// nothing), so a finished run whose terminal frame cannot commit must not be done.
func (m *mockState) armStatusAppendFault() { m.mu.Lock(); m.failStatusAppend = true; m.mu.Unlock() }

func (m *mockState) readStream(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID, _ := body["thread_id"].(string)
	after, _ := body["after"].(float64)
	var out []streamItem
	for _, item := range m.streams {
		if item.ThreadID == threadID && int64(after) < item.Sequence {
			out = append(out, item)
		}
	}
	if _, bounded := body["max_response_bytes"]; bounded {
		limit, _ := body["limit"].(float64)
		hasMore := limit > 0 && len(out) > int(limit)
		if hasMore {
			out = out[:int(limit)]
		}
		nextAfter := int64(after)
		if len(out) > 0 {
			nextAfter = out[len(out)-1].Sequence
		}
		writeJSON(w, http.StatusOK, map[string]any{"events": out, "has_more": hasMore, "next_after": nextAfter})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

// durableStatusOfThread states what the durable State plane says about a thread: the
// status of its LAST run (by sequence) and how many frames its stream carries. Tests use
// it to prove a durable fact ("this thread is durably canceled with zero frames") instead
// of trusting Space's in-memory view.
func (m *mockState) durableStatusOfThread(threadID string) (string, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	status := ""
	var best int64 = -1
	for _, r := range m.runs {
		if r.ThreadID == threadID && r.Sequence > best {
			best, status = r.Sequence, r.Status
		}
	}
	frames := 0
	for _, s := range m.streams {
		if s.ThreadID == threadID {
			frames++
		}
	}
	return status, frames
}

// seedClosedThread states a durable fact directly: a thread with exactly one run in the
// given (terminal) status and NO stream events. That is the shape of a chat that was
// canceled BEFORE anything ever wrote a terminal frame - the two threads named in the D4
// evidence. No amount of fixing the cancel path rewrites those rows, so they are how a
// test proves the SSE tail itself terminates a finished thread.
func (m *mockState) seedClosedThread(contextID, callerBearer, runStatus string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	threadID := contracts.NewID()
	m.threads[threadID] = &threadRecord{
		ID: threadID, ContextID: contextID, Epoch: 1,
		CreatedAt: time.Now().UTC(), Owner: callerBearer,
	}
	m.nextSeq++
	runID := contracts.NewID()
	m.runs[runID] = &runRecord{
		ID: runID, ThreadID: threadID, TaskID: "task-" + runID,
		Status: runStatus, Sequence: m.nextSeq,
	}
	return threadID
}

func (m *mockState) putKV(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, _, epoch := fenceOf(body)
	run, ok := m.runs[runID]
	if !ok {
		writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "run not found")
		return
	}
	th := m.threads[run.ThreadID]
	if th == nil || epoch != th.Epoch {
		writeErr(w, http.StatusConflict, "STALE_EPOCH", "stale lease epoch")
		return
	}
	ns := nsList(body["namespace"])
	key, _ := body["key"].(string)
	var value json.RawMessage
	if v, ok := body["value"]; ok {
		value, _ = json.Marshal(v)
	}
	version := int64(num(body["expected_version"])) + 1
	item := kvEntry{Namespace: ns, Key: key, Value: value, Version: version, ThreadID: run.ThreadID, UpdatedAt: time.Now().UTC()}
	m.kv[nsJoin(ns)+"/"+key] = item
	writeJSON(w, http.StatusOK, item)
}

func (m *mockState) getKV(w http.ResponseWriter, body map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ns := nsList(body["namespace"])
	key, _ := body["key"].(string)
	if it, ok := m.kv[nsJoin(ns)+"/"+key]; ok {
		writeJSON(w, http.StatusOK, it)
		return
	}
	writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "kv not found")
}

// fenceOf extracts the nested {"fence":{run_id,worker,epoch}} issued by the
// production StateClient for fenced write operations, falling back to flat
// fields for robustness. It mirrors the published State wire contract where
// RecordIntent/UpdateIntent/PutCheckpoint/PutKV/AckInbox/AppendStream carry a
// nested fence (Heartbeat/Release/Suspend keep flat fences).
func fenceOf(body map[string]any) (runID, worker string, epoch int64) {
	if f, ok := body["fence"].(map[string]any); ok {
		runID, _ = f["run_id"].(string)
		worker, _ = f["worker"].(string)
		epoch = int64(num(f["epoch"]))
		return
	}
	runID, _ = body["run_id"].(string)
	worker, _ = body["worker"].(string)
	epoch = int64(num(body["epoch"]))
	return
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{"code": code, "message": msg})
}

func num(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

func nsList(v any) []string {
	raw, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, e := range raw {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func nsJoin(ns []string) string {
	return strings.Join(ns, "/")
}
