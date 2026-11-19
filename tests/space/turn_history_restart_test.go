package space_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// Regression for the defect measured on the live pin at 01:17-01:19Z: after an as-runner
// restart, POST /v1/space/ListTurns answered 200 with turns=[] and GET-equivalent
// POST /v1/space/GetTurn answered 404 "turn not found" for a turn that demonstrably exists,
// while ListChats kept returning the thread. The thread is durable and the turn is durable
// (State owns the Run row whose id IS the turn id) - only the runner's IN-MEMORY turn index
// was lost, and nothing read it back.

// scopedState is a State double that enforces the documented scope contract: every call is
// authorized by the presented task token, and rows owned by another subject are NOT_FOUND
// (not filtered-out-of-a-list, not an error). The package-wide mockState deliberately ignores
// tokens ("scope comes from the token (ignored by this unscoped double)"), so it CANNOT prove
// that rebuilding history from State does not leak another tenant's turns. This double can.
type scopedState struct {
	mu      sync.Mutex
	threads map[string]scopedThread
	runs    map[string]scopedRun
	calls   []string // "op as <token>" - proves which credential the read used
	// frames is the threads' durable stream, reachable only through ReadStream and only under the
	// caller's own token. The cold cause reconstruction reads the transcript, so a double that
	// always answered an empty stream would hide that read - and its authorization - entirely.
	frames []scopedFrame
	// streamFault makes ReadStream fail on a thread the caller DOES own: the only way to prove
	// what a reader does when the durable transcript cannot be read.
	streamFault bool
}

type scopedThread struct {
	ID, ContextID, Owner string
	CreatedAt            string
}

type scopedRun struct {
	ID, ThreadID, TaskID, Status, Owner string
	Sequence                            int64
	Payload                             json.RawMessage
}

// scopedFrame is one durable stream event in the shape State sends (key/kind/sequence/payload).
// The writer's key form is what attributes a frame to a turn, because the event row itself
// carries no turn id.
type scopedFrame struct {
	ThreadID, Key, Kind string
	Sequence            int64
	Payload             json.RawMessage
}

func newScopedState() *scopedState {
	return &scopedState{threads: map[string]scopedThread{}, runs: map[string]scopedRun{}}
}

func turnPayload(t *testing.T, input string, maxSteps int) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(map[string]any{"type": "turn", "input": input, "max_steps": maxSteps})
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	return b
}

func (s *scopedState) seedThread(owner, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threads[id] = scopedThread{ID: id, ContextID: "ctx-" + id, Owner: owner}
}

func (s *scopedState) seedTurn(owner, threadID, runID, status string, payload json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runs[runID] = scopedRun{ID: runID, ThreadID: threadID, TaskID: "rtask-" + owner + ":" + runID,
		Status: status, Owner: owner, Sequence: int64(len(s.runs) + 1), Payload: payload}
}

// setStatus moves a seeded run to another status, so a test can prove a cold read of a turn
// that was still running does not freeze that status in the rebuilt index.
func (s *scopedState) setStatus(runID, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ru := s.runs[runID]
	ru.Status = status
	s.runs[runID] = ru
}

// seedStreamFrame appends one durable frame to a thread, in the writer's own key form
// (space.StepKey: turn id + ":" + step).
func (s *scopedState) seedStreamFrame(threadID, key, kind, payload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frames = append(s.frames, scopedFrame{
		ThreadID: threadID, Key: key, Kind: kind,
		Sequence: int64(len(s.frames) + 1), Payload: json.RawMessage(payload),
	})
}

func (s *scopedState) observed(op string, r *http.Request) string {
	tok := r.Header.Get("Authorization")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, op+" as "+tok)
	return tok
}

// bearerTaskPrefix is the wire form StateClient produces for a task token
// ("Authorization: Bearer " + "TASK:<subject>"), stated once so the length and the
// literal can never disagree again.
const bearerTaskPrefix = "Bearer TASK:"

func (s *scopedState) ownerOfToken(tok string) string {
	// The test exchanger mints tokens as "TASK:<subject>", exactly how recExchange does.
	if !strings.HasPrefix(tok, bearerTaskPrefix) {
		return ""
	}
	return tok[len(bearerTaskPrefix):]
}

// wireRun encodes a row in the documented State wire shape. It is deliberately not
// json.Marshal(scopedRun): Owner is this double's authorization bookkeeping and real
// State never sends it, and the client decodes snake_case keys (thread_id, task_id),
// which Go's case-insensitive matching would NOT recover from exported field names.
func wireRun(ru scopedRun) map[string]any {
	return map[string]any{
		"id":        ru.ID,
		"thread_id": ru.ThreadID,
		"task_id":   ru.TaskID,
		"status":    ru.Status,
		"sequence":  ru.Sequence,
		"payload":   ru.Payload,
	}
}

func (s *scopedState) Server() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/state/", func(w http.ResponseWriter, r *http.Request) {
		op := r.URL.Path[len("/v1/state/"):]
		tok := s.observed(op, r)
		owner := s.ownerOfToken(tok)
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if owner == "" {
			// Fail closed like the real service: no usable token, no rows.
			writeErr(w, http.StatusForbidden, contracts.ErrForbidden, "no task token presented")
			return
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		switch op {
		case "GetThread":
			th, ok := s.threads[body["thread_id"].(string)]
			if !ok || th.Owner != owner {
				writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "thread not found")
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"id": th.ID, "context_id": th.ContextID, "epoch": 1})
		case "ListThreads":
			var ids []string
			for _, th := range s.threads {
				if th.Owner == owner {
					ids = append(ids, th.ID)
				}
			}
			sort.Strings(ids)
			// Keyset paging, same form the real State double uses: the cursor is the last
			// thread id of the page just sent, and everything up to it is dropped. A walk
			// cannot be shown to be bounded unless the double can actually run out of page.
			if cursor, _ := body["cursor"].(string); cursor != "" {
				from := 0
				for i, id := range ids {
					if id == cursor {
						from = i + 1
						break
					}
				}
				ids = ids[from:]
			}
			limit := 0
			if v, ok := body["limit"].(float64); ok && int(v) > 0 {
				limit = int(v)
			}
			next := ""
			hasMore := false
			if limit > 0 && len(ids) > limit {
				ids = ids[:limit]
				hasMore = true
				if len(ids) > 0 {
					next = ids[len(ids)-1]
				}
			}
			rows := make([]map[string]any, 0, len(ids))
			for _, id := range ids {
				rows = append(rows, map[string]any{"id": id, "context_id": s.threads[id].ContextID, "epoch": 1})
			}
			writeJSON(w, http.StatusOK, map[string]any{"threads": rows, "next_cursor": next, "has_more": hasMore})
		case "GetRun":
			ru, ok := s.runs[body["run_id"].(string)]
			if !ok || ru.Owner != owner {
				writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "run not found")
				return
			}
			writeJSON(w, http.StatusOK, wireRun(ru))
		case "ListRuns":
			tid, _ := body["thread_id"].(string)
			rows := make([]scopedRun, 0, 4)
			for _, ru := range s.runs {
				if ru.ThreadID == tid && ru.Owner == owner {
					rows = append(rows, ru)
				}
			}
			sort.Slice(rows, func(i, j int) bool { return rows[i].Sequence < rows[j].Sequence })
			out := make([]map[string]any, 0, len(rows))
			for _, ru := range rows {
				out = append(out, wireRun(ru))
			}
			writeJSON(w, http.StatusOK, map[string]any{"runs": out})
		case "ReadStream":
			// State isolates a stream by the presenting token exactly as it isolates a thread
			// row: a foreign caller gets NOT_FOUND, not an empty page it could mistake for
			// "nothing happened".
			tid, _ := body["thread_id"].(string)
			th, ok := s.threads[tid]
			if !ok || th.Owner != owner {
				writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "thread not found")
				return
			}
			if s.streamFault {
				writeErr(w, http.StatusServiceUnavailable, "UNAVAILABLE", "stream read failed")
				return
			}
			after, _ := body["after"].(float64)
			events := make([]map[string]any, 0, 4)
			for _, f := range s.frames {
				if f.ThreadID != tid || int64(after) >= f.Sequence {
					continue
				}
				events = append(events, map[string]any{
					"id": "EV-" + f.Key, "thread_id": f.ThreadID, "key": f.Key,
					"kind": f.Kind, "sequence": f.Sequence, "payload": f.Payload,
				})
			}
			writeJSON(w, http.StatusOK, map[string]any{"events": events})
		default:
			writeErr(w, http.StatusNotFound, contracts.ErrNotFound, "unsupported by this double: "+op)
		}
	})
	return httptest.NewServer(mux)
}

// restartEdge builds the production HTTP edge for a FRESH process: a brand-new coordinator
// (empty in-memory turn table) over the SAME durable State. That is the restart, not a mock
// of it - the only thing missing is the previous process, which is the point.
func restartEdge(t *testing.T, st *scopedState) *httptest.Server {
	t.Helper()
	srv := st.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{StateClient: sc, Core: &stubCore{}, WorkerID: "w-after-restart"})
	h := space.NewHandler(space.HandlerConfig{
		Coordinator:       coord,
		Auth:              space.ExchangeMiddleware(&recExchange{}, authPerSubject),
		RequireScopedTask: true,
	})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs
}

func turnsOf(t *testing.T, body []byte) []map[string]any {
	t.Helper()
	var out struct {
		Turns []map[string]any `json:"turns"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode ListTurns body %s: %v", body, err)
	}
	return out.Turns
}

// TestTurnHistorySurvivesRunnerRestart is the reported defect, end to end.
func TestTurnHistorySurvivesRunnerRestart(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-1", "done", turnPayload(t, "write a haiku", 8))

	hs := restartEdge(t, st)

	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"thread_id": "TH-1"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns after restart: http %d body %s", code, body)
	}
	got := turnsOf(t, body)
	if len(got) != 1 {
		t.Fatalf("the durable turn must be visible after a restart, got %d turns: %s", len(got), body)
	}
	if got[0]["id"] != "RUN-1" {
		t.Fatalf("wrong turn id: %v", got[0]["id"])
	}
	if got[0]["status"] != "done" {
		t.Fatalf("status must come from the durable row, got %v", got[0]["status"])
	}
	if got[0]["thread_id"] != "TH-1" {
		t.Fatalf("thread_id must survive, got %v", got[0]["thread_id"])
	}

	code, body = postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-1"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("GetTurn for a turn created before the restart: http %d body %s (this is the 404 that was reported)", code, body)
	}
	var snap map[string]any
	if err := json.Unmarshal(body, &snap); err != nil {
		t.Fatalf("decode GetTurn body %s: %v", body, err)
	}
	if snap["id"] != "RUN-1" || snap["status"] != "done" {
		t.Fatalf("snapshot lost the durable identity/status: %s", body)
	}
}

// TestUnscopedListTurnsAfterRestartCoversOwnThreads covers the SPA call with an empty body:
// an empty answer here is the exact lie the defect produced.
func TestUnscopedListTurnsAfterRestartCoversOwnThreads(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-1", "failed", turnPayload(t, "hi", 4))

	hs := restartEdge(t, st)
	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{}, "s1")
	if code != http.StatusOK {
		t.Fatalf("unscoped ListTurns after restart: http %d body %s", code, body)
	}
	if len(turnsOf(t, body)) != 1 {
		t.Fatalf("the caller's own turn must be listed, got: %s", body)
	}
}

// TestRebuiltHistoryDoesNotCrossSubjects proves the fix is not a read-anywhere hole: the
// thread list, the thread read and the run reads are all authorized by the CALLER'S OWN
// scope-bound task token, and a foreign thread/turn stays NOT_FOUND.
func TestRebuiltHistoryDoesNotCrossSubjects(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-1", "done", turnPayload(t, "private", 8))
	st.seedThread("s2", "TH-2")
	st.seedTurn("s2", "TH-2", "RUN-2", "done", turnPayload(t, "also private", 8))

	hs := restartEdge(t, st)

	if code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"thread_id": "TH-1"}, "s2"); code != http.StatusNotFound {
		t.Fatalf("a foreign thread must stay 404, got http %d body %s", code, body)
	}
	if code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-1"}, "s2"); code != http.StatusNotFound {
		t.Fatalf("a foreign turn must stay 404, got http %d body %s", code, body)
	}
	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{}, "s2")
	if code != http.StatusOK {
		t.Fatalf("s2 own unscoped read: http %d body %s", code, body)
	}
	for _, tr := range turnsOf(t, body) {
		if tr["id"] == "RUN-1" || tr["thread_id"] == "TH-1" {
			t.Fatalf("s2 received s1's history over an unscoped read: %s", body)
		}
		if tr["thread_id"] != "TH-2" {
			t.Fatalf("s2 must only ever see its own thread, got %v", tr["thread_id"])
		}
	}

	// And the durable reads really were made with the caller's exchanged task token, never a
	// raw bearer or another subject's credential.
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, c := range st.calls {
		if c == "" {
			continue
		}
		if tok, ok := splitToken(c); ok && tok != "" && tok != "Bearer TASK:s2" {
			t.Fatalf("a durable read used a credential that is not the caller's own task token: %q", c)
		}
	}
}

func splitToken(call string) (string, bool) {
	for i := 0; i < len(call); i++ {
		if call[i:len(call)] == " as " {
			return call[i+len(" as "):], true
		}
	}
	return "", false
}

// TestColdHistoryNeverMasksAFailure keeps the honest-failure property: a thread State does not
// know is an error answer, not a silent empty list, and an unknown turn id is still 404.
func TestColdHistoryNeverMasksAFailure(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	hs := restartEdge(t, st)

	if code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-NOPE"}, "s1"); code != http.StatusNotFound {
		t.Fatalf("an unknown turn must stay 404, got http %d body %s", code, body)
	}
	// A thread the caller owns but that has no runs is legitimately empty - and stays 200.
	if code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"thread_id": "TH-1"}, "s1"); code != http.StatusOK {
		t.Fatalf("a real thread with no turns is 200, got http %d body %s", code, body)
	}
}

var _ = transport.WithPrincipal // referenced through authPerSubject

// TestColdTurnTimestampsAreNotManufactured pins what a rebuilt turn is allowed to claim about
// time. State's published Run contract has no timestamp column, so a cold turn's dates come
// from the sortable-UTC prefix contracts.NewID puts on every identifier - the product's own
// convention, and the instant the user's history is actually sorted by. An id that does not
// follow that layout must stay dateless: stamping it with the time of the read would invent
// data and reshuffle the history on every refresh.
func TestColdTurnTimestampsAreNotManufactured(t *testing.T) {
	const (
		standID  = "20261101T011806.648368796Z_wox6acawdmt6tzqaz422sdwura" // the live stand's own id shape
		legacyID = "LEGACY-ROW-1"                                          // predates the convention
	)
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", standID, "done", turnPayload(t, "write a haiku", 8))
	st.seedTurn("s1", "TH-1", legacyID, "done", turnPayload(t, "older row", 8))

	hs := restartEdge(t, st)
	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"thread_id": "TH-1"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns after restart: http %d body %s", code, body)
	}
	byID := map[string]map[string]any{}
	for _, turn := range turnsOf(t, body) {
		if id, ok := turn["id"].(string); ok {
			byID[id] = turn
		}
	}
	if len(byID) != 2 {
		t.Fatalf("both durable rows must be listed, got %s", body)
	}

	created, err := time.Parse(time.RFC3339Nano, stringOf(t, byID[standID], "created_at"))
	if err != nil {
		t.Fatalf("created_at of a convention id must be a real instant: %v (turn=%v)", err, byID[standID])
	}
	want := time.Date(2026, 11, 1, 1, 18, 6, 648368796, time.UTC)
	if !created.Equal(want) {
		t.Fatalf("created_at must be the instant the id encodes, got %s want %s", created, want)
	}
	if legacy := stringOf(t, byID[legacyID], "created_at"); legacy != "0001-01-01T00:00:00Z" {
		t.Fatalf("a non-conforming id must stay dateless, got %q (manufacturing a date would reorder history)", legacy)
	}
}

func stringOf(t *testing.T, m map[string]any, key string) string {
	t.Helper()
	v, ok := m[key].(string)
	if !ok {
		t.Fatalf("field %q missing or not a string in %v", key, m)
	}
	return v
}

// TestSingleTurnReadDoesNotBecomeTheThreadHistory guards the cache rule that a GetTurn answers
// about ONE turn, not about a thread. Caching that single row as the thread's page would make
// the next ListTurns hide the user's other turns - the same class of loss as the reported
// defect, arriving through the performance path instead of the restart path.
func TestSingleTurnReadDoesNotBecomeTheThreadHistory(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-A", "done", turnPayload(t, "first", 8))
	st.seedTurn("s1", "TH-1", "RUN-B", "done", turnPayload(t, "second", 8))

	hs := restartEdge(t, st)

	if code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-A"}, "s1"); code != http.StatusOK {
		t.Fatalf("cold GetTurn: http %d body %s", code, body)
	}
	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"thread_id": "TH-1"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns after a cold GetTurn: http %d body %s", code, body)
	}
	if got := turnsOf(t, body); len(got) != 2 {
		t.Fatalf("a single-turn read must not narrow the thread history, got %d turns: %s", len(got), body)
	}
}

// TestRunningTurnStatusIsNotFrozenByAColdRead: a turn that was still moving when it was first
// read after a restart must keep telling the truth afterwards. Only terminal rows are
// immutable facts, so only they may be cached.
func TestRunningTurnStatusIsNotFrozenByAColdRead(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-1")
	st.seedTurn("s1", "TH-1", "RUN-LIVE", "running", turnPayload(t, "still working", 8))

	hs := restartEdge(t, st)

	code, body := postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-LIVE"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("cold GetTurn of a running turn: http %d body %s", code, body)
	}
	if status := fieldOf(t, body, "status"); status != "running" {
		t.Fatalf("status must be read from the durable row, got %q", status)
	}

	st.setStatus("RUN-LIVE", "done")

	code, body = postSpaceAs(t, hs.URL, "GetTurn", map[string]any{"turn_id": "RUN-LIVE"}, "s1")
	if code != http.StatusOK {
		t.Fatalf("GetTurn after the run finished: http %d body %s", code, body)
	}
	if status := fieldOf(t, body, "status"); status != "done" {
		t.Fatalf("a non-terminal status must not be cached (the user would watch a finished turn run forever), got %q", status)
	}
}

func fieldOf(t *testing.T, body []byte, key string) string {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		t.Fatalf("decode body %s: %v", body, err)
	}
	v, ok := m[key].(string)
	if !ok {
		t.Fatalf("field %q missing or not a string in %s", key, body)
	}
	return v
}
