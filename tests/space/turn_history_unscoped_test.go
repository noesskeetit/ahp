package space_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The second half of the same defect, measured on the DEPLOYED pin at 02:26Z: with two warm
// turns in the runner's memory and cold history in State, POST /v1/space/ListTurns with {}
// returned only the two warm turns and no durable_partial. The first version of the fix read
// State only when the live table came back EMPTY, so any warm turn hid the whole cold history
// silently. These tests pin the contract instead: ListTurns {} is the caller's turn list, and a
// list that does not contain everything the caller has must say so.

// coldEdge is the production HTTP edge over a full-write State double (mockState understands
// the whole AcceptTurn/DispatchTurn path, so a turn can be genuinely WARM here) with the
// durable fan-out bound set by the test.
func coldEdge(t *testing.T, threads int, core space.CoreRunner) (*httptest.Server, *mockState, *space.Coordinator) {
	t.Helper()
	ms := newMockState()
	srv := ms.Server()
	t.Cleanup(srv.Close)
	sc := space.NewStateClient(srv.URL, nil)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient:        sc,
		Core:               core,
		WorkerID:           "w-warm-cold",
		TurnHistoryThreads: threads,
	})
	h := space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: authPerSubject})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs, ms, coord
}

// seedColdHistory writes history this process never executed: a thread plus terminal runs,
// straight into the durable store the way an earlier process left it. The run ids carry the
// usual sortable-UTC prefix, which is what makes the merged ordering assertable.
func seedColdHistory(t *testing.T, ms *mockState, threadID string, at time.Time, inputs ...string) []string {
	t.Helper()
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.threads[threadID] = &threadRecord{ID: threadID, ContextID: "ctx-" + threadID, Epoch: 1, CreatedAt: at}
	ids := make([]string, 0, len(inputs))
	for i, in := range inputs {
		ms.nextSeq++
		payload, err := json.Marshal(map[string]any{"type": "turn", "input": in, "max_steps": 8})
		if err != nil {
			t.Fatalf("payload: %v", err)
		}
		rec := &runRecord{
			ID:       at.Add(time.Duration(i)*time.Minute).UTC().Format("20060102T150405.000000000Z") + "_cold" + threadID[len(threadID)-6:],
			ThreadID: threadID,
			TaskID:   "rtask-cold-" + threadID,
			Status:   "done",
			Sequence: ms.nextSeq,
			Payload:  payload,
		}
		ms.runs[rec.ID] = rec
		ids = append(ids, rec.ID)
	}
	return ids
}

func decodeListTurns(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode ListTurns body %s: %v", body, err)
	}
	return out
}

func boolField(t *testing.T, m map[string]any, key string) bool {
	t.Helper()
	v, ok := m[key].(bool)
	if !ok {
		return false
	}
	return v
}

func turnIDs(turns []map[string]any) map[string]bool {
	seen := map[string]bool{}
	for _, turn := range turns {
		if id, ok := turn["id"].(string); ok {
			seen[id] = true
		}
	}
	return seen
}

// TestUnscopedListTurnsShowsColdHistoryWhileWarmTurnsExist is the deputy's measurement as a
// test: a warm turn in this process's memory must not make cold history disappear, and it must
// not make the answer claim completeness it has not earned.
func TestUnscopedListTurnsShowsColdHistoryWhileWarmTurnsExist(t *testing.T) {
	hs, ms, _ := coldEdge(t, space.DefaultTurnHistoryThreads, &stubCore{})

	warmTurn, warmThread := runTurnAcceptDispatchStream(t, hs.URL, "s1", "warm turn in memory")
	if warmTurn == "" || warmThread == "" {
		t.Fatalf("no warm turn produced")
	}
	cold := seedColdHistory(t, ms, "20261101T021600.000000000Z_coldthreadaaaaaaaaaaaaaaaaaaaa",
		time.Date(2026, 11, 1, 2, 16, 0, 0, time.UTC), "cold turn one", "cold turn two")

	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns {} = %d, want 200 (body=%s)", code, body)
	}
	got := turnIDs(turnsOf(t, body))
	if !got[warmTurn] {
		t.Fatalf("the warm turn must stay listed after the durable merge, got %s", body)
	}
	for _, id := range cold {
		if !got[id] {
			t.Fatalf("cold turn %s is missing from the unscoped list while a warm turn exists: %s", id, body)
		}
	}
	if len(got) != len(cold)+1 {
		t.Fatalf("the warm turn is also a durable row and must appear exactly once: got %d rows, want %d (%s)", len(got), len(cold)+1, body)
	}
	if !boolField(t, decodeListTurns(t, body), "durable_partial") {
		t.Logf("list covered every seeded thread, so no partial flag is owed; body=%s", body)
	}
}

// TestUnscopedListTurnsPaginatesWithTheStateCursor pins that the fan-out is bounded and that
// the bound is recoverable: one thread per walk, and the published cursor must actually
// continue the walk instead of restarting or losing the rest.
func TestUnscopedListTurnsPaginatesWithTheStateCursor(t *testing.T) {
	hs, ms, _ := coldEdge(t, 1, &stubCore{})

	first := seedColdHistory(t, ms, "20261101T030000.000000000Z_coldthreadaaaaaaaaaaaaaaaaa1",
		time.Date(2026, 11, 1, 3, 0, 0, 0, time.UTC), "newest cold turn")
	second := seedColdHistory(t, ms, "20261101T020000.000000000Z_coldthreadaaaaaaaaaaaaaaaaa2",
		time.Date(2026, 11, 1, 2, 0, 0, 0, time.UTC), "older cold turn")

	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"limit": 50}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns page 1 = %d, want 200 (body=%s)", code, body)
	}
	page1 := decodeListTurns(t, body)
	if !boolField(t, page1, "durable_partial") || !boolField(t, page1, "has_more") {
		t.Fatalf("a walk over a bounded thread page must be reported as partial, got %s", body)
	}
	next, _ := page1["next_cursor"].(string)
	if next == "" {
		t.Fatalf("a partial walk must publish where to continue, got %s", body)
	}
	page1IDs := turnIDs(turnsOf(t, body))
	for _, id := range second {
		if page1IDs[id] {
			t.Fatalf("page 1 walked past its bound and returned the older thread: %s", body)
		}
	}
	for _, id := range first {
		if !page1IDs[id] {
			t.Fatalf("page 1 must contain the newest thread's turns, got %s", body)
		}
	}

	code, body = postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"cursor": next, "limit": 50}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns page 2 = %d, want 200 (body=%s)", code, body)
	}
	page2 := turnIDs(turnsOf(t, body))
	for _, id := range second {
		if !page2[id] {
			t.Fatalf("cursor %q did not continue the walk to the next thread, got %s", next, body)
		}
	}
}

// TestUnscopedListTurnsTrimsWithLimitAndSaysSo guards the second way a list can lie: a limit
// that silently drops rows. Rows held back by the caller's own limit are still reported.
func TestUnscopedListTurnsTrimsWithLimitAndSaysSo(t *testing.T) {
	hs, ms, _ := coldEdge(t, space.DefaultTurnHistoryThreads, &stubCore{})
	seedColdHistory(t, ms, "20261101T040000.000000000Z_coldthreadaaaaaaaaaaaaaaaaa3",
		time.Date(2026, 11, 1, 4, 0, 0, 0, time.UTC), "one", "two", "three")

	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"limit": 2}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns limit=2 = %d, want 200 (body=%s)", code, body)
	}
	page := decodeListTurns(t, body)
	if turns, _ := page["turns"].([]any); len(turns) != 2 {
		t.Fatalf("limit must cap the returned rows, got %s", body)
	}
	if !boolField(t, page, "durable_partial") || !boolField(t, page, "has_more") {
		t.Fatalf("rows held back by limit must be flagged, not truncated quietly: %s", body)
	}
}

// TestUnscopedListTurnsWalkIsBoundedAndMeasured reads the bound at the unit the HTTP layer
// hides: the walk reports how many threads it actually touched and how long it took, so
// "bounded" is a measurable claim in the evidence rather than a comment.
func TestUnscopedListTurnsWalkIsBoundedAndMeasured(t *testing.T) {
	st := newScopedState()
	for i, id := range []string{"TH-1", "TH-2", "TH-3"} {
		st.seedThread("s1", id)
		st.seedTurn("s1", id, "RUN-"+id, "done", turnPayload(t, "turn", 8))
		_ = i
	}
	srv := st.Server()
	t.Cleanup(srv.Close)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient:        space.NewStateClient(srv.URL, nil),
		Core:               &stubCore{},
		WorkerID:           "w-bounded",
		TurnHistoryThreads: 2,
		TurnHistoryBudget:  2 * time.Second,
	})
	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "s1"}

	walk, err := coord.HydrateRecentTurnsFromState(context.Background(), "TASK:s1", "", scope)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if walk.ThreadsWalked == 0 || walk.ThreadsWalked > 2 {
		t.Fatalf("the walk must stay inside its thread bound and report what it did, walked=%d", walk.ThreadsWalked)
	}
	if walk.Elapsed <= 0 {
		t.Fatalf("the walk must report its cost, elapsed=%s", walk.Elapsed)
	}
	if !walk.Partial || walk.NextCursor == "" {
		t.Fatalf("three threads with a bound of two must be partial with a cursor, got %+v", walk)
	}

	resume, err := coord.HydrateRecentTurnsFromState(context.Background(), "TASK:s1", walk.NextCursor, scope)
	if err != nil {
		t.Fatalf("resumed walk: %v", err)
	}
	if len(resume.Turns) == 0 {
		t.Fatalf("the cursor must reach the remaining thread, got %+v", resume)
	}
}

// restartEdgeWith is restartEdge with the fan-out knobs the paging tests need.
func restartEdgeWith(t *testing.T, st *scopedState, threads int, budget time.Duration) *httptest.Server {
	t.Helper()
	srv := st.Server()
	t.Cleanup(srv.Close)
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient:        space.NewStateClient(srv.URL, nil),
		Core:               &stubCore{},
		WorkerID:           "w-after-restart",
		TurnHistoryThreads: threads,
		TurnHistoryBudget:  budget,
	})
	h := space.NewHandler(space.HandlerConfig{
		Coordinator:       coord,
		Auth:              space.ExchangeMiddleware(&recExchange{}, authPerSubject),
		RequireScopedTask: true,
	})
	hs := httptest.NewServer(h)
	t.Cleanup(hs.Close)
	return hs
}

// TestUnscopedListTurnsNeverReadsAnotherSubjectsHistory holds the scope line while the fan-out
// is open: the rebuild walks the caller's own threads with the caller's own task token, and
// another subject's thread stays NOT_FOUND rather than becoming an empty list.
func TestUnscopedListTurnsNeverReadsAnotherSubjectsHistory(t *testing.T) {
	st := newScopedState()
	st.seedThread("s1", "TH-OURS")
	st.seedTurn("s1", "TH-OURS", "RUN-OURS", "done", turnPayload(t, "ours", 8))
	st.seedThread("s2", "TH-THEIRS")
	st.seedTurn("s2", "TH-THEIRS", "RUN-THEIRS", "done", turnPayload(t, "theirs", 8))

	hs := restartEdgeWith(t, st, 25, 2*time.Second)

	code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{}, "s1")
	if code != http.StatusOK {
		t.Fatalf("ListTurns {} as s1 = %d, want 200 (body=%s)", code, body)
	}
	got := turnIDs(turnsOf(t, body))
	if !got["RUN-OURS"] {
		t.Fatalf("the caller's own cold turn must be listed, got %s", body)
	}
	if got["RUN-THEIRS"] {
		t.Fatalf("another subject's turn leaked into the unscoped list: %s", body)
	}

	st.mu.Lock()
	calls := append([]string(nil), st.calls...)
	st.mu.Unlock()
	own := bearerTaskPrefix + "s1"
	if len(calls) == 0 {
		t.Fatalf("the unscoped read must actually consult State, saw no calls")
	}
	for _, call := range calls {
		if !strings.HasSuffix(call, own) {
			t.Fatalf("every durable read must use the caller's own token, saw %q", call)
		}
	}

	if code, body := postSpaceAs(t, hs.URL, "ListTurns", map[string]any{"thread_id": "TH-THEIRS"}, "s1"); code != 404 {
		t.Fatalf("a foreign thread_id must stay 404, got %d (%s)", code, body)
	}
}

// TestUnscopedWalkRefusesAThreadAnotherSubjectExecuted pins the product's own refusal, not the
// double's politeness. The scope filter in the State double could satisfy the previous test on
// its own, so this one removes that filter (a thread handed to every caller, as a
// scope-blind State server would) and still requires Space to keep bob out of alice's history.
// ThreadsSkipped is the measured proof the refusal fired rather than silently coinciding.
func TestUnscopedWalkRefusesAThreadAnotherSubjectExecuted(t *testing.T) {
	hs, ms, coord := coldEdge(t, space.DefaultTurnHistoryThreads, &stubCore{})

	aliceTurn, _ := runTurnAcceptDispatchStream(t, hs.URL, "alice", "mine")
	bobTurn, bobThread := runTurnAcceptDispatchStream(t, hs.URL, "bob", "not yours")

	ms.mu.Lock()
	if rec, ok := ms.threads[bobThread]; ok {
		rec.Owner = "" // a server that enumerates this thread for everyone
	}
	ms.mu.Unlock()

	scope := contracts.Scope{Tenant: "t1", Deployment: "d1", Subject: "alice"}
	walk, err := coord.HydrateRecentTurnsFromState(context.Background(), "tok-alice", "", scope)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	got := map[string]bool{}
	for _, turn := range walk.Turns {
		got[turn.ID] = true
	}
	if got[bobTurn] {
		t.Fatalf("a thread this process executed for bob was merged into alice's history")
	}
	if !got[aliceTurn] {
		t.Fatalf("alice's own turn must still come through, got %d turns", len(walk.Turns))
	}
	if walk.ThreadsSkipped == 0 {
		t.Fatalf("the refusal must be counted, otherwise a passing test cannot tell defense from luck")
	}
}
