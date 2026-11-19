//go:build state_integration

package space_test

// delete_chat_realstate_test.go — the SAME delete-chat promises as
// delete_chat_test.go, measured on the REAL State service over PostgreSQL
// (production stateapi handler, mTLS workload identity, Ed25519 task tokens, real
// lease fencing) behind the production Space HTTP edge, on this lane's own database
// and its own ephemeral ports.
//
// This leg carries what the offline double cannot prove:
//
//   - the CASCADE is the database's, not a loop in Go: after the delete the rows are
//     gone in the tables themselves, read back over SQL, because a list op of the
//     same API whose filters or caches could hide a row is not evidence;
//   - the liveness gate uses the REAL run-status vocabulary. The gate set
//     {pending, running, waiting} is the State server's own definition of a run that
//     still has work (internal/state/events.go asks for exactly that triple when it
//     decides which run an inbox event wakes, and runs_status_enum is the authority
//     on the rest) — and only a real claim leaves a real 'running' row to refuse.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/space"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// countRows answers one COUNT(*) against this lane's own database.
func (e *cEnv) countRows(t *testing.T, query string, args ...any) int64 {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("delete-chat real-state test needs AHP_TEST_DATABASE_URL")
	}
	pool, err := pgxpool.New(e.ctx, dsn)
	if err != nil {
		t.Fatalf("count pool: %v", err)
	}
	defer pool.Close()
	var n int64
	if err := pool.QueryRow(e.ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return n
}

// threadRowCounts counts one row in every table a chat delete is supposed to
// cascade. Intents are counted by run id, which is the whole point: after the
// cascade the run row is gone too, so only the journal's own key shows that the
// duplicate-effect barrier really left with it.
func (e *cEnv) threadRowCounts(t *testing.T, threadID, runID string) map[string]int64 {
	t.Helper()
	return map[string]int64{
		"threads":       e.countRows(t, `SELECT count(*) FROM ahp_state.threads WHERE id = $1`, threadID),
		"runs":          e.countRows(t, `SELECT count(*) FROM ahp_state.runs WHERE thread_id = $1`, threadID),
		"intents":       e.countRows(t, `SELECT count(*) FROM ahp_state.intents WHERE run_id = $1`, runID),
		"stream_events": e.countRows(t, `SELECT count(*) FROM ahp_state.stream_events WHERE thread_id = $1`, threadID),
		"inbox_events":  e.countRows(t, `SELECT count(*) FROM ahp_state.inbox_events WHERE thread_id = $1`, threadID),
		"checkpoints":   e.countRows(t, `SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id = $1`, threadID),
		"kv":            e.countRows(t, `SELECT count(*) FROM ahp_state.kv WHERE thread_id = $1`, threadID),
	}
}

// chatEdgeOnRealState mounts the production Space edge (Handler + Coordinator +
// real StateClient) over this env's real State, with a Core that must never run:
// deleting a chat executes nothing, so a Core call here would itself be the bug.
func chatEdgeOnRealState(t *testing.T, e *cEnv) (*httptest.Server, string) {
	t.Helper()
	coord := space.NewCoordinator(space.CoordinatorConfig{
		StateClient: e.client,
		Core:        &stubCore{},
		WorkerID:    "w-delete-" + contracts.NewID(),
	})
	auth := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := transport.WithPrincipal(r.Context(), transport.Principal{
				Scope: contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: cSubject},
			})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	srv := httptest.NewServer(space.NewHandler(space.HandlerConfig{Coordinator: coord, Auth: auth}))
	t.Cleanup(srv.Close)
	return srv, e.signTask(t, "task-"+contracts.NewID())
}

// seedDeletedChatShape creates a real thread whose one run is closed (done) and that
// owns at least one real row in EVERY table State cascades: run, intent journal
// (recorded -> dispatched -> succeeded, i.e. a completed external effect), stream
// event, inbox event, checkpoint and KV.
func seedDeletedChatShape(t *testing.T, e *cEnv, owner string) (string, string) {
	t.Helper()
	ctx := e.ctx
	tag := contracts.NewID()
	th, err := e.client.CreateThread(ctx, owner, contracts.Scope{}, "ctx-dc-"+tag, "dc-thread-"+tag)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	// task_id is empty on purpose: State binds a run to the task of the PRESENTING
	// token, and `owner` is signed for its own task, so naming a different task id here
	// is refused (stateapi: "task_id does not match the presenting task token"). The
	// worker credential this run needs comes from ClaimRun below, exactly as in the
	// other real-State files.
	run, err := e.client.CreateRun(ctx, owner, th.ID, "dc-run-"+tag, json.RawMessage(`{"type":"turn","input":"write the report"}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w-delete", 120*time.Second)
	if err != nil || claim.Lease == nil {
		t.Fatalf("ClaimRun: %v (lease=%v)", err, claim.Lease)
	}
	if claim.Lease.Run.ID != run.ID {
		t.Fatalf("ClaimRun handed out %s, expected the run just created (%s)", claim.Lease.Run.ID, run.ID)
	}
	tok := claim.TaskToken
	fence := space.Fence{RunID: run.ID, Worker: "w-delete", Epoch: claim.Lease.Epoch}

	if _, err := e.client.AppendStream(ctx, tok, th.ID, fence.RunID, fence.Worker, fence.Epoch,
		space.StepKey(fence.RunID, 0), "model", json.RawMessage(`{"content":"working"}`)); err != nil {
		t.Fatalf("AppendStream: %v", err)
	}
	if _, err := e.client.Enqueue(ctx, owner, th.ID, "dc-inbox-"+tag, "steering",
		json.RawMessage(`{"instruction":"shorter"}`)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := e.client.PutCheckpoint(ctx, tok, fence, space.CheckpointNamespace,
		"dc-cp-"+tag, "", "dc-cp-"+tag, 1, json.RawMessage(`{"node":"model"}`), nil); err != nil {
		t.Fatalf("PutCheckpoint: %v", err)
	}
	if _, err := e.client.RecordIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch,
		"tools", "send_report", "tool", "smtp", json.RawMessage(`{"to":"board"}`)); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	if _, err := e.client.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch,
		"tools", "send_report", "dispatched", nil); err != nil {
		t.Fatalf("UpdateIntent(dispatched): %v", err)
	}
	if _, err := e.client.UpdateIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch,
		"tools", "send_report", "succeeded", json.RawMessage(`{"message_id":"m-1"}`)); err != nil {
		t.Fatalf("UpdateIntent(succeeded): %v", err)
	}
	if _, err := e.client.PutKV(ctx, tok, fence, []string{"memory"}, "dc-k-"+tag, "dc-op-"+tag, 0,
		json.RawMessage(`{"draft":"final"}`)); err != nil {
		t.Fatalf("PutKV: %v", err)
	}
	// Close the run the way a finished turn does, so the chat is legitimately
	// deletable: the run row is terminal and the lease is gone.
	if err := e.client.Release(ctx, owner, fence.RunID, fence.Worker, fence.Epoch, "done"); err != nil {
		t.Fatalf("Release(done): %v", err)
	}
	return th.ID, run.ID
}

func TestDeleteChatRealStateCascadesEveryDurableRow(t *testing.T) {
	e := cNewEnv(t)
	spaceSrv, owner := chatEdgeOnRealState(t, e)
	threadID, runID := seedDeletedChatShape(t, e, owner)

	before := e.threadRowCounts(t, threadID, runID)
	for _, table := range []string{"threads", "runs", "intents", "stream_events", "inbox_events", "checkpoints", "kv"} {
		if before[table] == 0 {
			t.Fatalf("the seed wrote no %s rows - the assertion below would be vacuous", table)
		}
	}

	code, body := postSpace(t, spaceSrv.URL, "DeleteChat", map[string]any{"thread_id": threadID}, owner)
	if code != http.StatusOK {
		t.Fatalf("DeleteChat(own chat) = %d, want 200 (body=%s)", code, body)
	}

	after := e.threadRowCounts(t, threadID, runID)
	for _, table := range []string{"threads", "runs", "intents", "stream_events", "inbox_events", "checkpoints", "kv"} {
		if after[table] != 0 {
			t.Fatalf("%s rows survived the delete of thread %s: %d (before=%d)", table, threadID, after[table], before[table])
		}
	}

	// The API agrees with the tables: the chat is off the list, its durable history
	// is unreachable, and a second delete is NOT_FOUND rather than a second 200.
	if c, b := postSpace(t, spaceSrv.URL, "ListChats", map[string]any{"limit": 50}, owner); c != http.StatusOK {
		t.Fatalf("ListChats = %d, want 200 (body=%s)", c, b)
	} else if parseChatIDs(t, b)[threadID] {
		t.Fatalf("deleted thread %s still in ListChats: %s", threadID, b)
	}
	if c, b := postSpace(t, spaceSrv.URL, "ListRunHistory", map[string]any{"thread_id": threadID}, owner); c != http.StatusNotFound {
		t.Fatalf("ListRunHistory(deleted) = %d, want 404 (body=%s)", c, b)
	}
	if c, b := postSpace(t, spaceSrv.URL, "DeleteChat", map[string]any{"thread_id": threadID}, owner); c != http.StatusNotFound {
		t.Fatalf("DeleteChat(second) = %d, want 404 (body=%s)", c, b)
	}
}

func TestDeleteChatRealStateRefusesLiveRunAndKeepsTheJournal(t *testing.T) {
	e := cNewEnv(t)
	spaceSrv, owner := chatEdgeOnRealState(t, e)
	ctx := e.ctx

	tag := contracts.NewID()
	th, err := e.client.CreateThread(ctx, owner, contracts.Scope{}, "ctx-dclive-"+tag, "dc-live-"+tag)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	run, err := e.client.CreateRun(ctx, owner, th.ID, "dc-live-run-"+tag, json.RawMessage(`{"type":"turn","input":"go"}`), "")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	claim, err := e.client.ClaimRun(ctx, "", "w-delete-live", 120*time.Second)
	if err != nil || claim.Lease == nil {
		t.Fatalf("ClaimRun: %v (lease=%v)", err, claim.Lease)
	}
	tok := claim.TaskToken
	fence := space.Fence{RunID: run.ID, Worker: "w-delete-live", Epoch: claim.Lease.Epoch}
	if _, err := e.client.RecordIntent(ctx, tok, fence.RunID, fence.Worker, fence.Epoch,
		"tools", "charge_invoice", "tool", "billing", json.RawMessage(`{"amount":100}`)); err != nil {
		t.Fatalf("RecordIntent: %v", err)
	}
	// The refusal below is only meaningful against a REAL live row: a claim by the
	// production client must leave the run 'running' in the database.
	if n := e.countRows(t, `SELECT count(*) FROM ahp_state.runs WHERE id = $1 AND status = 'running'`, run.ID); n != 1 {
		t.Fatalf("the claim left no durable 'running' row (rows=%d) - nothing for the gate to refuse", n)
	}

	code, body := postSpace(t, spaceSrv.URL, "DeleteChat", map[string]any{"thread_id": th.ID}, owner)
	if code != http.StatusConflict {
		t.Fatalf("DeleteChat(live run) = %d, want 409 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrConflict {
		t.Fatalf("DeleteChat(live run) code = %q, want %s (body=%s)", got, contracts.ErrConflict, body)
	}

	during := e.threadRowCounts(t, th.ID, run.ID)
	if during["threads"] != 1 || during["runs"] != 1 || during["intents"] < 1 {
		t.Fatalf("a refused delete must change nothing: %+v", during)
	}

	// The worker finishes the run for real; only then is the chat deletable, and the
	// same cascade applies.
	if err := e.client.Release(ctx, owner, fence.RunID, fence.Worker, fence.Epoch, "done"); err != nil {
		t.Fatalf("Release(done): %v", err)
	}
	if c, b := postSpace(t, spaceSrv.URL, "DeleteChat", map[string]any{"thread_id": th.ID}, owner); c != http.StatusOK {
		t.Fatalf("DeleteChat(after the run closed) = %d, want 200 (body=%s)", c, b)
	}
	if after := e.threadRowCounts(t, th.ID, run.ID); after["threads"] != 0 || after["runs"] != 0 || after["intents"] != 0 {
		t.Fatalf("rows survived the delete once the run was terminal: %+v", after)
	}
}

func TestDeleteChatRealStateLegalholdRefusesDeletion(t *testing.T) {
	e := cNewEnv(t)
	spaceSrv, owner := chatEdgeOnRealState(t, e)

	tag := contracts.NewID()
	th, err := e.client.CreateThread(e.ctx, owner, contracts.Scope{}, "ctx-dchold-"+tag, "dc-hold-"+tag)
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	if code, resp := e.adminRaw(t, "SetLegalhold", cSubject, map[string]string{"thread_id": th.ID, "reason": "litigation"}); code != http.StatusOK {
		t.Fatalf("SetLegalhold = %d (%v)", code, resp)
	}

	code, body := postSpace(t, spaceSrv.URL, "DeleteChat", map[string]any{"thread_id": th.ID}, owner)
	if code != http.StatusForbidden {
		t.Fatalf("DeleteChat(legalhold) = %d, want 403 (body=%s)", code, body)
	}
	if got, _ := decodeMap(t, body)["code"].(string); got != contracts.ErrForbidden {
		t.Fatalf("DeleteChat(legalhold) code = %q, want %s (body=%s)", got, contracts.ErrForbidden, body)
	}
	if n := e.countRows(t, `SELECT count(*) FROM ahp_state.threads WHERE id = $1`, th.ID); n != 1 {
		t.Fatalf("a legalhold refusal deleted the thread anyway (rows=%d)", n)
	}

	// The hold is what blocked it: lift it and the identical delete goes through.
	if code, resp := e.adminRaw(t, "RemoveLegalhold", cSubject, map[string]string{"thread_id": th.ID}); code != http.StatusOK {
		t.Fatalf("RemoveLegalhold = %d (%v)", code, resp)
	}
	if c, b := postSpace(t, spaceSrv.URL, "DeleteChat", map[string]any{"thread_id": th.ID}, owner); c != http.StatusOK {
		t.Fatalf("DeleteChat(after the hold) = %d, want 200 (body=%s)", c, b)
	}
	if n := e.countRows(t, `SELECT count(*) FROM ahp_state.threads WHERE id = $1`, th.ID); n != 0 {
		t.Fatalf("the thread survived a delete after the hold was lifted (rows=%d)", n)
	}
}
