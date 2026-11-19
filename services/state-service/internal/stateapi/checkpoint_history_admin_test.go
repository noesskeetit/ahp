package stateapi

// Retention over the wire (matrix row 15, PDF section 4 Lifecycle and the
// administrative admission at accept/space-pdf-rev32.txt:1787-1792, :1918-1920).
//
// A bound nobody can reach is a code comment, not a feature. The legs here are
// the operator's door: the internal administrative route applies the checkpoint
// history bound, a task credential is refused at both doors, a human without
// the administrative grant is refused, and the answer an operator gets back is
// checked against the rows the database actually holds.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/ahp-platform/ahp/internal/state"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// writeCheckpointHistory gives a thread a history deeper than the bound below.
func writeCheckpointHistory(t *testing.T, e *env, scope contracts.Scope, count int) string {
	t.Helper()
	thread, err := e.store.CreateThread(e.ctx, scope, "hist-"+t.Name()+"-"+contracts.NewID(), "k-"+contracts.NewID())
	if err != nil {
		t.Fatal(err)
	}
	run, err := e.store.CreateRun(e.ctx, scope, thread.ID, "task-"+contracts.NewID(), "k-"+contracts.NewID(),
		json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	lease, err := e.store.ClaimRun(e.ctx, scope.Tenant, scope.Deployment, "w1", 6e10)
	if err != nil || lease == nil {
		t.Fatal(err)
	}
	fence := state.Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	for i := 0; i < count; i++ {
		checkpoint := state.Checkpoint{ID: fmt.Sprintf("cp-%d", i), Namespace: "ns", Version: int64(i + 1),
			Parts: []state.Part{{Channel: "values", Version: fmt.Sprintf("v%d", i),
				Data: []byte(fmt.Sprintf("value %d", i))}}}
		if i > 0 {
			checkpoint.Parent = fmt.Sprintf("cp-%d", i-1)
		}
		if _, err := e.store.PutCheckpoint(e.ctx, scope, fence, checkpoint, "k-cp-"+fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	return thread.ID
}

func historyRows(t *testing.T, e *env, threadID string) int {
	t.Helper()
	var rows int
	if err := e.store.Pool().QueryRow(e.ctx,
		`SELECT count(*) FROM ahp_state.checkpoints WHERE thread_id = $1`, threadID).Scan(&rows); err != nil {
		t.Fatalf("count checkpoints out of band: %v", err)
	}
	return rows
}

func TestCheckpointHistoryRetentionIsAnAdministrativeOperation(t *testing.T) {
	requireLaneDatabase(t)
	e := newEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	const depth, bound = 6, 2
	threadID := writeCheckpointHistory(t, e, scope, depth)
	if got := historyRows(t, e, threadID); got != depth {
		t.Fatalf("fixture wrote %d checkpoints, want %d", got, depth)
	}
	input := fmt.Sprintf(`{"thread_id":%q,"namespace":"ns","limit":%d}`, threadID, bound)

	// A task credential is refused at the working door...
	task := e.signTask(t, "history-task")
	if code, body := e.call(t, "PruneCheckpointHistory", task, input); code != http.StatusForbidden {
		t.Fatalf("task token reached retention through /v1/state/: %d %v", code, body)
	}
	// ...and at the administrative door, even though that door exists.
	if code, body := doCall(t, e.srv.Client(), e.srv.URL+"/v1/state-admin/PruneCheckpointHistory",
		http.MethodPost, task,
		fmt.Sprintf(`{"target":{"deployment":%q,"subject":%q},"input":%s}`, e.deployment, testSubject, input)); code != http.StatusForbidden {
		t.Fatalf("task token reached retention through /v1/state-admin/: %d %v", code, body)
	}
	if got := historyRows(t, e, threadID); got != depth {
		t.Fatalf("a refused retention call deleted history: %d rows remain, want %d", got, depth)
	}
	// A human without the administrative grant is refused too.
	if code, body := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject,
		"PruneCheckpointHistory", input, "ordinary-human"); code != http.StatusForbidden {
		t.Fatalf("unprivileged human ran retention: %d %v", code, body)
	}
	if got := historyRows(t, e, threadID); got != depth {
		t.Fatalf("an unprivileged human changed history: %d rows remain", got)
	}

	// The administrative human applies the bound, and the report matches the
	// rows the database holds.
	code, report := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject,
		"PruneCheckpointHistory", input, "state-admin")
	mustStatus(t, code, http.StatusOK, report)
	if got := report["limit"]; got != float64(bound) {
		t.Fatalf("report limit = %v, want %d", got, bound)
	}
	if got := report["pruned"]; got != float64(depth-bound) {
		t.Fatalf("report pruned = %v, want %d", got, depth-bound)
	}
	if got := historyRows(t, e, threadID); got != bound {
		t.Fatalf("the database holds %d checkpoints after retention, want the bound of %d", got, bound)
	}
	// The readable window really is what survived.
	kept, err := e.store.ListCheckpoints(e.ctx, scope, threadID, "ns", 0)
	if err != nil {
		t.Fatalf("ListCheckpoints after retention: %v", err)
	}
	if len(kept) != bound || kept[0].ID != "cp-5" {
		t.Fatalf("surviving history = %v, want the two newest checkpoints", kept)
	}
	// Idempotent: the same administrative call again changes nothing.
	if _, again := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject,
		"PruneCheckpointHistory", input, "state-admin"); again["pruned"] != float64(0) {
		t.Fatalf("a second retention pass pruned again: %v", again)
	}
}

// TestCheckpointHistoryRetentionCannotCrossTenants: the administrative route
// acts inside the caller's own tenant. Another tenant's administrator must not
// be able to truncate this tenant's history, and a refusal must not delete
// anything on the way out.
func TestCheckpointHistoryRetentionCannotCrossTenants(t *testing.T) {
	requireLaneDatabase(t)
	e := newEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	const depth, bound = 4, 1
	threadID := writeCheckpointHistory(t, e, scope, depth)
	input := fmt.Sprintf(`{"thread_id":%q,"namespace":"ns","limit":%d}`, threadID, bound)

	foreign := "t-hist-foreign-" + contracts.NewID()
	if code, body := adminCall(t, e, e.srv.Client(), foreign, e.deployment, testSubject,
		"PruneCheckpointHistory", input, "state-admin"); code == http.StatusOK {
		t.Fatalf("another tenant applied retention to this tenant's thread: %v", body)
	}
	if got := historyRows(t, e, threadID); got != depth {
		t.Fatalf("a cross-tenant retention attempt changed history: %d rows, want %d", got, depth)
	}
}
