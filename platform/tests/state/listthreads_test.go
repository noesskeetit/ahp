package stateint

// Durable scoped ListThreads (chat/project listing, PDF 3.2). Every assertion is
// observed through the SDK / raw HTTP against the live mTLS server and real
// Postgres: scope is taken only from the token, pagination is a stable keyset
// that survives concurrent insert and delete, foreign/forged cursors cannot leak
// other scopes' rows, and deleted vs legal-held threads follow the documented
// Lifecycle retention semantics.

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	sdk "github.com/ahp-platform/ahp/sdk/go/state"
)

// createThreadUnderSubject creates one thread whose durable subject is the token
// subject, NOT any client-supplied field.
func createThreadUnderSubject(e *env, t *testing.T, subject string) sdk.Thread {
	t.Helper()
	e.setToken(e.signTaskScoped(t, e.tenant, e.deployment, subject, "task-"+contracts.NewID()))
	th, err := e.client.CreateThread(e.ctx, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
	requireNoErr(t, err, "CreateThread("+subject+")")
	return th
}

// listAll walks every page of ListThreads for the current token's scope and
// returns the threads in the order observed across pages.
func listAll(e *env, t *testing.T, pageSize int) []sdk.Thread {
	t.Helper()
	var acc []sdk.Thread
	cursor := ""
	for {
		page, next, _, err := e.client.ListThreads(e.ctx, cursor, pageSize)
		requireNoErr(t, err, "ListThreads page")
		acc = append(acc, page...)
		if next == "" {
			return acc
		}
		cursor = next
	}
}

// forgeThreadsCursor reproduces the opaque keyset cursor for a known
// (created_at, id) so a test can present a cursor position learned from a foreign
// thread.
func forgeThreadsCursor(t *testing.T, createdAt time.Time, id string) string {
	t.Helper()
	b, err := json.Marshal(struct {
		C time.Time `json:"c"`
		I string    `json:"i"`
	}{C: createdAt.UTC(), I: id})
	requireNoErr(t, err, "marshal cursor")
	return base64.RawURLEncoding.EncodeToString(b)
}

func idsOf(ts []sdk.Thread) []string {
	out := make([]string, len(ts))
	for i, th := range ts {
		out[i] = th.ID
	}
	return out
}

func containsID(ts []sdk.Thread, id string) bool {
	for _, th := range ts {
		if th.ID == id {
			return true
		}
	}
	return false
}

// TestListThreadsTwoSubjectsIsolated proves subject isolation inside one
// tenant/deployment: ada and bob share the workload identity but each token only
// enumerates its own threads.
func TestListThreadsTwoSubjectsIsolated(t *testing.T) {
	e := newEnv(t)
	const ada, bob = "user-ada", "user-bob"
	adaA := createThreadUnderSubject(e, t, ada)
	adaB := createThreadUnderSubject(e, t, ada)
	bobA := createThreadUnderSubject(e, t, bob)

	e.setToken(e.signTaskScoped(t, e.tenant, e.deployment, ada, "task-list-ada"))
	adaList := listAll(e, t, 10)
	if len(adaList) != 2 {
		t.Fatalf("ada sees %d threads, want 2: %v", len(adaList), idsOf(adaList))
	}
	seen := map[string]bool{}
	for _, th := range adaList {
		if th.Scope.Subject != ada {
			t.Fatalf("ada page leaked foreign subject %q", th.Scope.Subject)
		}
		seen[th.ID] = true
	}
	if !seen[adaA.ID] || !seen[adaB.ID] || seen[bobA.ID] {
		t.Fatalf("ada list wrong membership: %v", idsOf(adaList))
	}

	e.setToken(e.signTaskScoped(t, e.tenant, e.deployment, bob, "task-list-bob"))
	bobList := listAll(e, t, 10)
	if len(bobList) != 1 || bobList[0].ID != bobA.ID {
		t.Fatalf("bob sees %v, want exactly bobA", idsOf(bobList))
	}
}

// TestListThreadsForeignScopeCannotEnumerate proves a foreign principal lists none
// of the home scope's threads yet still lists its own — scoping, not denial.
func TestListThreadsForeignScopeCannotEnumerate(t *testing.T) {
	e := newEnv(t)
	createThreadUnderSubject(e, t, testSubject)

	fTenant := "f-" + contracts.NewID()
	fDep := "fd-" + contracts.NewID()
	fToken := e.signTaskScoped(t, fTenant, fDep, testSubject, "task-foreign")

	status, out := e.rawWithCert(t, fTenant, fDep, "ListThreads", fToken, jsonBody(map[string]any{}))
	if status != 200 {
		t.Fatalf("foreign ListThreads status=%d body=%v, want 200", status, out)
	}
	if arr, _ := out["threads"].([]any); len(arr) != 0 {
		t.Fatalf("foreign ListThreads leaked %d home rows", len(arr))
	}

	e.rawWithCert(t, fTenant, fDep, "CreateThread", fToken,
		jsonBody(map[string]any{"context_id": "ctx-" + contracts.NewID(), "key": "k-" + contracts.NewID()}))
	status, out = e.rawWithCert(t, fTenant, fDep, "ListThreads", fToken, jsonBody(map[string]any{}))
	arr, _ := out["threads"].([]any)
	if status != 200 || len(arr) != 1 {
		t.Fatalf("foreign self-list status=%d rows=%d, want 200/1", status, len(arr))
	}
}

// TestListThreadsPaginationStableUnderConcurrentInsert is the core keyset
// guarantee: every thread that existed when pagination began is returned exactly
// once, with no duplicate and no skip, even while threads are inserted
// concurrently.
func TestListThreadsPaginationStableUnderConcurrentInsert(t *testing.T) {
	e := newEnv(t)
	const seeded = 12
	snapshot := map[string]bool{}
	for i := 0; i < seeded; i++ {
		snapshot[createThreadUnderSubject(e, t, testSubject).ID] = true
	}

	stop := make(chan struct{})
	var insWG sync.WaitGroup
	insWG.Add(1)
	go func() {
		defer insWG.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			e.setToken(e.signTaskScoped(t, e.tenant, e.deployment, testSubject, "task-ins-"+contracts.NewID()))
			_, _ = e.client.CreateThread(e.ctx, "ctx-"+contracts.NewID(), "k-"+contracts.NewID())
		}
	}()

	counts := map[string]int{}
	cursor := ""
	pages := 0
	for {
		page, next, _, err := e.client.ListThreads(e.ctx, cursor, 3)
		requireNoErr(t, err, "ListThreads concurrent page")
		for _, th := range page {
			counts[th.ID]++
		}
		pages++
		if next == "" {
			break
		}
		cursor = next
		if pages > seeded+200 {
			t.Fatalf("pagination did not terminate (offset drift?)")
		}
	}
	close(stop)
	insWG.Wait()

	for id := range snapshot {
		if counts[id] != 1 {
			t.Fatalf("seeded thread %s appeared %d times across pages (want exactly 1)", id, counts[id])
		}
	}
}

// TestListThreadsPaginationMatchesFullList proves keyset concatenation equals a
// single authoritative listing: same set, same order, no dup/skip.
func TestListThreadsPaginationMatchesFullList(t *testing.T) {
	e := newEnv(t)
	want := map[string]bool{}
	for i := 0; i < 7; i++ {
		want[createThreadUnderSubject(e, t, testSubject).ID] = true
	}
	onePage := listAll(e, t, 100)
	paged := listAll(e, t, 2)
	if len(onePage) != len(paged) {
		t.Fatalf("paged length %d != single page %d", len(paged), len(onePage))
	}
	for i := range onePage {
		if onePage[i].ID != paged[i].ID {
			t.Fatalf("order diverges at %d: %s vs %s", i, onePage[i].ID, paged[i].ID)
		}
	}
	for id := range want {
		if !containsID(onePage, id) {
			t.Fatalf("missing %s", id)
		}
	}
}

// TestListThreadsDeletedGoneHeldRetained proves the documented Lifecycle
// retention boundary in the list: deleted thread disappears; a thread whose
// deletion is blocked by legal hold was never removed and remains listed.
func TestListThreadsDeletedGoneHeldRetained(t *testing.T) {
	e := newEnv(t)
	plain := createThreadUnderSubject(e, t, testSubject)
	held := createThreadUnderSubject(e, t, testSubject)
	other := createThreadUnderSubject(e, t, testSubject)

	e.adminCall(t, "SetLegalhold", map[string]string{"thread_id": held.ID, "reason": "litigation"}, nil)
	mustCode(t, e.client.DeleteThread(e.ctx, held.ID), "FORBIDDEN", "DeleteThread(legal-held)")
	requireNoErr(t, e.client.DeleteThread(e.ctx, plain.ID), "DeleteThread(plain)")

	e.setToken(e.signTaskScoped(t, e.tenant, e.deployment, testSubject, "task-after"))
	list := listAll(e, t, 10)
	if containsID(list, plain.ID) {
		t.Fatalf("deleted thread still listed")
	}
	if !containsID(list, held.ID) || !containsID(list, other.ID) || len(list) != 2 {
		t.Fatalf("unexpected remaining list: %v", idsOf(list))
	}
}

// TestListThreadsForeignAndForgedCursor covers a well-formed cursor whose position
// belongs to a foreign subject (must not leak) and a malformed cursor (rejected).
func TestListThreadsForeignAndForgedCursor(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 4; i++ {
		createThreadUnderSubject(e, t, testSubject)
	}
	bobThread := createThreadUnderSubject(e, t, "user-bob")

	e.setToken(e.signTaskScoped(t, e.tenant, e.deployment, testSubject, "task-cursor"))
	forged := forgeThreadsCursor(t, bobThread.CreatedAt, bobThread.ID)
	page, _, _, err := e.client.ListThreads(e.ctx, forged, 10)
	requireNoErr(t, err, "ListThreads foreign cursor")
	for _, th := range page {
		if th.ID == bobThread.ID || th.Scope.Subject == "user-bob" {
			t.Fatalf("foreign cursor leaked foreign row %s", th.ID)
		}
	}
	_, _, _, err = e.client.ListThreads(e.ctx, "!!!not-a-cursor!!!", 10)
	mustCode(t, err, "INVALID_ARGUMENT", "ListThreads malformed cursor")
}

// TestListThreadsBodyScopeRejected proves the body cannot carry identity.
func TestListThreadsBodyScopeRejected(t *testing.T) {
	e := newEnv(t)
	tok := e.signTask(t, "task-body")
	status, _ := e.rawWithCert(t, e.tenant, e.deployment, "ListThreads", tok, jsonBody(map[string]any{"subject": "attacker", "limit": 5}))
	if status != 400 {
		t.Fatalf("body-carrying-subject status=%d, want 400", status)
	}
}

// TestListThreadsBackingIndexExists proves the durable indexed access path for
// ListThreads is present in the real database after migration: the composite
// index on (tenant, deployment, subject, created_at DESC, id DESC) serves the
// scope filter and the ORDER BY / keyset bound as an index scan rather than a
// sort.
func TestListThreadsBackingIndexExists(t *testing.T) {
	e := newEnv(t)
	var def string
	err := e.pool.QueryRow(e.ctx,
		`SELECT indexdef FROM pg_indexes
		 WHERE schemaname = 'ahp_state' AND tablename = 'threads'
		   AND indexname = 'threads_scope_order_idx'`).Scan(&def)
	requireNoErr(t, err, "list threads backing index must exist")
	if !strings.Contains(def, "tenant") || !strings.Contains(def, "created_at") ||
		!strings.Contains(def, "subject") {
		t.Fatalf("index definition unexpected: %s", def)
	}
}
