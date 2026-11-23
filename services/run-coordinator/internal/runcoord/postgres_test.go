package runcoord

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/statejournal"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5/pgxpool"
)

type databaseState struct {
	scope contracts.Scope
	store *statejournal.Store
}

func (db databaseState) EnqueueTurn(ctx context.Context, r journal.EnqueueRequest) (journal.EnqueueReceipt, error) {
	return db.store.EnqueueTurn(ctx, db.scope, r)
}
func (db databaseState) GetTurnStatus(ctx context.Context, s string) (journal.TurnStatus, error) {
	return db.store.GetTurnStatus(ctx, db.scope, s)
}
func (db databaseState) InspectSession(ctx context.Context, s string) (journal.ResumeState, error) {
	return db.store.InspectSession(ctx, db.scope, s)
}

func (db databaseState) GetEvents(ctx context.Context, s string, after int64, n int, _ string) (journal.EventPage, error) {
	return db.store.GetEvents(ctx, db.scope, s, after, n)
}
func (db databaseState) CancelSession(ctx context.Context, s string, t int64) (journal.TurnStatus, error) {
	return db.store.CancelTurn(ctx, db.scope, s, t)
}
func (db databaseState) ListRecoverableVMPage(ctx context.Context, status string, cursor *journal.VMReconcileCursor) (journal.VMReconcilePage, error) {
	return db.store.ListRecoverableVMPage(ctx, db.scope, status, cursor, 64)
}
func (db databaseState) RequeueFencedVMTurn(ctx context.Context, s string, r journal.FenceRequeueRequest) error {
	return db.store.RequeueFencedVMTurn(ctx, db.scope, s, r)
}

func openPG(t *testing.T) (context.Context, *pgxpool.Pool, databaseState) {
	t.Helper()
	dsn := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires an isolated PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(cfg.ConnConfig.Database, "ahp_carbon_compact") {
		t.Fatal("use an isolated test database")
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db := databaseState{store: statejournal.New(pool), scope: contracts.Scope{Tenant: "runcoord-" + rand.Text(), Deployment: "agent", Subject: "user"}}
	if err := db.store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, pool, db
}

type fakeVM struct {
	mu     sync.Mutex
	live   map[string]Execution
	boots  map[string]int
	events []string
}

func (d *fakeVM) EnsureRunning(_ context.Context, e Execution) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.live == nil {
		d.live = make(map[string]Execution)
		d.boots = make(map[string]int)
	}
	if _, ok := d.live[e.ExecutionID]; !ok {
		d.live[e.ExecutionID] = e
		d.boots[e.ExecutionID]++
		d.events = append(d.events, "start:"+e.ExecutionID)
	}
	return nil
}
func (d *fakeVM) Fence(_ context.Context, e Execution) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.live, e.ExecutionID)
	d.events = append(d.events, "fence:"+e.ExecutionID)
	return nil
}

func TestQueuedAndActiveVMReconcilePostgres(t *testing.T) {
	ctx, pool, db := openPG(t)
	admission, err := db.EnqueueTurn(ctx, journal.EnqueueRequest{
		SessionID: "s", Harness: "go", FormatVersion: 1, Key: "ingress-commit",
		ExecutionMode: journal.ExecutionVMManaged, Input: journal.Payload{Inline: []byte(`{"message":"go"}`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE ahp_journal.sessions SET queued_at=now()-interval '3 seconds' WHERE tenant_id=$1", db.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	vm := &fakeVM{}
	first := &Coordinator{Scope: db.scope, State: db, Dispatcher: vm}
	second := &Coordinator{Scope: db.scope, State: db, Dispatcher: vm}
	var group sync.WaitGroup
	for _, c := range []*Coordinator{first, second} {
		group.Go(func() {
			if err := c.Reconcile(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	firstID := journal.DeterministicExecutionID(db.scope, "s", admission.HeadSeq, 1)
	if vm.boots[firstID] != 1 || len(vm.live) != 1 {
		t.Fatalf("duplicate startup: %+v", vm.boots)
	}
	delete(vm.live, firstID)
	if err := second.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if vm.boots[firstID] != 2 || len(vm.live) != 1 {
		t.Fatalf("failed startup did not recover: %+v", vm.boots)
	}
	turn, err := db.store.ClaimVMTurn(ctx, db.scope, "s", journal.VMTurnClaimRequest{TurnSeq: admission.HeadSeq, Generation: 1, ExecutionID: firstID}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	digest := journal.SHA256Digest{1}
	_, err = db.store.AppendEvents(ctx, db.scope, "s", turn.Lease, false, journal.AppendRequest{
		Key: "intent", TurnSeq: turn.InputSeq, ExpectedSeq: turn.HeadSeq,
		Intents: []journal.PreparedIntent{{CallID: "send-mail", ToolName: "mail", ArgsDigest: digest, IdempotencyKey: "send-mail-once"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tool := journal.StandaloneToolRequest{TurnSeq: turn.InputSeq, CallID: "send-mail", ToolName: "mail", ArgsDigest: digest}
	attempt, err := db.store.BeginStandaloneTool(ctx, db.scope, "s", turn.Lease, tool)
	if err != nil || !attempt.CanExecute {
		t.Fatalf("first tool admission: %+v %v", attempt, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE ahp_journal.sessions SET lease_expires_at=now() WHERE tenant_id=$1", db.scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if err := first.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	secondID := journal.DeterministicExecutionID(db.scope, "s", turn.InputSeq, 2)
	tail := vm.events[len(vm.events)-2:]
	if strings.Join(tail, ",") != "fence:"+firstID+",start:"+secondID {
		t.Fatalf("recovery order: %v", vm.events)
	}
	next, err := db.store.ClaimVMTurn(ctx, db.scope, "s", journal.VMTurnClaimRequest{TurnSeq: turn.InputSeq, Generation: 2, ExecutionID: secondID}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	after, err := db.store.BeginStandaloneTool(ctx, db.scope, "s", next.Lease, tool)
	if err != nil || after.CanExecute || after.Status != statejournal.IntentUnknown {
		t.Fatalf("recovery repeated uncertain external operation: %+v %v", after, err)
	}
	if _, err := db.store.AppendEvents(ctx, db.scope, "s", turn.Lease, false, journal.AppendRequest{
		Key: "late-worker", TurnSeq: turn.InputSeq, ExpectedSeq: next.HeadSeq, FinishTurn: "IDLE",
	}); contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("expired owner wrote state: %v", err)
	}
}

func TestCoordinatorZeroMemoryGrowth(t *testing.T) {
	ctx, pool, db := openPG(t)
	bus := NewPGSignalBus(pool)
	defer func() {
		bus.mu.Lock()
		defer bus.mu.Unlock()
		if len(bus.subscribers) != 0 {
			t.Errorf("orphaned SSE subscribers: %d", len(bus.subscribers))
		}
	}()
	service := &Coordinator{Scope: db.scope, State: db, Signals: bus}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		service.stream(w, r, strings.TrimPrefix(r.URL.Path, "/"))
	}))
	defer server.Close()
	payload := journal.Payload{Inline: []byte(fmt.Sprintf(`{"message":"%s"}`, strings.Repeat("x", 100<<10)))}
	burst := func(start, end int) error {
		errs := make(chan error, end-start)
		var work sync.WaitGroup
		for begin := start; begin < end; begin += 16 {
			for i := begin; i < min(begin+16, end); i++ {
				work.Go(func() {
					session := fmt.Sprintf("s-%d", i)
					in, err := db.EnqueueTurn(ctx, journal.EnqueueRequest{
						SessionID: session, Harness: "go", FormatVersion: 1, Key: "start", Input: payload, ExecutionMode: journal.ExecutionVMManaged,
					})
					if err != nil {
						errs <- err
						return
					}
					turn, err := db.store.ClaimVMTurn(ctx, db.scope, session, journal.VMTurnClaimRequest{
						TurnSeq: in.HeadSeq, Generation: 1, ExecutionID: journal.DeterministicExecutionID(db.scope, session, in.HeadSeq, 1),
					}, time.Minute)
					if err != nil {
						errs <- err
						return
					}
					if _, err := db.store.AppendEvents(ctx, db.scope, session, turn.Lease, false, journal.AppendRequest{
						Key: "finish", ExpectedSeq: turn.HeadSeq, TurnSeq: turn.InputSeq, FinishTurn: "IDLE",
					}); err != nil {
						errs <- err
						return
					}
					request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/"+session+"?turn_seq=1", nil)
					if err != nil {
						errs <- err
						return
					}
					response, err := server.Client().Do(request)
					if err != nil {
						errs <- err
						return
					}
					defer response.Body.Close()
					body, err := io.ReadAll(io.LimitReader(response.Body, 200<<10))
					if err != nil {
						errs <- err
						return
					}
					if response.StatusCode != http.StatusOK || !strings.Contains(string(body), "event: end") {
						errs <- errors.New("SSE did not reach the end of the turn")
					}
				})
			}
			work.Wait()
		}
		close(errs)
		var failures []error
		for err := range errs {
			failures = append(failures, err)
		}
		return errors.Join(failures...)
	}
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	if err := burst(0, 200); err != nil {
		t.Fatal(err)
	}
	baseline := heap()
	if err := burst(200, 2000); err != nil {
		t.Fatal(err)
	}
	final := heap()
	t.Logf("200=%d bytes, 2000=%d bytes, change=%d bytes", baseline, final, int64(final)-int64(baseline))
	if final > baseline+2<<20 {
		t.Fatalf("coordinator heap grew by more than 2 MiB")
	}
}
