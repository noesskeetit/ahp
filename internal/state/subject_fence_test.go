package state

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pauseThreadInsert struct{ entered, release chan struct{} }

func (p *pauseThreadInsert) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "INSERT INTO ahp_state.threads") {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	}
	return ctx
}
func (*pauseThreadInsert) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestSubjectFenceDrainsUncommittedThreadCreation(t *testing.T) {
	testFenceDrainsUncommittedThreadCreation(t, false)
}
func TestTenantFenceDrainsUncommittedThreadCreation(t *testing.T) {
	testFenceDrainsUncommittedThreadCreation(t, true)
}
func testFenceDrainsUncommittedThreadCreation(t *testing.T, tenant bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	observer, _ := isolatedPool(t)
	st := New(observer)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	pause := &pauseThreadInsert{make(chan struct{}), make(chan struct{})}
	cfg := isolatedConfig(t)
	cfg.MaxConns = 1
	cfg.ConnConfig.Tracer = pause
	writerPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("writer pool: %T", err)
	}
	defer writerPool.Close()
	eraseCfg := isolatedConfig(t)
	eraseCfg.MaxConns = 1
	eraserPool, err := pgxpool.NewWithConfig(ctx, eraseCfg)
	if err != nil {
		t.Fatalf("eraser pool: %T", err)
	}
	defer eraserPool.Close()
	var pid int64
	if err = eraserPool.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: "fence-" + contracts.NewID(), Deployment: "inflight", Subject: "alice"}
	written := make(chan error, 1)
	go func() { _, err := New(writerPool).CreateThread(ctx, scope, "before-fence", "writer"); written <- err }()
	<-pause.entered
	fenced := make(chan error, 1)
	go func() {
		if tenant {
			fenced <- New(eraserPool).FenceTenant(ctx, scope.Tenant)
		} else {
			fenced <- New(eraserPool).FenceSubject(ctx, scope)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		select {
		case err := <-fenced:
			close(pause.release)
			<-written
			t.Fatalf("fence acknowledged before admitted writer committed: %v", err)
		default:
		}
		if err = observer.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid).Scan(&waiting); err != nil {
			close(pause.release)
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(pause.release)
	if !waiting {
		t.Fatal("eraser did not wait on the admitted transaction")
	}
	if err = <-written; err != nil {
		t.Fatal(err)
	}
	if err = <-fenced; err != nil {
		t.Fatal(err)
	}
	scopes, err := st.SubjectScopes(ctx, scope.Tenant, scope.Subject)
	if err != nil || len(scopes) != 1 {
		t.Fatalf("post-fence discovery missed admitted write: %v %v", scopes, err)
	}
	if _, err = st.DeleteSubjectWithKey(ctx, scope, "erase"); err != nil {
		t.Fatal(err)
	}
	scope.Deployment = "late"
	if _, err = st.CreateThread(ctx, scope, "after-fence", "late"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("post-fence write: %v", err)
	}
}

func TestSubjectFenceStopsHeldStateWritersAndClaims(t *testing.T) {
	s := intentStore(t)
	ctx := context.Background()
	scope, run, lease := intentFixture(t, s)
	pending, err := s.CreateThread(ctx, scope, "pending-context", "pending-thread")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateRun(ctx, scope, pending.ID, "pending-task", "pending-run", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err = s.SetLegalhold(ctx, scope, "", "preserve existing content"); err != nil {
		t.Fatal(err)
	}
	if err = s.FenceSubject(ctx, scope); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateRun(ctx, scope, pending.ID, "late-task", "late-run", json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("CreateRun=%v", err)
	}
	if _, err = s.Enqueue(ctx, scope, pending.ID, "late-inbox", "user", json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("Enqueue=%v", err)
	}
	f := Fence{RunID: run.ID, Worker: lease.Worker, Epoch: lease.Epoch}
	if _, err = s.PutKV(ctx, scope, f, nil, "late", "late-kv", 0, json.RawMessage(`{"secret":"late"}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("PutKV=%v", err)
	}
	if _, err = s.PutCheckpoint(ctx, scope, f, Checkpoint{ID: "late", Namespace: "main", Version: 1}, "late-cp"); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("PutCheckpoint=%v", err)
	}
	if _, err = s.AppendStream(ctx, scope, run.ID, lease.Worker, lease.Epoch, "late-event", "token", json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("AppendStream=%v", err)
	}
	if _, err = s.RecordIntent(ctx, scope, run.ID, lease.Worker, lease.Epoch, "n", "call", "http", "remote", json.RawMessage(`{}`)); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("RecordIntent=%v", err)
	}
	if _, err = s.DeleteSubjectWithKey(ctx, scope, "held-delete"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetThread(ctx, scope, lease.Thread.ID); err != nil {
		t.Fatalf("held content lost: %v", err)
	}
	foreign := scope
	foreign.Subject = "other-human"
	foreignThread, err := s.CreateThread(ctx, foreign, "foreign-context", "foreign-thread")
	if err != nil {
		t.Fatal(err)
	}
	foreignRun, err := s.CreateRun(ctx, foreign, foreignThread.ID, "foreign-task", "foreign-run", json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "another-worker", time.Minute)
	if err != nil || claimed == nil || claimed.Run.ID != foreignRun.ID {
		t.Fatalf("claim did not skip erased subject: %+v %v", claimed, err)
	}
}
