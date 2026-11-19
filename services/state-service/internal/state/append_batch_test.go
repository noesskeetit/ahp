package state

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type appendBatchTrace struct {
	mu                    sync.Mutex
	groups                []int
	closed                int
	insertBeforeReadClose bool
}

func (t *appendBatchTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceQueryStartData) context.Context {
	if strings.HasPrefix(strings.TrimSpace(d.SQL), "INSERT INTO ahp_state.stream_events") {
		t.mu.Lock()
		t.insertBeforeReadClose = t.closed != 3
		t.mu.Unlock()
	}
	return ctx
}
func (*appendBatchTrace) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
func (t *appendBatchTrace) TraceBatchStart(ctx context.Context, _ *pgx.Conn, d pgx.TraceBatchStartData) context.Context {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.groups = append(t.groups, d.Batch.Len())
	for _, q := range d.Batch.QueuedQueries {
		if !strings.HasPrefix(strings.TrimSpace(q.SQL), "SELECT") {
			t.insertBeforeReadClose = true
		}
	}
	return ctx
}
func (*appendBatchTrace) TraceBatchQuery(context.Context, *pgx.Conn, pgx.TraceBatchQueryData) {}
func (t *appendBatchTrace) TraceBatchEnd(context.Context, *pgx.Conn, pgx.TraceBatchEndData) {
	t.mu.Lock()
	t.closed++
	t.mu.Unlock()
}

func appendBatchStore(t *testing.T, admin *Store, trace *appendBatchTrace, app string) *Store {
	t.Helper()
	cfg := admin.pool.Config().Copy()
	cfg.ConnConfig.Tracer = trace
	cfg.ConnConfig.RuntimeParams["application_name"] = app
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	var actualApp string
	if err = pool.QueryRow(context.Background(), `SELECT current_setting('application_name')`).Scan(&actualApp); err != nil {
		t.Fatal(err)
	}
	if actualApp != app {
		t.Fatal("fixture application_name not applied")
	}
	return New(pool)
}

func appendBatchApp(prefix string) string {
	id := contracts.NewID()
	return prefix + id[len(id)-20:] // PostgreSQL application_name is at most63bytes.
}

func TestAppendStreamUsesReadOnlyBatches_RealState(t *testing.T) {
	admin, scope, l, _ := appendLeaseFixture(t)
	trace := &appendBatchTrace{}
	s := appendBatchStore(t, admin, trace, appendBatchApp("batch-"))
	if _, err := s.AppendStream(context.Background(), scope, l.Run.ID, l.Worker, l.Epoch, "piece", "model_delta", json.RawMessage(`"result0000 "`)); err != nil {
		t.Fatal(err)
	}
	trace.mu.Lock()
	defer trace.mu.Unlock()
	if !reflect.DeepEqual(trace.groups, []int{4, 2, 2}) || trace.closed != 3 || trace.insertBeforeReadClose {
		t.Fatalf("fresh append requires3closed read-only SendBatch groups4/2/2 beforeINSERT; groups=%v closed=%d premature_mutation=%t", trace.groups, trace.closed, trace.insertBeforeReadClose)
	}
}

func waitAppendBlocked(t *testing.T, s *Store, app string, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("AppendStream returned before controlled lock: code=%s sqlstate=%s", contracts.Code(err), appendProjectionSQLState(err))
		default:
		}
		var blocked bool
		if err := s.pool.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock')`, app).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("controlled AppendStream did not reach PostgreSQL lock wait")
}

func TestAppendStreamBatchSeesEraseCommittedWhileLockWaits_RealState(t *testing.T) {
	for _, tenant := range []bool{false, true} {
		name := "subject"
		if tenant {
			name = "tenant"
		}
		t.Run(name, func(t *testing.T) {
			admin, scope, l, _ := appendLeaseFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			blocker, err := admin.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer blocker.Rollback(context.Background())
			key := subjectFenceKey(scope)
			if tenant {
				key = tenantFenceKey(scope.Tenant)
			}
			if _, err = blocker.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, key); err != nil {
				t.Fatal(err)
			}
			app := appendBatchApp("erase-wait-")
			s := appendBatchStore(t, admin, &appendBatchTrace{}, app)
			done := make(chan error, 1)
			go func() {
				_, e := s.AppendStream(ctx, scope, l.Run.ID, l.Worker, l.Epoch, "refused", "model_delta", json.RawMessage(`"result0000 "`))
				done <- e
			}()
			waitAppendBlocked(t, admin, app, done)
			if tenant {
				_, err = blocker.Exec(ctx, `INSERT INTO ahp_state.erased_tenants(tenant) VALUES($1)`, scope.Tenant)
			} else {
				_, err = blocker.Exec(ctx, `INSERT INTO ahp_state.erased_subjects(tenant,subject) VALUES($1,$2)`, scope.Tenant, scope.Subject)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err = blocker.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err = <-done; contracts.Code(err) != contracts.ErrForbidden {
				t.Fatalf("post-lock erasure must be visible, got code=%s", contracts.Code(err))
			}
			var count int
			if err = admin.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_state.stream_events WHERE thread_id=$1`, l.Thread.ID).Scan(&count); err != nil || count != 0 {
				t.Fatal("erased subject received a delta")
			}
		})
	}
}

func TestAppendStreamBatchChecksDatabaseClockAfterRowLock_RealState(t *testing.T) {
	admin, scope, l, _ := appendLeaseFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	blocker, err := admin.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `SELECT id FROM ahp_state.threads WHERE id=$1 FOR UPDATE`, l.Thread.ID); err != nil {
		t.Fatal(err)
	}
	app := appendBatchApp("clock-wait-")
	s := appendBatchStore(t, admin, &appendBatchTrace{}, app)
	done := make(chan error, 1)
	go func() {
		_, e := s.AppendStream(ctx, scope, l.Run.ID, l.Worker, l.Epoch, "expired", "model_delta", json.RawMessage(`"result0000 "`))
		done <- e
	}()
	waitAppendBlocked(t, admin, app, done)
	var expires time.Time
	if err = blocker.QueryRow(ctx, `UPDATE ahp_state.threads SET lease_expires=clock_timestamp()+interval '150 milliseconds' WHERE id=$1 RETURNING lease_expires`, l.Thread.ID).Scan(&expires); err != nil {
		t.Fatal(err)
	}
	for {
		var expired bool
		if err = blocker.QueryRow(ctx, `SELECT clock_timestamp()>$1::timestamptz`, expires).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; contracts.Code(err) != contracts.ErrStaleEpoch {
		t.Fatalf("clock sampled before row wait admitted an expired lease: code=%s", contracts.Code(err))
	}
	var count int
	if err = admin.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_state.stream_events WHERE thread_id=$1`, l.Thread.ID).Scan(&count); err != nil || count != 0 {
		t.Fatal("expired lease wrote delta")
	}
}
