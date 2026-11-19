package identity

// Wire-level abort accounting and the root fixture, shared by the default
// availability legs (delegate_retry_budget_test.go) and the tagged R4-OPEN
// measurement legs (delegate_ssi_realpath_test.go, -tags r4_open_measure). One
// definition for both builds, so a measurement cannot drift between them.
//
// Why a wire counter: every Identity write runs SERIALIZABLE and runTxMode
// replays 40001/40P01 in silence, so a caller never observes an abort no matter
// how often the database raises one. Counting at the wire is the only way to see
// what the retry loop absorbs; counting returned errors would report zero for a
// store that aborts on every attempt but the last. A leg that cannot show its own
// contention is a leg that can pass for the wrong reason.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// wireAborts tallies the two PostgreSQL states that prove the database aborted a
// transaction, per statement, including statements inside attempts the retry
// loop later replays.
type wireAborts struct {
	serialization atomic.Int64
	deadlock      atomic.Int64
	statements    atomic.Int64

	mu     sync.Mutex
	sample map[string]int
}

// sampleStatement records the first statements the database aborted, so the
// shape that costs the most is named by the measurement rather than by reading
// the code and guessing.
func (w *wireAborts) sampleStatement(sql string, code string) {
	oneLine := strings.Join(strings.Fields(sql), " ")
	if len(oneLine) > 110 {
		oneLine = oneLine[:110]
	}
	key := code + " " + oneLine
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sample == nil {
		w.sample = map[string]int{}
	}
	w.sample[key]++
}

func (w *wireAborts) samples() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, 0, len(w.sample))
	for key, n := range w.sample {
		out = append(out, fmt.Sprintf("%d x %s", n, key))
	}
	sort.Strings(out)
	return out
}

func (w *wireAborts) observe(sql string, err error) {
	if err == nil {
		return
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		if pgErr.Code == "40001" || pgErr.Code == "40P01" {
			w.sampleStatement(sql, pgErr.Code)
		}
		switch pgErr.Code {
		case "40001":
			w.serialization.Add(1)
			return
		case "40P01":
			w.deadlock.Add(1)
			return
		}
	}
}

func (w *wireAborts) total() int64 { return w.serialization.Load() + w.deadlock.Load() }

type abortTracer struct{ w *wireAborts }

// tracedStatementKey carries the statement text from TraceQueryStart to
// TraceQueryEnd, where the abort is reported without the SQL alongside it.
type tracedStatementKey struct{}

func (abortTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, tracedStatementKey{}, data.SQL)
}

func (t abortTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	t.w.statements.Add(1)
	sql, _ := ctx.Value(tracedStatementKey{}).(string)
	t.w.observe(sql, data.Err)
}

// tracedStore returns a Store on its own pool whose every statement passes
// through an abort counter. The pool is bound to the same per-run database
// isolatedPool provisions, so the substrate stays private to this run.
func tracedStore(t *testing.T) (*Store, *wireAborts) {
	t.Helper()
	return tracedStoreRole(t, "default")
}

// tracedStoreRole is tracedStore with the role written into the application_name,
// so a role's aborts can be attributed at the wire level rather than inferred.
func tracedStoreRole(t *testing.T, role string) (*Store, *wireAborts) {
	t.Helper()
	_, note := isolatedPool(t)
	if isolateCfg == nil {
		t.Skip("identity: no usable test database configuration")
	}
	t.Logf("identity R4 substrate: %s", note)
	cfg := isolateCfg.Copy()
	counted := &wireAborts{}
	cfg.ConnConfig.Tracer = abortTracer{w: counted}
	cfg.ConnConfig.RuntimeParams["application_name"] = "identity-r4-" + role
	if cfg.MaxConns < 16 {
		cfg.MaxConns = 16
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Skipf("identity: pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("identity: PostgreSQL not reachable (%v)", err)
	}
	t.Cleanup(pool.Close)
	store := New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("identity: migrate: %v", err)
	}
	return store, counted
}

// reportAborts prints the before/after numbers in a form the report can quote.
func reportAborts(t *testing.T, shape string, w *wireAborts, calls, failures int64, elapsed time.Duration) {
	t.Helper()
	t.Logf("R4_REALPATH shape=%s calls=%d caller_failures=%d wire_40001=%d wire_40P01=%d wire_aborts_total=%d statements=%d elapsed_ms=%d",
		shape, calls, failures, w.serialization.Load(), w.deadlock.Load(), w.total(), w.statements.Load(), elapsed.Milliseconds())
	for _, sample := range w.samples() {
		t.Logf("R4_ABORT_STATEMENT shape=%s %s", shape, sample)
	}
}

// noRetryableSurface fails when a caller was handed a state the store is
// supposed to absorb by replay: a caller-visible 40001/40P01 means the retry
// budget was exhausted, i.e. contention became an outage.
func noRetryableSurface(t *testing.T, shape string, err error) {
	t.Helper()
	if err != nil && retryableAbort(err) {
		t.Errorf("R4-OPEN-3 %s: a caller was handed a raw serialization failure after the retry budget of maxTxAttempts was exhausted: %v", shape, err)
	}
}

func createRootForTest(t *testing.T, s *Store, scope contracts.Scope, task, contextID string) Grant {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	grant, err := s.CreateRoot(ctx, scope, RootRequest{
		Task:         task,
		Context:      contextID,
		Deployment:   scope.Deployment,
		Capabilities: []string{"read"},
		TTL:          time.Hour,
		MaxFanout:    MaxTreeFanout,
		MaxActive:    MaxTreeActive,
		AllowSelf:    true,
	})
	if err != nil {
		t.Fatalf("CreateRoot(%s): %v", task, err)
	}
	return grant
}
