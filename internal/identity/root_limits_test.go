package identity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// pgxError builds a PostgreSQL wire error with the given sqlstate.
func pgxError(code string) error {
	return &pgconn.PgError{Code: code, Message: "test-sqlstate-" + code}
}

func TestNormalizeLimits(t *testing.T) {
	cases := []struct {
		name                           string
		in                             RootRequest
		wantDepth, wantFan, wantActive int
		wantCode                       string
	}{
		{name: "zero selects defaults", wantDepth: DefaultMaxDepth, wantFan: DefaultMaxFanout, wantActive: DefaultMaxActive},
		{name: "explicit defaults equal zero", in: RootRequest{MaxDepth: 4, MaxFanout: 32, MaxActive: 256},
			wantDepth: 4, wantFan: 32, wantActive: 256},
		{name: "hard ceilings accepted", in: RootRequest{MaxDepth: 16, MaxFanout: 1024, MaxActive: 100000},
			wantDepth: 16, wantFan: 1024, wantActive: 100000},
		{name: "narrower accepted", in: RootRequest{MaxDepth: 1, MaxFanout: 1, MaxActive: 1},
			wantDepth: 1, wantFan: 1, wantActive: 1},
		{name: "depth above ceiling", in: RootRequest{MaxDepth: 17}, wantCode: contracts.ErrInvalid},
		{name: "depth negative", in: RootRequest{MaxDepth: -1}, wantCode: contracts.ErrInvalid},
		{name: "fanout above ceiling", in: RootRequest{MaxFanout: 1025}, wantCode: contracts.ErrInvalid},
		{name: "fanout negative", in: RootRequest{MaxFanout: -3}, wantCode: contracts.ErrInvalid},
		{name: "active above ceiling", in: RootRequest{MaxActive: 100001}, wantCode: contracts.ErrInvalid},
		{name: "active negative", in: RootRequest{MaxActive: -1}, wantCode: contracts.ErrInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, f, a, err := normalizeLimits(tc.in)
			if tc.wantCode != "" {
				if err == nil || contracts.Code(err) != tc.wantCode {
					t.Fatalf("want code %s, got %v", tc.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizeLimits: %v", err)
			}
			if d != tc.wantDepth || f != tc.wantFan || a != tc.wantActive {
				t.Errorf("got (%d,%d,%d), want (%d,%d,%d)", d, f, a, tc.wantDepth, tc.wantFan, tc.wantActive)
			}
		})
	}
}

func TestRootDigestNormalizesLimits(t *testing.T) {
	scope := uniqueScope(t)
	base := RootRequest{Task: "t", Context: "c", Deployment: scope.Deployment}
	caps := []string{"read"}

	zero, err := rootDigest(scope, base, caps, time.Hour, DefaultMaxDepth, DefaultMaxFanout, DefaultMaxActive, false)
	if err != nil {
		t.Fatalf("rootDigest: %v", err)
	}
	explicit, err := rootDigest(scope, base, caps, time.Hour, 4, 32, 256, false)
	if err != nil {
		t.Fatalf("rootDigest: %v", err)
	}
	if zero != explicit {
		t.Errorf("omitted and explicit default limits must digest identically")
	}
	for _, mut := range []struct {
		d, f, a int
		self    bool
	}{
		{5, 32, 256, false},
		{4, 33, 256, false},
		{4, 32, 257, false},
		{4, 32, 256, true},
	} {
		other, err := rootDigest(scope, base, caps, time.Hour, mut.d, mut.f, mut.a, mut.self)
		if err != nil {
			t.Fatalf("rootDigest: %v", err)
		}
		if other == zero {
			t.Errorf("limits %+v must change the digest", mut)
		}
	}
}

func TestCreateRootLimitsDefaultAndCeilings(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	def, err := s.CreateRoot(ctx, scope,
		rootRequest("task-lim-default", "ctx-lim", scope.Deployment, time.Hour, "read"))
	if err != nil {
		t.Fatalf("CreateRoot defaults: %v", err)
	}
	if def.MaxDepth != DefaultMaxDepth || def.MaxFanout != DefaultMaxFanout ||
		def.MaxActive != DefaultMaxActive || def.AllowSelf {
		t.Errorf("defaults not applied: depth=%d fanout=%d active=%d self=%v",
			def.MaxDepth, def.MaxFanout, def.MaxActive, def.AllowSelf)
	}

	// Replaying with the explicit defaults is the same canonical request.
	replay, err := s.CreateRoot(ctx, scope, func() RootRequest {
		r := rootRequest("task-lim-default", "ctx-lim", scope.Deployment, time.Hour, "read")
		r.MaxDepth, r.MaxFanout, r.MaxActive = 4, 32, 256
		return r
	}())
	if err != nil {
		t.Fatalf("replay with explicit defaults: %v", err)
	}
	if replay.ID != def.ID {
		t.Errorf("explicit defaults replayed as a new grant: %q vs %q", replay.ID, def.ID)
	}

	maxed, err := s.CreateRoot(ctx, scope, func() RootRequest {
		r := rootRequest("task-lim-max", "ctx-lim", scope.Deployment, time.Hour, "read")
		r.MaxDepth, r.MaxFanout, r.MaxActive, r.AllowSelf = 16, 1024, 100000, true
		return r
	}())
	if err != nil {
		t.Fatalf("CreateRoot ceilings: %v", err)
	}
	if maxed.MaxDepth != 16 || maxed.MaxFanout != 1024 || maxed.MaxActive != 100000 || !maxed.AllowSelf {
		t.Errorf("ceilings not stored: %+v", maxed)
	}
}

func TestCreateRootInvalidLimits(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	for _, mut := range []func(*RootRequest){
		func(r *RootRequest) { r.MaxDepth = 17 },
		func(r *RootRequest) { r.MaxDepth = -1 },
		func(r *RootRequest) { r.MaxFanout = 1025 },
		func(r *RootRequest) { r.MaxFanout = -2 },
		func(r *RootRequest) { r.MaxActive = 100001 },
		func(r *RootRequest) { r.MaxActive = -1 },
	} {
		r := rootRequest("task-lim-invalid", "ctx-lim", scope.Deployment, time.Hour, "read")
		mut(&r)
		_, err := s.CreateRoot(ctx, scope, r)
		assertCode(t, err, contracts.ErrInvalid)
	}
}

func TestCreateRootChangedLimitsKeyReuse(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope,
		rootRequest("task-lim-reuse", "ctx-lim", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	r := rootRequest("task-lim-reuse", "ctx-lim", scope.Deployment, time.Hour, "read")
	r.MaxDepth = 8
	_, err := s.CreateRoot(ctx, scope, r)
	assertCode(t, err, contracts.ErrKeyReuse)

	r = rootRequest("task-lim-reuse", "ctx-lim", scope.Deployment, time.Hour, "read")
	r.AllowSelf = true
	_, err = s.CreateRoot(ctx, scope, r)
	assertCode(t, err, contracts.ErrKeyReuse)

	got, err := s.Get(ctx, scope, "task-lim-reuse")
	if err != nil {
		t.Fatalf("Get after key reuse: %v", err)
	}
	if got.MaxDepth != DefaultMaxDepth || got.AllowSelf {
		t.Errorf("original grant mutated by mismatched retry: depth=%d self=%v", got.MaxDepth, got.AllowSelf)
	}
}

func TestCreateRootConcurrentIdentical(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	req := rootRequest("task-conc", "ctx-conc", scope.Deployment, time.Hour, "read")

	const workers = 20
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		ids  = make([]string, 0, workers)
		errs []error
	)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			g, err := s.CreateRoot(ctx, scope, req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids = append(ids, g.ID)
		}()
	}
	close(start)
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("concurrent identical CreateRoot errors: %v", errs)
	}
	if len(ids) != workers {
		t.Fatalf("collected %d ids, want %d", len(ids), workers)
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("concurrent identical creates produced distinct grants: %q vs %q", id, ids[0])
		}
	}
}

func TestCreateRootDistinctRootsNoConflict(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	const workers = 8
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		ids  = make(map[string]string, workers)
		errs []error
	)
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			// Distinct task ids and distinct context journals: none of these
			// roots may conflict with another, even created concurrently.
			g, err := s.CreateRoot(ctx, scope,
				rootRequest("task-distinct-"+string(rune('a'+i)),
					"ctx-distinct-"+string(rune('a'+i)), scope.Deployment, time.Hour, "read"))
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			ids[g.Task] = g.ID
		}(i)
	}
	close(start)
	wg.Wait()
	if len(errs) > 0 {
		t.Fatalf("concurrent distinct-root creates errored: %v", errs)
	}
	if len(ids) != workers {
		t.Fatalf("created %d distinct roots, want %d", len(ids), workers)
	}
	seen := make(map[string]bool, workers)
	for task, id := range ids {
		if seen[id] {
			t.Fatalf("distinct tasks %s share grant id %q", task, id)
		}
		seen[id] = true
	}
}

func TestRunTxModeRetriesOnlyKnownAborts(t *testing.T) {
	if !retryableAbort(pgxError("40001")) {
		t.Errorf("40001 serialization_failure must be retried")
	}
	if !retryableAbort(pgxError("40P01")) {
		t.Errorf("40P01 deadlock_detected must be retried")
	}
	for _, code := range []string{"23505", "23503", "23514", "57P01", "08006", ""} {
		if retryableAbort(pgxError(code)) {
			t.Errorf("sqlstate %s must never be retried", code)
		}
	}
	if retryableAbort(errors.New("identity: unrelated failure")) {
		t.Errorf("an error without a sqlstate must never be retried")
	}
}

func TestRunTxModeRetryLoop(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)

	if err := s.runTxMode(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable},
		func(context.Context, pgx.Tx) error { return pgx.ErrNoRows }); err == nil {
		t.Fatalf("unknown error must surface, not retry")
	}

	runs := 0
	err := s.runTxMode(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable},
		func(context.Context, pgx.Tx) error {
			runs++
			if runs < 3 {
				return pgxError("40001")
			}
			return nil
		})
	if err != nil {
		t.Fatalf("retryable aborts must be retried to success: %v", err)
	}
	if runs != 3 {
		t.Fatalf("ran %d times, want 3", runs)
	}

	// A persistent abort must stop, and the thing that stops it is the replay
	// budget in TIME, not the safety valve. maxTxAttempts exists so a store that
	// aborts instantaneously forever cannot spin unboundedly inside its budget;
	// asserting it as the stopping count used to pin the wrong rule (a legitimate
	// writer gave up at 16 attempts with its caller's deadline minutes away).
	runs = 0
	budgeted := time.Now()
	err = s.runTxMode(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable},
		func(context.Context, pgx.Tx) error {
			runs++
			return pgxError("40P01")
		})
	elapsed := time.Since(budgeted)
	if err == nil {
		t.Fatalf("persistent abort must eventually surface")
	}
	if !errors.Is(err, errRetryBudgetExhausted) {
		t.Fatalf("persistent abort must surface as a budget-exhausted CONFLICT a caller can branch on, got %v", err)
	}
	if contracts.Code(err) != contracts.ErrConflict {
		t.Fatalf("budget exhaustion must keep the CONFLICT class clients replay on, got %q", contracts.Code(err))
	}
	if runs >= maxTxAttempts {
		t.Fatalf("ran %d times: the time budget must end the loop before the safety valve of %d does", runs, maxTxAttempts)
	}
	if elapsed > txReplayBudget+time.Second {
		t.Fatalf("replay outlived its budget: %s for a budget of %s (%d attempts)", elapsed, txReplayBudget, runs)
	}
	t.Logf("R4_RETRY_LOOP persistent abort: stopped after %d attempts in %s on a budget of %s (valve %d)",
		runs, elapsed.Round(time.Millisecond), txReplayBudget, maxTxAttempts)

	cancelled, cancel := context.WithCancel(context.Background())
	ran := 0
	err = s.runTxMode(cancelled, pgx.TxOptions{IsoLevel: pgx.Serializable},
		func(context.Context, pgx.Tx) error {
			ran++
			cancel() // abort before the backoff of the first retry
			return pgxError("40001")
		})
	if err == nil {
		t.Fatalf("cancelled context must abort the retry loop")
	}
	if ran != 1 {
		t.Fatalf("ran %d times after cancellation, want the first attempt only", ran)
	}
}
