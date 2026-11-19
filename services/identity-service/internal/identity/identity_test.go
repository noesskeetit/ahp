package identity

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// newStore returns a migrated Store on a dedicated pool, or skips when
// AHP_TEST_DATABASE_URL is absent. The pool is closed by test cleanup.
//
// The pool comes from isolatedPool (isolation_test.go), which binds the whole
// test-binary run to a fresh database: grant/context ids in this package are
// unique across the schema and some are literal strings, so a second run
// against a shared database used to fail on rows the first run left behind.
func newStore(t *testing.T) *Store {
	t.Helper()
	pool, note := isolatedPool(t)
	t.Logf("identity test substrate: %s", note)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := New(pool)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("identity: migrate: %v", err)
	}
	return s
}

// uniqueScope derives a scope from the test name so parallel runs never share
// an idempotency key.
func uniqueScope(t *testing.T) contracts.Scope {
	suffix := sanitizeID(contracts.NewID())
	return contracts.Scope{
		Tenant:     "t-" + suffix,
		Deployment: "dep-" + suffix,
		Subject:    "subj-" + suffix,
	}
}

// sanitizeID keeps only valid scope characters from an id fragment.
func sanitizeID(s string) string {
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b = append(b, c)
		}
	}
	return string(b)
}

func rootRequest(task, contextID, deployment string, ttl time.Duration, caps ...string) RootRequest {
	return RootRequest{
		Task:         task,
		Context:      contextID + "-" + deployment,
		Deployment:   deployment,
		Capabilities: caps,
		TTL:          ttl,
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func assertCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error with code %s, got nil", want)
	}
	if got := contracts.Code(err); got != want {
		t.Fatalf("want code %s, got %q (err: %v)", want, got, err)
	}
}

func TestCreateRoot(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	grant, err := s.CreateRoot(ctx, scope,
		rootRequest("task-create", "ctx-create", scope.Deployment, time.Hour, "read", "write"))
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	if grant.ID == "" || grant.Root != grant.ID {
		t.Errorf("root grant must have Root == ID, got id=%q root=%q", grant.ID, grant.Root)
	}
	if grant.Parent != "" || grant.Depth != 0 || grant.Status != StatusActive {
		t.Errorf("unexpected root shape: parent=%q depth=%d status=%q", grant.Parent, grant.Depth, grant.Status)
	}
	if grant.Task != "task-create" || grant.Context != "ctx-create-"+scope.Deployment || grant.Subject != scope.Subject {
		t.Errorf("grant does not echo the request: %+v", grant)
	}
	want := []string{"read", "write"}
	if fmt.Sprint(grant.Capabilities) != fmt.Sprint(want) {
		t.Errorf("capabilities = %v, want %v", grant.Capabilities, want)
	}
	if d := time.Until(grant.ExpiresAt); d < 55*time.Minute || d > time.Hour+time.Minute {
		t.Errorf("ExpiresAt in %v, want ~1h", d)
	}

	got, err := s.Get(ctx, scope, "task-create")
	if err != nil {
		t.Fatalf("Get after create: %v", err)
	}
	if got.ID != grant.ID {
		t.Errorf("Get returned grant %q, created %q", got.ID, grant.ID)
	}
}

func TestCreateRootReplay(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	req := rootRequest("task-replay", "ctx-replay", scope.Deployment, time.Hour, "read")

	first, err := s.CreateRoot(ctx, scope, req)
	if err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	second, err := s.CreateRoot(ctx, scope, req)
	if err != nil {
		t.Fatalf("replayed CreateRoot: %v", err)
	}
	if first.ID != second.ID || first.Root != second.Root {
		t.Errorf("replay created a new grant: %q vs %q", first.ID, second.ID)
	}
	if !first.ExpiresAt.Equal(second.ExpiresAt) {
		t.Errorf("replay changed expiry: %v vs %v", first.ExpiresAt, second.ExpiresAt)
	}
}

func TestCreateRootKeyReuseMismatch(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope,
		rootRequest("task-mismatch", "ctx-a", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	_, err := s.CreateRoot(ctx, scope,
		rootRequest("task-mismatch", "ctx-a", scope.Deployment, time.Hour, "read", "write"))
	assertCode(t, err, contracts.ErrKeyReuse)

	// The mismatch must not have destroyed or replaced the original grant.
	got, err := s.Get(ctx, scope, "task-mismatch")
	if err != nil {
		t.Fatalf("Get after mismatch: %v", err)
	}
	if fmt.Sprint(got.Capabilities) != fmt.Sprint([]string{"read"}) {
		t.Errorf("original grant mutated by mismatched retry: %v", got.Capabilities)
	}
}

func TestGetCrossSubjectNotFound(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope,
		rootRequest("task-cross", "ctx-cross", scope.Deployment, time.Hour, "read")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	other := uniqueScope(t)
	other.Tenant = scope.Tenant
	other.Deployment = scope.Deployment
	_, err := s.Get(ctx, other, "task-cross")
	assertCode(t, err, contracts.ErrNotFound)
}

func TestEvaluate(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)
	if _, err := s.CreateRoot(ctx, scope,
		rootRequest("task-eval", "ctx-eval", scope.Deployment, time.Hour, "read", "write")); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}

	grant, err := s.Evaluate(ctx, scope, "task-eval", "read")
	if err != nil {
		t.Fatalf("Evaluate allowed capability: %v", err)
	}
	if grant.Task != "task-eval" {
		t.Errorf("Evaluate returned grant for task %q", grant.Task)
	}

	_, err = s.Evaluate(ctx, scope, "task-eval", "badcap")
	assertCode(t, err, contracts.ErrForbidden)
}

func TestCreateRootRevokedSubjectForbidden(t *testing.T) {
	s := newStore(t)
	ctx := testContext(t)
	scope := uniqueScope(t)

	pool := s.pool
	if _, err := pool.Exec(ctx,
		`INSERT INTO ahp_identity.subjects (tenant, subject) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		scope.Tenant, scope.Subject); err != nil {
		t.Fatalf("seed subject tombstone: %v", err)
	}

	_, err := s.CreateRoot(ctx, scope,
		rootRequest("task-revoked", "ctx-revoked", scope.Deployment, time.Hour, "read"))
	assertCode(t, err, contracts.ErrForbidden)

	var m *contracts.Error
	if errors.Is(err, m) {
		t.Fatalf("error must be a typed *contracts.Error, got %#v", err)
	}
}
