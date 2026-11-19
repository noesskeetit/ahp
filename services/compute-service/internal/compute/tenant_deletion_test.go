package compute

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestTenantDeletionStopsComputeAdmissionAndWaitsForIncarnation(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := New(pool)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	scope := contracts.Scope{Tenant: contracts.NewID(), Subject: "alice", Deployment: "dep"}
	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, "first")
	if err != nil {
		t.Fatal(err)
	}
	inc, err := s.AcquireIncarnation(ctx, scope, c.ID, "test", "node", "")
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := any(s).(interface {
		FenceTenant(context.Context, string) error
		DeleteTenant(context.Context, string) error
	})
	if !ok {
		t.Fatal("compute owner has no tenant deletion")
	}
	if err = owner.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, "late"); err == nil {
		t.Fatal("late computer admitted")
	}
	if err = owner.DeleteTenant(ctx, scope.Tenant); err == nil {
		t.Fatal("live incarnation erased")
	}
	if err = s.StopIncarnation(ctx, scope, c.ID, inc.ID); err != nil {
		t.Fatal(err)
	}
	if err = owner.DeleteTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetComputer(ctx, scope, c.ID); contracts.Code(err) != contracts.ErrNotFound {
		t.Fatalf("computer survives: %v", err)
	}
}

func tenantComputeFixture(t *testing.T) (*Store, contracts.Scope) {
	t.Helper()
	ctx := context.Background()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("AHP_TEST_DATABASE_URL required")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	s := New(pool)
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return s, contracts.Scope{Tenant: contracts.NewID(), Deployment: "compute", Subject: "alice"}
}
func TestTenantRunnerDeletesOwnedFilesAfterRedisLeaseDrain(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx := context.Background()
	root := t.TempDir()
	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfilePersonal}, "personal")
	if err != nil {
		t.Fatal(err)
	}
	foreign := scope
	foreign.Tenant += "-other"
	other, err := s.CreateComputer(ctx, foreign, NewComputerRequest{Class: ClassS, Profile: ProfilePersonal}, "personal")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{c.ID, other.ID} {
		dir := filepath.Join(root, "computers", id, "home")
		if err = os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(dir, "private.txt"), []byte(id), 0600); err != nil {
			t.Fatal(err)
		}
	}
	addr := os.Getenv("AHP_REDIS_ADDR")
	if addr == "" {
		t.Fatal("AHP_REDIS_ADDR required")
	}
	redis := NewRedis(addr)
	leases, err := NewLeaseService(s, redis)
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(s, NewRegistry(), root, "test", nil).WithLeaseService(leases)
	if _, err = leases.AcquireWriter(ctx, scope, c.ID, "writer", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err = runner.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = leases.AcquireWriter(ctx, scope, c.ID, "late", time.Minute); contracts.Code(err) != contracts.ErrForbidden {
		t.Fatalf("late Redis writer: %v", err)
	}
	if err = runner.DeleteTenant(ctx, scope.Tenant); err == nil {
		t.Fatal("live writer data removed")
	}
	if err = leases.ReleaseWriter(ctx, scope, c.ID, "writer"); err != nil {
		t.Fatal(err)
	}
	if err = runner.DeleteTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(root, "computers", c.ID)); !os.IsNotExist(err) {
		t.Fatalf("owned bytes survive: %v", err)
	}
	if _, err = os.Stat(filepath.Join(root, "computers", other.ID, "home", "private.txt")); err != nil {
		t.Fatalf("foreign bytes harmed: %v", err)
	}
}
func TestTenantRunnerRejectsSnapshotOutsideItsComputer(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx := context.Background()
	root := t.TempDir()
	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfilePersonal}, "personal")
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(root, "computers", "foreign", "secret")
	if err = os.MkdirAll(filepath.Dir(outside), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(outside, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = s.RecordSnapshot(ctx, scope, c.ID, SnapshotKind("directory"), outside, "digest", 7); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(s, NewRegistry(), root, "test", nil)
	if err = runner.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if err = runner.DeleteTenant(ctx, scope.Tenant); err == nil {
		t.Fatal("foreign snapshot path accepted")
	}
	if _, err = os.Stat(outside); err != nil {
		t.Fatalf("foreign bytes removed: %v", err)
	}
}

type tenantBlockingBackend struct {
	*MockBackend
	started chan struct{}
}

func (b *tenantBlockingBackend) Exec(ctx context.Context, _ Sandbox, _ ExecSpec) (ExecResult, error) {
	close(b.started)
	<-ctx.Done()
	return ExecResult{}, ctx.Err()
}
func TestTenantFenceCancelsBackendAndPreservesUnknownCommand(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b := &tenantBlockingBackend{MockBackend: NewMockBackend(), started: make(chan struct{})}
	reg := NewRegistry(b)
	reg.SetClassBackend(ClassX, MockBackendName)
	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassX, Profile: ProfileEphemeral}, "x")
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(s, reg, t.TempDir(), "test", nil)
	done := make(chan CommandRecord, 1)
	go func() { rec, _ := runner.RunCommand(ctx, scope, c.ID, "run", "sleep 100", ""); done <- rec }()
	select {
	case <-b.started:
	case <-ctx.Done():
		t.Fatal("backend did not start")
	}
	if err = runner.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	select {
	case rec := <-done:
		if rec.Status != CommandSideEffectUnknown {
			t.Fatalf("cancel invented known outcome: %+v", rec)
		}
	case <-ctx.Done():
		t.Fatal("backend was not cancelled")
	}
	if err = runner.DeleteTenant(ctx, scope.Tenant); err == nil {
		t.Fatal("unknown command erased")
	}
}

type tenantPTY struct {
	done chan struct{}
	once sync.Once
}

func (p *tenantPTY) Read([]byte) (int, error)    { <-p.done; return 0, io.EOF }
func (p *tenantPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *tenantPTY) Resize(uint16, uint16) error { return nil }
func (p *tenantPTY) Wait() (int, error)          { <-p.done; return 0, nil }
func (p *tenantPTY) Close() error                { p.once.Do(func() { close(p.done) }); return nil }

type tenantPTYBackend struct {
	*MockBackend
	pty *tenantPTY
}

func (b *tenantPTYBackend) OpenPTY(context.Context, Sandbox, PTYSpec) (PTYSession, error) {
	return b.pty, nil
}

func TestTenantFenceClosesPTYBeforePurgingComputer(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx := context.Background()
	b := &tenantPTYBackend{MockBackend: NewMockBackend(), pty: &tenantPTY{done: make(chan struct{})}}
	reg := NewRegistry(b)
	reg.SetClassBackend(ClassX, MockBackendName)
	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassX, Profile: ProfileEphemeral}, "pty")
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(s, reg, t.TempDir(), "test", nil)
	pty, err := runner.OpenPTY(ctx, scope, c.ID, PTYSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer pty.Close()
	if err = runner.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	select {
	case <-b.pty.done:
	case <-time.After(time.Second):
		t.Fatal("tenant fence did not close PTY")
	}
	// Close waits for the once-only backend release and incarnation settlement.
	if err = pty.Close(); err != nil {
		t.Fatal(err)
	}
	if err = runner.DeleteTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
}

func TestTenantDeletionWaitsForEnvironmentSettlement(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx := context.Background()
	c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, "env")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `INSERT INTO ahp_compute.env_revisions(id,computer_id,revision,status) VALUES($1,$2,1,'pending')`, contracts.NewID(), c.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.FenceTenant(ctx, scope.Tenant); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteTenant(ctx, scope.Tenant); err == nil {
		t.Fatal("pending environment erased")
	}
}
