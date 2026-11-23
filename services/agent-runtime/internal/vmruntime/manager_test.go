//go:build unix

package vmruntime

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/internal/identity"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"github.com/jackc/pgx/v5/pgxpool"
)

type child struct {
	proc *os.Process
	done chan struct{}
}

type processFixture struct {
	mu       sync.Mutex
	children map[string]*child
	starts   int
}

func (p *processFixture) Start(_ context.Context, g Guest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cmd := exec.Command("/bin/sleep", "60")
	cmd.Env = []string{}
	if err := cmd.Start(); err != nil {
		return err
	}
	c := &child{cmd.Process, make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		close(c.done)
	}()
	p.children[g.ID()] = c
	p.starts++
	return nil
}

func (p *processFixture) Alive(_ context.Context, g Guest) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c := p.children[g.ID()]
	if c == nil {
		return false, nil
	}
	select {
	case <-c.done:
		return false, nil
	default:
		return true, nil
	}
}

func (p *processFixture) StopAndWait(ctx context.Context, g Guest) error {
	p.mu.Lock()
	c := p.children[g.ID()]
	p.mu.Unlock()
	return stopChild(ctx, c)
}

func stopChild(ctx context.Context, c *child) error {
	if c == nil {
		return nil
	}
	if err := c.proc.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func newProcessFixture(t *testing.T) *processFixture {
	t.Helper()
	p := &processFixture{children: make(map[string]*child)}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		p.mu.Lock()
		defer p.mu.Unlock()
		for _, c := range p.children {
			if err := stopChild(ctx, c); err != nil {
				t.Errorf("stop test process: %v", err)
			}
		}
	})
	return p
}

func execution() Execution {
	scope := contracts.Scope{Tenant: "vm-" + strings.ToLower(rand.Text()), Deployment: "agent", Subject: "user"}
	return Execution{Scope: scope, SessionID: "s", Harness: "go", LaunchTurnSeq: 1, Generation: 1,
		ID: journal.DeterministicExecutionID(scope, "s", 1, 1)}
}

func setup(t *testing.T) (context.Context, Manager, *identity.Store, Execution, Host, *processFixture) {
	t.Helper()
	dsn := os.Getenv("AHP_JOURNAL_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires an isolated PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	grants := identity.New(pool)
	if err := grants.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	p := newProcessFixture(t)
	host := Host{ID: "host-a", Dir: t.TempDir(), Driver: p}
	m := Manager{Pool: pool, Grants: grants, PlaceHost: func(Execution) string { return host.ID },
		Host: func(id string) (ExecutionHost, error) {
			if id != host.ID {
				return nil, unavailable("unexpected execution host")
			}
			return host, nil
		}}
	if err := m.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, m, grants, execution(), host, p
}

func expectCode(t *testing.T, err error, code string) {
	t.Helper()
	if contracts.Code(err) != code {
		t.Fatalf("got %v, want error code %s", err, code)
	}
}

type delayedGrant struct {
	GrantStore
	entered chan struct{}
	start   chan struct{}
}

func (d delayedGrant) CreateRoot(ctx context.Context, s contracts.Scope, req identity.RootRequest) (identity.Grant, error) {
	close(d.entered)
	select {
	case <-d.start:
		return d.GrantStore.CreateRoot(ctx, s, req)
	case <-ctx.Done():
		return identity.Grant{}, ctx.Err()
	}
}

func TestFenceRejectsLateGrantAndHostStartAcrossRestart(t *testing.T) {
	ctx, m, grants, e, host, p := setup(t)
	entered, start := make(chan struct{}), make(chan struct{})
	stale := m
	stale.Grants = delayedGrant{grants, entered, start}
	result := make(chan error, 1)
	go func() { result <- stale.EnsureRunning(ctx, e) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	if err := m.Fence(ctx, e); err != nil {
		t.Fatal(err)
	}
	close(start)
	expectCode(t, <-result, contracts.ErrForbidden)
	_, err := grants.Get(ctx, e.Scope, e.ID)
	expectCode(t, err, contracts.ErrNotFound)

	restarted := Host{ID: host.ID, Dir: host.Dir, Driver: p}
	expectCode(t, restarted.StartOrAdopt(ctx, e, 1), contracts.ErrConflict)
	if p.starts != 0 {
		t.Errorf("spawned %d processes after fencing", p.starts)
	}
}

type delayedPublish struct {
	Host
	entered chan struct{}
	finish  chan struct{}
}

func (d delayedPublish) StartOrAdopt(ctx context.Context, e Execution, attempt int64) error {
	if err := d.Host.StartOrAdopt(ctx, e, attempt); err != nil {
		return err
	}
	close(d.entered)
	select {
	case <-d.finish:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestTakeoverLeavesWinningProcessAndGrantLive(t *testing.T) {
	ctx, m, grants, e, host, p := setup(t)
	entered, finish := make(chan struct{}), make(chan struct{})
	stale := m
	stale.Host = func(string) (ExecutionHost, error) {
		return delayedPublish{host, entered, finish}, nil
	}
	result := make(chan error, 1)
	go func() { result <- stale.EnsureRunning(ctx, e) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := m.Pool.Exec(ctx, `
UPDATE ahp_runtime.executions SET updated_at=now()-interval '11 seconds'
WHERE tenant_id=$1 AND execution_id=$2`, e.Scope.Tenant, e.ID); err != nil {
		t.Fatal(err)
	}
	if err := m.EnsureRunning(ctx, e); err != nil {
		t.Fatal(err)
	}
	close(finish)
	expectCode(t, <-result, contracts.ErrConflict)
	expectCode(t, host.StartOrAdopt(ctx, e, 1), contracts.ErrConflict)
	if ok, err := p.Alive(ctx, Guest{Execution: e, Attempt: 2}); err != nil || !ok {
		t.Fatalf("winner lost its process: %v, %v", ok, err)
	}
	if ok, _ := p.Alive(ctx, Guest{Execution: e, Attempt: 1}); ok {
		t.Fatal("superseded process is still live")
	}
	if _, err := grants.Evaluate(ctx, e.Scope, e.ID, "state"); err != nil {
		t.Fatalf("stale CAS revoked the winner: %v", err)
	}
	// Fence uses the original physical ID even if another turn is active.
	e.LaunchTurnSeq = 2
	if err := m.Fence(ctx, e); err != nil {
		t.Fatal(err)
	}
	if ok, _ := p.Alive(ctx, Guest{Execution: e, Attempt: 2}); ok {
		t.Fatal("fence completed with the winning process still live")
	}
	_, err := grants.Evaluate(ctx, e.Scope, e.ID, "state")
	expectCode(t, err, contracts.ErrForbidden)
}

func TestFenceDoesNotAcknowledgeUnavailableHost(t *testing.T) {
	ctx, m, _, e, host, p := setup(t)
	if err := m.EnsureRunning(ctx, e); err != nil {
		t.Fatal(err)
	}
	offline := m
	offline.PlaceHost = func(Execution) string { return "wrong-host" }
	offline.Host = func(id string) (ExecutionHost, error) {
		if id != host.ID {
			t.Fatalf("fence attempted to relocate the execution to %s", id)
		}
		return nil, unavailable("offline")
	}
	expectCode(t, offline.Fence(ctx, e), contracts.ErrUnavailable)
	expectCode(t, m.EnsureRunning(ctx, e), contracts.ErrConflict)
	if err := m.Fence(ctx, e); err != nil {
		t.Fatal(err)
	}
	if ok, _ := p.Alive(ctx, Guest{Execution: e, Attempt: 1}); ok {
		t.Fatal("fence acknowledged before process exit")
	}
}

type lostStartReply struct{ Driver }

func (d lostStartReply) Start(ctx context.Context, guest Guest) error {
	if err := d.Driver.Start(ctx, guest); err != nil {
		return err
	}
	return unavailable("process started before the start reply was lost")
}

func TestFenceFindsProcessWhenStartWasNeverConfirmed(t *testing.T) {
	ctx, m, _, e, host, p := setup(t)
	broken := m
	broken.Host = func(string) (ExecutionHost, error) {
		return Host{ID: host.ID, Dir: host.Dir, Driver: lostStartReply{p}}, nil
	}
	expectCode(t, broken.EnsureRunning(ctx, e), contracts.ErrUnavailable)
	if active, _ := p.Alive(ctx, Guest{Execution: e, Attempt: 1}); !active {
		t.Fatal("test did not leave a real child after the lost reply")
	}
	if err := m.Fence(ctx, e); err != nil {
		t.Fatal(err)
	}
	if active, _ := p.Alive(ctx, Guest{Execution: e, Attempt: 1}); active {
		t.Fatal("fence did not stop the process in the unconfirmed slot")
	}
}
