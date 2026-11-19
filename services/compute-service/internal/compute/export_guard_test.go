package compute

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Observer queries distinguish real row/FK waits from simply exhausting the
// ordinary pool. The guard has its own bounded connection, even with pool1.
func TestExportGuardPoolOneSerializesMutations(t *testing.T) {
	observer, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cfg := observer.pool.Config()
	cfg.MaxConns = 1
	cfg.MinConns = 0
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := New(pool)
	for _, name := range []string{"AcquireIncarnation", "StopIncarnation", "ExecForeignKey", "PTYForeignKey", "InstallForeignKey"} {
		t.Run(name, func(t *testing.T) {
			c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, name)
			if err != nil {
				t.Fatal(err)
			}
			stoppedID := ""
			if name == "StopIncarnation" {
				inc, e := s.AcquireIncarnation(ctx, scope, c.ID, "test", "node", "")
				if e != nil {
					t.Fatal(e)
				}
				stoppedID = inc.ID
				if e = s.StopIncarnation(ctx, scope, c.ID, stoppedID); e != nil {
					t.Fatal(e)
				}
			}
			guard, err := s.beginExportGuard(ctx, scope, c.ID)
			if err != nil {
				t.Fatal("pool1 guard", err)
			}
			defer func() {
				if guard != nil {
					guard.close()
				}
			}()
			probeCtx, stopProbe := context.WithTimeout(ctx, time.Second)
			err = pool.Ping(probeCtx)
			stopProbe()
			if err != nil {
				t.Fatal("guard starved ordinary pool", err)
			}
			var mutationPID int32
			if err = pool.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&mutationPID); err != nil {
				t.Fatal(err)
			}
			var expectedQuery string
			mutation := func(call context.Context) error {
				switch name {
				case "AcquireIncarnation":
					_, e := s.AcquireIncarnation(call, scope, c.ID, "test", "node", "")
					return e
				case "StopIncarnation":
					return s.StopIncarnation(call, scope, c.ID, stoppedID)
				case "ExecForeignKey":
					_, _, e := s.Exec(call, scope, c.ID, "guard-command", "echo immutable", "")
					return e
				case "InstallForeignKey":
					_, e := s.BeginEnvironmentInstall(call, scope, c.ID, "python=3", "installer")
					return e
				default:
					_, e := s.AcquireLease(call, scope, c.ID, LeasePTY, "guard-pty", time.Minute)
					return e
				}
			}
			switch name {
			case "AcquireIncarnation":
				expectedQuery = "update ahp_compute.computers"
			case "StopIncarnation":
				expectedQuery = "for update"
			case "ExecForeignKey":
				expectedQuery = "insert into ahp_compute.commands"
			default:
				expectedQuery = "insert into ahp_compute.activity_leases"
			}
			waitCtx, stopWait := context.WithTimeout(ctx, 650*time.Millisecond)
			defer stopWait()
			result := make(chan error, 1)
			go func() { result <- mutation(waitCtx) }()
			observed := false
			for waitCtx.Err() == nil {
				var blocked bool
				var query string
				err = observer.pool.QueryRow(ctx, `SELECT COALESCE(wait_event_type='Lock',false) AND $2=ANY(pg_blocking_pids(pid)),query FROM pg_stat_activity WHERE pid=$1`, mutationPID, int32(guard.conn.PgConn().PID())).Scan(&blocked, &query)
				if err != nil {
					t.Fatal("observe mutation lock", err)
				}
				if blocked {
					if !strings.Contains(strings.ToLower(query), expectedQuery) {
						t.Fatalf("blocked at unexpected SQL, want %s", expectedQuery)
					}
					observed = true
					break
				}
				select {
				case e := <-result:
					t.Fatalf("mutation escaped export guard: %v", e)
				case <-time.After(5 * time.Millisecond):
				}
			}
			if !observed {
				t.Fatal("no real PostgreSQL guard lock observed")
			}
			err = <-result
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("guard wait did not honor request deadline: %v", err)
			}
			guard.close()
			guard = nil
			err = mutation(ctx)
			if name == "StopIncarnation" {
				// Stop is deliberately not idempotent: an already-stopped ID returns404.
				// This known refusal is reachable only after the row guard is released.
				if contracts.Code(err) != contracts.ErrNotFound {
					t.Fatalf("stopped incarnation result: %v", err)
				}
				inc, e := s.AcquireIncarnation(ctx, scope, c.ID, "test", "node", "")
				if e != nil {
					t.Fatal(e)
				}
				if e = s.StopIncarnation(ctx, scope, c.ID, inc.ID); e != nil {
					t.Fatal("valid stop after guard", e)
				}
			} else if err != nil {
				t.Fatal("mutation remained blocked after guard close", err)
			}
		})
	}
}

func TestExportGuardRejectsNonQuiescentSources(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, name := range []string{"incarnation", "accepted", "dispatched", "unknown", "pty", "pending_install"} {
		t.Run(name, func(t *testing.T) {
			c, err := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, name)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "incarnation":
				_, err = s.AcquireIncarnation(ctx, scope, c.ID, "test", "node", "")
			case "pty":
				_, err = s.AcquireLease(ctx, scope, c.ID, LeasePTY, "busy-pty", time.Minute)
			case "pending_install":
				_, err = s.BeginEnvironmentInstall(ctx, scope, c.ID, "python=3", "installer")
				if err == nil {
					err = s.ReleaseLease(ctx, scope, c.ID, LeaseWriter, "installer")
				}
			default:
				var cmd CommandRecord
				cmd, _, err = s.Exec(ctx, scope, c.ID, "busy", "echo busy", "")
				if err == nil && name != "accepted" {
					cmd, err = s.MarkDispatched(ctx, scope, c.ID, cmd.ID)
				}
				if err == nil && name == "unknown" {
					cmd, err = s.Fence(ctx, scope, c.ID, cmd.ID)
					if err == nil && cmd.Status != CommandSideEffectUnknown {
						t.Fatal("expected explicit UNKNOWN")
					}
				}
			}
			if err != nil {
				t.Fatal("create busy source", err)
			}
			guard, err := s.beginExportGuard(ctx, scope, c.ID)
			if guard != nil {
				guard.close()
				t.Fatal("non-quiescent source admitted")
			}
			if contracts.Code(err) != contracts.ErrConflict || contracts.ReasonOf(err) != "export_source_busy" {
				t.Fatalf("busy source refusal: %v", err)
			}
		})
	}
}

func TestExportGuardConnectionBoundAndCancellation(t *testing.T) {
	s, scope := tenantComputeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var computers []Computer
	for _, key := range []string{"a", "b", "c"} {
		c, e := s.CreateComputer(ctx, scope, NewComputerRequest{Class: ClassS, Profile: ProfileEphemeral}, key)
		if e != nil {
			t.Fatal(e)
		}
		computers = append(computers, c)
	}
	a, e := s.beginExportGuard(ctx, scope, computers[0].ID)
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		if a != nil {
			a.close()
		}
	}()
	b, e := s.beginExportGuard(ctx, scope, computers[1].ID)
	if e != nil {
		t.Fatal(e)
	}
	defer b.close()
	wait, stop := context.WithTimeout(ctx, 75*time.Millisecond)
	defer stop()
	if g, e := s.beginExportGuard(wait, scope, computers[2].ID); !errors.Is(e, context.DeadlineExceeded) || g != nil {
		if g != nil {
			g.close()
		}
		t.Fatalf("third guard exceeded process connection bound: %v", e)
	}
	a.close()
	a = nil
	c, e := s.beginExportGuard(ctx, scope, computers[2].ID)
	if e != nil {
		t.Fatal("released guard slot was not reusable", e)
	}
	c.close()
}
