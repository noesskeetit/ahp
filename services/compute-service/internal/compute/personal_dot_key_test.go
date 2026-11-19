package compute

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Index replacement is exercised in a fresh, exclusively owned database. Never
// recreate the old global index in a shared fixture or the deployed database.
func personalDotMigrationStore(t *testing.T) *Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(os.Getenv("AHP_TEST_DATABASE_URL"))
	if err != nil || os.Getenv("AHP_TEST_DATABASE_URL") == "" {
		t.Fatal("AHP_TEST_DATABASE_URL is required")
	}
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	name := "ahp_compute_dot_" + strings.ReplaceAll(contracts.NewID(), "-", "")
	if _, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		admin.Close()
		t.Fatalf("create isolated migration database: %v", err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
			t.Errorf("drop own migration database: %v", err)
		}
	})
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return New(pool)
}

func TestPersonalComputerDotKeyFreshAndExistingDatabase(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "fresh"
		if legacy {
			name = "upgrade"
		}
		t.Run(name, func(t *testing.T) {
			s := personalDotMigrationStore(t)
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			if err := s.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if legacy {
				// An existing deployment has the original, too-broad uniqueness.
				// Its first Computer and revision must survive the upgrade.
				_, err := s.pool.Exec(ctx, `DROP INDEX IF EXISTS ahp_compute.computers_personal_by_dot_active_idx;
CREATE UNIQUE INDEX IF NOT EXISTS computers_personal_active_idx ON ahp_compute.computers(tenant,subject,realm) WHERE profile='personal' AND status<>'destroyed'`)
				if err != nil {
					t.Fatal(err)
				}
			}
			first := contracts.Scope{Tenant: contracts.NewID(), Subject: "alice", Deployment: "dot-one"}
			other := first
			other.Deployment = "dot-two"
			req := NewComputerRequest{Class: ClassS, Profile: ProfilePersonal, Realm: "work"}
			one, err := s.CreateComputer(ctx, first, req, "same-key")
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			two, err := s.CreateComputer(ctx, other, req, "same-key")
			if err != nil {
				t.Fatalf("second Dot's same user/realm/key Computer was refused: %v", err)
			}
			if one.ID == two.ID {
				t.Fatal("two Dot deployments share a Computer")
			}
			if err = s.Migrate(ctx); err != nil {
				t.Fatalf("repeat migration after two Dots: %v", err)
			}
			for index, scope := range []contracts.Scope{first, other} {
				want := []Computer{one, two}[index]
				replay, err := s.CreateComputer(ctx, scope, req, "same-key")
				if err != nil || replay.ID != want.ID {
					t.Fatalf("existing Computer changed: %v", err)
				}
				if _, err = s.CreateComputer(ctx, scope, req, "different-key"); contracts.Code(err) != contracts.ErrConflict {
					t.Fatalf("same Dot admitted a duplicate: %v", err)
				}
				if _, err = s.GetComputer(ctx, scope, []Computer{two, one}[index].ID); contracts.Code(err) != contracts.ErrNotFound {
					t.Fatalf("cross-Dot read not refused: %v", err)
				}
			}
		})
	}
}
