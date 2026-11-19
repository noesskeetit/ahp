package state

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func appendLeaseFixture(t *testing.T) (*Store, contracts.Scope, *Lease, string) {
	t.Helper()
	s := newEventStore(t)
	ctx := context.Background()
	scope := eventScope("append-projection-" + contracts.NewID())
	th, err := s.CreateThread(ctx, scope, "ctx-"+contracts.NewID(), contracts.NewID())
	if err != nil {
		t.Fatal(err)
	}
	input := strings.Repeat("x", 102400)
	payload, _ := json.Marshal(map[string]any{"input": input, "max_steps": 8})
	run, err := s.CreateRun(ctx, scope, th.ID, "task", contracts.NewID(), payload)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.ClaimRun(ctx, scope.Tenant, scope.Deployment, "append-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Run.ID != run.ID {
		t.Fatal("fixture claimed another run")
	}
	return s, scope, lease, input
}

func appendProjectionSQLState(err error) string {
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return pg.SQLState()
	}
	return ""
}

// This role has column SELECT for every getRun metadata column EXCEPT payload.
// A table SELECT grant would override this proof, so actual privileges are
// asserted before use. Role/grants are fresh and removed after its pool closes.
func appendMetadataOnlyStore(t *testing.T, admin *Store) *Store {
	t.Helper()
	ctx := context.Background()
	role := "ahp_v11_append_" + contracts.NewID()
	quoted := pgx.Identifier{role}.Sanitize()
	if _, err := admin.pool.Exec(ctx, "CREATE ROLE "+quoted+" NOLOGIN NOINHERIT"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.pool.Exec(cleanup, "DROP OWNED BY "+quoted); err != nil {
			t.Error(err)
			return
		}
		if _, err := admin.pool.Exec(cleanup, "DROP ROLE "+quoted); err != nil {
			t.Error(err)
		}
	})
	for _, sql := range []string{
		"GRANT USAGE ON SCHEMA ahp_state TO " + quoted,
		"GRANT SELECT (id,thread_id,task_id,status,wait_for,sequence,deadline,traceparent,body_digest) ON ahp_state.runs TO " + quoted,
		"GRANT SELECT,UPDATE ON ahp_state.threads TO " + quoted,
		"GRANT SELECT ON ahp_state.erased_subjects,ahp_state.erased_tenants TO " + quoted,
		"GRANT SELECT,INSERT ON ahp_state.stream_events TO " + quoted,
	} {
		if _, err := admin.pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	var tableSelect, payloadSelect bool
	if err := admin.pool.QueryRow(ctx, `SELECT has_table_privilege($1,'ahp_state.runs','SELECT'),has_column_privilege($1,'ahp_state.runs','payload','SELECT')`, role).Scan(&tableSelect, &payloadSelect); err != nil {
		t.Fatal(err)
	}
	if tableSelect || payloadSelect {
		t.Fatal("restricted role can read payload; column proof is invalid")
	}
	cfg := admin.pool.Config().Copy()
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET ROLE "+quoted)
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return New(pool)
}

func TestAppendStreamLeaseDoesNotReadRunPayload_RealState(t *testing.T) {
	admin, scope, lease, input := appendLeaseFixture(t)
	limited := appendMetadataOnlyStore(t, admin)
	ctx := context.Background()
	payload := json.RawMessage(`"result0000 "`)
	first, err := limited.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "piece-1", "model_delta", payload)
	if err != nil {
		t.Fatalf("valid AppendStream must not SELECT runs.payload: code=%s sqlstate=%s", contracts.Code(err), appendProjectionSQLState(err))
	}
	again, err := limited.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "piece-1", "model_delta", payload)
	if err != nil || again.ID != first.ID || again.Sequence != first.Sequence {
		t.Fatal("duplicate no longer replays exact durable event")
	}
	if _, err = limited.AppendStream(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, "piece-1", "model_delta", json.RawMessage(`"changed"`)); !errors.Is(err, ErrKeyReuse) {
		t.Fatal("changed duplicate no longer refused")
	}
	if _, err = limited.GetRun(ctx, scope, lease.Run.ID); appendProjectionSQLState(err) != "42501" {
		t.Fatalf("public GetRun must retain full payload projection: sqlstate=%s", appendProjectionSQLState(err))
	}
	run, err := admin.GetRun(ctx, scope, lease.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertPayload := func(raw json.RawMessage) {
		var v struct {
			Input string `json:"input"`
		}
		if json.Unmarshal(raw, &v) != nil || v.Input != input {
			t.Fatal("full100KiB run input changed or disappeared")
		}
	}
	assertPayload(run.Payload)
	if err = admin.withLease(ctx, scope, lease.Run.ID, lease.Worker, lease.Epoch, func(_ pgx.Tx, l *Lease) error { assertPayload(l.Run.Payload); return nil }); err != nil {
		t.Fatal(err)
	}
}

func TestAppendStreamNarrowLeasePreservesFences_RealState(t *testing.T) {
	for _, name := range []string{"tenant", "deployment", "subject", "epoch", "worker", "expired", "subject_erased", "tenant_erased"} {
		t.Run(name, func(t *testing.T) {
			s, scope, l, _ := appendLeaseFixture(t)
			ctx := context.Background()
			worker, epoch := l.Worker, l.Epoch
			want := contracts.ErrNotFound
			switch name {
			case "tenant":
				scope.Tenant = "foreign"
			case "deployment":
				scope.Deployment = "foreign"
			case "subject":
				scope.Subject = "foreign"
			case "epoch":
				epoch++
				want = contracts.ErrStaleEpoch
			case "worker":
				worker = "other"
				want = contracts.ErrStaleEpoch
			case "expired":
				if _, err := s.pool.Exec(ctx, `UPDATE ahp_state.threads SET lease_expires=clock_timestamp()-interval '1 second' WHERE id=$1`, l.Thread.ID); err != nil {
					t.Fatal(err)
				}
				want = contracts.ErrStaleEpoch
			case "subject_erased":
				if err := s.FenceSubject(ctx, scope); err != nil {
					t.Fatal(err)
				}
				want = contracts.ErrForbidden
			case "tenant_erased":
				if err := s.FenceTenant(ctx, scope.Tenant); err != nil {
					t.Fatal(err)
				}
				want = contracts.ErrForbidden
			}
			_, err := s.AppendStream(ctx, scope, l.Run.ID, worker, epoch, "refused", "model_delta", json.RawMessage(`"result0000 "`))
			if contracts.Code(err) != want {
				t.Fatalf("fence=%s code=%s want=%s", name, contracts.Code(err), want)
			}
			var count int
			if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM ahp_state.stream_events WHERE thread_id=$1`, l.Thread.ID).Scan(&count); err != nil || count != 0 {
				t.Fatal("refused fence wrote a stream event")
			}
		})
	}
}
