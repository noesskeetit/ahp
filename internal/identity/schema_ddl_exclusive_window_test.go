package identity

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// The bump to schemaVersion 8 re-applies schemaDDL over an installation that has
// been running for months. Two facts decide whether that is a quiet operation or a
// stall in front of live traffic:
//
//   - DROP TABLE queues behind any reader holding ACCESS SHARE: measured on a real
//     server, `DROP TABLE IF EXISTS` against a table a transaction has selected
//     from fails with a lock timeout, while `CREATE TABLE IF NOT EXISTS` on the
//     same table succeeds alongside that reader.
//   - An installation in the field does not carry the abandoned token_hash shape of
//     browser_token_bindings, so on the overwhelming majority of databases the DROP
//     has nothing to do -- yet while it sat inside schemaDDL it was still a
//     statement in the migration, decided inside the exclusive window by a probe
//     that could only run once the window was open.
//
// So the decision moved in front of the window: Migrate asks the catalog, under its
// advisory lock and before any DDL, whether the retired shape exists, and sends the
// DROP only in that case. A healthy installation never has a DROP TABLE sent to it.

// relfilenode identifies the physical storage of a relation. A DROP followed by a
// CREATE is a different relation even when the name and the columns match again,
// which is what makes it observable that no drop happened.
func relfilenode(t *testing.T, pool *pgxpool.Pool, qualified string) string {
	t.Helper()
	var node string
	if err := pool.QueryRow(context.Background(),
		`select coalesce((select pg_relation_filepath(c.oid)::text || '#' || c.relfilenode::text
		    from pg_class c where c.oid = $1::regclass), 'absent')`, qualified).Scan(&node); err != nil {
		t.Fatalf("read the storage identity of %s: %v", qualified, err)
	}
	return node
}

// C2, the measured half. Over a stamped installation -- the rollout case, where the
// whole DDL runs again -- a reader holds ACCESS SHARE on browser_token_bindings for
// the entire migration. If the migration asked for ACCESS EXCLUSIVE on that table it
// would not complete: the pool here carries lock_timeout, so it would come back
// 55P03 rather than hang, and the sampling below would record the ungranted request.
func TestReapplyingTheIdentityDDLTakesNoExclusiveLockOnBrowserTokenBindings(t *testing.T) {
	pool, _ := isolatedPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store := New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("initial Migrate: %v", err)
	}
	const table = "ahp_identity.browser_token_bindings"

	// Make it a rollout: the sentinel says an older revision, so the DDL runs again.
	if _, err := pool.Exec(ctx, `UPDATE ahp_identity.schema_version SET version=$1 WHERE component='ddl'`, existingInstallationVersion); err != nil {
		t.Fatalf("stamp the installation %d: %v", existingInstallationVersion, err)
	}
	before := relfilenode(t, pool, table)

	// A reader that holds ACCESS SHARE for the whole window, exactly the session a
	// login flow would have open.
	holder, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire the reading session: %v", err)
	}
	defer holder.Release()
	if _, err = holder.Exec(ctx, "BEGIN"); err != nil {
		t.Fatalf("begin the reading transaction: %v", err)
	}
	defer func() { _, _ = holder.Exec(context.Background(), "ROLLBACK") }()
	var seen int64
	if err = holder.QueryRow(ctx, "select count(*) from "+table).Scan(&seen); err != nil {
		t.Fatalf("read the table (this is what takes ACCESS SHARE): %v", err)
	}

	// A migrator whose lock wait cannot exceed a short timeout, so a real request
	// for ACCESS EXCLUSIVE is a visible failure rather than an indefinite stall.
	strict := pool.Config().Copy()
	strict.ConnConfig.RuntimeParams["lock_timeout"] = "2s"
	strictPool, err := pgxpool.NewWithConfig(ctx, strict)
	if err != nil {
		t.Fatalf("build the strict-timeout pool: %v", err)
	}
	defer strictPool.Close()

	var ungranted int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			var n int64
			if err := strictPool.QueryRow(ctx, `select count(*) from pg_locks
               where relation = $1::regclass and not granted and mode = 'AccessExclusiveLock'`, table).Scan(&n); err != nil {
				return
			}
			atomic.AddInt64(&ungranted, n)
		}
	}()

	started := time.Now()
	err = New(strictPool).Migrate(ctx)
	elapsed := time.Since(started)
	close(stop)
	<-sampled

	observations := atomic.LoadInt64(&ungranted)
	after := relfilenode(t, pool, table)
	t.Logf("MEASURED migrate_over_a_reading_session elapsed=%v ungranted_access_exclusive_observations=%d storage_before=%s storage_after=%s rows_visible_to_reader=%d",
		elapsed.Round(time.Millisecond), observations, before, after, seen)
	if err != nil {
		t.Fatalf("DEF-C2: re-applying the identity DDL in front of a reader holding ACCESS SHARE on browser_token_bindings failed after %v: %v", elapsed.Round(time.Millisecond), err)
	}
	if observations != 0 {
		t.Fatalf("DEF-C2: the migration requested ACCESS EXCLUSIVE on browser_token_bindings %d time(s) although this installation carries none of the retired shape", observations)
	}
	if after != before {
		t.Fatalf("DEF-C2: the table was replaced (storage %s -> %s) although this installation carries none of the retired shape", before, after)
	}
}

// The pre-check is not a way of never dropping anything. An installation that does
// carry the abandoned shape still has to be brought to the current one, and this is
// the case the exclusive lock is worth taking.
func TestTheLegacyBrowserTokenBindingsShapeIsStillRetired(t *testing.T) {
	pool, _ := isolatedPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store := New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("initial Migrate: %v", err)
	}

	// Recreate the abandoned revision of the table by hand: the shape this
	// installation would have if it had been migrated before the jti key landed.
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS ahp_identity.browser_token_bindings CASCADE`); err != nil {
		t.Fatalf("remove the current shape: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE ahp_identity.browser_token_bindings (
        token_hash text PRIMARY KEY, session_id text NOT NULL, epoch bigint NOT NULL,
        expires_at timestamptz NOT NULL, revoked_at timestamptz)`); err != nil {
		t.Fatalf("create the legacy shape: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ahp_identity.schema_version SET version=$1 WHERE component='ddl'`, existingInstallationVersion); err != nil {
		t.Fatalf("stamp the installation %d: %v", existingInstallationVersion, err)
	}
	legacy, err := holdsLegacyBrowserTokenBindings(ctx, pool)
	if err != nil {
		t.Fatalf("pre-check the legacy shape: %v", err)
	}
	if !legacy {
		t.Fatal("the scenario is not the field state: the pre-check does not see the shape it exists to find")
	}

	if err = store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate over the retired shape: %v", err)
	}
	var columns string
	if err = pool.QueryRow(ctx, `select string_agg(attname, ',' order by attnum) from pg_attribute
       where attrelid = 'ahp_identity.browser_token_bindings'::regclass and attnum > 0 and not attisdropped`).Scan(&columns); err != nil {
		t.Fatalf("read the resulting shape: %v", err)
	}
	joined := columns // string_agg already produced the one list
	t.Logf("DURABLE browser_token_bindings columns after migrating the retired shape: %s", joined)
	if strings.Contains(joined, "token_hash") {
		t.Fatalf("DEF-C2: the retired shape survived the migration: %s", joined)
	}
	if !strings.Contains(joined, "token_jti") {
		t.Fatalf("DEF-C2: the migration did not recreate the current shape: %s", joined)
	}
}
