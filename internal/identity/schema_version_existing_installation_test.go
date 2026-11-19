package identity

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/labels"
)

// existingInstallationVersion is the stamp every database in the field carries,
// the canonical build included: 1152a8ea added ahp_identity.context_label_events
// to schemaDDL and left schemaVersion at 7, so every already-migrated database
// -- the live stand, the canon, every lane database cloned from one -- is stamped
// 7 and has no journal table. Migrate is the only thing that can close that gap,
// and it does so only when the sentinel is behind schemaVersion.
//
// This leg states the obligation as a permanent property. If a later revision
// makes 7 obsolete in some way that no longer needs reconciling, this leg is
// retired deliberately -- not by editing the number to whatever passes.
const existingInstallationVersion int64 = 7

func journalState(t *testing.T, pool *pgxpool.Pool) (version int64, table bool, triggers int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := pool.QueryRow(ctx, `select coalesce(max(version), -1) from ahp_identity.schema_version where component='ddl'`).Scan(&version); err != nil {
		t.Fatalf("read the migration sentinel: %v", err)
	}
	if err := pool.QueryRow(ctx, `select to_regclass('ahp_identity.context_label_events') is not null`).Scan(&table); err != nil {
		t.Fatalf("look for the journal table: %v", err)
	}
	if err := pool.QueryRow(ctx, `select count(*) from pg_trigger t join pg_class c on c.oid=t.tgrelid
	  where c.relname='context_label_events' and t.tgname='context_label_events_append_only' and not t.tgisinternal`).Scan(&triggers); err != nil {
		t.Fatalf("look for the append-only trigger: %v", err)
	}
	t.Logf("DURABLE sentinel_ddl_version=%d context_label_events_present=%v append_only_trigger_count=%d", version, table, triggers)
	return version, table, triggers
}

// TestMigrateReconcilesAnInstallationStampedSeven is the migration an existing
// installation was never given. The package's own suite cannot catch this on its
// own: isolatedPool creates a fresh database per run, so every leg here has only
// ever seen a clean database, where schemaDDL lands whole regardless of the
// stamp. A stamped database is a different world, and it is the world the product
// runs in.
func TestMigrateReconcilesAnInstallationStampedSeven(t *testing.T) {
	pool, _ := isolatedPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	store := New(pool)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("initial Migrate: %v", err)
	}

	// Make this database an installation whose DDL was applied at the previous
	// revision: the journal object and its trigger are gone, and the sentinel
	// says the work is done. This is real DDL against a real catalog, not a
	// stand-in for the migrator -- the migrator's own response is what is under
	// test.
	if _, err := pool.Exec(ctx, `DROP TABLE IF EXISTS ahp_identity.context_label_events CASCADE`); err != nil {
		t.Fatalf("drop the journal as the previous revision left it: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE ahp_identity.schema_version SET version=$1 WHERE component='ddl'`, existingInstallationVersion); err != nil {
		t.Fatalf("stamp the installation %d: %v", existingInstallationVersion, err)
	}
	if version, table, triggers := journalState(t, pool); version != existingInstallationVersion || table || triggers != 0 {
		t.Fatalf("the scenario is not the field state: version=%d table=%v triggers=%d", version, table, triggers)
	}

	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("Migrate over a stamped installation: %v", err)
	}
	_, table, triggers := journalState(t, pool)
	if !table {
		t.Fatalf("DEF-IDENTITY-SCHEMA-VERSION: Migrate left an installation stamped %d without ahp_identity.context_label_events, so every context journal append is 42P01", existingInstallationVersion)
	}
	if triggers != 1 {
		t.Fatalf("the journal arrived without its append-only trigger (count=%d): the append-only rule is a database rule, not a convention", triggers)
	}

	// The obligation the table exists to serve, through the product's own API.
	suffix := contracts.NewID()
	scope := contracts.Scope{Tenant: "sv8-" + suffix, Deployment: "dep-" + suffix, Subject: "subj-" + suffix}
	if _, err := store.CreateRoot(ctx, scope, RootRequest{Task: "task-" + suffix, Context: "ctx-" + suffix,
		Deployment: scope.Deployment, Capabilities: []string{"state"}, TTL: 10 * time.Minute}); err != nil {
		t.Fatalf("CreateRoot: %v", err)
	}
	if _, err := store.AddLabels(ctx, scope, "task-"+suffix, labels.Set{Subjects: []string{"widened-after-migration"}}); err != nil {
		t.Fatalf("DEF-IDENTITY-SCHEMA-VERSION: widening the context journal failed on a database Migrate claims is current: %v", err)
	}
	var events int
	if err := pool.QueryRow(ctx, `select count(*) from ahp_identity.context_label_events
	  where tenant=$1 and subject=$2`, scope.Tenant, scope.Subject).Scan(&events); err != nil {
		t.Fatalf("read the journal: %v", err)
	}
	if events != 1 {
		t.Fatalf("the widening wrote %d journal rows, expected exactly one immutable event", events)
	}

	// A second Migrate must be a no-op, not a re-run: the sentinel has to have
	// moved past the installation's stamp, and the objects must survive intact.
	versionBefore, _, triggerCountBefore := journalState(t, pool)
	var defBefore string
	if err := pool.QueryRow(ctx, `select pg_get_triggerdef(t.oid) from pg_trigger t join pg_class c on c.oid=t.tgrelid
	  where c.relname='context_label_events' and t.tgname='context_label_events_append_only' and not t.tgisinternal`).Scan(&defBefore); err != nil {
		t.Fatalf("read the trigger definition: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("repeated Migrate: %v", err)
	}
	versionAfter, tableAfter, triggerCountAfter := journalState(t, pool)
	if versionAfter != versionBefore || !tableAfter || triggerCountAfter != triggerCountBefore {
		t.Fatalf("repeated Migrate changed the schema state: %d/%v/%d became %d/%v/%d",
			versionBefore, tableAfter, triggerCountBefore, versionAfter, tableAfter, triggerCountAfter)
	}
	var defAfter string
	if err := pool.QueryRow(ctx, `select pg_get_triggerdef(t.oid) from pg_trigger t join pg_class c on c.oid=t.tgrelid
	  where c.relname='context_label_events' and t.tgname='context_label_events_append_only' and not t.tgisinternal`).Scan(&defAfter); err != nil {
		t.Fatalf("re-read the trigger definition: %v", err)
	}
	if defAfter != defBefore {
		t.Fatalf("repeated Migrate rewrote the append-only trigger: %q became %q", defBefore, defAfter)
	}
	if versionAfter <= existingInstallationVersion {
		t.Fatalf("the sentinel stayed at %d, so the next migrator will consider this installation current again", versionAfter)
	}
}
