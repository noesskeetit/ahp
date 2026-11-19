package stateapi

// P1 admin boundary, second pair of eyes.
//
// The independent audit reproduced this defect on the canonical history: a
// signed task credential, over verified mTLS, against a real Postgres, could
// set a legal hold (HTTP 200, body {"ok":true}). The PDF puts administrative
// operations - DeleteSubject, retention/legal hold, Export - on an internal
// AHP control-plane route and states the acceptance check plainly: a call with
// a task token must be refused (accept/space-pdf-rev32.txt:1918-1920).
//
// The legs here re-run that exact scenario on this revision and then do the
// half the audit could not: read the durable journal out of band (direct SQL,
// not through the API under test) to establish what an operator can actually
// review after an administrative call.

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// requireLaneDatabase keeps the auditor's guard with its correct meaning: an
// administrative leg must never touch a SHARED or production database - it is
// not a claim that only one particular lane may run it. The verdict comes from
// the environment of the run:
//
//   - AHP_TEST_DATABASE_URL (or DATABASE_URL) names the database the leg uses;
//   - a name inside a per-lane namespace is dedicated by construction:
//     prepare-lane.py refuses to create a lane database that already exists, so
//     one such name means one lane, and the leg runs there without the run
//     having to declare anything;
//   - AHP_DB_NAME is the name the run's own environment declared for its
//     database, which covers any other private name; AHP_TEST_DB_ISOLATED=1 is
//     the explicit opt-in for a database isolated some other way;
//   - a short denylist of shared/production names is refused unconditionally,
//     including under the opt-in and including under a namespace lookalike,
//     because a mis-set flag must not be enough to point an administrative leg
//     at the live stand.
//
// A refusal is a hard failure, never a silent skip: a leg that quietly does not
// run looks exactly like a leg that passed.
var sharedDatabases = []string{
	"ahp_hive_delivery_live", "ahp_hive", "ahp", "postgres", "template1", "template0",
}

// dedicatedDatabasePrefixes are the namespaces in which every database is one
// lane's own. The denylist above is consulted first and is not redundant: the
// live stand's database (ahp_hive_delivery_live) shares the ahp_hive_ stem with
// the lane namespace, so ordering is the only thing that keeps a prefix rule
// from welcoming it in.
var dedicatedDatabasePrefixes = []string{"ahp_hive_pdf_", "ahp_pdf_"}

// databaseName extracts the database from a URL DSN or a keyword/value DSN.
func databaseName(dsn string) string {
	if u, err := url.Parse(dsn); err == nil && strings.HasPrefix(u.Path, "/") {
		if name := strings.TrimPrefix(u.Path, "/"); name != "" {
			return name
		}
	}
	for _, field := range strings.Fields(dsn) {
		if key, value, ok := strings.Cut(field, "="); ok && key == "dbname" {
			return strings.Trim(value, `"'`)
		}
	}
	return ""
}

// isolatedDatabaseVerdict decides whether an administrative leg may use dsn,
// given the database name the run declared (AHP_DB_NAME) and the explicit
// isolation flag. It returns the database it resolved so the caller can name it
// in a failure message without ever printing credentials.
func isolatedDatabaseVerdict(dsn, declared, isolatedFlag string) (db string, allowed bool, reason string) {
	db = databaseName(dsn)
	if db == "" {
		return "", false, "cannot resolve a database name from AHP_TEST_DATABASE_URL"
	}
	for _, shared := range sharedDatabases {
		if db == shared {
			return db, false, "is a shared or production database; administrative legs must run on a lane database"
		}
	}
	for _, prefix := range dedicatedDatabasePrefixes {
		if strings.HasPrefix(db, prefix) && len(db) > len(prefix) {
			return db, true, ""
		}
	}
	if isolatedFlag == "1" {
		return db, true, ""
	}
	if declared == "" {
		return db, false, "run declares no AHP_DB_NAME: set AHP_DB_NAME to this lane's database, or AHP_TEST_DB_ISOLATED=1 for a database isolated some other way"
	}
	if declared != db {
		return db, false, "does not match the database the run declares (AHP_DB_NAME=" + declared + "); the leg refuses to act on a database nobody owns"
	}
	return db, true, ""
}

func requireLaneDatabase(t *testing.T) {
	t.Helper()
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Fatal("no test database configured: administrative legs never run without a dedicated database")
	}
	db, allowed, reason := isolatedDatabaseVerdict(dsn, os.Getenv("AHP_DB_NAME"), os.Getenv("AHP_TEST_DB_ISOLATED"))
	if !allowed {
		t.Fatalf("database %q: %s", db, reason)
	}
}

// TestAdministrativeLegDatabaseGuard covers the guard itself, without a
// database, so both halves are proven: every lane database is accepted, and a
// shared name is refused even by a caller who sets the isolation flag.
func TestAdministrativeLegDatabaseGuard(t *testing.T) {
	cases := []struct {
		name     string
		dsn      string
		declared string
		flag     string
		want     bool
	}{
		{"lane database declared by the run", "postgres://u:p@h:5432/ahp_hive_pdf_qwen_int", "ahp_hive_pdf_qwen_int", "", true},
		{"any other lane database", "postgres://u:p@h:5432/ahp_hive_pdf_state_admin", "ahp_hive_pdf_state_admin", "", true},
		// The point of the namespace rule: a leg handed another lane's database
		// runs there without the runner having declared or opted in to anything.
		{"another lane's database, nothing declared", "postgres://u:p@h:5432/ahp_hive_pdf_check_c", "", "", true},
		{"older lane namespace, nothing declared", "postgres://u:p@h:5432/ahp_pdf_20261102_integrated", "", "", true},
		{"keyword dsn parsed", "host=h dbname=ahp_hive_pdf_x password=p", "ahp_hive_pdf_x", "", true},
		{"keyword dsn in the lane namespace, undeclared", "host=h dbname=ahp_hive_pdf_check_c password=p", "", "", true},
		{"opt-in for a differently isolated database", "postgres://u:p@h:5432/audit_lane_db", "", "1", true},
		{"live stand refused even with the flag", "postgres://u:p@h:5432/ahp_hive_delivery_live", "ahp_hive_delivery_live", "1", false},
		{"live stand refused under a declared match too", "postgres://u:p@h:5432/ahp_hive_delivery_live", "ahp_hive_delivery_live", "", false},
		{"shared default database refused", "postgres://u:p@h:5432/postgres", "postgres", "1", false},
		{"shared stand root name refused", "postgres://u:p@h:5432/ahp_hive", "", "1", false},
		{"undeclared and unflagged database", "postgres://u:p@h:5432/someones_db", "", "", false},
		// A dedicated database stays dedicated even when the runner's declaration
		// points elsewhere, and that is the case this guard was loosened for: the
		// candidate worktree declares its own lane name while the leg is aimed at
		// another lane's database. Refusing it would put the admin legs back in the
		// position where only their author's checkout can run them.
		{"lane namespace wins over a mismatched declaration", "postgres://u:p@h:5432/ahp_hive_pdf_state_admin", "ahp_hive_pdf_other", "", true},
		{"no database name at all", "postgres://u:p@h:5432/", "", "1", false},
		{"a bare namespace prefix names no database", "postgres://u:p@h:5432/ahp_hive_pdf_", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, got, reason := isolatedDatabaseVerdict(tc.dsn, tc.declared, tc.flag)
			if got != tc.want {
				t.Fatalf("allowed=%v (db=%q reason=%q), want %v", got, db, reason, tc.want)
			}
			if !tc.want && reason == "" {
				t.Error("a refusal must always carry a reason the operator can act on")
			}
			if strings.Contains(reason, "password") || strings.Contains(db, "@") {
				t.Errorf("guard leaked connection material: db=%q reason=%q", db, reason)
			}
		})
	}
}

// TestAdministrativeLegGuardRefusesThisRunsSharedDatabases is the RED half the
// table cannot carry: it takes the connection string this run really has,
// changes nothing but the database name, and requires the guard to refuse each
// shared and production name - with the isolation flag on, and with nothing
// declared. No connection is opened: the leg proves the decision, which is what
// protects a shared database, and it is not tied to any one lane's name.
func TestAdministrativeLegGuardRefusesThisRunsSharedDatabases(t *testing.T) {
	dsn := os.Getenv("AHP_TEST_DATABASE_URL")
	if dsn == "" {
		dsn = os.Getenv("DATABASE_URL")
	}
	if dsn == "" {
		t.Skip("no AHP_TEST_DATABASE_URL in this environment: nothing to rewrite")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Host == "" {
		t.Skip("this run's DSN is not a URL DSN: the table above covers the keyword form")
	}
	for _, shared := range append([]string{"ahp_hive_delivery_live", "ahp_hive", "postgres"}, sharedDatabases...) {
		swapped := *parsed
		swapped.Path = "/" + shared
		candidate := swapped.String()
		for _, flag := range []string{"", "1"} {
			db, allowed, reason := isolatedDatabaseVerdict(candidate, "", flag)
			if allowed {
				t.Errorf("guard admitted shared database %q with AHP_TEST_DB_ISOLATED=%q", db, flag)
			}
			if db != shared {
				t.Errorf("guard resolved %q, want the database it is refusing", db)
			}
			if reason == "" {
				t.Errorf("refusal of %q carried no reason", db)
			}
			if strings.Contains(reason, "password") || strings.Contains(reason, "@") ||
				strings.Contains(reason, shared+"?") {
				t.Errorf("refusal text carries connection material: %q", reason)
			}
		}
		// The same rewrite for this run's own database must be admitted, or the
		// leg proves nothing: the refusal above would then be a blanket failure.
		own := *parsed
		own.Path = "/" + databaseName(dsn)
		if _, allowed, reason := isolatedDatabaseVerdict(own.String(), os.Getenv("AHP_DB_NAME"), os.Getenv("AHP_TEST_DB_ISOLATED")); !allowed {
			t.Fatalf("the guard refuses this run's own database: %s", reason)
		}
	}
}

// TestRootAuditTaskCannotSetLegalholdOnLaneDatabase is the audit's RED base,
// reproduced on this revision: signed task token + mTLS + real Postgres.
// It also proves the refusal is clean: a denied call must not leave a hold.
func TestRootAuditTaskCannotSetLegalholdOnLaneDatabase(t *testing.T) {
	requireLaneDatabase(t)
	e := newEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	th, err := e.store.CreateThread(e.ctx, scope, "root-audit-hold", "create")
	if err != nil {
		t.Fatal(err)
	}
	token := e.signTask(t, "rootaudit-task")
	code, body := e.call(t, "SetLegalhold", token, fmt.Sprintf(`{"thread_id":%q,"reason":"audit-only"}`, th.ID))
	if code != http.StatusForbidden {
		t.Fatalf("DEFECT: task token SetLegalhold returned %d, want 403; body=%v", code, body)
	}
	var held bool
	if err := e.store.Pool().QueryRow(e.ctx,
		`SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1 AND thread_id=$2)`,
		e.tenant, th.ID).Scan(&held); err != nil {
		t.Fatalf("out-of-band legalhold probe: %v", err)
	}
	if held {
		t.Fatal("a refused administrative call left a durable legal hold behind")
	}
}

// TestAdministrativeOperationsAreReviewableInDurableJournal is the journal
// review. Everything an administrator does through /v1/state-admin must leave
// a durable trace another process can read:
//   - SetLegalhold / RemoveLegalhold: the hold row itself (reason, target);
//   - DeleteSubject: the ahp_state.deletions receipt with counts;
//   - ExportThread: read-only, so it must NOT leave a mutation trace.
//
// Boundary pinned deliberately (open item STATE-ADMIN-ACTOR): the journal
// records the TARGET scope and the operation, not the human who performed it.
// The last assertion fails the day someone adds an actor column, which is the
// point - attribution is then a decision, not an accident.
func TestAdministrativeOperationsAreReviewableInDurableJournal(t *testing.T) {
	requireLaneDatabase(t)
	e := newEnv(t)
	scope := contracts.Scope{Tenant: e.tenant, Deployment: e.deployment, Subject: testSubject}
	th, err := e.store.CreateThread(e.ctx, scope, "root-audit-journal", "create")
	if err != nil {
		t.Fatal(err)
	}
	pool := e.store.Pool()
	holdPresent := func() bool {
		var held bool
		if err := pool.QueryRow(e.ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1 AND thread_id=$2)`,
			e.tenant, th.ID).Scan(&held); err != nil {
			t.Fatalf("out-of-band legalhold probe: %v", err)
		}
		return held
	}

	code, body := adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "SetLegalhold",
		fmt.Sprintf(`{"thread_id":%q,"reason":"retention review"}`, th.ID), "state-admin")
	mustStatus(t, code, http.StatusOK, body)
	var reason string
	if err := pool.QueryRow(e.ctx, `SELECT reason FROM ahp_state.legalhold WHERE tenant=$1 AND thread_id=$2`,
		e.tenant, th.ID).Scan(&reason); err != nil {
		t.Fatalf("administrative hold is not durable: %v", err)
	}
	if reason != "retention review" {
		t.Errorf("journal kept %q, want the reason the administrator supplied", reason)
	}

	before := deletionCount(t, pool, e)
	code, body = e.adminExport(t, th.ID)
	mustStatus(t, code, http.StatusOK, body)
	if got := deletionCount(t, pool, e); got != before {
		t.Errorf("ExportThread is read-only but wrote %d journal rows", got-before)
	}
	if _, err := e.store.GetThread(e.ctx, scope, th.ID); err != nil {
		t.Fatalf("export mutated the thread: %v", err)
	}

	code, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "RemoveLegalhold",
		fmt.Sprintf(`{"thread_id":%q}`, th.ID), "state-admin")
	mustStatus(t, code, http.StatusOK, body)
	if holdPresent() {
		t.Fatal("RemoveLegalhold did not clear the durable hold row")
	}

	code, body = adminCall(t, e, e.srv.Client(), e.tenant, e.deployment, testSubject, "DeleteSubject",
		`{"key":"root-audit-delete"}`, "state-admin")
	mustStatus(t, code, http.StatusOK, body)
	var deleted int64
	var operation string
	if err := pool.QueryRow(e.ctx, `SELECT operation, deleted_count FROM ahp_state.deletions
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND resource=$4`,
		e.tenant, e.deployment, testSubject, "root-audit-delete").Scan(&operation, &deleted); err != nil {
		t.Fatalf("administrative DeleteSubject left no durable receipt: %v", err)
	}
	if operation != "DeleteSubject" || deleted < 1 {
		t.Errorf("receipt says operation=%q deleted=%d, want DeleteSubject with at least one thread", operation, deleted)
	}

	// The pinned boundary: the receipt identifies the target, never the actor.
	rows, err := pool.Query(e.ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema='ahp_state' AND table_name='deletions'`)
	if err != nil {
		t.Fatalf("journal schema probe: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatal(err)
		}
		switch column {
		case "actor", "actor_subject", "performed_by":
			t.Errorf("deletions journal now carries actor attribution (%s): update this leg on purpose - "+
				"operator attribution is an open decision (STATE-ADMIN-ACTOR), not something to acquire silently", column)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}

func deletionCount(t *testing.T, pool *pgxpool.Pool, e *env) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(e.ctx, `SELECT count(*) FROM ahp_state.deletions WHERE tenant=$1`, e.tenant).Scan(&n); err != nil {
		t.Fatalf("out-of-band deletions probe: %v", err)
	}
	return n
}
