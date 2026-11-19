package artifacts

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// TestSplitStatements: the migration scripts must be cut into one statement per
// DDL object, so a lost IF NOT EXISTS race cannot roll the rest of the script
// back. Semicolons inside literals and comments do not cut.
func TestSplitStatements(t *testing.T) {
	for _, script := range []string{ddl, ddlLifecycle, ddlGrants, ddlContextGrants} {
		stmts := splitStatements(script)
		if len(stmts) < 2 {
			t.Fatalf("script split into %d statements, want several", len(stmts))
		}
		for _, stmt := range stmts {
			if strings.Contains(stmt, ";") {
				t.Fatalf("statement still holds a semicolon: %q", stmt)
			}
			if strings.Contains(stmt, "--") {
				t.Fatalf("statement still holds a comment: %q", stmt)
			}
			if stmt != strings.TrimSpace(stmt) {
				t.Fatalf("statement is not trimmed: %q", stmt)
			}
		}
	}
	// A literal semicolon survives, a comment one does not.
	got := splitStatements("SELECT 'a;b'; -- note; here\nSELECT 1;")
	if len(got) != 2 || got[0] != "SELECT 'a;b'" || got[1] != "SELECT 1" {
		t.Fatalf("split = %q", got)
	}
}

// A conditional migration is written as a DO block, and its body is full of
// semicolons. Cutting it anywhere but its own end makes PostgreSQL report an
// unterminated dollar-quoted string, and because Migrate runs at startup that
// mistake takes the whole data plane down on restart.
func TestSplitStatementsKeepsDollarQuotedBodiesWhole(t *testing.T) {
	for _, script := range []string{
		"DO $$ SELECT 1; SELECT 2; $$;",
		"DO $ahp$ BEGIN EXECUTE 'ALTER TABLE t DROP CONSTRAINT c'; END $ahp$;",
	} {
		if got := splitStatements(script); len(got) != 1 || got[0] != strings.TrimSuffix(script, ";") {
			t.Fatalf("dollar-quoted body was cut: %q", got)
		}
	}
	// The real upgrade script: one plain statement, then the guarded block.
	stmts := splitStatements(ddlContextGrantAuditUpgrade)
	if len(stmts) != 2 {
		t.Fatalf("upgrade script split into %d statements, want 2: %q", len(stmts), stmts)
	}
	if !strings.HasPrefix(stmts[0], "ALTER TABLE ") || !strings.Contains(stmts[0], "ADD COLUMN IF NOT EXISTS deployment") {
		t.Fatalf("first statement is not the column addition: %q", stmts[0])
	}
	if !strings.HasPrefix(stmts[1], "DO $ahp$") || !strings.HasSuffix(stmts[1], "END $ahp$") {
		t.Fatalf("the DO block is not whole: %q", stmts[1])
	}
	// A dollar sign that opens nothing stays ordinary text.
	if got := splitStatements("SELECT a $$ b $$ c; SELECT 2;"); len(got) != 2 || got[0] != "SELECT a $$ b $$ c" {
		t.Fatalf("untagged dollar text was mishandled: %q", got)
	}
}

// TestIsDuplicateObject names the PostgreSQL class that means "somebody else
// created it first", and nothing else.
func TestIsDuplicateObject(t *testing.T) {
	for _, code := range []string{"42P06", "42P07", "42P16", "42701", "42710"} {
		if !isDuplicateObject(&pgconn.PgError{Code: code}) {
			t.Errorf("code %s must count as already existing", code)
		}
	}
	for _, code := range []string{"22P02", "40001", "57P01"} {
		if isDuplicateObject(&pgconn.PgError{Code: code}) {
			t.Errorf("code %s must not count as already existing", code)
		}
	}
	// A lost race over the catalogs is a duplicate; a violation of a constraint the
	// migration declared is a data conflict and must stay an error.
	if !isDuplicateObject(&pgconn.PgError{Code: "23505", ConstraintName: "pg_type_typname_nsp_index"}) {
		t.Error("a catalog unique violation from a migration race must count as already existing")
	}
	if isDuplicateObject(&pgconn.PgError{Code: "23505", ConstraintName: "artifacts_idem_uidx"}) {
		t.Error("a violation of the migration's own constraint must stay an error")
	}
	if isDuplicateObject(errors.New("wrapped: boom")) {
		t.Error("a non-PG error must not count as already existing")
	}
}
