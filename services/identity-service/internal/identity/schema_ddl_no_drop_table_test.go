package identity

import (
	"strings"
	"testing"
)

// Review condition C2 of the schema-version-8 lane. Bumping schemaVersion makes an
// already-migrated installation run schemaDDL again, in front of live traffic. The
// rule this file states is the one that decides whether that is a quiet operation:
// no STATEMENT of this revision's DDL may drop a table, because DROP TABLE is the
// only kind of statement in the migration that queues behind a reader (measured on a
// live server: DROP TABLE behind a transaction holding ACCESS SHARE dies with a lock
// timeout, CREATE TABLE IF NOT EXISTS beside the same reader does not wait). A
// replacement the revision performs has to be decided BEFORE the exclusive window
// opens, from a catalog read, and sent only to an installation that needs it.
//
// Prose is not a statement, so the leg reads the DDL with its comments stripped.

// stripSQLComments removes `--` line comments so a leg can judge the statements a
// migration sends rather than the prose that explains them.
func stripSQLComments(ddl string) string {
	var kept []string
	for _, line := range strings.Split(ddl, "\n") {
		if i := strings.Index(line, "--"); i >= 0 {
			line = line[:i]
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

// C2, structural half. No statement of this revision's DDL may drop a table: the
// only replacement the revision performs is decided ahead of the DDL, in Go, against
// the catalog. This leg is what reddens if a DROP TABLE is ever written back into
// schemaDDL, where the decision would again be made inside the exclusive window.
func TestReapplyingTheIdentityDDLContainsNoDropTable(t *testing.T) {
	if strings.Contains(strings.ToUpper(stripSQLComments(schemaDDL)), "DROP TABLE") {
		t.Fatalf("DEF-C2: schemaDDL issues a DROP TABLE, so every re-application of the DDL requests ACCESS EXCLUSIVE on a table live traffic may be reading; decide the replacement in Migrate from a catalog read instead")
	}
}
