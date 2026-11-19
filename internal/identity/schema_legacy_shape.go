package identity

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// An installation that predates the jti-shaped browser token bindings can still
// hold the abandoned token_hash shape of that table. schemaDDL recreates the table
// in its current shape, so the legacy shape has to be removed first -- but that
// removal is a DROP TABLE, and DROP TABLE is the one statement in the migration
// that takes ACCESS EXCLUSIVE and therefore queues behind any reader. Issuing it
// unconditionally would make every migration of a healthy database a stall in
// front of live traffic; that is why the decision moved out of schemaDDL and into
// Migrate, ahead of the exclusive window.
//
// The rule: ask the catalog first, one read that takes no lock on the table, and
// send the DROP only to an installation that actually holds the shape being
// retired. A healthy installation never sees the statement.
const legacyBrowserTokenBindingsDropDDL = "DROP TABLE IF EXISTS ahp_identity.browser_token_bindings"

// legacyBrowserTokenBindingsShapeQuery is the pre-check. information_schema is a
// view over the catalogs, so this reads catalog metadata and takes no lock on
// ahp_identity.browser_token_bindings itself.
const legacyBrowserTokenBindingsShapeQuery = `select exists (
	select 1 from information_schema.columns
	 where table_schema = 'ahp_identity' and table_name = 'browser_token_bindings'
	   and column_name = 'token_hash')`

// rowQuerier and ddlExecer are the two capabilities Migrate has on its locked
// connection. They are named here so the pre-check can be exercised on the real
// connection without a second, more permissive way to reach the schema.
type (
	rowQuerier interface {
		QueryRow(ctx context.Context, query string, args ...any) pgx.Row
	}
	ddlExecer interface {
		Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
	}
)

// holdsLegacyBrowserTokenBindings reports whether this installation still carries
// the token_hash shape. It answers a question about the catalog and locks nothing
// belonging to live traffic, which is what makes it safe to ask before the DDL.
func holdsLegacyBrowserTokenBindings(ctx context.Context, q rowQuerier) (bool, error) {
	var legacy bool
	if err := q.QueryRow(ctx, legacyBrowserTokenBindingsShapeQuery).Scan(&legacy); err != nil {
		return false, fmt.Errorf("identity: read the browser token bindings shape: %w", err)
	}
	return legacy, nil
}

// retireLegacyBrowserTokenBindings drops the abandoned shape so schemaDDL can
// recreate the table in its current one. It is called only when the pre-check said
// the shape is present, and it is called before any other DDL of the revision, so
// the exclusive lock is taken once, at the front of the window, for the case that
// genuinely needs it.
func retireLegacyBrowserTokenBindings(ctx context.Context, e ddlExecer) error {
	if _, err := e.Exec(ctx, legacyBrowserTokenBindingsDropDDL); err != nil {
		return fmt.Errorf("identity: retire the legacy browser token bindings shape: %w", err)
	}
	return nil
}
