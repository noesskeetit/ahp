package operations

import (
	"fmt"
	"regexp"
	"strings"
)

const activeGuardIndex = "operations_active_guard_uidx"

// schemaPattern is the strict grammar a service-owned schema name must match:
// the literal "ahp_" prefix, a lowercase letter, then up to 40 lowercase
// letters, digits and underscores. The anchor and the explicit length bound
// keep the name far below PostgreSQL's 63-byte identifier limit, so the
// quoted identifier below can never be truncated into collision.
var schemaPattern = regexp.MustCompile(`^ahp_[a-z][a-z0-9_]{0,40}$`)

// validateSchema rejects any name outside the strict grammar. Schema names
// are the only dynamic fragment of the DDL this package emits, so they are
// validated here and additionally quoted everywhere they are used.
func validateSchema(schema string) error {
	if !schemaPattern.MatchString(schema) {
		return fmt.Errorf("invalid schema name %q: must match ^ahp_[a-z][a-z0-9_]{0,40}$", schema)
	}
	return nil
}

// quoteIdentifier renders an already-validated identifier as a double quoted
// PostgreSQL identifier. Validation guarantees the name contains no quote
// characters, so no escaping is possible or necessary.
func quoteIdentifier(name string) string {
	return `"` + name + `"`
}

// ddl returns the complete, idempotent, non-destructive DDL for this
// service's effect journal. CREATE ... IF NOT EXISTS only: an existing
// schema, tables, indexes and constraints are never altered or dropped.
//
// The operations table is the durable effect journal:
//   - PRIMARY KEY (tenant, deployment, key) is the scope key; a key can never
//     cross a tenant/deployment boundary and a tombstone (fence) permanently
//     occupies the key for later Accept/Dispatch attempts.
//   - The guard partial unique index is the cross-task, cross-deployment
//     mutual exclusion: at most one operation per (tenant, guard) may sit in
//     DISPATCHED or SIDE_EFFECT_UNKNOWN at any time, regardless of subject or
//     operation key, so a guard cannot be bypassed with a different key.
//     Reaching a normal terminal status releases the guard because the row no
//     longer matches the partial index predicate.
func ddl(schema string) string {
	q := quoteIdentifier(schema)
	var b strings.Builder

	b.WriteString("CREATE SCHEMA IF NOT EXISTS " + q + ";\n")

	b.WriteString(`CREATE TABLE IF NOT EXISTS ` + q + `.operations (
    tenant               text        NOT NULL,
    deployment           text        NOT NULL,
    subject              text        NOT NULL,
    key                  text        NOT NULL,
    kind                 text        NOT NULL,
    target               text        NOT NULL,
    guard                text        NOT NULL,
    fingerprint          text        NOT NULL,
    status               text        NOT NULL,
    result               jsonb,
    provenance           jsonb,
    result_digest        text,
    terminal             boolean     NOT NULL DEFAULT false,
    version              bigint      NOT NULL DEFAULT 1,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT operations_scope_key_pk PRIMARY KEY (tenant, deployment, key),
    CONSTRAINT operations_status_ck CHECK (status IN (
        'ACCEPTED', 'DISPATCHED', 'SUCCEEDED', 'FAILED', 'FENCED', 'SIDE_EFFECT_UNKNOWN'
    )),
    CONSTRAINT operations_terminal_ck CHECK (
        (terminal AND status IN ('SUCCEEDED', 'FAILED', 'FENCED')) OR
        (NOT terminal AND status IN ('ACCEPTED', 'DISPATCHED', 'SIDE_EFFECT_UNKNOWN'))
    ),
    CONSTRAINT operations_result_ck CHECK (
        (status IN ('SUCCEEDED', 'FAILED') AND result IS NOT NULL) OR
        (status NOT IN ('SUCCEEDED', 'FAILED') AND result IS NULL)
    )
);
`)

	b.WriteString(`CREATE UNIQUE INDEX IF NOT EXISTS ` + activeGuardIndex + `
    ON ` + q + `.operations (tenant, guard)
    WHERE status IN ('DISPATCHED', 'SIDE_EFFECT_UNKNOWN') AND guard <> '';
`)

	b.WriteString(`CREATE INDEX IF NOT EXISTS operations_scope_subject_idx
    ON ` + q + `.operations (tenant, deployment, subject);
`)

	return b.String()
}
