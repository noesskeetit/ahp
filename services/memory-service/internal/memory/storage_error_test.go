package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// pgErr builds a PostgreSQL error the way the wire delivers it.
func pgErr(code, msg string) error {
	return &pgconn.PgError{Code: code, Message: msg}
}

// TestClassifyStorage pins the error CLASS, because the class is the diagnosis.
// A missing pgvector extension or a missing relation used to arrive as CONFLICT,
// which told the caller "your write collided with committed state" while the
// truth was "this deployment is not provisioned".
func TestClassifyStorage(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantCode   string
		wantReason string
	}{
		{"undefined_table_is_dependency", pgErr("42P01", "relation \"ahp_memory.memories\" does not exist"), contracts.ErrUnavailable, "memory:storage:pgstate_42P01_dependency_missing"},
		{"undefined_schema_is_dependency", pgErr("3F000", `schema "ahp_memory" does not exist`), contracts.ErrUnavailable, "memory:storage:pgstate_3F000_dependency_missing"},
		{"undefined_type_is_dependency", pgErr("42704", `type "vector" does not exist`), contracts.ErrUnavailable, "memory:storage:pgstate_42704_dependency_missing"},
		{"extension_unavailable_is_dependency", pgErr("PP000", `extension "vector" is not available`), contracts.ErrUnavailable, "memory:storage:pgstate_PP000_dependency_missing"},
		{"undefined_function_is_dependency", pgErr("42883", `operator does not exist: <=>`), contracts.ErrUnavailable, "memory:storage:pgstate_42883_dependency_missing"},
		{"missing_database_is_dependency", pgErr("3D000", `database "ahp" does not exist`), contracts.ErrUnavailable, "memory:storage:pgstate_3D000_dependency_missing"},
		{"connection_exception_is_unavailable", pgErr("08006", "connection failure"), contracts.ErrUnavailable, "memory:storage:pgstate_08006_storage_unreachable"},
		{"admin_shutdown_is_unavailable", pgErr("57P01", "admin shutdown"), contracts.ErrUnavailable, "memory:storage:pgstate_57P01_storage_unreachable"},
		{"resource_shortage_is_unavailable", pgErr("53100", "disk full"), contracts.ErrUnavailable, "memory:storage:pgstate_53100_storage_unreachable"},
		{"unique_violation_stays_conflict", pgErr("23505", "duplicate key"), contracts.ErrConflict, "memory:storage:unique_violation"},
		{"serialization_stays_conflict", pgErr("40001", "could not serialize access"), contracts.ErrConflict, "memory:storage:pgstate_40001_retryable_conflict"},
		{"deadlock_stays_conflict", pgErr("40P01", "deadlock detected"), contracts.ErrConflict, "memory:storage:pgstate_40P01_retryable_conflict"},
		{"unknown_sqlstate_is_internal", pgErr("22P02", "invalid input syntax"), "INTERNAL", "memory:storage:pgstate_22P02"},
		{"deadline_is_unavailable", context.DeadlineExceeded, contracts.ErrUnavailable, "memory:storage:storage_timeout"},
		{"unclassified_is_internal", errors.New("something we cannot map"), "INTERNAL", "memory:storage:unclassified"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := storageError("test op", tc.err)
			if got == nil {
				t.Fatal("storageError returned nil for a real error")
			}
			if code := contracts.Code(got); string(code) != tc.wantCode {
				t.Fatalf("code = %s, want %s (err=%v)", code, tc.wantCode, got)
			}
			if reason := contracts.ReasonOf(got); reason != tc.wantReason {
				t.Fatalf("reason = %q, want %q", reason, tc.wantReason)
			}
		})
	}
}

// TestStorageErrorNeverLeaksDriverText keeps the secrecy half of the original
// contract: the driver's message names the DSN user, host and database, so it
// must not reach the surfaced message even now that the class is honest.
func TestStorageErrorNeverLeaksDriverText(t *testing.T) {
	raw := pgErr("42P01", `relation "ahp_memory.memories" does not exist`)
	got := storageError("write", raw).Error()
	if len(got) > 300 || contains(got, "secretuser") {
		t.Fatalf("message leaked or bloated: %s", got)
	}
	if !contains(got, "storage failure") {
		t.Fatalf("message lost its stable shape: %s", got)
	}
}

func contains(h, n string) bool {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}
