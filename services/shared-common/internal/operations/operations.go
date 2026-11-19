package operations

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Operation statuses. The strings are part of the persisted format.
const (
	StatusAccepted          = "ACCEPTED"
	StatusDispatched        = "DISPATCHED"
	StatusSucceeded         = "SUCCEEDED"
	StatusFailed            = "FAILED"
	StatusFenced            = "FENCED"
	StatusSideEffectUnknown = "SIDE_EFFECT_UNKNOWN"
)

// ErrAlreadyDispatched is returned by Dispatch when another caller already
// holds (or held) the right to send the effect. Callers must fall back to Get
// and wait for the terminal result instead of sending again.
var ErrAlreadyDispatched = contracts.NewError(contracts.ErrConflict, "operation dispatch already granted to another caller")

var errGuardHeld = contracts.NewError(contracts.ErrConflict, "guard is currently held by another operation in flight")

// MaxResultBytes bounds the serialized result stored in the operation ledger.
const MaxResultBytes = 1 << 20

// maxKeyLen bounds the idempotency key. Kind, target, guard and arguments
// are bounded by their own limits.
const (
	maxKeyLen        = 128
	maxKindLen       = 128
	maxTargetLen     = 512
	maxGuardLen      = 512
	maxArgumentsLen  = 1 << 20 // 1 MiB
	maxProvenanceLen = 8 << 10 // 8 KiB
)

// Operation is one journal entry. Result carries the terminal result JSON for
// SUCCEEDED and FAILED entries; Version increments on every state change.
type Operation struct {
	Key    string          `json:"key"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Digest string          `json:"digest"`
	Status string          `json:"status"`
	Guard  string          `json:"guard"`
	Scope  contracts.Scope `json:"scope"`
	Result json.RawMessage `json:"result,omitempty"`
	// Provenance holds the immutable, gateway-authored labels bound to the
	// operation at the moment its effect was dispatched (see
	// DispatchWithProvenance). NULL/absent means "no provenance was ever
	// recorded" -- typically a row written before provenance existed, or one
	// dispatched through a path that carried no verified task binding. It is
	// never synthesised from the current caller.
	Provenance json.RawMessage `json:"provenance,omitempty"`
	Version    int64           `json:"version"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// Request is the Accept-time description of one intended side effect.
// Arguments must be valid JSON; the canonical digest over
// kind/target/arguments/guard forms the operation fingerprint.
type Request struct {
	Key       string          `json:"key"`
	Kind      string          `json:"kind"`
	Target    string          `json:"target"`
	Guard     string          `json:"guard"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// Store is the effect journal for exactly one service schema.
type Store struct {
	pool   *pgxpool.Pool
	schema string
}

// New returns a Store writing to the given dedicated schema. The schema name
// must match ^ahp_[a-z][a-z0-9_]{0,40}$; anything else is rejected here and
// the name is additionally quoted in every statement.
func New(pool *pgxpool.Pool, schema string) (*Store, error) {
	if pool == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "operations: nil pool")
	}
	if err := validateSchema(schema); err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, err.Error())
	}
	return &Store{pool: pool, schema: schema}, nil
}

// Migrate idempotently creates the schema and journal tables. It never
// alters, truncates or drops anything that already exists.
func (s *Store) Migrate(ctx context.Context) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.schema+"/schema-migration"); err != nil {
			return err
		}
		q := quoteIdentifier(s.schema)
		if _, err := tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+q+`; CREATE TABLE IF NOT EXISTS `+q+`.operations_schema_migrations(version text PRIMARY KEY);`); err != nil {
			return err
		}
		var fenceDone bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+q+`.operations_schema_migrations WHERE version='subject-fence-v1')`).Scan(&fenceDone); err != nil {
			return err
		}
		if !fenceDone {
			if _, err := tx.Exec(ctx, ddl(s.schema)+`CREATE TABLE IF NOT EXISTS `+q+`.erased_tenants(tenant text PRIMARY KEY); CREATE TABLE IF NOT EXISTS `+q+`.erased_subjects(tenant text NOT NULL, subject text NOT NULL, PRIMARY KEY(tenant,subject));`); err != nil {
				return fmt.Errorf("operations: migrate schema %s: %w", s.schema, err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO `+q+`.operations_schema_migrations VALUES('subject-fence-v1')`); err != nil {
				return err
			}
		}
		// provenance-v1 is a separate, additive migration so existing
		// journals (which already carry subject-fence-v1) gain the column.
		// ALTER TABLE ... ADD COLUMN IF NOT EXISTS only ever adds a nullable
		// column: no existing row, constraint or index is modified.
		var provDone bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+q+`.operations_schema_migrations WHERE version='provenance-v1')`).Scan(&provDone); err != nil {
			return err
		}
		if provDone {
			return nil
		}
		if _, err := tx.Exec(ctx, `ALTER TABLE `+q+`.operations ADD COLUMN IF NOT EXISTS provenance jsonb`); err != nil {
			return fmt.Errorf("operations: migrate provenance in schema %s: %w", s.schema, err)
		}
		_, err := tx.Exec(ctx, `INSERT INTO `+q+`.operations_schema_migrations VALUES('provenance-v1')`)
		return err
	})
}

// selectColumns is the single read projection for operations rows. The
// fingerprint column is intentionally not selected: callers compare against
// the stored digest via the Digest field, which mirrors it.
const selectColumns = `subject, key, kind, target, guard, status, fingerprint,
    COALESCE(result, 'null'::jsonb)::text, version, created_at, updated_at,
    COALESCE(provenance, 'null'::jsonb)::text`

// resultColumns projects the stored terminal result after an UPDATE that
// sets it. The result is jsonb, so PostgreSQL re-serialises it; every later
// read (and therefore every idempotent replay comparison) sees the exact
// same bytes.
const resultColumns = `COALESCE(result, 'null'::jsonb)::text`

// get reads one operation by scope key. found is false when the key does not
// exist in this tenant/deployment at all. The caller's transaction is used so
// the read participates in the surrounding state transition.
func (s *Store) get(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key string) (Operation, bool, error) {
	return s.read(ctx, tx, scope, key, "")
}

// getForUpdate is get with a row lock, used where the row is about to be
// examined and mutated inside the same transaction.
func (s *Store) getForUpdate(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key string) (Operation, bool, error) {
	return s.read(ctx, tx, scope, key, " FOR UPDATE")
}

// read is the shared row reader; lockSuffix is either empty or the row-level
// lock clause.
func (s *Store) read(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key string, lockSuffix string) (Operation, bool, error) {
	q := `SELECT ` + selectColumns + `
    FROM ` + quoteIdentifier(s.schema) + `.operations
    WHERE tenant = $1 AND deployment = $2 AND key = $3` + lockSuffix
	var (
		op         Operation
		result     []byte
		provenance []byte
	)
	row := tx.QueryRow(ctx, q, scope.Tenant, scope.Deployment, key)
	err := row.Scan(
		&op.Scope.Subject, &op.Key, &op.Kind, &op.Target, &op.Guard,
		&op.Status, &op.Digest, &result, &op.Version,
		&op.CreatedAt, &op.UpdatedAt, &provenance,
	)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return Operation{}, false, nil
	case err != nil:
		return Operation{}, false, fmt.Errorf("operations: get: %w", err)
	}
	op.Scope.Tenant = scope.Tenant
	op.Scope.Deployment = scope.Deployment
	op.Result = json.RawMessage(result)
	if string(provenance) != "null" {
		op.Provenance = json.RawMessage(provenance)
	}
	return op, true, nil
}

// Accept records (or returns) the intent to perform one side effect.
//
// Same key plus identical canonical kind/target/arguments/guard returns the
// existing operation unchanged. A different fingerprint for an existing key
// is KEY_REUSE_MISMATCH. A fenced key is permanently rejected.
func (s *Store) Accept(ctx context.Context, scope contracts.Scope, req Request) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := validateRequest(req); err != nil {
		return Operation{}, err
	}
	fp, err := fingerprint(req)
	if err != nil {
		return Operation{}, err
	}

	var out Operation
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := s.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		// Blind insert: concurrent Accepts of the same key serialise on the
		// primary key and the loser's statement simply does nothing, so no
		// unique violation can ever surface to a caller.
		q := `INSERT INTO ` + quoteIdentifier(s.schema) + `.operations
    (tenant, deployment, subject, key, kind, target, guard, fingerprint, status, terminal)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'ACCEPTED', false)
ON CONFLICT (tenant, deployment, key) DO NOTHING
RETURNING ` + selectColumns
		var inserted Operation
		rowErr := tx.QueryRow(ctx, q,
			scope.Tenant, scope.Deployment, scope.Subject,
			req.Key, req.Kind, req.Target, req.Guard, fp,
		).Scan(
			&inserted.Scope.Subject, &inserted.Key, &inserted.Kind,
			&inserted.Target, &inserted.Guard, &inserted.Status,
			&inserted.Digest, new([]byte), &inserted.Version,
			&inserted.CreatedAt, &inserted.UpdatedAt,
			// A fresh insert can only have NULL provenance; the label is
			// written by the dispatch that follows.
			new([]byte),
		)
		if rowErr == nil {
			inserted.Scope.Tenant = scope.Tenant
			inserted.Scope.Deployment = scope.Deployment
			inserted.Result = nil
			out = inserted
			return nil
		}
		if !errors.Is(rowErr, pgx.ErrNoRows) {
			return fmt.Errorf("operations: accept insert: %w", rowErr)
		}

		// The key already exists (or is a tombstone). Lock the row so the
		// comparison below sees a stable state, then decide without ever
		// leaking cross-subject existence: a foreign key is NOT_FOUND.
		existing, found, err := s.getForUpdate(ctx, tx, scope, req.Key)
		if err != nil {
			return err
		}
		if !found || existing.Scope.Subject != scope.Subject {
			return contracts.NewError(contracts.ErrNotFound,
				"operation not found")
		}
		// A sealed key rejects new requests with CONFLICT regardless of
		// whether it was sealed by a fence tombstone or finalised by a
		// terminal result; the fingerprint comparison must never replay a
		// sealed row. SIDE_EFFECT_UNKNOWN is not sealed: Accept replays it
		// idempotently so callers can poll for the eventual outcome.
		if existing.Status == StatusFenced || existing.Status == StatusSucceeded || existing.Status == StatusFailed {
			return contracts.NewError(contracts.ErrConflict,
				"operation key is sealed and permanently rejects new requests")
		}
		if existing.Digest != fp {
			return contracts.NewError(contracts.ErrKeyReuse,
				"operation key was already used with a different canonical request")
		}
		out = existing
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return out, nil
}

// Get returns one operation. A key from another tenant/deployment or held by
// another subject is reported as NOT_FOUND so existence never leaks across
// subjects.
func (s *Store) Get(ctx context.Context, scope contracts.Scope, key string) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := validateKey(key); err != nil {
		return Operation{}, err
	}
	var out Operation
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		op, found, err := s.get(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found || op.Scope.Subject != scope.Subject {
			return contracts.NewError(contracts.ErrNotFound, "operation not found")
		}
		out = op
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return out, nil
}

// Dispatch atomically grants the right to perform the external effect to
// exactly one caller. The first successful call moves ACCEPTED to DISPATCHED
// and returns the operation; every later call returns ErrAlreadyDispatched
// and the caller must poll Get for the terminal result.
//
// If the operation carries a guard, the transition also acquires the guard:
// it fails with CONFLICT while any operation of the same tenant holds the
// same guard in DISPATCHED or SIDE_EFFECT_UNKNOWN, even under a different key
// or subject, so a guard can never be bypassed with another key.
func (s *Store) Dispatch(ctx context.Context, scope contracts.Scope, key string) (Operation, error) {
	return s.dispatchChecked(ctx, scope, key, nil, nil)
}

// DispatchWithProvenance grants the dispatch right and, in the SAME
// transaction, records the immutable provenance labels for the effect about to
// be sent. Binding at this point -- not at completion -- is what makes the
// labels describe the credential that actually authorised the effect: the
// transition ACCEPTED -> DISPATCHED happens exactly once per key, so the
// labels can never be rewritten by a later replay, a reconciliation or a
// different caller. Provenance must be a JSON object (or nil, which behaves
// exactly like Dispatch); it is stored verbatim and never merged.
func (s *Store) DispatchWithProvenance(ctx context.Context, scope contracts.Scope, key string, prov json.RawMessage) (Operation, error) {
	return s.DispatchCheckedWithProvenance(ctx, scope, key, nil, prov)
}

// DispatchCheckedWithProvenance combines the local dispatch precondition with
// write-once provenance under the same transaction.
func (s *Store) DispatchCheckedWithProvenance(ctx context.Context, scope contracts.Scope, key string, check func(context.Context, pgx.Tx) error, prov json.RawMessage) (Operation, error) {
	if err := validateProvenance(prov); err != nil {
		return Operation{}, err
	}
	return s.dispatchChecked(ctx, scope, key, check, prov)
}

// DispatchChecked evaluates a local transactional precondition before granting
// the dispatch right. The check may lock its authority row so revocation and
// ACCEPTED -> DISPATCHED have one commit order. It must not perform network I/O.
// Authority locks precede the operation-row lock; callers must use that order.
// Once this transaction commits, the effect is in flight; the transaction is
// never kept open while the external effect is sent.
func (s *Store) DispatchChecked(ctx context.Context, scope contracts.Scope, key string, check func(context.Context, pgx.Tx) error) (Operation, error) {
	return s.dispatchChecked(ctx, scope, key, check, nil)
}

// dispatchChecked is the shared dispatch grant; prov is written atomically
// with the ACCEPTED -> DISPATCHED transition when non-nil.
func (s *Store) dispatchChecked(ctx context.Context, scope contracts.Scope, key string, check func(context.Context, pgx.Tx) error, prov json.RawMessage) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := validateKey(key); err != nil {
		return Operation{}, err
	}
	var out Operation
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.CheckTenantWritable(ctx, tx, scope.Tenant); err != nil {
			return err
		}
		if err := s.CheckSubjectWritable(ctx, tx, scope.Tenant, scope.Subject); err != nil {
			return err
		}
		if check != nil {
			if err := check(ctx, tx); err != nil {
				return err
			}
		}
		// Lock the row so the status check, the guard probe and the
		// ACCEPTED=>DISPATCHED update all observe one stable state.
		op, found, err := s.getForUpdate(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found || op.Scope.Subject != scope.Subject {
			return contracts.NewError(contracts.ErrNotFound, "operation not found")
		}
		switch op.Status {
		case StatusAccepted:
			// proceed
		case StatusDispatched, StatusSideEffectUnknown:
			return ErrAlreadyDispatched
		default:
			// The operation reached a terminal state (SUCCEEDED/FAILED/FENCED):
			// the dispatch right was already taken and resolved. Per the
			// Dispatch contract callers must not re-invoke the side effect but
			// poll Get for the stored terminal result, so we surface the same
			// sentinel used for in-flight re-dispatch. It carries the CONFLICT
			// code, preserving error-code based callers.
			return ErrAlreadyDispatched
		}

		// Lock an existing holder before checking the guard. When both
		// dispatches see no holder, the partial unique index arbitrates.
		if op.Guard != "" {
			var holder string
			q := `SELECT key FROM ` + quoteIdentifier(s.schema) + `.operations
    WHERE tenant = $1 AND guard = $2
      AND status IN ('DISPATCHED', 'SIDE_EFFECT_UNKNOWN')
      AND NOT (deployment = $3 AND key = $4)
    LIMIT 1
    FOR UPDATE`
			err := tx.QueryRow(ctx, q, scope.Tenant, op.Guard, scope.Deployment, key).Scan(&holder)
			switch {
			case err == nil:
				return errGuardHeld
			case errors.Is(err, pgx.ErrNoRows):
				// guard free
			default:
				return fmt.Errorf("operations: dispatch guard check: %w", err)
			}
		}

		// COALESCE keeps provenance write-once in practice: the guarded
		// WHERE clause lets only the ACCEPTED -> DISPATCHED transition
		// through, and a nil prov never erases labels that exist.
		q := `UPDATE ` + quoteIdentifier(s.schema) + `.operations
    SET status = 'DISPATCHED', terminal = false, version = version + 1, updated_at = now(),
        provenance = COALESCE($4::jsonb, provenance)
    WHERE tenant = $1 AND deployment = $2 AND key = $3 AND status = 'ACCEPTED'`
		tag, err := tx.Exec(ctx, q, scope.Tenant, scope.Deployment, key, provParam(prov))
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == activeGuardIndex {
				return errGuardHeld
			}
			return fmt.Errorf("operations: dispatch: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Lost the race to another dispatcher; they hold the grant.
			return ErrAlreadyDispatched
		}
		updated, found, err := s.get(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrConflict, "operation vanished during dispatch")
		}
		out = updated
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return out, nil
}

// Complete records the outcome of a dispatched effect. Only DISPATCHED rows
// transition, to SUCCEEDED or FAILED, and the result is stored durably. A
// repeated Complete with the identical terminal status and byte-identical
// result is idempotent and returns the stored operation; any other repeat
// (different status or result) is rejected. Reaching a terminal status
// releases the operation's guard.
func (s *Store) Complete(ctx context.Context, scope contracts.Scope, key string, result json.RawMessage, failed bool) (Operation, error) {
	return s.finish(ctx, scope, key, result, failed, false)
}

// Reconcile drives a SIDE_EFFECT_UNKNOWN operation to a terminal status once
// the true outcome has been established out of band. It is reserved for the
// trusted service/controller path; HTTP authorisation is enforced upstream.
// No other status may be reconciled and terminal rows are never rewritten.
func (s *Store) Reconcile(ctx context.Context, scope contracts.Scope, key string, result json.RawMessage, failed bool) (Operation, error) {
	return s.finish(ctx, scope, key, result, failed, true)
}

// finish implements the shared terminal transition for Complete and
// Reconcile. fromUnknown selects which non-terminal status is accepted.
func (s *Store) finish(ctx context.Context, scope contracts.Scope, key string, result json.RawMessage, failed bool, fromUnknown bool) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := validateKey(key); err != nil {
		return Operation{}, err
	}
	if err := validateResult(result); err != nil {
		return Operation{}, err
	}
	status := StatusSucceeded
	if failed {
		status = StatusFailed
	}
	from := StatusDispatched
	action := "complete"
	if fromUnknown {
		from = StatusSideEffectUnknown
		action = "reconcile"
	}

	var out Operation
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// Lock the row first: the status check and the terminal update below
		// must observe one stable state, so a concurrent Fence either
		// completes before us (we see SIDE_EFFECT_UNKNOWN and reject
		// Complete, leaving reconciliation to the trusted path) or blocks
		// behind this transaction. A late dispatch response can therefore
		// never bypass the guard: only Reconcile finalises an unknown row,
		// and that transition is what releases the guard.
		op, found, err := s.getForUpdate(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found || op.Scope.Subject != scope.Subject {
			return contracts.NewError(contracts.ErrNotFound, "operation not found")
		}
		switch op.Status {
		case from:
			// proceed
		case StatusSucceeded, StatusFailed:
			// Terminal rows are final. Reconcile never replays: a final
			// status is always reached through Complete, so any Reconcile
			// attempt on a terminal row is a caller bug and must surface.
			// Complete replays idempotently only for the identical
			// canonical terminal result.
			if !fromUnknown && op.Status == status && sameJSON(op.Result, result) {
				out = op // idempotent replay of the same terminal result
				return nil
			}
			return contracts.NewError(contracts.ErrConflict,
				"operation already holds a terminal result and cannot be "+action+"d")
		default:
			return contracts.NewError(contracts.ErrConflict,
				"operation status "+op.Status+" cannot be "+action+"d")
		}

		q := `UPDATE ` + quoteIdentifier(s.schema) + `.operations
    SET status = $4, terminal = true, result = $5::jsonb,
        version = version + 1, updated_at = now()
    WHERE tenant = $1 AND deployment = $2 AND key = $3 AND status = $6`
		var stored []byte
		rowErr := tx.QueryRow(ctx, q+` RETURNING `+resultColumns,
			scope.Tenant, scope.Deployment, key, status, []byte(result), from,
		).Scan(&stored)
		if errors.Is(rowErr, pgx.ErrNoRows) {
			return contracts.NewError(contracts.ErrConflict,
				"operation changed concurrently; re-read before retrying")
		}
		if rowErr != nil {
			return fmt.Errorf("operations: %s: %w", action, rowErr)
		}
		updated, found, err := s.get(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrConflict, "operation vanished during "+action)
		}
		updated.Result = json.RawMessage(stored)
		out = updated
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return out, nil
}

// Fence seals an operation key.
//
//   - Absent key: a FENCED tombstone is created that permanently rejects any
//     later Accept or Dispatch on that key.
//   - ACCEPTED: the row becomes FENCED (terminal) and its guard is released.
//   - DISPATCHED: the row becomes SIDE_EFFECT_UNKNOWN; the guard stays held
//     because the true outcome of the external effect is unresolved.
//   - SIDE_EFFECT_UNKNOWN: unchanged (idempotent); only Reconcile finalises.
//   - Terminal statuses (SUCCEEDED/FAILED/FENCED): returned unchanged; a
//     fence never overwrites a final status.
func (s *Store) Fence(ctx context.Context, scope contracts.Scope, key string) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := validateKey(key); err != nil {
		return Operation{}, err
	}
	var out Operation
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		op, found, err := s.getForUpdate(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found {
			// Tombstone: occupy the scope key so Accept and Dispatch can
			// never claim it again.
			q := `INSERT INTO ` + quoteIdentifier(s.schema) + `.operations
    (tenant, deployment, subject, key, kind, target, guard, fingerprint, status, terminal)
VALUES ($1, $2, $3, $4, '', '', '', '', 'FENCED', true)
ON CONFLICT (tenant, deployment, key) DO NOTHING
RETURNING version`
			var version int64
			rowErr := tx.QueryRow(ctx, q,
				scope.Tenant, scope.Deployment, scope.Subject, key,
			).Scan(&version)
			if errors.Is(rowErr, pgx.ErrNoRows) {
				// Raced with a real Accept; lock and fence that row.
				op, found, err = s.getForUpdate(ctx, tx, scope, key)
				if err != nil {
					return err
				}
				if !found {
					return contracts.NewError(contracts.ErrConflict, "operation vanished during fence")
				}
			} else if rowErr != nil {
				return fmt.Errorf("operations: fence tombstone: %w", rowErr)
			} else {
				out = Operation{
					Key: key, Status: StatusFenced, Scope: scope,
					Version: version,
				}
				return nil
			}
		}
		if op.Scope.Subject != scope.Subject {
			return contracts.NewError(contracts.ErrNotFound, "operation not found")
		}
		switch op.Status {
		case StatusAccepted:
			q := `UPDATE ` + quoteIdentifier(s.schema) + `.operations
    SET status = 'FENCED', terminal = true, version = version + 1, updated_at = now()
    WHERE tenant = $1 AND deployment = $2 AND key = $3 AND status = 'ACCEPTED'`
			if _, err := tx.Exec(ctx, q, scope.Tenant, scope.Deployment, key); err != nil {
				return fmt.Errorf("operations: fence accepted: %w", err)
			}
		case StatusDispatched:
			q := `UPDATE ` + quoteIdentifier(s.schema) + `.operations
    SET status = 'SIDE_EFFECT_UNKNOWN', terminal = false, version = version + 1, updated_at = now()
    WHERE tenant = $1 AND deployment = $2 AND key = $3 AND status = 'DISPATCHED'`
			if _, err := tx.Exec(ctx, q, scope.Tenant, scope.Deployment, key); err != nil {
				return fmt.Errorf("operations: fence dispatched: %w", err)
			}
		case StatusSideEffectUnknown, StatusSucceeded, StatusFailed, StatusFenced:
			// Idempotent: never overwrite an unknown or final status.
			out = op
			return nil
		}
		updated, found, err := s.get(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrConflict, "operation vanished during fence")
		}
		out = updated
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return out, nil
}

// MarkUnknown moves a DISPATCHED operation to SIDE_EFFECT_UNKNOWN without
// releasing its guard: the outcome of the external effect could not be
// established and only Reconcile or Fence may touch the row afterwards.
func (s *Store) MarkUnknown(ctx context.Context, scope contracts.Scope, key string) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := validateKey(key); err != nil {
		return Operation{}, err
	}
	var out Operation
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		op, found, err := s.getForUpdate(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found || op.Scope.Subject != scope.Subject {
			return contracts.NewError(contracts.ErrNotFound, "operation not found")
		}
		switch op.Status {
		case StatusDispatched:
			q := `UPDATE ` + quoteIdentifier(s.schema) + `.operations
    SET status = 'SIDE_EFFECT_UNKNOWN', terminal = false, version = version + 1, updated_at = now()
    WHERE tenant = $1 AND deployment = $2 AND key = $3 AND status = 'DISPATCHED'`
			if _, err := tx.Exec(ctx, q, scope.Tenant, scope.Deployment, key); err != nil {
				return fmt.Errorf("operations: mark unknown: %w", err)
			}
		case StatusSideEffectUnknown:
			out = op // idempotent
			return nil
		default:
			return contracts.NewError(contracts.ErrConflict,
				"operation status "+op.Status+" cannot be marked unknown")
		}
		updated, found, err := s.get(ctx, tx, scope, key)
		if err != nil {
			return err
		}
		if !found {
			return contracts.NewError(contracts.ErrConflict, "operation vanished during mark unknown")
		}
		out = updated
		return nil
	})
	if err != nil {
		return Operation{}, err
	}
	return out, nil
}

// sameJSON reports whether two JSON documents carry the same canonical
// value. Stored results come back from jsonb re-serialised, so byte
// comparison would spuriously reject an idempotent replay.
func sameJSON(a, b json.RawMessage) bool {
	da, err := contracts.Digest(json.RawMessage(a))
	if err != nil {
		return false
	}
	db, err := contracts.Digest(json.RawMessage(b))
	if err != nil {
		return false
	}
	return da == db
}

// fingerprint is the canonical digest over the request fields that define
// the intended effect. Two Accepts share a key legally only when their
// fingerprints match.
//
// Arguments are handed to contracts.Digest as raw JSON, never decoded into
// Go values first: the canonicaliser keeps exact numeric values (so
// 9007199254740992 and 9007199254740993 produce different fingerprints) and
// rejects duplicate object keys instead of silently keeping the last one.
func fingerprint(req Request) (string, error) {
	args := json.RawMessage(req.Arguments)
	if len(args) == 0 {
		args = json.RawMessage("null")
	}
	d, err := contracts.Digest(map[string]any{
		"kind":      req.Kind,
		"target":    req.Target,
		"guard":     req.Guard,
		"arguments": args,
	})
	if err != nil {
		return "", contracts.NewError(contracts.ErrInvalid, "cannot fingerprint operation: "+err.Error())
	}
	return d, nil
}

// validateRequest enforces presence and size bounds on every request field.
func validateRequest(req Request) error {
	if err := validateKey(req.Key); err != nil {
		return err
	}
	if req.Kind == "" {
		return contracts.NewError(contracts.ErrInvalid, "operation kind must not be empty")
	}
	if len(req.Kind) > maxKindLen {
		return contracts.NewError(contracts.ErrInvalid, "operation kind exceeds 128 bytes")
	}
	if req.Target == "" {
		return contracts.NewError(contracts.ErrInvalid, "operation target must not be empty")
	}
	if len(req.Target) > maxTargetLen {
		return contracts.NewError(contracts.ErrInvalid, "operation target exceeds 512 bytes")
	}
	if len(req.Guard) > maxGuardLen {
		return contracts.NewError(contracts.ErrInvalid, "operation guard exceeds 512 bytes")
	}
	if len(req.Arguments) > maxArgumentsLen {
		return contracts.NewError(contracts.ErrInvalid, "operation arguments exceed 1 MiB")
	}
	if len(req.Arguments) > 0 && !json.Valid(req.Arguments) {
		return contracts.NewError(contracts.ErrInvalid, "operation arguments must be valid JSON")
	}
	// Reject duplicate object keys up front: json.Valid accepts them, but
	// the canonicaliser refuses them, and Accept must never store a request
	// whose fingerprint cannot be recomputed.
	if len(req.Arguments) > 0 {
		if _, err := contracts.Digest(json.RawMessage(req.Arguments)); err != nil {
			return contracts.NewError(contracts.ErrInvalid,
				"operation arguments must be canonicalisable JSON: duplicate keys or unsupported values are rejected")
		}
	}
	return nil
}

func validateKey(key string) error {
	if key == "" {
		return contracts.NewError(contracts.ErrInvalid, "operation key is required")
	}
	if len(key) > maxKeyLen {
		return contracts.NewError(contracts.ErrInvalid, "operation key exceeds 128 bytes")
	}
	return nil
}

func validateResult(result json.RawMessage) error {
	if len(result) == 0 || !json.Valid(result) {
		return contracts.NewError(contracts.ErrInvalid, "operation result must be non-empty valid JSON")
	}
	if len(result) > MaxResultBytes {
		return contracts.NewError(contracts.ErrInvalid, "operation result exceeds 1 MiB")
	}
	return nil
}

// validateProvenance accepts only a JSON object (or nothing at all). Provenance
// is gateway-authored label material, so a scalar or array is a caller bug and
// is refused rather than stored.
func validateProvenance(prov json.RawMessage) error {
	if len(prov) == 0 {
		return nil
	}
	if !json.Valid(prov) {
		return contracts.NewError(contracts.ErrInvalid, "operation provenance must be valid JSON")
	}
	var obj map[string]any
	if err := json.Unmarshal(prov, &obj); err != nil {
		return contracts.NewError(contracts.ErrInvalid, "operation provenance must be a JSON object")
	}
	if len(prov) > maxProvenanceLen {
		return contracts.NewError(contracts.ErrInvalid, "operation provenance exceeds 8 KiB")
	}
	return nil
}

// provParam maps provenance to a query parameter, turning "absent" into a real
// SQL NULL so COALESCE keeps any labels already stored on the row.
func provParam(prov json.RawMessage) any {
	if len(prov) == 0 {
		return nil
	}
	return []byte(prov)
}
