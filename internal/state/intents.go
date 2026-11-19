package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Intent lifecycle statuses. They are part of the wire contract.
const (
	IntentRecorded   = "recorded"
	IntentDispatched = "dispatched"
	IntentSucceeded  = "succeeded"
	IntentFailed     = "failed"
	IntentUnknown    = "unknown"
	IntentFenced     = "fenced"
)

// MaxIntentArgs is the strict upper bound of a canonicalized intent argument
// blob. Larger payloads are rejected before any database write.
const MaxIntentArgs = 1 << 20 // 1 MiB

// ErrNotReconcilable reports an attempt to move an unknown intent to a
// completion through the ordinary update path. Reconciliation is the separate
// privileged operation ReconcileIntent and is never reachable from
// UpdateIntent.
var ErrNotReconcilable = contracts.NewError(contracts.ErrConflict,
	"unknown intents reconcile only through the privileged ReconcileIntent")

// Intent is the journaled side effect of one node call inside one run. The
// state layer records the intent and its status; actual effect deduplication
// is owned by the execution layer through the operation key.
type Intent struct {
	ID     string          `json:"id"`
	RunID  string          `json:"run_id"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Digest string          `json:"digest"`
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
}

// MigrateIntents creates the intents table and the idempotency-operations
// registry. It is idempotent and touches only ahp_state objects.
// MigrateIntents applies the intents DDL on the store's own pool. Migrate calls the same body on
// the connection that already holds the migrate advisory lock, which is what
// keeps a MaxConns=1 pool from deadlocking at startup.
func (s *Store) MigrateIntents(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	return s.migrateIntents(ctx, s.pool)
}

func (s *Store) migrateIntents(ctx context.Context, ex ddlExec) error {
	_, err := ex.Exec(ctx, `
CREATE TABLE IF NOT EXISTS ahp_state.intents (
  id         text PRIMARY KEY,
  run_id     text NOT NULL REFERENCES ahp_state.runs(id) ON DELETE CASCADE,
  node       text NOT NULL,
  call       text NOT NULL,
  kind       text NOT NULL,
  target     text NOT NULL,
  digest     text NOT NULL,
  status     text NOT NULL,
  result     jsonb,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT intents_status_check CHECK (status IN
    ('recorded','dispatched','succeeded','failed','unknown','fenced')),
  CONSTRAINT intents_key_uniq UNIQUE (run_id, node, call)
);
CREATE TABLE IF NOT EXISTS ahp_state.id_operations (
  operation_key text PRIMARY KEY,
  resource_id   text NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Scope/locator metadata survives deletion of the intent/run, so operation
-- receipts remain attributable and exportable without guessing opaque IDs.
ALTER TABLE ahp_state.id_operations ADD COLUMN IF NOT EXISTS tenant text;
ALTER TABLE ahp_state.id_operations ADD COLUMN IF NOT EXISTS deployment text;
ALTER TABLE ahp_state.id_operations ADD COLUMN IF NOT EXISTS subject text;
ALTER TABLE ahp_state.id_operations ADD COLUMN IF NOT EXISTS run_id text;
ALTER TABLE ahp_state.id_operations ADD COLUMN IF NOT EXISTS node text;
ALTER TABLE ahp_state.id_operations ADD COLUMN IF NOT EXISTS call text;
UPDATE ahp_state.id_operations o SET tenant=t.tenant,deployment=t.deployment,subject=t.subject,run_id=i.run_id,node=i.node,call=i.call
 FROM ahp_state.intents i JOIN ahp_state.runs r ON r.id=i.run_id JOIN ahp_state.threads t ON t.id=r.thread_id
 WHERE o.resource_id=i.id AND (o.tenant IS NULL OR o.deployment IS NULL OR o.subject IS NULL OR o.run_id IS NULL OR o.node IS NULL OR o.call IS NULL);
CREATE INDEX IF NOT EXISTS id_operations_scope_v2_idx ON ahp_state.id_operations(tenant,deployment,subject);`)
	return err
}

// RecordIntent journals the intent for one node call under the run's current
// lease. The (run_id, node, call) key is idempotent: replaying the identical
// fingerprint returns the original intent, a different fingerprint under the
// same key fails with KEY_REUSE_MISMATCH. All writes happen inside the lease
// transaction, so a stale epoch aborts the whole operation.
func (s *Store) RecordIntent(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, node, call, kind, target string, args json.RawMessage) (Intent, error) {
	if err := scope.Validate(); err != nil {
		return Intent{}, err
	}
	if node == "" || call == "" || kind == "" {
		return Intent{}, contracts.NewError(contracts.ErrInvalid, "intent node, call and kind are required")
	}
	// args is optional on the wire: an absent field is the JSON null document,
	// which still fingerprints stably, so an intent without args is legal.
	if len(args) > 0 && !json.Valid(args) {
		return Intent{}, contracts.NewError(contracts.ErrInvalid, "intent args must be valid JSON")
	}
	if len(args) > MaxIntentArgs {
		return Intent{}, contracts.NewError(contracts.ErrInvalid, "intent args exceed 1MiB")
	}
	digest, err := contracts.Digest(struct {
		Kind   string          `json:"kind"`
		Target string          `json:"target"`
		Args   json.RawMessage `json:"args"`
	}{Kind: kind, Target: target, Args: args})
	if err != nil {
		return Intent{}, err
	}
	opKey := contracts.OperationKey(scope, runID, node, call)

	var out Intent
	err = s.withLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		var id, fp, storedStatus string
		qerr := tx.QueryRow(ctx, `
SELECT id, digest, status FROM ahp_state.intents
WHERE run_id = $1 AND node = $2 AND call = $3`, runID, node, call).Scan(&id, &fp, &storedStatus)
		switch {
		case qerr == nil:
			if fp != digest {
				return contracts.NewError(contracts.ErrKeyReuse, "intent key reused with a different fingerprint")
			}
			out = Intent{ID: id, RunID: runID, Node: node, Call: call, Kind: kind,
				Target: target, Digest: digest, Status: storedStatus}
			return nil
		case !errors.Is(qerr, pgx.ErrNoRows):
			return qerr
		}
		id = contracts.NewID()
		_, qerr = tx.Exec(ctx, `
INSERT INTO ahp_state.intents (id, run_id, node, call, kind, target, digest, status)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
			id, runID, node, call, kind, target, digest, IntentRecorded)
		if qerr != nil {
			if intentUniqueViolation(qerr) {
				return contracts.NewError(contracts.ErrKeyReuse, "intent key reused concurrently")
			}
			return qerr
		}
		_, qerr = tx.Exec(ctx, `
INSERT INTO ahp_state.id_operations (operation_key, resource_id,tenant,deployment,subject,run_id,node,call)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (operation_key) DO NOTHING`, opKey, id, scope.Tenant, scope.Deployment, scope.Subject, runID, node, call)
		if qerr != nil {
			return qerr
		}
		out = Intent{ID: id, RunID: runID, Node: node, Call: call, Kind: kind,
			Target: target, Digest: digest, Status: IntentRecorded}
		return nil
	})
	if err != nil {
		return Intent{}, err
	}
	return out, nil
}

// UpdateIntent advances one intent under the run's current lease.
// Transitions are monotonic: recorded -> dispatched -> terminal, and
// recorded -> fenced. A completion (succeeded/failed) may only be replayed
// with the byte-identical result. An unknown intent can never return to
// dispatched and can never be completed here; only fencing remains allowed.
// Reconciling unknown intents is reserved for ReconcileIntent.
func (s *Store) UpdateIntent(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, node, call, status string, result json.RawMessage) (Intent, error) {
	if err := scope.Validate(); err != nil {
		return Intent{}, err
	}
	if !validIntentStatus(status) {
		return Intent{}, contracts.NewError(contracts.ErrInvalid, "unknown intent status")
	}
	if len(result) > 0 && !json.Valid(result) {
		return Intent{}, contracts.NewError(contracts.ErrInvalid, "intent result must be valid JSON")
	}

	var out Intent
	err := s.withLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		it, err := scanIntent(tx.QueryRow(ctx, `
SELECT id, run_id, node, call, kind, target, digest, status, result
FROM ahp_state.intents WHERE run_id = $1 AND node = $2 AND call = $3`,
			runID, node, call))
		if err != nil {
			return err
		}
		if !allowIntentTransition(it.Status, status) {
			if it.Status == IntentUnknown {
				return ErrNotReconcilable
			}
			return contracts.NewError(contracts.ErrConflict,
				fmt.Sprintf("illegal intent transition %s -> %s", it.Status, status))
		}
		if isIntentCompletion(it.Status) {
			if !sameJSON(it.Result, result) {
				return contracts.NewError(contracts.ErrConflict,
					"intent already completed with a different result")
			}
			out = it
			return nil
		}
		if _, err := tx.Exec(ctx, `
UPDATE ahp_state.intents
SET status = $4, result = COALESCE($5, result), updated_at = clock_timestamp()
WHERE run_id = $1 AND node = $2 AND call = $3`, runID, node, call, status, result); err != nil {
			return err
		}
		it.Status = status
		if len(result) > 0 {
			it.Result = result
		}
		out = it
		return nil
	})
	if err != nil {
		return Intent{}, err
	}
	return out, nil
}

// ReconcileIntent is the privileged reconciliation entry point: it is the ONLY
// path that may move an unknown intent to succeeded or failed, and only with
// the exact original fingerprint. It requires the current lease and is
// intended for a trusted reconciler, never for the normal update path.
func (s *Store) ReconcileIntent(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, node, call, status, digest string, result json.RawMessage) (Intent, error) {
	if err := scope.Validate(); err != nil {
		return Intent{}, err
	}
	if status != IntentSucceeded && status != IntentFailed {
		return Intent{}, contracts.NewError(contracts.ErrInvalid, "reconciliation target must be succeeded or failed")
	}
	var out Intent
	err := s.withLease(ctx, scope, runID, worker, epoch, func(tx pgx.Tx, l *Lease) error {
		it, err := scanIntent(tx.QueryRow(ctx, `
SELECT id, run_id, node, call, kind, target, digest, status, result
FROM ahp_state.intents WHERE run_id = $1 AND node = $2 AND call = $3`,
			runID, node, call))
		if err != nil {
			return err
		}
		if it.Status != IntentUnknown {
			return contracts.NewError(contracts.ErrConflict, "only unknown intents can be reconciled")
		}
		if it.Digest != digest {
			return contracts.NewError(contracts.ErrKeyReuse, "reconciliation fingerprint mismatch")
		}
		if _, err := tx.Exec(ctx, `
UPDATE ahp_state.intents
SET status = $4, result = $5, updated_at = clock_timestamp()
WHERE run_id = $1 AND node = $2 AND call = $3`, runID, node, call, status, result); err != nil {
			return err
		}
		it.Status = status
		if len(result) > 0 {
			it.Result = result
		}
		out = it
		return nil
	})
	if err != nil {
		return Intent{}, err
	}
	return out, nil
}

// GetIntent reads one intent inside the caller scope. Foreign-scope intents
// are reported as NOT_FOUND.
func (s *Store) GetIntent(ctx context.Context, scope contracts.Scope, runID, node, call string) (Intent, error) {
	if err := scope.Validate(); err != nil {
		return Intent{}, err
	}
	row := s.pool.QueryRow(ctx, `
SELECT i.id, i.run_id, i.node, i.call, i.kind, i.target, i.digest, i.status, i.result
FROM ahp_state.intents i
JOIN ahp_state.runs r ON r.id = i.run_id
JOIN ahp_state.threads t ON t.id = r.thread_id
WHERE t.tenant = $1 AND t.deployment = $2 AND t.subject = $3
  AND r.id = $4 AND i.node = $5 AND i.call = $6`,
		scope.Tenant, scope.Deployment, scope.Subject, runID, node, call)
	it, err := scanIntent(row)
	if err != nil {
		return Intent{}, err
	}
	return it, nil
}

type intenter interface{ Scan(dest ...any) error }

func scanIntent(row intenter) (Intent, error) {
	var (
		it     Intent
		result []byte
	)
	err := row.Scan(&it.ID, &it.RunID, &it.Node, &it.Call, &it.Kind, &it.Target,
		&it.Digest, &it.Status, &result)
	if errors.Is(err, pgx.ErrNoRows) {
		return Intent{}, contracts.NewError(contracts.ErrNotFound, "intent not found")
	}
	if err != nil {
		return Intent{}, err
	}
	if len(result) > 0 {
		it.Result = json.RawMessage(result)
	}
	return it, nil
}

// sameJSON compares two result blobs canonically so JSONB normalization on the
// stored side does not break completion replay.
func sameJSON(a, b json.RawMessage) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	ca, err1 := contracts.Digest(json.RawMessage(a))
	cb, err2 := contracts.Digest(json.RawMessage(b))
	if err1 != nil || err2 != nil {
		return string(a) == string(b)
	}
	return ca == cb
}

func validIntentStatus(status string) bool {
	switch status {
	case IntentRecorded, IntentDispatched, IntentSucceeded, IntentFailed, IntentUnknown, IntentFenced:
		return true
	}
	return false
}

func isIntentCompletion(status string) bool {
	return status == IntentSucceeded || status == IntentFailed
}

func allowIntentTransition(from, to string) bool {
	if from == to {
		return true
	}
	switch from {
	case IntentRecorded:
		return to == IntentDispatched || to == IntentFenced
	case IntentDispatched:
		return isIntentCompletion(to) || to == IntentUnknown || to == IntentFenced
	case IntentUnknown:
		return to == IntentFenced
	default:
		return false
	}
}

func intentUniqueViolation(err error) bool {
	var pe interface{ SQLState() string }
	return errors.As(err, &pe) && pe.SQLState() == "23505"
}
