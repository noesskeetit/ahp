package memory

import (
	"context"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

// beginMutation reserves the caller's key inside the SAME transaction as its
// write. ON CONFLICT waits for any concurrent owner to commit or roll back.
// Only identifiers/counts are retained here; deleted memory text is not copied
// into an idempotency journal.
func (s *Store) beginMutation(ctx context.Context, scope contracts.Scope, key, digest string) (pgx.Tx, string, int64, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, "", 0, false, storageError("mutation begin", err)
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)); err != nil {
		tx.Rollback(ctx)
		return nil, "", 0, false, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO ahp_memory.operations (tenant,deployment,subject,op_key,digest,status) VALUES ($1,$2,$3,$4,$5,'ACCEPTED') ON CONFLICT DO NOTHING`, scope.Tenant, scope.Deployment, scope.Subject, key, digest)
	if err != nil {
		tx.Rollback(ctx)
		return nil, "", 0, false, storageError("mutation reserve", err)
	}
	var saved, id, subject, status string
	var count int64
	err = tx.QueryRow(ctx, `SELECT digest,result_id,result_count,subject,status FROM ahp_memory.operations WHERE tenant=$1 AND deployment=$2 AND op_key=$3 FOR UPDATE`, scope.Tenant, scope.Deployment, key).Scan(&saved, &id, &count, &subject, &status)
	if err != nil {
		tx.Rollback(ctx)
		return nil, "", 0, false, storageError("mutation read", err)
	}
	if subject != scope.Subject {
		tx.Rollback(ctx)
		return nil, "", 0, false, contracts.NewError(contracts.ErrKeyReuse, "memory: operation key is already reserved")
	}
	if status == "FENCED" {
		tx.Rollback(ctx)
		return nil, "", 0, false, contracts.NewReasoned(contracts.ErrConflict, "FENCED", "memory: operation was fenced before commit")
	}
	if saved != digest {
		tx.Rollback(ctx)
		return nil, "", 0, false, contracts.NewError(contracts.ErrKeyReuse, "memory: operation key reused with different arguments")
	}
	return tx, id, count, tag.RowsAffected() == 0, nil
}

func finishMutation(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key, id string, count int64) error {
	_, err := tx.Exec(ctx, `UPDATE ahp_memory.operations SET result_id=$5,result_count=$6,status='SUCCEEDED' WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND op_key=$4 AND status='ACCEPTED'`, scope.Tenant, scope.Deployment, scope.Subject, key, id, count)
	if err != nil {
		return storageError("mutation finish", err)
	}
	return storageError("mutation commit", tx.Commit(ctx))
}

// Operation contains only reconciliation metadata. It never copies memory
// content, provenance, evidence, or a fingerprint into a second journal.
type Operation struct {
	OpKey       string `json:"op_key"`
	Status      string `json:"status"`
	ResultID    string `json:"result_id,omitempty"`
	ResultCount int64  `json:"result_count"`
}

// GetOperation addresses the key, while Get/Read addresses a memory record.
// Receipts are deployment-wide but readable only by their originating subject.
func (s *Store) GetOperation(ctx context.Context, scope contracts.Scope, key string) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := requireOpKey(key); err != nil {
		return Operation{}, err
	}
	if err := s.authorize(ctx, scope, CapabilityGateway, "memory:operation"); err != nil {
		return Operation{}, err
	}
	var out Operation
	var subject string
	err := s.pool.QueryRow(ctx, `SELECT op_key,status,result_id,result_count,subject FROM ahp_memory.operations WHERE tenant=$1 AND deployment=$2 AND op_key=$3`, scope.Tenant, scope.Deployment, key).Scan(&out.OpKey, &out.Status, &out.ResultID, &out.ResultCount, &subject)
	if err == pgx.ErrNoRows || err == nil && subject != scope.Subject {
		return Operation{}, contracts.NewError(contracts.ErrNotFound, "memory: operation not found")
	}
	if err != nil {
		return Operation{}, storageError("operation read", err)
	}
	return s.visibleOperation(ctx, scope, out), nil
}

func (s *Store) visibleOperation(ctx context.Context, scope contracts.Scope, out Operation) Operation {
	// The terminal outcome remains available after deletion/revocation, but an
	// object reference is emitted only under its current read and label rules.
	if out.ResultID != "" {
		if _, err := s.Get(ctx, scope, out.ResultID); err != nil {
			out.ResultID = ""
		}
	}
	return out
}

// Fence closes a not-yet-committed key forever. The unique key and row lock
// serialize it with the actual write: a committed write wins as SUCCEEDED;
// an earlier fence wins as FENCED. This owner has no asynchronous external
// mutation dispatch, so no ambiguous external effect is invented here.
func (s *Store) Fence(ctx context.Context, scope contracts.Scope, key string) (Operation, error) {
	if err := scope.Validate(); err != nil {
		return Operation{}, err
	}
	if err := requireOpKey(key); err != nil {
		return Operation{}, err
	}
	if err := s.authorize(ctx, scope, CapabilityGateway, "memory:operation"); err != nil {
		return Operation{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Operation{}, storageError("fence begin", err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(hashtextextended($1,0))`, tenantFenceKey(scope.Tenant)); err != nil {
		return Operation{}, err
	}
	if err = denyErasedTenant(ctx, tx, scope.Tenant); err != nil {
		return Operation{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO ahp_memory.operations(tenant,deployment,subject,op_key,digest,status) VALUES($1,$2,$3,$4,'','FENCED') ON CONFLICT DO NOTHING`, scope.Tenant, scope.Deployment, scope.Subject, key); err != nil {
		return Operation{}, storageError("fence reserve", err)
	}
	var out Operation
	var subject string
	err = tx.QueryRow(ctx, `SELECT op_key,status,result_id,result_count,subject FROM ahp_memory.operations WHERE tenant=$1 AND deployment=$2 AND op_key=$3 FOR UPDATE`, scope.Tenant, scope.Deployment, key).Scan(&out.OpKey, &out.Status, &out.ResultID, &out.ResultCount, &subject)
	if err != nil {
		return Operation{}, storageError("fence receipt", err)
	}
	if subject != scope.Subject {
		return Operation{}, contracts.NewError(contracts.ErrNotFound, "memory: operation not found")
	}
	if err = tx.Commit(ctx); err != nil {
		return Operation{}, storageError("fence commit", err)
	}
	return s.visibleOperation(ctx, scope, out), nil
}

func mutationDigest(op, id string, payload any) (string, error) {
	return contracts.Digest(map[string]any{"operation": op, "id": id, "payload": payload})
}

// A committed proposal may already have been reviewed and its predecessor
// superseded. Resolve its original key before re-validating a new correction.
// Current source visibility and labels are still checked on every replay.
func (s *Store) proposalReplay(ctx context.Context, scope contracts.Scope, key, digest string) (ProposeResult, bool, error) {
	var subject, saved, id, status string
	err := s.pool.QueryRow(ctx, `SELECT subject,digest,result_id,status FROM ahp_memory.operations WHERE tenant=$1 AND deployment=$2 AND op_key=$3`, scope.Tenant, scope.Deployment, key).Scan(&subject, &saved, &id, &status)
	if err == pgx.ErrNoRows {
		return ProposeResult{}, false, nil
	}
	if err != nil {
		return ProposeResult{}, false, storageError("proposal operation read", err)
	}
	if subject != scope.Subject {
		return ProposeResult{}, false, contracts.NewError(contracts.ErrKeyReuse, "memory: operation key is already reserved")
	}
	if status == "FENCED" {
		return ProposeResult{}, false, contracts.NewReasoned(contracts.ErrConflict, "FENCED", "memory: operation was fenced before commit")
	}
	if saved != digest {
		return ProposeResult{}, false, contracts.NewError(contracts.ErrKeyReuse, "memory: operation key reused with different arguments")
	}
	if status != "SUCCEEDED" {
		return ProposeResult{}, false, contracts.NewError(contracts.ErrConflict, "memory: operation is not committed")
	}
	m, err := s.Get(ctx, scope, id)
	return ProposeResult{Memory: m, Replay: true}, true, err
}

func sameMemoryNamespace(a, b Memory) bool {
	if a.Kind != b.Kind {
		return false
	}
	switch a.Kind {
	case KindWorkspace:
		return a.Workspace == b.Workspace
	case KindUser:
		return a.Subject == b.Subject
	case KindAgent:
		return a.Deployment == b.Deployment
	default:
		return false
	}
}

func readMutationMemory(ctx context.Context, tx pgx.Tx, tenant, id string) (Memory, error) {
	m, err := scanMemory(tx.QueryRow(ctx, `SELECT `+columns+` FROM ahp_memory.memories WHERE tenant=$1 AND id=$2 FOR UPDATE`, tenant, id))
	if err == pgx.ErrNoRows {
		return Memory{}, contracts.NewError(contracts.ErrNotFound, "memory: not found")
	}
	if err != nil {
		return Memory{}, storageError("mutation record", err)
	}
	return m, nil
}

// The lock is tenant+subject wide: proposals from any deployment are ordered
// against the erasure tombstone and cannot appear after the deletion commits.
func lockSubjectMutation(ctx context.Context, tx pgx.Tx, tenant, subject string) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "memory-erasure/"+tenant+"/"+subject)
	return err
}
