package state

import (
	"context"
	"errors"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
)

const deletionSchema = `CREATE TABLE IF NOT EXISTS ahp_state.deletions (
 id text PRIMARY KEY, tenant text NOT NULL,deployment text NOT NULL,subject text NOT NULL,
 operation text NOT NULL,resource text NOT NULL,
 context_ids text[] NOT NULL DEFAULT '{}',task_ids text[] NOT NULL DEFAULT '{}',
 deleted_count bigint NOT NULL DEFAULT 0,held_count bigint NOT NULL DEFAULT 0,
 artifacts_done boolean NOT NULL DEFAULT false,memory_done boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(),completed_at timestamptz,
 UNIQUE(tenant,deployment,subject,operation,resource)
);`

// Deletion is a durable receipt of the State transaction. It carries only
// scope and dependency identifiers, never checkpoints or user content.
type Deletion struct {
	ID            string
	Scope         contracts.Scope
	Operation     string
	Resource      string
	ContextIDs    []string
	TaskIDs       []string
	DeletedCount  int64
	HeldCount     int64
	ArtifactsDone bool
	MemoryDone    bool
	Completed     bool
}

func (s *Store) GetDeletion(ctx context.Context, scope contracts.Scope, operation, resource string) (Deletion, error) {
	if err := scope.Validate(); err != nil {
		return Deletion{}, err
	}
	d := Deletion{Scope: scope, Operation: operation, Resource: resource}
	err := s.pool.QueryRow(ctx, `SELECT id,context_ids,task_ids,deleted_count,held_count,artifacts_done,memory_done,completed_at IS NOT NULL FROM ahp_state.deletions WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND operation=$4 AND resource=$5`, scope.Tenant, scope.Deployment, scope.Subject, operation, resource).Scan(&d.ID, &d.ContextIDs, &d.TaskIDs, &d.DeletedCount, &d.HeldCount, &d.ArtifactsDone, &d.MemoryDone, &d.Completed)
	if errors.Is(err, pgx.ErrNoRows) {
		return Deletion{}, contracts.NewError(contracts.ErrNotFound, "state: deletion receipt not found")
	}
	return d, err
}

// CompleteDeletionStep persists acknowledgements independently. Replaying a
// failed request invokes only unfinished owner APIs with the same receipt ID.
func (s *Store) CompleteDeletionStep(ctx context.Context, scope contracts.Scope, id, step string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	var query string
	switch step {
	case "artifacts":
		query = `UPDATE ahp_state.deletions SET artifacts_done=true WHERE id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4`
	case "memory":
		query = `UPDATE ahp_state.deletions SET memory_done=true WHERE id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4`
	case "complete":
		query = `UPDATE ahp_state.deletions SET completed_at=now() WHERE id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4 AND artifacts_done AND memory_done`
	default:
		return contracts.NewError(contracts.ErrInvalid, "state: unknown deletion step")
	}
	tag, err := s.pool.Exec(ctx, query, id, scope.Tenant, scope.Deployment, scope.Subject)
	if err == nil && tag.RowsAffected() != 1 {
		return contracts.NewError(contracts.ErrConflict, "state: deletion receipt not ready")
	}
	return err
}

func (s *Store) recordDeletion(ctx context.Context, tx pgx.Tx, scope contracts.Scope, operation, resource string, threads []Thread, held int64) error {
	contexts := []string{}
	tasks := []string{}
	for _, th := range threads {
		contexts = append(contexts, th.ID, th.ContextID)
		rows, err := tx.Query(ctx, `SELECT id,task_id FROM ahp_state.runs WHERE thread_id=$1`, th.ID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var id, task string
			if err = rows.Scan(&id, &task); err != nil {
				rows.Close()
				return err
			}
			tasks = append(tasks, id)
			if task != "" {
				tasks = append(tasks, task)
			}
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO ahp_state.deletions(id,tenant,deployment,subject,operation,resource,context_ids,task_ids,deleted_count,held_count,memory_done) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, contracts.NewID(), scope.Tenant, scope.Deployment, scope.Subject, operation, resource, contexts, tasks, len(threads), held, operation == "DeleteThread" || held > 0)
	return err
}

// DeleteSubjectWithKey deletes one explicit administrative operation. The
// exact set of dependencies is committed atomically before any row vanishes.
func (s *Store) DeleteSubjectWithKey(ctx context.Context, scope contracts.Scope, key string) (int64, error) {
	if err := scope.Validate(); err != nil {
		return 0, err
	}
	if err := validIdentifier(key, "deletion key"); err != nil {
		return 0, err
	}
	var deleted int64
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		// Serialise same-scope deletion operations, including an empty subject.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, scope.Tenant+"/"+scope.Deployment+"/"+scope.Subject+"/delete"); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, `SELECT deleted_count FROM ahp_state.deletions WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND operation='DeleteSubject' AND resource=$4`, scope.Tenant, scope.Deployment, scope.Subject, key).Scan(&deleted)
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT `+threadColumns+` FROM ahp_state.threads WHERE tenant=$1 AND deployment=$2 AND subject=$3 ORDER BY id FOR UPDATE`, scope.Tenant, scope.Deployment, scope.Subject)
		if err != nil {
			return err
		}
		var all []Thread
		for rows.Next() {
			th, err := scanThread(rows)
			if err != nil {
				rows.Close()
				return err
			}
			all = append(all, th)
		}
		rows.Close()
		if err = rows.Err(); err != nil {
			return err
		}
		var eligible []Thread
		var held int64
		for _, th := range all {
			var keep bool
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1 AND (subject='' OR subject=$2) AND (thread_id='' OR thread_id=$3))`, scope.Tenant, scope.Subject, th.ID).Scan(&keep); err != nil {
				return err
			}
			if keep {
				held++
			} else {
				eligible = append(eligible, th)
			}
		}
		var subjectHeld bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM ahp_state.legalhold WHERE tenant=$1 AND (subject='' OR subject=$2) AND thread_id='')`, scope.Tenant, scope.Subject).Scan(&subjectHeld); err != nil {
			return err
		}
		if subjectHeld && held == 0 {
			held = 1
		}
		if err = s.recordDeletion(ctx, tx, scope, "DeleteSubject", key, eligible, held); err != nil {
			return err
		}
		for _, th := range eligible {
			if _, err = tx.Exec(ctx, `DELETE FROM ahp_state.threads WHERE id=$1`, th.ID); err != nil {
				return err
			}
		}
		if held == 0 {
			if _, err = tx.Exec(ctx, `DELETE FROM ahp_state.kv WHERE tenant=$1 AND deployment=$2 AND subject=$3`, scope.Tenant, scope.Deployment, scope.Subject); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, `DELETE FROM ahp_state.kv_ops WHERE tenant=$1 AND deployment=$2 AND subject=$3`, scope.Tenant, scope.Deployment, scope.Subject); err != nil {
				return err
			}
		}
		deleted = int64(len(eligible))
		return nil
	})
	return deleted, err
}
