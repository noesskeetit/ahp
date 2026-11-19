package state

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
	"github.com/jackc/pgx/v5"
)

// RunCompletion carries identifiers and verified claims, never a bearer token.
// Control-plane consumers use this API, not direct reads of State's tables.
type RunCompletion struct {
	ID        string      `json:"id"`
	Run       Run         `json:"run"`
	Thread    Thread      `json:"thread"`
	Authority auth.Claims `json:"authority"`
	CreatedAt time.Time   `json:"created_at"`
	LeaseKey  string      `json:"-"`
}

// MigrateRunCompletions applies the run completions DDL on the store's own pool. Migrate calls the same body on
// the connection that already holds the migrate advisory lock, which is what
// keeps a MaxConns=1 pool from deadlocking at startup.
func (s *Store) MigrateRunCompletions(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	return s.migrateRunCompletions(ctx, s.pool)
}

func (s *Store) migrateRunCompletions(ctx context.Context, ex ddlExec) error {
	_, err := ex.Exec(ctx, `CREATE TABLE IF NOT EXISTS ahp_state.run_completions(
 id text PRIMARY KEY REFERENCES ahp_state.runs(id) ON DELETE CASCADE,
 thread_id text NOT NULL REFERENCES ahp_state.threads(id) ON DELETE CASCADE,
 sequence bigint NOT NULL,body jsonb NOT NULL,status text NOT NULL DEFAULT 'pending',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),next_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 lease_key text,lease_until timestamptz,attempts integer NOT NULL DEFAULT 0,error_code text NOT NULL DEFAULT '');
 CREATE INDEX IF NOT EXISTS run_completion_due ON ahp_state.run_completions(status,next_at);
 CREATE INDEX IF NOT EXISTS run_completion_thread ON ahp_state.run_completions(thread_id,sequence,status);`)
	return err
}

// publishRunCompletionIfTerminal is the SINGLE decision and the SINGLE door for the
// completion outbox. Both ways a run can end - Release and the atomic PutBatch
// run_status half - call it, so the two can never disagree about WHEN a terminal run
// publishes a completion event. That disagreement was the defect PDF rev3.2 §2.5
// forbids in one line: the batch exists precisely so that "outbox событий не расходится
// с состоянием" - a batch that flips a run to done/failed and then skips this call
// leaves State terminal with no published event, which is the divergence the guarantee
// names.
//
// The predicate is copied verbatim from the door that has always published (release):
// done and failed, never canceled - a run the user stopped produced no turn verdict and
// is not a completion. The write is idempotent by construction: publishRunCompletion
// inserts under the run's own id with ON CONFLICT DO NOTHING, so a run terminalized
// twice (a batch replay, or a release that later meets a batch already closed the run)
// yields exactly one row.
func (s *Store) publishRunCompletionIfTerminal(ctx context.Context, tx pgx.Tx, scope contracts.Scope, runID, status string) error {
	if !s.completionOutbox {
		return nil
	}
	if status != RunDone && status != RunFailed {
		return nil
	}
	return s.publishRunCompletion(ctx, tx, scope, runID)
}

func (s *Store) publishRunCompletion(ctx context.Context, tx pgx.Tx, scope contracts.Scope, runID string) error {
	p, ok := transport.FromContext(ctx)
	if !ok || p.Scope != scope || p.Claims.Scope() != scope || p.Claims.Kind != auth.KindTask {
		return contracts.NewError(contracts.ErrForbidden, "run completion requires verified task authority")
	}
	var run Run
	if err := tx.QueryRow(ctx, `SELECT id,thread_id,task_id,status,wait_for,sequence,payload,deadline,traceparent FROM ahp_state.runs WHERE id=$1`, runID).Scan(&run.ID, &run.ThreadID, &run.TaskID, &run.Status, &run.WaitFor, &run.Sequence, &run.Payload, &run.Deadline, &run.TraceParent); err != nil {
		return err
	}
	if run.TaskID != p.Claims.Task {
		return contracts.NewError(contracts.ErrForbidden, "completion authority differs from run task")
	}
	var thread Thread
	if err := tx.QueryRow(ctx, `SELECT id,context_id,tenant,deployment,subject,epoch,created_at FROM ahp_state.threads WHERE id=$1`, run.ThreadID).Scan(&thread.ID, &thread.ContextID, &thread.Scope.Tenant, &thread.Scope.Deployment, &thread.Scope.Subject, &thread.Epoch, &thread.CreatedAt); err != nil {
		return err
	}
	if thread.Scope != scope || thread.ContextID != p.Claims.Context {
		return contracts.NewError(contracts.ErrForbidden, "completion context differs from run context")
	}
	body, err := json.Marshal(RunCompletion{ID: run.ID, Run: run, Thread: thread, Authority: p.Claims})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO ahp_state.run_completions(id,thread_id,sequence,body) VALUES($1,$2,$3,$4::jsonb) ON CONFLICT(id) DO NOTHING`, run.ID, run.ThreadID, run.Sequence, body)
	return err
}

// ClaimRunCompletions is for a trusted composed consumer, not a public route.
// A thread's next completion cannot overtake an earlier pending completion.
func (s *Store) ClaimRunCompletions(ctx context.Context, limit int) ([]RunCompletion, error) {
	if limit < 1 || limit > 100 {
		return nil, invalid("completion batch must be between 1 and 100")
	}
	key := contracts.NewID()
	rows, err := s.pool.Query(ctx, `WITH due AS (
 SELECT c.id FROM ahp_state.run_completions c WHERE c.status='pending' AND c.next_at<=clock_timestamp()
 AND (c.lease_until IS NULL OR c.lease_until<clock_timestamp())
 AND NOT EXISTS(SELECT 1 FROM ahp_state.run_completions p WHERE p.thread_id=c.thread_id AND p.sequence<c.sequence AND p.status='pending')
 ORDER BY c.created_at LIMIT $1 FOR UPDATE SKIP LOCKED)
 UPDATE ahp_state.run_completions c SET lease_key=$2,lease_until=clock_timestamp()+interval '90 seconds',attempts=c.attempts+1
 FROM due WHERE c.id=due.id RETURNING c.body,c.created_at`, limit, key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RunCompletion{}
	for rows.Next() {
		var raw []byte
		var at time.Time
		if err := rows.Scan(&raw, &at); err != nil {
			return nil, err
		}
		var e RunCompletion
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, err
		}
		e.CreatedAt = at
		e.LeaseKey = key
		out = append(out, e)
	}
	return out, rows.Err()
}
func (s *Store) FinishRunCompletion(ctx context.Context, id, key, code string, permanent bool) error {
	status := "delivered"
	if code != "" {
		status = "pending"
		if permanent {
			status = "failed"
		}
	}
	tag, err := s.pool.Exec(ctx, `UPDATE ahp_state.run_completions SET status=$3,error_code=$4,next_at=clock_timestamp()+interval '30 seconds',lease_key=NULL,lease_until=NULL WHERE id=$1 AND lease_key=$2`, id, key, status, code)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return staleEpoch()
	}
	return nil
}
