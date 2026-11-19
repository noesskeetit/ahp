package state

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/ahp-platform/ahp/internal/references"
	"github.com/ahp-platform/ahp/pkg/contracts"
	aot "github.com/ahp-platform/ahp/pkg/otel"
)

//go:embed schema.sql
var schemaFS embed.FS

const (
	// maxPayloadBytes bounds a run payload (1 MiB).
	maxPayloadBytes = 1 << 20
	// maxKeyBytes bounds idempotency keys and identifiers.
	maxKeyBytes = 512
	// maxWorkerBytes bounds a worker identity.
	maxWorkerBytes = 256
	// minLeaseTTL and maxLeaseTTL bound a requested lease duration.
	minLeaseTTL = time.Millisecond
	maxLeaseTTL = 5 * time.Minute
)

// Store is the durable State API core bound to one PostgreSQL pool. It is safe
// for concurrent use; every method is a single short transaction.
type Store struct {
	pool                     *pgxpool.Pool
	kvOwnerPolicies          map[string]KVNamespacePolicy
	scopeTransfer            ScopeTransferConfig
	checkpointArtifacts      references.CheckpointGuard
	checkpointHistoryLimit   int
	completionOutbox         bool
	commonRecords            bool
	commonPayloadObjects     bool
	commonArtifactReferences bool
	commonIncrementalAppend  bool
	commonRecordVersions     bool
	commonRetentionProfiles  map[string]bool
	deploymentTransfer       DeploymentTransferConfig
	// boundary selects the experimental checkpoint cadence for the runtime stream.
	// Chunk (default) keeps every AppendStream a durable write; step/turn buffer a
	// volatile per-run tail flushed at a boundary. See stream_tail.go.
	boundary contracts.CheckpointBoundary
	// resultsOnly selects the results-only retention experiment: text fragments
	// (kind model_delta) are buffered in the volatile tail for live delivery but
	// NEVER persisted to stream_events; only completed non-fragment results and a
	// coarse per-thread sequence high-water are durable. True only when the
	// boundary carries a volatile tail (step/turn). See contracts and stream_tail.go.
	resultsOnly bool
	// streamTailsMu guards streamTails; it is never held across a database call.
	streamTailsMu sync.Mutex
	streamTails   map[string]*runTail
	// streamSealedOrder is the FIFO of run ids whose results-only tails were sealed
	// at a terminal boundary but are retained (no durable fragment rows to re-read)
	// until a live reader drains them or they are reaped by the bounded reaper.
	// Guarded by streamTailsMu; gives streamTails a global capacity + lifetime.
	streamSealedOrder []string
	// leaseRetries counts the replays withTransientAbortRetry performed for
	// each replayable lease door. It is read through LeaseRetryCounts.
	leaseRetries struct {
		release leaseAbortCounter
		suspend leaseAbortCounter
	}
}

type StoreOption func(*Store)

// WithRunCompletionOutbox publishes durable terminal-run events in the same
// transaction as lease release. The composed Memory consumer owns delivery.
func WithRunCompletionOutbox() StoreOption { return func(s *Store) { s.completionOutbox = true } }

// New binds the State API to an existing pgx pool.
func New(pool *pgxpool.Pool, options ...StoreOption) *Store {
	s := &Store{
		pool:                   pool,
		checkpointHistoryLimit: DefaultCheckpointHistoryLimit,
		boundary:               contracts.CurrentCheckpointBoundary(),
		resultsOnly:            contracts.ResultsOnlyRetention(),
		streamTails:            make(map[string]*runTail),
	}
	for _, option := range options {
		option(s)
	}
	return s
}

// Pool exposes the underlying pool for service-level health checks. It must
// not be used to reach ahp_state tables outside this package.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Migrate creates the ahp_state schema and its tables. It is idempotent and
// non-destructive: existing rows are never modified or removed.
// ddlExec is what a migration needs from the database: one Exec capable of
// running multi-statement DDL. Both the pool and a single checked-out connection
// satisfy it, which is the whole point - a migration that is already holding a
// connection must be able to use that connection.
type ddlExec interface {
	Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error)
}

func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	ddl, err := schemaFS.ReadFile("schema.sql")
	if err != nil {
		return fmt.Errorf("state: read embedded schema: %w", err)
	}
	// Serialise DDL across every process and connection that migrates the same
	// database. Concurrent CREATE/ALTER statements otherwise deadlock. A
	// session-scoped advisory lock held on one pooled connection turns parallel
	// migrations into a queue: the first applies the schema, the rest observe
	// the already-current state via the idempotent IF NOT EXISTS DDL.
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("state: migrate acquire conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtextextended('ahp_state:migrate', 0))`); err != nil {
		return fmt.Errorf("state: migrate advisory lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtextextended('ahp_state:migrate', 0))`)
	}()
	if _, err := conn.Exec(ctx, string(ddl)); err != nil {
		return fmt.Errorf("state: migrate: %w", err)
	}
	// The State block owns the whole ahp_state schema, so a single Migrate
	// brings up every table the State API and its SDKs require on a fresh,
	// isolated database: checkpoints and their parts, KV, ordered stream and
	// inbox events, intents, and the control-plane legalhold surface used by
	// DeleteThread/DeleteSubject. Each sub-migration is idempotent and
	// non-destructive, and they all run while the migrate advisory lock above
	// is held, so concurrent migrators queue rather than deadlock.
	// Every DDL statement runs on the connection that holds the advisory lock.
	// Going back through the pool here is what deadlocked a MaxConns=1 startup:
	// the lock connection is checked out, so the child migration asks for a
	// second connection that can never exist, and the pool waits until the
	// caller's deadline instead of the DDL ever running.
	if err := s.migrateCheckpoints(ctx, conn); err != nil {
		return fmt.Errorf("state: migrate checkpoints: %w", err)
	}
	if err := s.migrateKV(ctx, conn); err != nil {
		return fmt.Errorf("state: migrate kv: %w", err)
	}
	if err := s.migrateEvents(ctx, conn); err != nil {
		return fmt.Errorf("state: migrate events: %w", err)
	}
	if err := s.migrateIntents(ctx, conn); err != nil {
		return fmt.Errorf("state: migrate intents: %w", err)
	}
	if err := s.migrateControl(ctx, conn); err != nil {
		return fmt.Errorf("state: migrate control: %w", err)
	}
	if err := s.migrateRunCompletions(ctx, conn); err != nil {
		return fmt.Errorf("state: migrate completion outbox: %w", err)
	}
	if s.commonRecords {
		if _, err := conn.Exec(ctx, commonRecordsDDL+commonPayloadDDL); err != nil {
			return fmt.Errorf("state: migrate experimental common records: %w", err)
		}
	}
	if s.commonRecordVersions || len(s.commonRetentionProfiles) > 0 {
		if _, err := conn.Exec(ctx, commonVersionDDL); err != nil {
			return fmt.Errorf("state: migrate common versions: %w", err)
		}
	}
	if s.commonIncrementalAppend {
		if _, err := conn.Exec(ctx, commonAppendDDL); err != nil {
			return fmt.Errorf("state: migrate common append: %w", err)
		}
	}
	if s.commonArtifactReferences {
		if _, err := conn.Exec(ctx, commonArtifactDDL); err != nil {
			return fmt.Errorf("state: migrate common artifact refs: %w", err)
		}
	}
	if len(s.commonRetentionProfiles) > 0 {
		if _, err := conn.Exec(ctx, commonRetentionDDL); err != nil {
			return fmt.Errorf("state: migrate common retention: %w", err)
		}
	}
	if s.deploymentTransfer.Authorize != nil {
		if _, err := conn.Exec(ctx, deploymentV3DDL); err != nil {
			return fmt.Errorf("state: migrate deployment transfer: %w", err)
		}
	}
	if _, err := conn.Exec(ctx, scopeV2DDL); err != nil {
		return fmt.Errorf("state: migrate scope v2: %w", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO ahp_state.scope_store_v2(singleton,id) VALUES(true,$1) ON CONFLICT DO NOTHING`, contracts.NewID()); err != nil {
		return fmt.Errorf("state: initialize physical store identity: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Threads
// ---------------------------------------------------------------------------

const threadColumns = `id, context_id, tenant, deployment, subject, epoch, created_at`

// CreateThread creates (or idempotently replays) a thread for contextID.
//
// The idempotency key is scoped by tenant, deployment and subject, and its
// body fingerprint covers the context id: replaying the same key with the same
// context returns the original thread, while reusing the key for a different
// context fails with KEY_REUSE_MISMATCH.
func (s *Store) CreateThread(ctx context.Context, scope contracts.Scope, contextID, key string) (Thread, error) {
	if err := scope.Validate(); err != nil {
		return Thread{}, asInvalid(err)
	}
	if err := validIdentifier(contextID, "context id"); err != nil {
		return Thread{}, err
	}
	if err := validIdentifier(key, "idempotency key"); err != nil {
		return Thread{}, err
	}
	digest, err := contracts.Digest(map[string]any{"context_id": contextID})
	if err != nil {
		return Thread{}, asInvalid(err)
	}

	id := contracts.NewID()
	var out Thread
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO ahp_state.threads
				(id, tenant, deployment, subject, context_id, idem_key, body_digest)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (tenant, deployment, subject, idem_key) DO NOTHING
			RETURNING `+threadColumns,
			id, scope.Tenant, scope.Deployment, scope.Subject, contextID, key, digest)
		t, err := scanThread(row)
		if err == nil {
			out = t
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		// The key already exists in this scope: return the original thread or
		// report the mismatch.
		existing, getErr := s.getThread(ctx, tx, scope, "", key)
		if getErr != nil {
			return getErr
		}
		if existing.ContextID != contextID {
			return contracts.NewError(contracts.ErrKeyReuse,
				"idempotency key was already used with a different thread body")
		}
		out = existing
		return nil
	})
	// A concurrent creation of the same context under a different key can lose
	// against the unique (scope, context_id) index; the winner's row now
	// satisfies the request, so replay the lookup once.
	if isUniqueViolation(err) {
		return s.GetThreadByContext(ctx, scope, contextID)
	}
	if err != nil {
		return Thread{}, err
	}
	return out, nil
}

// GetThread returns the thread identified by threadID inside scope. Threads
// belonging to another tenant, deployment or subject are reported NOT_FOUND so
// that the API cannot be used to probe foreign resources.
func (s *Store) GetThread(ctx context.Context, scope contracts.Scope, threadID string) (Thread, error) {
	if err := scope.Validate(); err != nil {
		return Thread{}, asInvalid(err)
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return Thread{}, err
	}
	var out Thread
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		t, err := s.getThread(ctx, tx, scope, threadID, "")
		if err != nil {
			return err
		}
		out = t
		return nil
	})
	if err != nil {
		return Thread{}, err
	}
	return out, nil
}

// contextTakenInDeployment reports whether any thread of (tenant, deployment)
// already carries contextID, whatever subject owns it. Reads stay scoped by
// scope and CreateThread stays idempotent per scope, but import is checked
// across subjects inside one deployment: context ids are how the human edge
// resolves a thread, so two subjects of one tenant materialising the same
// context_id would rewire identity on ingress - a foreign subject would take
// over the address of a context it cannot even read. The check stops at the
// deployment edge on purpose, so the same identity can still carry a context to
// another deployment (the hand-off between drivers that export/import is for).
func (s *Store) contextTakenInDeployment(ctx context.Context, tenant, deployment, contextID string) (bool, error) {
	var one int
	err := s.pool.QueryRow(ctx,
		`SELECT 1 FROM ahp_state.threads WHERE tenant = $1 AND deployment = $2 AND context_id = $3 LIMIT 1`,
		tenant, deployment, contextID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// GetThreadByContext returns the thread that carries contextID inside scope.
// Context ids are unique per scope, so this is the deterministic recovery path
// after a concurrent creation raced on the unique (scope, context_id) index.
func (s *Store) GetThreadByContext(ctx context.Context, scope contracts.Scope, contextID string) (Thread, error) {
	if err := scope.Validate(); err != nil {
		return Thread{}, asInvalid(err)
	}
	if err := validIdentifier(contextID, "context id"); err != nil {
		return Thread{}, err
	}
	const q = `SELECT ` + threadColumns + ` FROM ahp_state.threads
		WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND context_id = $4`
	t, err := scanThread(s.pool.QueryRow(ctx, q, scope.Tenant, scope.Deployment, scope.Subject, contextID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Thread{}, notFound("thread")
	}
	if err != nil {
		return Thread{}, err
	}
	return t, nil
}

// getThread loads a thread by id or by idempotency key inside scope. tx may be
// nil, in which case the pool is used.
func (s *Store) getThread(ctx context.Context, tx pgx.Tx, scope contracts.Scope, threadID, key string) (Thread, error) {
	const q = `SELECT ` + threadColumns + ` FROM ahp_state.threads
		WHERE tenant = $1 AND deployment = $2 AND subject = $3
		  AND (($4 <> '' AND id = $4) OR ($5 <> '' AND idem_key = $5))`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, q, scope.Tenant, scope.Deployment, scope.Subject, threadID, key)
	} else {
		row = s.pool.QueryRow(ctx, q, scope.Tenant, scope.Deployment, scope.Subject, threadID, key)
	}
	t, err := scanThread(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Thread{}, notFound("thread")
	}
	return t, err
}

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

const runColumns = `id, thread_id, task_id, status, wait_for, sequence, payload, deadline, traceparent`

// runRow is a run plus the owning thread and body fingerprint needed for lease
// handling and idempotency comparison. The fields are named (not embedded) so
// the public Run and Thread JSON shapes stay unambiguous.
type runRow struct {
	Run    Run
	Thread Thread
	digest string
}

// CreateRun appends a run to the end of the thread FIFO.
//
// The idempotency key is scoped by thread and its body fingerprint covers the
// task id and the canonical payload, so a replay returns the original run and a
// differing body under the same key fails with KEY_REUSE_MISMATCH. Sequence
// numbers are assigned under the thread row lock and the unique
// (thread_id, sequence) index guarantees that no two concurrent inserts can
// ever share a FIFO position.
func (s *Store) CreateRun(ctx context.Context, scope contracts.Scope, threadID, taskID, key string, payload json.RawMessage) (Run, error) {
	if err := scope.Validate(); err != nil {
		return Run{}, asInvalid(err)
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return Run{}, err
	}
	if err := validIdentifier(taskID, "task id"); err != nil {
		return Run{}, err
	}
	if err := validIdentifier(key, "idempotency key"); err != nil {
		return Run{}, err
	}
	stored, digest, err := canonicalPayload(payload)
	if err != nil {
		return Run{}, err
	}

	id := contracts.NewID()
	var out Run
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		if err := checkSubjectWritable(ctx, tx, scope); err != nil {
			return err
		}
		if _, err := s.getThread(ctx, tx, scope, threadID, ""); err != nil {
			return err
		}
		// Serialise FIFO position assignment on the thread row.
		if _, err := tx.Exec(ctx, `SELECT 1 FROM ahp_state.threads WHERE id = $1 FOR UPDATE`, threadID); err != nil {
			return err
		}
		var seq int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(MAX(sequence), 0) + 1
			FROM ahp_state.runs
			WHERE thread_id = $1`, threadID).Scan(&seq); err != nil {
			return err
		}
		row := tx.QueryRow(ctx, `
			INSERT INTO ahp_state.runs
				(id, thread_id, task_id, status, sequence, payload, body_digest, idem_key, traceparent)
			VALUES ($1, $2, $3, '`+RunPending+`', $4, $5::jsonb, $6, $7, $8)
			ON CONFLICT (thread_id, idem_key) DO NOTHING
			RETURNING `+runColumns,
			id, threadID, taskID, seq, stored, digest, key, aot.TraceParent(ctx))
		r, err := scanRun(row)
		if err == nil {
			out = r
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		existing, getErr := s.getRunByKey(ctx, tx, threadID, key)
		if getErr != nil {
			return getErr
		}
		if existing.Run.TaskID != taskID || existing.digest != digest {
			return contracts.NewError(contracts.ErrKeyReuse,
				"idempotency key was already used with a different run body")
		}
		out = existing.Run
		return nil
	})
	// The thread row lock above already serialises inserts, so a unique
	// violation here can only come from a concurrent replay of the same key;
	// the winner's row satisfies the request.
	if isUniqueViolation(err) {
		existing, getErr := s.getRunByKey(ctx, nil, threadID, key)
		if getErr != nil {
			return Run{}, getErr
		}
		if existing.Run.TaskID != taskID || existing.digest != digest {
			return Run{}, contracts.NewError(contracts.ErrKeyReuse,
				"idempotency key was already used with a different run body")
		}
		return existing.Run, nil
	}
	if err != nil {
		return Run{}, err
	}
	return out, nil
}

// GetRun returns the run identified by runID inside scope. Foreign runs are
// NOT_FOUND.
func (s *Store) GetRun(ctx context.Context, scope contracts.Scope, runID string) (Run, error) {
	if err := scope.Validate(); err != nil {
		return Run{}, asInvalid(err)
	}
	if err := validIdentifier(runID, "run id"); err != nil {
		return Run{}, err
	}
	var out Run
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		r, err := s.getRun(ctx, tx, scope, runID)
		if err != nil {
			return err
		}
		out = r.Run
		return nil
	})
	if err != nil {
		return Run{}, err
	}
	return out, nil
}

// ListRuns returns every run of a thread in FIFO (sequence) order.
func (s *Store) ListRuns(ctx context.Context, scope contracts.Scope, threadID string) ([]Run, error) {
	if err := scope.Validate(); err != nil {
		return nil, asInvalid(err)
	}
	if err := validIdentifier(threadID, "thread id"); err != nil {
		return nil, err
	}
	var out []Run
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		if _, err := s.getThread(ctx, tx, scope, threadID, ""); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT `+runColumns+`
			FROM ahp_state.runs
			WHERE thread_id = $1
			ORDER BY sequence`, threadID)
		if err != nil {
			return err
		}
		defer rows.Close()
		out = []Run{}
		for rows.Next() {
			r, err := scanRun(rows)
			if err != nil {
				return err
			}
			out = append(out, r)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// getRun loads one run joined to its owning thread so the scope filter is
// applied atomically. tx may be nil (pool).
func (s *Store) getRun(ctx context.Context, tx pgx.Tx, scope contracts.Scope, runID string) (runRow, error) {
	const q = `SELECT r.id, r.thread_id, r.task_id, r.status, r.wait_for, r.sequence, r.payload, r.deadline, r.traceparent,
		r.body_digest, t.tenant, t.deployment, t.subject, t.context_id, t.epoch, t.created_at
		FROM ahp_state.runs r
		JOIN ahp_state.threads t ON t.id = r.thread_id
		WHERE r.id = $1 AND t.tenant = $2 AND t.deployment = $3 AND t.subject = $4`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, q, runID, scope.Tenant, scope.Deployment, scope.Subject)
	} else {
		row = s.pool.QueryRow(ctx, q, runID, scope.Tenant, scope.Deployment, scope.Subject)
	}
	r, err := scanRunWithThread(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return runRow{}, notFound("run")
	}
	return r, err
}

// getRunForAppend retains the same scoped metadata projection as getRun, but
// does not detoast/copy the original turn input for each small stream fragment.
// Only AppendStream uses this projection; full run/lease readers keep payload.
const appendRunMetadataSQL = `SELECT r.id, r.thread_id, r.task_id, r.status, r.wait_for, r.sequence, NULL::jsonb, r.deadline, r.traceparent,
		r.body_digest, t.tenant, t.deployment, t.subject, t.context_id, t.epoch, t.created_at
		FROM ahp_state.runs r
		JOIN ahp_state.threads t ON t.id = r.thread_id
		WHERE r.id = $1 AND t.tenant = $2 AND t.deployment = $3 AND t.subject = $4`

func (s *Store) getRunForAppend(ctx context.Context, tx pgx.Tx, scope contracts.Scope, runID string) (runRow, error) {
	r, err := scanRunWithThread(tx.QueryRow(ctx, appendRunMetadataSQL, runID, scope.Tenant, scope.Deployment, scope.Subject))
	if errors.Is(err, pgx.ErrNoRows) {
		return runRow{}, notFound("run")
	}
	return r, err
}

// getRunByKey loads a run by its per-thread idempotency key.
func (s *Store) getRunByKey(ctx context.Context, tx pgx.Tx, threadID, key string) (runRow, error) {
	const q = `SELECT r.id, r.thread_id, r.task_id, r.status, r.wait_for, r.sequence, r.payload, r.deadline, r.traceparent,
		r.body_digest, t.tenant, t.deployment, t.subject, t.context_id, t.epoch, t.created_at
		FROM ahp_state.runs r
		JOIN ahp_state.threads t ON t.id = r.thread_id
		WHERE r.thread_id = $1 AND r.idem_key = $2`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, q, threadID, key)
	} else {
		row = s.pool.QueryRow(ctx, q, threadID, key)
	}
	r, err := scanRunWithThread(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return runRow{}, notFound("run")
	}
	return r, err
}

// ---------------------------------------------------------------------------
// Claiming
// ---------------------------------------------------------------------------

// claimCandidate identifies one thread head that may be claimed right now.
type claimCandidate struct {
	scope    contracts.Scope
	threadID string
	runID    string
}

// claimCandidatesSQL returns, for the caller's tenant/deployment pair, the
// earliest unfinished run of every thread that may be claimed right now:
//
//   - the thread holds no lease, or its lease has expired in database time;
//   - no earlier unfinished run blocks the FIFO;
//   - the candidate itself is pending or running (a running row is only
//     reachable here once its lease expired, i.e. the worker died), or is
//     waiting with a deadline the database clock has already passed.
//
// Because the candidate is always the earliest unfinished run, a waiting run
// without a deadline (or with a future one) blocks every later run of its
// thread: later runs can never jump the queue, and neither can they overtake a
// run whose lease is still held or whose lease just expired.
//
// Lock order, stated once because the whole lease cycle depends on it: every
// writer of a run row holds that run's THREAD row lock first. The thread row is
// the single lease slot, and it is what currentLease takes before it touches a
// run (lease release, heartbeat, suspend, batch), what CreateRun takes before
// it inserts one, what PostInboxEvent takes before it wakes one, and what
// CancelWaitingRun takes before it closes one. So the candidate scan locks the
// thread (OF t), never the head run: locking the run first inverted that order
// against every one of those writers, and a claim racing a release of the same
// thread was a run<->thread cycle (SQLSTATE 40P01) that PostgreSQL resolved by
// aborting one side.
//
// SKIP LOCKED still earns its place and it is not what kept the cycle away: it
// keeps concurrent claims from queueing behind each other - and behind a lease
// writer - on a busy thread, and the ORDER BY thread/head-sequence gives strict
// FIFO fairness across threads. A skipped thread is not a lost claim: the
// candidate list is a hint set, and grant() re-validates everything under the
// thread lock it actually holds, so the thread is claimable again on the next
// poll the moment its current writer commits.
const claimCandidatesSQL = `
	WITH candidates AS (
		SELECT r.thread_id, MIN(r.sequence) AS seq
		FROM ahp_state.runs r
		JOIN ahp_state.threads t ON t.id = r.thread_id
		WHERE t.tenant = $1 AND t.deployment = $2
		  AND NOT EXISTS(SELECT 1 FROM ahp_state.erased_subjects e WHERE e.tenant=t.tenant AND e.subject=t.subject)
 AND NOT EXISTS(SELECT 1 FROM ahp_state.erased_tenants e WHERE e.tenant=t.tenant)
		  AND r.status IN ('` + RunPending + `', '` + RunRunning + `', '` + RunWaiting + `')
		GROUP BY r.thread_id
	)
	SELECT t.tenant, t.deployment, t.subject, t.id, r.id
	FROM candidates c
	JOIN ahp_state.runs r ON r.thread_id = c.thread_id AND r.sequence = c.seq
	JOIN ahp_state.threads t ON t.id = c.thread_id
	WHERE ($3::text = '' OR r.id = $3)
	  AND ($4::text = '' OR r.payload->>'type' = $4)
	  AND (t.lease_expires IS NULL OR t.lease_expires <= clock_timestamp())
	  AND (r.status IN ('` + RunPending + `', '` + RunRunning + `')
	       OR (r.deadline IS NOT NULL AND r.deadline <= clock_timestamp()))
	  AND NOT EXISTS (
		SELECT 1 FROM ahp_state.runs b
		WHERE b.thread_id = r.thread_id AND b.sequence < r.sequence
		  AND b.status NOT IN ('` + RunDone + `', '` + RunFailed + `', '` + RunCanceled + `')
	  )
	ORDER BY t.tenant, t.deployment, t.subject, r.thread_id, r.sequence
	FOR UPDATE OF t SKIP LOCKED
	LIMIT 1`

// ClaimRun atomically acquires the single lease of one eligible thread in the
// caller's tenant/deployment and returns the granted lease, or (nil, nil) when
// there is nothing to do.
//
// Claim identity is deployment-level: tenant and deployment come from verified
// workload identity and runs of any other deployment are never considered.
// Subject is not a claim filter, but it is returned with the lease so every
// follow-up operation stays scope-checked.
//
// Every grant increments the thread epoch, so a worker whose lease was lost
// (expired or taken over) is fenced off with STALE_EPOCH on its next call.
func (s *Store) ClaimRun(ctx context.Context, tenant, deployment, worker string, ttl time.Duration) (*Lease, error) {
	return s.ClaimRunFor(ctx, tenant, deployment, worker, "", ttl)
}

// ClaimRunFor restricts the eligible FIFO candidates to runID when nonempty.
// It never skips an earlier unfinished run in that thread.
func (s *Store) ClaimRunFor(ctx context.Context, tenant, deployment, worker, runID string, ttl time.Duration) (*Lease, error) {
	return s.ClaimRunWithFilter(ctx, tenant, deployment, worker, ttl, ClaimFilter{RunID: runID})
}

// ClaimFilter narrows eligible FIFO heads without interpreting their payloads.
// Empty fields preserve the deployment-wide queue. A type never skips an
// earlier unfinished run in the same thread.
type ClaimFilter struct {
	RunID       string
	PayloadType string
}

func (s *Store) ClaimRunWithFilter(ctx context.Context, tenant, deployment, worker string, ttl time.Duration, filter ClaimFilter) (*Lease, error) {
	if len(filter.PayloadType) > 128 || !utf8.ValidString(filter.PayloadType) {
		return nil, invalid("payload_type must be valid UTF-8 of at most 128 bytes")
	}
	// Claim identity is deployment-level, so there is no subject to validate
	// here; the same identifier rules still apply to both parts.
	if err := validScopePart(tenant, "tenant"); err != nil {
		return nil, err
	}
	if err := validScopePart(deployment, "deployment"); err != nil {
		return nil, err
	}
	if err := validWorker(worker); err != nil {
		return nil, err
	}
	if err := validTTL(ttl); err != nil {
		return nil, err
	}

	var out *Lease
	// Belt and braces, not the fix: with the candidate scan locking the thread
	// (see claimCandidatesSQL) claims and lease writers share one order, so no
	// cycle is expected here. The loop stays because an abort can still come
	// from a writer this package does not own - a tenant-wide erasure sweep, a
	// future door that forgets the order - and a claim is safe to replay from
	// scratch: an aborted attempt leaves no lease and no epoch bump, because both
	// live inside the transaction that aborted.
	for attempt := 0; ; attempt++ {
		err := s.claimOnce(ctx, tenant, deployment, worker, filter, ttl, &out)
		if err == nil {
			return out, nil
		}
		if !isDeadlockViolation(err) || attempt >= maxClaimAttempts {
			return nil, err
		}
	}
}

// maxClaimAttempts bounds the deadlock-retry loop of ClaimRun.
const maxClaimAttempts = 8

// claimOnce performs one ClaimRun transaction.
func (s *Store) claimOnce(ctx context.Context, tenant, deployment, worker string, filter ClaimFilter, ttl time.Duration, out **Lease) error {
	*out = nil // an aborted prior attempt never supplies a lease on an empty retry
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		for {
			rows, err := tx.Query(ctx, claimCandidatesSQL, tenant, deployment, filter.RunID, filter.PayloadType)
			if err != nil {
				return err
			}
			var cand []claimCandidate
			for rows.Next() {
				var c claimCandidate
				if err := rows.Scan(&c.scope.Tenant, &c.scope.Deployment, &c.scope.Subject,
					&c.threadID, &c.runID); err != nil {
					rows.Close()
					return err
				}
				cand = append(cand, c)
			}
			rows.Close()
			if err := rows.Err(); err != nil {
				return err
			}
			if len(cand) == 0 {
				return nil
			}
			lease, err := s.grant(ctx, tx, cand[0], worker, ttl)
			if errors.Is(err, errClaimFencePending) {
				// Release row locks; retry on the next scheduler poll.
				return nil
			}
			if errors.Is(err, errClaimRace) {
				// Another worker took this thread between the candidate scan
				// and the grant; look for the next eligible thread.
				continue
			}
			if err != nil {
				return err
			}
			*out = lease
			return nil
		}
	})
	return err
}

// errClaimRace signals that a candidate thread was taken concurrently.
var errClaimRace = errors.New("state: claim race")
var errClaimFencePending = errors.New("state: subject erasure pending")

// grant takes the lease of the candidate run and its thread. The candidate
// THREAD row is already locked by the FOR UPDATE OF t ... SKIP LOCKED scan: it
// is the single lease slot and the point this transaction is serialised on.
// The run row is locked later, by the UPDATE below, so it is only ever touched
// while the thread lock is held - which is the invariant the lock order is
// built on. Everything the hint scan decided is re-checked under that lock:
// the lease is still free, and the run is still the claimable head under
// exactly the scan's rules.
func (s *Store) grant(ctx context.Context, tx pgx.Tx, c claimCandidate, worker string, ttl time.Duration) (*Lease, error) {
	locked, err := trySubjectWritableForClaim(ctx, tx, c.scope)
	if err != nil {
		if contracts.Code(err) == contracts.ErrForbidden {
			return nil, errClaimRace
		}
		return nil, err
	}
	if !locked {
		return nil, errClaimFencePending
	}
	var curExpires *time.Time
	err = tx.QueryRow(ctx, `
		SELECT lease_expires
		FROM ahp_state.threads
		WHERE id = $1
		FOR UPDATE`, c.threadID).Scan(&curExpires)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, notFound("thread")
		}
		return nil, err
	}
	// The lease may have been taken or renewed between the candidate scan and
	// this lock; only an expired (or absent) lease may be granted.
	if curExpires != nil {
		var free bool
		if err := tx.QueryRow(ctx,
			`SELECT $1::timestamptz <= clock_timestamp()`, curExpires).Scan(&free); err != nil {
			return nil, err
		}
		if !free {
			return nil, errClaimRace
		}
	}

	// Re-check under the lock that the candidate is still the claimable head,
	// with exactly the same rules as the candidate scan.
	var stillHead bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM ahp_state.runs r
			WHERE r.id = $1 AND r.thread_id = $2
			  AND (r.status IN ('`+RunPending+`', '`+RunRunning+`')
			       OR (r.deadline IS NOT NULL AND r.deadline <= clock_timestamp()))
			  AND NOT EXISTS (
				SELECT 1 FROM ahp_state.runs b
				WHERE b.thread_id = r.thread_id AND b.sequence < r.sequence
				  AND b.status NOT IN ('`+RunDone+`', '`+RunFailed+`', '`+RunCanceled+`')
			  )
		)`, c.runID, c.threadID).Scan(&stillHead); err != nil {
		return nil, err
	}
	if !stillHead {
		return nil, errClaimRace
	}

	// Every claim or reclaim increments the epoch: the fence grows monotonically.
	var epoch int64
	if err := tx.QueryRow(ctx, `
		UPDATE ahp_state.threads
		SET epoch = epoch + 1
		WHERE id = $1
		RETURNING epoch`, c.threadID).Scan(&epoch); err != nil {
		return nil, err
	}

	// A granted run always executes: it becomes running and its wait state is
	// cleared (a woken run no longer waits for the event that parked it).
	var status string
	if err := tx.QueryRow(ctx, `
		UPDATE ahp_state.runs
		SET status = '`+RunRunning+`', wait_for = '', deadline = NULL, updated_at = clock_timestamp()
		WHERE id = $1
		RETURNING status`, c.runID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, notFound("run")
		}
		return nil, err
	}

	// Install the lease; the database clock is the only time source.
	var expiresAt time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE ahp_state.threads
		SET lease_run_id = $2, lease_worker = $3, lease_epoch = $4,
		    lease_expires = clock_timestamp() + ($5 * interval '1 microsecond')
		WHERE id = $1
		RETURNING lease_expires`,
		c.threadID, c.runID, worker, epoch, ttl.Microseconds()).Scan(&expiresAt); err != nil {
		return nil, err
	}

	full, err := s.getRun(ctx, tx, c.scope, c.runID)
	if err != nil {
		return nil, err
	}
	run := full.Run
	run.Status = status
	thread := full.Thread
	thread.Epoch = epoch
	return &Lease{
		Thread:    thread,
		Run:       run,
		Worker:    worker,
		Epoch:     epoch,
		ExpiresAt: utcOf(expiresAt),
		Scope:     c.scope,
	}, nil
}

// ---------------------------------------------------------------------------
// Lease operations
// ---------------------------------------------------------------------------

// Heartbeat extends the lease of runID for the given worker and epoch and
// returns the new expiry in database time. A lease that was taken over or has
// already expired is rejected with STALE_EPOCH and extends nothing.
func (s *Store) Heartbeat(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, ttl time.Duration) (time.Time, error) {
	if err := validLeaseArgs(scope, runID, worker, epoch); err != nil {
		return time.Time{}, err
	}
	if err := validTTL(ttl); err != nil {
		return time.Time{}, err
	}
	var out time.Time
	err := s.inTx(ctx, func(tx pgx.Tx) error {
		l, err := s.currentLease(ctx, tx, scope, runID, worker, epoch)
		if err != nil {
			return err
		}
		var expires time.Time
		if err := tx.QueryRow(ctx, `
			UPDATE ahp_state.threads
			SET lease_expires = clock_timestamp() + ($3 * interval '1 microsecond')
			WHERE id = $1 AND lease_run_id = $2 AND lease_worker = $4 AND lease_epoch = $5
			  AND lease_expires > clock_timestamp()
			RETURNING lease_expires`,
			l.Thread.ID, runID, ttl.Microseconds(), worker, epoch).Scan(&expires); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return staleEpoch()
			}
			return err
		}
		out = utcOf(expires)
		return nil
	})
	if err != nil {
		return time.Time{}, err
	}
	return out, nil
}

// Release ends a run: status must be terminal (done/failed/canceled) or
// pending, which yields the run's head position back to the queue. The thread
// lease is cleared so the next run becomes claimable. A stale lease token is
// rejected with STALE_EPOCH and changes nothing.
func (s *Store) Release(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, status string) error {
	return s.release(ctx, scope, runID, worker, epoch, status, "", "")
}

// ReleaseWithReason ends a run's lease and, when the transition is terminal,
// records WHY the run ended inside the run's EXISTING payload document
// (ahp_state.runs.payload, a jsonb column that has always been there).
//
// No migration: the column is not touched, no column is added, no accepted
// document changes meaning. The key written is exactly the key the execution
// plane already journals on a task (gateway.MetadataCloseReason =
// "close_reason"), so an auditor comparing the two ends of an A->B chain reads
// the same word in both places instead of inventing a second vocabulary.
//
// An empty reason writes nothing at all: a run that has no verdict keeps the
// payload it had, byte for byte, which is what makes this safe to ship behind a
// caller that only sometimes knows the answer.
func (s *Store) ReleaseWithReason(ctx context.Context, scope contracts.Scope, runID, worker string,
	epoch int64, status, closeReason string) error {
	return s.release(ctx, scope, runID, worker, epoch, status, closeReason, "")
}

// ReleaseWithReasonCause is ReleaseWithReason plus the refusing plane's own sentence:
// besides the dictionary word in close_reason, it records the caller's verbatim cause
// under RunPayloadFailureCause in the same existing payload document (see close_reason.go
// for why the two keys are separate). An empty cause writes nothing extra, and a
// non-empty cause is only lawful on a terminal transition, exactly like the reason.
func (s *Store) ReleaseWithReasonCause(ctx context.Context, scope contracts.Scope, runID, worker string,
	epoch int64, status, closeReason, cause string) error {
	return s.release(ctx, scope, runID, worker, epoch, status, closeReason, cause)
}

func (s *Store) release(ctx context.Context, scope contracts.Scope, runID, worker string,
	epoch int64, status, closeReason, cause string) error {
	if err := validLeaseArgs(scope, runID, worker, epoch); err != nil {
		return err
	}
	if status != RunPending && !IsTerminalStatus(status) {
		return invalid("release status must be done, failed, canceled or pending")
	}
	// A reason belongs to an ended run only. Recording why a run is still running
	// would let a mid-flight worker stamp a verdict the run has not reached.
	if (closeReason != "" || cause != "") && !IsTerminalStatus(status) {
		return invalid("close_reason may only be recorded on a terminal transition")
	}
	// DEF-RELEASE-REASON-1: the other half of the rule DEF-BATCH-1 wrote into the batch
	// door. A run may not reach failed or canceled through this door with nobody saying
	// why - before the fix it could, and the row was unreadable afterwards (the measured
	// case: Release(..., "failed") with no reason returned nil and left runs.status=failed
	// with no close_reason and no failure_cause). Asked of the same writer the batch
	// door asks, so both doors answer with one sentence. done is not covered by this
	// contract: the transcript carries the verdict that ended the run, and honest done
	// callers exist in numbers; the scope is stated, not accidental.
	if err := runEndReasonPolicy("release", status, closeReason, cause, RunFailed, RunCanceled); err != nil {
		return err
	}
	// Replayed only on a definite database abort. withTransientAbortRetry
	// states why one release attempt can never leave a second effect behind.
	releaseErr := s.withTransientAbortRetry(ctx, "release", &s.leaseRetries.release,
		func() error {
			return s.releaseTx(ctx, scope, runID, worker, epoch, status, closeReason, cause)
		})
	// A terminal release bounds the experimental volatile stream tail: even if the
	// boundary flush failed, a finished run's tail is no longer needed and its memory
	// is reclaimed. No-op in the default chunk mode. A non-terminal (pending) release
	// keeps the tail so a resuming owner still sees the same cursor base.
	if releaseErr == nil && IsTerminalStatus(status) {
		s.DiscardRunTail(runID)
	}
	return releaseErr
}

// releaseTx is one release attempt: the run row, its terminal event and the
// thread lease all move inside a single transaction, so an attempt the database
// aborted leaves nothing to undo.
func (s *Store) releaseTx(ctx context.Context, scope contracts.Scope, runID, worker string,
	epoch int64, status, closeReason, cause string) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		l, err := s.currentLease(ctx, tx, scope, runID, worker, epoch)
		if err != nil {
			return err
		}
		// One journal writer for both doors: release and PutBatch's run_status end a
		// run's row through the same jsonb_set code, so the two can never disagree
		// about where or how a stated reason lives.
		tag, err := applyRunStatusJournal(ctx, tx, runID, status, closeReason, cause)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return notFound("run")
		}
		// One publish door for every way a run ends (see publishRunCompletionIfTerminal):
		// release does not own a private copy of the "when to publish" rule any more.
		if err := s.publishRunCompletionIfTerminal(ctx, tx, scope, l.Run.ID, status); err != nil {
			return err
		}
		return s.clearLease(ctx, tx, l.Thread.ID, runID, worker, epoch)
	})
}

// Suspend parks the run in waiting until an inbox event for waitFor arrives or
// its optional deadline passes in database time. An already available matching
// inbox event keeps it pending, so an early response cannot be lost. The lease is released,
// but the waiting run keeps its FIFO position and therefore blocks every later
// run of the thread. A stale lease token is rejected with STALE_EPOCH.
func (s *Store) Suspend(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, waitFor string, deadline *time.Time) error {
	if err := validLeaseArgs(scope, runID, worker, epoch); err != nil {
		return err
	}
	if err := validIdentifier(waitFor, "wait target"); err != nil {
		return err
	}
	// Replayed only on a definite database abort (see withTransientAbortRetry).
	return s.withTransientAbortRetry(ctx, "suspend", &s.leaseRetries.suspend,
		func() error {
			return s.suspendTx(ctx, scope, runID, worker, epoch, waitFor, deadline)
		})
}

// suspendTx is one suspend attempt: the run parks and the thread lease clears in
// the same transaction, so an aborted attempt leaves the lease exactly as it was.
func (s *Store) suspendTx(ctx context.Context, scope contracts.Scope, runID, worker string,
	epoch int64, waitFor string, deadline *time.Time) error {
	return s.inTx(ctx, func(tx pgx.Tx) error {
		l, err := s.currentLease(ctx, tx, scope, runID, worker, epoch)
		if err != nil {
			return err
		}
		if err := applyRunWaiting(ctx, tx, l.Thread.ID, runID, waitFor, deadline); err != nil {
			return err
		}
		return s.clearLease(ctx, tx, l.Thread.ID, runID, worker, epoch)
	})
}

// applyRunWaiting requires the same thread lock as Enqueue/AckInbox. It is
// shared by Suspend and atomic checkpoint commits, so an answer racing with
// either operation cannot leave a run asleep with an unread matching input.
func applyRunWaiting(ctx context.Context, tx pgx.Tx, threadID, runID, waitFor string, deadline *time.Time) error {
	var ready bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
 SELECT 1 FROM ahp_state.inbox_events
 WHERE thread_id=$1 AND NOT acked AND kind=$2
)`, threadID, waitFor).Scan(&ready); err != nil {
		return err
	}
	status, target := RunWaiting, waitFor
	if ready {
		status, target, deadline = RunPending, "", nil
	}
	tag, err := tx.Exec(ctx, `UPDATE ahp_state.runs
 SET status=$2, wait_for=$3, deadline=$4, updated_at=clock_timestamp()
 WHERE id=$1`, runID, status, target, deadline)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return notFound("run")
	}
	return nil
}

// withLease runs fn inside a transaction whose lease check has just succeeded:
// the run belongs to scope, and the thread lease still names this run, worker
// and epoch without having expired in database time. Any stale or foreign
// token aborts with STALE_EPOCH before fn runs, and everything fn writes is
// applied atomically together with that check. This is the extension point for
// the follow-up State API tasks (checkpoints, writes, inbox, batches).
func (s *Store) withLease(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, fn func(pgx.Tx, *Lease) error) error {
	return s.withLeaseProjection(ctx, scope, runID, worker, epoch, true, fn)
}

// withAppendStreamLease omits only unused run payload, never the lease fence.
func (s *Store) withAppendStreamLease(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, fn func(pgx.Tx, *Lease) error) error {
	return s.withLeaseProjection(ctx, scope, runID, worker, epoch, false, fn)
}

func (s *Store) withLeaseProjection(ctx context.Context, scope contracts.Scope, runID, worker string, epoch int64, includeRunPayload bool, fn func(pgx.Tx, *Lease) error) error {
	if err := validLeaseArgs(scope, runID, worker, epoch); err != nil {
		return err
	}
	if fn == nil {
		return invalid("withLease requires a callback")
	}
	return s.inTx(ctx, func(tx pgx.Tx) error {
		l, err := s.currentLeaseProjection(ctx, tx, scope, runID, worker, epoch, includeRunPayload)
		if err != nil {
			return err
		}
		return fn(tx, l)
	})
}

// clearLease removes the thread lease held by exactly this run/worker/epoch.
func (s *Store) clearLease(ctx context.Context, tx pgx.Tx, threadID, runID, worker string, epoch int64) error {
	tag, err := tx.Exec(ctx, `
		UPDATE ahp_state.threads
		SET lease_run_id = NULL, lease_worker = NULL, lease_epoch = NULL, lease_expires = NULL
		WHERE id = $1 AND lease_run_id = $2 AND lease_worker = $3 AND lease_epoch = $4`,
		threadID, runID, worker, epoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return staleEpoch()
	}
	return nil
}

// currentLease loads the thread lease under a row lock and verifies it still
// belongs to this run, worker and epoch and has not expired in database time.
// It is the single fence shared by every lease operation.
func (s *Store) currentLease(ctx context.Context, tx pgx.Tx, scope contracts.Scope, runID, worker string, epoch int64) (*Lease, error) {
	return s.currentLeaseProjection(ctx, tx, scope, runID, worker, epoch, true)
}

func (s *Store) currentLeaseProjection(ctx context.Context, tx pgx.Tx, scope contracts.Scope, runID, worker string, epoch int64, includeRunPayload bool) (*Lease, error) {
	if !includeRunPayload {
		return s.currentAppendStreamLease(ctx, tx, scope, runID, worker, epoch)
	}
	if err := checkSubjectWritable(ctx, tx, scope); err != nil {
		return nil, err
	}
	var r runRow
	var err error
	if includeRunPayload {
		r, err = s.getRun(ctx, tx, scope, runID)
	} else {
		r, err = s.getRunForAppend(ctx, tx, scope, runID)
	}
	if err != nil {
		return nil, err
	}
	var (
		leaseRunID   *string
		leaseWorker  *string
		leaseEpoch   *int64
		leaseExpires *time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT lease_run_id, lease_worker, lease_epoch, lease_expires
		FROM ahp_state.threads
		WHERE id = $1
		FOR UPDATE`, r.Run.ThreadID).Scan(&leaseRunID, &leaseWorker, &leaseEpoch, &leaseExpires)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, notFound("thread")
		}
		return nil, err
	}
	if leaseRunID == nil || *leaseRunID != runID ||
		leaseWorker == nil || *leaseWorker != worker ||
		leaseEpoch == nil || *leaseEpoch != epoch {
		return nil, staleEpoch()
	}
	// Expiry is decided by the database clock, never by the application clock.
	var live bool
	if err := tx.QueryRow(ctx,
		`SELECT $1::timestamptz > clock_timestamp()`, *leaseExpires).Scan(&live); err != nil {
		return nil, err
	}
	if !live {
		return nil, staleEpoch()
	}
	thread := r.Thread
	thread.Epoch = *leaseEpoch
	return &Lease{
		Thread:    thread,
		Run:       r.Run,
		Worker:    worker,
		Epoch:     epoch,
		ExpiresAt: utcOf(*leaseExpires),
		Scope:     scope,
	}, nil
}

// Only the append hot path batches non-mutating checks. Every result is read
// and the batch closed before any refusal or subsequent SQL. Separate Execute
// commands retain their Read Committed snapshots across advisory/row lock waits.
func (s *Store) currentAppendStreamLease(ctx context.Context, tx pgx.Tx, scope contracts.Scope, runID, worker string, epoch int64) (*Lease, error) {
	owner := &pgx.Batch{}
	queueSubjectWritable(owner, scope)
	owner.Queue(appendRunMetadataSQL, runID, scope.Tenant, scope.Deployment, scope.Subject)
	results := tx.SendBatch(ctx, owner)
	_, tenantErr := results.Exec()
	_, subjectErr := results.Exec()
	var erased bool
	eraseErr := results.QueryRow().Scan(&erased)
	r, runErr := scanRunWithThread(results.QueryRow())
	closeErr := results.Close()
	for _, err := range []error{tenantErr, subjectErr, eraseErr} {
		if err != nil {
			return nil, err
		}
	}
	if erased {
		return nil, contracts.NewError(contracts.ErrForbidden, "state: subject erasure forbids further writes")
	}
	if errors.Is(runErr, pgx.ErrNoRows) {
		return nil, notFound("run")
	}
	if runErr != nil {
		return nil, runErr
	}
	if closeErr != nil {
		return nil, closeErr
	}

	lease := &pgx.Batch{}
	lease.Queue(`SELECT lease_run_id, lease_worker, lease_epoch, lease_expires FROM ahp_state.threads WHERE id=$1 FOR UPDATE`, r.Run.ThreadID)
	// The expiry value is not yet available when queueing. Fetch database time
	// in the NEXT command, after FOR UPDATE has acquired its possibly-waited lock.
	lease.Queue(`SELECT clock_timestamp()`)
	results = tx.SendBatch(ctx, lease)
	var leaseRunID, leaseWorker *string
	var leaseEpoch *int64
	var leaseExpires *time.Time
	lockErr := results.QueryRow().Scan(&leaseRunID, &leaseWorker, &leaseEpoch, &leaseExpires)
	var databaseNow time.Time
	clockErr := results.QueryRow().Scan(&databaseNow)
	closeErr = results.Close()
	if errors.Is(lockErr, pgx.ErrNoRows) {
		return nil, notFound("thread")
	}
	if lockErr != nil {
		return nil, lockErr
	}
	if clockErr != nil {
		return nil, clockErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if leaseRunID == nil || *leaseRunID != runID || leaseWorker == nil || *leaseWorker != worker ||
		leaseEpoch == nil || *leaseEpoch != epoch || leaseExpires == nil || !leaseExpires.After(databaseNow) {
		return nil, staleEpoch()
	}
	thread := r.Thread
	thread.Epoch = *leaseEpoch
	return &Lease{Thread: thread, Run: r.Run, Worker: worker, Epoch: epoch, ExpiresAt: utcOf(*leaseExpires), Scope: scope}, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// inTx runs fn inside a transaction and rolls back on error.
func (s *Store) inTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if s == nil || s.pool == nil {
		return invalid("state: store is not initialised")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("state: begin: %w", err)
	}
	attempt, _ := ctx.Value(checkpointAttemptKey{}).(*checkpointAttempt)
	defer s.releaseCheckpointAttempt(attempt)
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if attempt != nil {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, checkpointOperationKey(attempt.id)); err != nil {
			return err
		}
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return classifyStoreError(err)
	}
	if attempt != nil && len(attempt.refs) > 0 {
		if s.checkpointArtifacts == nil {
			return contracts.NewError(contracts.ErrUnavailable, "checkpoint artifact owner is not configured")
		}
		if err := s.checkpointArtifacts.PinCheckpointReferences(ctx, attempt.scope, attempt.id, attempt.refs); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return classifyStoreError(fmt.Errorf("state: commit: %w", err))
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanThread(row scanner) (Thread, error) {
	var (
		id, contextID, tenant, deployment, subject string
		epoch                                      int64
		createdAt                                  time.Time
	)
	if err := row.Scan(&id, &contextID, &tenant, &deployment, &subject, &epoch, &createdAt); err != nil {
		return Thread{}, err
	}
	return Thread{
		ID:        id,
		ContextID: contextID,
		Scope:     contracts.Scope{Tenant: tenant, Deployment: deployment, Subject: subject},
		Epoch:     epoch,
		CreatedAt: createdAt.UTC(),
	}, nil
}

func scanRun(row scanner) (Run, error) {
	var (
		id, threadID, taskID, status, waitFor, traceparent string
		seq                                                int64
		payload                                            []byte
		deadline                                           *time.Time
	)
	if err := row.Scan(&id, &threadID, &taskID, &status, &waitFor, &seq, &payload, &deadline, &traceparent); err != nil {
		return Run{}, err
	}
	return Run{
		ID:          id,
		ThreadID:    threadID,
		TaskID:      taskID,
		Status:      status,
		WaitFor:     waitFor,
		Sequence:    seq,
		Payload:     json.RawMessage(payload),
		Deadline:    utcPtr(deadline),
		TraceParent: traceparent,
	}, nil
}

func scanRunWithThread(row scanner) (runRow, error) {
	var (
		id, taskID, status, waitFor, traceparent string
		threadID                                 string
		sequence                                 int64
		payload                                  json.RawMessage
		deadline                                 *time.Time
		digest                                   string
		tenant, deployment, subject, contextID   string
		epoch                                    int64
		createdAt                                time.Time
	)
	if err := row.Scan(&id, &threadID, &taskID, &status, &waitFor, &sequence, &payload,
		&deadline, &traceparent, &digest, &tenant, &deployment, &subject, &contextID, &epoch, &createdAt); err != nil {
		return runRow{}, err
	}
	return runRow{
		Run: Run{
			ID:          id,
			ThreadID:    threadID,
			TaskID:      taskID,
			Status:      status,
			WaitFor:     waitFor,
			Sequence:    sequence,
			Payload:     payload,
			Deadline:    utcPtr(deadline),
			TraceParent: traceparent,
		},
		Thread: Thread{
			ID:        threadID,
			ContextID: contextID,
			Scope:     contracts.Scope{Tenant: tenant, Deployment: deployment, Subject: subject},
			Epoch:     epoch,
			CreatedAt: createdAt.UTC(),
		},
		digest: digest,
	}, nil
}

func utcPtr(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func utcOf(t time.Time) time.Time { return t.UTC() }

// canonicalPayload validates, compacts and fingerprints a run payload. Invalid
// JSON, empty payloads and payloads above 1 MiB are rejected; the digest is
// taken over the canonical form so key-equivalent bodies always fingerprint
// identically.
func canonicalPayload(payload json.RawMessage) (stored string, digest string, err error) {
	if len(payload) == 0 {
		return "", "", invalid("payload must not be empty")
	}
	if len(payload) > maxPayloadBytes {
		return "", "", invalid("payload exceeds 1 MiB")
	}
	// contracts.Digest validates the JSON (rejecting trailing data, duplicate
	// keys and non-canonical numbers) and fingerprints the canonical form, so
	// key-equivalent bodies always produce the same fingerprint.
	digest, derr := contracts.Digest(payload)
	if derr != nil {
		return "", "", asInvalid(derr)
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, payload); err != nil {
		return "", "", invalid("payload is not valid JSON")
	}
	if buf.Len() > maxPayloadBytes {
		return "", "", invalid("payload exceeds 1 MiB")
	}
	return buf.String(), digest, nil
}

func validIdentifier(v, what string) error {
	if v == "" {
		return invalid(what + " must not be empty")
	}
	if len(v) > maxKeyBytes {
		return invalid(what + " exceeds 512 bytes")
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '-', c == '_', c == ':', c == '@', c == '/':
		default:
			return invalid(what + " contains an unsafe identifier character")
		}
	}
	return nil
}

// validScopePart applies the shared identifier rules to one scope component.
// It exists so deployment-level operations (claims) can validate tenant and
// deployment without requiring a subject.
func validScopePart(value, name string) error {
	if value == "" {
		return invalid("scope " + name + " must not be empty")
	}
	if len(value) > 128 {
		return invalid("scope " + name + " exceeds 128 bytes")
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '-', c == '_', c == ':', c == '@':
		default:
			return invalid("scope " + name + " contains an unsafe identifier character")
		}
	}
	return nil
}

func validWorker(worker string) error {
	if worker == "" {
		return invalid("worker must not be empty")
	}
	if len(worker) > maxWorkerBytes {
		return invalid("worker exceeds 256 bytes")
	}
	return nil
}

func validTTL(ttl time.Duration) error {
	if ttl < minLeaseTTL {
		return invalid("lease ttl must be positive and at least one microsecond")
	}
	if ttl > maxLeaseTTL {
		return invalid("lease ttl must not exceed 5 minutes")
	}
	return nil
}

func validLeaseArgs(scope contracts.Scope, runID, worker string, epoch int64) error {
	if err := scope.Validate(); err != nil {
		return asInvalid(err)
	}
	if err := validIdentifier(runID, "run id"); err != nil {
		return err
	}
	if err := validWorker(worker); err != nil {
		return err
	}
	if epoch < 1 {
		return invalid("lease epoch must be positive")
	}
	return nil
}

func invalid(msg string) error { return contracts.NewError(contracts.ErrInvalid, msg) }

func asInvalid(err error) error {
	if contracts.Code(err) != "" {
		return err
	}
	return invalid(err.Error())
}

func notFound(what string) error {
	return contracts.NewError(contracts.ErrNotFound, what+" not found")
}

func staleEpoch() error {
	return contracts.NewError(contracts.ErrStaleEpoch, "lease epoch is stale or the lease has expired")
}

// classifiedError carries a stable contract code while keeping the original
// driver error in the unwrap chain. Callers that branch on the PostgreSQL
// error (deadlock retry, unique-violation replay detection) keep working, and
// the transport still sees a distinguishable code instead of a bare 500.
type classifiedError struct {
	contract error
	raw      error
}

func (e *classifiedError) Error() string { return e.contract.Error() }

func (e *classifiedError) Unwrap() []error { return []error{e.contract, e.raw} }

// classifyStoreError gives every uncategorized database failure a contract
// code. Data-integrity and data-value failures are a conflict or a bad
// request, never an anonymous 500, and the driver text - which can carry
// identifiers - never reaches the client. A failure of the connection itself is
// left untouched: for a dependency that cannot be reached at all, 500 is the
// honest answer until a health check says otherwise.
//
// A database abort is availability too, and it is not a conflict. 40001 and
// 40P01 mean the caller's transaction was never evaluated against the competing
// fact - it was refused permission to commit - while 23505 means it was
// evaluated and lost on a rule the caller can actually fix. The two demand
// opposite actions (replay versus change the request), so they must not share a
// code: a caller that replays a uniqueness bug corrupts nothing but learns
// nothing, and a caller that treats an abort as final loses a write it was
// entitled to make.
func classifyStoreError(err error) error {
	if err == nil || contracts.Code(err) != "" {
		return err
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return err
	}
	switch pe.Code {
	case "23505": // unique_violation outside any handled idempotency path
		return &classifiedError{contracts.NewError(contracts.ErrConflict,
			"state: a uniqueness rule rejected the write"), err}
	case "23503", "23514": // foreign_key_violation, check_violation
		return &classifiedError{contracts.NewError(contracts.ErrNotFound,
			"state: the referenced row does not exist in this scope"), err}
	case "23502": // not_null_violation
		return &classifiedError{contracts.NewError(contracts.ErrInvalid,
			"state: a required field is missing"), err}
	case "22001", "22003": // string_data_right_truncation, numeric_value_out_of_range
		return &classifiedError{contracts.NewError(contracts.ErrInvalid,
			"state: a field is out of range"), err}
	case "22P02": // invalid_text_representation
		return &classifiedError{contracts.NewError(contracts.ErrInvalid,
			"state: a field has the wrong type"), err}
	case "40001", "40P01": // serialization_failure, deadlock_detected
		// The SQLSTATE is the only driver detail quoted: it is a fixed
		// five-character code, never a name or a value, so the rule that driver
		// text never reaches the client still holds. The transport turns
		// UNAVAILABLE into 503 (see pkg/transport/json.go), which is the status
		// an operator triages and a client is allowed to replay.
		return &classifiedError{contracts.NewError(contracts.ErrUnavailable,
			"state: the database aborted this transaction (SQLSTATE "+pe.Code+"); replay the operation"), err}
	default:
		return err
	}
}

// isUniqueViolation reports whether err is a PostgreSQL unique-constraint
// violation. Concurrent creations that race on a unique index surface this
// way; callers translate it into the idempotent replay path.
func isUniqueViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

// isDeadlockViolation reports the PostgreSQL deadlock detection code, which
// concurrent claims can trigger while competing for the same thread rows.
func isDeadlockViolation(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "40P01"
}

// isTransientAbort reports the two SQL states that mean "the database
// deliberately aborted this transaction; a fresh one may well succeed". It works
// on an already-classified error too, because classifiedError keeps the driver
// error in its unwrap chain.
func isTransientAbort(err error) bool {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return false
	}
	return pe.Code == "40001" || pe.Code == "40P01"
}

// maxLeaseAbortAttempts bounds one lease operation's replays. Eight is the same
// bound the claim loop has always used, and it is a bound on collisions, not on
// latency: with the backoff below, a fully exhausted operation waits under half
// a second before it reports the abort honestly.
const maxLeaseAbortAttempts = 8

// leaseAbortCounter is one operation's replay/give-up pair. Replays counts
// transactions this store ran again; GiveUps counts the operations that ran out
// of attempts and reported the abort to the caller. GiveUps is the number worth
// alarming on - Replays alone is contention doing its job.
type leaseAbortCounter struct {
	replays atomic.Uint64
	giveUps atomic.Uint64
}

// LeaseRetryCount is a readable snapshot of one operation's counters.
type LeaseRetryCount struct {
	Replays uint64
	GiveUps uint64
}

// LeaseRetryCounts reports this store's transient-abort replays by operation.
// The package has no metrics registry to push to, so the counters are exposed
// for a service-level handler and for tests; nothing here scrapes anything.
func (s *Store) LeaseRetryCounts() map[string]LeaseRetryCount {
	return map[string]LeaseRetryCount{
		"release": {Replays: s.leaseRetries.release.replays.Load(), GiveUps: s.leaseRetries.release.giveUps.Load()},
		"suspend": {Replays: s.leaseRetries.suspend.replays.Load(), GiveUps: s.leaseRetries.suspend.giveUps.Load()},
	}
}

// leaseAbortSleep waits out one backoff; a test shortens it. It returns false
// when the caller's context ends first, so a cancelled request stops replaying
// instead of waiting the abort out.
var leaseAbortSleep = func(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// withTransientAbortRetry runs fn again while PostgreSQL keeps aborting it for
// concurrency reasons, and returns anything else - including every honest
// rejection - immediately and untouched.
//
// Only doors proven replay-safe belong here, and the proof is the point:
// an abort is definite (the server reported it, so nothing of that attempt
// committed), and every write an attempt makes is a state-set or a fenced
// clear, never an increment or an unkeyed insert - the run status is written by
// value, the terminal event is inserted ON CONFLICT DO NOTHING under the run's
// own id, and the lease is cleared by a WHERE that names run, worker and epoch.
// A replay therefore ends in exactly one state, the same one one attempt would
// have produced.
//
// PutBatch is deliberately NOT replayed here, and the reason is not caution but
// an unread proof: the batch's checkpoint guard is built before the transaction
// with a fresh random id (checkpointReferenceContext) and every attempt appends
// its artifact refs to that same guard (markCheckpointPublication), which
// inTx then hands to PinCheckpointReferences after commit. Re-running the
// transaction without rebuilding the guard would pin the accumulated list, and
// BatchResult is assigned inside the closure rather than reset per attempt.
// A caller that gets 503 replays the whole batch instead, and that path IS
// idempotent - the checkpoint, writes and stream rows are all keyed off
// req.Key and digest-compared on replay - so the guarantee is kept at the door
// that has the proof, and the fix for the batch is to move the guard inside the
// attempt, which is a change of its own.
func (s *Store) withTransientAbortRetry(ctx context.Context, op string, counters *leaseAbortCounter, fn func() error) error {
	delay := 2 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil || !isTransientAbort(err) {
			return err
		}
		if attempt >= maxLeaseAbortAttempts {
			counters.giveUps.Add(1)
			return err
		}
		counters.replays.Add(1)
		// Jittered so contending workers do not wake in lockstep and collide
		// again - the same reason the identity store jitters its own backoff.
		wait := delay + time.Duration(rand.Int64N(int64(delay)+1))
		if !leaseAbortSleep(ctx, wait) {
			return err
		}
		if delay *= 2; delay > 64*time.Millisecond {
			delay = 64 * time.Millisecond
		}
	}
}
