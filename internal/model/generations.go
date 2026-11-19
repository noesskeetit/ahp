package model

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Generation lifecycle states. The strings are part of the persisted format.
const (
	GenPending   = "PENDING"
	GenRunning   = "RUNNING"
	GenStreaming = "STREAMING"
	GenCompleted = "COMPLETED"
	GenFailed    = "FAILED"
	GenCanceled  = "CANCELED"
)

// terminal reports whether a generation state is final.
func terminal(state string) bool {
	switch state {
	case GenCompleted, GenFailed, GenCanceled:
		return true
	}
	return false
}

// Generation is one durable model generation. Output is exact at this read's
// snapshot; Usage is settled from the provider's final accounting.
type Generation struct {
	FinishReason  string          `json:"finish_reason,omitempty"`
	DeltaSequence int64           `json:"delta_sequence,omitempty"`
	ID            string          `json:"id"`
	Scope         contracts.Scope `json:"-"`
	Task          string          `json:"task,omitempty"`
	Key           string          `json:"key"`
	Model         string          `json:"model"`
	// Provider is the provider COPY this generation was admitted and executed
	// against -- the upstream whose quota window paid and whose client dialled.
	// Empty means this deployment named no provider (single-provider wiring).
	Provider        string          `json:"provider,omitempty"`
	Status          string          `json:"status"`
	StatusVersion   int64           `json:"status_version"`
	Request         json.RawMessage `json:"request"`
	Output          string          `json:"output"`
	Usage           Usage           `json:"usage"`
	Error           string          `json:"error,omitempty"`
	CancelRequested bool            `json:"cancel_requested"`
	EffectKey       string          `json:"effect_key"`
	BudgetID        string          `json:"budget_id,omitempty"`
	ReserveKey      string          `json:"reserve_key,omitempty"`
	Reserved        int64           `json:"reserved,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	digest          string
}

const genColsBeforeRequest = `id, tenant, subject, deployment, COALESCE(task,''), key, model, status,
  status_version, `

// Read the parent and its immutable deltas in one MVCC snapshot. Missing
// fragments/counter mismatches are unavailable data, never empty completions.
const genOutput = `CASE WHEN output_from_deltas THEN
 (SELECT CASE WHEN count(*)=generations.delta_sequence
                   AND COALESCE(max(d.sequence),0)=generations.delta_sequence
                   AND COALESCE(sum(octet_length(d.text)),0)=generations.streamed_bytes
              THEN COALESCE(string_agg(d.text,'' ORDER BY d.sequence),'') ELSE NULL END
    FROM ahp_model.generation_deltas d
   WHERE d.generation_id=generations.id)
 ELSE output END`
const genColsAfterOutput = `, usage_prompt, usage_completion, usage_total,
  COALESCE(error,''), cancel_requested, effect_key, COALESCE(budget_id,''),
  COALESCE(reserve_key,''), reserved, created_at, updated_at, digest, COALESCE(provider,''), delta_sequence, finish_reason`
const genColsAfterRequest = `, ` + genOutput + genColsAfterOutput
const genCols = genColsBeforeRequest + `request::text` + genColsAfterRequest
const genStreamCols = genColsBeforeRequest + `NULL::text` + genColsAfterRequest
const genSnapshotCols = genColsBeforeRequest + `NULL::text, CASE
 WHEN status IN ('PENDING','RUNNING','STREAMING') THEN '' ELSE ` + genOutput + ` END` + genColsAfterOutput

// GenerationStore owns the ahp_model.generations table.
type GenerationStore struct {
	pool     *pgxpool.Pool
	boundary contracts.CheckpointBoundary
	// resultsOnly selects the results-only retention experiment: text fragments
	// are buffered in the volatile tail for live delivery but NEVER persisted as
	// fragment rows; only one aggregate generation result is durable. It is only
	// true when the boundary carries a volatile tail (see contracts).
	resultsOnly bool
	// tailsMu guards the per-generation volatile tails used by the step/turn
	// checkpoint-boundary experiment. It is never held across a database call.
	tailsMu sync.Mutex
	tails   map[string]*genTail
	// sealedOrder is the FIFO of generation ids whose results-only tails were
	// sealed at their boundary flush but are retained (no durable fragment rows to
	// fall back on) until a live reader drains them or they expire. The reaper
	// evicts from the front, so this gives the tails map a global capacity and a
	// bounded lifetime instead of an unbounded per-generation lifetime. Guarded by
	// tailsMu.
	sealedOrder []string
}

func NewGenerationStore(pool *pgxpool.Pool) *GenerationStore {
	return &GenerationStore{
		pool:        pool,
		boundary:    contracts.CurrentCheckpointBoundary(),
		resultsOnly: contracts.ResultsOnlyRetention(),
		tails:       make(map[string]*genTail),
	}
}

// Migrate idempotently creates the generations table (schema ahp_model already
// exists from the budget store; CREATE SCHEMA IF NOT EXISTS keeps this test).
func (s *GenerationStore) Migrate(ctx context.Context) error {
	err := migrateModel(ctx, s.pool, `
CREATE SCHEMA IF NOT EXISTS ahp_model;
CREATE TABLE IF NOT EXISTS ahp_model.generations (
  id               text PRIMARY KEY,
  tenant           text NOT NULL,
  subject          text NOT NULL,
  deployment       text NOT NULL,
  task             text,
  key              text NOT NULL,
  model            text NOT NULL,
  status           text NOT NULL,
  status_version   bigint NOT NULL DEFAULT 0,
  request          jsonb NOT NULL,
  output           text NOT NULL DEFAULT '',
  usage_prompt     integer NOT NULL DEFAULT 0,
  usage_completion integer NOT NULL DEFAULT 0,
  usage_total      integer NOT NULL DEFAULT 0,
  error            text,
  cancel_requested boolean NOT NULL DEFAULT false,
  effect_key       text NOT NULL,
  budget_id        text,
  reserve_key      text,
  reserved         bigint NOT NULL DEFAULT 0 CHECK (reserved >= 0),
  created_at       timestamptz NOT NULL DEFAULT now(),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  digest           text NOT NULL,
  UNIQUE (tenant, subject, deployment, key),
  CHECK (status IN ('PENDING','RUNNING','STREAMING','COMPLETED','FAILED','CANCELED'))
);
CREATE INDEX IF NOT EXISTS generations_task_idx ON ahp_model.generations (tenant, subject, deployment, task);`)
	if err != nil {
		return err
	}
	// Which provider COPY actually served this generation. Additive and nullable
	// by default (''), so every row written before this column exists keeps its
	// meaning and no existing path changes: the value is only as good as the
	// routing that named it, and a deployment without routing leaves it empty.
	//
	// It exists because the audit found the ledger able to say "tokens" but not
	// "tokens at which upstream": an operator reading ahp_model.generations could
	// not tell which copy a charge belongs to, so the quota row and the spend
	// could not be reconciled against each other (ARCH-02).
	err = migrateModel(ctx, s.pool, `
ALTER TABLE ahp_model.generations
  ADD COLUMN IF NOT EXISTS provider text NOT NULL DEFAULT ''`+tenantInsertGuards("generations"))
	if err != nil {
		return err
	}
	return migrateModel(ctx, s.pool, deltaDDL)
}

func requestDigest(scope contracts.Scope, task, key, model string, req ChatRequest) (string, error) {
	return contracts.Digest([]any{"gen.v1", scope, task, key, model, req})
}

func (s *GenerationStore) scanGen(row pgx.Row) (Generation, error) {
	var g Generation
	var output *string
	err := row.Scan(&g.ID, &g.Scope.Tenant, &g.Scope.Subject, &g.Scope.Deployment,
		&g.Task, &g.Key, &g.Model, &g.Status, &g.StatusVersion, &g.Request, &output,
		&g.Usage.PromptTokens, &g.Usage.CompletionTokens, &g.Usage.TotalTokens,
		&g.Error, &g.CancelRequested, &g.EffectKey, &g.BudgetID, &g.ReserveKey,
		&g.Reserved, &g.CreatedAt, &g.UpdatedAt, &g.digest, &g.Provider, &g.DeltaSequence, &g.FinishReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return g, contracts.NewError(contracts.ErrNotFound, "generation not found")
	}
	if err != nil {
		return g, err
	}
	if output == nil {
		return Generation{}, contracts.NewError(contracts.ErrUnavailable, "model: incomplete generation delta ledger")
	}
	g.Output = *output
	return g, nil
}

// Submit creates or replays a generation. The idempotency key is scoped to the
// caller; a replay with the same canonical request returns the stored row with
// created=false, a different request under a used key is ErrKeyReuse. budget
// reservation and the effect-ledger Accept happen in the Service so the
// admission decision (reserve-before-spend) lives in one place.
func (s *GenerationStore) Submit(ctx context.Context, scope contracts.Scope, task, key, model string, req ChatRequest, budgetID, reserveKey string, reserved int64, providers ...string) (Generation, bool, error) {
	provider := ""
	if len(providers) > 0 {
		provider = providers[0]
	}
	if err := scope.Validate(); err != nil {
		return Generation{}, false, err
	}
	if key == "" || model == "" {
		return Generation{}, false, contracts.NewError(contracts.ErrInvalid, "key and model required")
	}
	reqJSON, err := json.Marshal(req)
	if err != nil {
		return Generation{}, false, err
	}
	dg, err := requestDigest(scope, task, key, model, req)
	if err != nil {
		return Generation{}, false, err
	}
	effectKey := contracts.OperationKey(scope, task, "generation", key)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Generation{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if existing, err := s.lookup(ctx, tx, scope, key); err == nil {
		if existing.digest != dg || existing.Provider != provider || existing.BudgetID != budgetID || existing.ReserveKey != reserveKey || existing.Reserved != reserved {
			return Generation{}, false, contracts.NewError(contracts.ErrKeyReuse, "generation key reused with different request")
		}
		if err := tx.Commit(ctx); err != nil {
			return Generation{}, false, err
		}
		return existing, false, nil
	} else if contracts.Code(err) != contracts.ErrNotFound {
		return Generation{}, false, err
	}

	if _, err := tx.Exec(ctx,
		`INSERT INTO ahp_model.generations
		 (id,tenant,subject,deployment,task,key,model,status,request,effect_key,budget_id,reserve_key,reserved,digest,provider,output_from_deltas)
		 VALUES ($1,$2,$3,$4,NULLIF($5,''),$6,$7,$8,$9::jsonb,$10,NULLIF($11,''),NULLIF($12,''),$13,$14,$15,true)`,
		contracts.NewID(), scope.Tenant, scope.Subject, scope.Deployment, task, key, model,
		GenPending, reqJSON, effectKey, budgetID, reserveKey, reserved, dg, provider); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.SQLState() == "23505" {
			return Generation{}, false, errConflictRetry
		}
		return Generation{}, false, err
	}
	g, err := s.lookup(ctx, tx, scope, key)
	if err != nil {
		return Generation{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Generation{}, false, err
	}
	return g, true, nil
}

func (s *GenerationStore) lookup(ctx context.Context, q interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}, scope contracts.Scope, key string) (Generation, error) {
	return s.scanGen(q.QueryRow(ctx, `SELECT `+genCols+` FROM ahp_model.generations
		WHERE tenant=$1 AND subject=$2 AND deployment=$3 AND key=$4`,
		scope.Tenant, scope.Subject, scope.Deployment, key))
}

// Get returns a generation within scope.
func (s *GenerationStore) Get(ctx context.Context, scope contracts.Scope, id string) (Generation, error) {
	if err := scope.Validate(); err != nil {
		return Generation{}, err
	}
	return s.scanGen(s.pool.QueryRow(ctx, `SELECT `+genCols+` FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment))
}

// GetForStream loads the same scoped metadata and terminal fields as Get without
// reading the stored request. The NULL slot keeps the shared row scanner intact.
func (s *GenerationStore) GetForStream(ctx context.Context, scope contracts.Scope, id string) (Generation, error) {
	if err := scope.Validate(); err != nil {
		return Generation{}, err
	}
	return s.scanGen(s.pool.QueryRow(ctx, `SELECT `+genStreamCols+` FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment))
}

// GetStreamSnapshot is the internal hot reader: active content travels in
// ReadDeltas pages, not Generation.Output. Terminal fields are exact, except
// the request is omitted. Public Get/GetForStream/lookup remain unchanged.
func (s *GenerationStore) GetStreamSnapshot(ctx context.Context, scope contracts.Scope, id string) (Generation, error) {
	if err := scope.Validate(); err != nil {
		return Generation{}, err
	}
	return s.scanGen(s.pool.QueryRow(ctx, `SELECT `+genSnapshotCols+` FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment))
}

// The delta projection must run in a NEW command after this lock is acquired:
// a subquery in a waiting FOR UPDATE still has the pre-wait ledger snapshot.
// All callers keep the parent lock until transaction commit/rollback.
func (s *GenerationStore) lockGeneration(ctx context.Context, tx pgx.Tx, scope contracts.Scope, id string) error {
	var locked string
	err := tx.QueryRow(ctx, `SELECT id FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4 FOR UPDATE`, id, scope.Tenant, scope.Subject, scope.Deployment).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.NewError(contracts.ErrNotFound, "generation not found")
	}
	return err
}

// Transition moves a generation between non-terminal states, bumping
// status_version. It fails (ErrConflict) if the current status is not one of
// the allowed predecessors, so a stale worker can never regress a generation.
func (s *GenerationStore) Transition(ctx context.Context, scope contracts.Scope, id string, from []string, to string) (Generation, error) {
	if terminal(to) {
		return Generation{}, contracts.NewError(contracts.ErrInvalid, "use Finalize for terminal states")
	}
	ph := make([]string, len(from))
	args := []any{to, id, scope.Tenant, scope.Subject, scope.Deployment}
	base := len(args)
	for i, f := range from {
		ph[i] = "$" + itoa(base+1+i)
		args = append(args, f)
	}
	q := `UPDATE ahp_model.generations SET status=$1, status_version=status_version+1, updated_at=now()
	      WHERE id=$2 AND tenant=$3 AND subject=$4 AND deployment=$5 AND status IN (` + join(ph, ",") + `)
	      RETURNING id`
	if _, err := s.pool.Exec(ctx, q, args...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Generation{}, contracts.NewError(contracts.ErrConflict, "generation status transition not allowed")
		}
		return Generation{}, err
	}
	return s.Get(ctx, scope, id)
}

// ShouldCancel reports whether cancellation has been requested for a live
// generation. The service polls this cheaply between deltas.
func (s *GenerationStore) ShouldCancel(ctx context.Context, scope contracts.Scope, id string) (bool, error) {
	var cancel bool
	err := s.pool.QueryRow(ctx,
		`SELECT cancel_requested FROM ahp_model.generations
		 WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`,
		id, scope.Tenant, scope.Subject, scope.Deployment).Scan(&cancel)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, contracts.NewError(contracts.ErrNotFound, "generation not found")
	}
	return cancel, err
}

// RequestCancel marks a non-terminal generation for cancellation and, if it is
// still PENDING, transitions it straight to CANCELED. Live generations are
// observed by the worker which flips them to CANCELED at the next checkpoint.
func (s *GenerationStore) RequestCancel(ctx context.Context, scope contracts.Scope, id string) (Generation, error) {
	if err := scope.Validate(); err != nil {
		return Generation{}, err
	}
	if _, err := s.pool.Exec(ctx,
		`UPDATE ahp_model.generations SET cancel_requested = true, updated_at = now()
		 WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4
		   AND status IN ('PENDING','RUNNING','STREAMING')`,
		id, scope.Tenant, scope.Subject, scope.Deployment); err != nil {
		return Generation{}, err
	}
	// Best-effort immediate terminal transition for not-yet-started work.
	_, _ = s.pool.Exec(ctx,
		`UPDATE ahp_model.generations SET status='CANCELED', status_version=status_version+1, updated_at=now()
		 WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4 AND status='PENDING'`,
		id, scope.Tenant, scope.Subject, scope.Deployment)
	return s.Get(ctx, scope, id)
}

// Finalize writes a terminal state plus the final error/usage. Replays of the
// same terminal state are idempotent; a terminal state can never be overwritten
// by a different terminal state.
func (s *GenerationStore) Finalize(ctx context.Context, scope contracts.Scope, id, status, errText string, usage Usage) (Generation, error) {
	if !terminal(status) {
		return Generation{}, contracts.NewError(contracts.ErrInvalid, "not a terminal status")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Generation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.lockGeneration(ctx, tx, scope, id); err != nil {
		return Generation{}, err
	}
	g, err := s.scanGen(tx.QueryRow(ctx, `SELECT `+genCols+` FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment))
	if err != nil {
		return Generation{}, err
	}
	if terminal(g.Status) {
		if err := tx.Commit(ctx); err != nil {
			return Generation{}, err
		}
		return g, nil // idempotent: first terminal writer wins
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_model.generations
		 SET status=$2, status_version=status_version+1, error=NULLIF($3,''),
		     usage_prompt=$4, usage_completion=$5, usage_total=$6, updated_at=now()
		 WHERE id=$1`, id, status, errText, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens); err != nil {
		return Generation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Generation{}, err
	}
	return s.Get(ctx, scope, id)
}

// FinalizeWithOutput is Finalize with one atomic rewrite of the output column.
// It exists for the tool-wire finish: when the assistant's terminal message is
// a set of tool calls, the accumulated stream text is NOT the generation's
// result -- the reassembled calls are, and they must land in the same
// transaction that declares the generation COMPLETED, never as a second write
// that could be lost between "completed" and "output stored".
// An empty newOutput keeps Finalize's semantics exactly.
func (s *GenerationStore) FinalizeWithOutput(ctx context.Context, scope contracts.Scope, id, status, errText, newOutput string, usage Usage, finishReasons ...string) (Generation, error) {
	if newOutput == "" && len(finishReasons) == 0 {
		return s.Finalize(ctx, scope, id, status, errText, usage)
	}
	if !terminal(status) {
		return Generation{}, contracts.NewError(contracts.ErrInvalid, "not a terminal status")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Generation{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.lockGeneration(ctx, tx, scope, id); err != nil {
		return Generation{}, err
	}
	g, err := s.scanGen(tx.QueryRow(ctx, `SELECT `+genCols+` FROM ahp_model.generations
		WHERE id=$1 AND tenant=$2 AND subject=$3 AND deployment=$4`, id, scope.Tenant, scope.Subject, scope.Deployment))
	if err != nil {
		return Generation{}, err
	}
	if terminal(g.Status) {
		if err := tx.Commit(ctx); err != nil {
			return Generation{}, err
		}
		return g, nil // idempotent: first terminal writer wins
	}
	if _, err := tx.Exec(ctx,
		`UPDATE ahp_model.generations
		 SET status=$2, status_version=status_version+1, error=NULLIF($3,''), output=CASE WHEN $7='' THEN output ELSE $7 END,
		     output_from_deltas=CASE WHEN $7='' THEN output_from_deltas ELSE false END, finish_reason=$8,
		     usage_prompt=$4, usage_completion=$5, usage_total=$6, updated_at=now()
		 WHERE id=$1`, id, status, errText, usage.PromptTokens, usage.CompletionTokens, usage.TotalTokens, newOutput, firstFinishReason(finishReasons)); err != nil {
		return Generation{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Generation{}, err
	}
	return s.Get(ctx, scope, id)
}

// helper shims (avoid importing strconv just for placeholders).
func itoa(n int) string { return jsonItoa(n) }

func join(parts []string, sep string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}

func firstFinishReason(reasons []string) string {
	if len(reasons) > 0 {
		return reasons[0]
	}
	return ""
}
