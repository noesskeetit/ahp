package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaPattern is the allow-list for the schema name a gateway owns. It is
// deliberately identical in strictness to the effect ledger's own rule so one
// service can never be tricked into writing into another service's schema.
var schemaPattern = regexp.MustCompile(`^ahp_[a-z][a-z0-9_]{0,40}$`)

// Store is the durable PostgreSQL state of the gateway. It owns its schema and
// nothing else: it never reads or writes another service's tables.
type Store struct {
	pool   *pgxpool.Pool
	schema string
}

// NewStore returns a store bound to schema, which must be an ahp_* identifier.
func NewStore(pool *pgxpool.Pool, schema string) (*Store, error) {
	if pool == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: nil pool")
	}
	if !schemaPattern.MatchString(schema) {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"gateway: schema must match ^ahp_[a-z][a-z0-9_]{0,40}$")
	}
	return &Store{pool: pool, schema: schema}, nil
}

// Pool exposes the pool so the service can share it with its effect ledger.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// queryable is satisfied by both *pgxpool.Pool and pgx.Tx so one helper can run
// either on the pool or inside a caller's transaction.
type queryable interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (s *Store) t(name string) string { return s.schema + "." + name }

// validTaskState rejects the empty sentinel and any value the official a2a
// type set does not define. a2a-go exposes no Valid() method, so the gateway
// owns this check at its ingest boundary.
func validTaskState(state a2a.TaskState) bool {
	switch state {
	case a2a.TaskStateSubmitted, a2a.TaskStateWorking, a2a.TaskStateCompleted,
		a2a.TaskStateFailed, a2a.TaskStateCanceled, a2a.TaskStateRejected,
		a2a.TaskStateInputRequired, a2a.TaskStateAuthRequired:
		return true
	default:
		return false
	}
}

// Migrate creates the gateway schema and all of its tables. It is idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + s.schema,
		`CREATE TABLE IF NOT EXISTS ` + s.t("contexts") + ` (
			context_id  text PRIMARY KEY,
			tenant      text NOT NULL,
			deployment  text NOT NULL,
			subject     text NOT NULL,
			caller      text NOT NULL,
			created_at  timestamptz NOT NULL DEFAULT now()
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s.t("tasks") + ` (
			task_id          text PRIMARY KEY,
			tenant           text NOT NULL,
			deployment       text NOT NULL,
			subject          text NOT NULL,
			caller           text NOT NULL,
			context_id       text NOT NULL,
			kind             text NOT NULL DEFAULT '',
			parent_task      text NOT NULL DEFAULT '',
			state            text NOT NULL,
			status_message   jsonb,
			artifacts        jsonb NOT NULL DEFAULT '[]'::jsonb,
			metadata         jsonb,
			status_version   bigint NOT NULL DEFAULT 1,
			fencing_token    bigint NOT NULL DEFAULT 1,
			lease_owner      text NOT NULL DEFAULT '',
			lease_expires_at timestamptz,
			cancel_requested boolean NOT NULL DEFAULT false,
			attempts         integer NOT NULL DEFAULT 0,
			created_at       timestamptz NOT NULL DEFAULT now(),
			updated_at       timestamptz NOT NULL DEFAULT now(),
			terminal_at      timestamptz
		)`,
		// Index names are never schema qualified: PostgreSQL takes only a bare
		// ColId there and places the index in the table's schema.
		`CREATE INDEX IF NOT EXISTS tasks_scope_idx
			ON ` + s.t("tasks") + ` (tenant, deployment, subject, context_id, created_at)`,
		`CREATE INDEX IF NOT EXISTS tasks_claim_idx
			ON ` + s.t("tasks") + ` (state, lease_expires_at)`,
		`CREATE TABLE IF NOT EXISTS ` + s.t("task_messages") + ` (
			task_id    text NOT NULL REFERENCES ` + s.t("tasks") + ` ON DELETE CASCADE,
			seq        bigserial,
			role       text NOT NULL,
			payload    jsonb NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (task_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s.t("task_events") + ` (
			task_id    text NOT NULL,
			version    bigint NOT NULL,
			tenant     text NOT NULL,
			deployment text NOT NULL,
			subject    text NOT NULL,
			kind       text NOT NULL,
			payload    jsonb NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (task_id, version)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s.t("messages") + ` (
			tenant     text NOT NULL,
			deployment text NOT NULL,
			subject    text NOT NULL,
			caller     text NOT NULL,
			target     text NOT NULL,
			message_id text NOT NULL,
			digest     text NOT NULL,
			task_id    text NOT NULL,
			replayed   boolean NOT NULL DEFAULT false,
			created_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant, deployment, subject, caller, target, message_id)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + s.t("outbox") + ` (
			seq            bigserial PRIMARY KEY,
			tenant         text NOT NULL,
			deployment     text NOT NULL,
			subject        text NOT NULL,
			task_id        text NOT NULL,
			parent_task    text NOT NULL DEFAULT '',
			kind           text NOT NULL,
			status_version bigint NOT NULL,
			op_key         text NOT NULL,
			fencing_token  bigint NOT NULL,
			payload        jsonb NOT NULL,
			state          text NOT NULL DEFAULT 'PENDING',
			attempts       integer NOT NULL DEFAULT 0,
			last_error     text NOT NULL DEFAULT '',
			next_at        timestamptz NOT NULL DEFAULT now(),
			created_at     timestamptz NOT NULL DEFAULT now(),
			updated_at     timestamptz NOT NULL DEFAULT now(),
			UNIQUE (tenant, deployment, subject, op_key)
		)`,
		`CREATE INDEX IF NOT EXISTS outbox_claim_idx
			ON ` + s.t("outbox") + ` (state, next_at)`,
		`CREATE TABLE IF NOT EXISTS ` + s.t("push_configs") + ` (
			tenant     text NOT NULL,
			deployment text NOT NULL,
			subject    text NOT NULL,
			task_id    text NOT NULL,
			id         text NOT NULL,
			url        text NOT NULL,
			token      text NOT NULL DEFAULT '',
			auth       jsonb,
			created_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant, deployment, subject, task_id, id)
		)`,
		// child_status_inbox is the receiving half of the child-status
		// contract: deliveries arrive at-least-once, so the parent's own
		// deployment must deduplicate them durably. The primary key is the
		// idempotency identity (who owns the graph, which child, which
		// version); body_digest separates a byte-identical redelivery from a
		// conflicting reuse of the same version.
		`CREATE TABLE IF NOT EXISTS ` + s.t("child_status_inbox") + ` (
			tenant         text NOT NULL,
			deployment     text NOT NULL,
			subject        text NOT NULL,
			parent_task    text NOT NULL,
			task_id        text NOT NULL,
			status_version bigint NOT NULL,
			state          text NOT NULL,
			body_digest    text NOT NULL,
			delivery_key   text NOT NULL DEFAULT '',
			applied_at     timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (tenant, deployment, subject, task_id, status_version)
		)`,
		`CREATE INDEX IF NOT EXISTS child_status_parent_idx
			ON ` + s.t("child_status_inbox") + ` (tenant, deployment, subject, parent_task)`,
	}
	for _, stmt := range stmts {
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("gateway: migrate: %w", err)
		}
	}
	return s.tenantGuards(ctx)
}

// TaskRow is one durable task record.
type TaskRow struct {
	ID              string
	Scope           contracts.Scope
	Caller          string
	ContextID       string
	Kind            string
	ParentTask      string
	State           a2a.TaskState
	StatusMessage   *a2a.Message
	Artifacts       []*a2a.Artifact
	Metadata        map[string]any
	StatusVersion   int64
	FencingToken    int64
	LeaseOwner      string
	LeaseExpiresAt  *time.Time
	CancelRequested bool
	Attempts        int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	TerminalAt      *time.Time
}

// Terminal reports whether no further transition is allowed for the task.
func (t TaskRow) Terminal() bool { return t.State.Terminal() }

// Event is one journalled task transition.
type Event struct {
	TaskID  string
	Version int64
	Kind    string // "status" | "artifact"
	Scope   contracts.Scope
	Payload a2a.Event
	Created time.Time
}

// OutboxEntry is a durable at-least-once delivery obligation.
type OutboxEntry struct {
	Seq          int64
	Scope        contracts.Scope
	TaskID       string
	ParentTask   string
	Kind         string
	StatusVer    int64
	OpKey        string
	FencingToken int64
	Payload      json.RawMessage
	State        string
	Attempts     int
}

// Outbox states.
const (
	OutboxPending     = "PENDING"
	OutboxInFlight    = "IN_FLIGHT"
	OutboxDone        = "DONE"
	OutboxFailed      = "FAILED"
	OutboxUnknown     = "SIDE_EFFECT_UNKNOWN"
	OutboxUnavailable = "RETRY_EXHAUSTED"
)

// AcquireContext binds a context id to (scope, caller) or verifies that an
// existing id is owned by exactly that pair. Cross-caller or cross-scope reuse
// is a FORBIDDEN, never a silent adoption.
func (s *Store) AcquireContext(ctx context.Context, scope contracts.Scope, caller, contextID string) (string, error) {
	return s.acquireContext(ctx, nil, scope, caller, contextID)
}

// AcquireContextTx is AcquireContext bound to an open transaction. The gateway
// takes every write inside a transaction, so the store must never borrow a
// second pool connection while one is already held.
func (s *Store) AcquireContextTx(ctx context.Context, tx pgx.Tx, scope contracts.Scope, caller, contextID string) (string, error) {
	return s.acquireContext(ctx, tx, scope, caller, contextID)
}

func (s *Store) acquireContext(ctx context.Context, tx pgx.Tx, scope contracts.Scope, caller, contextID string) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if caller == "" {
		return "", contracts.NewReasoned(contracts.ErrInvalid, ReasonCallerRequired, "gateway: caller required")
	}
	var q queryable = s.pool
	if tx != nil {
		q = tx
	}
	if contextID == "" {
		id := "ctx_" + contracts.NewID()
		_, err := q.Exec(ctx, `INSERT INTO `+s.t("contexts")+
			` (context_id, tenant, deployment, subject, caller) VALUES ($1,$2,$3,$4,$5)`,
			id, scope.Tenant, scope.Deployment, scope.Subject, caller)
		if err != nil {
			return "", fmt.Errorf("gateway: create context: %w", err)
		}
		return id, nil
	}
	if len(contextID) > 256 {
		return "", contracts.NewReasoned(contracts.ErrInvalid, ReasonContextIDTooLong, "gateway: context id too long")
	}
	var owner string
	err := q.QueryRow(ctx, `SELECT caller FROM `+s.t("contexts")+
		` WHERE context_id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4`,
		contextID, scope.Tenant, scope.Deployment, scope.Subject).Scan(&owner)
	if errors.Is(err, pgx.ErrNoRows) {
		// A caller may establish a client-named context on first use: the
		// gateway claims it for exactly this caller+scope. Concurrent claims
		// collapse onto one owner; the loser then sees the winner's owner and
		// is rejected below, so adoption is never silent.
		tag, cerr := q.Exec(ctx, `INSERT INTO `+s.t("contexts")+
			` (context_id, tenant, deployment, subject, caller) VALUES ($1,$2,$3,$4,$5)`+
			` ON CONFLICT (context_id) DO NOTHING`,
			contextID, scope.Tenant, scope.Deployment, scope.Subject, caller)
		if cerr != nil {
			return "", fmt.Errorf("gateway: claim context: %w", cerr)
		}
		if tag.RowsAffected() == 1 {
			return contextID, nil
		}
		if rerr := q.QueryRow(ctx, `SELECT caller FROM `+s.t("contexts")+
			` WHERE context_id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4`,
			contextID, scope.Tenant, scope.Deployment, scope.Subject).Scan(&owner); rerr != nil {
			if errors.Is(rerr, pgx.ErrNoRows) {
				return "", contracts.NewReasoned(contracts.ErrForbidden, ReasonContextForeignScope,
					"gateway: context is owned outside this scope")
			}
			return "", fmt.Errorf("gateway: lookup context after claim race: %w", rerr)
		}
		if owner != caller {
			return "", contracts.NewReasoned(contracts.ErrForbidden, ReasonContextNotOwned,
				"gateway: context is not owned by this caller")
		}
		return contextID, nil
	}
	if err != nil {
		return "", fmt.Errorf("gateway: lookup context: %w", err)
	}
	if owner != caller {
		return "", contracts.NewReasoned(contracts.ErrForbidden, ReasonContextNotOwned,
			"gateway: context is not owned by this caller")
	}
	return contextID, nil
}

// MessageKey identifies the idempotency record of one inbound message.
type MessageKey struct {
	Caller    string
	Target    string // task id for a follow-up, context id for a new task
	MessageID string
}

// normalizeTarget maps the empty target of a brand-new conversation to a
// reserved sentinel so retries of "start a new conversation with messageId X"
// collapse onto one record instead of silently creating a second task.
func normalizeTarget(target string) string {
	if target == "" {
		return "-"
	}
	return target
}

// ClaimMessage reserves the idempotency key inside tx. It returns replay=true
// with the task the original request created, or an error when the same key is
// reused with a different canonical digest.
func (s *Store) ClaimMessage(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key MessageKey, digest string) (taskID string, replay bool, err error) {
	if key.Caller == "" || key.MessageID == "" {
		return "", false, contracts.NewReasoned(contracts.ErrInvalid, ReasonIdempotencyKeyPartial, "gateway: incomplete idempotency key")
	}
	key.Target = normalizeTarget(key.Target)
	if len(digest) < len(contracts.DigestPrefix)+16 {
		return "", false, contracts.NewReasoned(contracts.ErrInvalid, ReasonDigestUnusable, "gateway: bad digest")
	}
	var existing, existingTask string
	q := `SELECT digest, task_id FROM ` + s.t("messages") + `
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND caller=$4 AND target=$5 AND message_id=$6`
	err = tx.QueryRow(ctx, q, scope.Tenant, scope.Deployment, scope.Subject,
		key.Caller, key.Target, key.MessageID).Scan(&existing, &existingTask)
	switch {
	case err == nil:
		if existing != digest {
			return "", false, contracts.NewReasoned(contracts.ErrKeyReuse, ReasonKeyReuseMismatch,
				"gateway: message id reused with a different payload")
		}
		return existingTask, true, nil
	case !errors.Is(err, pgx.ErrNoRows):
		return "", false, fmt.Errorf("gateway: claim message: %w", err)
	}
	return "", false, nil
}

// RecordMessage stores the idempotency result once the task is known.
func (s *Store) RecordMessage(ctx context.Context, tx pgx.Tx, scope contracts.Scope, key MessageKey, digest, taskID string) error {
	key.Target = normalizeTarget(key.Target)
	q := `INSERT INTO ` + s.t("messages") + `
		(tenant, deployment, subject, caller, target, message_id, digest, task_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
		ON CONFLICT (tenant, deployment, subject, caller, target, message_id) DO NOTHING`
	_, err := tx.Exec(ctx, q, scope.Tenant, scope.Deployment, scope.Subject,
		key.Caller, key.Target, key.MessageID, digest, taskID)
	if err != nil {
		return fmt.Errorf("gateway: record message: %w", err)
	}
	return nil
}

// InsertTask creates a task in TASK_STATE_SUBMITTED with status_version 1.
func (s *Store) InsertTask(ctx context.Context, tx pgx.Tx, row TaskRow) (TaskRow, error) {
	if err := row.Scope.Validate(); err != nil {
		return TaskRow{}, err
	}
	if row.ID == "" {
		row.ID = string(a2a.NewTaskID())
	}
	if row.Kind == "" {
		return TaskRow{}, contracts.NewReasoned(contracts.ErrInvalid, ReasonTaskKindMissing, "gateway: task kind required")
	}
	artifacts, err := json.Marshal(row.Artifacts)
	if err != nil {
		return TaskRow{}, fmt.Errorf("gateway: encode artifacts: %w", err)
	}
	metadata, err := json.Marshal(row.Metadata)
	if err != nil {
		return TaskRow{}, fmt.Errorf("gateway: encode metadata: %w", err)
	}
	q := `INSERT INTO ` + s.t("tasks") + `
		(task_id, tenant, deployment, subject, caller, context_id, kind, parent_task,
		 state, artifacts, metadata, status_version, fencing_token)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,1,1)
		RETURNING created_at, updated_at`
	err = tx.QueryRow(ctx, q, row.ID, row.Scope.Tenant, row.Scope.Deployment, row.Scope.Subject,
		row.Caller, row.ContextID, row.Kind, row.ParentTask, string(row.State),
		string(artifacts), string(metadata)).Scan(&row.CreatedAt, &row.UpdatedAt)
	if err != nil {
		return TaskRow{}, fmt.Errorf("gateway: insert task: %w", err)
	}
	row.State = a2a.TaskStateSubmitted
	row.StatusVersion = 1
	row.FencingToken = 1
	return row, nil
}

// ResolveParentChainTx audits a declared parent task inside the caller's
// tenant and subject and returns the tree root and the depth the NEW child
// would occupy (root itself is depth 0). Tree fields are gateway-set facts
// (PDF rev3.2, 6.1), so a parent this gateway cannot see is refused, not
// trusted: an unverifiable parent would put the child -- and its Model
// Gateway budget carve-out -- into a tree the platform cannot audit.
// Cycles and chains beyond MaxTaskTreeDepth fail closed: the audit must
// never be able to spin.
func (s *Store) ResolveParentChainTx(ctx context.Context, tx pgx.Tx, scope contracts.Scope, parentID string) (string, int, error) {
	seen := make(map[string]bool, 8)
	cur := parentID
	for depth := 1; ; depth++ {
		if seen[cur] {
			return "", 0, contracts.NewReasoned(contracts.ErrInvalid, ReasonParentCycle, "gateway: parent chain contains a cycle")
		}
		seen[cur] = true
		var gp string
		err := tx.QueryRow(ctx, `SELECT parent_task FROM `+s.t("tasks")+`
			WHERE task_id=$1 AND tenant=$2 AND subject=$3`,
			cur, scope.Tenant, scope.Subject).Scan(&gp)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", 0, contracts.NewReasoned(contracts.ErrInvalid, ReasonParentUnavailable, "gateway: parent task not found in this tenant and subject")
		}
		if err != nil {
			return "", 0, fmt.Errorf("gateway: audit parent chain: %w", err)
		}
		if gp == "" {
			if depth > MaxTaskTreeDepth {
				return "", 0, contracts.NewReasoned(contracts.ErrInvalid, ReasonTreeDepth, "gateway: task tree exceeds depth guard")
			}
			return cur, depth, nil
		}
		if depth > MaxTaskTreeDepth {
			return "", 0, contracts.NewReasoned(contracts.ErrInvalid, ReasonTreeDepth, "gateway: task tree exceeds depth guard")
		}
		cur = gp
	}
}

const taskColumns = `task_id, tenant, deployment, subject, caller, context_id, kind, parent_task,
	state, status_message, artifacts, metadata, status_version, fencing_token, lease_owner,
	lease_expires_at, cancel_requested, attempts, created_at, updated_at, terminal_at`

func scanTask(row pgx.Row) (TaskRow, error) {
	var (
		t       TaskRow
		status  []byte
		arts    []byte
		meta    []byte
		state   string
		leaseAt *time.Time
	)
	err := row.Scan(&t.ID, &t.Scope.Tenant, &t.Scope.Deployment, &t.Scope.Subject, &t.Caller,
		&t.ContextID, &t.Kind, &t.ParentTask, &state, &status, &arts, &meta, &t.StatusVersion,
		&t.FencingToken, &t.LeaseOwner, &leaseAt, &t.CancelRequested, &t.Attempts,
		&t.CreatedAt, &t.UpdatedAt, &t.TerminalAt)
	if err != nil {
		return TaskRow{}, err
	}
	t.State = a2a.TaskState(state)
	t.LeaseExpiresAt = leaseAt
	if len(status) > 0 {
		var m a2a.Message
		if err := json.Unmarshal(status, &m); err != nil {
			return TaskRow{}, fmt.Errorf("gateway: decode status message: %w", err)
		}
		t.StatusMessage = &m
	}
	if len(arts) > 0 {
		if err := json.Unmarshal(arts, &t.Artifacts); err != nil {
			return TaskRow{}, fmt.Errorf("gateway: decode artifacts: %w", err)
		}
	}
	if len(meta) > 0 {
		if err := json.Unmarshal(meta, &t.Metadata); err != nil {
			return TaskRow{}, fmt.Errorf("gateway: decode metadata: %w", err)
		}
	}
	return t, nil
}

// Task loads one task of the given scope; a foreign task is NOT_FOUND.
func (s *Store) Task(ctx context.Context, scope contracts.Scope, id string) (TaskRow, error) {
	q := `SELECT ` + taskColumns + ` FROM ` + s.t("tasks") + `
		WHERE task_id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4`
	row, err := scanTask(s.pool.QueryRow(ctx, q, id, scope.Tenant, scope.Deployment, scope.Subject))
	if errors.Is(err, pgx.ErrNoRows) {
		return TaskRow{}, a2a.ErrTaskNotFound
	}
	if err != nil {
		return TaskRow{}, fmt.Errorf("gateway: load task: %w", err)
	}
	return row, nil
}

// ListTasks returns a scope-filtered page of tasks.
func (s *Store) ListTasks(ctx context.Context, scope contracts.Scope, contextID string, state a2a.TaskState, pageSize int, pageToken string) ([]TaskRow, int, string, error) {
	if err := scope.Validate(); err != nil {
		return nil, 0, "", err
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50
	}
	offset := int64(0)
	if pageToken != "" {
		n, err := strconv.ParseInt(pageToken, 10, 64)
		if err != nil || n < 0 {
			return nil, 0, "", contracts.NewReasoned(contracts.ErrInvalid, ReasonPageTokenUnusable, "gateway: bad page token")
		}
		offset = n
	}
	where := `tenant=$1 AND deployment=$2 AND subject=$3`
	args := []any{scope.Tenant, scope.Deployment, scope.Subject}
	if contextID != "" {
		args = append(args, contextID)
		where += ` AND context_id=$` + strconv.Itoa(len(args))
	}
	if state != "" {
		args = append(args, string(state))
		where += ` AND state=$` + strconv.Itoa(len(args))
	}
	var total int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+s.t("tasks")+` WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, "", fmt.Errorf("gateway: count tasks: %w", err)
	}
	args = append(args, pageSize, offset)
	q := `SELECT ` + taskColumns + ` FROM ` + s.t("tasks") + ` WHERE ` + where +
		` ORDER BY created_at DESC, task_id DESC LIMIT $` + strconv.Itoa(len(args)-1) +
		` OFFSET $` + strconv.Itoa(len(args))
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, "", fmt.Errorf("gateway: list tasks: %w", err)
	}
	defer rows.Close()
	var out []TaskRow
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, 0, "", fmt.Errorf("gateway: scan task: %w", err)
		}
		out = append(out, t)
	}
	next := ""
	if int(offset)+len(out) < total {
		next = strconv.FormatInt(offset+int64(len(out)), 10)
	}
	return out, total, next, rows.Err()
}

// History returns the ordered conversation of a task.
func (s *Store) History(ctx context.Context, taskID string, limit int) ([]*a2a.Message, error) {
	q := `SELECT payload FROM ` + s.t("task_messages") + ` WHERE task_id=$1 ORDER BY seq`
	if limit > 0 {
		q += ` DESC LIMIT $2`
	}
	args := []any{taskID}
	if limit > 0 {
		args = append(args, limit)
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("gateway: history: %w", err)
	}
	defer rows.Close()
	var out []*a2a.Message
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return nil, fmt.Errorf("gateway: history scan: %w", err)
		}
		var m a2a.Message
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("gateway: history decode: %w", err)
		}
		out = append(out, &m)
	}
	if limit > 0 {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out, rows.Err()
}

// AppendMessage journals an inbound or outbound message inside tx.
func (s *Store) AppendMessage(ctx context.Context, tx pgx.Tx, taskID string, m *a2a.Message) error {
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("gateway: encode message: %w", err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+s.t("task_messages")+` (task_id, role, payload) VALUES ($1,$2,$3::jsonb)`,
		taskID, string(m.Role), string(raw))
	if err != nil {
		return fmt.Errorf("gateway: append message: %w", err)
	}
	return nil
}

// AdvanceStatus moves a task forward. A version mismatch means another writer
// won the race: the caller gets STALE_EPOCH and must not retry blindly.
func (s *Store) AdvanceStatus(ctx context.Context, tx pgx.Tx, scope contracts.Scope, taskID string, expected int64, state a2a.TaskState, msg *a2a.Message) (TaskRow, error) {
	if !validTaskState(state) {
		return TaskRow{}, contracts.NewReasoned(contracts.ErrInvalid, ReasonTaskStateUnknown, "gateway: unknown task state")
	}
	var statusArg any
	if msg != nil {
		raw, err := json.Marshal(msg)
		if err != nil {
			return TaskRow{}, fmt.Errorf("gateway: encode status message: %w", err)
		}
		statusArg = string(raw)
	}
	q := `UPDATE ` + s.t("tasks") + `
		SET state=$5, status_message=COALESCE($6::jsonb, status_message),
		    status_version = status_version + 1,
		    updated_at = now(),
		    terminal_at = CASE WHEN $7::boolean THEN now() ELSE terminal_at END
		WHERE task_id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4
		  AND status_version=$8 AND state <> $5
		RETURNING ` + taskColumns
	row, err := scanTask(tx.QueryRow(ctx, q, taskID, scope.Tenant, scope.Deployment, scope.Subject,
		string(state), statusArg, state.Terminal(), expected))
	if errors.Is(err, pgx.ErrNoRows) {
		current, lookupErr := s.Task(ctx, scope, taskID)
		if lookupErr != nil {
			return TaskRow{}, lookupErr
		}
		if current.StatusVersion != expected {
			return TaskRow{}, contracts.NewError(contracts.ErrStaleEpoch,
				"gateway: status version moved under us")
		}
		return TaskRow{}, contracts.NewError(contracts.ErrConflict,
			"gateway: task already in this state")
	}
	if err != nil {
		return TaskRow{}, fmt.Errorf("gateway: advance status: %w", err)
	}
	return row, nil
}

// SetArtifacts replaces the artifact list of a task inside tx.
func (s *Store) SetArtifacts(ctx context.Context, tx pgx.Tx, taskID string, artifacts []*a2a.Artifact) error {
	raw, err := json.Marshal(artifacts)
	if err != nil {
		return fmt.Errorf("gateway: encode artifacts: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE `+s.t("tasks")+` SET artifacts=$2::jsonb, updated_at=now() WHERE task_id=$1`,
		taskID, string(raw))
	if err != nil {
		return fmt.Errorf("gateway: set artifacts: %w", err)
	}
	return nil
}

// RequestCancel flags a task for cooperative cancellation and returns it.
func (s *Store) RequestCancel(ctx context.Context, scope contracts.Scope, taskID string) (TaskRow, error) {
	q := `UPDATE ` + s.t("tasks") + ` SET cancel_requested=true, updated_at=now()
		WHERE task_id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4
		RETURNING ` + taskColumns
	row, err := scanTask(s.pool.QueryRow(ctx, q, taskID, scope.Tenant, scope.Deployment, scope.Subject))
	if errors.Is(err, pgx.ErrNoRows) {
		return TaskRow{}, a2a.ErrTaskNotFound
	}
	if err != nil {
		return TaskRow{}, fmt.Errorf("gateway: request cancel: %w", err)
	}
	return row, nil
}

// ClaimTask takes the lease of one runnable task, bumping its fencing token.
// It returns nil when there is nothing to do.
func (s *Store) ClaimTask(ctx context.Context, worker string, lease time.Duration) (*TaskRow, error) {
	if worker == "" {
		return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonWorkerMissing, "gateway: worker id required")
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("gateway: claim begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	q := `SELECT ` + taskColumns + ` FROM ` + s.t("tasks") + `
		WHERE state IN ('TASK_STATE_SUBMITTED', 'TASK_STATE_WORKING')
		  AND (lease_owner = '' OR lease_expires_at IS NULL OR lease_expires_at < now())
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 1`
	row, err := scanTask(tx.QueryRow(ctx, q))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("gateway: claim select: %w", err)
	}
	// The claim IS the WORKING transition: a task the plane has accepted is
	// SUBMITTED, and the moment a worker fences it it is WORKING -- the state
	// the SSE/subscription stream and clients observe between acceptance and a
	// terminal outcome. The status version advances exactly once here so the
	// journalled sequence is SUBMITTED(1) -> WORKING(2) -> terminal(3); the
	// claimed row carries the new version, which every downstream fenced write
	// (commitResult / fail / settle) uses as its optimistic-concurrency guard.
	// The claim predicate also re-claims an expired-lease WORKING orphan, so
	// crash recovery is unchanged: the effect ledger -- not the task state --
	// is what makes a re-dispatch of an in-flight attempt conflict instead of
	// duplicate a side effect.
	u := `UPDATE ` + s.t("tasks") + `
		SET state='TASK_STATE_WORKING', lease_owner=$2, lease_expires_at=now()+$3::interval,
		    attempts=attempts+1, fencing_token=fencing_token+1,
		    status_version=status_version+1, updated_at=now()
		WHERE task_id=$1
		RETURNING ` + taskColumns
	claimed, err := scanTask(tx.QueryRow(ctx, u, row.ID, worker, fmt.Sprintf("%f seconds", lease.Seconds())))
	if err != nil {
		return nil, fmt.Errorf("gateway: claim lease: %w", err)
	}
	if err := s.AppendEvent(ctx, tx, Event{
		TaskID: claimed.ID, Version: claimed.StatusVersion, Scope: claimed.Scope, Kind: "status",
		Payload: statusEvent(claimed, a2a.TaskStateWorking, nil),
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("gateway: claim commit: %w", err)
	}
	return &claimed, nil
}

// RenewLease extends the lease; a lost lease is reported as LEASE_LOST.
func (s *Store) RenewLease(ctx context.Context, taskID, worker string, lease time.Duration) error {
	q := `UPDATE ` + s.t("tasks") + ` SET lease_expires_at=now()+$3::interval, updated_at=now()
		WHERE task_id=$1 AND lease_owner=$2
		  AND (lease_expires_at IS NULL OR lease_expires_at > now())`
	tag, err := s.pool.Exec(ctx, q, taskID, worker, fmt.Sprintf("%f seconds", lease.Seconds()))
	if err != nil {
		return fmt.Errorf("gateway: renew lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return contracts.NewError(contracts.ErrLeaseLost, "gateway: lease no longer held")
	}
	return nil
}

// CancelHeld marks a leased task CANCELED, but only for the lease holder, so a
// superseded attempt can never write a terminal state. Unlike advanceHeld it
// does not require cancel_requested=false: a cooperative cancel is exactly the
// path where the flag is already set, so the lease+status-version fence alone
// decides the winner.
func (s *Store) CancelHeld(ctx context.Context, tx pgx.Tx, scope contracts.Scope, taskID, worker string, expected int64) (TaskRow, error) {
	return s.advanceHeld(ctx, tx, scope, taskID, worker, expected, a2a.TaskStateCanceled, nil, nil, false)
}

// advanceHeld transitions a held task to a new state, guarded by the lease
// owner and the expected status version. When requireNotCancelled is true a
// concurrent cancellation request wins and this writer loses the lease; cancel
// itself passes false because it is the cancellation. Artifacts are merged with
// COALESCE so a status-only transition never drops already-stored artifacts, and
// terminal_at is stamped only for terminal states.
func (s *Store) advanceHeld(ctx context.Context, tx pgx.Tx, scope contracts.Scope, taskID, worker string, expected int64, state a2a.TaskState, msg *a2a.Message, artifacts []*a2a.Artifact, requireNotCancelled bool) (TaskRow, error) {
	var statusArg any
	if msg != nil {
		raw, err := json.Marshal(msg)
		if err != nil {
			return TaskRow{}, fmt.Errorf("gateway: encode status message: %w", err)
		}
		statusArg = string(raw)
	}
	var artArg any
	if len(artifacts) > 0 {
		raw, err := json.Marshal(artifacts)
		if err != nil {
			return TaskRow{}, fmt.Errorf("gateway: encode artifacts: %w", err)
		}
		artArg = string(raw)
	}
	terminalExpr := "terminal_at"
	if state.Terminal() {
		terminalExpr = "now()"
	}
	guard := ""
	if requireNotCancelled {
		guard = " AND cancel_requested=false"
	}
	q := `UPDATE ` + s.t("tasks") + `
		SET state=$6, status_message=COALESCE($7::jsonb, status_message),
		    artifacts=COALESCE($9::jsonb, artifacts),
		    status_version=status_version+1, updated_at=now(),
		    lease_owner='', lease_expires_at=NULL, terminal_at=` + terminalExpr + `
		WHERE task_id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4
		  AND lease_owner=$5 AND status_version=$8` + guard + `
		RETURNING ` + taskColumns
	row, err := scanTask(tx.QueryRow(ctx, q, taskID, scope.Tenant, scope.Deployment, scope.Subject,
		worker, string(state), statusArg, expected, artArg))
	if errors.Is(err, pgx.ErrNoRows) {
		return TaskRow{}, contracts.NewError(contracts.ErrLeaseLost,
			"gateway: lease or status version lost")
	}
	if err != nil {
		return TaskRow{}, fmt.Errorf("gateway: advance held task: %w", err)
	}
	return row, nil
}

// CompleteHeld finalises a leased task with the executor's outcome. A terminal
// state stamps terminal_at; an intermediate state (input/auth required) does not
// and is abandoned if a cancel arrived concurrently.
func (s *Store) CompleteHeld(ctx context.Context, tx pgx.Tx, scope contracts.Scope, taskID, worker string, expected int64, state a2a.TaskState, msg *a2a.Message, artifacts []*a2a.Artifact) (TaskRow, error) {
	return s.advanceHeld(ctx, tx, scope, taskID, worker, expected, state, msg, artifacts, true)
}

// ReleaseLease drops a lease so another worker can retry the task.
func (s *Store) ReleaseLease(ctx context.Context, taskID, worker string) error {
	_, err := s.pool.Exec(ctx, `UPDATE `+s.t("tasks")+`
		SET lease_owner='', lease_expires_at=NULL, updated_at=now()
		WHERE task_id=$1 AND lease_owner=$2`, taskID, worker)
	if err != nil {
		return fmt.Errorf("gateway: release lease: %w", err)
	}
	return nil
}

// MergeMetadata adds (or overwrites) keys on a task's metadata object inside the
// caller's transaction. Facts the executor reports about an attempt -- steps
// consumed, the budget that bounded it, why the loop closed -- belong on the
// task, because that is the object every consumer already reads; the alternative
// of a second event version would corrupt the status-version sequence watchers
// rely on. A missing metadata column value is treated as an empty object.
//
// "Missing" has THREE shapes and all of them must be handled here, because the
// alternative is data loss: a task created without metadata stores the result of
// json.Marshal(nil map), which is the jsonb scalar `null` -- not SQL NULL. SQL
// COALESCE does not catch a jsonb `null`, and PostgreSQL concatenates a scalar
// with an object by BUILDING AN ARRAY ([null, {...}]). That array is then
// unmarshalable into map[string]any, so every later read of the task -- GetTask,
// CancelTask, the stream -- fails with a gateway internal error and the completed
// task becomes permanently unreadable. Measured on candidate 913d0287a:
// "gateway: load task: gateway: decode metadata: json: cannot unmarshal array
// into Go value of type map[string]interface {}". The merge is therefore written
// as a total function over the object domain.
func (s *Store) MergeMetadataTx(ctx context.Context, tx pgx.Tx, taskID string, patch map[string]any) error {
	if len(patch) == 0 {
		return nil
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("gateway: encode metadata patch: %w", err)
	}
	_, err = tx.Exec(ctx, `UPDATE `+s.t("tasks")+`
		SET metadata = (CASE
			WHEN metadata IS NULL OR jsonb_typeof(metadata) <> 'object' THEN '{}'::jsonb
			ELSE metadata END) || $2::jsonb, updated_at = now()
		WHERE task_id = $1`, taskID, string(raw))
	if err != nil {
		return fmt.Errorf("gateway: merge metadata: %w", err)
	}
	return nil
}

// AppendEvent journals a transition; version must be strictly new for the task.
func (s *Store) AppendEvent(ctx context.Context, tx pgx.Tx, ev Event) error {
	raw, err := json.Marshal(ev.Payload)
	if err != nil {
		return fmt.Errorf("gateway: encode event: %w", err)
	}
	raw, err = injectEventKind(raw, ev.Kind)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO `+s.t("task_events")+`
		(task_id, version, tenant, deployment, subject, kind, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb)`,
		ev.TaskID, ev.Version, ev.Scope.Tenant, ev.Scope.Deployment, ev.Scope.Subject, ev.Kind, string(raw))
	if err != nil {
		return fmt.Errorf("gateway: append event: %w", err)
	}
	return nil
}

// Events replays the journalled transitions of a task from fromVersion.
func (s *Store) Events(ctx context.Context, scope contracts.Scope, taskID string, fromVersion int64) ([]Event, error) {
	q := `SELECT version, kind, payload, created_at FROM ` + s.t("task_events") + `
		WHERE task_id=$1 AND tenant=$2 AND deployment=$3 AND subject=$4 AND version>$5
		ORDER BY version`
	rows, err := s.pool.Query(ctx, q, taskID, scope.Tenant, scope.Deployment, scope.Subject, fromVersion)
	if err != nil {
		return nil, fmt.Errorf("gateway: events: %w", err)
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var (
			ev   Event
			raw  []byte
			kind string
		)
		if err := rows.Scan(&ev.Version, &kind, &raw, &ev.Created); err != nil {
			return nil, fmt.Errorf("gateway: events scan: %w", err)
		}
		ev.TaskID, ev.Kind = taskID, kind
		ev.Scope = scope
		parsed, err := decodeEvent(raw)
		if err != nil {
			return nil, err
		}
		ev.Payload = parsed
		out = append(out, ev)
	}
	return out, rows.Err()
}

// EnqueueOutbox records a delivery obligation inside tx. Re-enqueueing the same
// op key is a no-op, which is what makes producer retries safe.
func (s *Store) EnqueueOutbox(ctx context.Context, tx pgx.Tx, e OutboxEntry) error {
	if e.OpKey == "" || e.Kind == "" {
		return contracts.NewReasoned(contracts.ErrInvalid, ReasonOutboxKeyPartial, "gateway: outbox needs op key and kind")
	}
	raw := e.Payload
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	_, err := tx.Exec(ctx, `INSERT INTO `+s.t("outbox")+`
		(tenant, deployment, subject, task_id, parent_task, kind, status_version, op_key, fencing_token, payload)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
		ON CONFLICT (tenant, deployment, subject, op_key) DO NOTHING`,
		e.Scope.Tenant, e.Scope.Deployment, e.Scope.Subject, e.TaskID, e.ParentTask, e.Kind,
		e.StatusVer, e.OpKey, e.FencingToken, string(raw))
	if err != nil {
		return fmt.Errorf("gateway: enqueue outbox: %w", err)
	}
	return nil
}

// ClaimOutbox takes up to n pending entries for delivery.
func (s *Store) ClaimOutbox(ctx context.Context, worker string, n int) ([]OutboxEntry, error) {
	if n <= 0 {
		n = 16
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("gateway: outbox claim begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT seq, tenant, deployment, subject, task_id, parent_task, kind,
		status_version, op_key, fencing_token, payload, attempts FROM `+s.t("outbox")+`
		WHERE state = 'PENDING' AND next_at <= now()
		ORDER BY seq
		FOR UPDATE SKIP LOCKED
		LIMIT $1`, n)
	if err != nil {
		return nil, fmt.Errorf("gateway: outbox claim: %w", err)
	}
	var out []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		var raw []byte
		if err := rows.Scan(&e.Seq, &e.Scope.Tenant, &e.Scope.Deployment, &e.Scope.Subject, &e.TaskID,
			&e.ParentTask, &e.Kind, &e.StatusVer, &e.OpKey, &e.FencingToken, &raw, &e.Attempts); err != nil {
			return nil, fmt.Errorf("gateway: outbox scan: %w", err)
		}
		e.Payload = append(json.RawMessage(nil), raw...)
		e.State = OutboxInFlight
		out = append(out, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, tx.Commit(ctx)
	}
	ids := make([]int64, len(out))
	for i, e := range out {
		ids[i] = e.Seq
	}
	if _, err := tx.Exec(ctx, `UPDATE `+s.t("outbox")+`
		SET state='IN_FLIGHT', attempts=attempts+1, updated_at=now() WHERE seq = ANY($1)`, ids); err != nil {
		return nil, fmt.Errorf("gateway: outbox mark: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("gateway: outbox claim commit: %w", err)
	}
	return out, nil
}

// CompleteOutbox settles a delivery with its final state.
func (s *Store) CompleteOutbox(ctx context.Context, seq int64, state, reason string) error {
	if state != OutboxPending && state != OutboxDone && state != OutboxFailed &&
		state != OutboxUnknown && state != OutboxUnavailable {
		return contracts.NewReasoned(contracts.ErrInvalid, ReasonOutboxStateInvalid, "gateway: bad outbox state")
	}
	_, err := s.pool.Exec(ctx, `UPDATE `+s.t("outbox")+`
		SET state=$2, last_error=$3, updated_at=now(),
		    next_at = CASE WHEN $2 = 'PENDING' THEN now() + interval '5 seconds' ELSE next_at END
		WHERE seq=$1`, seq, state, truncate(reason, 512))
	if err != nil {
		return fmt.Errorf("gateway: complete outbox: %w", err)
	}
	return nil
}

// OutboxState reads one entry, mainly for tests and reconciliation.
func (s *Store) OutboxState(ctx context.Context, seq int64) (string, int, error) {
	var state string
	var attempts int
	err := s.pool.QueryRow(ctx, `SELECT state, attempts FROM `+s.t("outbox")+` WHERE seq=$1`, seq).Scan(&state, &attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", 0, a2a.ErrTaskNotFound
	}
	if err != nil {
		return "", 0, fmt.Errorf("gateway: outbox state: %w", err)
	}
	return state, attempts, nil
}

// PutPushConfig stores or replaces a push configuration.
func (s *Store) PutPushConfig(ctx context.Context, cfg *a2a.PushConfig, scope contracts.Scope) (*a2a.PushConfig, error) {
	return s.putPushConfig(ctx, nil, cfg, scope)
}

// PutPushConfigTx is PutPushConfig bound to an open transaction; tx may be nil
// for the pool.
func (s *Store) PutPushConfigTx(ctx context.Context, tx pgx.Tx, cfg *a2a.PushConfig, scope contracts.Scope) (*a2a.PushConfig, error) {
	return s.putPushConfig(ctx, tx, cfg, scope)
}

func (s *Store) putPushConfig(ctx context.Context, tx pgx.Tx, cfg *a2a.PushConfig, scope contracts.Scope) (*a2a.PushConfig, error) {
	if cfg == nil || cfg.TaskID == "" || cfg.URL == "" {
		return nil, contracts.NewReasoned(contracts.ErrInvalid, ReasonPushConfigIncomplete, "gateway: push config needs task and url")
	}
	id := cfg.ID
	if id == "" {
		id = "default"
	}
	var authArg any
	if cfg.Auth != nil {
		raw, err := json.Marshal(cfg.Auth)
		if err != nil {
			return nil, fmt.Errorf("gateway: encode push auth: %w", err)
		}
		authArg = string(raw)
	}
	var q queryable = s.pool
	if tx != nil {
		q = tx
	}
	_, err := q.Exec(ctx, `INSERT INTO `+s.t("push_configs")+`
		(tenant, deployment, subject, task_id, id, url, token, auth)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
		ON CONFLICT (tenant, deployment, subject, task_id, id)
		DO UPDATE SET url=EXCLUDED.url, token=EXCLUDED.token, auth=EXCLUDED.auth`,
		scope.Tenant, scope.Deployment, scope.Subject, string(cfg.TaskID), id, cfg.URL, cfg.Token, authArg)
	if err != nil {
		return nil, fmt.Errorf("gateway: put push config: %w", err)
	}
	cfg.TaskID = a2a.TaskID(string(cfg.TaskID))
	cfg.ID = id
	return cfg, nil
}

// PushConfigs lists the push configurations of a task.
func (s *Store) PushConfigs(ctx context.Context, scope contracts.Scope, taskID string) ([]*a2a.PushConfig, error) {
	return s.pushConfigs(ctx, nil, scope, taskID)
}

func (s *Store) pushConfigs(ctx context.Context, tx pgx.Tx, scope contracts.Scope, taskID string) ([]*a2a.PushConfig, error) {
	var q queryable = s.pool
	if tx != nil {
		q = tx
	}
	rows, err := q.Query(ctx, `SELECT id, url, token, auth FROM `+s.t("push_configs")+`
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task_id=$4
		ORDER BY id`, scope.Tenant, scope.Deployment, scope.Subject, taskID)
	if err != nil {
		return nil, fmt.Errorf("gateway: push configs: %w", err)
	}
	defer rows.Close()
	var out []*a2a.PushConfig
	for rows.Next() {
		var (
			id, url, token string
			auth           []byte
		)
		if err := rows.Scan(&id, &url, &token, &auth); err != nil {
			return nil, fmt.Errorf("gateway: push configs scan: %w", err)
		}
		cfg := &a2a.PushConfig{TaskID: a2a.TaskID(taskID), ID: id, URL: url, Token: token}
		if len(auth) > 0 {
			var info a2a.PushAuthInfo
			if err := json.Unmarshal(auth, &info); err == nil {
				cfg.Auth = &info
			}
		}
		out = append(out, cfg)
	}
	return out, rows.Err()
}

// DeletePushConfig removes one push configuration.
func (s *Store) DeletePushConfig(ctx context.Context, scope contracts.Scope, taskID, id string) error {
	if id == "" {
		id = "default"
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM `+s.t("push_configs")+`
		WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task_id=$4 AND id=$5`,
		scope.Tenant, scope.Deployment, scope.Subject, taskID, id)
	if err != nil {
		return fmt.Errorf("gateway: delete push config: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return contracts.NewError(contracts.ErrNotFound, "gateway: push config not found")
	}
	return nil
}

func decodeEvent(raw []byte) (a2a.Event, error) {
	var probe struct {
		Kind string `json:"ahp_kind"`
	}
	_ = json.Unmarshal(raw, &probe)
	switch probe.Kind {
	case "artifact":
		var ev a2a.TaskArtifactUpdateEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, fmt.Errorf("gateway: decode artifact event: %w", err)
		}
		return &ev, nil
	case "message":
		var m a2a.Message
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("gateway: decode message event: %w", err)
		}
		return &m, nil
	default:
		var ev a2a.TaskStatusUpdateEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return nil, fmt.Errorf("gateway: decode status event: %w", err)
		}
		return &ev, nil
	}
}

// injectEventKind re-serialises an event body with an internal discriminator so
// decodeEvent can pick the right concrete a2a type on replay. The field is a
// gateway storage detail and is stripped again by the handler on the wire.
func injectEventKind(raw []byte, kind string) ([]byte, error) {
	if kind == "" {
		kind = "status"
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("gateway: event payload is not an object: %w", err)
	}
	kindRaw, err := json.Marshal(kind)
	if err != nil {
		return nil, err
	}
	obj["ahp_kind"] = kindRaw
	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("gateway: encode event discriminator: %w", err)
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
