package triggers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EffectKind is the ledger journal kind for a trigger fire side effect.
const EffectKind = "trigger.fire"

// schemaRe guards the interpolated schema identifier against injection into the
// DDL/DML below.
var schemaRe = regexp.MustCompile(`^ahp_[a-z][a-z0-9_]{0,40}$`)

// DelegationIssuer re-issues a task token from a stored delegation reference at
// fire time. The Trigger never holds the token itself (PDF: triggers store a
// delegation reference, Identity & Policy re-issues the token afresh).
type DelegationIssuer interface {
	IssueToken(ctx context.Context, scope contracts.Scope, delegationRef string, ttl time.Duration) (token string, expiresAt time.Time, err error)
}

// Dispatcher starts the actual background work for a fire, returning the new
// task id (if any) and an arbitrary detail payload.
type Dispatcher interface {
	Fire(ctx context.Context, scope contracts.Scope, req FireRequest) (FireResult, error)
}

// SecretResolver resolves a webhook signing-secret reference to its bytes via
// the secret store; the raw secret is never stored on the Trigger row.
type SecretResolver interface {
	Resolve(ctx context.Context, scope contracts.Scope, secretRef string) ([]byte, error)
}

// TaskAuthority reports whether a parent task is still active, used to gate
// non-delegated triggers so they cannot outlive their parent.
type TaskAuthority interface {
	IsTaskActive(ctx context.Context, scope contracts.Scope, taskID string) (bool, error)
}

// FireRequest is what the Dispatcher receives for a single fire. Token is the
// freshly re-issued bearer token (never persisted) and is excluded from JSON.
type FireRequest struct {
	TriggerID string          `json:"trigger_id"`
	Sequence  int64           `json:"sequence"`
	Token     string          `json:"-"`
	ExpiresAt time.Time       `json:"expires_at"`
	Target    string          `json:"target"`
	Kind      string          `json:"kind"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Delegated bool            `json:"delegated"`
}

// FireResult is what the Dispatcher returns after starting work.
type FireResult struct {
	TaskID string          `json:"task_id,omitempty"`
	Detail json.RawMessage `json:"detail,omitempty"`
}

// Config wires a Service to its collaborators. Issuer, Dispatcher, Secrets and
// Tasks are all required.
type Config struct {
	Issuer     DelegationIssuer
	Dispatcher Dispatcher
	Secrets    SecretResolver
	Tasks      TaskAuthority
	FireTTL    time.Duration
	Clock      func() time.Time
}

// Service is the execution-plane Triggers subsystem.
type Service struct {
	pool   *pgxpool.Pool
	schema string
	ledger *operations.Store
	cfg    Config
}

// New validates configuration, ensures the effect ledger and triggers table
// exist, and returns a ready Service.
func New(ctx context.Context, pool *pgxpool.Pool, schema string, cfg Config) (*Service, error) {
	if !schemaRe.MatchString(schema) {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: schema name must match ^ahp_[a-z][a-z0-9_]{0,40}$")
	}
	if cfg.Issuer == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: DelegationIssuer is required")
	}
	if cfg.Dispatcher == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: Dispatcher is required")
	}
	if cfg.Secrets == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: SecretResolver is required")
	}
	if cfg.Tasks == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "triggers: TaskAuthority is required")
	}
	if cfg.FireTTL <= 0 {
		cfg.FireTTL = 5 * time.Minute
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	ledger, err := operations.New(pool, schema)
	if err != nil {
		return nil, err
	}
	if err := ledger.Migrate(ctx); err != nil {
		return nil, fmt.Errorf("triggers: migrate ledger: %w", err)
	}
	s := &Service{pool: pool, schema: schema, ledger: ledger, cfg: cfg}
	for _, q := range triggersDDL(schema) {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return nil, fmt.Errorf("triggers: apply ddl: %w", err)
		}
	}
	if err := s.tenantGuards(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

// isTerminal reports whether an operation status seals the effect (UNKNOWN is
// deliberately NOT terminal: it requires explicit reconciliation).
func isTerminal(status string) bool {
	switch status {
	case operations.StatusSucceeded, operations.StatusFailed, operations.StatusFenced:
		return true
	default:
		return false
	}
}

// CreateScheduleSpec describes a new bounded periodic trigger.
type CreateScheduleSpec struct {
	ParentTask    string          `json:"parent_task"`
	DelegationRef string          `json:"delegation_ref"`
	Target        string          `json:"target"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Interval      time.Duration   `json:"interval"`
	MaxFires      int64           `json:"max_fires"`
	Delegated     bool            `json:"delegated"`
	FirstFireAt   *time.Time      `json:"first_fire_at,omitempty"`
}

// CreateSchedule registers a bounded schedule. Proactive behaviour must be
// bounded: MaxFires is required and must be positive (PDF).
func (s *Service) CreateSchedule(ctx context.Context, scope contracts.Scope, spec CreateScheduleSpec) (Trigger, error) {
	if err := scope.Validate(); err != nil {
		return Trigger{}, err
	}
	if spec.ParentTask == "" || spec.DelegationRef == "" || spec.Target == "" {
		return Trigger{}, contracts.NewError(contracts.ErrInvalid, "triggers: parent_task, delegation_ref and target are required")
	}
	if spec.Interval <= 0 {
		return Trigger{}, contracts.NewError(contracts.ErrInvalid, "triggers: interval must be positive")
	}
	if spec.MaxFires <= 0 {
		return Trigger{}, contracts.NewError(contracts.ErrInvalid, "triggers: max_fires must be positive (proactive work must be bounded)")
	}
	next := s.cfg.Clock()
	if spec.FirstFireAt != nil {
		next = *spec.FirstFireAt
	}
	intervalMS := spec.Interval.Milliseconds()
	maxFires := spec.MaxFires
	t := Trigger{
		TriggerID:     contracts.NewID(),
		Tenant:        scope.Tenant,
		Deployment:    scope.Deployment,
		Subject:       scope.Subject,
		Kind:          KindSchedule,
		ParentTask:    spec.ParentTask,
		DelegationRef: spec.DelegationRef,
		Delegated:     spec.Delegated,
		Target:        spec.Target,
		Payload:       rawOrEmpty(spec.Payload),
		Status:        StatusActive,
		IntervalMS:    &intervalMS,
		NextFireAt:    &next,
		MaxFires:      &maxFires,
	}
	if err := insertTrigger(ctx, s.pool, s.schema, t); err != nil {
		return Trigger{}, err
	}
	return s.Get(ctx, scope, t.TriggerID)
}

// CreateWebhookSpec describes a new webhook trigger.
type CreateWebhookSpec struct {
	ParentTask    string          `json:"parent_task"`
	DelegationRef string          `json:"delegation_ref"`
	Target        string          `json:"target"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	SecretRef     string          `json:"secret_ref"`
	MaxFires      int64           `json:"max_fires"`
	Delegated     bool            `json:"delegated"`
}

// CreateWebhook registers a webhook trigger bound to a secret reference.
func (s *Service) CreateWebhook(ctx context.Context, scope contracts.Scope, spec CreateWebhookSpec) (Trigger, error) {
	if err := scope.Validate(); err != nil {
		return Trigger{}, err
	}
	if spec.ParentTask == "" || spec.DelegationRef == "" || spec.Target == "" {
		return Trigger{}, contracts.NewError(contracts.ErrInvalid, "triggers: parent_task, delegation_ref and target are required")
	}
	if spec.SecretRef == "" {
		return Trigger{}, contracts.NewError(contracts.ErrInvalid, "triggers: secret_ref is required for a webhook")
	}
	t := Trigger{
		TriggerID:     contracts.NewID(),
		Tenant:        scope.Tenant,
		Deployment:    scope.Deployment,
		Subject:       scope.Subject,
		Kind:          KindWebhook,
		ParentTask:    spec.ParentTask,
		DelegationRef: spec.DelegationRef,
		Delegated:     spec.Delegated,
		Target:        spec.Target,
		Payload:       rawOrEmpty(spec.Payload),
		Status:        StatusActive,
		SecretRef:     spec.SecretRef,
	}
	// Webhooks are reactive, not bounded proactive work. MaxFires is optional:
	// 0 means unbounded (nil pointer); a positive value caps the fire budget.
	if spec.MaxFires > 0 {
		mf := spec.MaxFires
		t.MaxFires = &mf
	}
	if err := insertTrigger(ctx, s.pool, s.schema, t); err != nil {
		return Trigger{}, err
	}
	return s.Get(ctx, scope, t.TriggerID)
}

// Get fetches a trigger within the caller's tenant/deployment/subject, enforcing scope
// isolation (a foreign trigger is NOT_FOUND, not FORBIDDEN).
func (s *Service) Get(ctx context.Context, scope contracts.Scope, id string) (Trigger, error) {
	if err := scope.Validate(); err != nil {
		return Trigger{}, err
	}
	q := fmt.Sprintf("SELECT %s FROM %s.triggers WHERE trigger_id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4", triggerSelect, quoteIdent(s.schema))
	row := s.pool.QueryRow(ctx, q, id, scope.Tenant, scope.Deployment, scope.Subject)
	t, err := scanTrigger(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Trigger{}, contracts.NewError(contracts.ErrNotFound, "triggers: trigger not found")
	}
	if err != nil {
		return Trigger{}, fmt.Errorf("triggers: get: %w", err)
	}
	return t, nil
}

// List returns only triggers owned by the caller's complete scope.
func (s *Service) List(ctx context.Context, scope contracts.Scope) ([]Trigger, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	q := fmt.Sprintf("SELECT %s FROM %s.triggers WHERE tenant = $1 AND deployment = $2 AND subject = $3 ORDER BY created_at, trigger_id", triggerSelect, quoteIdent(s.schema))
	rows, err := s.pool.Query(ctx, q, scope.Tenant, scope.Deployment, scope.Subject)
	if err != nil {
		return nil, fmt.Errorf("triggers: list: %w", err)
	}
	defer rows.Close()
	out := []Trigger{}
	for rows.Next() {
		t, err := scanTrigger(rows)
		if err != nil {
			return nil, fmt.Errorf("triggers: list scan: %w", err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Revoke transitions a trigger to REVOKED and clears its next fire. It is
// idempotent: revoking an already-revoked trigger succeeds without further
// effect.
func (s *Service) Revoke(ctx context.Context, scope contracts.Scope, id string) (Trigger, error) {
	t, err := s.Get(ctx, scope, id)
	if err != nil {
		return Trigger{}, err
	}
	if t.Status == StatusRevoked {
		return t, nil
	}
	q := fmt.Sprintf(`UPDATE %s.triggers
		SET status = 'REVOKED', next_fire_at = NULL, version = version + 1, updated_at = now()
		WHERE trigger_id = $1 AND tenant = $2 AND deployment = $3 AND subject = $4 AND status <> 'REVOKED'`,
		quoteIdent(s.schema))
	_, err = s.pool.Exec(ctx, q, id, scope.Tenant, scope.Deployment, scope.Subject)
	if err != nil {
		return Trigger{}, fmt.Errorf("triggers: revoke: %w", err)
	}
	// Revocation is monotonic. A concurrent admission or scheduler advance
	// must not turn a successful revoke into an ACTIVE receipt via a stale CAS.
	return s.Get(ctx, scope, id)
}

// ProcessDue evaluates SCHEDULE triggers whose next fire time has elapsed and
// attempts to fire each exactly once. It returns the number of triggers that
// actually dispatched new work. Idempotency is enforced by the effect-ledger
// key (trigger id + fire sequence), so re-running with the same clock value
// cannot double-fire.
func (s *Service) ProcessDue(ctx context.Context, now time.Time, limit int) (int, error) {
	return s.processDue(ctx, now, limit, nil)
}

// ProcessDueScoped is the task-facing nudge. Only the background scheduler
// calls ProcessDue across subjects; a user's request is bounded by its scope.
func (s *Service) ProcessDueScoped(ctx context.Context, scope contracts.Scope, now time.Time, limit int) (int, error) {
	if err := scope.Validate(); err != nil {
		return 0, err
	}
	return s.processDue(ctx, now, limit, &scope)
}

func (s *Service) processDue(ctx context.Context, now time.Time, limit int, scope *contracts.Scope) (int, error) {
	if limit <= 0 {
		limit = 50
	}
	filter := ""
	args := []any{now, limit}
	if scope != nil {
		filter = " AND tenant = $3 AND deployment = $4 AND subject = $5"
		args = append(args, scope.Tenant, scope.Deployment, scope.Subject)
	}
	q := fmt.Sprintf(`SELECT %s FROM %s.triggers
		WHERE status = 'ACTIVE' AND kind = 'SCHEDULE' AND next_fire_at IS NOT NULL
			AND next_fire_at <= $1 AND (max_fires IS NULL OR fired_count < max_fires)%s
		ORDER BY next_fire_at, trigger_id
		LIMIT $2`, triggerSelect, quoteIdent(s.schema), filter)
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return 0, fmt.Errorf("triggers: process due query: %w", err)
	}
	var due []Trigger
	for rows.Next() {
		t, serr := scanTrigger(rows)
		if serr != nil {
			rows.Close()
			return 0, fmt.Errorf("triggers: process due scan: %w", serr)
		}
		due = append(due, t)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("triggers: process due rows: %w", err)
	}
	fired := 0
	for _, t := range due {
		ok, ferr := s.fireOne(ctx, t)
		if ferr != nil {
			return fired, ferr
		}
		if ok {
			fired++
		}
	}
	return fired, nil
}

// fireOne runs the durable pipeline for a single scheduled fire: parent-liveness
// gate, ledger accept/dispatch, dispatch to the Dispatcher, then advance.
func (s *Service) fireOne(ctx context.Context, trig Trigger) (bool, error) {
	scope := trig.scope()
	if !trig.Delegated {
		active, err := s.cfg.Tasks.IsTaskActive(ctx, scope, trig.ParentTask)
		if err != nil {
			return false, fmt.Errorf("triggers: parent liveness: %w", err)
		}
		if !active {
			// A non-delegated trigger must not outlive its parent: revoke it.
			q := fmt.Sprintf(`UPDATE %s.triggers
				SET status = 'REVOKED', next_fire_at = NULL, version = version + 1, updated_at = now()
				WHERE trigger_id = $1 AND tenant = $2 AND deployment = $3 AND fired_count = $4`,
				quoteIdent(s.schema))
			if _, err := s.pool.Exec(ctx, q, trig.TriggerID, scope.Tenant, scope.Deployment, trig.FiredCount); err != nil {
				return false, fmt.Errorf("triggers: revoke dead parent: %w", err)
			}
			return false, nil
		}
	}
	seq := trig.FiredCount + 1
	key := fmt.Sprintf("trigger:%s:fire:%d", trig.TriggerID, seq)
	args, err := json.Marshal(map[string]any{
		"sequence":  seq,
		"target":    trig.Target,
		"delegated": trig.Delegated,
		"payload":   rawOrEmpty(trig.Payload),
	})
	if err != nil {
		return false, fmt.Errorf("triggers: marshal args: %w", err)
	}
	op, err := s.ledger.Accept(ctx, scope, operations.Request{Key: key, Kind: EffectKind, Target: trig.Target, Arguments: args})
	if err != nil {
		if contracts.Code(err) == contracts.ErrConflict {
			g, gerr := s.ledger.Get(ctx, scope, key)
			if gerr != nil {
				return false, gerr
			}
			if isTerminal(g.Status) {
				if aerr := s.advance(ctx, trig, seq); aerr != nil {
					return false, aerr
				}
			}
			return false, nil
		}
		return false, err
	}
	if op.Status != operations.StatusAccepted {
		// A prior attempt is in flight (DISPATCHED/UNKNOWN) or sealed. Never
		// re-dispatch. Advance only when the prior attempt is terminal.
		if isTerminal(op.Status) {
			if aerr := s.advance(ctx, trig, seq); aerr != nil {
				return false, aerr
			}
		}
		return false, nil
	}
	if _, derr := s.ledger.Dispatch(ctx, scope, key); derr != nil {
		if errors.Is(derr, operations.ErrAlreadyDispatched) {
			return false, nil
		}
		return false, derr
	}
	return s.dispatch(ctx, scope, trig, seq, key)
}

// dispatch re-issues a fresh token from the delegation reference, fires the work
// and journals the outcome. An ambiguous fire is sealed as UNKNOWN and never
// auto-replayed.
func (s *Service) dispatch(ctx context.Context, scope contracts.Scope, trig Trigger, seq int64, key string) (bool, error) {
	token, exp, err := s.cfg.Issuer.IssueToken(ctx, scope, trig.DelegationRef, s.cfg.FireTTL)
	if err != nil {
		// An authority verdict (revoked delegation, dismissed subject) stops the
		// trigger instead of retrying it forever; a transient issuer failure does
		// not (PDF rev 3.2, c. 48 and c. 54).
		stopped := s.stopIfDelegationGone(ctx, trig, err)
		result := json.RawMessage(`{"error":"issue_token"}`)
		if stopped {
			result = json.RawMessage(`{"error":"issue_token","trigger":"REVOKED","reason":"delegation_revoked"}`)
		}
		if _, cerr := s.ledger.Complete(ctx, scope, key, result, true); cerr != nil {
			return false, fmt.Errorf("triggers: issue token failed and complete failed: %v / %w", err, cerr)
		}
		if stopped {
			return false, nil
		}
		return false, fmt.Errorf("triggers: issue token: %w", err)
	}
	res, ferr := s.cfg.Dispatcher.Fire(ctx, scope, FireRequest{
		TriggerID: trig.TriggerID,
		Sequence:  seq,
		Token:     token,
		ExpiresAt: exp,
		Target:    trig.Target,
		Kind:      trig.Kind,
		Payload:   rawOrEmpty(trig.Payload),
		Delegated: trig.Delegated,
	})
	if ferr != nil {
		// A provable pre-effect refusal is sealed FAILED and the schedule moves
		// on; anything ambiguous is SIDE_EFFECT_UNKNOWN and is never replayed
		// automatically (PDF rev 3.2, c. 13 and c. 54).
		if errors.Is(ferr, ErrDispatchRefused) {
			refused, _ := json.Marshal(map[string]any{"error": "dispatch_refused"})
			if _, cerr := s.ledger.Complete(ctx, scope, key, refused, true); cerr != nil {
				return false, fmt.Errorf("triggers: refused and complete failed: %v / %w", ferr, cerr)
			}
			if aerr := s.advance(ctx, trig, seq); aerr != nil {
				return false, aerr
			}
			return false, nil
		}
		if _, merr := s.ledger.MarkUnknown(ctx, scope, key); merr != nil {
			return false, fmt.Errorf("triggers: fire unknown and mark failed: %v / %w", ferr, merr)
		}
		return false, fmt.Errorf("triggers: fire outcome unknown: %w", ferr)
	}
	payload, merr := json.Marshal(res)
	if merr != nil {
		payload = json.RawMessage(`{"marshal_error":true}`)
	}
	if _, cerr := s.ledger.Complete(ctx, scope, key, payload, false); cerr != nil {
		return false, fmt.Errorf("triggers: complete: %w", cerr)
	}
	if aerr := s.advance(ctx, trig, seq); aerr != nil {
		return false, aerr
	}
	return true, nil
}

// advance rolls a trigger's fired_count forward (compare-and-set on the prior
// count), computes the next fire time, and marks the trigger exhausted once its
// bounded MaxFires budget is spent.
func (s *Service) advance(ctx context.Context, trig Trigger, seq int64) error {
	var next *time.Time
	status := StatusActive
	exhausted := trig.MaxFires != nil && seq >= *trig.MaxFires
	if exhausted {
		status = StatusExhausted
	} else if trig.IntervalMS != nil {
		base := s.cfg.Clock()
		if trig.NextFireAt != nil {
			base = *trig.NextFireAt
		}
		n := base.Add(time.Duration(*trig.IntervalMS) * time.Millisecond)
		next = &n
	}
	q := fmt.Sprintf(`UPDATE %s.triggers
		SET fired_count = $2, next_fire_at = $3, status = $4, version = version + 1, updated_at = now()
		WHERE trigger_id = $1 AND tenant = $5 AND deployment = $6 AND fired_count = $7 AND status='ACTIVE'`,
		quoteIdent(s.schema))
	if _, err := s.pool.Exec(ctx, q, trig.TriggerID, seq, next, status, trig.Tenant, trig.Deployment, seq-1); err != nil {
		return fmt.Errorf("triggers: advance: %w", err)
	}
	return nil
}
