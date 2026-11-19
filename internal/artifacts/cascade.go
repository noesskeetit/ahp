package artifacts

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The PDF makes deletion a cascade the control plane performs: "Удаление.
// Каскадом по контексту, субъекту и tenant. Каждая запись знает tenant, деплой,
// субъект и контекст - этого хватает для удаления, retention и удержания по
// юридическому требованию." Those four columns are the selection key this file
// implements, and the guarantee that a deleted thread stops serving its
// artifacts rests on them. A legal hold is part of the same obligation, so a
// cascade that meets one leaves those bytes exactly where they are and reports
// it, instead of either failing the deletion or deleting under the hold.

// CascadeResult is what one cascade deletion did. Held counts artifacts the
// cascade refused to tombstone because a legal hold pins them, Referenced counts
// the ones it refused to tombstone because something outside the artifact plane
// still names them (DEF-H24-15), and AlreadyDeleted makes a repeated cascade
// provably a no-op rather than a silence.
type CascadeResult struct {
	Tombstoned     int `json:"tombstoned"`
	Held           int `json:"held"`
	Referenced     int `json:"referenced"`
	AlreadyDeleted int `json:"already_deleted"`
}

// DeleteByContext tombstones every artifact of the caller's exact scope written
// under contextID: the thread cascade for artifacts that arrived with a context.
// Artifacts under legal hold are skipped and reported. Idempotent.
func (s *Store) DeleteByContext(ctx context.Context, scope contracts.Scope, contextID string) (CascadeResult, error) {
	return s.deleteByContext(ctx, scope, contextID, nil)
}

// RetireDeletedContext uses the deletion time of the owning State session as
// the start of its artifacts' retention. Only the retention collector calls
// this after the State payload was disconnected.
func (s *Store) RetireDeletedContext(ctx context.Context, scope contracts.Scope, contextID string, deletedAt time.Time) (CascadeResult, error) {
	if deletedAt.IsZero() || s.keep == nil {
		return CascadeResult{}, contracts.NewError(contracts.ErrUnavailable, "retiring a context requires its deletion time and reference owner")
	}
	return s.deleteByContext(ctx, scope, contextID, &deletedAt)
}

func (s *Store) deleteByContext(ctx context.Context, scope contracts.Scope, contextID string, deletedAt *time.Time) (CascadeResult, error) {
	if err := scope.Validate(); err != nil {
		return CascadeResult{}, err
	}
	if err := requireBindingValue("context id", contextID); err != nil {
		return CascadeResult{}, err
	}
	if err := s.authorize(ctx, scope, "delete", "artifacts:context:"+contextID); err != nil {
		return CascadeResult{}, err
	}
	b := newCascade(scope)
	b.reason = "cascade:context"
	b.deletedAt = deletedAt
	b.equal("context_id", contextID)
	return s.cascade(ctx, b)
}

// DeleteByTasks tombstones every artifact of the caller's exact scope that one
// of taskIDs produced. State is the authority on which tasks belong to a thread,
// so a thread cascade passes that set here. Unknown or foreign ids touch nothing.
func (s *Store) DeleteByTasks(ctx context.Context, scope contracts.Scope, taskIDs []string) (CascadeResult, error) {
	if err := scope.Validate(); err != nil {
		return CascadeResult{}, err
	}
	clean, err := validateBindingValues("task id", taskIDs)
	if err != nil {
		return CascadeResult{}, err
	}
	if len(clean) == 0 {
		return CascadeResult{}, contracts.NewError(contracts.ErrInvalid,
			"artifacts: at least one task id is required")
	}
	if err := s.authorize(ctx, scope, "delete", "artifacts:tasks"); err != nil {
		return CascadeResult{}, err
	}
	b := newCascade(scope)
	b.reason = "cascade:tasks"
	b.anyOf("task_id", clean)
	return s.cascade(ctx, b)
}

// DeleteByThread is the cascade one thread deletion performs: it tombstones
// every artifact of the caller's scope written under one of contextIDs or
// produced by one of taskIDs, in one statement inside one transaction.
//
// Each selector takes a set because a thread is named more than once in this
// platform: State knows a context id and a thread id (the PDF makes them equal
// for a chat, and a writer may have recorded either), and a run has both its A2A
// task id and its State run id. Matching all of them in one sweep is what covers
// every writer without any of them having to know the others - and doing it in
// one sweep is also what keeps the counters honest, where two sweeps would count
// the rows the first one already tombstoned a second time.
func (s *Store) DeleteByThread(ctx context.Context, scope contracts.Scope, contextIDs, taskIDs []string) (CascadeResult, error) {
	if err := scope.Validate(); err != nil {
		return CascadeResult{}, err
	}
	contexts, err := validateBindingValues("context id", contextIDs)
	if err != nil {
		return CascadeResult{}, err
	}
	tasks, err := validateBindingValues("task id", taskIDs)
	if err != nil {
		return CascadeResult{}, err
	}
	if len(contexts) == 0 && len(tasks) == 0 {
		return CascadeResult{}, nil
	}
	if err := s.authorize(ctx, scope, "delete", "artifacts:thread"); err != nil {
		return CascadeResult{}, err
	}
	b := newCascade(scope)
	b.reason = "cascade:thread"
	if len(contexts) > 0 {
		b.anyOf("context_id", contexts)
	}
	if len(tasks) > 0 {
		b.anyOf("task_id", tasks)
	}
	return s.cascade(ctx, b)
}

// DeleteBySubject tombstones every artifact of the caller's exact
// tenant/deployment/subject: the artifact half of State's DeleteSubject, so
// "удаление чата и сотрудника" reaches the artifact plane the same way and with
// the same legal-hold rule. No other scope is reachable, because the selector is
// the caller's own verified scope.
func (s *Store) DeleteBySubject(ctx context.Context, scope contracts.Scope) (CascadeResult, error) {
	if err := scope.Validate(); err != nil {
		return CascadeResult{}, err
	}
	if err := s.authorize(ctx, scope, "delete", "artifacts:subject"); err != nil {
		return CascadeResult{}, err
	}
	b := newCascade(scope)
	b.reason = "cascade:subject"
	return s.cascade(ctx, b)
}

// cascadeBuilder assembles the scope-strict matcher of one sweep. Every
// cascade starts from the caller's verified scope and only ever narrows it, so
// there is no way to express a cascade that reaches outside the caller.
type cascadeBuilder struct {
	args       []any
	parts      []string
	reason     string
	tenant     string
	deployment string
	subject    string
	deletedAt  *time.Time
}

func newCascade(scope contracts.Scope) *cascadeBuilder {
	return &cascadeBuilder{
		args:       []any{scope.Tenant, scope.Deployment, scope.Subject},
		tenant:     scope.Tenant,
		deployment: scope.Deployment,
		subject:    scope.Subject,
	}
}

func (b *cascadeBuilder) equal(column, value string) {
	b.parts = append(b.parts, column+" = "+b.placeholder(value))
}

func (b *cascadeBuilder) anyOf(column string, values []string) {
	b.parts = append(b.parts, column+" = ANY("+b.placeholder(values)+")")
}

func (b *cascadeBuilder) placeholder(value any) string {
	b.args = append(b.args, value)
	return "$" + strconv.Itoa(len(b.args))
}

func (b *cascadeBuilder) where() string {
	where := `tenant = $1 AND deployment = $2 AND subject = $3`
	if len(b.parts) > 0 {
		where += ` AND (` + strings.Join(b.parts, " OR ") + `)`
	}
	return where
}

// ListByContext reports the still-readable artifacts of one context in the
// caller's exact scope. The control plane needs it to name what a cascade will
// touch, and acceptance legs need it to prove the cascade selected the right
// rows; it applies the same admission as any read, so a foreign scope sees
// nothing and a tombstone is indistinguishable from a row that never existed.
func (s *Store) ListByContext(ctx context.Context, scope contracts.Scope, contextID string) ([]ArtifactRef, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if err := requireBindingValue("context id", contextID); err != nil {
		return nil, err
	}
	if err := s.authorize(ctx, scope, "read", "artifacts:context:"+contextID); err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
SELECT id FROM ahp_artifacts.artifacts
 WHERE tenant = $1 AND deployment = $2 AND subject = $3 AND context_id = $4
 ORDER BY created_at, id`, scope.Tenant, scope.Deployment, scope.Subject, contextID)
	if err != nil {
		return nil, storageError("list context artifacts", err)
	}
	defer rows.Close()
	var out []ArtifactRef
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, storageError("scan context artifact", err)
		}
		ref, err := s.get(ctx, scope, id, false)
		if err != nil {
			if contracts.Code(err) == contracts.ErrNotFound {
				continue
			}
			return nil, err
		}
		out = append(out, ref)
	}
	return out, storageError("iterate context artifacts", rows.Err())
}

// validateBindingValue accepts an empty binding (a writer that holds no context
// simply produces an artifact no context cascade can reach) and rejects anything
// that is not a bounded, safe identifier. A binding is not an authorization
// input - isolation is the scope's job.
func validateBindingValue(name, value string) error {
	if value == "" {
		return nil
	}
	if !idPattern.MatchString(value) {
		return contracts.NewError(contracts.ErrInvalid, "artifacts: "+name+" is malformed")
	}
	return nil
}

// requireBindingValue is validateBindingValue where an empty value is refused.
// A writer that holds no context legitimately produces an unbound artifact, but
// a cascade that cannot name what it means would select exactly those unbound
// rows, so an empty selector is a client error and never a scope.
func requireBindingValue(name, value string) error {
	if value == "" {
		return contracts.NewError(contracts.ErrInvalid, "artifacts: "+name+" is required")
	}
	return validateBindingValue(name, value)
}

func validateBindingValues(name string, values []string) ([]string, error) {
	clean := make([]string, 0, len(values))
	for _, v := range values {
		if v == "" {
			continue
		}
		if err := validateBindingValue(name, v); err != nil {
			return nil, err
		}
		clean = append(clean, v)
	}
	return clean, nil
}
