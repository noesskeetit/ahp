package memory

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Scope string

const (
	Personal Scope = "personal"
	Project  Scope = "project"
)

func (s Scope) Valid() bool { return s == Personal || s == Project }

type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string                    { return e.Code + ": " + e.Message }
func Fail(status int, code, message string) error { return &Error{code, message, status} }

var ErrMissing = errors.New("record not found")

// Identity is supplied by a trusted platform adapter, never by request JSON.
type Identity struct {
	TenantID  string          `json:"tenant_id"`
	UserID    string          `json:"user_id"`
	ActorID   string          `json:"actor_id"`
	ActorKind string          `json:"actor_kind"`
	GrantID   string          `json:"grant_id"`
	TaskID    string          `json:"task_id"`
	ExpiresAt time.Time       `json:"expires_at"`
	Labels    []string        `json:"labels,omitempty"`
	ContextID string          `json:"context_id,omitempty"`
	Authority json.RawMessage `json:"authority,omitempty"`
}

func (i Identity) Owner() string {
	b, _ := json.Marshal([]string{i.TenantID, i.UserID})
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func (i Identity) Validate(now time.Time) error {
	if i.TenantID == "" || i.UserID == "" || i.ActorID == "" || i.GrantID == "" || !i.ExpiresAt.After(now) {
		return Fail(403, "invalid_identity", "a live delegated user identity is required")
	}
	if len(i.TenantID) > 200 || len(i.UserID) > 200 || len(i.ActorID) > 200 || len(i.GrantID) > 500 || len(i.Labels) > 100 {
		return Fail(403, "invalid_identity", "identity exceeds configured bounds")
	}
	return nil
}

type AccessRequest struct {
	Action    string   `json:"action"`
	ProjectID string   `json:"project_id,omitempty"`
	Scopes    []Scope  `json:"scopes,omitempty"`
	Sources   []Source `json:"sources,omitempty"`
}

// Access must check subject AND actor AND grant, including source restrictions.
// Bind must register inherited data labels before any retrieved text is returned.
type Access interface {
	Authorize(context.Context, string, AccessRequest) (Identity, error)
	Revalidate(context.Context, Identity, AccessRequest) error
	Bind(context.Context, Identity, []string) error
}

type Source struct {
	Ref           string    `json:"ref"`
	Role          string    `json:"role"`
	Text          string    `json:"text"`
	At            time.Time `json:"at"`
	AllowedScopes []Scope   `json:"allowed_scopes"`
	Labels        []string  `json:"labels,omitempty"`
}

type TurnRequest struct {
	IntentDigest string   `json:"-"`
	ProjectID    string   `json:"project_id"`
	SessionID    string   `json:"session_id"`
	Sequence     int64    `json:"sequence"`
	Sources      []Source `json:"sources"`
	Sanitized    bool     `json:"sanitized"`
	Flush        bool     `json:"flush,omitempty"`
}
type RememberRequest struct {
	ProjectID  string   `json:"project_id,omitempty"`
	Target     Scope    `json:"target"`
	Text       string   `json:"text"`
	SourceRefs []string `json:"source_refs"`
}
type SearchRequest struct {
	ProjectID string `json:"project_id,omitempty"`
	Scope     string `json:"scope"`
	Query     string `json:"query"`
}
type ContextRequest struct {
	ProjectID       string `json:"project_id"`
	Query           string `json:"query"`
	IncludePersonal bool   `json:"include_personal"`
}
type SettingsRequest struct {
	ProjectID       string `json:"project_id,omitempty"`
	PersonalEnabled *bool  `json:"personal_enabled,omitempty"`
	ProjectEnabled  *bool  `json:"project_enabled,omitempty"`
	UsePersonal     *bool  `json:"use_personal,omitempty"`
	IdleDays        *int   `json:"idle_days,omitempty"`
}
type ClearRequest struct {
	ProjectID string `json:"project_id,omitempty"`
	Scope     Scope  `json:"scope"`
}
type CorrectRequest struct {
	Scope     Scope  `json:"scope"`
	ProjectID string `json:"project_id,omitempty"`
	Text      string `json:"text"`
	Reason    string `json:"reason"`
}

type Settings struct {
	PersonalEnabled bool      `json:"personal_enabled"`
	ProjectEnabled  bool      `json:"project_enabled"`
	UsePersonal     bool      `json:"use_personal"`
	IdleDays        int       `json:"idle_days"`
	LastActivity    time.Time `json:"last_activity,omitempty"`
}

type Fact struct {
	Ref         string            `json:"memory_ref"`
	Scope       Scope             `json:"scope"`
	Text        string            `json:"text"`
	Type        string            `json:"type,omitempty"`
	MentionedAt string            `json:"mentioned_at,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}
type SearchResult struct {
	Facts    []Fact  `json:"facts"`
	Degraded []Scope `json:"degraded,omitempty"`
}
type Operation struct {
	ID string `json:"id"`
	// FactsCommitted, when set on a completed retain, reports how many memory
	// facts the upstream extractor actually committed. A retain can complete
	// with zero facts (correctly, e.g. runtime noise the mission rejects), so
	// this is an observable signal of a partial or empty extraction, not a
	// change to success semantics. It is absent until the worker can observe it.
	FactsCommitted *int   `json:"facts_committed,omitempty"`
	State          string `json:"state"`
	Scope          Scope  `json:"scope"`
	Error          string `json:"error,omitempty"`
}

type User struct {
	Enabled bool
	Since   time.Time
}
type ProjectState struct {
	ID            string
	Enabled       bool
	UsePersonal   bool
	PersonalSince time.Time
	ProjectSince  time.Time
	IdleDays      int
	LastActivity  time.Time
	Deleted       bool
}
type Bank struct {
	ID          string
	Scope       Scope
	ProjectID   string
	Generation  int
	State       string // active -> deleting -> deleted; physical IDs are never reused
	CreatedAt   time.Time
	SourceFloor time.Time
	ClosedAt    time.Time
	NextPurge   time.Time
	Revision    int64
	Labels      []string
}
type Turn struct {
	ID         string
	Hash       string
	Request    TurnRequest
	Identity   Identity
	ReceivedAt time.Time
}
type Cursor struct {
	ID                string
	BankID            string
	ProjectID         string
	SessionID         string
	Pending           []string
	PendingSince      time.Time
	JobID             string
	ProcessedSequence int64
	LastSequence      int64
	Flush             bool
}
type Job struct {
	ID             string
	Kind           string // retain, correct, invalidate
	BankID         string
	Scope          Scope
	ProjectID      string
	CursorID       string
	TurnIDs        []string
	Identity       Identity
	Sources        []Source
	Content        string
	Metadata       map[string]string
	MemoryID       string
	Reason         string
	State          string // queued, submitted, succeeded, failed, cancelled
	Attempts       int
	Failures       int
	NextAt         time.Time
	LeaseToken     string
	LeaseUntil     time.Time
	Error          string
	CreatedAt      time.Time
	FinishedAt     time.Time
	PayloadExpired bool
}
type Idempotency struct {
	Hash     string
	Response json.RawMessage
}
type Run struct {
	ID        string
	ProjectID string
	Until     time.Time
	Finished  bool
}
type RunRequest struct {
	ProjectID string `json:"project_id"`
	RunID     string `json:"run_id"`
	Finished  bool   `json:"finished"`
}

// Tx is scoped to a single (tenant,user) and serialized by the durable store.
// No network calls may be made inside Store.Within.
type Tx interface {
	RegisterIdentity(Identity) error
	ForgetIdentity() error
	TenantFrozen() (bool, error)
	Get(kind, id string, into any) error
	Put(kind, id string, value any) error
	Delete(kind, id string) error
	DeleteKind(kind string) error
	List(kind string) ([]json.RawMessage, error)
	Schedule(time.Time) error
}
type Store interface {
	FenceTenant(context.Context, string) error
	TenantOwners(context.Context, string) ([]Identity, error)
	Within(context.Context, string, func(Tx) error) error
	Due(context.Context, time.Time, int) ([]string, error)
	Ping(context.Context) error
}

type NativeFact struct {
	ID          string            `json:"id"`
	Text        string            `json:"text"`
	Type        string            `json:"type"`
	MentionedAt string            `json:"mentioned_at"`
	Metadata    map[string]string `json:"metadata"`
}
type Engine interface {
	Configure(context.Context, Bank) error
	Retain(context.Context, Bank, Job) error
	Status(context.Context, string, string) (string, error)
	Cancel(context.Context, string, string) error
	Recall(context.Context, Bank, string, int) ([]NativeFact, error)
	Correct(context.Context, Bank, Job) error
	DeleteBank(context.Context, string) error
}

// RetainStatsEngine is an optional Engine capability (discovered with a type
// assertion, exactly like ListEngine): once a retain operation has completed it
// can report how many memory facts the upstream extractor actually committed.
// The worker records this on the job so the durable Operation carries an
// observable "completed with N facts" signal instead of failing silently when an
// extraction produced nothing the caller expected. Engines that do not implement
// it simply leave the operation without that field.
type RetainStatsEngine interface {
	RetainFactsCommitted(context.Context, string, string) (int, error)
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Errorf("random id: %w", err))
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func list[T any](tx Tx, kind string) ([]T, error) {
	rows, err := tx.List(kind)
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(rows))
	for _, row := range rows {
		var v T
		if err := json.Unmarshal(row, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}
