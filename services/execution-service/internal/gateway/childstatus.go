package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/netpolicy"
	"github.com/ahp-platform/ahp/pkg/transport"
)

// MetaChildStatusURL is the explicit delegation reference a parent owner
// attaches when it creates a child task: the absolute URL that must receive
// the child's status events. Without it the gateway reports to its own
// configured child-status endpoint. Because the reference is written by an
// authenticated parent request and validated against the sink's host
// allowlist, a child can never redirect its own status events to an arbitrary
// third party.
const MetaChildStatusURL = "ahp_child_status_url"

// DefaultChildStatusPath is appended to the sink base when the child carries
// no explicit delegation reference.
const DefaultChildStatusPath = "/v1/gateway/child-status"

// Acknowledgement statuses the receiver returns. DUPLICATE and STALE are
// successes from the sender's point of view: the obligation is settled because
// the event is already known or superseded.
const (
	ChildStatusAccepted  = "ACCEPTED"
	ChildStatusDuplicate = "DUPLICATE"
	ChildStatusStale     = "STALE"
)

// ChildStatusAck is the acknowledgement body of the child-status endpoint.
type ChildStatusAck struct {
	Status string `json:"status"`
}

// ChildStatusPayload is the event a child's deployment reports to whoever owns
// its parent. The body deliberately carries no timestamps or attempt counters:
// an at-least-once redelivery must be byte-identical, otherwise the receiver
// cannot tell a duplicate from a conflicting reuse of the idempotency key.
//
// CallbackURL is routing information only. The receiver ignores it when
// applying the event; it exists so one shared sink can serve many parents.
type ChildStatusPayload struct {
	ParentTaskID  string `json:"parentTaskId"`
	TaskID        string `json:"taskId"`
	ContextID     string `json:"contextId,omitempty"`
	State         string `json:"state"`
	StatusVersion int64  `json:"statusVersion"`
	FencingToken  int64  `json:"fencingToken"`
	CallbackURL   string `json:"callbackUrl,omitempty"`
}

// canonical returns the projection the receiver deduplicates on. Routing data
// is excluded: the same logical event stays the same event even if the
// reference it carries is refreshed.
func (p ChildStatusPayload) canonical() map[string]any {
	return map[string]any{
		"parentTaskId":  p.ParentTaskID,
		"taskId":        p.TaskID,
		"contextId":     p.ContextID,
		"state":         p.State,
		"statusVersion": p.StatusVersion,
		"fencingToken":  p.FencingToken,
	}
}

// Digest is the canonical identity of the event, independent of routing data.
func (p ChildStatusPayload) Digest() (string, error) { return contracts.Digest(p.canonical()) }

func (p ChildStatusPayload) validate() error {
	if p.ParentTaskID == "" || p.TaskID == "" {
		return contracts.NewError(contracts.ErrInvalid, "gateway: child status needs parent and child task ids")
	}
	if p.State == "" {
		return contracts.NewError(contracts.ErrInvalid, "gateway: child status needs a state")
	}
	if p.StatusVersion < 1 {
		return contracts.NewError(contracts.ErrInvalid, "gateway: child status needs a positive status version")
	}
	return nil
}

// childStatusCallback validates a delegation reference. It must be an absolute
// http(s) origin+path with no credentials, query or fragment, so a reference
// can neither smuggle credentials nor hide parameters from the policy check.
func childStatusCallback(raw string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: child status callback: "+err.Error())
	}
	if u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil ||
		u.RawQuery != "" || u.Fragment != "" {
		return nil, contracts.NewError(contracts.ErrInvalid,
			"gateway: child status callback must be an absolute http(s) url without credentials or query")
	}
	return u, nil
}

func joinPath(base *url.URL, path string) string {
	u := *base
	u.Path = strings.TrimRight(base.Path, "/") + path
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

// ChildStatusSinkConfig configures the child-status sender.
type ChildStatusSinkConfig struct {
	// Base is the parent deployment's AHP API origin, optionally with a path
	// prefix. Events without a delegation reference go to Base+path.
	Base string
	// Path overrides DefaultChildStatusPath.
	Path string
	// CallbackHosts is the allowlist of hosts an explicit delegation reference
	// may name. It is empty by default, which fails closed: without an
	// operator-registered host, delegating a child's status elsewhere is
	// refused rather than trusted.
	CallbackHosts []string
}

// ChildStatusSink reports a child task's terminal status to whoever owns the
// parent. Every call carries the outbox operation key as Idempotency-Key, so a
// re-claimed entry can never be counted twice by the receiver.
type ChildStatusSink struct {
	base          *url.URL
	path          string
	callbackHosts map[string]bool
	client        *http.Client
	tokens        TokenSource
}

// NewChildStatusSink builds the sink. policy must allow the resulting hosts;
// because loopback is denied unconditionally, tests inject a resolver/dialer.
func NewChildStatusSink(cfg ChildStatusSinkConfig, policy netpolicy.Policy, tokens TokenSource, opts ...netpolicy.Option) (*ChildStatusSink, error) {
	base, err := childStatusCallback(cfg.Base)
	if err != nil {
		return nil, fmt.Errorf("gateway: child status base: %w", err)
	}
	path := strings.TrimSpace(cfg.Path)
	if path == "" {
		path = DefaultChildStatusPath
	}
	if !strings.HasPrefix(path, "/") {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: child status path must start with /")
	}
	allowed := map[string]bool{}
	for _, h := range cfg.CallbackHosts {
		h = strings.TrimSpace(strings.ToLower(h))
		if h == "" {
			continue
		}
		allowed[h] = true
	}
	client, err := netpolicy.NewClient(policy, opts...)
	if err != nil {
		return nil, fmt.Errorf("gateway: child status client: %w", err)
	}
	return &ChildStatusSink{base: base, path: path, callbackHosts: allowed, client: client, tokens: tokens}, nil
}

// target resolves where one event is delivered.
func (s *ChildStatusSink) target(p ChildStatusPayload) (string, error) {
	raw := strings.TrimSpace(p.CallbackURL)
	if raw == "" {
		return joinPath(s.base, s.path), nil
	}
	u, err := childStatusCallback(raw)
	if err != nil {
		return "", err
	}
	if u.Host == s.base.Host {
		return u.String(), nil
	}
	if !s.callbackHosts[u.Host] && !s.callbackHosts[u.Hostname()] {
		return "", fmt.Errorf("%w: child status callback host %q is not registered", ErrPermanent, u.Host)
	}
	return u.String(), nil
}

// Deliver implements Sink.
func (s *ChildStatusSink) Deliver(ctx context.Context, e OutboxEntry) error {
	if e.Kind != OutboxKindChildStatus {
		return fmt.Errorf("%w: child status sink received kind %q", ErrPermanent, e.Kind)
	}
	var p ChildStatusPayload
	if err := json.Unmarshal(e.Payload, &p); err != nil {
		return fmt.Errorf("%w: child status payload: %v", ErrPermanent, err)
	}
	if err := p.validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	}
	target, err := s.target(p)
	if err != nil {
		return err
	}
	// Marshal from the decoded struct: the outbox column is jsonb, whose key
	// order is not the sender's, and a duplicate must be byte-identical.
	body, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("%w: child status body: %v", ErrPermanent, err)
	}
	headers := map[string]string{"Idempotency-Key": e.OpKey}
	if s.tokens != nil {
		token, err := s.tokens.Token(ctx)
		if err != nil {
			return fmt.Errorf("gateway: child status token: %w", err)
		}
		if token != "" {
			headers["Authorization"] = "Bearer " + token
		}
	}
	status, raw, err := postJSON(ctx, s.client, target, headers, body)
	if err != nil {
		return err
	}
	if status >= 200 && status < 300 {
		var ack ChildStatusAck
		if err := json.Unmarshal(raw, &ack); err != nil || len(raw) == 0 {
			// A bare 2xx without an acknowledgement is still a durable accept
			// by the receiver's contract.
			return nil
		}
		switch strings.ToUpper(strings.TrimSpace(ack.Status)) {
		case ChildStatusAccepted, ChildStatusDuplicate, ChildStatusStale:
			return nil
		default:
			return fmt.Errorf("%w: unrecognised child status acknowledgement %q", ErrPermanent, ack.Status)
		}
	}
	// A conflict or stale epoch means the parent already moved past this
	// status: replaying is useless and must be settled, not retried.
	if text := string(raw); strings.Contains(text, string(contracts.ErrStaleEpoch)) ||
		strings.Contains(text, "STALE_EPOCH") || strings.Contains(text, string(contracts.ErrConflict)) ||
		strings.Contains(text, string(contracts.ErrKeyReuse)) || status == http.StatusNotFound {
		return fmt.Errorf("%w: parent rejected %s@%d: %s", ErrPermanent, p.TaskID, p.StatusVersion, truncate(text, 200))
	}
	return classifyStatus(status, raw, "child status")
}

// ChildStatusObserver sees every newly accepted child status. The callback
// runs inside the receiving transaction: returning an error rolls the accept
// back so the sender retries. This is where a parent deployment forwards the
// event to its run coordinator or the State inbox when the task graph is busy.
type ChildStatusObserver interface {
	OnChildStatus(ctx context.Context, ev ChildStatusEvent) error
}

// ChildStatusObserverFunc adapts a function to ChildStatusObserver.
type ChildStatusObserverFunc func(ctx context.Context, ev ChildStatusEvent) error

// OnChildStatus implements ChildStatusObserver.
func (f ChildStatusObserverFunc) OnChildStatus(ctx context.Context, ev ChildStatusEvent) error {
	return f(ctx, ev)
}

// ChildStatusEvent is one accepted child status together with its receipt.
type ChildStatusEvent struct {
	Scope contracts.Scope
	Key   string
	Event ChildStatusPayload
}

// ChildStatusInbox is the durable, deduplicating receiver side of the
// child-status contract. It stores receipts in the parent deployment's own
// schema: delivery is at-least-once, so the receiver owns idempotency, and the
// graph is reconciled through tasks/get after a pause.
type ChildStatusInbox struct {
	store *Store
}

// NewChildStatusInbox binds an inbox to a migrated gateway store.
func NewChildStatusInbox(store *Store) (*ChildStatusInbox, error) {
	if store == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: child status inbox needs a store")
	}
	return &ChildStatusInbox{store: store}, nil
}

// Receive durably records one event. It returns ChildStatusDuplicate when the
// exact event was applied before and ChildStatusStale when a newer status of
// the same child is already known; neither calls the observer again.
func (i *ChildStatusInbox) Receive(ctx context.Context, scope contracts.Scope, key string,
	p ChildStatusPayload, obs ChildStatusObserver) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := p.validate(); err != nil {
		return "", err
	}
	digest, err := p.Digest()
	if err != nil {
		return "", err
	}
	if key == "" {
		key = "child:" + p.TaskID + ":" + fmt.Sprint(p.StatusVersion)
	}
	var outcome string
	err = pgx.BeginFunc(ctx, i.store.pool, func(tx pgx.Tx) error {
		// Serialise per child so two concurrent deliveries of different
		// statuses cannot interleave and apply an older version last.
		lockKey := childStatusLockKey(scope, p.TaskID)
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockKey); err != nil {
			return fmt.Errorf("gateway: child status lock: %w", err)
		}
		// The parent must exist in this scope. A delivery for a task the
		// receiver does not own is refused, never silently adopted.
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT true FROM `+i.store.t("tasks")+
			` WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task_id=$4`,
			scope.Tenant, scope.Deployment, scope.Subject, p.ParentTaskID).Scan(&exists); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return contracts.NewError(contracts.ErrNotFound, "gateway: parent task is not owned by this scope")
			}
			return fmt.Errorf("gateway: child status parent: %w", err)
		}
		// Highest version durably applied for this child; the advisory lock above
		// guarantees it cannot move underneath us.
		var knownVersion int64
		if err := tx.QueryRow(ctx, `SELECT coalesce(max(status_version), 0) FROM `+i.store.t("child_status_inbox")+
			` WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task_id=$4`,
			scope.Tenant, scope.Deployment, scope.Subject, p.TaskID).Scan(&knownVersion); err != nil {
			return fmt.Errorf("gateway: child status version lookup: %w", err)
		}
		if knownVersion > p.StatusVersion {
			outcome = ChildStatusStale
			return nil
		}
		// The row for exactly this version, if any: its digest separates a
		// byte-identical redelivery from a conflicting reuse of the version.
		var knownDigest string
		lookupErr := tx.QueryRow(ctx, `SELECT body_digest FROM `+i.store.t("child_status_inbox")+
			` WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task_id=$4 AND status_version=$5`,
			scope.Tenant, scope.Deployment, scope.Subject, p.TaskID, p.StatusVersion).Scan(&knownDigest)
		switch {
		case lookupErr == nil:
			if knownDigest == digest {
				outcome = ChildStatusDuplicate
				return nil
			}
			return contracts.NewError(contracts.ErrKeyReuse,
				"gateway: child status version already applied with a different body")
		case errors.Is(lookupErr, pgx.ErrNoRows):
			// A genuinely newer version: apply it below.
		default:
			return fmt.Errorf("gateway: child status lookup: %w", lookupErr)
		}
		tag, err := tx.Exec(ctx, `INSERT INTO `+i.store.t("child_status_inbox")+`
			(tenant, deployment, subject, parent_task, task_id, status_version, state, body_digest, delivery_key)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
			ON CONFLICT (tenant, deployment, subject, task_id, status_version) DO NOTHING`,
			scope.Tenant, scope.Deployment, scope.Subject, p.ParentTaskID, p.TaskID,
			p.StatusVersion, p.State, digest, key)
		if err != nil {
			return fmt.Errorf("gateway: child status insert: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Lost a race with an identical delivery: treat it as duplicate.
			outcome = ChildStatusDuplicate
			return nil
		}
		outcome = ChildStatusAccepted
		if obs != nil {
			return obs.OnChildStatus(ctx, ChildStatusEvent{Scope: scope, Key: key, Event: p})
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// Applied reports the highest status version durably applied for a child,
// and whether any receipt exists at all.
func (i *ChildStatusInbox) Applied(ctx context.Context, scope contracts.Scope, childTaskID string) (int64, bool, error) {
	var max int64
	err := i.store.pool.QueryRow(ctx, `SELECT coalesce(max(status_version), 0) FROM `+i.store.t("child_status_inbox")+
		` WHERE tenant=$1 AND deployment=$2 AND subject=$3 AND task_id=$4`,
		scope.Tenant, scope.Deployment, scope.Subject, childTaskID).Scan(&max)
	if err != nil {
		return 0, false, fmt.Errorf("gateway: child status applied: %w", err)
	}
	return max, max > 0, nil
}

// ChildStatusHandlerConfig tunes the receiving endpoint.
type ChildStatusHandlerConfig struct {
	MaxBodyBytes int64
}

func (c *ChildStatusHandlerConfig) withDefaults() {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = transport.DefaultMaxBodyBytes
	}
}

// ChildStatusHandler is the HTTP receiver a parent deployment mounts. Identity
// comes from the verified principal only; a delivery for a task the receiver
// does not own is answered 404 so probing cannot enumerate other tenants.
type ChildStatusHandler struct {
	inbox *ChildStatusInbox
	obs   ChildStatusObserver
	cfg   ChildStatusHandlerConfig
}

// NewChildStatusHandler builds the endpoint.
func NewChildStatusHandler(inbox *ChildStatusInbox, obs ChildStatusObserver, cfg ChildStatusHandlerConfig) (*ChildStatusHandler, error) {
	if inbox == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: child status handler needs an inbox")
	}
	cfg.withDefaults()
	return &ChildStatusHandler{inbox: inbox, obs: obs, cfg: cfg}, nil
}

// ServeHTTP implements http.Handler.
func (h *ChildStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrInvalid, "method not allowed"))
		return
	}
	principal, ok := transport.FromContext(r.Context())
	if !ok {
		transport.WriteError(w, r, contracts.NewError(contracts.ErrForbidden, "gateway: no verified principal"))
		return
	}
	var p ChildStatusPayload
	if err := transport.ReadJSON(w, r, &p, h.cfg.MaxBodyBytes); err != nil {
		return
	}
	outcome, err := h.inbox.Receive(r.Context(), principal.Scope, r.Header.Get("Idempotency-Key"), p, h.obs)
	if err != nil {
		transport.WriteError(w, r, err)
		return
	}
	transport.WriteJSON(w, http.StatusOK, ChildStatusAck{Status: outcome})
}

// childStatusLockKey builds an unambiguous advisory-lock key from the receiving
// scope and the child task id. PostgreSQL text values cannot contain NUL, so the
// fields are length-prefixed: the encoding is injective without any separator
// byte, which lets concurrent deliveries of the same child serialise on one
// lock while distinct scopes never share it.
func childStatusLockKey(scope contracts.Scope, childTaskID string) string {
	var b strings.Builder
	for _, f := range []string{scope.Tenant, scope.Deployment, scope.Subject, childTaskID} {
		b.WriteString(fmt.Sprintf("%d:", len(f)))
		b.WriteString(f)
	}
	return b.String()
}
