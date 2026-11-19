package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/jackc/pgx/v5"

	"github.com/ahp-platform/ahp/internal/operations"
	"github.com/ahp-platform/ahp/pkg/contracts"
)

// Metadata keys the gateway understands on an inbound SendMessageRequest. They
// are the only way a caller may name a task kind or a parent task; everything
// else in metadata is stored verbatim but never interpreted here.
const (
	// MetaTaskKind selects the executor-visible task kind ("generic" default).
	MetaTaskKind = "ahp_task_kind"
	// MetaParentTask links the new task to a parent task in the same tenant.
	MetaParentTask = "ahp_parent_task"
	// MetaTreeRoot and MetaTreeDepth are the server-set tree fields of the
	// task tree (PDF rev3.2, 6.1): the gateway derives them from its own task
	// store and values arriving under these keys in a message are ignored.
	// They are what makes tree accounting (and the Model Gateway budget tree)
	// auditable without trusting anything the agent said.
	MetaTreeRoot  = "ahp.root"
	MetaTreeDepth = "ahp.depth"
	// MaxTaskTreeDepth bounds the parent-chain audit. The tenant policy may
	// set a smaller working depth (the PDF default is 4); this guard is the
	// DoS floor below which the audit itself cannot be made to spin.
	MaxTaskTreeDepth = 64
	// DefaultTaskKind is used when the caller does not name a kind.
	DefaultTaskKind = "generic"
)

// Outbox delivery kinds.
const (
	// OutboxKindPush fans a terminal status out to the task's push configs.
	OutboxKindPush = "push"
	// OutboxKindChildStatus reports a child status to the parent task owner.
	OutboxKindChildStatus = "child_status"
)

func metaString(md map[string]any, key string) (string, bool) {
	if md == nil {
		return "", false
	}
	v, ok := md[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

// taskAttributes reads the task kind and parent link from a send request.
func taskAttributes(req *a2a.SendMessageRequest) (kind, parent string) {
	kind = DefaultTaskKind
	if req == nil {
		return kind, ""
	}
	if k, ok := metaString(req.Metadata, MetaTaskKind); ok {
		kind = k
	}
	parent, _ = metaString(req.Metadata, MetaParentTask)
	return kind, parent
}

func pushConfigOf(req *a2a.SendMessageRequest) *a2a.PushConfig {
	if req == nil || req.Config == nil {
		return nil
	}
	return req.Config.PushConfig
}

func returnImmediately(req *a2a.SendMessageRequest) bool {
	return req != nil && req.Config != nil && req.Config.ReturnImmediately
}

// enqueueTerminalOutbox records the delivery obligations created by a terminal
// transition. It runs inside the same transaction as the status write, so a
// committed status always has its notifications recorded and vice versa.
func (s *Service) enqueueTerminalOutbox(ctx context.Context, tx pgx.Tx, scope contracts.Scope, row TaskRow) error {
	if row.ParentTask != "" {
		// The delegation reference is read from the task's own metadata, which
		// only an authenticated parent request could have written, and the sink
		// re-validates it against a registered-host allowlist.
		callback, _ := metaString(row.Metadata, MetaChildStatusURL)
		payload, err := json.Marshal(ChildStatusPayload{
			ParentTaskID:  row.ParentTask,
			TaskID:        row.ID,
			ContextID:     row.ContextID,
			State:         string(row.State),
			StatusVersion: row.StatusVersion,
			FencingToken:  row.FencingToken,
			CallbackURL:   callback,
		})
		if err != nil {
			return fmt.Errorf("gateway: child status payload: %w", err)
		}
		if err := s.store.EnqueueOutbox(ctx, tx, OutboxEntry{
			Scope:        scope,
			TaskID:       row.ID,
			ParentTask:   row.ParentTask,
			Kind:         OutboxKindChildStatus,
			StatusVer:    row.StatusVersion,
			OpKey:        "child:" + row.ID + ":" + strconv.FormatInt(row.StatusVersion, 10),
			FencingToken: row.FencingToken,
			Payload:      payload,
		}); err != nil {
			return err
		}
	}
	configs, err := s.store.pushConfigs(ctx, tx, scope, row.ID)
	if err != nil {
		return err
	}
	if len(configs) == 0 {
		return nil
	}
	payload, err := json.Marshal(map[string]any{
		"taskId":        row.ID,
		"statusVersion": row.StatusVersion,
	})
	if err != nil {
		return fmt.Errorf("gateway: push payload: %w", err)
	}
	return s.store.EnqueueOutbox(ctx, tx, OutboxEntry{
		Scope:        scope,
		TaskID:       row.ID,
		ParentTask:   row.ParentTask,
		Kind:         OutboxKindPush,
		StatusVer:    row.StatusVersion,
		OpKey:        "push:" + row.ID + ":" + strconv.FormatInt(row.StatusVersion, 10),
		FencingToken: row.FencingToken,
		Payload:      payload,
	})
}

// Sink delivers one outbox entry to the outside world. Implementations must
// return ErrAmbiguous when the request may have reached the peer but the
// outcome is unknown; the runner then parks the entry as SIDE_EFFECT_UNKNOWN
// instead of replaying a possibly duplicated notification.
type Sink interface {
	Deliver(ctx context.Context, entry OutboxEntry) error
}

// SinkFunc adapts a function to Sink.
type SinkFunc func(ctx context.Context, entry OutboxEntry) error

// Deliver implements Sink.
func (f SinkFunc) Deliver(ctx context.Context, entry OutboxEntry) error { return f(ctx, entry) }

// NullSink drops every entry; useful for deployments without push transport.
type NullSink struct{}

// Deliver implements Sink.
func (NullSink) Deliver(context.Context, OutboxEntry) error { return nil }

// OutboxConfig tunes the delivery runner.
type OutboxConfig struct {
	WorkerID    string
	Poll        time.Duration // idle poll interval; default 250ms
	Batch       int           // entries per claim; default 16
	MaxAttempts int           // attempts before RETRY_EXHAUSTED; default 5
	RetryAfter  time.Duration // re-queue delay; default 5s
	Logger      *slog.Logger
}

func (c *OutboxConfig) withDefaults() {
	if c.WorkerID == "" {
		c.WorkerID = "obx-" + contracts.NewID()
	}
	if c.Poll <= 0 {
		c.Poll = 250 * time.Millisecond
	}
	if c.Batch <= 0 {
		c.Batch = 16
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 5
	}
	if c.RetryAfter <= 0 {
		c.RetryAfter = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// OutboxRunner drains the transactional outbox. Every attempt is itself an
// effect-ledger operation, so a delivery is at-most-once per attempt key: an
// attempt that ended SIDE_EFFECT_UNKNOWN is never replayed, and a new attempt
// gets a new ledger key with the same monotonic guard.
type OutboxRunner struct {
	store  *Store
	ledger *operations.Store
	sinks  map[string]Sink
	cfg    OutboxConfig
	notify Notifier
}

// NewOutboxRunner wires a runner. sinks is keyed by OutboxEntry.Kind; a kind
// without a sink is parked as failed rather than silently dropped.
func NewOutboxRunner(store *Store, ledger *operations.Store, sinks map[string]Sink, cfg OutboxConfig, notify Notifier) (*OutboxRunner, error) {
	if store == nil || ledger == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "gateway: outbox needs store and ledger")
	}
	if sinks == nil {
		sinks = map[string]Sink{}
	}
	if notify == nil {
		notify = NullNotifier{}
	}
	cfg.withDefaults()
	return &OutboxRunner{store: store, ledger: ledger, sinks: sinks, cfg: cfg, notify: notify}, nil
}

// Run drains until ctx is cancelled.
func (r *OutboxRunner) Run(ctx context.Context) error {
	for {
		n, err := r.DrainOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			r.cfg.Logger.Error("gateway: outbox drain", slog.String("error", err.Error()))
		}
		if n == 0 {
			r.notify.Wait(ctx, claimChannel(), r.cfg.Poll)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}
	}
}

// DrainOnce delivers one batch and returns how many entries were taken.
func (r *OutboxRunner) DrainOnce(ctx context.Context) (int, error) {
	entries, err := r.store.ClaimOutbox(ctx, r.cfg.WorkerID, r.cfg.Batch)
	if err != nil {
		return 0, err
	}
	for _, e := range entries {
		if err := r.deliver(ctx, e); err != nil {
			r.cfg.Logger.Error("gateway: outbox delivery",
				slog.Int64("seq", e.Seq), slog.String("kind", e.Kind), slog.String("error", err.Error()))
		}
	}
	return len(entries), nil
}

func (r *OutboxRunner) deliver(ctx context.Context, e OutboxEntry) error {
	sink, ok := r.sinks[e.Kind]
	if !ok {
		return r.store.CompleteOutbox(ctx, e.Seq, OutboxFailed, "no sink for kind "+e.Kind)
	}
	// Per-attempt ledger key: Dispatch cannot revive a terminal operation, and
	// replaying an attempt whose effect is unknown is exactly what the ledger
	// must forbid. The guard is stable across attempts, so two concurrent
	// workers can never deliver the same entry twice.
	opKey := "obx:" + e.OpKey + ":" + strconv.Itoa(e.Attempts)
	scope := e.Scope
	op, err := r.ledger.Accept(ctx, scope, operations.Request{
		Key:    opKey,
		Kind:   "gateway.outbox." + e.Kind,
		Target: e.TaskID,
		Guard:  "obx:" + e.OpKey,
	})
	if err != nil {
		return err
	}
	switch op.Status {
	case operations.StatusSucceeded:
		return r.store.CompleteOutbox(ctx, e.Seq, OutboxDone, "")
	case operations.StatusSideEffectUnknown:
		return r.store.CompleteOutbox(ctx, e.Seq, OutboxUnknown, "attempt "+strconv.Itoa(e.Attempts)+" unresolved")
	case operations.StatusFailed, operations.StatusFenced:
		// A settled-failed attempt is history: a fresh attempt was already
		// accepted under a new key, so re-dispatch here would be wrong.
		return r.requeue(ctx, e, "attempt "+strconv.Itoa(e.Attempts)+" failed")
	}
	if _, err := r.ledger.Dispatch(ctx, scope, opKey); err != nil {
		if errors.Is(err, operations.ErrAlreadyDispatched) || contracts.Code(err) == contracts.ErrConflict {
			return r.requeue(ctx, e, "attempt already in flight")
		}
		return err
	}
	derr := sink.Deliver(ctx, e)
	switch {
	case derr == nil:
		if _, err := r.ledger.Complete(ctx, scope, opKey, json.RawMessage(`{"delivered":true}`), false); err != nil {
			return err
		}
		return r.store.CompleteOutbox(ctx, e.Seq, OutboxDone, "")
	case errors.Is(derr, ErrAmbiguous):
		if _, err := r.ledger.MarkUnknown(ctx, scope, opKey); err != nil {
			return err
		}
		return r.store.CompleteOutbox(ctx, e.Seq, OutboxUnknown, derr.Error())
	default:
		if errors.Is(derr, ErrPermanent) {
			// The peer refused definitively: settle the attempt as failed so
			// the guard is released and no further attempt is made.
			if _, err := r.ledger.Complete(ctx, scope, opKey, json.RawMessage(`{"delivered":false}`), true); err != nil {
				return err
			}
			return r.store.CompleteOutbox(ctx, e.Seq, OutboxFailed, derr.Error())
		}
		// The ledger row stays DISPATCHED: the guard keeps other workers out
		// until this entry is either settled by a later attempt key or the
		// operation is fenced by an operator.
		return r.requeue(ctx, e, derr.Error())
	}
}

func (r *OutboxRunner) requeue(ctx context.Context, e OutboxEntry, reason string) error {
	state := OutboxPending
	if e.Attempts >= r.cfg.MaxAttempts {
		state = OutboxUnavailable
	}
	err := r.store.CompleteOutbox(ctx, e.Seq, state, reason)
	if err == nil && state == OutboxPending {
		_ = r.notify.Notify(ctx, claimChannel())
	}
	return err
}
