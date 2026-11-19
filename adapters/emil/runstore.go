package emil

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/space"
)

// StateRunStore is the T1 translation seam from the Emil reference Core Agent's
// workflow.py run-store (a private PostgreSQL state machine that duplicated the
// AHP State API + RunCoordinator) onto AHP's real State API client + Coordinator.
//
// Per the accepted gap-doc (docs/emil-workflow-state-gap.md, Table A) and the
// director's State wire contract (fence {run_id,worker,epoch} in the request BODY;
// namespace a body field of the checkpoint reads; OBJECT envelopes for reads;
// scope/tenant/subject ONLY from the task-token principal; strict decode), this
// bridge forwards to the existing, authoritative internal/space.StateClient rather
// than duplicating the wire. Its value is the workflow.py-facing surface (Table A),
// the lease/epoch semantics translation (Emil LEASE_LOST -> STALE_EPOCH), and clean
// exported DTOs so the (stateless) engine can persist exclusively through State.
//
// The Emil container name/tag/port and its upstream immutability are NOT a gate
// (decisions/2026-10-30-emil-image-scope-correction.md); this is AHP-owned Go code.

// ErrStaleEpoch is returned when a fenced write is rejected because the caller's
// lease epoch is no longer current (a takeover happened). It maps the State API's
// STALE_EPOCH error (and Emil's older LEASE_LOST) to a typed sentinel so callers
// can stop as a stale writer without string sniffing.
var ErrStaleEpoch = errors.New("emil/runstore: stale epoch (fence rejected)")

// Lease is the run-lease coordinates returned by ClaimLease; it carries the
// task token State minted for the lease, used for all fenced writes.
type Lease struct {
	ThreadID  string
	RunID     string
	TaskID    string
	TaskToken string
	Worker    string
	Epoch     int64
}

// Fence builds the optimistic-concurrency fence for fenced writes.
func (l Lease) Fence() space.Fence {
	return space.Fence{RunID: l.RunID, Worker: l.Worker, Epoch: l.Epoch}
}

// Event is a thread stream event (workflow.py core_events -> State AppendStream).
type Event struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

// InboxEvent is a durable steering/inbox entry (workflow.py inbound queue -> State inbox).
type InboxEvent struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
	Acked    bool            `json:"acked"`
}

// RunInfo is a run record (workflow.py get/lookup -> State GetRun).
type RunInfo struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	TaskID   string          `json:"task_id"`
	Status   string          `json:"status"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

// CheckpointInfo is a resumable checkpoint (workflow.py history reseeding -> State checkpoints).
type CheckpointInfo struct {
	ID        string          `json:"id"`
	Namespace string          `json:"namespace"`
	Parent    string          `json:"parent"`
	Version   int64           `json:"version"`
	Metadata  json.RawMessage `json:"metadata"`
	Parts     []space.Part    `json:"parts"`
}

// RunStore is the workflow.py run-store surface, expressed in AHP State terms.
type RunStore interface {
	ClaimLease(ctx context.Context, token, worker string, ttl time.Duration) (*Lease, error)
	Heartbeat(ctx context.Context, l Lease, ttl time.Duration) error
	Release(ctx context.Context, l Lease, status string) error
	AppendEvent(ctx context.Context, l Lease, key, kind string, payload json.RawMessage) (*Event, error)
	ReadEvents(ctx context.Context, token, threadID string, after, limit int64) ([]Event, error)
	EnqueueInbound(ctx context.Context, token, threadID, key, kind string, payload json.RawMessage) (*InboxEvent, error)
	ReadInbox(ctx context.Context, token, threadID string, after int64, limit int) ([]InboxEvent, error)
	AckInbound(ctx context.Context, l Lease, ids []string) error
	GetRun(ctx context.Context, token, runID string) (*RunInfo, error)
	HeadCheckpoint(ctx context.Context, token, threadID string) (*CheckpointInfo, error)
	SaveCheckpoint(ctx context.Context, l Lease, id, key string, version int64, metadata json.RawMessage, parts []space.Part) (*CheckpointInfo, error)
	RecordIntent(ctx context.Context, l Lease, node, call, kind, target string, args json.RawMessage) (*Intent, error)
	UpdateIntent(ctx context.Context, l Lease, node, call, status string, result json.RawMessage) (*Intent, error)
	GetIntent(ctx context.Context, token, runID, node, call string) (*Intent, error)
	ReconcileIntent(ctx context.Context, l Lease, node, call, status, digest string, result json.RawMessage) (*Intent, error)
}

// StateRunStore implements RunStore over the authoritative AHP State API client.
type StateRunStore struct{ state *space.StateClient }

// NewStateRunStore wires the bridge to an existing StateClient (never a new wire).
func NewStateRunStore(sc *space.StateClient) *StateRunStore { return &StateRunStore{state: sc} }

var _ RunStore = (*StateRunStore)(nil)

// mapStateErr converts the State API's fenced-write rejection into ErrStaleEpoch.
func mapStateErr(err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	if strings.Contains(msg, "STALE_EPOCH") || strings.Contains(msg, "LEASE_LOST") {
		return ErrStaleEpoch
	}
	return err
}

func (s *StateRunStore) ClaimLease(ctx context.Context, token, worker string, ttl time.Duration) (*Lease, error) {
	c, err := s.state.ClaimRun(ctx, token, worker, ttl)
	if err != nil {
		return nil, mapStateErr(err)
	}
	return &Lease{
		ThreadID:  c.Lease.Thread.ID,
		RunID:     c.Lease.Run.ID,
		TaskID:    c.Lease.Run.TaskID,
		TaskToken: c.TaskToken,
		Worker:    c.Lease.Worker,
		Epoch:     c.Lease.Epoch,
	}, nil
}

func (s *StateRunStore) Heartbeat(ctx context.Context, l Lease, ttl time.Duration) error {
	return mapStateErr(s.state.Heartbeat(ctx, l.TaskToken, l.RunID, l.Worker, l.Epoch, ttl))
}

func (s *StateRunStore) Release(ctx context.Context, l Lease, status string) error {
	return mapStateErr(s.state.Release(ctx, l.TaskToken, l.RunID, l.Worker, l.Epoch, status))
}

// ReasonReleaser is the optional capability of a run store that can name WHY the
// run ended. It is deliberately not a method of RunStore: the seam is implemented
// by fakes across the tree, and a terminal reason must not become a compile-time
// burden on every one of them. Callers assert it, and *StateRunStore satisfies it.
//
// Without this carrier the as-runner path could only ever close a run silently:
// RunStore.Release has no reason parameter, so the run plane's terminal row stayed
// empty no matter what the execution plane knew.
type ReasonReleaser interface {
	ReleaseWithReason(ctx context.Context, l Lease, status, closeReason string) error
}

// ReleaseWithReason closes the lease and journals the caller's terminal reason in
// the run payload, verbatim. An empty reason is exactly Release.
func (s *StateRunStore) ReleaseWithReason(ctx context.Context, l Lease, status, closeReason string) error {
	return mapStateErr(s.state.ReleaseWithReason(ctx, l.TaskToken, l.RunID, l.Worker, l.Epoch, status, closeReason))
}

// compile-time proof that the production seam carries the reason.
var _ ReasonReleaser = (*StateRunStore)(nil)

func (s *StateRunStore) AppendEvent(ctx context.Context, l Lease, key, kind string, payload json.RawMessage) (*Event, error) {
	ev, err := s.state.AppendStream(ctx, l.TaskToken, l.ThreadID, l.RunID, l.Worker, l.Epoch, key, kind, payload)
	if err != nil {
		return nil, mapStateErr(err)
	}
	return &Event{ID: ev.ID, ThreadID: ev.ThreadID, Key: ev.Key, Kind: ev.Kind, Sequence: ev.Sequence, Payload: ev.Payload}, nil
}

func (s *StateRunStore) ReadEvents(ctx context.Context, token, threadID string, after, limit int64) ([]Event, error) {
	evs, err := s.state.ReadStream(ctx, token, threadID, after, int(limit))
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(evs))
	for _, e := range evs {
		out = append(out, Event{ID: e.ID, ThreadID: e.ThreadID, Key: e.Key, Kind: e.Kind, Sequence: e.Sequence, Payload: e.Payload})
	}
	return out, nil
}

func (s *StateRunStore) EnqueueInbound(ctx context.Context, token, threadID, key, kind string, payload json.RawMessage) (*InboxEvent, error) {
	ib, err := s.state.Enqueue(ctx, token, threadID, key, kind, payload)
	if err != nil {
		return nil, err
	}
	return &InboxEvent{ID: ib.ID, ThreadID: ib.ThreadID, Key: ib.Key, Kind: ib.Kind, Sequence: ib.Sequence, Payload: ib.Payload, Acked: ib.Acked}, nil
}

func (s *StateRunStore) ReadInbox(ctx context.Context, token, threadID string, after int64, limit int) ([]InboxEvent, error) {
	evs, err := s.state.ReadInbox(ctx, token, threadID, after, limit)
	if err != nil {
		return nil, err
	}
	out := make([]InboxEvent, 0, len(evs))
	for _, e := range evs {
		out = append(out, InboxEvent{ID: e.ID, ThreadID: e.ThreadID, Key: e.Key, Kind: e.Kind, Sequence: e.Sequence, Payload: e.Payload, Acked: e.Acked})
	}
	return out, nil
}

func (s *StateRunStore) AckInbound(ctx context.Context, l Lease, ids []string) error {
	return mapStateErr(s.state.AckInbox(ctx, l.TaskToken, l.RunID, l.Worker, l.Epoch, ids))
}

func (s *StateRunStore) GetRun(ctx context.Context, token, runID string) (*RunInfo, error) {
	r, err := s.state.GetRun(ctx, token, runID)
	if err != nil {
		return nil, err
	}
	return &RunInfo{ID: r.ID, ThreadID: r.ThreadID, TaskID: r.TaskID, Status: r.Status, Sequence: r.Sequence, Payload: r.Payload}, nil
}

func (s *StateRunStore) HeadCheckpoint(ctx context.Context, token, threadID string) (*CheckpointInfo, error) {
	cps, err := s.state.ListCheckpoints(ctx, token, threadID, space.CheckpointNamespace, 1)
	if err != nil {
		return nil, err
	}
	if len(cps) == 0 {
		return nil, nil
	}
	return toCpInfo(cps[0].ID, cps[0].Namespace, cps[0].Parent, cps[0].Version, cps[0].Metadata, cps[0].Parts), nil
}

func (s *StateRunStore) SaveCheckpoint(ctx context.Context, l Lease, id, key string, version int64, metadata json.RawMessage, parts []space.Part) (*CheckpointInfo, error) {
	cp, err := s.state.PutCheckpoint(ctx, l.TaskToken, l.Fence(), space.CheckpointNamespace, id, "", key, version, metadata, parts)
	if err != nil {
		return nil, mapStateErr(err)
	}
	return toCpInfo(cp.ID, cp.Namespace, cp.Parent, cp.Version, cp.Metadata, cp.Parts), nil
}

func toCpInfo(id, ns, parent string, version int64, metadata json.RawMessage, parts []space.Part) *CheckpointInfo {
	return &CheckpointInfo{ID: id, Namespace: ns, Parent: parent, Version: version, Metadata: metadata, Parts: parts}
}
