package space

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// StateClient talks to the State API over HTTP. Space never opens a direct
// database connection; all persistence flows through this client.
type StateClient struct {
	baseURL string
	client  *http.Client
}

// NewStateClient creates a client pointed at the State API base URL.
func NewStateClient(baseURL string, client *http.Client) *StateClient {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &StateClient{baseURL: baseURL, client: client}
}

// stateError is the JSON error shape returned by the State API.
type stateError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Reason  string `json:"reason,omitempty"`
}

func (e *stateError) ReasonCode() string { return e.Reason }

func (e *stateError) Error() string {
	return fmt.Sprintf("state: %s: %s", e.Code, e.Message)
}

// As converts a State failure into the shared contracts error vocabulary, so
// errors.As(err, &*contracts.Error) - and therefore contracts.Code and
// transport.WriteError - see the code State actually returned (STALE_EPOCH,
// CONFLICT, FORBIDDEN, NOT_FOUND, KEY_REUSE_MISMATCH, ...) instead of a bare
// 500 INTERNAL.
//
// This conversion is the ONLY reason a fence refusal reaches its caller as a
// 409: Space wraps State failures with fmt.Errorf("%w") all the way up, so
// without an As path every wrapper loses the code and the transport default
// answers 500 for a refusal State made for a reason. State owns the verdict;
// Space must transport it unchanged - never re-classify it, never swallow it.
func (e *stateError) As(target any) bool {
	ce, ok := target.(**contracts.Error)
	if !ok {
		return false
	}
	*ce = &contracts.Error{Code: e.Code, Message: e.Message}
	return true
}

// codeForStatus is the honest fallback when the State peer answers with an
// error that carries no contracts envelope (a proxy page, a plain-text
// rejection, a truncated body). The HTTP status is the only evidence left, and
// a refusal that is plainly a 409/403/404 must NOT collapse into an
// unclassified internal error. Anything unrecognised stays codeless on
// purpose: that IS a genuine internal failure, and inventing a client-visible
// code for it would be a lie of a different shape.
func codeForStatus(status int) string {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return contracts.ErrForbidden
	case http.StatusNotFound:
		return contracts.ErrNotFound
	case http.StatusConflict, http.StatusLocked:
		return contracts.ErrConflict
	case http.StatusBadRequest:
		return contracts.ErrInvalid
	default:
		return ""
	}
}

// stateStreamPageSize keeps legacy internal recovery/history pages below the
// response ceiling even when JSON HTML escaping expands a 1MiB stored frame
// sixfold. Request and inline State limits remain unchanged.
const stateStreamPageSize = 2

// Recovery uses the existing byte-bounded protocol so small delta receipts do
// not require one network round trip per pair. Large escaped frames still stop
// at the full response ceiling; callers must follow the explicit has_more.
const recoveryStreamPageSize = 128
const recoveryStreamPageBytes = 16 << 20

// call posts a JSON body to /v1/state/{op} with the bearer task token.
func (c *StateClient) call(ctx context.Context, op, token string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("stateclient marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/v1/state/"+op, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("stateclient %s: %w", op, err)
	}
	defer resp.Body.Close()
	responseLimit := int64(4 << 20)
	if op == "ReadStream" {
		responseLimit = 16 << 20
	}
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, responseLimit+1))
	if err != nil {
		return fmt.Errorf("stateclient %s read: %w", op, err)
	}
	if int64(len(respBody)) > responseLimit {
		return fmt.Errorf("stateclient %s response exceeds %d bytes", op, responseLimit)
	}
	if resp.StatusCode >= 400 {
		var se stateError
		if json.Unmarshal(respBody, &se) == nil && se.Code != "" {
			return &se
		}
		if code := codeForStatus(resp.StatusCode); code != "" {
			return fmt.Errorf("stateclient %s: %w", op, &stateError{
				Code:    code,
				Message: fmt.Sprintf("State answered HTTP %d without a contracts error envelope: %s", resp.StatusCode, string(respBody)),
			})
		}
		return fmt.Errorf("stateclient %s: HTTP %d: %s", op, resp.StatusCode, string(respBody))
	}
	if out != nil {
		return json.Unmarshal(respBody, out)
	}
	return nil
}

// Thread and Run operations

func (c *StateClient) CreateThread(ctx context.Context, token string, scope contracts.Scope, contextID, key string) (*threadResponse, error) {
	var out threadResponse
	err := c.call(ctx, "CreateThread", token, map[string]string{
		"context_id": contextID,
		"key":        key,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *StateClient) GetThread(ctx context.Context, token, threadID string) (*threadResponse, error) {
	var out threadResponse
	err := c.call(ctx, "GetThread", token, map[string]string{"thread_id": threadID}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteThread deletes the caller's own thread and everything the State schema
// cascades from it (runs, intents, stream and inbox events, checkpoints). It is
// the durable half of "delete this chat": scope comes from the presenting task
// token and never from the body, so a foreign or already-absent thread answers
// NOT_FOUND and a thread under legalhold answers FORBIDDEN - both travel back as
// coded errors through stateError.As, so the edge forwards State's verdict instead
// of re-classifying it.
//
// A transport failure here is NOT a failed delete: the caller cannot tell "nothing
// happened" from "the row is gone and the ACK was lost". The operation is idempotent
// by thread id, so a retry converges - it answers NOT_FOUND once the row is gone,
// which is exactly how the caller learns the deletion is durable.
func (c *StateClient) DeleteThread(ctx context.Context, token, threadID string) error {
	return c.call(ctx, "DeleteThread", token, map[string]string{"thread_id": threadID}, nil)
}

func (c *StateClient) CreateRun(ctx context.Context, token, threadID, key string, payload json.RawMessage, taskID string) (*runResponse, error) {
	var out runResponse
	err := c.call(ctx, "CreateRun", token, map[string]any{
		"thread_id": threadID,
		"key":       key,
		"payload":   payload,
		"task_id":   taskID,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *StateClient) GetRun(ctx context.Context, token, runID string) (*runResponse, error) {
	var out runResponse
	err := c.call(ctx, "GetRun", token, map[string]string{"run_id": runID}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *StateClient) ClaimRun(ctx context.Context, token, worker string, ttl time.Duration) (*claimResponse, error) {
	var out claimResponse
	err := c.call(ctx, "ClaimRun", token, map[string]any{
		"worker":       worker,
		"ttl_ms":       ttl.Milliseconds(),
		"payload_type": "turn",
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *StateClient) Heartbeat(ctx context.Context, token, runID, worker string, epoch int64, ttl time.Duration) error {
	return c.call(ctx, "Heartbeat", token, map[string]any{
		"run_id": runID,
		"worker": worker,
		"epoch":  epoch,
		"ttl_ms": ttl.Milliseconds(),
	}, nil)
}

func (c *StateClient) Release(ctx context.Context, token, runID, worker string, epoch int64, status string) error {
	return c.release(ctx, token, runID, worker, epoch, status, "", "")
}

// ReleaseWithReason ends the lease and, on a terminal status, records the reason
// the caller gave under the run payload's close_reason key. Empty reason is
// exactly Release: nothing is written and nothing is invented.
func (c *StateClient) ReleaseWithReason(ctx context.Context, token, runID, worker string,
	epoch int64, status, closeReason string) error {
	return c.release(ctx, token, runID, worker, epoch, status, closeReason, "")
}

// ReleaseWithReasonCause ends the lease and, on a terminal status, records both the
// dictionary word (close_reason) and the refusing plane's own sentence (failure_cause,
// see internal/state RunPayloadFailureCause). An empty cause sends no key at all, which
// keeps this byte-identical to ReleaseWithReason for every existing caller.
func (c *StateClient) ReleaseWithReasonCause(ctx context.Context, token, runID, worker string,
	epoch int64, status, closeReason, cause string) error {
	return c.release(ctx, token, runID, worker, epoch, status, closeReason, cause)
}

func (c *StateClient) release(ctx context.Context, token, runID, worker string,
	epoch int64, status, closeReason, cause string) error {
	lease := map[string]any{"run_id": runID, "worker": worker, "epoch": epoch, "status": status}
	// DEF-H22-3 and DEF-RELEASE-REASON-1, in that order of what they protect.
	//
	// DEF-H22-3: the terminal extras are OPTIONAL on the wire, and they must never be
	// the reason a run cannot close. The State face decodes bodies strictly (transport
	// DisallowUnknownFields), so a State peer that predates the close_reason field
	// refused the whole release - not just the reason - and a refused release leaves the
	// lease to expire with the run still non-terminal: the orphan sweep re-claimed it and
	// re-executed a turn whose model work was already finished, once per sweep, forever
	// (measured: one run re-claimed from epoch 1 to epoch 26 with 33 model events under a
	// declared max_steps of 3). So:
	//   1. nothing optional is ever sent empty - an empty extra is not sent at all, so an
	//      old sender's bytes stay byte-identical to the historical Release wire form;
	//   2. if the peer refuses a key BY NAME, that key alone is dropped and the release
	//      is retried - for the statuses whose contract still allows an unstated end.
	//
	// DEF-RELEASE-REASON-1 bounds rule 2: a failed or canceled run cannot end without a
	// stated reason, so dropping the key the peer refused there is not a workaround, it
	// is the forbidden write performed by the client. For those two statuses the refusal
	// is returned as what it means - this peer cannot host this terminal - and the run
	// stays non-terminal where the caller can see it. Both rules apply to every optional
	// key, failure_cause included.
	if closeReason != "" {
		lease["close_reason"] = closeReason
	}
	if cause != "" {
		lease["failure_cause"] = cause
	}
	for {
		err := c.call(ctx, "Release", token, lease, nil)
		if err == nil {
			return nil
		}
		dropped, refused := refusedOptionalKey(err)
		if !refused {
			return err
		}
		if endRequiresStatedReason(status) {
			// The peer will not take the reason, and this status may not travel without
			// one. Say which of the two contracts the deployment is violating instead of
			// silently producing the unexplained terminal the door just made impossible.
			return contracts.NewError(contracts.ErrUnavailable,
				"space: State at "+c.baseURL+" does not accept the terminal reason key "+dropped+
					", and a "+status+" run may not end without a stated reason (DEF-RELEASE-REASON-1): "+err.Error())
		}
		slog.Warn("space: State does not know this optional terminal key; a done run may still land without it",
			"run_id", runID, "status", status, "key", dropped, "err", err.Error())
		delete(lease, dropped)
	}
}

// endRequiresStatedReason mirrors, on the sending side, the set internal/state's
// runEndReasonPolicy enforces on the receiving side: failed and canceled. It is a
// mirror, not a second source of authority - the door still decides - and it exists so
// this client never proposes a wire form the door has already refused to accept.
func endRequiresStatedReason(status string) bool {
	return status == "failed" || status == "canceled"
}

// refusedOptionalKey names the ONE class of refusal a sender may work around: a strict
// decoder saying it does not know one of the optional release keys, by name. The name is
// echoed back so only the refused key is dropped - a peer that knows close_reason but not
// failure_cause loses exactly one field, not the audit vocabulary too.
func refusedOptionalKey(err error) (string, bool) {
	for _, key := range []string{"close_reason", "failure_cause"} {
		if err != nil && strings.Contains(err.Error(), `unknown field "`+key+`"`) {
			return key, true
		}
	}
	return "", false
}

func (c *StateClient) Suspend(ctx context.Context, token, runID, worker string, epoch int64, waitFor string, deadline *time.Time) error {
	body := map[string]any{
		"run_id":   runID,
		"worker":   worker,
		"epoch":    epoch,
		"wait_for": waitFor,
	}
	if deadline != nil {
		body["deadline"] = deadline
	}
	return c.call(ctx, "Suspend", token, body, nil)
}

// Intent operations

func (c *StateClient) RecordIntent(ctx context.Context, token, runID, worker string, epoch int64, node, call, kind, target string, args json.RawMessage) (*intentResponse, error) {
	var out intentResponse
	body := map[string]any{
		"fence":  Fence{RunID: runID, Worker: worker, Epoch: epoch},
		"node":   node,
		"call":   call,
		"kind":   kind,
		"target": target,
	}
	if len(args) > 0 {
		body["args"] = args
	}
	err := c.call(ctx, "RecordIntent", token, body, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// UpdateIntent advances an intent's lifecycle. The intent is addressed by its
// (run_id, node, call) coordinates — the published State contract keys intents
// that way, not by an opaque intent id.
func (c *StateClient) UpdateIntent(ctx context.Context, token, runID, worker string, epoch int64, node, call, status string, result json.RawMessage) (*intentResponse, error) {
	body := map[string]any{
		"fence":  Fence{RunID: runID, Worker: worker, Epoch: epoch},
		"node":   node,
		"call":   call,
		"status": status,
	}
	if len(result) > 0 {
		body["result"] = result
	}
	var out intentResponse
	if err := c.call(ctx, "UpdateIntent", token, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetIntent reads an intent by its (run_id, node, call) coordinates. The
// published State GetIntent request carries no fence -- it is a read, so it is
// authorized purely by the task token's scope.
func (c *StateClient) GetIntent(ctx context.Context, token, runID, node, call string) (*intentResponse, error) {
	var out intentResponse
	body := map[string]any{"run_id": runID, "node": node, "call": call}
	if err := c.call(ctx, "GetIntent", token, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ReconcileIntent is the privileged transition that moves an intent parked in
// UNKNOWN to succeeded or failed. It is the ONLY legal exit from UNKNOWN and
// requires the current lease plus the exact original fingerprint (digest), so a
// reconciler cannot overwrite an intent it did not observe. The body mirrors
// the State ReconcileIntent wire shape: {fence, node, call, status, digest,
// result}.
func (c *StateClient) ReconcileIntent(ctx context.Context, token string, f Fence, node, call, status, digest string, result json.RawMessage) (*intentResponse, error) {
	var out intentResponse
	body := map[string]any{
		"fence":  f,
		"node":   node,
		"call":   call,
		"status": status,
		"digest": digest,
	}
	if len(result) > 0 {
		body["result"] = result
	}
	if err := c.call(ctx, "ReconcileIntent", token, body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Checkpoint operations

// PutCheckpoint writes a checkpoint under the given fence. The State service
// derives the thread from the run lease (never from the body), validates the
// fence (a stale epoch is rejected with STALE_EPOCH) and applies the write
// idempotently by (thread, namespace, key). The checkpoint keeps its own id,
// parent chain, version and part-versions in Space's namespace.
func (c *StateClient) PutCheckpoint(ctx context.Context, token string, f Fence, namespace, id, parent, key string, version int64, metadata json.RawMessage, parts []Part) (*checkpointResponse, error) {
	var out checkpointResponse
	cp := map[string]any{
		"id":        id,
		"namespace": namespace,
		"version":   version,
	}
	if parent != "" {
		cp["parent"] = parent
	}
	if len(metadata) > 0 {
		cp["metadata"] = metadata
	}
	if len(parts) > 0 {
		cp["parts"] = parts
	}
	err := c.call(ctx, "PutCheckpoint", token, map[string]any{
		"fence":      f,
		"checkpoint": cp,
		"key":        key,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// ListCheckpoints returns the most recent checkpoints for a thread within a
// namespace, newest-first. The published State contract has no single
// "load latest" op; cold resume asks for limit=1 and takes the head of this
// list. Thread scope is enforced server-side before ordering.
func (c *StateClient) ListCheckpoints(ctx context.Context, token, threadID, namespace string, limit int) ([]checkpointResponse, error) {
	var out struct {
		Checkpoints []checkpointResponse `json:"checkpoints"`
	}
	err := c.call(ctx, "ListCheckpoints", token, map[string]any{
		"thread_id": threadID,
		"namespace": namespace,
		"limit":     limit,
	}, &out)
	if err != nil {
		return nil, err
	}
	return out.Checkpoints, nil
}

// ListThreads returns one durable page of the caller's own threads, newest-first,
// with keyset pagination. Isolation is enforced by the State server from the
// presenting task-token principal; no scope/subject appears in the body. This is
// the cross-thread chat list the browser needs for the as-workspace view (PDF §3.2
// Spaces/chats enumeration). The mock used in offline tests is unscoped, so this
// proves the wire shape offline; subject isolation is proven on the real State.
func (c *StateClient) ListThreads(ctx context.Context, token, cursor string, limit int) ([]threadResponse, string, bool, error) {
	var out struct {
		Threads    []threadResponse `json:"threads"`
		NextCursor string           `json:"next_cursor"`
		HasMore    bool             `json:"has_more"`
	}
	err := c.call(ctx, "ListThreads", token, map[string]any{"cursor": cursor, "limit": limit}, &out)
	if err != nil {
		return nil, "", false, err
	}
	return out.Threads, out.NextCursor, out.HasMore, nil
}

// ListRuns returns the durable runs of a thread in FIFO (sequence) order via the
// accepted State op ListRuns. scope is enforced by the State server from the
// presenting task-token principal, never from the body. This is the durable run
// enumeration the coordinator needs for restart-safe turn/history views (PDF §3.4
// reseed history); it reads State, not the coordinator's in-memory turn table.
func (c *StateClient) ListRuns(ctx context.Context, token, threadID string) ([]runResponse, error) {
	var out struct {
		Runs []runResponse `json:"runs"`
	}
	err := c.call(ctx, "ListRuns", token, map[string]any{"thread_id": threadID}, &out)
	if err != nil {
		return nil, err
	}
	return out.Runs, nil
}

// Inbox operations

// Enqueue appends a durable inbox event to a thread. Enqueue carries no lease
// fence: the published State contract treats it as an append keyed by
// (thread, key) and applies it idempotently by payload digest.
func (c *StateClient) Enqueue(ctx context.Context, token, threadID, key, kind string, payload json.RawMessage) (*inboxResponse, error) {
	var out inboxResponse
	err := c.call(ctx, "Enqueue", token, map[string]any{
		"thread_id": threadID,
		"key":       key,
		"kind":      kind,
		"payload":   payload,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *StateClient) ReadInbox(ctx context.Context, token, threadID string, after int64, limit int) ([]inboxEvent, error) {
	var out struct {
		Events []inboxEvent `json:"events"`
	}
	err := c.call(ctx, "ReadInbox", token, map[string]any{
		"thread_id": threadID,
		"after":     after,
		"limit":     limit,
	}, &out)
	if err != nil {
		return nil, err
	}
	return out.Events, nil
}

func (c *StateClient) AckInbox(ctx context.Context, token, runID, worker string, epoch int64, ids []string) error {
	return c.call(ctx, "AckInbox", token, map[string]any{
		"fence": Fence{RunID: runID, Worker: worker, Epoch: epoch},
		"ids":   ids,
	}, nil)
}

// Stream operations

func (c *StateClient) AppendStream(ctx context.Context, token, threadID, runID, worker string, epoch int64, key, kind string, payload json.RawMessage) (*streamEventResponse, error) {
	var out streamEventResponse
	_ = threadID // thread is derived from the run lease, never from the body
	err := c.call(ctx, "AppendStream", token, map[string]any{
		"fence":   Fence{RunID: runID, Worker: worker, Epoch: epoch},
		"key":     key,
		"kind":    kind,
		"payload": payload,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// FlushStream persists the run's experimental volatile stream tail at a step or
// turn boundary in one transaction (a no-op server-side in the default chunk
// mode). A STALE_EPOCH or other refusal is returned so the runtime must not
// acknowledge a terminal past a failed flush.
func (c *StateClient) FlushStream(ctx context.Context, token, runID, worker string, epoch int64) ([]streamEventResponse, error) {
	var out struct {
		Events []streamEventResponse `json:"events"`
	}
	if err := c.call(ctx, "FlushStream", token, map[string]any{
		"fence": Fence{RunID: runID, Worker: worker, Epoch: epoch},
	}, &out); err != nil {
		return nil, err
	}
	return out.Events, nil
}

func (c *StateClient) ReadStream(ctx context.Context, token, threadID string, after int64, limit int) ([]streamEventResponse, error) {
	var out struct {
		Events []streamEventResponse `json:"events"`
	}
	err := c.call(ctx, "ReadStream", token, map[string]any{
		"thread_id": threadID,
		"after":     after,
		"limit":     limit,
	}, &out)
	if err != nil {
		return nil, err
	}
	return out.Events, nil
}

// ReadStreamPage opts into State's explicit count AND full-response byte bound.
// Legacy count readers stay unchanged; SSE must follow HasMore rather than
// infer EOF from the length of a byte-limited page.
func (c *StateClient) ReadStreamPage(ctx context.Context, token, threadID string, after int64, limit, maxBytes int) ([]streamEventResponse, bool, error) {
	return c.readStreamPage(ctx, token, threadID, after, limit, maxBytes, false)
}

func (c *StateClient) readRecoveryStreamPage(ctx context.Context, token, threadID string, after int64, limit, maxBytes int) ([]streamEventResponse, bool, error) {
	return c.readStreamPage(ctx, token, threadID, after, limit, maxBytes, true)
}

func (c *StateClient) readStreamPage(ctx context.Context, token, threadID string, after int64, limit, maxBytes int, durableOnly bool) ([]streamEventResponse, bool, error) {
	var out struct {
		Events    []streamEventResponse `json:"events"`
		HasMore   *bool                 `json:"has_more"`
		NextAfter *int64                `json:"next_after"`
	}
	body := map[string]any{"thread_id": threadID, "after": after, "limit": limit, "max_response_bytes": maxBytes}
	if durableOnly {
		body["durable_only"] = true
	}
	err := c.call(ctx, "ReadStream", token, body, &out)
	if err != nil {
		return nil, false, err
	}
	if out.HasMore == nil || out.NextAfter == nil || len(out.Events) > limit || (len(out.Events) == 0 && *out.HasMore) {
		return nil, false, contracts.NewError(contracts.ErrInvalid, "stateclient: invalid bounded stream page")
	}
	cursor := after
	for _, ev := range out.Events {
		if ev.Sequence <= cursor || ev.ThreadID != threadID {
			return nil, false, contracts.NewError(contracts.ErrInvalid, "stateclient: invalid bounded stream cursor")
		}
		cursor = ev.Sequence
	}
	if *out.NextAfter != cursor {
		return nil, false, contracts.NewError(contracts.ErrInvalid, "stateclient: inconsistent bounded stream cursor")
	}
	return out.Events, *out.HasMore, nil
}

// KV operations

// PutKV writes a namespace-scoped KV entry, fenced by the run lease and
// guarded by an operation key and expected version for idempotent, conflict-safe
// updates (the published State contract).
func (c *StateClient) PutKV(ctx context.Context, token string, f Fence, namespace []string, key, operationKey string, expectedVersion int64, value json.RawMessage) (*kvItem, error) {
	var out kvItem
	err := c.call(ctx, "PutKV", token, map[string]any{
		"fence":            f,
		"namespace":        namespace,
		"key":              key,
		"operation_key":    operationKey,
		"expected_version": expectedVersion,
		"value":            value,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// GetKV reads a namespace-scoped KV entry.
func (c *StateClient) GetKV(ctx context.Context, token string, namespace []string, key string) (*kvItem, error) {
	var out kvItem
	err := c.call(ctx, "GetKV", token, map[string]any{
		"namespace": namespace,
		"key":       key,
	}, &out)
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// Response shapes matching the State API wire format.

type threadResponse struct {
	ID        string    `json:"id"`
	ContextID string    `json:"context_id"`
	Epoch     int64     `json:"epoch"`
	CreatedAt time.Time `json:"created_at"`
}

type runResponse struct {
	TraceParent string `json:"traceparent,omitempty"`
	ID          string `json:"id"`
	ThreadID    string `json:"thread_id"`
	TaskID      string `json:"task_id"`
	Status      string `json:"status"`
	// WaitFor names what a parked run is waiting for. It is the only evidence a cold read has
	// that State's `waiting` is a HITL gate rather than some other wait, and the real State Run
	// contract publishes it (internal/state Run.wait_for); it is empty for every run that is not
	// parked, so no live status depends on it.
	WaitFor  string          `json:"wait_for,omitempty"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

// runStatusWaiting is State's word for a parked run; hitlWaitFor is the wait target SubmitHITL
// parks it on. Both belong to State's wire vocabulary (internal/state RunWaiting, the wait_for
// argument of Suspend). Space reads them off HTTP and names them here rather than importing the
// State server package for two strings, the way liveRunStatuses already restates run states.
const (
	runStatusWaiting = "waiting"
	hitlWaitFor      = "hitl_response"
)

type claimResponse struct {
	Lease     *leaseResponse `json:"lease"`
	TaskToken string         `json:"task_token"`
}

type leaseResponse struct {
	Thread    threadResponse  `json:"thread"`
	Run       runResponse     `json:"run"`
	Scope     contracts.Scope `json:"scope"`
	Worker    string          `json:"worker"`
	Epoch     int64           `json:"epoch"`
	ExpiresAt time.Time       `json:"expires_at"`
}

type intentResponse struct {
	ID     string          `json:"id"`
	RunID  string          `json:"run_id"`
	Node   string          `json:"node"`
	Call   string          `json:"call"`
	Kind   string          `json:"kind"`
	Target string          `json:"target"`
	Digest string          `json:"digest"`
	Status string          `json:"status"`
	Result json.RawMessage `json:"result,omitempty"`
}

type checkpointResponse struct {
	ID        string          `json:"id"`
	ThreadID  string          `json:"thread_id"`
	Namespace string          `json:"namespace"`
	Parent    string          `json:"parent,omitempty"`
	Version   int64           `json:"version"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
	Parts     []Part          `json:"parts,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// kvItem is the published State KV response shape.
type kvItem struct {
	Namespace []string        `json:"namespace"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   int64           `json:"version"`
	ThreadID  string          `json:"thread_id,omitempty"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type inboxResponse struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
	Acked    bool            `json:"acked"`
}

type inboxEvent struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
	Acked    bool            `json:"acked"`
}

type streamEventResponse struct {
	ID       string          `json:"id"`
	ThreadID string          `json:"thread_id"`
	Key      string          `json:"key"`
	Kind     string          `json:"kind"`
	Sequence int64           `json:"sequence"`
	Payload  json.RawMessage `json:"payload"`
}

// CancelWaitingRun closes an unleased run under a conversation-bound caller token.
func (c *StateClient) CancelWaitingRun(ctx context.Context, token, runID string) error {
	return c.call(ctx, "CancelWaitingRun", token, map[string]string{"run_id": runID}, nil)
}
