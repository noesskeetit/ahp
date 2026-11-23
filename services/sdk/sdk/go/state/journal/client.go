// Package journal is the Go client for the compact AHP State API.
package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
)

const sessions = "/v1/state/sessions"

const inlinePayloadLimit = 256 << 10

type Config struct {
	BaseURL     string
	HTTPClient  *http.Client
	TokenSource func(context.Context) (string, error)
}

// Client obtains the current task token for every call. It never retries a
// mutating request. After a transport failure, reconcile the same stable
// operation key before starting another action.
type Client struct {
	http *clienthttp.Client
}

func New(config Config) (*Client, error) {
	transport, err := clienthttp.New(clienthttp.Config{
		BaseURL: config.BaseURL, HTTPClient: config.HTTPClient, TokenSource: config.TokenSource,
	})
	if err != nil {
		return nil, err
	}
	return &Client{http: transport}, nil
}

type SessionLease struct {
	Control *TurnControlDelta `json:"control,omitempty"`
	Lease
	ResumeState
	ToolLeaseProof string `json:"tool_lease_proof,omitempty"`
}

type ClaimedTurn struct {
	TurnLease
	ToolLeaseProof string `json:"tool_lease_proof,omitempty"`
}

func (c *Client) AcquireSession(ctx context.Context, session, harness string, version int, ttl time.Duration) (SessionLease, error) {
	var lease SessionLease
	request := struct {
		Session string `json:"session_id"`
		Harness string `json:"harness_type"`
		Version int    `json:"format_version"`
		TTL     int64  `json:"ttl_ms"`
	}{session, harness, version, ttl.Milliseconds()}
	err := c.http.Call(ctx, sessions+":acquire", request, &lease, true)
	return lease, err
}

func (c *Client) HeartbeatLease(ctx context.Context, session string, epoch int64, ttl time.Duration) (SessionLease, error) {
	var lease SessionLease
	err := c.http.Call(ctx, sessionPath(session)+":heartbeat", leaseChange{Epoch: epoch, TTL: ttl.Milliseconds()}, &lease, true)
	return lease, err
}

func (c *Client) ReleaseLease(ctx context.Context, session string, epoch int64) error {
	return c.ReleaseLeaseTo(ctx, session, epoch, "")
}

// ReleaseLeaseTo releases the lease with status IDLE or WAITING_INPUT.
func (c *Client) ReleaseLeaseTo(ctx context.Context, session string, epoch int64, status string) error {
	return c.http.Call(ctx, sessionPath(session)+":heartbeat",
		leaseChange{Epoch: epoch, Release: true, Status: status}, nil, true)
}

// RunHeartbeat renews an acquired lease every third of ttl. Use the returned
// context for lease-bound work and call stop before releasing the lease.
// A failed renewal cancels that context with the error as its cause.
func (c *Client) RunHeartbeat(ctx context.Context, session string, epoch int64, ttl time.Duration) (context.Context, context.CancelFunc) {
	return c.RunHeartbeatWithControl(ctx, session, epoch, ttl, nil)
}

func (c *Client) RunHeartbeatWithControl(ctx context.Context, session string, epoch int64, ttl time.Duration, onControl func(TurnControlDelta)) (context.Context, context.CancelFunc) {
	return c.runHeartbeat(ctx, session, epoch, ttl, func(renewed SessionLease) {
		if onControl != nil && renewed.Control != nil {
			onControl(*renewed.Control)
		}
	})
}

func (c *Client) runHeartbeat(ctx context.Context, session string, epoch int64, ttl time.Duration, onRenewal func(SessionLease)) (context.Context, context.CancelFunc) {
	leaseCtx, cancel := context.WithCancelCause(ctx)
	stop := func() { cancel(context.Canceled) }
	if ttl.Milliseconds() < 1 {
		cancel(contracts.NewError(contracts.ErrInvalid, "lease duration must be at least one millisecond"))
		return leaseCtx, stop
	}
	go func() {
		timer := time.NewTicker(ttl / 3)
		defer timer.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-timer.C:
				renewalContext, end := context.WithTimeout(leaseCtx, ttl/3)
				renewed, err := c.HeartbeatLease(renewalContext, session, epoch, ttl)
				end()
				if err != nil {
					cancel(err)
					return
				}
				if leaseCtx.Err() == nil {
					onRenewal(renewed)
				}
			}
		}
	}()
	return leaseCtx, stop
}

type leaseChange struct {
	Epoch   int64  `json:"lease_epoch"`
	TTL     int64  `json:"ttl_ms,omitempty"`
	Release bool   `json:"release,omitempty"`
	Status  string `json:"status,omitempty"`
}

func (c *Client) EnqueueTurn(ctx context.Context, input EnqueueRequest) (EnqueueReceipt, error) {
	var receipt EnqueueReceipt
	err := c.http.Call(ctx, sessions+":enqueue", input, &receipt, true)
	return receipt, err
}

func (c *Client) ClaimTurn(ctx context.Context, session string, ttl time.Duration) (ClaimedTurn, error) {
	var turn ClaimedTurn
	req := struct {
		Session string `json:"session_id,omitempty"`
		TTL     int64  `json:"ttl_ms"`
	}{session, ttl.Milliseconds()}
	err := c.http.Call(ctx, sessions+":claim-turn", req, &turn, true)
	return turn, err
}

func (c *Client) GetTurnStatus(ctx context.Context, session string) (TurnStatus, error) {
	var result TurnStatus
	err := c.get(ctx, sessionPath(session), nil, &result)
	return result, err
}

func (c *Client) InspectSession(ctx context.Context, session string) (ResumeState, error) {
	var result ResumeState
	err := c.get(ctx, sessionPath(session), url.Values{"view": {"resume"}}, &result)
	return result, err
}

func (c *Client) ListSessions(ctx context.Context, harness, cursor string, limit int) (SessionPage, error) {
	var result SessionPage
	query := url.Values{"harness": {harness}, "cursor": {cursor}, "limit": {strconv.Itoa(limit)}}
	err := c.get(ctx, sessions, query, &result)
	return result, err
}

// ListCatalog returns the selected harness's session cards, ordered by
// recent activity. Pass NextCursor as Cursor to continue the listing.
func (c *Client) ListCatalog(ctx context.Context, filter CatalogQuery) (CatalogPage, error) {
	var result CatalogPage
	query := url.Values{"view": {"catalog"}, "harness": {filter.Harness}}
	for key, value := range map[string]string{
		"app_id": filter.AppID, "agent_id": filter.AgentID, "user_id": filter.UserID,
		"status": filter.Status, "search_query": filter.Search, "cursor": filter.Cursor,
		"label": filter.Label, "spawned_by": filter.SpawnedBy,
	} {
		if value != "" {
			query.Set(key, value)
		}
	}
	if filter.Limit != 0 {
		query.Set("limit", strconv.Itoa(filter.Limit))
	}
	if filter.Offset != 0 {
		query.Set("offset", strconv.Itoa(filter.Offset))
	}
	if filter.UpdatedAfter != nil {
		query.Set("updated_after", filter.UpdatedAfter.Format(time.RFC3339Nano))
	}
	if len(filter.AgentIDs) > 0 {
		query["configured_agent_id"] = filter.AgentIDs
	}
	err := c.get(ctx, sessions, query, &result)
	return result, err
}

func (c *Client) DeleteSession(ctx context.Context, session string) error {
	return c.http.CallMethod(ctx, http.MethodDelete, sessionPath(session), nil, nil, true)
}

func (c *Client) SetLegalHold(ctx context.Context, session string, held bool) error {
	req := struct {
		Held bool `json:"held"`
	}{held}
	return c.http.CallMethod(ctx, http.MethodPatch, sessionPath(session)+"/legal-hold", req, nil, true)
}

func (c *Client) AppendEvents(ctx context.Context, session string, epoch int64, appendRequest AppendRequest) (AppendReceipt, error) {
	var receipt AppendReceipt
	input := struct {
		Epoch int64 `json:"lease_epoch"`
		AppendRequest
	}{epoch, appendRequest}
	err := c.http.Call(ctx, sessionPath(session)+"/events:append", input, &receipt, true)
	return receipt, err
}

func (c *Client) GetEvents(ctx context.Context, session string, afterSeq int64, limit int, stateNamespace string) (EventPage, error) {
	var result EventPage
	query := url.Values{"after_seq": {strconv.FormatInt(afterSeq, 10)}, "limit": {strconv.Itoa(limit)}}
	if stateNamespace != "" {
		query.Set("state_namespace", stateNamespace)
	}
	err := c.get(ctx, sessionPath(session)+"/events", query, &result)
	return result, err
}

func (c *Client) PutCheckpoint(ctx context.Context, session string, epoch int64, checkpoint CheckpointCommit) (string, error) {
	result, err := c.CommitCheckpoint(ctx, session, epoch, checkpoint)
	return result.ID, err
}

func (c *Client) CommitCheckpoint(ctx context.Context, session string, epoch int64, checkpoint CheckpointCommit) (CheckpointReceipt, error) {
	var result CheckpointReceipt
	request := struct {
		Epoch int64 `json:"lease_epoch"`
		CheckpointCommit
	}{epoch, checkpoint}
	err := c.http.Call(ctx, sessionPath(session)+"/checkpoints", request, &result, true)
	if err == nil && result.ID == "" {
		err = contracts.NewError(contracts.ErrInvalid, "missing checkpoint receipt ID")
	}
	return result, err
}

func (c *Client) GetCheckpoint(ctx context.Context, session, namespace, id string) (CheckpointTuple, error) {
	var result CheckpointTuple
	query := url.Values{"checkpoint_ns": {namespace}, "checkpoint_id": {id}}
	err := c.get(ctx, sessionPath(session)+"/checkpoints", query, &result)
	return result, err
}

func (c *Client) ListCheckpoints(ctx context.Context, session, namespace, before string, limit int) ([]string, error) {
	var result struct {
		IDs []string `json:"checkpoint_ids"`
	}
	query := url.Values{"list": {"true"}, "checkpoint_ns": {namespace}, "before": {before}, "limit": {strconv.Itoa(limit)}}
	err := c.get(ctx, sessionPath(session)+"/checkpoints", query, &result)
	return result.IDs, err
}

// ListCheckpointTuples returns one page of checkpoints and their pending
// writes. Optional metadata is a JSON object matched by containment.
func (c *Client) ListCheckpointTuples(ctx context.Context, session, namespace, before string, limit int, metadata json.RawMessage) ([]CheckpointTuple, error) {
	var result struct {
		Tuples []CheckpointTuple `json:"checkpoint_tuples"`
	}
	query := url.Values{
		"list": {"true"}, "include_tuples": {"true"}, "checkpoint_ns": {namespace}, "before": {before},
	}
	if limit != 0 {
		query.Set("limit", strconv.Itoa(limit))
	}
	if len(metadata) > 0 {
		query.Set("metadata", string(metadata))
	}
	err := c.get(ctx, sessionPath(session)+"/checkpoints", query, &result)
	return result.Tuples, err
}

func (c *Client) PutWrites(ctx context.Context, session, namespace, checkpoint string, epoch int64, writes []PendingWrite) error {
	return c.PutTurnWrites(ctx, session, namespace, checkpoint, epoch, writes, 0)
}

func (c *Client) PutTurnWrites(ctx context.Context, session, namespace, checkpoint string, epoch int64, writes []PendingWrite, turnSeq int64) error {
	request := struct {
		TurnSeq   int64          `json:"turn_seq,omitempty"`
		Epoch     int64          `json:"lease_epoch"`
		Namespace string         `json:"namespace"`
		Writes    []PendingWrite `json:"writes"`
	}{turnSeq, epoch, namespace, writes}
	return c.http.Call(ctx, sessionPath(session)+"/checkpoints/"+url.PathEscape(checkpoint)+"/writes", request, nil, true)
}

func (c *Client) GetToolIntent(ctx context.Context, session, callID string) (ToolIntent, error) {
	var result ToolIntent
	query := url.Values{"session_id": {session}, "call_id": {callID}}
	err := c.get(ctx, "/v1/state/aux:mutate", query, &result)
	return result, err
}

func (c *Client) MutateToolIntent(ctx context.Context, session string, epoch int64, transition IntentTransition) error {
	request := struct {
		Session    string           `json:"session_id"`
		Epoch      int64            `json:"lease_epoch"`
		Transition IntentTransition `json:"transition"`
	}{session, epoch, transition}
	return c.http.Call(ctx, "/v1/state/aux:mutate", request, nil, true)
}

func (c *Client) GetSharedState(ctx context.Context, namespace string) ([]KVEntry, error) {
	var result []KVEntry
	err := c.get(ctx, "/v1/state/aux:mutate", url.Values{"state_namespace": {namespace}}, &result)
	return result, err
}

func (c *Client) GetKV(ctx context.Context, session, scopeType, namespace, key string, limit int) ([]KVEntry, error) {
	var result []KVEntry
	query := url.Values{"session_id": {session}, "scope_type": {scopeType},
		"namespace": {namespace}, "key": {key}, "limit": {strconv.Itoa(limit)}}
	err := c.get(ctx, "/v1/state/kv", query, &result)
	return result, err
}

func (c *Client) MutateKV(ctx context.Context, session string, epoch, expectedSeq int64, key string, changes []KVMutation) (AppendReceipt, error) {
	return c.MutateTurnKV(ctx, session, epoch, 0, expectedSeq, key, changes)
}

// MutateTurnKV applies a KV batch within the specified active VM-managed turn.
func (c *Client) MutateTurnKV(ctx context.Context, session string, epoch, turnSeq, expectedSeq int64, key string, changes []KVMutation) (AppendReceipt, error) {
	var result AppendReceipt
	request := struct {
		Session string       `json:"session_id"`
		Epoch   int64        `json:"lease_epoch"`
		TurnSeq int64        `json:"turn_seq,omitempty"`
		Seq     int64        `json:"expected_seq"`
		Key     string       `json:"key"`
		KV      []KVMutation `json:"kv"`
	}{session, epoch, turnSeq, expectedSeq, key, changes}
	err := c.http.Call(ctx, "/v1/state/kv:mutate", request, &result, true)
	return result, err
}

func (c *Client) GetChatHistory(ctx context.Context, session, namespace, checkpoint string) (ChatHistory, error) {
	var result ChatHistory
	query := url.Values{"checkpoint_ns": {namespace}, "checkpoint_id": {checkpoint}}
	err := c.get(ctx, sessionPath(session)+"/chat-history", query, &result)
	return result, err
}

// StageBlob uploads a large value. Publish the returned reference in a
// checkpoint, event or session KV update to retain it past staging.
func (c *Client) StageBlob(ctx context.Context, session string, epoch int64, data []byte) (BlobRef, error) {
	var ref BlobRef
	input := struct {
		Epoch int64  `json:"lease_epoch"`
		Data  []byte `json:"data"`
	}{epoch, data}
	err := c.http.Call(ctx, sessionPath(session)+"/blobs:stage", input, &ref, true)
	return ref, err
}

func (c *Client) ReadBlob(ctx context.Context, session string, ref BlobRef) ([]byte, error) {
	var result struct {
		Data []byte `json:"data"`
	}
	err := c.http.Call(ctx, sessionPath(session)+"/blobs:read", ref, &result, false)
	return result.Data, err
}

// EncodePayload keeps up to 256 KiB inline and stages larger values for
// publication in a checkpoint, event, or session KV update.
func (c *Client) EncodePayload(ctx context.Context, session string, epoch int64, data []byte) (Payload, error) {
	if len(data) <= inlinePayloadLimit {
		inline := make([]byte, len(data))
		copy(inline, data)
		return Payload{Inline: inline}, nil
	}
	ref, err := c.StageBlob(ctx, session, epoch, data)
	if err != nil {
		return Payload{}, err
	}
	if !matchesBlob(data, ref) {
		return Payload{}, contracts.NewError(contracts.ErrInvalid, "staged journal blob differs from its descriptor")
	}
	return Payload{Blob: &ref}, nil
}

// DecodePayload loads a staged or inline value. Blob content is returned
// only after checking the committed byte count and SHA-256 digest.
func (c *Client) DecodePayload(ctx context.Context, session string, payload Payload) ([]byte, error) {
	if payload.Blob == nil && payload.Inline != nil {
		return payload.Inline, nil
	}
	if payload.Blob == nil || payload.Inline != nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "expected exactly one journal payload representation")
	}
	data, err := c.ReadBlob(ctx, session, *payload.Blob)
	if err != nil {
		return nil, err
	}
	if !matchesBlob(data, *payload.Blob) {
		return nil, contracts.NewError(contracts.ErrInvalid, "journal blob differs from its descriptor")
	}
	return data, nil
}

func matchesBlob(data []byte, ref BlobRef) bool {
	digest := sha256.Sum256(data)
	return int64(len(data)) == ref.SizeBytes && strings.EqualFold(ref.SHA256, hex.EncodeToString(digest[:]))
}

func (c *Client) get(ctx context.Context, path string, query url.Values, out any) error {
	if len(query) > 0 {
		path += "?" + query.Encode()
	}
	return c.http.CallMethod(ctx, http.MethodGet, path, nil, out, false)
}

func sessionPath(session string) string {
	return sessions + "/" + url.PathEscape(session)
}

// BeginStandaloneTool returns a one-time dispatch decision for a prepared
// tool. An unknown transport outcome is never permission to execute.
func (c *Client) BeginStandaloneTool(ctx context.Context, session string, epoch int64, tool StandaloneToolRequest) (StandaloneToolDecision, error) {
	var result StandaloneToolDecision
	request := struct {
		Epoch int64 `json:"lease_epoch"`
		StandaloneToolRequest
	}{epoch, tool}
	path := sessionPath(session) + "/tools:begin-standalone"
	err := c.http.Call(ctx, path, request, &result, true)
	var transportFailure *clienthttp.Error
	if !tool.AckLost && errors.As(err, &transportFailure) && transportFailure.OutcomeUnknown && ctx.Err() == nil {
		request.AckLost = true
		result = StandaloneToolDecision{}
		err = c.http.Call(ctx, path, request, &result, true)
	}
	return result, err
}
