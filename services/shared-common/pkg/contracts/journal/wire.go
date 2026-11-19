// Package journal defines the portable wire types for the compact State API.
package journal

import (
	"crypto/sha256"
	"encoding/json"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

type Payload struct {
	Inline []byte   `json:"inline"`
	Blob   *BlobRef `json:"blob,omitempty"`
}

type BlobRef struct {
	ArtifactID string `json:"artifact_id,omitempty"`
	Version    string `json:"version,omitempty"`
	SHA256     string `json:"sha256"`
	SizeBytes  int64  `json:"size_bytes,omitempty"`
	// Preserve legacy descriptors on reads. New writes require an owner ref.
	URI  string `json:"uri,omitempty"`
	Size int64  `json:"size,omitempty"`
}

type EventInput struct {
	ID       string  `json:"id"`
	ParentID string  `json:"parent_id,omitempty"`
	Type     string  `json:"type"`
	Role     string  `json:"role"`
	Codec    string  `json:"codec"`
	Payload  Payload `json:"payload"`
	Text     string  `json:"content_text,omitempty"`
}

type KVMutation struct {
	ScopeType       string  `json:"scope_type"`
	Namespace       string  `json:"namespace"`
	Key             string  `json:"key"`
	ExpectedVersion int64   `json:"expected_version"`
	Payload         Payload `json:"payload"`
	Delete          bool    `json:"delete,omitempty"`
}

type PreparedIntent struct {
	CallID         string       `json:"call_id"`
	ToolName       string       `json:"tool_name"`
	ArgsDigest     SHA256Digest `json:"args_digest"`
	IdempotencyKey string       `json:"idempotency_key"`
}

// SHA256Digest accepts the existing byte array and the standard JSON base64
// representation of a byte slice. Writes keep the established byte array.
type SHA256Digest [sha256.Size]byte

type AppendRequest struct {
	Key          string                 `json:"key"`
	ExpectedSeq  int64                  `json:"expected_seq"`
	Events       []EventInput           `json:"events"`
	KV           []KVMutation           `json:"kv,omitempty"`
	Intents      []PreparedIntent       `json:"intents,omitempty"`
	ToolOutcomes []IntentTransition     `json:"tool_outcomes,omitempty"`
	Pointers     *PointerUpdate         `json:"pointers,omitempty"`
	FinishTurn   string                 `json:"finish_turn,omitempty"`
	Catalog      *OpenClawCatalogUpdate `json:"catalog,omitempty"`
	EventPatches []EventPatch           `json:"event_patches,omitempty"`
}

type EventPatch struct {
	Seq       int64 `json:"seq"`
	Active    *bool `json:"is_active,omitempty"`
	Compacted *bool `json:"is_compacted,omitempty"`
}

// A nil pointer leaves the current entry unchanged. An empty string clears it.
type PointerUpdate struct {
	Leaf         *string `json:"leaf_entry_id,omitempty"`
	AppendParent *string `json:"append_parent_id,omitempty"`
}

type AppendReceipt struct {
	FirstSeq int64 `json:"first_seq"`
	HeadSeq  int64 `json:"head_seq"`
}

type Event struct {
	Seq int64 `json:"seq"`
	EventInput
	Active    bool   `json:"is_active"`
	Compacted bool   `json:"is_compacted"`
	TraceID   string `json:"otel_trace_id,omitempty"`
	SpanID    string `json:"otel_span_id,omitempty"`
}

type EventPage struct {
	Epoch        int64     `json:"lease_epoch"`
	HeadSeq      int64     `json:"head_seq"`
	Leaf         *string   `json:"leaf_entry_id,omitempty"`
	AppendParent *string   `json:"append_parent_id,omitempty"`
	Events       []Event   `json:"events"`
	State        []KVEntry `json:"state,omitempty"`
}

func (digest *SHA256Digest) UnmarshalJSON(raw []byte) error {
	var decoded []byte
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	if len(decoded) != sha256.Size {
		return contracts.NewError(contracts.ErrInvalid, "tool arguments digest must be exactly 32 bytes")
	}
	*digest = SHA256Digest(decoded)
	return nil
}

type PendingWrite struct {
	TaskID   string  `json:"task_id"`
	TaskPath string  `json:"task_path,omitempty"`
	Index    int32   `json:"index"`
	Channel  string  `json:"channel"`
	Codec    string  `json:"codec"`
	Payload  Payload `json:"payload"`
}

type Checkpoint struct {
	Namespace       string          `json:"namespace"`
	ID              string          `json:"id"`
	ParentID        string          `json:"parent_id,omitempty"`
	SnapshotSeq     int64           `json:"snapshot_seq"`
	Metadata        json.RawMessage `json:"metadata"`
	ChannelVersions json.RawMessage `json:"channel_versions"`
	Codec           string          `json:"codec"`
	Payload         Payload         `json:"payload"`
	ChatEventID     string          `json:"chat_event_id,omitempty"`
}

type CheckpointCommit struct {
	Key          string             `json:"key"`
	Checkpoint   Checkpoint         `json:"checkpoint"`
	Writes       []PendingWrite     `json:"writes,omitempty"`
	Intents      []PreparedIntent   `json:"intents,omitempty"`
	ToolOutcomes []IntentTransition `json:"tool_outcomes,omitempty"`
	Chat         *ChatDelta         `json:"chat,omitempty"`
}

type CheckpointTuple struct {
	Checkpoint Checkpoint     `json:"checkpoint"`
	Writes     []PendingWrite `json:"writes"`
}

type IntentTransition struct {
	CallID             string   `json:"call_id"`
	Status             string   `json:"status"`
	GatewayOperationID string   `json:"gateway_operation_id"`
	Result             *Payload `json:"result,omitempty"`
}

type ToolIntent struct {
	IntentTransition
	ToolName   string `json:"tool_name"`
	ArgsDigest []byte `json:"args_digest"`
}

type EnqueueRequest struct {
	SessionID     string  `json:"session_id"`
	Harness       string  `json:"harness_type"`
	FormatVersion int     `json:"format_version"`
	Key           string  `json:"key"`
	Input         Payload `json:"input"`
}

type EnqueueReceipt struct {
	SessionID string `json:"session_id"`
	HeadSeq   int64  `json:"head_seq"`
}

type TurnLease struct {
	Lease
	SessionID string  `json:"session_id"`
	Input     Payload `json:"input"`
	InputSeq  int64   `json:"input_seq"`
	HeadSeq   int64   `json:"head_seq"`
}

type TurnStatus struct {
	Status   string `json:"status"`
	Epoch    int64  `json:"lease_epoch"`
	HeadSeq  int64  `json:"head_seq"`
	InputSeq *int64 `json:"input_seq,omitempty"`
}

type Lease struct {
	Worker    string    `json:"worker"`
	Epoch     int64     `json:"epoch"`
	ExpiresAt time.Time `json:"expires_at"`
}

type SessionPage struct {
	IDs        []string `json:"session_ids"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

type KVEntry struct {
	Namespace string     `json:"namespace,omitempty"`
	ScopeType string     `json:"scope_type"`
	Key       string     `json:"key"`
	Version   int64      `json:"version"`
	Payload   Payload    `json:"payload"`
	Deleted   bool       `json:"deleted,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

type ChatMessage struct {
	ID      string          `json:"id"`
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// ChatDelta describes the changes from the native parent checkpoint. Order
// is only set when deleting IDs and appending the new IDs changes ordering.
type ChatDelta struct {
	Upserts []ChatMessage `json:"upserts,omitempty"`
	Remove  []string      `json:"remove,omitempty"`
	Order   []string      `json:"order,omitempty"`
}

type ChatHistory struct {
	CheckpointID string        `json:"checkpoint_id"`
	Messages     []ChatMessage `json:"messages"`
}

// OpenClawCatalogUpdate is committed in the same transaction as a journal
// event. Omitted display fields retain their previous values.
type OpenClawCatalogUpdate struct {
	ParentSessionID *string         `json:"parent_session_id,omitempty"`
	AgentID         string          `json:"agent_id,omitempty"`
	Key             string          `json:"native_key,omitempty"`
	Title           *string         `json:"title,omitempty"`
	Summary         json.RawMessage `json:"summary_metadata,omitempty"`
	Prompt          *string         `json:"prompt,omitempty"`
}

type CatalogPage struct {
	Sessions   []CatalogSession `json:"sessions"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type CatalogSession struct {
	ParentSessionID *string         `json:"parent_session_id,omitempty"`
	ID              string          `json:"session_id"`
	AgentID         string          `json:"agent_id,omitempty"`
	Key             string          `json:"native_key,omitempty"`
	Title           string          `json:"title,omitempty"`
	Status          string          `json:"status"`
	UpdatedAt       time.Time       `json:"updated_at"`
	CreatedAt       time.Time       `json:"created_at"`
	HeadSeq         int64           `json:"head_seq"`
	Leaf            *string         `json:"leaf_entry_id,omitempty"`
	PromptRef       string          `json:"prompt_ref,omitempty"`
	Summary         json.RawMessage `json:"summary_metadata,omitempty"`
}

// StandaloneToolRequest identifies an already committed tool intent.
type StandaloneToolRequest struct {
	CallID     string       `json:"call_id"`
	ToolName   string       `json:"tool_name"`
	ArgsDigest SHA256Digest `json:"args_digest"`
	// AckLost declares uncertainty about a previous admission response. This
	// request can close an uncertain intent but can never grant dispatch.
	AckLost bool `json:"ack_lost,omitempty"`
}

// StandaloneToolDecision permits one dispatch only. A lost response or an
// interrupted dispatch leaves the intent unresolved and must be reconciled.
type StandaloneToolDecision struct {
	IntentTransition
	CanExecute bool `json:"can_execute"`
}

// ResumeState contains the durable read pointers and interrupted tool intents.
type ResumeState struct {
	ParentSessionID *string      `json:"parent_session_id,omitempty"`
	SessionID       string       `json:"session_id"`
	Harness         string       `json:"harness_type"`
	FormatVersion   int          `json:"format_version"`
	Status          string       `json:"status"`
	HeadSeq         int64        `json:"head_seq"`
	HeadCheckpoint  *string      `json:"head_checkpoint_id,omitempty"`
	Leaf            *string      `json:"leaf_entry_id,omitempty"`
	AppendParent    *string      `json:"append_parent_id,omitempty"`
	Unresolved      []ToolIntent `json:"unresolved_intents"`
}
