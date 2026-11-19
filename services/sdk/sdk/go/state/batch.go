package state

import (
	"encoding/json"
	"time"
)

// StreamOp describes one stream append within a batch.
type StreamOp struct {
	Key     string          `json:"key"`
	Kind    string          `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// RunStatusOp describes a run status transition within a batch.
type RunStatusOp struct {
	Status       string     `json:"status"`
	CloseReason  string     `json:"close_reason,omitempty"`
	FailureCause string     `json:"failure_cause,omitempty"`
	WaitFor      string     `json:"wait_for,omitempty"`
	Deadline     *time.Time `json:"deadline,omitempty"`
}

// BatchRequest is an atomic batch: checkpoint + writes + stream + run status.
type BatchRequest struct {
	Checkpoint *Checkpoint    `json:"checkpoint,omitempty"`
	Key        string         `json:"key"`
	Stream     *StreamOp      `json:"stream,omitempty"`
	RunStatus  *RunStatusOp   `json:"run_status,omitempty"`
	Writes     []PendingWrite `json:"writes,omitempty"`
	WritesNs   string         `json:"writes_namespace,omitempty"`
}

// BatchResult is returned when the batch was committed.
type BatchResult struct {
	Checkpoint  *Checkpoint  `json:"checkpoint,omitempty"`
	StreamEvent *StreamEvent `json:"stream_event,omitempty"`
	OK          bool         `json:"ok"`
}

// ControlExport is the serialized thread representation for export/import.
type ControlExport struct {
	Thread      Thread           `json:"thread"`
	Runs        []Run            `json:"runs"`
	Checkpoints []Checkpoint     `json:"checkpoints,omitempty"`
	Writes      []ExportedWrites `json:"writes,omitempty"`
	Stream      []StreamEvent    `json:"stream,omitempty"`
	Inbox       []InboxEvent     `json:"inbox,omitempty"`
	ExportedAt  time.Time        `json:"exported_at"`
}

// ExportedWrites carries the pending writes of one checkpoint, addressed by
// (namespace, checkpoint id) because public ids are unique only within a
// (thread, namespace).
type ExportedWrites struct {
	Namespace    string         `json:"namespace"`
	CheckpointID string         `json:"checkpoint_id"`
	Writes       []PendingWrite `json:"writes"`
}

// Part is a checkpoint part reference (channel + version + optional inline data).
// Alias for CheckpointPart to match the server's naming.
type Part = CheckpointPart

// putBatchRequest is the wire body for PutBatch.
type putBatchRequest struct {
	Fence fenceDTO     `json:"fence"`
	Batch BatchRequest `json:"batch"`
}
