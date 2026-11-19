package state

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// CheckpointPart is one channel's data within a checkpoint.
type CheckpointPart struct {
	Channel  string `json:"channel"`
	Version  string `json:"version"`
	Data     []byte `json:"data"`
	Encoding string `json:"encoding,omitempty"`
}

// Checkpoint is an immutable snapshot of a thread namespace.
type Checkpoint struct {
	ID               string           `json:"id"`
	ThreadID         string           `json:"thread_id,omitempty"`
	Namespace        string           `json:"namespace"`
	Parent           string           `json:"parent,omitempty"`
	Version          int64            `json:"version"`
	Metadata         json.RawMessage  `json:"metadata,omitempty"`
	MetadataEncoding string           `json:"metadata_encoding,omitempty"`
	Parts            []CheckpointPart `json:"parts,omitempty"`
	CreatedAt        string           `json:"created_at,omitempty"`
}

// PendingWrite is one partial channel result recorded before the checkpoint
// that finalises it.
type PendingWrite struct {
	Task     string `json:"task"`
	Index    int32  `json:"index"`
	Channel  string `json:"channel"`
	Data     []byte `json:"data,omitempty"`
	Encoding string `json:"encoding,omitempty"`
}

// PutCheckpoint stores a checkpoint under the current lease.
func (c *Client) PutCheckpoint(ctx context.Context, f Fence, cp Checkpoint, key string) (Checkpoint, error) {
	if err := validateFence(f); err != nil {
		return Checkpoint{}, err
	}
	wire, err := c.spillCheckpoint(ctx, f.RunID, cp)
	if err != nil {
		return Checkpoint{}, err
	}
	var out Checkpoint
	req := struct {
		Fence      fenceDTO   `json:"fence"`
		Checkpoint Checkpoint `json:"checkpoint"`
		Key        string     `json:"key"`
	}{toFenceDTO(f), wire, key}
	if err := c.callTask(ctx, "PutCheckpoint", req, &out); err != nil {
		return Checkpoint{}, err
	}
	return c.hydrateCheckpoint(ctx, out)
}

// GetCheckpoint retrieves a checkpoint. Empty id returns the latest.
func (c *Client) GetCheckpoint(ctx context.Context, threadID, namespace, id string) (Checkpoint, error) {
	var out Checkpoint
	req := struct {
		ThreadID  string `json:"thread_id"`
		Namespace string `json:"namespace"`
		ID        string `json:"id,omitempty"`
	}{threadID, namespace, id}
	if err := c.callTask(ctx, "GetCheckpoint", req, &out); err != nil {
		return Checkpoint{}, err
	}
	return c.hydrateCheckpoint(ctx, out)
}

// ListCheckpoints returns checkpoints newest-first, limited by count.
func (c *Client) ListCheckpoints(ctx context.Context, threadID, namespace string, limit int) ([]Checkpoint, error) {
	var out struct {
		Checkpoints []Checkpoint `json:"checkpoints"`
	}
	req := struct {
		ThreadID  string `json:"thread_id"`
		Namespace string `json:"namespace"`
		Limit     int    `json:"limit,omitempty"`
	}{threadID, namespace, limit}
	if err := c.callTask(ctx, "ListCheckpoints", req, &out); err != nil {
		return nil, err
	}
	for i, cp := range out.Checkpoints {
		hydrated, err := c.hydrateCheckpoint(ctx, cp)
		if err != nil {
			return nil, err
		}
		out.Checkpoints[i] = hydrated
	}
	return out.Checkpoints, nil
}

// PutWrites records pending writes for a checkpoint.
func (c *Client) PutWrites(ctx context.Context, f Fence, namespace, checkpointID string, writes []PendingWrite) error {
	if err := validateFence(f); err != nil {
		return err
	}
	wire, err := c.spillWrites(ctx, f.RunID, writes)
	if err != nil {
		return err
	}
	var out okResponse
	req := struct {
		Fence        fenceDTO       `json:"fence"`
		Namespace    string         `json:"namespace"`
		CheckpointID string         `json:"checkpoint_id"`
		Writes       []PendingWrite `json:"writes"`
	}{toFenceDTO(f), namespace, checkpointID, wire}
	if err := c.callTask(ctx, "PutWrites", req, &out); err != nil {
		return err
	}
	if !out.OK {
		return contracts.NewError(contracts.ErrConflict, "state sdk: put_writes not acknowledged")
	}
	return nil
}

// GetWrites returns pending writes for a checkpoint.
func (c *Client) GetWrites(ctx context.Context, threadID, namespace, checkpointID string) ([]PendingWrite, error) {
	var out struct {
		Writes []PendingWrite `json:"writes"`
	}
	req := struct {
		ThreadID     string `json:"thread_id"`
		Namespace    string `json:"namespace"`
		CheckpointID string `json:"checkpoint_id"`
	}{threadID, namespace, checkpointID}
	if err := c.callTask(ctx, "GetWrites", req, &out); err != nil {
		return nil, err
	}
	for i, w := range out.Writes {
		data, err := c.hydrate(ctx, w.Data, w.Encoding)
		if err != nil {
			return nil, err
		}
		out.Writes[i].Data = data
		out.Writes[i].Encoding = ""
	}
	return out.Writes, nil
}
