package sdk

import (
	"context"
	"encoding/json"
	"fmt"

	sdkstate "github.com/ahp-platform/ahp/sdk/go/state"
)

// CheckpointSaver provides LangGraph-compatible checkpoint persistence over the
// AHP State API. It wraps a lease (fence) and a thread so that all writes are
// epoch-checked.
type CheckpointSaver struct {
	client   *sdkstate.Client
	fence    sdkstate.Fence
	threadID string
}

// NewCheckpointSaver creates a saver scoped to the given lease and thread.
func NewCheckpointSaver(client *sdkstate.Client, fence sdkstate.Fence, threadID string) *CheckpointSaver {
	return &CheckpointSaver{client: client, fence: fence, threadID: threadID}
}

// ThreadID returns the thread this saver writes to.
func (s *CheckpointSaver) ThreadID() string { return s.threadID }

// PutCheckpoint stores a checkpoint snapshot. The key provides idempotency.
func (s *CheckpointSaver) PutCheckpoint(ctx context.Context, namespace string, cp sdkstate.Checkpoint, key string) (sdkstate.Checkpoint, error) {
	cp.ThreadID = s.threadID
	cp.Namespace = namespace
	result, err := s.client.PutCheckpoint(ctx, s.fence, cp, key)
	if err != nil {
		return sdkstate.Checkpoint{}, fmt.Errorf("sdk: PutCheckpoint: %w", err)
	}
	return result, nil
}

// GetLatest returns the most recent checkpoint in a namespace.
func (s *CheckpointSaver) GetLatest(ctx context.Context, namespace string) (sdkstate.Checkpoint, error) {
	cp, err := s.client.GetCheckpoint(ctx, s.threadID, namespace, "")
	if err != nil {
		return sdkstate.Checkpoint{}, fmt.Errorf("sdk: GetLatest: %w", err)
	}
	return s.hydrateParts(ctx, cp)
}

// GetByID returns a specific checkpoint by id.
func (s *CheckpointSaver) GetByID(ctx context.Context, namespace, id string) (sdkstate.Checkpoint, error) {
	cp, err := s.client.GetCheckpoint(ctx, s.threadID, namespace, id)
	if err != nil {
		return sdkstate.Checkpoint{}, fmt.Errorf("sdk: GetByID: %w", err)
	}
	return s.hydrateParts(ctx, cp)
}

// List returns checkpoints newest-first, limited by count.
func (s *CheckpointSaver) List(ctx context.Context, namespace string, limit int) ([]sdkstate.Checkpoint, error) {
	cps, err := s.client.ListCheckpoints(ctx, s.threadID, namespace, limit)
	if err != nil {
		return nil, fmt.Errorf("sdk: List: %w", err)
	}
	for i, cp := range cps {
		hydrated, err := s.hydrateParts(ctx, cp)
		if err != nil {
			return nil, err
		}
		cps[i] = hydrated
	}
	return cps, nil
}

// hydrateParts resolves State's immutable channel/version references through the
// public SDK; its GetPart transparently reauthorizes artifact-backed values.
func (s *CheckpointSaver) hydrateParts(ctx context.Context, cp sdkstate.Checkpoint) (sdkstate.Checkpoint, error) {
	for i, part := range cp.Parts {
		if part.Data != nil {
			continue
		}
		data, err := s.client.GetPart(ctx, s.threadID, cp.Namespace, part.Channel, part.Version)
		if err != nil {
			return sdkstate.Checkpoint{}, fmt.Errorf("sdk: checkpoint part: %w", err)
		}
		cp.Parts[i].Data = data
	}
	return cp, nil
}

// PutWrites records pending writes for a checkpoint under the current lease.
func (s *CheckpointSaver) PutWrites(ctx context.Context, namespace, checkpointID string, writes []sdkstate.PendingWrite) error {
	if err := s.client.PutWrites(ctx, s.fence, namespace, checkpointID, writes); err != nil {
		return fmt.Errorf("sdk: PutWrites: %w", err)
	}
	return nil
}

// GetWrites returns pending writes for a checkpoint.
func (s *CheckpointSaver) GetWrites(ctx context.Context, namespace, checkpointID string) ([]sdkstate.PendingWrite, error) {
	writes, err := s.client.GetWrites(ctx, s.threadID, namespace, checkpointID)
	if err != nil {
		return nil, fmt.Errorf("sdk: GetWrites: %w", err)
	}
	return writes, nil
}

// ---------------------------------------------------------------------------
// AHPStore: KV state replacement for LangGraph's built-in store.
// ---------------------------------------------------------------------------

// AHPStore provides versioned key-value storage scoped to a thread and lease.
type AHPStore struct {
	client *sdkstate.Client
	fence  sdkstate.Fence
}

// NewAHPStore creates a KV store scoped to the given lease.
func NewAHPStore(client *sdkstate.Client, fence sdkstate.Fence) *AHPStore {
	return &AHPStore{client: client, fence: fence}
}

// Get reads one key.
func (s *AHPStore) Get(ctx context.Context, namespace []string, key string) (sdkstate.KVItem, error) {
	item, err := s.client.GetKV(ctx, namespace, key)
	if err != nil {
		return sdkstate.KVItem{}, fmt.Errorf("sdk: KV Get: %w", err)
	}
	return item, nil
}

// Put writes a key with an operation key for idempotency.
func (s *AHPStore) Put(ctx context.Context, namespace []string, key, operationKey string, expectedVersion int64, value json.RawMessage) (sdkstate.KVItem, error) {
	item, err := s.client.PutKV(ctx, s.fence, namespace, key, operationKey, expectedVersion, value)
	if err != nil {
		return sdkstate.KVItem{}, fmt.Errorf("sdk: KV Put: %w", err)
	}
	return item, nil
}

// Delete removes a key.
func (s *AHPStore) Delete(ctx context.Context, namespace []string, key, operationKey string, expectedVersion int64) error {
	if err := s.client.DeleteKV(ctx, s.fence, namespace, key, operationKey, expectedVersion); err != nil {
		return fmt.Errorf("sdk: KV Delete: %w", err)
	}
	return nil
}
