package state

import (
	"context"
	"encoding/json"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// KVItem is a versioned key-value entry in a namespace.
type KVItem struct {
	Namespace []string        `json:"namespace"`
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	Version   int64           `json:"version"`
	ThreadID  string          `json:"thread_id,omitempty"`
	UpdatedAt string          `json:"updated_at"`
}

// GetKV reads one KV item by namespace and key.
func (c *Client) GetKV(ctx context.Context, namespace []string, key string) (KVItem, error) {
	if key == "" {
		return KVItem{}, newInvalid("key must not be empty")
	}
	var out KVItem
	req := struct {
		Namespace []string `json:"namespace"`
		Key       string   `json:"key"`
	}{namespace, key}
	if err := c.callTask(ctx, "GetKV", req, &out); err != nil {
		return KVItem{}, err
	}
	return out, nil
}

// PutKV writes a KV item under the current lease.
func (c *Client) PutKV(ctx context.Context, f Fence, namespace []string, key, operationKey string, expectedVersion int64, value json.RawMessage) (KVItem, error) {
	if err := validateFence(f); err != nil {
		return KVItem{}, err
	}
	if key == "" {
		return KVItem{}, newInvalid("key must not be empty")
	}
	if operationKey == "" {
		return KVItem{}, newInvalid("operation_key must not be empty")
	}
	var out KVItem
	req := struct {
		Fence           fenceDTO        `json:"fence"`
		Namespace       []string        `json:"namespace"`
		Key             string          `json:"key"`
		OperationKey    string          `json:"operation_key"`
		ExpectedVersion int64           `json:"expected_version,omitempty"`
		Value           json.RawMessage `json:"value"`
	}{toFenceDTO(f), namespace, key, operationKey, expectedVersion, value}
	if err := c.callTask(ctx, "PutKV", req, &out); err != nil {
		return KVItem{}, err
	}
	return out, nil
}

// DeleteKV removes a KV item under the current lease.
func (c *Client) DeleteKV(ctx context.Context, f Fence, namespace []string, key, operationKey string, expectedVersion int64) error {
	if err := validateFence(f); err != nil {
		return err
	}
	if key == "" {
		return newInvalid("key must not be empty")
	}
	var out okResponse
	req := struct {
		Fence           fenceDTO `json:"fence"`
		Namespace       []string `json:"namespace"`
		Key             string   `json:"key"`
		OperationKey    string   `json:"operation_key"`
		ExpectedVersion int64    `json:"expected_version,omitempty"`
	}{toFenceDTO(f), namespace, key, operationKey, expectedVersion}
	if err := c.callTask(ctx, "DeleteKV", req, &out); err != nil {
		return err
	}
	if !out.OK {
		return contracts.NewError(contracts.ErrConflict, "state sdk: delete not acknowledged")
	}
	return nil
}
