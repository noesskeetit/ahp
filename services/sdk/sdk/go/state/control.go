package state

import (
	"context"
	"encoding/json"
)

// ListThreads returns one page of threads for the caller's scope.
// Wire contract: request {"cursor","limit"}, response {"threads","next_cursor","has_more"}.
func (c *Client) ListThreads(ctx context.Context, cursor string, limit int) ([]Thread, string, bool, error) {
	var out struct {
		Threads    []Thread `json:"threads"`
		NextCursor string   `json:"next_cursor"`
		HasMore    bool     `json:"has_more"`
	}
	req := struct {
		Cursor string `json:"cursor"`
		Limit  int    `json:"limit"`
	}{cursor, limit}
	err := c.callTask(ctx, "ListThreads", req, &out)
	return out.Threads, out.NextCursor, out.HasMore, err
}

// GetPart retrieves the raw data of a single checkpoint part.
// Server contract: request (thread_id, namespace, channel, version), response {"data": []byte, "encoding"?: string}.
func (c *Client) GetPart(ctx context.Context, threadID, namespace, channel, version string) ([]byte, error) {
	if channel == "" {
		return nil, newInvalid("channel must not be empty")
	}
	if version == "" {
		return nil, newInvalid("version must not be empty")
	}
	var out struct {
		Data     []byte `json:"data"`
		Encoding string `json:"encoding,omitempty"`
	}
	req := struct {
		ThreadID  string `json:"thread_id"`
		Namespace string `json:"namespace"`
		Channel   string `json:"channel"`
		Version   string `json:"version"`
	}{threadID, namespace, channel, version}
	err := c.callTask(ctx, "GetPart", req, &out)
	if err != nil {
		return nil, err
	}
	return c.hydrate(ctx, out.Data, out.Encoding)
}

// SearchKVResult holds the result of a SearchKV operation.
type SearchKVResult struct {
	Items []KVItem `json:"items"`
}

// SearchKV searches KV entries by namespace prefix.
func (c *Client) SearchKV(ctx context.Context, namespacePrefix []string, keyPrefix string, filter map[string]any, limit int) (SearchKVResult, error) {
	var out SearchKVResult
	req := struct {
		NamespacePrefix []string       `json:"namespace_prefix,omitempty"`
		KeyPrefix       string         `json:"key_prefix,omitempty"`
		Filter          map[string]any `json:"filter,omitempty"`
		Limit           int            `json:"limit,omitempty"`
	}{namespacePrefix, keyPrefix, filter, limit}
	err := c.callTask(ctx, "SearchKV", req, &out)
	return out, err
}

// GetIntent reads a single intent by run_id+node+call.
func (c *Client) GetIntent(ctx context.Context, runID, node, call string) (Intent, error) {
	var out Intent
	req := struct {
		RunID string `json:"run_id"`
		Node  string `json:"node"`
		Call  string `json:"call"`
	}{runID, node, call}
	err := c.callTask(ctx, "GetIntent", req, &out)
	return out, err
}

// ReconcileIntent reconciles a potentially incomplete intent after crash recovery.
func (c *Client) ReconcileIntent(ctx context.Context, f Fence, node, call, status, digest string, result json.RawMessage) (Intent, error) {
	if err := validateFence(f); err != nil {
		return Intent{}, err
	}
	var out Intent
	req := struct {
		Fence  fenceDTO        `json:"fence"`
		Node   string          `json:"node"`
		Call   string          `json:"call"`
		Status string          `json:"status"`
		Digest string          `json:"digest"`
		Result json.RawMessage `json:"result,omitempty"`
	}{toFenceDTO(f), node, call, status, digest, result}
	err := c.callTask(ctx, "ReconcileIntent", req, &out)
	return out, err
}

// PutBatch submits multiple mutations in one atomic call.
func (c *Client) PutBatch(ctx context.Context, f Fence, batch BatchRequest) (BatchResult, error) {
	if err := validateFence(f); err != nil {
		return BatchResult{}, err
	}
	if batch.Key == "" {
		return BatchResult{}, newInvalid("batch key must not be empty")
	}
	if batch.Checkpoint != nil {
		cp, err := c.spillCheckpointBudget(ctx, f.RunID, *batch.Checkpoint, inlineBatchBytes/2)
		if err != nil {
			return BatchResult{}, err
		}
		batch.Checkpoint = &cp
	}
	writes, err := c.spillWritesBudget(ctx, f.RunID, batch.Writes, inlineBatchBytes/2)
	if err != nil {
		return BatchResult{}, err
	}
	batch.Writes = writes
	var out BatchResult
	err = c.callTask(ctx, "PutBatch", putBatchRequest{Fence: toFenceDTO(f), Batch: batch}, &out)
	if err == nil && out.Checkpoint != nil {
		cp, e := c.hydrateCheckpoint(ctx, *out.Checkpoint)
		if e != nil {
			return BatchResult{}, e
		}
		out.Checkpoint = &cp
	}
	return out, err
}

// DeleteSubject deletes all data for the caller's subject. Returns the number of rows deleted.
func (c *Client) DeleteSubject(ctx context.Context) (int64, error) {
	var out struct {
		Deleted int64 `json:"deleted"`
	}
	err := c.callTask(ctx, "DeleteSubject", struct{}{}, &out)
	return out.Deleted, err
}

// SetLegalhold prevents deletion of a thread.
func (c *Client) SetLegalhold(ctx context.Context, threadID, reason string) error {
	if threadID == "" {
		return newInvalid("thread_id must not be empty")
	}
	var out okResponse
	req := struct {
		ThreadID string `json:"thread_id"`
		Reason   string `json:"reason,omitempty"`
	}{threadID, reason}
	return c.callTask(ctx, "SetLegalhold", req, &out)
}

// RemoveLegalhold removes the legal hold from a thread.
func (c *Client) RemoveLegalhold(ctx context.Context, threadID string) error {
	var out okResponse
	req := struct {
		ThreadID string `json:"thread_id"`
	}{threadID}
	return c.callTask(ctx, "RemoveLegalhold", req, &out)
}

// ExportThread retains the legacy task route, which the server refuses.
// Deprecated: use NewAdmin(...).ExportThread with explicit human authority.
func (c *Client) ExportThread(ctx context.Context, threadID string) (ControlExport, error) {
	var out ControlExport
	req := struct {
		ThreadID string `json:"thread_id"`
	}{threadID}
	err := c.callTask(ctx, "ExportThread", req, &out)
	return out, err
}

// ImportThread imports a thread snapshot. Returns the imported thread.
func (c *Client) ImportThread(ctx context.Context, export ControlExport) (Thread, error) {
	var out Thread
	req := struct {
		Export ControlExport `json:"export"`
	}{export}
	err := c.callTask(ctx, "ImportThread", req, &out)
	return out, err
}
