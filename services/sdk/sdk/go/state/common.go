package state

import (
	"context"
	"encoding/json"
)

const CommonFormat = "ahp-common-semantic-records/research-2"

type CommonRecord struct {
	Kind       string              `json:"kind"`
	NativeID   json.RawMessage     `json:"native_id,omitempty"`
	Sequence   json.RawMessage     `json:"sequence,omitempty"`
	OwnerHint  json.RawMessage     `json:"owner_hint,omitempty"`
	Attributes json.RawMessage     `json:"attributes"`
	Relations  map[string][]string `json:"relations"`
}
type CommonDocument struct {
	Format  string                  `json:"format"`
	Profile string                  `json:"profile"`
	Root    string                  `json:"root"`
	Records map[string]CommonRecord `json:"records"`
}
type CommonRelationDelta struct {
	Source         string   `json:"source"`
	Role           string   `json:"role"`
	Mode           string   `json:"mode"`
	ExpectedLength *int     `json:"expected_length,omitempty"`
	Targets        []string `json:"targets,omitempty"`
}
type CommonDelta struct {
	Format    string                  `json:"format"`
	Profile   string                  `json:"profile"`
	Root      string                  `json:"root"`
	Put       map[string]CommonRecord `json:"put,omitempty"`
	Delete    []string                `json:"delete,omitempty"`
	Relations []CommonRelationDelta   `json:"relations,omitempty"`
}
type KVRead struct {
	Namespace     []string `json:"namespace"`
	Key           string   `json:"key"`
	ExpectedOwner string   `json:"expected_owner"`
}
type KVPut struct {
	Namespace       []string        `json:"namespace"`
	Key             string          `json:"key"`
	ExpectedOwner   string          `json:"expected_owner"`
	ExpectedVersion int64           `json:"expected_version"`
	Value           json.RawMessage `json:"value"`
}
type CheckpointArtifactReference = checkpointArtifact
type CommonArtifactReference struct {
	RecordID string                      `json:"record_id"`
	Artifact CheckpointArtifactReference `json:"artifact"`
}
type CommonCommit struct {
	Fence              Fence                     `json:"fence"`
	Namespace          string                    `json:"namespace"`
	Key                string                    `json:"key"`
	ExpectedRevision   int64                     `json:"expected_revision"`
	Document           *CommonDocument           `json:"document,omitempty"`
	Delta              *CommonDelta              `json:"delta,omitempty"`
	ArtifactReferences []CommonArtifactReference `json:"artifact_references,omitempty"`
	KV                 []KVPut                   `json:"kv,omitempty"`
	AckInbox           []string                  `json:"ack_inbox,omitempty"`
	Stream             *StreamOp                 `json:"stream,omitempty"`
	RunStatus          *RunStatusOp              `json:"run_status,omitempty"`
}
type CommonReceipt struct {
	CutID       string   `json:"cut_id"`
	RunID       string   `json:"run_id"`
	Namespace   string   `json:"namespace"`
	Revision    int64    `json:"revision"`
	Key         string   `json:"key"`
	RecordCount int      `json:"record_count"`
	CreatedAt   string   `json:"created_at"`
	KV          []KVItem `json:"kv,omitempty"`
}
type CommonRead struct {
	Document           CommonDocument            `json:"document"`
	Receipt            CommonReceipt             `json:"receipt"`
	ArtifactReferences []CommonArtifactReference `json:"artifact_references,omitempty"`
}
type CommonBundle struct {
	Common    *CommonRead    `json:"common"`
	Unchanged bool           `json:"common_unchanged,omitempty"`
	Receipt   *CommonReceipt `json:"receipt,omitempty"`
	KV        []*KVItem      `json:"kv"`
}

// CommitRecords makes exactly one authenticated request. The caller retains
// the same key/body after an unknown outcome; this client never invents retries.
func (c *Client) CommitRecords(ctx context.Context, in CommonCommit) (CommonReceipt, error) {
	var out CommonReceipt
	if e := validateFence(in.Fence); e != nil {
		return out, e
	}
	if in.Key == "" || in.ExpectedRevision < 0 || (in.Document == nil) == (in.Delta == nil) {
		return out, newInvalid("common publication requires key, nonnegative revision and exactly one document or delta")
	}
	e := c.callTask(ctx, "CommitRecords", in, &out)
	return out, e
}
func (c *Client) ReadRecords(ctx context.Context, thread, namespace string, revision int64) (CommonRead, error) {
	var out CommonRead
	if thread == "" || revision < 0 {
		return out, newInvalid("thread and nonnegative revision required")
	}
	e := c.callTask(ctx, "ReadRecords", struct {
		Thread    string `json:"thread_id"`
		Namespace string `json:"namespace"`
		Revision  int64  `json:"revision,omitempty"`
	}{thread, namespace, revision}, &out)
	return out, e
}
func (c *Client) ReadRecordsBundle(ctx context.Context, thread, namespace string, keys []KVRead, known int64) (CommonBundle, error) {
	var out CommonBundle
	if thread == "" || known < 0 || len(keys) < 1 || len(keys) > 8 {
		return out, newInvalid("thread, nonnegative cache revision and 1..8 owner keys required")
	}
	e := c.callTask(ctx, "ReadRecordsBundle", struct {
		Thread    string   `json:"thread_id"`
		Namespace string   `json:"namespace"`
		Keys      []KVRead `json:"keys"`
		Known     int64    `json:"known_revision,omitempty"`
	}{thread, namespace, keys, known}, &out)
	return out, e
}
func (c *Client) PutKVBatch(ctx context.Context, f Fence, key string, items []KVPut) ([]KVItem, error) {
	if e := validateFence(f); e != nil {
		return nil, e
	}
	if key == "" || len(items) < 1 || len(items) > 8 {
		return nil, newInvalid("batch requires operation key and 1..8 items")
	}
	var out struct {
		Items []KVItem `json:"items"`
	}
	e := c.callTask(ctx, "PutKVBatch", struct {
		Fence Fence `json:"fence"`
		Batch any   `json:"batch"`
	}{f, struct {
		Key   string  `json:"operation_key"`
		Items []KVPut `json:"items"`
	}{key, items}}, &out)
	return out.Items, e
}
func (c *Client) GetKVBatch(ctx context.Context, keys []KVRead) ([]*KVItem, error) {
	if len(keys) < 1 || len(keys) > 8 {
		return nil, newInvalid("batch requires 1..8 owner keys")
	}
	var out struct {
		Items []*KVItem `json:"items"`
	}
	e := c.callTask(ctx, "GetKVBatch", struct {
		Keys []KVRead `json:"keys"`
	}{keys}, &out)
	return out.Items, e
}
