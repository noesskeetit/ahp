// Package sharedmemory calls the explicit PDF Memory profile. It does not merge
// records with personal/private-project Hindsight banks or grant review rights.
package sharedmemory

import (
	"context"
	"time"

	"github.com/ahp-platform/ahp/pkg/labels"
	"github.com/ahp-platform/ahp/sdk/go/internal/clienthttp"
)

const prefix = "/v1/memory/shared/"
const (
	User      = "user"
	Workspace = "workspace"
	Agent     = "agent"
)

type Config = clienthttp.Config
type Error = clienthttp.Error
type Evidence struct {
	SourceKind string   `json:"source_kind"`
	Artifacts  []string `json:"artifacts,omitempty"`
	Detail     string   `json:"detail,omitempty"`
	Taints     []string `json:"taints,omitempty"`
}
type ProposeRequest struct {
	Kind           string         `json:"kind"`
	Workspace      string         `json:"workspace,omitempty"`
	Text           string         `json:"text"`
	SourceKind     string         `json:"source_kind,omitempty"`
	Evidence       []Evidence     `json:"evidence,omitempty"`
	Supersedes     string         `json:"supersedes,omitempty"`
	Provenance     map[string]any `json:"provenance,omitempty"`
	ReviewRequired bool           `json:"review_required,omitempty"`
	TTL            *time.Time     `json:"ttl,omitempty"`
}
type Memory struct {
	ID             string         `json:"id"`
	Kind           string         `json:"kind"`
	Workspace      string         `json:"workspace,omitempty"`
	Subject        string         `json:"subject"`
	Text           string         `json:"text"`
	State          string         `json:"state"`
	OpKey          string         `json:"op_key"`
	SourceKind     string         `json:"source_kind,omitempty"`
	Evidence       []Evidence     `json:"evidence,omitempty"`
	Supersedes     string         `json:"supersedes,omitempty"`
	ReviewRequired bool           `json:"review_required,omitempty"`
	Provenance     map[string]any `json:"provenance,omitempty"`
	Labels         labels.Set     `json:"labels"`
	TTL            *time.Time     `json:"ttl,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}
type SimilarHit struct {
	ID        string  `json:"id"`
	Kind      string  `json:"kind"`
	Workspace string  `json:"workspace,omitempty"`
	State     string  `json:"state"`
	Score     float64 `json:"score"`
	Exact     bool    `json:"exact"`
}
type ProposeResult struct {
	Memory  Memory       `json:"memory"`
	Similar []SimilarHit `json:"similar,omitempty"`
	Replay  bool         `json:"replay,omitempty"`
}
type SearchRequest struct {
	Query     string `json:"query,omitempty"`
	Kind      string `json:"kind,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Workspace string `json:"workspace,omitempty"`
}
type SearchHit struct {
	Memory Memory  `json:"memory"`
	Score  float64 `json:"score"`
}
type SearchResult struct {
	Hits []SearchHit `json:"hits"`
	Mode string      `json:"mode"`
}
type ReindexResult struct {
	Indexed int  `json:"indexed"`
	Replay  bool `json:"replay,omitempty"`
}
type Operation struct {
	OpKey       string `json:"op_key"`
	Status      string `json:"status"`
	ResultID    string `json:"result_id,omitempty"`
	ResultCount int64  `json:"result_count"`
}
type Client struct{ wire *clienthttp.Client }

// New requires caller-owned workload mTLS and a current task-token source.
func New(cfg Config) (*Client, error) {
	wire, err := clienthttp.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{wire}, nil
}

// Propose persists a proposal, never an active fact. After an uncertain outcome
// explicitly replay the same body and key; the client performs one attempt.
func (c *Client) Propose(ctx context.Context, key string, in ProposeRequest) (out ProposeResult, err error) {
	if err = clienthttp.Required(key); err != nil {
		return
	}
	payload := struct {
		ProposeRequest
		OpKey string `json:"op_key"`
	}{in, key}
	err = c.wire.Call(ctx, prefix+"Propose", payload, &out, true)
	return
}
func (c *Client) Search(ctx context.Context, in SearchRequest) (out SearchResult, err error) {
	err = c.wire.Call(ctx, prefix+"Search", in, &out, false)
	return
}
func (c *Client) Read(ctx context.Context, id string) (out Memory, err error) {
	if err = clienthttp.Required(id); err != nil {
		return
	}
	err = c.wire.Call(ctx, prefix+"Read", map[string]string{"id": id}, &out, false)
	return
}

func (c *Client) ReadInWorkspace(ctx context.Context, id, workspace string) (out Memory, err error) {
	for _, value := range []string{id, workspace} {
		if err = clienthttp.Required(value); err != nil {
			return
		}
	}
	err = c.wire.Call(ctx, prefix+"Read", map[string]string{"id": id, "workspace": workspace}, &out, false)
	return
}

// GetOperation reconciles a key without returning the record's content.
func (c *Client) GetOperation(ctx context.Context, key string) (out Operation, err error) {
	if err = clienthttp.Required(key); err != nil {
		return
	}
	err = c.wire.Call(ctx, prefix+"Get", map[string]string{"op_key": key}, &out, false)
	return
}

// Fence closes a key before commit; an already committed mutation stays
// SUCCEEDED. A lost fence ACK must be reconciled by GetOperation or same-key
// Fence, never by inventing another key or erasing a subject.
func (c *Client) Fence(ctx context.Context, key string) (out Operation, err error) {
	if err = clienthttp.Required(key); err != nil {
		return
	}
	err = c.wire.Call(ctx, prefix+"Fence", map[string]string{"op_key": key}, &out, true)
	return
}

// Review still requires the owner's memory:review authority. Constructing an SDK
// client or holding memory capability alone does not confer that authority.
func (c *Client) Review(ctx context.Context, key, id, decision string) (out Memory, err error) {
	for _, value := range []string{key, id, decision} {
		if err = clienthttp.Required(value); err != nil {
			return
		}
	}
	err = c.wire.Call(ctx, prefix+"Review", map[string]string{"op_key": key, "id": id, "decision": decision}, &out, true)
	return
}
func (c *Client) AddLabels(ctx context.Context, key, id string, set labels.Set) (out Memory, err error) {
	for _, value := range []string{key, id} {
		if err = clienthttp.Required(value); err != nil {
			return
		}
	}
	err = c.wire.Call(ctx, prefix+"AddLabels", struct {
		OpKey  string     `json:"op_key"`
		ID     string     `json:"id"`
		Labels labels.Set `json:"labels"`
	}{key, id, set}, &out, true)
	return
}
func (c *Client) Delete(ctx context.Context, key, id string) error {
	for _, value := range []string{key, id} {
		if err := clienthttp.Required(value); err != nil {
			return err
		}
	}
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.wire.Call(ctx, prefix+"Delete", map[string]string{"op_key": key, "id": id}, &out, true); err != nil {
		return err
	}
	if !out.OK {
		return &Error{Code: "INVALID_RESPONSE", Message: "memory deletion was not acknowledged", OutcomeUnknown: true}
	}
	return nil
}
func (c *Client) Reindex(ctx context.Context, key string, limit int) (out ReindexResult, err error) {
	if err = clienthttp.Required(key); err != nil {
		return
	}
	err = c.wire.Call(ctx, prefix+"Reindex", struct {
		OpKey string `json:"op_key"`
		Limit int    `json:"limit,omitempty"`
	}{key, limit}, &out, true)
	return
}

// DeleteSubject is a privileged erasure operation, not a per-operation Fence.
func (c *Client) DeleteSubject(ctx context.Context, key, subject string) (deleted int, err error) {
	for _, value := range []string{key, subject} {
		if err = clienthttp.Required(value); err != nil {
			return
		}
	}
	var out struct {
		Deleted int `json:"deleted"`
	}
	err = c.wire.Call(ctx, prefix+"DeleteSubject", map[string]string{"op_key": key, "subject": subject}, &out, true)
	return out.Deleted, err
}
