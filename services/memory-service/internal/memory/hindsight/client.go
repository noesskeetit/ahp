package hindsight

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
)

type Client struct {
	BaseURL string
	APIKey  string
	HTTP    *http.Client
}

func New(baseURL, key string) (*Client, error) {
	u, err := url.Parse(baseURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid Hindsight URL")
	}
	return &Client{strings.TrimRight(baseURL, "/"), key, &http.Client{Timeout: 15 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func path(bank string) string { return "/v1/default/banks/" + url.PathEscape(bank) }

// dnsNamespace is the RFC 4122 DNS namespace, the base for the version-5 UUIDs
// this client derives for native operation ids.
var dnsNamespace = [16]byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}

// nativeOperationID maps an AHP job id onto the operation id Hindsight accepts.
// Hindsight 1.1.0 validates operation_id as a UUID and answers 422 to anything
// else, while AHP job ids are ULID-shaped: without this mapping every retain
// and correct is rejected by a running server, which no fixture-based test can
// show. The mapping is a deterministic RFC 4122 v5 UUID, so Status and Cancel
// recompute it instead of carrying a second identity through the queue, and the
// AHP job id still travels in the item metadata for the reverse direction.
func nativeOperationID(jobID string) string {
	sum := sha1.Sum(append(append([]byte{}, dnsNamespace[:]...), []byte("ahp-memory-op/"+jobID)...))
	var uuid [16]byte
	copy(uuid[:], sum[:16])
	uuid[6] = (uuid[6] & 0x0f) | 0x50 // version 5
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}
func (c *Client) request(ctx context.Context, method, path string, body, out any, missingOK bool) error {
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return memory.Fail(502, "hindsight_unavailable", "Hindsight request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound && missingOK {
		return memory.ErrMissing
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Never relay upstream bodies: they can include prompts, source text or keys.
		if resp.StatusCode == 400 || resp.StatusCode == 422 {
			return memory.Fail(422, "hindsight_rejected", "Hindsight rejected this operation")
		}
		return memory.Fail(502, "hindsight_unavailable", "Hindsight returned an unsuccessful response")
	}
	if out == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<20))
		return err
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(out); err != nil {
		return memory.Fail(502, "invalid_hindsight_response", "invalid Hindsight response")
	}
	return nil
}
func (c *Client) Configure(ctx context.Context, b memory.Bank) error {
	mission := memory.ProjectMission
	mode := "concise"
	if b.Scope == memory.Personal {
		mission = memory.PersonalMission
		// The personal plane is the employee's own record point (PDF rev3.2 L196,
		// L374): what the employee stores must be recallable verbatim, so a
		// code word / identifier / link survives extraction instead of being
		// distilled away. The PersonalMission keeps the noise discipline, so
		// runtime/telemetry lines still extract nothing. The project/workspace
		// plane keeps "concise" distillation, which is what the PDF asks there.
		mode = "verbatim"
	}
	profile := map[string]any{"retain_mission": mission, "retain_extraction_mode": mode, "enable_observations": true}
	if err := c.request(ctx, http.MethodPut, path(b.ID), profile, nil, false); err != nil {
		return err
	}
	return c.request(ctx, http.MethodPatch, path(b.ID)+"/config", map[string]any{"updates": map[string]any{"retain_mission": mission, "retain_extraction_mode": mode, "store_document_text": false, "enable_observations": true, "enable_auto_consolidation": true}}, nil, false)
}
func (c *Client) Retain(ctx context.Context, b memory.Bank, j memory.Job) error {
	metadata := map[string]string{}
	for k, v := range j.Metadata {
		metadata[k] = v
	}
	metadata["tenant_id"] = j.Identity.TenantID
	metadata["user_id"] = j.Identity.UserID
	metadata["project_id"] = j.ProjectID
	metadata["operation_id"] = j.ID
	metadata["mission_version"] = memory.MissionVersion
	item := map[string]any{"content": j.Content, "document_id": "ahp-" + j.ID, "timestamp": j.CreatedAt.Format(time.RFC3339Nano), "metadata": metadata}
	var result struct {
		OperationID string `json:"operation_id"`
		Success     bool   `json:"success"`
	}
	op := nativeOperationID(j.ID)
	if err := c.request(ctx, http.MethodPost, path(b.ID)+"/memories", map[string]any{"items": []any{item}, "async": true, "operation_id": op}, &result, false); err != nil {
		return err
	}
	if !result.Success || result.OperationID != op {
		return memory.Fail(502, "operation_identity_mismatch", "Hindsight did not acknowledge the requested operation ID")
	}
	return nil
}
func (c *Client) Status(ctx context.Context, bank, id string) (string, error) {
	var result struct {
		Status string `json:"status"`
	}
	err := c.request(ctx, http.MethodGet, path(bank)+"/operations/"+url.PathEscape(nativeOperationID(id)), nil, &result, true)
	if err == memory.ErrMissing {
		return "not_found", nil
	}
	return result.Status, err
}

// RetainFactsCommitted reports how many memory facts a completed retain
// operation committed upstream, read from the operation's progress/result
// metadata. It is best-effort: an operation that omits the counts yields 0 with
// no error, so the caller records an observable "0 facts committed" signal for a
// silent or empty extraction rather than treating an older upstream as a hard
// failure. Upstream bodies are never relayed; only the two integer counts are
// parsed (facts_committed takes precedence over unit_ids_count).
func (c *Client) RetainFactsCommitted(ctx context.Context, bank, id string) (int, error) {
	var result struct {
		Progress struct {
			Detail struct {
				FactsCommitted *int `json:"facts_committed"`
			} `json:"detail"`
		} `json:"progress"`
		ResultMetadata struct {
			UnitIDsCount *int `json:"unit_ids_count"`
		} `json:"result_metadata"`
	}
	if err := c.request(ctx, http.MethodGet, path(bank)+"/operations/"+url.PathEscape(nativeOperationID(id)), nil, &result, true); err != nil {
		if err == memory.ErrMissing {
			return 0, nil
		}
		return 0, err
	}
	switch {
	case result.Progress.Detail.FactsCommitted != nil:
		return *result.Progress.Detail.FactsCommitted, nil
	case result.ResultMetadata.UnitIDsCount != nil:
		return *result.ResultMetadata.UnitIDsCount, nil
	}
	return 0, nil
}

func (c *Client) Cancel(ctx context.Context, bank, id string) error {
	state, err := c.Status(ctx, bank, id)
	if err != nil {
		return err
	}
	if state != "pending" && state != "processing" {
		return nil
	}
	err = c.request(ctx, http.MethodDelete, path(bank)+"/operations/"+url.PathEscape(nativeOperationID(id)), nil, nil, true)
	if err == memory.ErrMissing {
		return nil
	}
	return err
}
func (c *Client) Recall(ctx context.Context, b memory.Bank, query string, budget int) ([]memory.NativeFact, error) {
	var result struct {
		Results []memory.NativeFact `json:"results"`
	}
	err := c.request(ctx, http.MethodPost, path(b.ID)+"/memories/recall", map[string]any{"query": query, "max_tokens": budget, "budget": "mid", "prefer_observations": true, "trace": false}, &result, true)
	if err == memory.ErrMissing {
		return []memory.NativeFact{}, nil
	}
	return result.Results, err
}
func (c *Client) Correct(ctx context.Context, b memory.Bank, j memory.Job) error {
	p := path(b.ID) + "/memories/" + url.PathEscape(j.MemoryID)
	var fact memory.NativeFact
	if err := c.request(ctx, http.MethodGet, p, nil, &fact, true); err != nil {
		if err == memory.ErrMissing {
			return memory.Fail(404, "memory_not_found", "source memory does not exist")
		}
		return err
	}
	if fact.Type != "world" && fact.Type != "experience" {
		return memory.Fail(422, "derived_memory", "correct source facts instead of observations")
	}
	body := map[string]any{"text": j.Content, "reason": j.Reason}
	if j.Kind == "invalidate" {
		body = map[string]any{"state": "invalidated", "reason": j.Reason}
	}
	return c.request(ctx, http.MethodPatch, p, body, nil, false)
}
func (c *Client) DeleteBank(ctx context.Context, bank string) error {
	err := c.request(ctx, http.MethodDelete, path(bank), nil, nil, true)
	if err == memory.ErrMissing {
		return nil
	}
	return err
}

func (c *Client) List(ctx context.Context, b memory.Bank, limit, offset int) ([]memory.NativeFact, int, error) {
	var result struct {
		Items []struct {
			ID          string            `json:"id"`
			Text        string            `json:"text"`
			Type        string            `json:"fact_type"`
			MentionedAt string            `json:"mentioned_at"`
			Metadata    map[string]string `json:"metadata"`
		} `json:"items"`
		Total int `json:"total"`
	}
	q := url.Values{"limit": {strconv.Itoa(limit)}, "offset": {strconv.Itoa(offset)}, "state": {"valid"}}
	err := c.request(ctx, http.MethodGet, path(b.ID)+"/memories/list?"+q.Encode(), nil, &result, true)
	if err == memory.ErrMissing {
		return []memory.NativeFact{}, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	facts := make([]memory.NativeFact, 0, len(result.Items))
	for _, f := range result.Items {
		facts = append(facts, memory.NativeFact{ID: f.ID, Text: f.Text, Type: f.Type, MentionedAt: f.MentionedAt, Metadata: f.Metadata})
	}
	return facts, result.Total, nil
}
