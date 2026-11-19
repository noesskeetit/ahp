package space

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// AgentControl speaks only to the Registry owner. The remote task handle and
// workload authority are resolved there, never supplied by a browser or runner.
type AgentControl interface {
	Send(context.Context, string, string, json.RawMessage, string) (*AgentTaskSnapshot, error)
	Read(context.Context, string, string) (*AgentTaskSnapshot, error)
	Cancel(context.Context, string, string) (*AgentTaskSnapshot, error)
}

type AgentTaskSnapshot struct {
	ID        string `json:"id"`
	ContextID string `json:"contextId,omitempty"`
	Status    struct {
		State   string          `json:"state"`
		Message json.RawMessage `json:"message,omitempty"`
	} `json:"status"`
	Artifacts []json.RawMessage `json:"artifacts,omitempty"`
	History   []json.RawMessage `json:"history,omitempty"`
}

func NewAgentControlClient(origin string, client *http.Client) (AgentControl, error) {
	owner, err := newRegistryOwnerClient(origin, client)
	if err != nil {
		return nil, err
	}
	return owner, nil
}

func (p *profileClient) Send(ctx context.Context, token, installation string, input json.RawMessage, key string) (*AgentTaskSnapshot, error) {
	body := map[string]any{"installation_id": installation, "input": input}
	if key != "" {
		body["continue"] = true
		body["key"] = key
	}
	return p.agentControl(ctx, "SendAgentTurn", token, body)
}

func (p *profileClient) Read(ctx context.Context, token, installation string) (*AgentTaskSnapshot, error) {
	return p.agentControl(ctx, "ReadAgentTurn", token, map[string]any{"installation_id": installation})
}

func (p *profileClient) Cancel(ctx context.Context, token, installation string) (*AgentTaskSnapshot, error) {
	return p.agentControl(ctx, "CancelAgentTurn", token, map[string]any{"installation_id": installation})
}

func (p *profileClient) agentControl(ctx context.Context, op, token string, body map[string]any) (*AgentTaskSnapshot, error) {
	if strings.TrimSpace(token) == "" || strings.TrimSpace(body["installation_id"].(string)) == "" {
		return nil, contracts.NewError(contracts.ErrInvalid, "agent control needs a run token and installation")
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, p.origin+"/v1/registry/"+op, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	setServiceRequest(r, token, "application/json")
	response, err := p.client.Do(r)
	if err != nil {
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent control owner API unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, profileRefusal(response)
	}
	data, err = io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil || len(data) > 1<<20 {
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent control owner response exceeded its bound")
	}
	var result struct {
		Task *AgentTaskSnapshot `json:"task"`
	}
	if json.Unmarshal(data, &result) != nil || result.Task == nil || result.Task.ID == "" {
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent control owner returned an invalid task")
	}
	// The pinned native SDK speaks A2A 1.0. Coordinator uses its own stable
	// status vocabulary; accept only known wire values, never an arbitrary
	// string obtained by stripping a prefix. Legacy snapshots remain readable.
	if state, ok := map[string]string{
		"TASK_STATE_SUBMITTED": "submitted", "TASK_STATE_WORKING": "working",
		"TASK_STATE_COMPLETED": "completed", "TASK_STATE_CANCELED": "canceled",
		"TASK_STATE_FAILED": "failed", "TASK_STATE_REJECTED": "rejected",
		"TASK_STATE_INPUT_REQUIRED": "input-required", "TASK_STATE_AUTH_REQUIRED": "auth-required",
	}[result.Task.Status.State]; ok {
		result.Task.Status.State = state
	}
	switch result.Task.Status.State {
	case "submitted", "working", "input-required", "completed", "canceled", "failed", "rejected", "auth-required", "unknown":
		return result.Task, nil
	default:
		return nil, contracts.NewError(contracts.ErrUnavailable, "agent control owner returned an invalid task state")
	}
}
